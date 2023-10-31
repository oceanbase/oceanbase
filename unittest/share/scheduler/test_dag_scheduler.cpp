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

#define USING_LOG_PREFIX TEST
#include <getopt.h>
#include <unistd.h>
#include <gtest/gtest.h>
#define protected public
#define private public
#include "storage/compaction/ob_tenant_tablet_scheduler.h"
#include "share/scheduler/ob_tenant_dag_scheduler.h"
#include "share/scheduler/ob_sys_task_stat.h"
#include "lib/atomic/ob_atomic.h"
#include "observer/omt/ob_tenant_node_balancer.h"
#include "share/scheduler/ob_dag_warning_history_mgr.h"
#include "storage/compaction/ob_tenant_compaction_progress.h"

int64_t dag_cnt = 1;
int64_t stress_time= 1; // 100ms
char log_level[20] = "INFO";
uint32_t time_slice = 1000;
uint32_t sleep_slice = 2 * time_slice;
const int64_t CHECK_TIMEOUT = 2 * 1000 * 1000; // larger than SCHEDULER_WAIT_TIME_MS

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
using namespace lib;
using namespace share;
using namespace omt;
namespace unittest
{

class TestAddTask : public ObITask
{
public:
  TestAddTask()
    : ObITask(ObITaskType::TASK_TYPE_UT), counter_(NULL),
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
  virtual int generate_next_task(ObITask *&next_task)
  {
    int ret = OB_SUCCESS;
    if (seq_ >= task_cnt_ - 1) {
      ret = OB_ITER_END;
      COMMON_LOG(INFO, "generate task end", K_(seq), K_(task_cnt));
    } else {
      ObIDag *dag = get_dag();
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

class TestMulTask : public ObITask
{
public:
  TestMulTask()
    : ObITask(ObITaskType::TASK_TYPE_UT), counter_(NULL), sleep_us_(0)
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

static const int64_t SLEEP_SLICE = 100;

class AtomicMulTask : public ObITask
{
public:
  AtomicMulTask() :
    ObITask(ObITask::TASK_TYPE_UT), seq_(0), cnt_(0), error_seq_(-1), op_(NULL), sleep_us_(0)
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
  virtual int generate_next_task(ObITask *&task)
  {
    int ret = OB_SUCCESS;
    ObIDag *dag = NULL;
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
      if (OB_FAIL(dag_yield())) {
        if (OB_CANCELED == ret) {
          COMMON_LOG(INFO, "Cancel this task since the whole dag is canceled", K(ret));
          break;
        } else {
          COMMON_LOG(WARN, "Invalid return value for dag_yield", K(ret));
        }
      }
      ::usleep(SLEEP_SLICE);
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
  // The seq triger process return OB_ERR_UNEXPECTED to test error handing
  int64_t error_seq_;
  AtomicOperator *op_;
  int sleep_us_;
};

class AtomicIncTask : public ObITask
{
public:
  AtomicIncTask() :
    ObITask(ObITask::TASK_TYPE_UT), seq_(0), cnt_(0), error_seq_(-1), op_(NULL), sleep_us_(0)
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
  virtual int generate_next_task(ObITask *&task)
  {
    int ret = OB_SUCCESS;
    ObIDag *dag = NULL;
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
    int64_t cnt = sleep_us_ / SLEEP_SLICE;
    for (int64_t i = 0; i < cnt; ++i) {
      if (OB_FAIL(dag_yield())) {
        if (OB_CANCELED == ret) {
          COMMON_LOG(INFO, "Cancel this task since the whole dag is canceled", K(ret));
          break;
        } else {
          COMMON_LOG(WARN, "Invalid return value for dag_yield", K(ret));
        }
      }
      ::usleep(SLEEP_SLICE);
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
  AtomicOperator *op_;
  int sleep_us_;
};

class LoopWaitTask : public ObITask
{
public:
  LoopWaitTask() :
    ObITask(ObITask::TASK_TYPE_UT), seq_(0), cnt_(0)
  {}
  int init(int64_t seq, int64_t cnt, bool &finish_flag)
  {
    seq_ = seq;
    cnt_ = cnt;
    finish_flag_ = &finish_flag;
    return OB_SUCCESS;
  }
  virtual int generate_next_task(ObITask *&task)
  {
    int ret = OB_SUCCESS;
    ObIDag *dag = NULL;
    LoopWaitTask *ntask = NULL;
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
      ntask->init(seq_ + 1, cnt_, *finish_flag_);
      task = ntask;
    }
    return ret;
  }
  virtual int process()
  {
    int ret = OB_SUCCESS;
    while (true) {
      if (OB_FAIL(dag_yield())) {
        if (OB_CANCELED == ret) {
          COMMON_LOG(INFO, "Cancel this task since the whole dag is canceled", K(ret));
          break;
        } else {
          COMMON_LOG(WARN, "Invalid return value for dag_yield", K(ret));
        }
      }
      if (nullptr != finish_flag_) {
        if (ATOMIC_LOAD(finish_flag_) == true) {
          break;
        }
      }
      ::usleep(SLEEP_SLICE);
    }
    return ret;
  }
  VIRTUAL_TO_STRING_KV("type", "LoopWait", K(*dag_), K_(seq), K_(cnt));
private:
  int64_t seq_;
  int64_t cnt_;
  bool *finish_flag_;
};

class MaybeCanceledLoopWaitTask : public ObITask
{
public:
  MaybeCanceledLoopWaitTask() :
    ObITask(ObITask::TASK_TYPE_UT), seq_(0), cnt_(0), cancel_seq_(0)
  {}
  int init(int64_t seq, int64_t cnt, int64_t cancel_seq, bool &finish_flag)
  {
    seq_ = seq;
    cnt_ = cnt;
    cancel_seq_ = cancel_seq;
    finish_flag_ = &finish_flag;
    return OB_SUCCESS;
  }
  virtual int generate_next_task(ObITask *&task)
  {
    int ret = OB_SUCCESS;
    ObIDag *dag = NULL;
    MaybeCanceledLoopWaitTask *ntask = NULL;
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
      ntask->init(seq_ + 1, cnt_, cancel_seq_, *finish_flag_);
      task = ntask;
    }
    return ret;
  }
  virtual int process()
  {
    int ret = OB_SUCCESS;
    ObTenantDagScheduler *scheduler = nullptr;

    if (seq_ == cancel_seq_) {
      if (OB_FAIL(dag_yield())) {
        if (OB_CANCELED != ret) {
          COMMON_LOG(WARN, "Invalid return value for dag_yield", K(ret));
        }
      }
      ::usleep(2*SLEEP_SLICE);
      if (OB_ISNULL(scheduler = MTL(ObTenantDagScheduler*))) {
        COMMON_LOG(WARN, "Failed to get tenant dag scheduler for this tenant", K_(seq), K_(cnt), K_(cancel_seq), KP(scheduler));
        ret = OB_ERR_UNEXPECTED;
      } else if (OB_FAIL(scheduler->cancel_dag(dag_, true))) {
        COMMON_LOG(WARN, "Failed to triger cancel this (running) dag", K_(seq), K_(cnt), K_(cancel_seq));
      } else {
        ret = OB_CANCELED;
        COMMON_LOG(INFO, "Successfully triger cancel this (running) dag", K_(seq), K_(cnt), K_(cancel_seq));
      }
    } else {
      while (true) {
        if (OB_FAIL(dag_yield()) && OB_CANCELED == ret) {
          COMMON_LOG(INFO, "Cancel this task since the whole dag is canceled", K_(seq), K_(cnt), K_(cancel_seq));
          break;
        }
        if (nullptr != finish_flag_) {
          if (ATOMIC_LOAD(finish_flag_) == true || seq_ % 2 == 1) {
            break;
          }
        }
        ::usleep(SLEEP_SLICE);
      }
    }
    return ret;
  }
  VIRTUAL_TO_STRING_KV("type", "MaybeCanceledLoopWaitTask", K(*dag_), K_(seq), K_(cnt), K_(cancel_seq));

private:
  int64_t seq_;
  int64_t cnt_;
  int64_t cancel_seq_;
  bool *finish_flag_;
};

template<class T>
int alloc_task(ObIDag &dag, T *&task) {
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

void wait_scheduler() {
  ObTenantDagScheduler *scheduler = MTL(ObTenantDagScheduler*);
  ASSERT_TRUE(nullptr != scheduler);
  while (!scheduler->is_empty()) {
    ::usleep(100000);
  }
}

class TestDag : public ObIDag
{
public:
  TestDag() :
      ObIDag(ObDagType::DAG_TYPE_MERGE_EXECUTE), id_(0), expect_(-1), expect_ret_(0), running_(false), tester_(NULL) { }
  explicit TestDag(const ObDagType::ObDagTypeEnum type) :
      ObIDag(type), id_(0), expect_(-1), expect_ret_(0), running_(false), tester_(NULL) { }
  virtual ~TestDag()
  {
    if (get_dag_status() == ObIDag::DAG_STATUS_FINISH
        || get_dag_status() == ObIDag::DAG_STATUS_NODE_FAILED) {
      if (running_ && -1 != expect_) {
        if (op_.value() != expect_
            || get_dag_ret() != expect_ret_) {
          if (OB_ALLOCATE_MEMORY_FAILED != get_dag_ret()) {
            if (NULL != tester_) {
              tester_->stop();
            }
            COMMON_LOG_RET(ERROR, OB_ERROR, "FATAL ERROR!!!", K_(expect), K(op_.value()), K_(expect_ret),
                K(get_dag_ret()), K_(id));
            common::right_to_die_or_duty_to_live();
          }
        }
      }
    }
  }
  int init(int64_t id, int expect_ret = 0, int64_t expect = -1, lib::ThreadPool *tester = NULL)
  {
    id_ = id;
    expect_ret_ = expect_ret;
    expect_ = expect;
    tester_ = tester;
    ObAddr addr(1683068975,9999);
    if (OB_SUCCESS != (ObSysTaskStatMgr::get_instance().set_self_addr(addr))) {
      COMMON_LOG_RET(WARN, OB_ERROR, "failed to add sys task", K(addr));
    }
    return OB_SUCCESS;
  }
  virtual int64_t hash() const { return murmurhash(&id_, sizeof(id_), 0);}
  virtual bool operator == (const ObIDag &other) const
  {
    bool bret = false;
    if (get_type() == other.get_type()) {
      const TestDag &dag = static_cast<const TestDag &>(other);
      bret = dag.id_ == id_;
    }
    return bret;
  }
  void set_id(int64_t id) { id_ = id; }
  AtomicOperator &get_op() { return op_; }
  void set_running() { running_ = true; }
  virtual int fill_info_param(compaction::ObIBasicInfoParam *&out_param,
      ObIAllocator &allocator) const override
  {
    int ret = OB_SUCCESS;
    if (!is_inited_) {
      ret = OB_NOT_INIT;
    } else if (OB_FAIL(ADD_DAG_WARN_INFO_PARAM(out_param, allocator, get_type(), 1, 1, "table_id", 10))) {
      COMMON_LOG(WARN, "fail to add dag warning info param", K(ret));
    }
    return ret;
  }
  virtual int fill_dag_key(char *buf, const int64_t buf_len) const override
  {
    UNUSEDx(buf, buf_len);
    return OB_SUCCESS;
  }
  virtual lib::Worker::CompatMode get_compat_mode() const override
  { return lib::Worker::CompatMode::MYSQL; }
  virtual uint64_t get_consumer_group_id() const override
  { return consumer_group_id_; }
  virtual bool is_ha_dag() const override { return false; }
  VIRTUAL_TO_STRING_KV(K_(is_inited), K_(type), K_(id), K(task_list_.get_size()));
protected:
  int64_t id_;
  // expect ret for op_
  int64_t expect_;
  // expect ret for this dag
  int expect_ret_;
  AtomicOperator op_;
  bool running_;
  lib::ThreadPool *tester_;
private:
  DISALLOW_COPY_AND_ASSIGN(TestDag);
};

class TestLPDag : public TestDag
{
public:
  TestLPDag() : TestDag(ObDagType::DAG_TYPE_DDL) {}
private:
  DISALLOW_COPY_AND_ASSIGN(TestLPDag);
};

class TestMPDag : public TestDag
{
public:
  TestMPDag() : TestDag(ObDagType::DAG_TYPE_FAST_MIGRATE) {}
private:
  DISALLOW_COPY_AND_ASSIGN(TestMPDag);
};

class TestHALowDag : public TestDag
{
public:
  TestHALowDag() : TestDag(ObDagType::DAG_TYPE_BACKUP_PREPARE) {}
private:
  DISALLOW_COPY_AND_ASSIGN(TestHALowDag);
};

class TestCompMidDag : public TestDag
{
public:
  TestCompMidDag() : TestDag(ObDagType::DAG_TYPE_MERGE_EXECUTE) {}
private:
  DISALLOW_COPY_AND_ASSIGN(TestCompMidDag);
};

class TestMemRelatedDag : public TestDag
{
public:
  TestMemRelatedDag() : TestDag(ObDagType::DAG_TYPE_MINI_MERGE) {}
private:
  DISALLOW_COPY_AND_ASSIGN(TestMemRelatedDag);
};

class TestCompLowDag : public TestDag
{
public:
  TestCompLowDag() : TestDag(ObDagType::DAG_TYPE_MAJOR_MERGE) {}
private:
  DISALLOW_COPY_AND_ASSIGN(TestCompLowDag);
};

class TestDDLDag : public TestDag
{
public:
  TestDDLDag() : TestDag(ObDagType::DAG_TYPE_DDL) {}
private:
  DISALLOW_COPY_AND_ASSIGN(TestDDLDag);
};

class TestPrepareTask : public ObITask
{
  static const int64_t inc_task_cnt = 8;
  static const int64_t mul_task_cnt = 6;
public:
  TestPrepareTask() : ObITask(ObITask::TASK_TYPE_UT), dag_id_(0), is_error_(false), sleep_us_(0), op_(NULL)
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

class TestCancelDag : public TestDag
{
public:
  static int64_t test_round;
  typedef TestPrepareTask CancelFinishTask;
  typedef MaybeCanceledLoopWaitTask CancelPrepareTask;
  TestCancelDag() : TestDag() {}
  int init_dag(int64_t cnt, int64_t cancel_seq, bool &finish_flag, AtomicOperator *op) {
    int ret = OB_SUCCESS;
    test_round++;
    COMMON_LOG(INFO, "Start Testing TestCancelDag", K(test_round));

    CancelPrepareTask *cancel_task = nullptr;
    CancelFinishTask *finish_task = nullptr;

    EXPECT_EQ(OB_SUCCESS, alloc_task(cancel_task));
    EXPECT_EQ(OB_SUCCESS, cancel_task->init(0, cnt, cancel_seq, finish_flag));
    EXPECT_EQ(OB_SUCCESS, add_task(*cancel_task));

    EXPECT_EQ(OB_SUCCESS, alloc_task(finish_task));
    EXPECT_EQ(OB_SUCCESS, finish_task->init(1, op));
    EXPECT_EQ(OB_SUCCESS, add_task(*finish_task));
    EXPECT_EQ(OB_SUCCESS, cancel_task->add_child(*finish_task));
    return ret;
  }

private:
  DISALLOW_COPY_AND_ASSIGN(TestCancelDag);
};

int64_t TestCancelDag::test_round = 0;

class TestCyclePrepare : public ObITask
{
public:
  TestCyclePrepare()
    : ObITask(ObITask::TASK_TYPE_UT), op_(NULL) {}
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
      dag->free_task(*mul_task);
      COMMON_LOG(WARN, "failed to add_task", K(ret));
    }
    return ret;
  }
private:
  AtomicOperator *op_;
};

class DagSchedulerStressTester : public lib::ThreadPool
{
  static const int64_t STRESS_THREAD_NUM = 16;
public:
  DagSchedulerStressTester() : scheduler_(nullptr), test_time_(0) {}
  ~DagSchedulerStressTester() { scheduler_ = nullptr; }

  int init(ObTenantDagScheduler *scheduler, int64_t test_time)
  {
    int ret = OB_SUCCESS;
    if (OB_ISNULL(scheduler)) {
      ret = OB_INVALID_ARGUMENT;
      COMMON_LOG(WARN, "get invalid args", K(ret), K(scheduler));
    } else {
      scheduler_ = scheduler;
      test_time_ = test_time * 1000;
    }
    return ret;
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
    int ret_code = system("grep ERROR test_dag_scheduler.log -q | grep -v 'Fail to lock' | grep -v 'invalid tg id'");
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
    ObTenantBase * tenant_base = OB_NEW(ObTenantBase, "TestBase", 1001);
    tenant_base->init();
    tenant_base->set(scheduler_);
    ObTenantEnv::set_tenant(tenant_base);

    while (!has_set_stop()
           && OB_SUCC(ret)
           && (ObTimeUtility::current_time() - start_time < test_time_)) {
      const int64_t dag_id = get_dag_id();
      TestDag *dag = NULL;
      TestPrepareTask *task = NULL;
      int expect_ret = (dag_id % 10 == 0 ? OB_ERR_UNEXPECTED : OB_SUCCESS);
      int64_t expect_value = (dag_id % 10 == 0 ? 0 : 8);

      switch (dag_id % ObDagPrio::DAG_PRIO_MAX) {
      case ObDagPrio::DAG_PRIO_DDL: {
        TestLPDag *lp_dag = NULL;
        if (OB_SUCCESS != (tmp_ret = scheduler_->alloc_dag(lp_dag))) {
          if (OB_ALLOCATE_MEMORY_FAILED != tmp_ret) {
            ret = tmp_ret;
            COMMON_LOG(ERROR, "failed to allocate dag", K(ret));
          } else {
            COMMON_LOG(WARN, "out of memory", K(scheduler_->get_cur_dag_cnt()));
          }
        } else {
          dag = lp_dag;
        }
        break;
      }
      case ObDagPrio::DAG_PRIO_COMPACTION_LOW: {
        TestMPDag *mp_dag= NULL;
        if (OB_SUCCESS != (tmp_ret = scheduler_->alloc_dag(mp_dag))) {
          if (OB_ALLOCATE_MEMORY_FAILED != tmp_ret) {
            ret = tmp_ret;
            COMMON_LOG(ERROR, "failed to allocate dag", K(ret));
          } else {
            COMMON_LOG(WARN, "out of memory", K(scheduler_->get_cur_dag_cnt()));
          }
        } else {
          dag = mp_dag;
        }
        break;
      }
      default : {
        TestMPDag *hp_dag= NULL;
        if (OB_SUCCESS != (tmp_ret = scheduler_->alloc_dag(hp_dag))) {
          if (OB_ALLOCATE_MEMORY_FAILED != tmp_ret) {
            ret = tmp_ret;
            COMMON_LOG(ERROR, "failed to allocate dag", K(ret));
          } else {
            COMMON_LOG(WARN, "out of memory", K(scheduler_->get_cur_dag_cnt()));
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
      } else if (OB_SUCCESS != (tmp_ret = alloc_task(*dag, task))){
        COMMON_LOG(WARN, "failed to alloc task", K(tmp_ret));
      } else if (OB_FAIL(task->init(dag_id, NULL, expect_ret != OB_SUCCESS))) {
        COMMON_LOG(WARN, "failed to init task", K(ret));
      } else if (OB_FAIL(dag->add_task(*task))) {
        COMMON_LOG(WARN, "failed to add task", K(ret));
      } else {
        if (OB_SUCCESS != (tmp_ret = scheduler_->add_dag(dag))) {
          if (OB_SIZE_OVERFLOW != tmp_ret) {
            COMMON_LOG(ERROR, "failed to add dag", K(tmp_ret), K(*dag));
          }
          scheduler_->free_dag(*dag);
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
  ObTenantDagScheduler *scheduler_;
  int64_t test_time_;
};

int64_t DagSchedulerStressTester::counter_ = 0;

class TestDagScheduler : public ::testing::Test
{
public:
  TestDagScheduler()
    : tenant_id_(1001),
      tablet_scheduler_(nullptr),
      scheduler_(nullptr),
      dag_history_mgr_(nullptr),
      tenant_base_(1001),
      allocator_("DagScheduler"),
      inited_(false)
  { }
  ~TestDagScheduler() {}
  void SetUp()
  {
    if (!inited_) {
      ObMallocAllocator::get_instance()->create_and_add_tenant_allocator(1001);
      inited_ = true;
    }
    tablet_scheduler_ = OB_NEW(compaction::ObTenantTabletScheduler, ObModIds::TEST);
    tenant_base_.set(tablet_scheduler_);

    scheduler_ = OB_NEW(ObTenantDagScheduler, ObModIds::TEST);
    tenant_base_.set(scheduler_);

    dag_history_mgr_ = OB_NEW(ObDagWarningHistoryManager, ObModIds::TEST);
    tenant_base_.set(dag_history_mgr_);

    ObTenantEnv::set_tenant(&tenant_base_);
    ASSERT_EQ(OB_SUCCESS, tenant_base_.init());

    ObMallocAllocator *ma = ObMallocAllocator::get_instance();
    ASSERT_EQ(OB_SUCCESS, ma->set_tenant_limit(tenant_id_, 1LL << 30));
  }
  void TearDown()
  {
    tablet_scheduler_->destroy();
    tablet_scheduler_ = nullptr;
    scheduler_->destroy();
    scheduler_ = nullptr;
    dag_history_mgr_->~ObDagWarningHistoryManager();
    dag_history_mgr_ = nullptr;
    tenant_base_.destroy();
    ObTenantEnv::set_tenant(nullptr);
  }
private:
  const uint64_t tenant_id_;
  compaction::ObTenantTabletScheduler *tablet_scheduler_;
  ObTenantDagScheduler *scheduler_;
  ObDagWarningHistoryManager *dag_history_mgr_;
  ObTenantBase tenant_base_;
  ObArenaAllocator allocator_;
  bool inited_;
  DISALLOW_COPY_AND_ASSIGN(TestDagScheduler);
};


TEST_F(TestDagScheduler, test_init)
{
  ObTenantDagScheduler *scheduler = MTL(ObTenantDagScheduler*);
  ASSERT_TRUE(nullptr != scheduler);

  // invalid thread cnt
  EXPECT_EQ(OB_INVALID_ARGUMENT, scheduler->init(MTL_ID(), time_slice, time_slice, -1));

  EXPECT_EQ(OB_SUCCESS, scheduler->init(MTL_ID(),time_slice, time_slice, 100));
  EXPECT_EQ(OB_INIT_TWICE, scheduler->init(MTL_ID()));
}


TEST_F(TestDagScheduler, basic_test)
{
  ObTenantDagScheduler *scheduler = MTL(ObTenantDagScheduler*);
  ASSERT_TRUE(nullptr != scheduler);
  ASSERT_EQ(OB_SUCCESS, scheduler->init(MTL_ID(), time_slice));

  int ret = OB_SUCCESS;
  TestDag *dag = NULL;
  TestDag *dup_dag = NULL;
  int64_t counter = 0;
  //simple two-level dag
  if (OB_FAIL(scheduler->alloc_dag(dag))) {
    COMMON_LOG(WARN, "failed to alloc dag");
  } else if (NULL == dag) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "dag is null", K(ret));
  } else if (OB_FAIL(dag->init(1))) {
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
  if (OB_FAIL(scheduler->alloc_dag(dup_dag))) {
    COMMON_LOG(WARN, "failed to alloc dag");
  } else if (NULL == dup_dag) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "dag is null", K(ret));
  } else if (OB_FAIL(dup_dag->init(1))) {
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
        dup_dag->free_task(*mul_task);
      }
    }
  }
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_EQ(OB_SUCCESS, scheduler->add_dag(dag));
  EXPECT_EQ(OB_EAGAIN, scheduler->add_dag(dup_dag));
  if (OB_NOT_NULL(dup_dag)) {
    scheduler->free_dag(*dup_dag);
  }
  wait_scheduler();
  EXPECT_EQ(counter, 20);

  // three level dag that each level would generate dynamic tasks
  AtomicOperator op(0);
  TestDag *dag1 = NULL;
  AtomicIncTask *inc_task = NULL;
  AtomicIncTask *inc_task1 = NULL;
  AtomicMulTask *mul_task = NULL;
  EXPECT_EQ(OB_SUCCESS, scheduler->alloc_dag(dag1));

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
  EXPECT_EQ(OB_SUCCESS, scheduler->add_dag(dag1));
  wait_scheduler();
  EXPECT_EQ(170, op.value());

  // two-level dag with 2 tasks on the first level
  op.reset();
  EXPECT_EQ(OB_SUCCESS, scheduler->alloc_dag(dag1));
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
  EXPECT_EQ(OB_SUCCESS, scheduler->add_dag(dag1));
  wait_scheduler();
  EXPECT_EQ(320, op.value());

  // a dag with single task which generate all other tasks while processing
  TestPrepareTask *prepare_task = NULL;
  op.reset();
  EXPECT_EQ(OB_SUCCESS, scheduler->alloc_dag(dag1));
  EXPECT_EQ(OB_SUCCESS, dag1->init(1));
  EXPECT_EQ(OB_SUCCESS, alloc_task(*dag1, prepare_task));
  EXPECT_EQ(OB_SUCCESS, prepare_task->init(1, &op));
  EXPECT_EQ(OB_SUCCESS, dag1->add_task(*prepare_task));
  EXPECT_EQ(OB_SUCCESS, scheduler->add_dag(dag1));
  wait_scheduler();
  EXPECT_EQ(8, op.value());

  op.reset();
  EXPECT_EQ(OB_SUCCESS, scheduler->alloc_dag(dag1));
  EXPECT_EQ(OB_SUCCESS, dag1->init(1));
  EXPECT_EQ(OB_SUCCESS, alloc_task(*dag1, prepare_task));
  EXPECT_EQ(OB_SUCCESS, prepare_task->init(1, &op, false, 1000*1000));
  EXPECT_EQ(OB_SUCCESS, dag1->add_task(*prepare_task));
  EXPECT_EQ(OB_SUCCESS, scheduler->add_dag(dag1));
  wait_scheduler();
  EXPECT_EQ(8, op.value());
}

TEST_F(TestDagScheduler, test_cycle)
{
  ObTenantDagScheduler *scheduler = MTL(ObTenantDagScheduler*);
  ASSERT_TRUE(nullptr != scheduler);
  ASSERT_EQ(OB_SUCCESS, scheduler->init(MTL_ID(), time_slice));

  int ret = OB_SUCCESS;
  TestDag *dag = NULL;
  int64_t counter = 0;
  if (OB_FAIL(scheduler->alloc_dag(dag))) {
    COMMON_LOG(WARN, "failed to alloc dag");
  } else if (NULL == dag) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "dag is null", K(ret));
  } else if (OB_FAIL(dag->init(1))) {
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
        scheduler->free_dag(*dag);
      }
    }
  }
  TestCyclePrepare *prepare_task = NULL;
  AtomicOperator op;
  EXPECT_EQ(OB_SUCCESS, scheduler->alloc_dag(dag));
  EXPECT_EQ(OB_SUCCESS, dag->init(1, OB_INVALID_ARGUMENT, 0));
  EXPECT_EQ(OB_SUCCESS, alloc_task(*dag, prepare_task));
  EXPECT_EQ(OB_SUCCESS, prepare_task->init(&op));
  EXPECT_EQ(OB_SUCCESS, dag->add_task(*prepare_task));
  EXPECT_EQ(OB_SUCCESS, scheduler->add_dag(dag));
  wait_scheduler();
  EXPECT_EQ(0, op.value());
}

TEST_F(TestDagScheduler, test_priority)
{
  ObTenantDagScheduler *scheduler = MTL(ObTenantDagScheduler*);
  ASSERT_TRUE(nullptr != scheduler);
  ASSERT_EQ(OB_SUCCESS, scheduler->init(MTL_ID(), time_slice));

  AtomicOperator op(0);

  int32_t thread_cnt = scheduler->get_work_thread_num();

  int32_t concurrency = 60;
  bool finish_flag[5] = {false, false, false, false, false};

  TestDDLDag *dag1 = NULL;
  LoopWaitTask *inc_task = NULL;
  EXPECT_EQ(OB_SUCCESS, scheduler->alloc_dag(dag1));
  EXPECT_EQ(OB_SUCCESS, dag1->init(1));
  EXPECT_EQ(OB_SUCCESS, alloc_task(*dag1, inc_task));
  EXPECT_EQ(OB_SUCCESS, inc_task->init(1, concurrency, finish_flag[0]));
  EXPECT_EQ(OB_SUCCESS, dag1->add_task(*inc_task));
  TestHALowDag *dag2 = NULL;
  LoopWaitTask *mul_task = NULL;
  EXPECT_EQ(OB_SUCCESS, scheduler->alloc_dag(dag2));
  EXPECT_EQ(OB_SUCCESS, dag2->init(2));
  EXPECT_EQ(OB_SUCCESS, alloc_task(*dag2, mul_task));
  EXPECT_EQ(OB_SUCCESS, mul_task->init(1, concurrency, finish_flag[1]));
  EXPECT_EQ(OB_SUCCESS, dag2->add_task(*mul_task));
  TestCompLowDag *dag3 = NULL;
  EXPECT_EQ(OB_SUCCESS, scheduler->alloc_dag(dag3));
  EXPECT_EQ(OB_SUCCESS, dag3->init(3));
  EXPECT_EQ(OB_SUCCESS, alloc_task(*dag3, inc_task));
  EXPECT_EQ(OB_SUCCESS, inc_task->init(1, concurrency, finish_flag[2]));
  EXPECT_EQ(OB_SUCCESS, dag3->add_task(*inc_task));
  TestMemRelatedDag *dag4 = NULL;
  EXPECT_EQ(OB_SUCCESS, scheduler->alloc_dag(dag4));
  EXPECT_EQ(OB_SUCCESS, dag4->init(4));
  EXPECT_EQ(OB_SUCCESS, alloc_task(*dag4, inc_task));
  EXPECT_EQ(OB_SUCCESS, inc_task->init(1, concurrency, finish_flag[3]));
  EXPECT_EQ(OB_SUCCESS, dag4->add_task(*inc_task));
  TestCompMidDag *dag5 = NULL;
  EXPECT_EQ(OB_SUCCESS, scheduler->alloc_dag(dag5));
  EXPECT_EQ(OB_SUCCESS, dag5->init(5));
  EXPECT_EQ(OB_SUCCESS, alloc_task(*dag5, inc_task));
  EXPECT_EQ(OB_SUCCESS, inc_task->init(1, concurrency, finish_flag[4]));
  EXPECT_EQ(OB_SUCCESS, dag5->add_task(*inc_task));

  const int64_t uplimit = 10;
  EXPECT_EQ(OB_SUCCESS, scheduler->set_thread_score(ObDagPrio::DAG_PRIO_DDL, uplimit));
  EXPECT_EQ(OB_SUCCESS, scheduler->set_thread_score(ObDagPrio::DAG_PRIO_HA_LOW, uplimit));
  EXPECT_EQ(OB_SUCCESS, scheduler->set_thread_score(ObDagPrio::DAG_PRIO_COMPACTION_MID, uplimit));
  EXPECT_EQ(OB_SUCCESS, scheduler->set_thread_score(ObDagPrio::DAG_PRIO_HA_HIGH, uplimit));
  EXPECT_EQ(OB_SUCCESS, scheduler->set_thread_score(ObDagPrio::DAG_PRIO_COMPACTION_HIGH, uplimit));

  // 13 threads in total
  int64_t max_uplimit = uplimit;
  int64_t threads_sum = 0;
  int64_t work_thread_num = 0;
  for (int64_t i = 0; i < ObDagPrio::DAG_PRIO_MAX; ++i) { // calc sum of default_low_limit
    threads_sum += scheduler->prio_sche_[i].limits_;
  }
  work_thread_num = threads_sum;

  EXPECT_EQ(work_thread_num, scheduler->get_work_thread_num());
  scheduler->dump_dag_status();

  // low priority run at max speed
  EXPECT_EQ(OB_SUCCESS, scheduler->add_dag(dag1));
  CHECK_EQ_UTIL_TIMEOUT(uplimit, scheduler->get_running_task_cnt(ObDagPrio::DAG_PRIO_DDL));

  EXPECT_EQ(OB_SUCCESS, scheduler->add_dag(dag2));
  CHECK_EQ_UTIL_TIMEOUT(uplimit, scheduler->get_running_task_cnt(ObDagPrio::DAG_PRIO_HA_LOW));
  CHECK_EQ_UTIL_TIMEOUT(uplimit, scheduler->get_running_task_cnt(ObDagPrio::DAG_PRIO_DDL));

  dag3->set_priority(ObDagPrio::DAG_PRIO_HA_HIGH);
  EXPECT_EQ(OB_SUCCESS, scheduler->add_dag(dag3));
  CHECK_EQ_UTIL_TIMEOUT(uplimit, scheduler->get_running_task_cnt(ObDagPrio::DAG_PRIO_HA_HIGH));
  CHECK_EQ_UTIL_TIMEOUT(uplimit, scheduler->get_running_task_cnt(ObDagPrio::DAG_PRIO_HA_LOW));
  CHECK_EQ_UTIL_TIMEOUT(uplimit, scheduler->get_running_task_cnt(ObDagPrio::DAG_PRIO_DDL));

  EXPECT_EQ(OB_SUCCESS, scheduler->add_dag(dag4));
  CHECK_EQ_UTIL_TIMEOUT(uplimit, scheduler->get_running_task_cnt(ObDagPrio::DAG_PRIO_COMPACTION_HIGH));
  CHECK_EQ_UTIL_TIMEOUT(uplimit, scheduler->get_running_task_cnt(ObDagPrio::DAG_PRIO_HA_HIGH));
  CHECK_EQ_UTIL_TIMEOUT(uplimit, scheduler->get_running_task_cnt(ObDagPrio::DAG_PRIO_HA_LOW));
  CHECK_EQ_UTIL_TIMEOUT(uplimit, scheduler->get_running_task_cnt(ObDagPrio::DAG_PRIO_DDL));

  EXPECT_EQ(OB_SUCCESS, scheduler->add_dag(dag5));
  CHECK_EQ_UTIL_TIMEOUT(uplimit, scheduler->get_running_task_cnt(ObDagPrio::DAG_PRIO_COMPACTION_HIGH));
  CHECK_EQ_UTIL_TIMEOUT(uplimit, scheduler->get_running_task_cnt(ObDagPrio::DAG_PRIO_HA_HIGH));
  CHECK_EQ_UTIL_TIMEOUT(uplimit, scheduler->get_running_task_cnt(ObDagPrio::DAG_PRIO_COMPACTION_MID));
  CHECK_EQ_UTIL_TIMEOUT(uplimit, scheduler->get_running_task_cnt(ObDagPrio::DAG_PRIO_HA_LOW));
  CHECK_EQ_UTIL_TIMEOUT(uplimit, scheduler->get_running_task_cnt(ObDagPrio::DAG_PRIO_DDL));

  for (int i = 0; i < 5; ++i) {
    finish_flag[i] = true;
  }

  wait_scheduler();
}

TEST_F(TestDagScheduler, test_error_handling)
{
  ObTenantDagScheduler *scheduler = MTL(ObTenantDagScheduler*);
  ASSERT_TRUE(nullptr != scheduler);
  ASSERT_EQ(OB_SUCCESS, scheduler->init(MTL_ID(), time_slice));

  AtomicOperator op(0);
  TestDag *dag = NULL;
  AtomicMulTask *mul_task = NULL;
  AtomicIncTask *inc_task = NULL;
  EXPECT_EQ(OB_SUCCESS, scheduler->alloc_dag(dag));
  EXPECT_EQ(OB_SUCCESS, dag->init(1));
  EXPECT_EQ(OB_SUCCESS, alloc_task(*dag, mul_task));
  EXPECT_EQ(OB_SUCCESS, mul_task->init(1, 10, op, 0, 8));
  EXPECT_EQ(OB_SUCCESS, dag->add_task(*mul_task));
  EXPECT_EQ(OB_SUCCESS, alloc_task(*dag, inc_task));
  EXPECT_EQ(OB_SUCCESS, inc_task->init(1, 10, op));
  EXPECT_EQ(OB_SUCCESS, mul_task->add_child(*inc_task));
  EXPECT_EQ(OB_SUCCESS, dag->add_task(*inc_task));
  EXPECT_EQ(OB_SUCCESS, scheduler->add_dag(dag));
  wait_scheduler();
  EXPECT_EQ(0, op.value());

  EXPECT_EQ(OB_SUCCESS, scheduler->alloc_dag(dag));
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
  EXPECT_EQ(OB_SUCCESS, scheduler->add_dag(dag));
  wait_scheduler();
  EXPECT_EQ(10, op.value());

  TestDag *dag1 = NULL;
  TestPrepareTask *prepare_task = NULL;
  op.reset();
  EXPECT_EQ(OB_SUCCESS, scheduler->alloc_dag(dag1));
  EXPECT_EQ(OB_SUCCESS, dag1->init(1));
  EXPECT_EQ(OB_SUCCESS, alloc_task(*dag1, prepare_task));
  EXPECT_EQ(OB_SUCCESS, prepare_task->init(1, &op, true));
  EXPECT_EQ(OB_SUCCESS, dag1->add_task(*prepare_task));
  EXPECT_EQ(OB_SUCCESS, scheduler->add_dag(dag1));
  wait_scheduler();
  EXPECT_EQ(0, op.value());

  op.reset();
  EXPECT_EQ(OB_SUCCESS, scheduler->alloc_dag(dag1));
  EXPECT_EQ(OB_SUCCESS, dag1->init(1));
  EXPECT_EQ(OB_SUCCESS, alloc_task(*dag1, prepare_task));
  EXPECT_EQ(OB_SUCCESS, prepare_task->init(1, &op, true, 1000*1000));
  EXPECT_EQ(OB_SUCCESS, dag1->add_task(*prepare_task));
  EXPECT_EQ(OB_SUCCESS, scheduler->add_dag(dag1));
  wait_scheduler();
  EXPECT_EQ(0, op.value());
}

void print_state(int64_t idx)
{
  ObTenantDagScheduler *scheduler = MTL(ObTenantDagScheduler*);
  ASSERT_TRUE(nullptr != scheduler);
  COMMON_LOG(INFO, "scheduler state: ", K(scheduler->get_total_running_task_cnt()), K(scheduler->work_thread_num_),
      K(scheduler->total_worker_cnt_), K(scheduler->prio_sche_[idx].limits_),
      K(scheduler->prio_sche_[idx].running_task_cnts_));
}

TEST_F(TestDagScheduler, test_set_concurrency)
{
  ObTenantDagScheduler *scheduler = MTL(ObTenantDagScheduler*);
  ASSERT_TRUE(nullptr != scheduler);

  int64_t prio = ObDagPrio::DAG_PRIO_HA_MID;
  int32_t uplimit = OB_DAG_PRIOS[prio].score_;
  EXPECT_EQ(OB_SUCCESS, scheduler->init(MTL_ID(), time_slice));

  int cnt = 30;

  scheduler->dump_dag_status();
  TestMPDag *dag = NULL;
  AtomicIncTask *inc_task = NULL;
  EXPECT_EQ(OB_SUCCESS, scheduler->alloc_dag(dag));
  EXPECT_EQ(OB_SUCCESS, dag->init(1));
  EXPECT_EQ(OB_SUCCESS, alloc_task(*dag, inc_task));
  EXPECT_EQ(OB_SUCCESS, inc_task->init(1, cnt, dag->get_op(), 10 * sleep_slice));
  EXPECT_EQ(OB_SUCCESS, dag->add_task(*inc_task));
  EXPECT_EQ(OB_SUCCESS, scheduler->add_dag(dag));

  CHECK_EQ_UTIL_TIMEOUT(OB_DAG_PRIOS[prio].score_, scheduler->prio_sche_[prio].limits_);
  CHECK_EQ_UTIL_TIMEOUT(uplimit, scheduler->prio_sche_[prio].limits_);
  CHECK_EQ_UTIL_TIMEOUT(uplimit, scheduler->get_total_running_task_cnt());
  EXPECT_TRUE(uplimit <= scheduler->work_thread_num_);
  // set max to 20
  EXPECT_EQ(OB_SUCCESS, scheduler->set_thread_score(prio, 20));
  EXPECT_EQ(20, scheduler->prio_sche_[prio].limits_);
  //EXPECT_EQ(41, scheduler->work_thread_num_);
  // set to 1
  EXPECT_EQ(OB_SUCCESS, scheduler->set_thread_score(prio, 1));
  print_state(prio);
  EXPECT_EQ(1, scheduler->prio_sche_[prio].limits_);
  CHECK_EQ_UTIL_TIMEOUT(1, scheduler->get_total_running_task_cnt());
  EXPECT_TRUE(1 <= scheduler->work_thread_num_);
  // set to 2
  EXPECT_EQ(OB_SUCCESS, scheduler->set_thread_score(prio, 2));
  print_state(prio);
  EXPECT_EQ(2, scheduler->prio_sche_[prio].limits_);
  CHECK_EQ_UTIL_TIMEOUT(2, scheduler->get_total_running_task_cnt());
  EXPECT_TRUE(2 <= scheduler->work_thread_num_);
  // set to 5
  EXPECT_EQ(OB_SUCCESS, scheduler->set_thread_score(prio, 5));
  print_state(prio);
  EXPECT_EQ(5, scheduler->prio_sche_[prio].limits_);
  EXPECT_TRUE(5 <= scheduler->work_thread_num_);
  // set to 0
  EXPECT_EQ(OB_SUCCESS, scheduler->set_thread_score(prio, 0));
  print_state(prio);
  EXPECT_EQ(uplimit, scheduler->prio_sche_[prio].limits_);
  EXPECT_TRUE(uplimit <= scheduler->work_thread_num_);
  wait_scheduler();
}

TEST_F(TestDagScheduler, stress_test)
{
  ObTenantDagScheduler *scheduler = MTL(ObTenantDagScheduler*);
  ASSERT_TRUE(nullptr != scheduler);
  ASSERT_EQ(OB_SUCCESS, scheduler->init(MTL_ID(), time_slice, 64));

  DagSchedulerStressTester tester;
  tester.init(scheduler, stress_time);
  EXPECT_EQ(OB_SUCCESS, tester.do_stress());
  scheduler->destroy();
}

TEST_F(TestDagScheduler, test_get_dag_count)
{
  ObTenantDagScheduler *scheduler = MTL(ObTenantDagScheduler*);
  ASSERT_TRUE(nullptr != scheduler);
  ASSERT_EQ(OB_SUCCESS, scheduler->init(MTL_ID(), time_slice));

  TestMPDag *dag = NULL;
  TestMPDag *dag2 = NULL;
  TestMulTask *mul_task = NULL;
  TestMulTask *mul_task2 = NULL;
  int64_t counter = 1;

  EXPECT_EQ(0, scheduler->get_dag_count(ObDagType::DAG_TYPE_FAST_MIGRATE));
  EXPECT_EQ(0, scheduler->get_dag_count(ObDagType::DAG_TYPE_MERGE_EXECUTE));
  EXPECT_EQ(0, scheduler->get_dag_count(ObDagType::DAG_TYPE_DDL));
  EXPECT_EQ(-1, scheduler->get_dag_count(ObDagType::DAG_TYPE_MAX));

  EXPECT_EQ(OB_SUCCESS, scheduler->alloc_dag(dag));
  EXPECT_EQ(OB_SUCCESS, dag->init(1));
  EXPECT_EQ(OB_SUCCESS, dag->alloc_task(mul_task));
  EXPECT_EQ(OB_SUCCESS, mul_task->init(&counter));
  EXPECT_EQ(OB_SUCCESS, dag->add_task(*mul_task));
  EXPECT_EQ(OB_SUCCESS, scheduler->add_dag(dag));
  sleep(10);
  EXPECT_EQ(0, scheduler->get_dag_count(ObDagType::DAG_TYPE_MERGE_EXECUTE));
  CHECK_EQ_UTIL_TIMEOUT(0, scheduler->get_dag_count(ObDagType::DAG_TYPE_FAST_MIGRATE));
  EXPECT_EQ(0, scheduler->get_dag_count(ObDagType::DAG_TYPE_DDL));
  EXPECT_EQ(-1, scheduler->get_dag_count(ObDagType::DAG_TYPE_MAX));
  scheduler->stop();
  EXPECT_EQ(OB_SUCCESS, scheduler->alloc_dag(dag));
  EXPECT_EQ(OB_SUCCESS, dag->init(1));
  EXPECT_EQ(OB_SUCCESS, dag->alloc_task(mul_task));
  EXPECT_EQ(OB_SUCCESS, mul_task->init(&counter));
  EXPECT_EQ(OB_SUCCESS, dag->add_task(*mul_task));
  EXPECT_EQ(OB_SUCCESS, scheduler->add_dag(dag));
  EXPECT_EQ(0, scheduler->get_dag_count(ObDagType::DAG_TYPE_MERGE_EXECUTE));
  EXPECT_EQ(1, scheduler->get_dag_count(ObDagType::DAG_TYPE_FAST_MIGRATE));
  EXPECT_EQ(0, scheduler->get_dag_count(ObDagType::DAG_TYPE_DDL));
  EXPECT_EQ(-1, scheduler->get_dag_count(ObDagType::DAG_TYPE_MAX));
  EXPECT_EQ(OB_SUCCESS, scheduler->alloc_dag(dag2));
  EXPECT_EQ(OB_SUCCESS, dag2->init(2));
  EXPECT_EQ(OB_SUCCESS, dag2->alloc_task(mul_task2));
  EXPECT_EQ(OB_SUCCESS, mul_task2->init(&counter));
  EXPECT_EQ(OB_SUCCESS, dag2->add_task(*mul_task2));
  EXPECT_EQ(OB_SUCCESS, scheduler->add_dag(dag2));
  EXPECT_EQ(0, scheduler->get_dag_count(ObDagType::DAG_TYPE_MERGE_EXECUTE));
  EXPECT_EQ(2, scheduler->get_dag_count(ObDagType::DAG_TYPE_FAST_MIGRATE));
  EXPECT_EQ(0, scheduler->get_dag_count(ObDagType::DAG_TYPE_DDL));
  EXPECT_EQ(-1, scheduler->get_dag_count(ObDagType::DAG_TYPE_MAX));
}

TEST_F(TestDagScheduler, test_destroy_when_running)
{
  ObTenantDagScheduler *scheduler = MTL(ObTenantDagScheduler*);
  ASSERT_TRUE(nullptr != scheduler);
  ASSERT_EQ(OB_SUCCESS, scheduler->init(MTL_ID(), time_slice));

  void *buf = nullptr;
  ASSERT_TRUE(nullptr != (buf = allocator_.alloc(sizeof(AtomicOperator))));
  AtomicOperator *op = new(buf) AtomicOperator();

  TestLPDag *dag1 = NULL;
  AtomicIncTask *inc_task = NULL;
  int32_t thread_cnt = 64;
  int32_t lp_max = OB_DAG_PRIOS[ObDagPrio::DAG_PRIO_DDL].score_;
  scheduler->set_thread_score(ObDagPrio::DAG_PRIO_COMPACTION_MID, 100);
  // thread cnt is 121
  EXPECT_EQ(OB_SUCCESS, scheduler->alloc_dag(dag1));
  EXPECT_EQ(OB_SUCCESS, dag1->init(1));
  EXPECT_EQ(OB_SUCCESS, alloc_task(*dag1, inc_task));
  EXPECT_EQ(OB_SUCCESS, inc_task->init(1, thread_cnt * 3, *op, 2*sleep_slice));
  EXPECT_EQ(OB_SUCCESS, dag1->add_task(*inc_task));
  TestMPDag *dag2 = NULL;
  AtomicMulTask *mul_task = NULL;
  EXPECT_EQ(OB_SUCCESS, scheduler->alloc_dag(dag2));
  EXPECT_EQ(OB_SUCCESS, dag2->init(2));
  EXPECT_EQ(OB_SUCCESS, alloc_task(*dag2, mul_task));
  EXPECT_EQ(OB_SUCCESS, mul_task->init(1, thread_cnt * 10, *op, 3*sleep_slice));
  EXPECT_EQ(OB_SUCCESS, dag2->add_task(*mul_task));

  #ifndef BUILD_COVERAGE
  // not participate in coverage compilation to fix hang problem
  EXPECT_EQ(OB_SUCCESS, scheduler->add_dag(dag1));
  CHECK_EQ_UTIL_TIMEOUT(lp_max, scheduler->get_running_task_cnt(ObDagPrio::DAG_PRIO_DDL));
  EXPECT_EQ(OB_SUCCESS, scheduler->add_dag(dag2));
  // high priority preempt quotas from low priority, low priority run at min thread
  // CHECK_EQ_UTIL_TIMEOUT(hp_max, scheduler->get_running_task_cnt(ObDagPrio::DAG_PRIO_COMPACTION_MID));
  CHECK_EQ_UTIL_TIMEOUT(lp_max, scheduler->get_running_task_cnt(ObDagPrio::DAG_PRIO_DDL));
  #endif
}

TEST_F(TestDagScheduler, test_up_limit)
{
  ObTenantDagScheduler *scheduler = MTL(ObTenantDagScheduler*);
  ASSERT_TRUE(nullptr != scheduler);
  ASSERT_EQ(OB_SUCCESS, scheduler->init(MTL_ID(), time_slice));

  AtomicOperator op(0);

  TestLPDag *dag1 = NULL;
  AtomicIncTask *inc_task = NULL;
  const int32_t lp_max = scheduler->prio_sche_[ObDagPrio::DAG_PRIO_HA_LOW].limits_;
  const int32_t mp_max = scheduler->prio_sche_[ObDagPrio::DAG_PRIO_HA_MID].limits_;
  const int32_t hp_max = scheduler->prio_sche_[ObDagPrio::DAG_PRIO_HA_HIGH].limits_;
  const int64_t cnt = 64;
  scheduler->dump_dag_status();
  EXPECT_EQ(OB_SUCCESS, scheduler->alloc_dag(dag1));
  EXPECT_EQ(OB_SUCCESS, dag1->init(1));
  dag1->set_priority(ObDagPrio::DAG_PRIO_HA_LOW);
  EXPECT_EQ(OB_SUCCESS, alloc_task(*dag1, inc_task));
  EXPECT_EQ(OB_SUCCESS, inc_task->init(1, cnt, op, 2*sleep_slice));
  EXPECT_EQ(OB_SUCCESS, dag1->add_task(*inc_task));

  TestMPDag *dag2 = NULL;
  AtomicMulTask *mul_task = NULL;
  EXPECT_EQ(OB_SUCCESS, scheduler->alloc_dag(dag2));
  EXPECT_EQ(OB_SUCCESS, dag2->init(2));
  dag2->set_priority(ObDagPrio::DAG_PRIO_HA_HIGH);
  EXPECT_EQ(OB_SUCCESS, alloc_task(*dag2, mul_task));
  EXPECT_EQ(OB_SUCCESS, mul_task->init(1, cnt, op, 4*sleep_slice));
  EXPECT_EQ(OB_SUCCESS, dag2->add_task(*mul_task));

  TestMPDag *dag3 = NULL;
  EXPECT_EQ(OB_SUCCESS, scheduler->alloc_dag(dag3));
  EXPECT_EQ(OB_SUCCESS, dag3->init(3));
  dag3->set_priority(ObDagPrio::DAG_PRIO_HA_MID);
  EXPECT_EQ(OB_SUCCESS, alloc_task(*dag3, inc_task));
  EXPECT_EQ(OB_SUCCESS, inc_task->init(1, cnt, op, 3*sleep_slice));
  EXPECT_EQ(OB_SUCCESS, dag3->add_task(*inc_task));

  EXPECT_EQ(OB_SUCCESS, scheduler->add_dag(dag1));
  CHECK_EQ_UTIL_TIMEOUT(lp_max, scheduler->get_running_task_cnt(ObDagPrio::DAG_PRIO_HA_LOW));
  EXPECT_EQ(OB_SUCCESS, scheduler->add_dag(dag2));
  // high priority preempt quotas from low priority, low priority run at min thread
  CHECK_EQ_UTIL_TIMEOUT(hp_max, scheduler->get_running_task_cnt(ObDagPrio::DAG_PRIO_HA_HIGH));
  CHECK_EQ_UTIL_TIMEOUT(MIN(scheduler->work_thread_num_ - hp_max, lp_max), scheduler->get_running_task_cnt(ObDagPrio::DAG_PRIO_HA_LOW));

  EXPECT_EQ(OB_SUCCESS, scheduler->add_dag(dag3));
  CHECK_EQ_UTIL_TIMEOUT(hp_max, scheduler->get_running_task_cnt(ObDagPrio::DAG_PRIO_HA_HIGH));
  CHECK_EQ_UTIL_TIMEOUT(MIN(scheduler->work_thread_num_ - hp_max, mp_max), scheduler->get_running_task_cnt(ObDagPrio::DAG_PRIO_HA_MID));

  wait_scheduler();
}

TEST_F(TestDagScheduler, test_emergency_task)
{
  ObTenantDagScheduler *scheduler = MTL(ObTenantDagScheduler*);
  ASSERT_TRUE(nullptr != scheduler);
  ASSERT_EQ(OB_SUCCESS, scheduler->init(MTL_ID(), time_slice, 64));

  AtomicOperator op(0);

  EXPECT_EQ(OB_SUCCESS, scheduler->set_thread_score(ObDagPrio::DAG_PRIO_DDL, 1));
  EXPECT_EQ(1, scheduler->prio_sche_[ObDagPrio::DAG_PRIO_DDL].limits_);
  TestLPDag *dag1 = NULL;
  AtomicIncTask *inc_task = NULL;
  EXPECT_EQ(OB_SUCCESS, scheduler->alloc_dag(dag1));
  EXPECT_EQ(OB_SUCCESS, dag1->init(1));
  EXPECT_EQ(OB_SUCCESS, alloc_task(*dag1, inc_task));
  EXPECT_EQ(OB_SUCCESS, inc_task->init(1, 1, op, 20*sleep_slice));
  EXPECT_EQ(OB_SUCCESS, dag1->add_task(*inc_task));

  TestLPDag *dag2 = NULL;
  EXPECT_EQ(OB_SUCCESS, scheduler->alloc_dag(dag2));
  EXPECT_EQ(OB_SUCCESS, dag2->init(2));
  EXPECT_EQ(OB_SUCCESS, alloc_task(*dag2, inc_task));
  EXPECT_EQ(OB_SUCCESS, inc_task->init(1, 1, op, sleep_slice));
  EXPECT_EQ(OB_SUCCESS, dag2->add_task(*inc_task));

  EXPECT_EQ(OB_SUCCESS, scheduler->add_dag(dag1));
  EXPECT_EQ(OB_SUCCESS, scheduler->add_dag(dag2));

  while (scheduler->get_running_task_cnt(ObDagPrio::DAG_PRIO_DDL) == 0) {
    usleep(10);
  }

  TestLPDag *dag3 = NULL;
  AtomicMulTask *mul_task = NULL;
  EXPECT_EQ(OB_SUCCESS, scheduler->alloc_dag(dag3));
  EXPECT_EQ(OB_SUCCESS, dag3->init(3));
  EXPECT_EQ(OB_SUCCESS, alloc_task(*dag3, mul_task));
  EXPECT_EQ(OB_SUCCESS, mul_task->init(1, 1, op, 0));
  EXPECT_EQ(OB_SUCCESS, dag3->add_task(*mul_task));
  EXPECT_EQ(OB_SUCCESS, scheduler->add_dag(dag3, true));

  wait_scheduler();

  EXPECT_EQ(3, op.value());
}

class TestCompMidCancelDag : public compaction::ObTabletMergeDag
{
public:
  TestCompMidCancelDag()
    : compaction::ObTabletMergeDag(ObDagType::DAG_TYPE_MERGE_EXECUTE){}
  virtual const share::ObLSID & get_ls_id() const override { return ls_id_; }
  virtual lib::Worker::CompatMode get_compat_mode() const override
  { return lib::Worker::CompatMode::MYSQL; }
private:
  DISALLOW_COPY_AND_ASSIGN(TestCompMidCancelDag);
};

TEST_F(TestDagScheduler, test_check_ls_compaction_dag_exist_with_cancel)
{
  ObTenantDagScheduler *scheduler = MTL(ObTenantDagScheduler*);
  ASSERT_TRUE(nullptr != scheduler);
  ASSERT_EQ(OB_SUCCESS, scheduler->init(MTL_ID(), time_slice, 64));
  EXPECT_EQ(OB_SUCCESS, scheduler->set_thread_score(ObDagPrio::DAG_PRIO_COMPACTION_MID, 1));
  EXPECT_EQ(1, scheduler->prio_sche_[ObDagPrio::DAG_PRIO_COMPACTION_MID].limits_);

  LoopWaitTask *wait_task = nullptr;
  const int64_t dag_cnt = 6;
  // add 6 dag at prio = DAG_PRIO_COMPACTION_MID
  ObLSID ls_ids[2] = {ObLSID(1), ObLSID(2)};
  bool finish_flag[2] = {false, false};
  for (int64_t i = 0; i < dag_cnt; ++i) {
    const int64_t idx = i % 2;
    TestCompMidCancelDag *dag = NULL;
    EXPECT_EQ(OB_SUCCESS, scheduler->alloc_dag(dag));
    dag->ls_id_ = ls_ids[idx];
    dag->tablet_id_ = ObTabletID(i);
    EXPECT_EQ(OB_SUCCESS, alloc_task(*dag, wait_task));
    EXPECT_EQ(OB_SUCCESS, wait_task->init(1, 2, finish_flag[idx]));
    EXPECT_EQ(OB_SUCCESS, dag->add_task(*wait_task));
    EXPECT_EQ(OB_SUCCESS, scheduler->add_dag(dag));
  }
  EXPECT_EQ(dag_cnt, scheduler->dag_cnts_[ObDagType::DAG_TYPE_MERGE_EXECUTE]);
  CHECK_EQ_UTIL_TIMEOUT(1, scheduler->get_running_task_cnt(ObDagPrio::DAG_PRIO_COMPACTION_MID));

  // cancel two waiting dag of ls_ids[0]
  bool exist = false;
  EXPECT_EQ(OB_SUCCESS, scheduler->check_ls_compaction_dag_exist_with_cancel(ls_ids[0], exist));
  EXPECT_EQ(exist, true);
  EXPECT_EQ(4, scheduler->dag_cnts_[ObDagType::DAG_TYPE_MERGE_EXECUTE]);

  EXPECT_EQ(OB_SUCCESS, scheduler->check_ls_compaction_dag_exist_with_cancel(ls_ids[1], exist));
  EXPECT_EQ(exist, false);
  EXPECT_EQ(1, scheduler->dag_cnts_[ObDagType::DAG_TYPE_MERGE_EXECUTE]);

  finish_flag[0] = true;
  wait_scheduler();

  EXPECT_EQ(OB_SUCCESS, scheduler->check_ls_compaction_dag_exist_with_cancel(ls_ids[0], exist));
  EXPECT_EQ(exist, false);
}

TEST_F(TestDagScheduler, test_cancel_running_dag)
{
  ObTenantDagScheduler *scheduler = MTL(ObTenantDagScheduler*);
  ASSERT_TRUE(nullptr != scheduler);
  ASSERT_EQ(OB_SUCCESS, scheduler->init(MTL_ID(), time_slice));

  int64_t cnt = 10;
  int64_t cancel_seq = 0;
  bool finish_flag = false;
  AtomicOperator op(0);
  TestCancelDag *cancel_dag = nullptr;
  TestCancelDag *cancel_dag_key = nullptr;

  // The dag canceled in prepare task so the finish task can not finish
  // 1. Cancel in the first round of prepare task
  EXPECT_EQ(OB_SUCCESS, scheduler->alloc_dag(cancel_dag));
  EXPECT_EQ(OB_SUCCESS, cancel_dag->init(1));
  EXPECT_EQ(OB_SUCCESS, cancel_dag->init_dag(cnt, cancel_seq, finish_flag, &op));
  EXPECT_EQ(OB_SUCCESS, scheduler->add_dag(cancel_dag));
  wait_scheduler();
  EXPECT_EQ(0, op.value());

  // 2. Cancel in the middle round of prepare task
  cancel_seq = 5;
  op.reset();
  EXPECT_EQ(OB_SUCCESS, scheduler->alloc_dag(cancel_dag));
  EXPECT_EQ(OB_SUCCESS, cancel_dag->init(1));
  EXPECT_EQ(OB_SUCCESS, cancel_dag->init_dag(cnt, cancel_seq, finish_flag, &op));
  EXPECT_EQ(OB_SUCCESS, scheduler->add_dag(cancel_dag));
  wait_scheduler();
  EXPECT_EQ(0, op.value());

  // 3. Cancel in the last round of prepare task
  cancel_seq = 9;
  op.reset();
  EXPECT_EQ(OB_SUCCESS, scheduler->alloc_dag(cancel_dag));
  EXPECT_EQ(OB_SUCCESS, cancel_dag->init(1));
  EXPECT_EQ(OB_SUCCESS, cancel_dag->init_dag(cnt, cancel_seq, finish_flag, &op));
  EXPECT_EQ(OB_SUCCESS, scheduler->add_dag(cancel_dag));
  wait_scheduler();
  EXPECT_EQ(0, op.value());

  // 4. Cancel even all other prepare task finsihed
  finish_flag = true;
  cancel_seq = 9;
  op.reset();
  EXPECT_EQ(OB_SUCCESS, scheduler->alloc_dag(cancel_dag));
  EXPECT_EQ(OB_SUCCESS, cancel_dag->init(1));
  EXPECT_EQ(OB_SUCCESS, cancel_dag->init_dag(cnt, cancel_seq, finish_flag, &op));
  EXPECT_EQ(OB_SUCCESS, scheduler->add_dag(cancel_dag));
  wait_scheduler();
  EXPECT_EQ(0, op.value());

  // 5. Can not cancel if prepare task does not call yield
  finish_flag = true;
  cancel_seq = 11;
  op.reset();
  EXPECT_EQ(OB_SUCCESS, scheduler->alloc_dag(cancel_dag));
  EXPECT_EQ(OB_SUCCESS, cancel_dag->init(1));
  EXPECT_EQ(OB_SUCCESS, cancel_dag->init_dag(cnt, cancel_seq, finish_flag, &op));
  EXPECT_EQ(OB_SUCCESS, scheduler->add_dag(cancel_dag));
  wait_scheduler();
  EXPECT_EQ(8, op.value());

  // 6. Test cancel ready dag
  finish_flag = false;
  cancel_seq = 3;
  op.reset();
  EXPECT_EQ(OB_SUCCESS, scheduler->alloc_dag(cancel_dag));
  EXPECT_EQ(OB_SUCCESS, cancel_dag->init(1));
  EXPECT_EQ(OB_SUCCESS, cancel_dag->init_dag(cnt, cancel_seq, finish_flag, &op));
  EXPECT_EQ(OB_SUCCESS, scheduler->add_dag(cancel_dag));
  EXPECT_EQ(OB_SUCCESS, scheduler->cancel_dag(cancel_dag, true));
  wait_scheduler();
  EXPECT_EQ(0, op.value());

  // 7. Test cancel dag after dag finish(running ok)
  finish_flag = true;
  cancel_seq = 11;
  op.reset();
  EXPECT_EQ(OB_SUCCESS, scheduler->alloc_dag(cancel_dag));
  EXPECT_EQ(OB_SUCCESS, cancel_dag->init(1));
  EXPECT_EQ(OB_SUCCESS, cancel_dag->init_dag(cnt, cancel_seq, finish_flag, &op));
  EXPECT_EQ(OB_SUCCESS, scheduler->add_dag(cancel_dag));
  wait_scheduler();
  EXPECT_EQ(8, op.value());
  // Example for canceling a maybe finished dag
  EXPECT_EQ(OB_SUCCESS, scheduler->alloc_dag(cancel_dag_key));
  EXPECT_EQ(OB_SUCCESS, cancel_dag_key->init(1)); // the same hash key with cancel_dag
  EXPECT_EQ(OB_SUCCESS, scheduler->cancel_dag(cancel_dag_key, true));
  scheduler->free_dag(*cancel_dag_key);


  // 8. Test cancel dag after dag finish(canceled)
  finish_flag = true;
  cancel_seq = 9;
  op.reset();
  EXPECT_EQ(OB_SUCCESS, scheduler->alloc_dag(cancel_dag));
  EXPECT_EQ(OB_SUCCESS, cancel_dag->init(1));
  EXPECT_EQ(OB_SUCCESS, cancel_dag->init_dag(cnt, cancel_seq, finish_flag, &op));
  EXPECT_EQ(OB_SUCCESS, scheduler->add_dag(cancel_dag));
  wait_scheduler();
  EXPECT_EQ(0, op.value());
  EXPECT_EQ(OB_SUCCESS, scheduler->alloc_dag(cancel_dag_key));
  EXPECT_EQ(OB_SUCCESS, cancel_dag_key->init(1));
  EXPECT_EQ(OB_SUCCESS, scheduler->cancel_dag(cancel_dag_key, true));
  scheduler->free_dag(*cancel_dag_key);
}

/*
TEST_F(TestDagScheduler, test_large_thread_cnt)
{
  ObTenantDagScheduler *scheduler = MTL(ObTenantDagScheduler*);
  ASSERT_TRUE(nullptr != scheduler);

  const int64_t cpu_cnt = 128;
  ASSERT_EQ(OB_SUCCESS, scheduler->init(MTL_ID(), time_slice));
  scheduler->cpu_cnt_ = cpu_cnt;
  scheduler->get_default_config(cpu_cnt);
  scheduler->dump_dag_status();
  const int64_t max_concurrency = 100;
  scheduler->set_major_merge_concurrency(max_concurrency);
  EXPECT_EQ(max_concurrency, scheduler->up_limits_[ObIDag::DAG_ULT_MAJOR_MERGE]);
  EXPECT_EQ(max_concurrency / 2, scheduler->low_limits_[ObDagPrio::DAG_PRIO_COMPACTION_LOW]);
  const int64_t max_concurrency_2 = 10;
  scheduler->set_major_merge_concurrency(max_concurrency_2);
  EXPECT_EQ(max_concurrency_2, scheduler->up_limits_[ObIDag::DAG_ULT_MAJOR_MERGE]);
  EXPECT_EQ(max_concurrency_2 / 2, scheduler->low_limits_[ObDagPrio::DAG_PRIO_COMPACTION_LOW]);
  scheduler->destroy();

  const int64_t cpu_cnt_2 = 64;
  ASSERT_EQ(OB_SUCCESS, scheduler->init(MTL_ID(), time_slice));
  scheduler->cpu_cnt_ = cpu_cnt_2;
  scheduler->get_default_config(cpu_cnt_2);
  scheduler->dump_dag_status();
  scheduler->set_major_merge_concurrency(max_concurrency);
  EXPECT_EQ(cpu_cnt_2, scheduler->up_limits_[ObIDag::DAG_ULT_MAJOR_MERGE]);
  EXPECT_EQ(max_concurrency / 2, scheduler->low_limits_[ObDagPrio::DAG_PRIO_COMPACTION_LOW]);
  scheduler->set_major_merge_concurrency(max_concurrency_2);
  EXPECT_EQ(max_concurrency_2, scheduler->up_limits_[ObIDag::DAG_ULT_MAJOR_MERGE]);
  EXPECT_EQ(max_concurrency_2 / 2, scheduler->low_limits_[ObDagPrio::DAG_PRIO_COMPACTION_LOW]);
  scheduler->destroy();

  const int64_t cpu_cnt_3 = 512;
  ASSERT_EQ(OB_SUCCESS, scheduler->init(MTL_ID(), time_slice));
  scheduler->cpu_cnt_ = cpu_cnt_3;
  scheduler->get_default_config(cpu_cnt_3);
  scheduler->dump_dag_status();
  scheduler->set_major_merge_concurrency(max_concurrency);
  EXPECT_EQ(max_concurrency, scheduler->up_limits_[ObIDag::DAG_ULT_MAJOR_MERGE]);
  EXPECT_EQ(max_concurrency / 2, scheduler->low_limits_[ObDagPrio::DAG_PRIO_COMPACTION_LOW]);
  scheduler->set_major_merge_concurrency(max_concurrency_2);
  EXPECT_EQ(max_concurrency_2, scheduler->up_limits_[ObIDag::DAG_ULT_MAJOR_MERGE]);
  EXPECT_EQ(max_concurrency_2 / 2, scheduler->low_limits_[ObDagPrio::DAG_PRIO_COMPACTION_LOW]);

  const int64_t max_concurrency_3 = 256;
  scheduler->set_major_merge_concurrency(max_concurrency_3);
  EXPECT_EQ(max_concurrency_3, scheduler->up_limits_[ObIDag::DAG_ULT_MAJOR_MERGE]);
  EXPECT_EQ(max_concurrency_3 / 2, scheduler->low_limits_[ObDagPrio::DAG_PRIO_COMPACTION_LOW]);

  scheduler->set_major_merge_concurrency(0);
  EXPECT_EQ(ObTenantDagScheduler::DEFAULT_UP_LIMIT[ObIDag::DAG_ULT_MAJOR_MERGE]
      , scheduler->up_limits_[ObIDag::DAG_ULT_MAJOR_MERGE]);
  EXPECT_EQ(ObTenantDagScheduler::DEFAULT_LOW_LIMIT[ObDagPrio::DAG_PRIO_COMPACTION_LOW],
      scheduler->low_limits_[ObDagPrio::DAG_PRIO_COMPACTION_LOW]);
  scheduler->destroy();
}

TEST_F(TestDagScheduler, test_large_thread_cnt_2)
{
  ObTenantDagScheduler *scheduler = MTL(ObTenantDagScheduler*);
  ASSERT_TRUE(nullptr != scheduler);

  const int64_t cpu_cnt = 256;
  ASSERT_EQ(OB_SUCCESS, scheduler->init(MTL_ID(), time_slice));
  scheduler->cpu_cnt_ = cpu_cnt;
  scheduler->get_default_config(cpu_cnt);
  scheduler->dump_dag_status();
  int64_t max_concurrency = 500;
  scheduler->set_major_merge_concurrency(max_concurrency);
  EXPECT_EQ(cpu_cnt, scheduler->up_limits_[ObIDag::DAG_ULT_MAJOR_MERGE]);
  EXPECT_EQ(max_concurrency / 2, scheduler->low_limits_[ObDagPrio::DAG_PRIO_COMPACTION_LOW]);
  int64_t max_concurrency_2 = 10;
  scheduler->set_major_merge_concurrency(max_concurrency_2);
  EXPECT_EQ(max_concurrency_2, scheduler->up_limits_[ObIDag::DAG_ULT_MAJOR_MERGE]);
  EXPECT_EQ(max_concurrency_2 / 2, scheduler->low_limits_[ObDagPrio::DAG_PRIO_COMPACTION_LOW]);
  scheduler->destroy();

  const int64_t cpu_cnt_2 = 64;
  ASSERT_EQ(OB_SUCCESS, scheduler->init(MTL_ID(), time_slice));
  scheduler->cpu_cnt_ = cpu_cnt_2;
  scheduler->get_default_config(cpu_cnt_2);
  scheduler->dump_dag_status();
  max_concurrency = 200;
  scheduler->set_major_merge_concurrency(max_concurrency);
  EXPECT_EQ(cpu_cnt_2, scheduler->up_limits_[ObIDag::DAG_ULT_MAJOR_MERGE]);
  EXPECT_EQ(cpu_cnt_2, scheduler->low_limits_[ObDagPrio::DAG_PRIO_COMPACTION_LOW]);
  scheduler->set_major_merge_concurrency(max_concurrency_2);
  EXPECT_EQ(max_concurrency_2, scheduler->up_limits_[ObIDag::DAG_ULT_MAJOR_MERGE]);
  EXPECT_EQ(max_concurrency_2 / 2, scheduler->low_limits_[ObDagPrio::DAG_PRIO_COMPACTION_LOW]);
  scheduler->destroy();

  const int64_t cpu_cnt_3 = 512;
  ASSERT_EQ(OB_SUCCESS, scheduler->init(MTL_ID(), time_slice));
  scheduler->cpu_cnt_ = cpu_cnt_3;
  scheduler->get_default_config(cpu_cnt_3);
  scheduler->dump_dag_status();
  scheduler->set_major_merge_concurrency(max_concurrency);
  EXPECT_EQ(max_concurrency, scheduler->up_limits_[ObIDag::DAG_ULT_MAJOR_MERGE]);
  EXPECT_EQ(max_concurrency / 2, scheduler->low_limits_[ObDagPrio::DAG_PRIO_COMPACTION_LOW]);
  scheduler->set_major_merge_concurrency(max_concurrency_2);
  EXPECT_EQ(max_concurrency_2, scheduler->up_limits_[ObIDag::DAG_ULT_MAJOR_MERGE]);
  EXPECT_EQ(max_concurrency_2 / 2, scheduler->low_limits_[ObDagPrio::DAG_PRIO_COMPACTION_LOW]);

  const int64_t max_concurrency_3 = 800;
  scheduler->set_major_merge_concurrency(max_concurrency_3);
  EXPECT_EQ(cpu_cnt_3, scheduler->up_limits_[ObIDag::DAG_ULT_MAJOR_MERGE]);
  EXPECT_EQ(max_concurrency_3 / 2, scheduler->low_limits_[ObDagPrio::DAG_PRIO_COMPACTION_LOW]);

  scheduler->set_major_merge_concurrency(0);
  EXPECT_EQ(ObTenantDagScheduler::DEFAULT_UP_LIMIT[ObIDag::DAG_ULT_MAJOR_MERGE]
      , scheduler->up_limits_[ObIDag::DAG_ULT_MAJOR_MERGE]);
  EXPECT_EQ(ObTenantDagScheduler::DEFAULT_LOW_LIMIT[ObDagPrio::DAG_PRIO_COMPACTION_LOW],
      scheduler->low_limits_[ObDagPrio::DAG_PRIO_COMPACTION_LOW]);
  scheduler->destroy();
}
*/
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
  system("rm -f test_dag_scheduler.log*");
  OB_LOGGER.set_file_name("test_dag_scheduler.log");
  return RUN_ALL_TESTS();
}
