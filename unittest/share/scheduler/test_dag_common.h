/**
 * Copyright (c) 2025 OceanBase
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
#include <gtest/gtest.h>
#define protected public
#define private public
#include "storage/compaction/ob_tenant_tablet_scheduler.h"
#include "share/scheduler/ob_dag_warning_history_mgr.h"
namespace oceanbase
{
namespace unittest{

static const int64_t SLEEP_SLICE = 100;

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
  INHERIT_TO_STRING_KV("ObITask", ObITask, "type", "AtomicMul", K(*dag_), K_(seq), K_(cnt), KP_(op), K_(error_seq), K_(sleep_us));
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
  INHERIT_TO_STRING_KV("ObITask", ObITask, "type", "AtomicInc", K(*dag_), K_(seq), K_(cnt), KP_(op), K_(error_seq), K_(sleep_us));
private:
  int64_t seq_;
  int64_t cnt_;
  int64_t error_seq_;
  AtomicOperator *op_;
  int sleep_us_;
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
  ObIAllocator &basic_allocator = scheduler->get_allocator(false /*is_ha*/);
  ObIAllocator &basic_root_allocator = static_cast<ObParallelAllocator *>(&basic_allocator)->root_allocator_;
  while ((basic_allocator.used() - basic_root_allocator.used()) != 0) {
    ::usleep(100000);
  }
  while ((basic_allocator.total() - basic_root_allocator.total()) != 0) {
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
  virtual uint64_t hash() const override { return murmurhash(&id_, sizeof(id_), 0);}
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
  INHERIT_TO_STRING_KV("ObIDag", ObIDag, K_(is_inited), K_(type), K_(id), K(task_list_.get_size()));
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
  INHERIT_TO_STRING_KV("ObITask", ObITask, "type", "AddTask", KP_(counter), K_(seq), K_(task_cnt));
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
  INHERIT_TO_STRING_KV("ObITask", ObITask, "type", "MulTask", KP_(counter));
private:
  int64_t *counter_;
  int sleep_us_;
private:
  DISALLOW_COPY_AND_ASSIGN(TestMulTask);
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

} // end namespace unittest
} // end namespace oceanbase
