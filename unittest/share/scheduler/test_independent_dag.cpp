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

#define USING_LOG_PREFIX TEST
#include <getopt.h>
#include <gtest/gtest.h>
#define protected public
#define private public
#include "share/scheduler/test_dag_common.h"
#include "share/scheduler/ob_independent_dag.h"
#include "lib/random/ob_random.h"

const int64_t STRESS_ROUND_CNT = 32;
const int64_t STRESS_THREAD_CNT = 10;

namespace oceanbase
{
namespace unittest{
/*========================= TestIndependentDag ===================================*/
class TestIndependentDag : public ::testing::Test
{
public:
  class ObPriorityTask : public ObITask
  {
  public:
    ObPriorityTask() : ObITask(ObITaskType::TASK_TYPE_UT), priority_(ObITask::TASK_PRIO_1) {}
    virtual int process() { return OB_SUCCESS; }
    virtual ObITaskPriority get_priority() override { return priority_; }
    virtual void task_debug_info_to_string(char *buf, const int64_t buf_len, int64_t &pos) const override {
      BUF_PRINTF("ObPriorityTask : prio: %d, 40 more chars: ", priority_);
      for (int i = 0; i < 40; i++) {
        BUF_PRINTF("%c", 'A' + i);
      }
    }
    TO_STRING_KV(K_(priority));
  private:
  public:
    ObITaskPriority priority_;
  };
public:
  TestIndependentDag()
    : tenant_id_(1001),
      tenant_base_(1001),
      addr_(1683068975, 9999),
      allocator_("IndependentDag"),
      scheduler_(nullptr),
      is_inited_(false) {}
  virtual ~TestIndependentDag() {}
  void SetUp() override
  {
    if (!is_inited_) {
      ObMallocAllocator::get_instance()->create_and_add_tenant_allocator(1001);
      is_inited_ = true;
    }
    scheduler_ = OB_NEW(ObTenantDagScheduler, ObModIds::TEST);
    tenant_base_.set(scheduler_);

    ObTenantEnv::set_tenant(&tenant_base_);
    ASSERT_EQ(OB_SUCCESS, tenant_base_.init());

    ObMallocAllocator *ma = ObMallocAllocator::get_instance();
    ASSERT_EQ(OB_SUCCESS, ma->set_tenant_limit(tenant_id_, 1LL << 30));

    ASSERT_EQ(OB_SUCCESS, ObSysTaskStatMgr::get_instance().set_self_addr(addr_));
    ASSERT_EQ(OB_SUCCESS, scheduler_->init(MTL_ID(), 1000 /*check_period*/));
    COMMON_LOG(INFO, "SetUp TestIndependentDag success");
  }
  void TearDown() override
  {
    scheduler_->destroy();
    scheduler_ = nullptr;
    tenant_base_.destroy();
    ObTenantEnv::set_tenant(nullptr);
  }
public:
  void check_task_list_priority(ObIDag::TaskList &task_list);
  void alloc_and_add_priority_task(ObIDag *dag, const ObITask::ObITaskPriority priority);
private:
  const uint64_t tenant_id_;
  ObTenantBase tenant_base_;
  ObAddr addr_;
  ObArenaAllocator allocator_;
  ObTenantDagScheduler *scheduler_;
  bool is_inited_;
  DISALLOW_COPY_AND_ASSIGN(TestIndependentDag);
};

void TestIndependentDag::check_task_list_priority(ObIDag::TaskList &task_list)
{
  const ObITask *head = task_list.get_header();
  ObITask *cur_task = task_list.get_first();
  ObITask *pre_task = nullptr;
  while (head != cur_task) {
    if (OB_NOT_NULL(pre_task)) {
      ASSERT_GE(pre_task->get_priority(), cur_task->get_priority());
    }
    pre_task = cur_task;
    cur_task = cur_task->get_next();
  }
}

void TestIndependentDag::alloc_and_add_priority_task(
    ObIDag *dag,
    const ObITask::ObITaskPriority priority)
{
  ASSERT_NE(nullptr, dag);
  ASSERT_LE(priority, ObITask::ObITaskPriority::TASK_PRIO_MAX);
  ObPriorityTask* task = nullptr;
  ASSERT_EQ(OB_SUCCESS, dag->alloc_task(task));
  ASSERT_NE(nullptr, task);
  task->priority_ = priority;
  ASSERT_EQ(OB_SUCCESS, dag->add_task(*task));
}

/*========================= ObIndependentExampleDag ===================================*/
class ObTestIndependentBaseDag : public ObIndependentDag
{
public:
  ObTestIndependentBaseDag() : ObIndependentDag(ObDagType::DAG_TYPE_DDL) {}
  ~ObTestIndependentBaseDag() {}
public:
  virtual uint64_t hash() const override { return 0; }
  virtual bool operator == (const ObIDag &other) const { return get_type() == other.get_type(); }
  virtual lib::Worker::CompatMode get_compat_mode() const override { return lib::Worker::CompatMode::MYSQL; }
private:
  DISALLOW_COPY_AND_ASSIGN(ObTestIndependentBaseDag);
};

struct ObIndependentInitParam : public ObIDagInitParam
{
public:
  ObIndependentInitParam(int64_t *result1, int64_t *result2, int64_t x, int64_t y, bool random_abort = false, bool generate_before_process = true)
    : result1_(result1),
      result2_(result2),
      x_(x),
      y_(y),
      random_abort_(random_abort),
      generate_before_process_(generate_before_process)
  {}
  ~ObIndependentInitParam() {}
  bool is_valid() const override
  {
    return OB_NOT_NULL(result1_)
        && OB_NOT_NULL(result2_)
        && *result1_ == *result2_
        && x_ >= 0
        && y_ >= 0;
  }
  TO_STRING_KV(KP_(result1), KP_(result2), K_(x), K_(y), K_(random_abort), K_(generate_before_process));
public:
  int64_t *result1_;
  int64_t *result2_;
  int64_t x_;
  int64_t y_;
  bool random_abort_;
  bool generate_before_process_;
};

class ObIndependentExampleDag : public ObTestIndependentBaseDag
{
public:
  ObIndependentExampleDag()
    : ObTestIndependentBaseDag(),
      result1_(nullptr),
      result2_(nullptr),
      x_(-1),
      y_(-1),
      random_abort_(false),
      generate_before_process_(true),
      is_abort_(false)
   {}
  ~ObIndependentExampleDag() {}
  virtual int init_by_param(const ObIDagInitParam *param) override;
  virtual int create_first_task() override;
  int inner_init(const ObIndependentInitParam *param);
  int handle_random_abort();
  bool need_random_abort() const { return random_abort_; }
  bool is_abort() const { return ATOMIC_LOAD(&is_abort_); }
  void set_abort() { ATOMIC_SET(&is_abort_, true); }
private:
  int64_t *result1_;
  int64_t *result2_;
  int64_t x_;
  int64_t y_;
  bool random_abort_;
  bool generate_before_process_;
  bool is_abort_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObIndependentExampleDag);
};

class ObIndependentPrepareTask : public ObITask
{
public:
  ObIndependentPrepareTask() : ObITask(ObITaskType::TASK_TYPE_UT), is_inited_(false) {}
  ~ObIndependentPrepareTask() {}
  int init(int64_t *result1, int64_t *result2, int64_t x, int64_t y);
  virtual int process() override;
private:
  DISALLOW_COPY_AND_ASSIGN(ObIndependentPrepareTask);
private:
  int64_t *result1_;
  int64_t *result2_;
  int64_t x_;
  int64_t y_;
  bool is_inited_;
};

class ObIndependentAddTask : public TestAddTask
{
public:
  ObIndependentAddTask() : TestAddTask() {}
  ~ObIndependentAddTask() {}
  virtual int process() override;
  virtual int generate_next_task(ObITask *&next_task) override;
  virtual int post_generate_next_task(ObITask *&next_task) override;
private:
  DISALLOW_COPY_AND_ASSIGN(ObIndependentAddTask);
};

class ObIndependentMulTask : public TestMulTask
{
public:
  ObIndependentMulTask() : TestMulTask() {}
  ~ObIndependentMulTask() {}
  virtual int process() override;
private:
  DISALLOW_COPY_AND_ASSIGN(ObIndependentMulTask);
};

class ObIndependentCheckTask : public ObITask
{
public:
  ObIndependentCheckTask() : ObITask(ObITaskType::TASK_TYPE_UT), is_inited_(false) {}
  ~ObIndependentCheckTask() {}
  int init(const int expected_result, int64_t *result1, int64_t *result2);
  virtual int process() override;
private:
  int64_t expected_result_;
  int64_t *result1_;
  int64_t *result2_;
  bool is_inited_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObIndependentCheckTask);
};

class ObIndependentPowerDag : public ObIndependentExampleDag
{
public:
  ObIndependentPowerDag() : ObIndependentExampleDag() {}
  ~ObIndependentPowerDag() {}
  virtual int create_first_task() override;
private:
  DISALLOW_COPY_AND_ASSIGN(ObIndependentPowerDag);
};

class ObIndependentPowerPrepareTask : public ObIndependentPrepareTask
{
public:
  ObIndependentPowerPrepareTask() : ObIndependentPrepareTask() {}
  ~ObIndependentPowerPrepareTask() {}
  virtual int process() override;
private:
  DISALLOW_COPY_AND_ASSIGN(ObIndependentPowerPrepareTask);
};

class ObIndependentPowerTask : public ObITask
{
public:
  ObIndependentPowerTask()
    : ObITask(ObITaskType::TASK_TYPE_UT),
      is_inited_(false),
      counter_(nullptr),
      layer_(-1)
    {}
  ~ObIndependentPowerTask() {}
  int init(int64_t *counter, int64_t layer);
  virtual int process() override;
private:
  int generate_next_layer_task();
private:
  bool is_inited_;
  int64_t *counter_;
  int64_t layer_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObIndependentPowerTask);
};

struct ObIndependentSuspendInitParam : public ObIDagInitParam
{
public:
  ObIndependentSuspendInitParam(int64_t *result, int64_t round, int64_t step)
   : result_(result),
     round_(round),
     step_(step)
  {}
  ~ObIndependentSuspendInitParam() {}
  bool is_valid() const override { return OB_NOT_NULL(result_) && round_ > 0 && step_ > 0; }
  TO_STRING_KV(KP_(result), K_(round), K_(step));
public:
  int64_t *result_;
  int64_t round_;
  int64_t step_;
};

// NOTE: can only run in single thread pool
class ObIndependentSuspendDag : public ObTestIndependentBaseDag
{
public:
  ObIndependentSuspendDag()
    : ObTestIndependentBaseDag(),
      result_(nullptr),
      round_(0),
      step_(0)
    {}
  ~ObIndependentSuspendDag() {}
  virtual int init_by_param(const ObIDagInitParam *param) override;
  virtual int create_first_task() override;
  INHERIT_TO_STRING_KV("ObTestIndependentBaseDag", ObTestIndependentBaseDag, KP_(result), K_(round), K_(step));
protected:
  int64_t *result_;
  int64_t round_;
  int64_t step_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObIndependentSuspendDag);
};

class ObIndependentSuspendTask : public ObITask
{
public:
  ObIndependentSuspendTask()
    : ObITask(ObITaskType::TASK_TYPE_UT), counter_(nullptr), round_(0), step_(0), cur_round_(0)
  {}
  ~ObIndependentSuspendTask() {}
  int init(int64_t *counter, int64_t round, int64_t step);
  virtual int process() override;
  INHERIT_TO_STRING_KV("ObITask", ObITask, KP_(counter), K_(round), K_(step), K_(cur_round));
private:
  int64_t *counter_;
  int64_t round_;
  int64_t step_;
  int64_t cur_round_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObIndependentSuspendTask);
};

class ObIndependentSuspendDagV2 : public ObIndependentSuspendDag
{
public:
  ObIndependentSuspendDagV2() : ObIndependentSuspendDag() {}
  ~ObIndependentSuspendDagV2() {}
  virtual int create_first_task() override;
};

class ObIndependentSuspendTaskV2 : public ObIndependentSuspendTask
{
public:
  ObIndependentSuspendTaskV2() : ObIndependentSuspendTask() {}
  ~ObIndependentSuspendTaskV2() {}
  int init(int64_t *counter, int64_t round, int64_t step, int64_t cur_round);
  virtual int generate_next_task(ObITask *&next_task) override;
  virtual int process() override;
private:
  DISALLOW_COPY_AND_ASSIGN(ObIndependentSuspendTaskV2);
};

/* ========================= ObIndependentPostGenerateDag ========================= */
struct ObIndependentPostGenerateDagInitParam : public ObIDagInitParam
{
  ObIndependentPostGenerateDagInitParam() = default;
  virtual bool is_valid() const override { return true; }
};

class ObIndependentPostGenerateDag : public ObTestIndependentBaseDag
{
public:
  ObIndependentPostGenerateDag(): ObTestIndependentBaseDag(), counter_(0) {}
  ~ObIndependentPostGenerateDag() {}
  virtual int init_by_param(const ObIDagInitParam *param) override { return OB_SUCCESS; }
  virtual int create_first_task() override;
  int64_t* get_counter_ptr() { return &counter_; }
  int64_t get_counter() const { return counter_; }
  TO_STRING_KV(K_(counter));
public:
  static constexpr int64_t LOOP_TASK_NUM = 1000;
  static constexpr int64_t LOOP_TASK_POST_GENERATE_CNT = 100;
private:
  int64_t counter_;
  DISALLOW_COPY_AND_ASSIGN(ObIndependentPostGenerateDag);
};

class ObHighConcurrencyPrepareTask : public ObITask
{
public:
  ObHighConcurrencyPrepareTask()
    : ObITask(ObITaskType::TASK_TYPE_UT),
      task_num_(0),
      task_generate_cnt_(0),
      is_inited_(false)
  {}
  ~ObHighConcurrencyPrepareTask() {}
  int init(const int64_t task_num, const int64_t task_generate_cnt);
  virtual int process() override;
private:
  int64_t task_num_;
  int64_t task_generate_cnt_;
  bool is_inited_;
  DISALLOW_COPY_AND_ASSIGN(ObHighConcurrencyPrepareTask);
};

class ObHighConcurrencyFastLoopTask : public ObITask
{
public:
  ObHighConcurrencyFastLoopTask()
    : ObITask(ObITaskType::TASK_TYPE_UT),
      counter_(nullptr),
      idx_(-1),
      seq_(0),
      task_cnt_(0),
      is_inited_(false)
  {}
  ~ObHighConcurrencyFastLoopTask() {}
  int init(int64_t *counter, const int64_t idx, const int64_t seq, const int64_t task_cnt);
  virtual int process() override;
  virtual int post_generate_next_task(ObITask *&next_task) override;
  TO_STRING_KV(K_(idx), K_(seq), K_(task_cnt), K_(is_inited));
public:
  static constexpr int64_t LOOP_TIMEOUT = 1 * 1000; // 10 ms
  static constexpr int64_t SLEEP_TIME = 1 * 1000; // 1ms
private:
  int64_t *counter_;
  int64_t idx_;
  int64_t seq_;
  int64_t task_cnt_;
  bool is_inited_;
  DISALLOW_COPY_AND_ASSIGN(ObHighConcurrencyFastLoopTask);
};

class ObHighConcurrencyFinsihTask : public ObITask
{
public:
  ObHighConcurrencyFinsihTask() : ObITask(ObITaskType::TASK_TYPE_UT), expected_result_(0) {}
  ~ObHighConcurrencyFinsihTask() {}
  int init(const int64_t expected_result) {
    expected_result_ = expected_result;
    return OB_SUCCESS;
  }
  virtual int process() override;
private:
  int64_t expected_result_;
  DISALLOW_COPY_AND_ASSIGN(ObHighConcurrencyFinsihTask);
};

/* ========================= ObIndependentExampleDag =================================== */
int ObIndependentExampleDag::init_by_param(const ObIDagInitParam *param)
{
  int ret = OB_SUCCESS;
  const ObIndependentInitParam *independent_param = nullptr;
  if (OB_ISNULL(param) || OB_ISNULL(independent_param = static_cast<const ObIndependentInitParam *>(param))) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid arg to init ObIndependentExampleDag", K(ret), KP(param), KPC(independent_param));
  } else if (OB_FAIL(inner_init(independent_param))) {
    COMMON_LOG(WARN, "failed to inner init", K(ret), KPC(independent_param));
  }
  return ret;
}

int ObIndependentExampleDag::inner_init(const ObIndependentInitParam *param)
{
  int ret = OB_SUCCESS;
  // is_inited_ is set int ObIDag::basic_init
  if (OB_UNLIKELY(!param->is_valid() )) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid arg", K(ret), KPC(param));
  } else {
    result1_ = param->result1_;
    result2_ = param->result2_;
    x_ = param->x_;
    y_ = param->y_;
    random_abort_ = param->random_abort_;
    generate_before_process_ = param->generate_before_process_;
  }
  return ret;
}

int ObIndependentExampleDag::create_first_task()
{
  int ret = OB_SUCCESS;
  ObIndependentPrepareTask *prepare_task = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "ObIndependentExampleDag is not inited", K(ret));
  } else if (OB_FAIL(create_task(nullptr /*parent*/, prepare_task, result1_, result2_, x_, y_))) {
    COMMON_LOG(WARN, "failed to create task", K(ret));
  } else {
    COMMON_LOG(INFO, "ObIndependentExampleDag create first task success", KPC(this));
  }
  return ret;
}

int ObIndependentExampleDag::handle_random_abort()
{
  int ret = OB_SUCCESS;
  if (random_abort_) {
    if (is_abort()) {
      ret = OB_ERR_UNEXPECTED;
      COMMON_LOG(WARN, "dag is abort", K(ret));
    } else {
      const int64_t random_number = ObRandom::rand(0, 6);
      if (random_number < 3) {
        ret = OB_ERR_UNEXPECTED;
        set_abort();
        COMMON_LOG(WARN, "random abort", K(ret), K(random_number));
      }
    }
  }
  return ret;
}

/* ========================= ObIndependentPrepareTask =================================== */
int ObIndependentPrepareTask::init(
    int64_t *result1,
    int64_t *result2,
    int64_t x,
    int64_t y)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    COMMON_LOG(WARN, "task is inited", K(ret));
  } else {
    result1_ = result1;
    result2_ = result2;
    x_ = x;
    y_ = y;
    is_inited_ = true;
  }
  return ret;
}

int ObIndependentPrepareTask::process()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "ObIndependentPrepareTask is not inited", K(ret));
  } else {
    ObIndependentAddTask *add_task1 = nullptr;
    ObIndependentAddTask *add_task2 = nullptr;
    ObIndependentMulTask *multi_task1 = nullptr;
    ObIndependentMulTask *multi_task2 = nullptr;
    ObIndependentCheckTask *check_task = nullptr;
    const int64_t expected_result = (*result1_ + (x_ * y_)) * 2;
    if (OB_ISNULL(dag_)) {
      ret = OB_ERR_UNEXPECTED;
      COMMON_LOG(ERROR, "dag is null", K(ret), KPC_(dag));
    } else if (OB_FAIL(dag_->create_task(this, add_task1, result1_, x_, 0, y_))) {
      COMMON_LOG(WARN, "failed to create task", K(ret), KPC(add_task1));
    } else if (OB_FAIL(dag_->create_task(this, add_task2, result2_, y_, 0, x_))) {
      COMMON_LOG(WARN, "failed to create task", K(ret), KPC(add_task2));
    } else if (OB_FAIL(dag_->create_task(add_task1, multi_task1, result1_))) {
      COMMON_LOG(WARN, "failed to create task", K(ret), KPC(multi_task1));
    } else if (OB_FAIL(dag_->create_task(add_task2, multi_task2, result2_))) {
      COMMON_LOG(WARN, "failed to create task", K(ret), KPC(multi_task2));
    } else {
      if (OB_FAIL(dag_->alloc_task(check_task))) {
        COMMON_LOG(WARN, "failed to alloc task", K(ret), KPC(check_task));
      } else if (OB_FAIL(check_task->init(expected_result, result1_, result2_))) {
        COMMON_LOG(WARN, "failed to init task", K(ret), KPC(check_task));
      } else if (OB_FAIL(multi_task1->add_child(*check_task))) {
        COMMON_LOG(WARN, "failed to add child", K(ret), KPC(check_task));
      } else if (OB_FAIL(multi_task2->add_child(*check_task))) {
        COMMON_LOG(WARN, "failed to add child", K(ret), KPC(check_task));
      } else if (OB_FAIL(dag_->add_task(*check_task))) {
        COMMON_LOG(WARN, "failed to add task", K(ret), KPC(check_task));
      }
      if (OB_FAIL(ret) && OB_NOT_NULL(check_task)) {
        check_task->reset_node();
        dag_->remove_task(*check_task);
        check_task = nullptr;
      }
    }
    COMMON_LOG(INFO, "ObIndependentPrepareTask process finished", K(ret));
  }
  return ret;
};

/* ========================= ObIndependentAddTask/ObIndependentMulTask =================================== */
int ObIndependentAddTask::process()
{
  int ret = OB_SUCCESS;
  ObIndependentExampleDag *dag = static_cast<ObIndependentExampleDag *>(dag_);
  if (OB_ISNULL(dag)) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "dag is null", K(ret));
  } else if (OB_FAIL(dag->handle_random_abort())) {
    COMMON_LOG(WARN, "failed to handle random abort", K(ret));
  } else {
    ret = TestAddTask::process();
  }
  return ret;
}

int ObIndependentAddTask::generate_next_task(ObITask *&next_task)
{
  int ret = OB_SUCCESS;
  ObIndependentExampleDag *dag = static_cast<ObIndependentExampleDag *>(dag_);
  if (OB_ISNULL(dag)) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "dag is null", K(ret));
  } else if (dag->generate_before_process_) {
    ret = TestAddTask::generate_next_task(next_task);
  } else {
    ret = OB_ITER_END;
  }
  return ret;
}

int ObIndependentAddTask::post_generate_next_task(ObITask *&next_task)
{
  int ret = OB_SUCCESS;
  ObIndependentExampleDag *dag = static_cast<ObIndependentExampleDag *>(dag_);
  if (OB_ISNULL(dag)) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "dag is null", K(ret));
  } else if (dag->generate_before_process_) {
    ret = OB_ITER_END;
  } else if (OB_FAIL(TestAddTask::generate_next_task(next_task))) {
    COMMON_LOG(WARN, "failed to generate next task", K(ret));
  } else if (OB_ISNULL(next_task)) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "next task is null", K(ret));
  } else if (OB_FAIL(copy_children_to(*next_task))) {
    COMMON_LOG(WARN, "failed to copy dependency to new task", K(ret));
  } else {
    COMMON_LOG(INFO, "finish post generate next task", K(ret));
  }
  return ret;
}

int ObIndependentMulTask::process()
{
  int ret = OB_SUCCESS;
  ObIndependentExampleDag *dag = static_cast<ObIndependentExampleDag *>(dag_);
  if (OB_ISNULL(dag)) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "dag is null", K(ret));
  } else if (OB_FAIL(dag->handle_random_abort())) {
    COMMON_LOG(WARN, "failed to handle random abort", K(ret));
  } else {
    ret = TestMulTask::process();
  }
  return ret;
}

/* ========================= ObIndependentCheckTask =================================== */
int ObIndependentCheckTask::init(
  const int expected_result,
  int64_t *result1,
  int64_t *result2)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    COMMON_LOG(WARN, "task is inited", K(ret));
  } else if (OB_ISNULL(result1) || OB_ISNULL(result2)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid argument", K(ret), KP(result1), KP(result2));
  } else {
    expected_result_ = expected_result;
    result1_ = result1;
    result2_ = result2;
    is_inited_ = true;
  }
  return ret;
}

int ObIndependentCheckTask::process()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(result1_) || OB_ISNULL(result2_)) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "invalid argument", K(ret), KP(result1_), KP(result2_));
  } else {
    const int64_t res1 = *result1_;
    const int64_t res2 = *result2_;
    if (res1 != expected_result_ || res2 != expected_result_) {
      ret = OB_ERR_UNEXPECTED;
      COMMON_LOG(ERROR, "unexpected result", K(ret), K(expected_result_), K(res1), K(res2));
    } else {
      COMMON_LOG(INFO, "check success", K_(expected_result), K(res1), K(res2));
    }
  }
  return ret;
}

/* ========================= ObIndependentPowerDag ===================================*/
int ObIndependentPowerDag::create_first_task()
{
  int ret = OB_SUCCESS;
  ObIndependentPowerPrepareTask *prepare_task = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "ObIndependentPowerDag is not inited", K(ret));
  } else if (OB_FAIL(create_task(nullptr /*parent*/, prepare_task, result1_, result2_, x_, y_))) {
    COMMON_LOG(WARN, "failed to create task", K(ret));
  } else {
    COMMON_LOG(INFO, "ObIndependentPowerDag create first task success", KPC(this));
  }
  return ret;
}

int ObIndependentPowerPrepareTask::process()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "ObIndependentPowerPrepareTask is not inited", K(ret));
  } else {
    ObIndependentPowerTask *power_task1 = nullptr;
    ObIndependentPowerTask *power_task2 = nullptr;
    ObIndependentCheckTask *check_task = nullptr;
    *result1_ = x_;
    *result2_ = x_;
    const int64_t expected_result = x_ * std::pow(2, std::pow(2, y_) - 1);
    if (OB_ISNULL(dag_)) {
      ret = OB_ERR_UNEXPECTED;
      COMMON_LOG(ERROR, "dag is null", K(ret), KPC_(dag));
    } else if (OB_FAIL(dag_->create_task(this, power_task1, result1_, y_))) {
      COMMON_LOG(WARN, "failed to create task", K(ret), KPC(power_task1));
    } else if (OB_FAIL(dag_->create_task(this, power_task2, result2_, y_))) {
      COMMON_LOG(WARN, "failed to create task", K(ret), KPC(power_task2));
    } else {
      if (OB_FAIL(dag_->alloc_task(check_task))) {
        COMMON_LOG(WARN, "failed to alloc task", K(ret), KPC(check_task));
      } else if (OB_FAIL(check_task->init(expected_result, result1_, result2_))) {
        COMMON_LOG(WARN, "failed to init task", K(ret), KPC(check_task));
      } else if (OB_FAIL(power_task1->add_child(*check_task))) {
        COMMON_LOG(WARN, "failed to add child", K(ret), KPC(check_task));
      } else if (OB_FAIL(power_task2->add_child(*check_task))) {
        COMMON_LOG(WARN, "failed to add child", K(ret), KPC(check_task));
      } else if (OB_FAIL(dag_->add_task(*check_task))) {
        COMMON_LOG(WARN, "failed to add task", K(ret), KPC(check_task));
      }
      if (OB_FAIL(ret) && OB_NOT_NULL(check_task)) {
        check_task->reset_node();
        dag_->remove_task(*check_task);
        check_task = nullptr;
      }
    }
  }
  return ret;
}

int ObIndependentPowerTask::init(
    int64_t *counter,
    int64_t layer)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    COMMON_LOG(WARN, "ObIndependentPowerTask is inited", K(ret));
  } else if (OB_ISNULL(counter) || layer <= 0) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid args", K(ret), KP(counter), K(layer));
  } else {
    counter_ = counter;
    layer_ = layer;
    is_inited_ = true;
  }
  return ret;
}

int ObIndependentPowerTask::process()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "ObIndependentPowerTask is not inited", K(ret));
  } else {
    int64_t old_val, new_val;
    do {
      old_val = ATOMIC_LOAD(counter_);
      new_val = old_val * 2;
    } while (ATOMIC_VCAS(counter_, old_val, new_val) != old_val);

    if (OB_FAIL(generate_next_layer_task())) {
      COMMON_LOG(WARN, "failed to generate next layer task", K(ret), KPC(this));
    } else if (OB_FAIL(generate_next_layer_task())) {
      COMMON_LOG(WARN, "failed to generate next layer task", K(ret), KPC(this));
    }
  }
  return ret;
}

 int ObIndependentPowerTask::generate_next_layer_task()
 {
  int ret = OB_SUCCESS;
  const int64_t next_layer = layer_ - 1;
  if (next_layer > 0) {
    ObIndependentPowerTask *power_task = nullptr;
    if (OB_FAIL(dag_->alloc_task(power_task))) {
      COMMON_LOG(WARN, "failed to alloc task", K(ret), KPC(this));
    } else if (OB_FAIL(power_task->init(counter_, next_layer))) {
      COMMON_LOG(WARN, "failed to init task", K(ret), KPC(power_task));
    } else if (copy_children_to(*power_task)) {
      COMMON_LOG(WARN, "failed to copy children", K(ret), KPC(power_task));
    } else if (OB_FAIL(dag_->add_task(*power_task))) {
      COMMON_LOG(WARN, "failed to add task", K(ret), KPC(power_task));
    }
    if (OB_FAIL(ret) && OB_NOT_NULL(power_task)) {
      power_task->reset_node();
      dag_->remove_task(*power_task);
      power_task = nullptr;
    }
  }
  return ret;
 }

/* ========================= ObIndependentPostGenerateDag ========================= */
int ObIndependentPostGenerateDag::create_first_task()
{
  int ret = OB_SUCCESS;
  ObHighConcurrencyPrepareTask *prepare_task = nullptr;
  const int64_t task_num = LOOP_TASK_NUM;
  const int64_t post_generate_cnt = LOOP_TASK_POST_GENERATE_CNT;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "ObIndependentPostGenerateDag is not inited", K(ret));
  } else if (OB_FAIL(create_task(nullptr /*parent*/, prepare_task, task_num, post_generate_cnt))) {
    COMMON_LOG(WARN, "failed to create task", K(ret));
  } else {
    COMMON_LOG(INFO, "ObIndependentPostGenerateDag create first task success", KPC(this));
  }
  return ret;
}

int ObHighConcurrencyPrepareTask::init(const int64_t task_num, const int64_t task_generate_cnt)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    COMMON_LOG(WARN, "is inited", K(ret));
  } else if (OB_UNLIKELY(task_num <= 0 || task_generate_cnt <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid argument", K(ret), K(task_num), K(task_generate_cnt));
  } else {
    task_num_ = task_num;
    task_generate_cnt_ = task_generate_cnt;
    is_inited_ = true;
    COMMON_LOG(INFO, "ObHighConcurrencyPrepareTask init success", KPC(this));
  }
  return ret;
}

int ObHighConcurrencyPrepareTask::process()
{
  int ret = OB_SUCCESS;
  ObHighConcurrencyFinsihTask *finish_task = nullptr;
  if (OB_ISNULL(dag_)) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "dag_ is null", K(ret));
  } else if (OB_FAIL(dag_->create_task(this, finish_task, task_num_ * task_generate_cnt_))) {
    COMMON_LOG(WARN, "failed to create finish task", K(ret));
  } else {
    ObIndependentPostGenerateDag *post_generate_dag = static_cast<ObIndependentPostGenerateDag *>(dag_);
    for (int64_t i = 0; OB_SUCC(ret) && i < task_num_; ++i) {
      ObHighConcurrencyFastLoopTask *loop_task = nullptr;
      if (OB_FAIL(dag_->create_task(this, loop_task, post_generate_dag->get_counter_ptr(), i /*idx*/, 0 /*seq*/, task_generate_cnt_))) {
        COMMON_LOG(WARN, "failed to create loop task", K(ret));
      } else if (OB_FAIL(loop_task->add_child(*finish_task, false /*check_child_task_status*/))) {
        COMMON_LOG(WARN, "failed to add child for loop task", K(ret), KPC(loop_task), KPC(finish_task));
      }
    }
  }
  return ret;
}

int ObHighConcurrencyFastLoopTask::init(
    int64_t *counter,
    const int64_t idx,
    const int64_t seq,
    const int64_t task_cnt)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    COMMON_LOG(WARN, "init twice", K(ret));
  } else if (OB_ISNULL(counter)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid counter", K(ret));
  } else {
    counter_ = counter;
    idx_ = idx;
    seq_ = seq;
    task_cnt_ = task_cnt;
    is_inited_ = true;
  }
  return ret;
}

int ObHighConcurrencyFastLoopTask::process()
{
  int ret = OB_SUCCESS;
  const int64_t start_time = ObTimeUtility::current_time();
  int64_t diff = 0;
  COMMON_LOG(INFO, "start loop task process", K(ret), K(start_time));
  while (true) {
    int64_t current_time = ObTimeUtility::current_time();
    diff = current_time - start_time;
    if (diff > LOOP_TIMEOUT) {
      ATOMIC_AAF(counter_, 1);
      break;
    } else {
      COMMON_LOG(INFO, "loop task process, sleep it", K_(idx), K_(seq), K_(task_cnt), K(current_time), K(start_time), K(diff));
      // ::usleep(SLEEP_TIME);
      for (int64_t i = 0; i < 1000 * 100; i++) { // no sleep
        if (i % 1000 == 0) {
          COMMON_LOG(INFO, "loop task process, print log", K_(idx), K_(seq), K_(task_cnt), K(i), K(current_time), K(start_time), K(diff));
        }
      }
    }
  }
  COMMON_LOG(INFO, "loop task process end", K(ret), K_(idx), K_(seq), K(diff));
  return ret;
}

int ObHighConcurrencyFastLoopTask::post_generate_next_task(ObITask *&next_task)
{
  int ret = OB_SUCCESS;
  next_task = nullptr;
  ObHighConcurrencyFastLoopTask *ntask = nullptr;
  if (seq_ >= task_cnt_ - 1) {
    ret = OB_ITER_END;
    COMMON_LOG(INFO, "generate task end", K_(idx), K_(seq), K_(task_cnt));
  } else if (OB_ISNULL(dag_)) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "dag is null", K(ret));
  } else if (OB_FAIL(dag_->alloc_task(ntask))) {
    COMMON_LOG(WARN, "failed to alloc task", K(ret));
  } else if (OB_ISNULL(ntask)) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "task is null", K(ret));
  } else if (OB_FAIL(ntask->init(counter_, idx_, seq_ + 1, task_cnt_))) {
    COMMON_LOG(WARN, "failed to init task", K(ret));
  } else {
    next_task = ntask;
    COMMON_LOG(INFO, "next task created", KPC(next_task));
  }

  if (OB_FAIL(ret) && OB_NOT_NULL(ntask)) {
    ObITask *tmp_task = ntask;
    dag_->free_task(tmp_task);
  }
  return ret;
}

int ObHighConcurrencyFinsihTask::process()
{
  int ret = OB_SUCCESS;
  ObIndependentPostGenerateDag *post_generate_dag = nullptr;
  constexpr int64_t expected_result = ObIndependentPostGenerateDag::LOOP_TASK_NUM * ObIndependentPostGenerateDag::LOOP_TASK_POST_GENERATE_CNT;
  if (OB_ISNULL(post_generate_dag = static_cast<ObIndependentPostGenerateDag *>(dag_))) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "dag is null", K(ret));
  } else if (OB_UNLIKELY(expected_result != post_generate_dag->get_counter())) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "unexpected counter", K(ret), K(expected_result), KPC(post_generate_dag));
  } else {
    COMMON_LOG(INFO, "ObHighConcurrencyFinsihTask check finished", K(ret));
  }
  return ret = OB_SUCCESS;
}

/* ========================= ObIndependentSuspendDag ===================================*/
int ObIndependentSuspendDag::init_by_param(const ObIDagInitParam *param)
{
  int ret = OB_SUCCESS;
  const ObIndependentSuspendInitParam *init_param = nullptr;
  if (OB_ISNULL(param) || OB_ISNULL(init_param = static_cast<const ObIndependentSuspendInitParam *>(param))
      || OB_UNLIKELY(!init_param->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid arg to init ObIndependentSuspendDag", K(ret), KP(param), KPC(init_param));
  } else {
    result_ = init_param->result_;
    round_ = init_param->round_;
    step_ = init_param->step_;
    COMMON_LOG(INFO, "successfully init ObIndependentSuspendDag", K(ret), KPC(init_param));
  }
  return ret;
}

int ObIndependentSuspendDag::create_first_task()
{
  int ret = OB_SUCCESS;
  ObIndependentSuspendTask *suspend_task = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "ObIndependentSuspendDag not init", K(ret));
  } else if (OB_FAIL(create_task(nullptr /*parent*/, suspend_task, result_, round_, step_))) {
    COMMON_LOG(WARN, "fail to create suspend task", K(ret));
  } else {
    COMMON_LOG(INFO, "success to create suspend task", K(ret), KPC(suspend_task));
  }
  return ret;
}

int ObIndependentSuspendTask::init(
    int64_t *counter,
    int64_t round,
    int64_t step )
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(counter) || round <= 0 || step <= 0) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid argument", K(ret), KP(counter), K(round), K(step));
  } else {
    counter_ = counter;
    round_ = round;
    step_ = step;
    cur_round_ = 0;
  }
  return ret;
}

int ObIndependentSuspendTask::process()
{
  int ret = OB_SUCCESS;
  if (cur_round_ < round_) {
    (void)ATOMIC_AAF(counter_, step_);
    TestMulTask *multi_task = nullptr;
    if (OB_ISNULL(dag_)) {
      ret = OB_ERR_UNEXPECTED;
      COMMON_LOG(ERROR, "dag is null", K(ret), KPC_(dag));
    } else if (OB_FAIL(dag_->create_task(nullptr /*parent*/, multi_task, counter_))) {
      COMMON_LOG(WARN, "failed to create task", K(ret));
    } else {
      cur_round_++;
      ret = OB_DAG_TASK_IS_SUSPENDED;
      COMMON_LOG(INFO, "suspend task", K(ret));
    }
  } else {
    ret = OB_SUCCESS;
    COMMON_LOG(INFO, "finish all rounds", K(*counter_));
  }
  return ret;
}

int ObIndependentSuspendDagV2::create_first_task()
{
  int ret = OB_SUCCESS;
  ObIndependentSuspendTaskV2 *suspend_task = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "ObIndependentSuspendDagV2 not init", K(ret));
  } else if (OB_FAIL(create_task(nullptr /*parent*/, suspend_task, result_, round_, step_, 1))) {
    COMMON_LOG(WARN, "fail to create suspend task", K(ret));
  } else {
    COMMON_LOG(INFO, "success to create suspend task", K(ret), KPC(suspend_task));
  }
  return ret;
}

int ObIndependentSuspendTaskV2::init(
  int64_t *counter,
  int64_t round,
  int64_t step,
  int64_t cur_round)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObIndependentSuspendTask::init(counter, round, step))) {
    COMMON_LOG(WARN, "failed to init ObIndependentSuspendTask", K(ret));
  } else {
    cur_round_ = cur_round;
  }
  return ret;
}

int ObIndependentSuspendTaskV2::generate_next_task(ObITask *&next_task)
{
  int ret = OB_SUCCESS;
  ObIndependentSuspendTaskV2 *suspend_task = nullptr;
  if (cur_round_ >= round_) {
    ret = OB_ITER_END;
    COMMON_LOG(INFO, "generate next task end", K(ret));
  } else if (OB_ISNULL(dag_)) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(ERROR, "dag is null", K(ret), KPC_(dag));
  } else if (OB_FAIL(dag_->create_task(nullptr /*parent*/, suspend_task, counter_, round_, step_, cur_round_ + 1))) {
    COMMON_LOG(WARN, "failed to create task", K(ret));
  } else {
    next_task = suspend_task;
  }
  return ret;
}

int ObIndependentSuspendTaskV2::process()
{
  int ret = OB_SUCCESS;
  int rand_num = ObRandom::rand(0, 10);
  if (rand_num < 5) {
    ret = OB_DAG_TASK_IS_SUSPENDED;
    COMMON_LOG(INFO, "suspend task", K(ret), K_(cur_round));
  } else {
    (void) ATOMIC_AAF(counter_, step_);
    COMMON_LOG(INFO, "finish task", K_(cur_round));
  }
  return ret;
}

/* ========================= DagWorkerThreadPool ===================================*/
class DagWorkerThreadPool : public lib::ThreadPool
{
public:
  DagWorkerThreadPool() {}
  ~DagWorkerThreadPool() {}
  int init(ObDagExecutor *dag_executor, const int64_t thread_cnt);
  void run1() final;
private:
  ObDagExecutor *dag_executor_;
};

int DagWorkerThreadPool::init(ObDagExecutor *dag_executor, const int64_t thread_cnt)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(dag_executor) || thread_cnt <= 0) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid argument", K(ret), KPC(dag_executor), K(thread_cnt));
  } else if (OB_FAIL(set_thread_count(thread_cnt))) {
    COMMON_LOG(WARN, "set thread count failed", K(ret), K(thread_cnt));
  } else {
    dag_executor_ = dag_executor;
  }
  return ret;
}

void DagWorkerThreadPool::run1()
{
  int ret = OB_SUCCESS;
  const uint64_t thread_id = get_thread_idx();
  const int64_t start_time = ObTimeUtility::current_time();
  COMMON_LOG(INFO, "DagWorkerThread start", K(thread_id));
  if (OB_ISNULL(dag_executor_) || OB_UNLIKELY(!dag_executor_->is_inited())) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(ERROR, "dag_executor is null or not inited", K(ret), KPC_(dag_executor));
  } else if (OB_FAIL(dag_executor_->run())) {
    COMMON_LOG(WARN, "run dag failed", K(ret), KPC(dag_executor_));
  }
  const int64_t elapsed_time = (ObTimeUtility::current_time() - start_time) / 1000;
  COMMON_LOG(INFO, "DagWorkerThread run finished", K(thread_id), K(elapsed_time));
}

class PxWorkerSimulator : public lib::ThreadPool
{
public:
  PxWorkerSimulator() {}
  ~PxWorkerSimulator() {}
  int init(ObIndependentDag *dag, const int64_t thread_cnt);
  void run1() final;
private:
  ObIndependentDag *dag_;
};

int PxWorkerSimulator::init(ObIndependentDag *dag, const int64_t thread_cnt)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(dag) || thread_cnt <= 0) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid argument", K(ret), KPC(dag), K(thread_cnt));
  } else if (OB_FAIL(set_thread_count(thread_cnt))) {
    COMMON_LOG(WARN, "set thread count failed", K(ret), K(thread_cnt));
  } else {
    dag_ = dag;
  }
  return ret;
}

void PxWorkerSimulator::run1()
{
  int ret = OB_SUCCESS;
  const uint64_t thread_id = get_thread_idx();
  const int64_t start_time = ObTimeUtility::current_time();
  COMMON_LOG(INFO, "PxWorkerSimulator start", K(thread_id));
  if (OB_ISNULL(dag_)) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(ERROR, "dag is null", K(ret), KPC_(dag));
  } else if (OB_FAIL(dag_->process())) {
    COMMON_LOG(WARN, "process dag failed", K(ret), KPC(dag_));
  }
  const int64_t elapsed_time = (ObTimeUtility::current_time() - start_time) / 1000;
  COMMON_LOG(INFO, "PxWorkerSimulator run finished", K(thread_id), K(elapsed_time));
}

TEST_F(TestIndependentDag, test_independent_dag)
{
  ObDagExecutor dag_executor;
  int64_t res1 = 0;
  int64_t res2 = 0;
  const int64_t x = 10;
  const int64_t y = 20;
  ObIndependentInitParam param(&res1, &res2, x, y);
  ASSERT_EQ(OB_SUCCESS, dag_executor.init<ObIndependentExampleDag>(&allocator_, &param, nullptr /*dag_id*/));
  ASSERT_NE(nullptr, dag_executor.dag_);
  ASSERT_EQ(true, dag_executor.dag_->is_independent_);

  DagWorkerThreadPool dag_worker_thread_pool;
  dag_worker_thread_pool.init(&dag_executor, 10);
  dag_worker_thread_pool.start();
  dag_worker_thread_pool.wait();
  ASSERT_EQ(400, res1);
  ASSERT_EQ(400, res2);
}

TEST_F(TestIndependentDag, test_independent_dag_stress)
{
  for (int64_t i = 0; i < STRESS_ROUND_CNT; i++) {
    for (int64_t thread_cnt = 1; thread_cnt < STRESS_THREAD_CNT; thread_cnt++) {
      ObDagExecutor dag_executor;
      int64_t res1 = 0;
      int64_t res2 = 0;
      const int64_t x = 1 + ObRandom::rand(0, 20);
      const int64_t y = 1 + ObRandom::rand(0, 20);
      COMMON_LOG(INFO, "[TestIndependentDag] test_independent_dag_stress", K(i), K(thread_cnt), K(x), K(y));
      ObIndependentInitParam param(&res1, &res2, x, y);
      ASSERT_EQ(OB_SUCCESS, dag_executor.init<ObIndependentExampleDag>(&allocator_, &param, nullptr /*dag_id*/));
      ASSERT_NE(nullptr, dag_executor.dag_);
      ASSERT_EQ(true, dag_executor.dag_->is_independent_);

      DagWorkerThreadPool dag_worker_thread_pool;
      dag_worker_thread_pool.init(&dag_executor, thread_cnt);
      dag_worker_thread_pool.start();
      dag_worker_thread_pool.wait();
      ASSERT_EQ(x * y * 2, res1);
      ASSERT_EQ(x * y * 2, res2);
    }
  }
}

TEST_F(TestIndependentDag, test_pure_independent_dag)
{
  ObIndependentExampleDag *dag = nullptr;
  int64_t res1 = 0;
  int64_t res2 = 0;
  const int64_t x = 10;
  const int64_t y = 20;
  ObIndependentInitParam param(&res1, &res2, x, y);
  ASSERT_EQ(OB_SUCCESS, ObTenantDagScheduler::alloc_dag(allocator_,false /*is_ha_dag*/, dag));
  ASSERT_NE(nullptr, dag);

  ASSERT_EQ(OB_SUCCESS, dag->init(&param, nullptr /*dag_id*/));
  ASSERT_EQ(true, dag->is_independent_);

  PxWorkerSimulator px_worker_simulator;
  px_worker_simulator.init(dag, 10);
  px_worker_simulator.start();
  px_worker_simulator.wait();
  ASSERT_EQ(400, res1);
  ASSERT_EQ(400, res2);

  dag->~ObIndependentExampleDag();
  allocator_.free(dag);
  dag = nullptr;
}

TEST_F(TestIndependentDag, test_pure_independent_dag_stress)
{
  for (int64_t i = 0; i < STRESS_ROUND_CNT; i++) {
    for (int64_t thread_cnt = 1; thread_cnt < STRESS_THREAD_CNT; thread_cnt++) {
      ObIndependentExampleDag *dag = nullptr;
      int64_t res1 = 0;
      int64_t res2 = 0;
      const int64_t x = 1 + ObRandom::rand(0, 20);
      const int64_t y = 1 + ObRandom::rand(0, 20);
      bool random_abort = i % 2 == 0;
      bool generate_before_process = i % 3 == 0;
      COMMON_LOG(INFO, "[TestIndependentDag] test_pure_independent_dag_stress", K(i), K(thread_cnt), K(x), K(y), K(random_abort), K(generate_before_process));
      ObIndependentInitParam param(&res1, &res2, x, y, random_abort, generate_before_process);
      ASSERT_EQ(OB_SUCCESS, ObTenantDagScheduler::alloc_dag(allocator_, false /*is_ha_dag*/, dag));
      ASSERT_NE(nullptr, dag);

      ASSERT_EQ(OB_SUCCESS, dag->init(&param, nullptr /*dag_id*/));
      ASSERT_EQ(true, dag->is_independent_);

      PxWorkerSimulator px_worker_simulator;
      px_worker_simulator.init(dag, 10);
      px_worker_simulator.start();
      px_worker_simulator.wait();

      if (dag->is_abort()) {
        ASSERT_NE(ObIDag::DAG_STATUS_FINISH, dag->get_dag_status());
        ASSERT_EQ(false, res1 == x * y * 2 && res2 == x * y * 2);
      } else {
        ASSERT_EQ(x * y * 2, res1);
        ASSERT_EQ(x * y * 2, res2);
      }

      dag->~ObIndependentExampleDag();
      allocator_.free(dag);
      dag = nullptr;
    }
  }
}

TEST_F(TestIndependentDag, test_pure_power_dag)
{
  ObIndependentPowerDag *dag = nullptr;
  int64_t res1 = 0;
  int64_t res2 = 0;
  const int64_t base = 10;
  const int64_t layer = 4;
  ObIndependentInitParam param(&res1, &res2, base, layer);
  ASSERT_EQ(OB_SUCCESS, ObTenantDagScheduler::alloc_dag(allocator_, false /*is_ha_dag*/, dag));
  ASSERT_NE(nullptr, dag);

  ASSERT_EQ(OB_SUCCESS, dag->init(&param, nullptr /*dag_id*/));
  ASSERT_EQ(true, dag->is_independent_);

  PxWorkerSimulator px_worker_simulator;
  px_worker_simulator.init(dag, 10);
  px_worker_simulator.start();
  px_worker_simulator.wait();
  ASSERT_EQ(327680, res1);
  ASSERT_EQ(327680, res2);
}

TEST_F(TestIndependentDag, test_pure_power_dag_stress)
{
  for (int64_t i = 0; i < STRESS_ROUND_CNT; i++) {
    for (int64_t thread_cnt = 1; thread_cnt < STRESS_THREAD_CNT; thread_cnt++) {
      ObIndependentPowerDag *dag = nullptr;
      int64_t res1 = 0;
      int64_t res2 = 0;
      const int64_t base = 1 + ObRandom::rand(0, 20);
      const int64_t layer = 1 + ObRandom::rand(0, 3);
      COMMON_LOG(INFO, "[TestDagScheduler] test_pure_power_dag_stress", K(i), K(base), K(layer));
      ObIndependentInitParam param(&res1, &res2, base, layer);
      ASSERT_EQ(OB_SUCCESS, ObTenantDagScheduler::alloc_dag(allocator_, false /*is_ha_dag*/, dag));
      ASSERT_NE(nullptr, dag);

      ASSERT_EQ(OB_SUCCESS, dag->init(&param, nullptr /*dag_id*/));
      ASSERT_EQ(true, dag->is_independent_);

      PxWorkerSimulator px_worker_simulator;
      px_worker_simulator.init(dag, 10);
      px_worker_simulator.start();
      px_worker_simulator.wait();
      ASSERT_EQ(base * std::pow(2, std::pow(2, layer) - 1), res1);
      ASSERT_EQ(base * std::pow(2, std::pow(2, layer) - 1), res2);
    }
  }
}

TEST_F(TestIndependentDag, test_independent_post_generate_dag)
{
  OB_LOGGER.set_log_level("ERROR");
  ObIndependentPostGenerateDag *dag = nullptr;
  ObIndependentPostGenerateDagInitParam param;
  ASSERT_EQ(OB_SUCCESS, ObTenantDagScheduler::alloc_dag(allocator_, false /*is_ha_dag*/, dag));
  ASSERT_NE(nullptr, dag);
  ASSERT_EQ(OB_SUCCESS, dag->init(&param, nullptr /*dag_id*/));
  ASSERT_EQ(true, dag->is_independent_);

  PxWorkerSimulator px_worker_simulator;
  px_worker_simulator.init(dag, 100);
  px_worker_simulator.start();
  px_worker_simulator.wait();
  ASSERT_EQ(dag->get_counter(), ObIndependentPostGenerateDag::LOOP_TASK_NUM * ObIndependentPostGenerateDag::LOOP_TASK_POST_GENERATE_CNT);
  OB_LOGGER.set_log_level("INFO");
}

TEST_F(TestIndependentDag, test_increasing_add_task)
{
  OB_LOGGER.set_log_level("TRACE");
  ObIndependentExampleDag *dag = nullptr;
  ASSERT_EQ(OB_SUCCESS, ObTenantDagScheduler::alloc_dag(allocator_, false /*is_ha_dag*/, dag));
  ASSERT_NE(nullptr, dag);
  ASSERT_EQ(true, dag->is_independent_);
  ObIDag::TaskList task_list;

  ObPriorityTask tasks[5];
  ObPriorityTask dag_tasks[5];
  tasks[0].priority_ = ObITask::TASK_PRIO_3;
  tasks[1].priority_ = ObITask::TASK_PRIO_2;
  tasks[2].priority_ = ObITask::TASK_PRIO_4;
  tasks[3].priority_ = ObITask::TASK_PRIO_5;
  tasks[4].priority_ = ObITask::TASK_PRIO_1;

  ASSERT_TRUE(tasks[0] <= tasks[0]);
  ASSERT_TRUE(tasks[0] <= tasks[2]);
  ASSERT_TRUE(tasks[0] <= tasks[3]);
  ASSERT_TRUE(tasks[1] <= tasks[2]);
  ASSERT_TRUE(tasks[1] <= tasks[3]);
  ASSERT_TRUE(tasks[2] <= tasks[3]);
  ASSERT_TRUE(tasks[4] <= tasks[0]);
  ASSERT_TRUE(tasks[4] <= tasks[1]);
  ASSERT_TRUE(tasks[4] <= tasks[2]);
  ASSERT_TRUE(tasks[4] <= tasks[3]);

  for (int i = 0; i < 5; i++) {
    task_list.increasing_add(&tasks[i]);
    dag_tasks[i].priority_ = tasks[i].get_priority();
    dag->inner_add_task_into_list(&dag_tasks[i]);
  }
  dag->update_ready_task_list();
  check_task_list_priority(task_list);
  check_task_list_priority(dag->task_list_);
  OB_LOGGER.set_log_level("INFO");
}

TEST_F(TestIndependentDag, test_priority_task)
{
  OB_LOGGER.set_log_level("TRACE");
  ObIndependentExampleDag *dag = nullptr;
  int64_t res1 = 0;
  int64_t res2 = 0;
  const int64_t x = 10;
  const int64_t y = 20;
  ObIndependentInitParam param(&res1, &res2, x, y);
  ASSERT_EQ(OB_SUCCESS, ObTenantDagScheduler::alloc_dag(allocator_, false /*is_ha_dag*/, dag));
  ASSERT_NE(nullptr, dag);

  ASSERT_EQ(OB_SUCCESS, dag->init(&param, nullptr /*dag_id*/));
  ASSERT_EQ(true, dag->is_independent_);

  dag->clear_task_list(); // when init dag, the first task will be created
  ObSEArray<int64_t, ObITask::TASK_PRIO_MAX> priority_task_cnts;
  for (uint8_t prio = ObITask::TASK_PRIO_0; prio < ObITask::TASK_PRIO_MAX; prio++) {
    priority_task_cnts.push_back(0);
  }

  for (int64_t i = 1; i <= 1049; i++) {
    const uint8_t priority = ObRandom::rand(ObITask::TASK_PRIO_0, ObITask::TASK_PRIO_MAX - 1);
    ASSERT_TRUE(priority >= 0 && priority < ObITask::TASK_PRIO_MAX);
    priority_task_cnts[priority]++;
    (void) alloc_and_add_priority_task(dag, static_cast<ObITask::ObITaskPriority>(priority));

    if (i % 50 == 0) {
      COMMON_LOG(INFO, "before update", K(dag->waiting_task_list_.get_size()), K(dag->task_list_.get_size()), K(dag->waiting_task_list_), K(dag->task_list_), K(priority_task_cnts));
      dag->update_ready_task_list();
      COMMON_LOG(INFO, "after update", K(dag->waiting_task_list_.get_size()), K(dag->task_list_.get_size()), K(dag->waiting_task_list_), K(dag->task_list_), K(priority_task_cnts));

      // check update task list
      int64_t can_schedule_task_cnt = 0;
      for (uint8_t prio = ObITask::TASK_PRIO_1; prio < ObITask::TASK_PRIO_MAX; prio++) {
        can_schedule_task_cnt += priority_task_cnts[prio];
      }
      ASSERT_EQ(can_schedule_task_cnt, dag->task_list_.get_size());
      check_task_list_priority(dag->task_list_);
      DLIST_FOREACH_NORET(cur, dag->waiting_task_list_) {
        ASSERT_EQ(ObITask::TASK_PRIO_0, cur->get_priority());
      }
      ASSERT_EQ(priority_task_cnts[ObITask::TASK_PRIO_0], dag->waiting_task_list_.get_size());

      // clear ready task list and priority_task_cnts
      dag->inner_clear_task_list(dag->task_list_);
      dag->task_list_.reset();
      for (uint8_t prio = ObITask::TASK_PRIO_1; prio < ObITask::TASK_PRIO_MAX; prio++) {
        priority_task_cnts[prio] = 0;
      }
    }
  }

  dag->~ObIndependentExampleDag();
  ASSERT_EQ(0, dag->task_list_.get_size());
  ASSERT_EQ(0, dag->waiting_task_list_.get_size());
  allocator_.free(dag);
  dag = nullptr;
  OB_LOGGER.set_log_level("INFO");
}


TEST_F(TestIndependentDag, test_independent_suspend_task)
{
  ObIndependentSuspendDag *dag = nullptr;
  int64_t result = 0;
  int64_t round = 20;
  int64_t step = 10;
  ObIndependentSuspendInitParam param(&result, round, step);
  ASSERT_EQ(OB_SUCCESS, ObTenantDagScheduler::alloc_dag(allocator_, false /*is_ha_dag*/, dag));
  ASSERT_NE(nullptr, dag);
  ASSERT_EQ(OB_SUCCESS, dag->init(&param, nullptr /*dag_id*/));
  ASSERT_EQ(true, dag->is_independent_);

  PxWorkerSimulator px_worker_simulator;
  px_worker_simulator.init(dag, 1); // only one thread
  px_worker_simulator.start();
  px_worker_simulator.wait();

  int64_t expected_result = 0;
  int cur_round = 0;
  for (int64_t cur_round = 0; cur_round < round; ++cur_round) {
    expected_result += step;
    expected_result *= 2;
  }
  ASSERT_EQ(expected_result, result);
  ASSERT_EQ(20971500, result);
}

TEST_F(TestIndependentDag, test_independent_suspend_task_v2)
{
  ObIndependentSuspendDagV2 *dag = nullptr;
  int64_t result = 0;
  int64_t round = 20;
  int64_t step = 10;
  ObIndependentSuspendInitParam param(&result, round, step);
  ASSERT_EQ(OB_SUCCESS, ObTenantDagScheduler::alloc_dag(allocator_, false /*is_ha_dag*/, dag));
  ASSERT_NE(nullptr, dag);
  ASSERT_EQ(OB_SUCCESS, dag->init(&param, nullptr /*dag_id*/));
  ASSERT_EQ(true, dag->is_independent_);

  PxWorkerSimulator px_worker_simulator;
  px_worker_simulator.init(dag, 1); // only one thread
  px_worker_simulator.start();
  px_worker_simulator.wait();
  ASSERT_EQ(200, result);
}

TEST_F(TestIndependentDag, test_independent_dag_print)
{
  ObIndependentExampleDag *dag = nullptr;
  int64_t res1 = 0;
  int64_t res2 = 0;
  const int64_t x = 10;
  const int64_t y = 20;
  ObIndependentInitParam param(&res1, &res2, x, y);
  ASSERT_EQ(OB_SUCCESS, ObTenantDagScheduler::alloc_dag(allocator_, false /*is_ha_dag*/, dag));
  ASSERT_NE(nullptr, dag);

  ASSERT_EQ(OB_SUCCESS, dag->init(&param, nullptr /*dag_id*/));
  ASSERT_EQ(true, dag->is_independent_);
  dag->clear_task_list(); // when init dag, the first task will be created
  dag->dump_dag_status();

  for (int64_t i = 1; i <= 50; i++) {
    const uint8_t priority = ObRandom::rand(ObITask::TASK_PRIO_0, ObITask::TASK_PRIO_MAX - 1);
    ASSERT_TRUE(priority >= 0 && priority < ObITask::TASK_PRIO_MAX);
    (void) alloc_and_add_priority_task(dag, static_cast<ObITask::ObITaskPriority>(priority));
    if (i % 25 == 0) {
      dag->update_ready_task_list();
      check_task_list_priority(dag->task_list_);
      DLIST_FOREACH_NORET(cur, dag->waiting_task_list_) {
        ASSERT_EQ(ObITask::TASK_PRIO_0, cur->get_priority());
      }
      dag->dump_dag_status();
      dag->inner_clear_task_list(dag->task_list_);
      dag->task_list_.reset();
    }
  }

  dag->~ObIndependentExampleDag();
  allocator_.free(dag);
  dag = nullptr;
}

} // end namespace unittest
} // end namespace oceanbase

int main(int argc, char** argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  system("rm -rf test_independent_dag.log*");
  OB_LOGGER.set_file_name("test_independent_dag.log", true);
  OB_LOGGER.set_log_level("INFO");
  OB_LOGGER.set_max_file_size(256*1024*1024);
  return RUN_ALL_TESTS();
}
