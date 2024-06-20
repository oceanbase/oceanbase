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
#include "lib/atomic/ob_atomic.h"
#include "lib/alloc/ob_malloc_allocator.h"
#include "observer/omt/ob_tenant_node_balancer.h"
#include "share/scheduler/ob_dag_warning_history_mgr.h"
#include "storage/meta_mem/ob_tenant_meta_mem_mgr.h"

int64_t dag_cnt = 1;
int64_t stress_time= 1; // 100ms
char log_level[20] = "INFO";
uint32_t time_slice = 1000;
uint64_t check_waiting_list_period = 1000;
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
using namespace lib;

namespace storage
{

int64_t ObTenantMetaMemMgr::cal_adaptive_bucket_num()
{
  return 1000;
}

int ObTenantMetaMemMgr::fetch_tenant_config()
{
  return OB_SUCCESS;
}

}

namespace unittest
{

class TestDagScheduler : public ::testing::Test
{
public:
  TestDagScheduler()
    : tenant_id_(500),
      tablet_scheduler_(nullptr),
      scheduler_(nullptr),
      dag_history_mgr_(nullptr),
      tenant_base_(500)
  {}
  ~TestDagScheduler() {}
  void SetUp()
  {
    ObUnitInfoGetter::ObTenantConfig unit_config;
    unit_config.mode_ = lib::Worker::CompatMode::MYSQL;
    unit_config.tenant_id_ = 0;
    TenantUnits units;
    ASSERT_EQ(OB_SUCCESS, units.push_back(unit_config));

    ObTenantMetaMemMgr *t3m = OB_NEW(ObTenantMetaMemMgr, ObModIds::TEST, 500);
    tenant_base_.set(t3m);

    tablet_scheduler_ = OB_NEW(compaction::ObTenantTabletScheduler, ObModIds::TEST);
    tenant_base_.set(tablet_scheduler_);

    scheduler_ = OB_NEW(ObTenantDagScheduler, ObModIds::TEST);
    tenant_base_.set(scheduler_);

    dag_history_mgr_ = OB_NEW(ObDagWarningHistoryManager, ObModIds::TEST);
    tenant_base_.set(dag_history_mgr_);

    diagnose_mgr_ = OB_NEW(compaction::ObDiagnoseTabletMgr, ObModIds::TEST);
    tenant_base_.set(diagnose_mgr_);

    ObTenantEnv::set_tenant(&tenant_base_);
    ASSERT_EQ(OB_SUCCESS, tenant_base_.init());

    ObMallocAllocator *ma = ObMallocAllocator::get_instance();
    ASSERT_EQ(OB_SUCCESS, ma->create_and_add_tenant_allocator(tenant_id_));
    ASSERT_EQ(OB_SUCCESS, ma->set_tenant_limit(tenant_id_, 1LL << 30));

    ASSERT_EQ(OB_SUCCESS, t3m->init());
    ASSERT_EQ(OB_SUCCESS, scheduler_->init(tenant_id_, time_slice, check_waiting_list_period, MAX_DAG_CNT));
    ObAddr addr(1683068975,9999);
    if (OB_SUCCESS != (ObSysTaskStatMgr::get_instance().set_self_addr(addr))) {
      COMMON_LOG_RET(WARN, OB_ERROR, "failed to add sys task", K(addr));
    }
  }
  void TearDown()
  {
    tablet_scheduler_->destroy();
    tablet_scheduler_ = nullptr;
    scheduler_->destroy();
    scheduler_ = nullptr;
    dag_history_mgr_->~ObDagWarningHistoryManager();
    dag_history_mgr_ = nullptr;
    diagnose_mgr_->destroy();
    diagnose_mgr_ = nullptr;
    tenant_base_.destroy();
    ObTenantEnv::set_tenant(nullptr);
  }
private:
  const static int64_t MAX_DAG_CNT = 64;
  const uint64_t tenant_id_;
  compaction::ObTenantTabletScheduler *tablet_scheduler_;
  ObTenantDagScheduler *scheduler_;
  ObDagWarningHistoryManager *dag_history_mgr_;
  compaction::ObDiagnoseTabletMgr *diagnose_mgr_;
  ObTenantBase tenant_base_;
  DISALLOW_COPY_AND_ASSIGN(TestDagScheduler);
};

void wait_scheduler() {
  ObTenantDagScheduler *scheduler = MTL(ObTenantDagScheduler*);
  ASSERT_TRUE(nullptr != scheduler);
  while (!scheduler->is_empty()) {
    usleep(100000);
  }
}

class ObBasicDag : public ObIDag
{
public:
  ObBasicDag() :
    ObIDag(ObDagType::DAG_TYPE_MAJOR_MERGE),
    id_(ObTimeUtility::current_time() + random())
  {}
  void init(int64_t id) { id_ = id; }
  virtual int64_t hash() const { return murmurhash(&id_, sizeof(id_), 0);}
  virtual bool operator == (const ObIDag &other) const
  {
    bool bret = false;
    if (get_type() == other.get_type()) {
      const ObBasicDag &dag = static_cast<const ObBasicDag &>(other);
      bret = dag.id_ == id_;
    }
    return bret;
  }
  virtual int fill_info_param(compaction::ObIBasicInfoParam *&out_param,
      ObIAllocator &allocator) const override
  {
    int ret = OB_SUCCESS;
    if (!is_inited_) {
      ret = OB_NOT_INIT;
    } else if (OB_FAIL(ADD_DAG_WARN_INFO_PARAM(out_param, allocator, get_type(), id_, id_+1, "is_test", true))) {
      COMMON_LOG(WARN, "fail to add dag warning info param", K(ret));
    }
    return ret;
  }
  virtual int fill_dag_key(char *buf,const int64_t size) const override { UNUSEDx(buf, size); return OB_SUCCESS; }
  virtual lib::Worker::CompatMode get_compat_mode() const override
  { return lib::Worker::CompatMode::MYSQL; }
  virtual uint64_t get_consumer_group_id() const override
  { return consumer_group_id_; }
  virtual bool is_ha_dag() const override { return false; }

  INHERIT_TO_STRING_KV("ObIDag", ObIDag, K_(is_inited), K_(type), K_(id), K(task_list_.get_size()), K_(dag_ret));

private:
  int64_t id_;
  DISALLOW_COPY_AND_ASSIGN(ObBasicDag);
};

/*
 * check dag wait to schedule
 * */

class ObWaitTask : public ObITask
{
public:
  ObWaitTask() : ObITask(ObITaskType::TASK_TYPE_UT), cnt_(0), start_time_(0), finish_time_(0) {}
  virtual ~ObWaitTask() {}
  virtual int process()
  {
    int ret = OB_SUCCESS;
    if (cnt_ == 0) {
      start_time_ = ObTimeUtility::current_time();
    } else if (cnt_ < FINISH_CNT) {
      cnt_++;
      if (OB_FAIL(dag_yield())) {
        if (OB_CANCELED != ret) {
          COMMON_LOG(WARN, "Invalid return value for dag_yield", K(ret));
        }
      }
    } else {
      finish_time_ = ObTimeUtility::current_time();
      COMMON_LOG(INFO, "finish process", K(start_time_), K_(finish_time));
    }
    return ret;
  }
private:
  const static int64_t FINISH_CNT = 5;
  int cnt_;
  int64_t start_time_;
  int64_t finish_time_;
};

class ObWaitDag : public ObBasicDag
{
public:
  ObWaitDag() :
    ObBasicDag(),
    retry_times_(0),
    last_run_time_(0)
  {}
  virtual int create_first_task() override
  {
    int ret = OB_SUCCESS;
    ObWaitTask *task = NULL;
    if (OB_FAIL(alloc_task(task))) {
      COMMON_LOG(WARN, "Fail to alloc task", K(ret));
    } else if (OB_FAIL(add_task(*task))) {
      COMMON_LOG(WARN, "Fail to add task", K(ret));
    }
    return common::OB_SUCCESS;
  }
  bool check_can_retry()
  {
    bool bret = true;
    if (retry_times_++ > MAX_RETRY_TIMES) {
      bret = false;
    }
    return bret;
  }

  virtual bool check_can_schedule() override
  {
    bool bret = true;
    if (ObTimeUtility::current_time() - last_run_time_ < MAX_CHECK_INTERVAL) {
      bret = false;
    } else {
      last_run_time_ = ObTimeUtility::current_time();
      STORAGE_LOG(INFO, "check_can_schedule", KPC(this));
    }
    return bret;
  }
  INHERIT_TO_STRING_KV("ObBasicDag", ObBasicDag, K_(retry_times), K_(last_run_time));
private:
  const int64_t MAX_RETRY_TIMES = 20;
  const int64_t MAX_CHECK_INTERVAL = 1000L * 100L; // 100ms

  int64_t retry_times_;
  int64_t last_run_time_;
  DISALLOW_COPY_AND_ASSIGN(ObWaitDag);
};


TEST_F(TestDagScheduler, test_task_wait_to_schedule)
{
  ObTenantDagScheduler *scheduler = MTL(ObTenantDagScheduler*);
  ASSERT_TRUE(nullptr != scheduler);
  ObDagWarningHistoryManager* manager = MTL(ObDagWarningHistoryManager *);
  ASSERT_TRUE(nullptr != manager);
  EXPECT_EQ(OB_SUCCESS, MTL(ObDagWarningHistoryManager *)->init(true, MTL_ID(), "DagWarnHis"));

  for (int i = 0; i < 10; ++i) {
    EXPECT_EQ(OB_SUCCESS, scheduler->create_and_add_dag<ObWaitDag>(nullptr));
  }

  wait_scheduler();
  EXPECT_EQ(0, MTL(ObDagWarningHistoryManager *)->size());
}

/*
 * check task retry
 * */
class ObRetryTask : public ObITask
{
public:
  ObRetryTask() : ObITask(ObITaskType::TASK_TYPE_NORMAL_MINOR_MERGE), cnt_(0), seq_(0) {}
  virtual ~ObRetryTask() {}
  virtual int process()
  {
    int ret = OB_SUCCESS;
    if (cnt_++ < FINISH_CNT) {
      ret = OB_ERROR;
    }
    return ret;
  }
  void init(int64_t seq) { seq_ = seq; }
  virtual int generate_next_task(ObITask *&next_task)
  {
    int ret = OB_SUCCESS;
    if (seq_ >= MAX_SEQ) {
      ret = OB_ITER_END;
      COMMON_LOG(INFO, "generate task end", K_(seq));
    } else {
      ObIDag *dag = get_dag();
      ObRetryTask *ntask = NULL;
      if (NULL == dag) {
        ret = OB_ERR_UNEXPECTED;
        COMMON_LOG(WARN, "dag is null", K(ret));
      } else if (OB_FAIL(dag->alloc_task(ntask))) {
        COMMON_LOG(WARN, "failed to alloc task", K(ret));
      } else if (NULL == ntask) {
        ret = OB_ERR_UNEXPECTED;
        COMMON_LOG(WARN, "task is null", K(ret));
      } else {
        ntask->init(seq_ + 1);
        next_task = ntask;
      }
    }
    return ret;
  }
private:
  const int64_t FINISH_CNT = 3;
  const int64_t MAX_SEQ = 3;
  int cnt_;
  int64_t seq_;
};

class ObDagRetryTask : public ObITask
{
public:
  ObDagRetryTask() : ObITask(ObITaskType::TASK_TYPE_NORMAL_MINOR_MERGE) {}
  virtual ~ObDagRetryTask() {}
  virtual int process()
  {
    static int cnt_ = 0;
    int ret = OB_SUCCESS;
    if (cnt_++ < FINISH_CNT) {
      ret = OB_ERROR;
    }
    return ret;
  }
private:
  const int64_t FINISH_CNT = 1;
};

struct ObRetryDagInitParam : public ObIDagInitParam
{
  ObRetryDagInitParam() : id_(0), str_() {}
  virtual ~ObRetryDagInitParam() {}
  virtual bool is_valid() const override
  {
    return id_ > 0 && !str_.empty();
  }

  int assign(const ObRetryDagInitParam &other)
  {
    int ret = OB_SUCCESS;
    id_ = other.id_;
    if (OB_FAIL(deep_copy_str(other.str_.ptr(), str_))) {
      STORAGE_LOG(WARN, "deep copy string", K(ret));
    }
    return ret;
  }

  int deep_copy_str(const char *src, ObString &dest)
  {
    int ret = OB_SUCCESS;
    char *buf = NULL;

    if (OB_ISNULL(src)) {
      ret = OB_INVALID_ARGUMENT;
      STORAGE_LOG(WARN, "The src is NULL, ", K(ret));
    } else {
      int64_t len = strlen(src) + 1;
      if (NULL == (buf = static_cast<char *>(allocator_.alloc(len)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        STORAGE_LOG(ERROR, "Fail to allocate memory, ", K(len), K(ret));
      } else {
        MEMCPY(buf, src, len-1);
        buf[len-1] = '\0';
        dest.assign_ptr(buf, static_cast<ObString::obstr_size_t>(len-1));
      }
    }
    return ret;
  }
  int64_t id_;
  ObString str_;
  ObArenaAllocator allocator_;
};

class ObDagRetryDag : public ObBasicDag
{
public:
  ObDagRetryDag() : ObBasicDag() {}
  virtual int init_by_param(const ObIDagInitParam *param) override
  {
    int ret = OB_SUCCESS;
    if (OB_ISNULL(param) || !param->is_valid()) {
      ret = OB_INVALID_ARGUMENT;
      COMMON_LOG(WARN, "invalid argument", K(ret), K(param));
    } else if (OB_FAIL(param_.assign(*(static_cast<const ObRetryDagInitParam *>(param))))) {
      COMMON_LOG(WARN, "failed to assign param", K(ret));
    }
    return ret;
  }
  virtual int create_first_task() override
  {
    int ret = OB_SUCCESS;
    ObDagRetryTask *task = NULL;
    if (OB_FAIL(alloc_task(task))) {
      COMMON_LOG(WARN, "Fail to alloc task", K(ret));
    } else if (OB_FAIL(add_task(*task))) {
      COMMON_LOG(WARN, "Fail to add task", K(ret));
    }
    return common::OB_SUCCESS;
  }
  virtual int inner_reset_status_for_retry() override
  {
    int ret = OB_SUCCESS;
    if (OB_FAIL(init_by_param(&param_))) {
      COMMON_LOG(WARN, "failed to init param", K(ret));
    } else if (OB_FAIL(create_first_task())) {
      COMMON_LOG(WARN, "failed to create first task", K(ret));
    }
    return ret;
  }

private:
  ObRetryDagInitParam param_;
  DISALLOW_COPY_AND_ASSIGN(ObDagRetryDag);
};

TEST_F(TestDagScheduler, test_dag_retry)
{
  ObTenantDagScheduler *scheduler = MTL(ObTenantDagScheduler*);
  ASSERT_TRUE(nullptr != scheduler);
  ObDagWarningHistoryManager* manager = MTL(ObDagWarningHistoryManager *);
  ASSERT_TRUE(nullptr != manager);
  EXPECT_EQ(OB_SUCCESS, MTL(ObDagWarningHistoryManager *)->init(true, MTL_ID(), "DagWarnHis"));

  int ret = OB_SUCCESS;
  for (int i = 0; OB_SUCC(ret) && i < 5; ++i) {
    ObDagRetryDag *dag = NULL;
    ObRetryDagInitParam param;
    const int64_t str_len = 100;
    char str[str_len];
    param.id_ = i + 1;
    snprintf(str, str_len, "Hello OceanBase_%d", i);
    param.str_ = ObString(str);
    if (OB_FAIL(scheduler->create_dag(&param, dag))) {
      COMMON_LOG(WARN, "failed to create dag", K(ret));
    } else if (FALSE_IT(dag->set_max_retry_times(3))) {
    } else if (OB_FAIL(scheduler->add_dag(dag))) {
      COMMON_LOG(WARN, "failed to add dag", K(ret));
    }
    EXPECT_EQ(OB_SUCCESS, ret);
  }

  wait_scheduler();
  EXPECT_EQ(0, MTL(ObDagWarningHistoryManager *)->size());
}

class ObOperator
{
public:
  ObOperator() : num_(0) {}
  ~ObOperator() {}
  void inc() { ATOMIC_INC(&num_); }
  void dec() { ATOMIC_DEC(&num_); }
private:
  int64_t num_;
};

class ObRunningTask : public ObITask
{
public:
  ObRunningTask()
    : ObITask(ObITaskType::TASK_TYPE_UT),
      seq_(0),
      op_(),
      is_inc_(true)
  {
  }
  virtual ~ObRunningTask() {}
  void init(const int64_t seq, ObOperator &op, bool is_inc) { seq_ = seq; op_ = &op; is_inc_ = is_inc; }
  virtual int process()
  {
    if (is_inc_) {
      op_->inc();
    } else {
      op_->dec();
    }
    return OB_SUCCESS;
  }
  virtual int generate_next_task(ObITask *&next_task)
  {
    int ret = OB_SUCCESS;
    if (seq_ >= MAX_SEQ) {
      ret = OB_ITER_END;
      COMMON_LOG(INFO, "generate task end", K_(seq));
    } else {
      ObIDag *dag = get_dag();
      ObRunningTask *ntask = NULL;
      if (NULL == dag) {
        ret = OB_ERR_UNEXPECTED;
        COMMON_LOG(WARN, "dag is null", K(ret));
      } else if (OB_FAIL(dag->alloc_task(ntask))) {
        COMMON_LOG(WARN, "failed to alloc task", K(ret));
      } else if (NULL == ntask) {
        ret = OB_ERR_UNEXPECTED;
        COMMON_LOG(WARN, "task is null", K(ret));
      } else {
        ntask->init(seq_ + 1, *op_, is_inc_);
        next_task = ntask;
      }
    }
    return ret;
  }
private:
  const int64_t MAX_SEQ = 5;
  int64_t seq_;
  ObOperator *op_;
  bool is_inc_;
};

class ObChildDag : public ObBasicDag
{
public:
  ObChildDag()
   : ObBasicDag(),
     op_(nullptr),
     is_inc_(true),
     cnt_(0)
  {}
  static const int64_t MAX_CHILD_DAG_CNT = 3;
  void init(ObOperator &op, bool is_inc) { op_ = &op; is_inc_ = is_inc; }
  virtual int create_first_task() override
  {
    int ret = OB_SUCCESS;
    ObRunningTask *task = NULL;
    if (OB_FAIL(alloc_task(task))) {
      COMMON_LOG(WARN, "Fail to alloc task", K(ret));
    } else if (FALSE_IT(task->init(0, *op_, is_inc_))) {
    } else if (OB_FAIL(add_task(*task))) {
      COMMON_LOG(WARN, "Fail to add task", K(ret));
    } else {
      COMMON_LOG(INFO, "success to add task", K(ret), KPC(this), KPC(task));
    }
    return common::OB_SUCCESS;
  }
  virtual bool check_can_schedule() override
  {
    bool bret = true;
    if (cnt_ < WAIT_CNT) {
      ++cnt_;
      bret  = false;
    }
    COMMON_LOG(INFO, "check_can_schedule", K(bret), KPC(this), K(cnt_));
    return bret;
  }
  INHERIT_TO_STRING_KV("BasicDag", ObBasicDag, K_(is_inited), K_(type), K_(id), K(task_list_.get_size()), K_(cnt));

private:
  const int64_t WAIT_CNT = 1;
  ObOperator *op_;
  bool is_inc_;
  int64_t cnt_;
};

class ObFatherPrepareTask : public ObITask
{
public:
  ObFatherPrepareTask()
    : ObITask(ObITaskType::TASK_TYPE_UT),
      op_(nullptr)
  {
  }
  virtual ~ObFatherPrepareTask() {}
  void init(ObOperator &op) { op_ = &op; }
  virtual int process()
  {
    int ret = OB_SUCCESS;
    ObTenantDagScheduler *scheduler = MTL(ObTenantDagScheduler*);
    if (OB_ISNULL(scheduler)) {
      ret = OB_ERR_UNEXPECTED;
      COMMON_LOG(WARN, "ObTenantDagScheduler is unexpected null", K(ret));
    }

    for (int i = 0; OB_SUCC(ret) && i < CHILD_DAG_CNT; ++i) {
      ObChildDag *child_dag = nullptr;
      if (OB_FAIL(scheduler->alloc_dag(child_dag))) {
        COMMON_LOG(WARN, "failed to alloc child dag", K(ret));
      } else if (FALSE_IT(child_dag->init(*op_, 0 == i % 2))) {
      } else if (OB_FAIL(child_dag->create_first_task())) {
        COMMON_LOG(WARN, "failed to create first task for child dag", K(ret), KPC(child_dag));
      } else if (OB_FAIL(dag_->add_child(*child_dag))) {
        COMMON_LOG(WARN, "failed to alloc dependency dag", K(ret), KPC(dag_), KPC(child_dag));
      } else if (OB_FAIL(scheduler->add_dag(child_dag))) {
        if (OB_EAGAIN != ret && OB_SIZE_OVERFLOW != ret) {
          COMMON_LOG(WARN, "failed to add dag", K(ret), KPC(dag_), KPC(child_dag));
        } else {
          ret = OB_SUCCESS;
        }
      } else {
        COMMON_LOG(INFO, "success to alloc child dag", K(ret), KPC(this), KPC(child_dag));
      }
    }
    return ret;
  }
private:
  const static int64_t CHILD_DAG_CNT = 2;
  ObOperator *op_;
};

class ObFatherPrepareDag : public ObBasicDag
{
public:
  ObFatherPrepareDag()
   : ObBasicDag(),
     op_(nullptr)
  {}
  void init(ObOperator &op) { op_ = &op;}
  virtual int create_first_task() override
  {
    int ret = OB_SUCCESS;
    ObFatherPrepareTask *task = NULL;
    if (OB_FAIL(alloc_task(task))) {
      COMMON_LOG(WARN, "Fail to alloc task", K(ret));
    } else if (FALSE_IT(task->init(*op_))) {
    } else if (OB_FAIL(add_task(*task))) {
      COMMON_LOG(WARN, "Fail to add task", K(ret));
    } else {
      COMMON_LOG(INFO, "success to add task", K(ret), KPC(this), KPC(task));
    }
    return common::OB_SUCCESS;
  }
  INHERIT_TO_STRING_KV("BasicDag", ObBasicDag, K_(is_inited), K_(type), K_(id), K(task_list_.get_size()));

private:
  ObOperator *op_;
};

class ObFatherFinishTask : public ObITask
{
public:
  ObFatherFinishTask()
    : ObITask(ObITaskType::TASK_TYPE_UT),
      op_(nullptr)
  {
  }
  virtual ~ObFatherFinishTask() {}
  void init(ObOperator &op) { op_ = &op; }
  virtual int process();

private:
  ObOperator *op_;
};

class ObFatherFinishDag : public ObBasicDag
{
public:
  ObFatherFinishDag()
   : ObBasicDag(),
     op_(nullptr)
  {}
  void init(ObOperator &op) { op_ = &op;}
  virtual int create_first_task() override
  {
    int ret = OB_SUCCESS;
    ObFatherFinishTask *task = NULL;
    if (OB_FAIL(alloc_task(task))) {
      COMMON_LOG(WARN, "Fail to alloc task", K(ret));
    } else if (FALSE_IT(task->init(*op_))) {
    } else if (OB_FAIL(add_task(*task))) {
      COMMON_LOG(WARN, "Fail to add task", K(ret));
    } else {
      COMMON_LOG(INFO, "success to add task", K(ret), KPC(this), KPC(task));
    }
    return common::OB_SUCCESS;
  }
  INHERIT_TO_STRING_KV("BasicDag", ObBasicDag, K_(is_inited), K_(type), K_(id), K(task_list_.get_size()));

private:
  ObOperator *op_;
};

class ObFatherDagNet : public ObIDagNet
{
public:
  ObFatherDagNet() :
    ObIDagNet(ObDagNetType::DAG_NET_TYPE_MIGRATION),
    id_(ObTimeUtility::current_time() + random()),
    op_()
  {}
  void init(int64_t id) { id_ = id; }
  bool is_valid() const { return true; }
  virtual int start_running() override
  {
    int ret = OB_SUCCESS;
    ObFatherPrepareDag *prepare_dag = NULL;
    ObFatherFinishDag *finish_dag = NULL;

    // create dag and connections
    if (OB_FAIL(MTL(ObTenantDagScheduler*)->alloc_dag(prepare_dag))) {
      COMMON_LOG(WARN, "Fail to create dag", K(ret));
    } else if (FALSE_IT(prepare_dag->init(op_))) {
    } else if (OB_FAIL(prepare_dag->create_first_task())) {
      COMMON_LOG(WARN, "Fail to create first task", K(ret));
    } else if (OB_FAIL(add_dag_into_dag_net(*prepare_dag))) { // add first dag into this dag_net
      COMMON_LOG(WARN, "Fail to add dag into dag_net", K(ret));
    } else   if (OB_FAIL(MTL(ObTenantDagScheduler*)->alloc_dag(finish_dag))) {
      COMMON_LOG(WARN, "Fail to create dag", K(ret));
    } else if (FALSE_IT(finish_dag->init(op_))) {
    } else if (OB_FAIL(finish_dag->create_first_task())) {
      COMMON_LOG(WARN, "Fail to create first task", K(ret));
    } else if (OB_FAIL(prepare_dag->add_child(*finish_dag))) {
      COMMON_LOG(WARN, "Fail to add child", K(ret), KPC(prepare_dag), KPC(finish_dag));
    } else if (OB_FAIL(MTL(ObTenantDagScheduler*)->add_dag(prepare_dag))
        || OB_FAIL(MTL(ObTenantDagScheduler*)->add_dag(finish_dag))) {
      COMMON_LOG(WARN, "Fail to add dag into dag_scheduler", K(ret));
    } else {
      // add all dags into dag_scheduler
      COMMON_LOG(INFO, "success to add dag into dag_scheduler", K(ret));
    }
    return ret;
  }
  virtual int64_t hash() const { return murmurhash(&id_, sizeof(id_), 0);}
  virtual bool operator == (const ObIDagNet &other) const
  {
    bool bret = false;
    if (get_type() == other.get_type()) {
      const ObFatherDagNet &dag = static_cast<const ObFatherDagNet &>(other);
      bret = dag.id_ == id_;
    }
    return bret;
  }
  virtual int fill_comment(char *buf, const int64_t buf_len) const override
  { UNUSEDx(buf, buf_len); return OB_SUCCESS; }
  virtual int fill_dag_net_key(char *buf, const int64_t buf_len) const override
  { UNUSEDx(buf, buf_len); return OB_SUCCESS; }
  virtual bool is_ha_dag_net() const override { return false; }
  INHERIT_TO_STRING_KV("ObIDagNet", ObIDagNet, K_(type), K_(id));
  virtual int clear_dag_net_ctx() override
  {
    int ret = OB_SUCCESS;
    int tmp_ret = OB_SUCCESS;
    ObTenantDagWorker *worker = ObTenantDagWorker::self();

    if (worker != nullptr) {
      ObITask * task = worker->get_task();
      ObTenantDagWorker::DagWorkerStatus status = worker->get_status();
      EXPECT_EQ(status, ObTenantDagWorker::DWS_FREE);
      EXPECT_EQ(task, nullptr);
      if(OB_TMP_FAIL(worker->yield())) {
        if (tmp_ret == OB_CANCELED) {
          ret = OB_SUCCESS;
        }
      }
      COMMON_LOG(WARN, "call worker->yiled() after task is destoryed", K(ret), K(tmp_ret));
    } else {
      COMMON_LOG(WARN, "worker in this thread is nullptr");
    }

    return ret;
  }
private:

  int64_t id_;
  ObOperator op_;
  DISALLOW_COPY_AND_ASSIGN(ObFatherDagNet);
};

int ObFatherFinishTask::process()
{
  EXPECT_EQ(0, op_->num_);
  return OB_SUCCESS;
}

TEST_F(TestDagScheduler, test_basic_dag_net)
{
  ObTenantDagScheduler *scheduler = MTL(ObTenantDagScheduler*);
  ASSERT_TRUE(nullptr != scheduler);
  ObDagWarningHistoryManager* manager = MTL(ObDagWarningHistoryManager *);
  ASSERT_TRUE(nullptr != manager);
  EXPECT_EQ(OB_SUCCESS, MTL(ObDagWarningHistoryManager *)->init(true, MTL_ID(), "DagWarnHis"));

  for (int i = 0; i < 2; ++i) {
    EXPECT_EQ(OB_SUCCESS, scheduler->create_and_add_dag_net<ObFatherDagNet>(nullptr));
  }

  wait_scheduler();
  EXPECT_EQ(0, MTL(ObDagWarningHistoryManager *)->size());
}

class ObFatherWithRetryDagNet : public ObFatherDagNet
{
public:
  virtual int start_running() override
  {
    int ret = OB_SUCCESS;
    ObFatherPrepareDag *prepare_dag = NULL;
    ObFatherFinishDag *finish_dag = NULL;

    // create dag and connections
    if (OB_FAIL(MTL(ObTenantDagScheduler*)->alloc_dag(prepare_dag))) {
      COMMON_LOG(WARN, "Fail to create dag", K(ret));
    } else if (FALSE_IT(prepare_dag->init(op_))) {
    } else if (OB_FAIL(prepare_dag->create_first_task())) {
      COMMON_LOG(WARN, "Fail to create first task", K(ret));
    } else if (OB_FAIL(add_dag_into_dag_net(*prepare_dag))) { // add first dag into this dag_net
      COMMON_LOG(WARN, "Fail to add dag into dag_net", K(ret));
    } else if (OB_FAIL(MTL(ObTenantDagScheduler*)->alloc_dag(finish_dag))) {
      COMMON_LOG(WARN, "Fail to create dag", K(ret));
    } else if (FALSE_IT(finish_dag->init(op_))) {
    } else if (OB_FAIL(finish_dag->create_first_task())) {
      COMMON_LOG(WARN, "Fail to create first task", K(ret));
    } else if (OB_FAIL(prepare_dag->add_child(*finish_dag))) {
      COMMON_LOG(WARN, "Fail to add child", K(ret), KPC(prepare_dag), KPC(finish_dag));
    }
    for (int i = 0; OB_SUCC(ret) && i < 1; ++i) {
      ObDagRetryDag *dag = NULL;
      ObRetryDagInitParam param;
      const int64_t str_len = 100;
      char str[str_len];
      param.id_ = i + 1;
      snprintf(str, str_len, "Hello OceanBase_%d", i);
      param.str_ = ObString(str);
      if (OB_FAIL(MTL(ObTenantDagScheduler*)->create_dag(&param, dag))) {
        COMMON_LOG(WARN, "failed to create first task", K(ret));
      } else {
        dag->set_max_retry_times(3);
        if (OB_FAIL(prepare_dag->add_child(*dag))) {
          COMMON_LOG(WARN, "Fail to add child", K(ret), KPC(prepare_dag), KPC(finish_dag));
        } else if (OB_FAIL(MTL(ObTenantDagScheduler*)->add_dag(dag))) {
          COMMON_LOG(WARN, "Fail to add dag into dag_scheduler", K(ret));
        }
      }
      EXPECT_EQ(OB_SUCCESS, ret);
    }

    if (OB_FAIL(MTL(ObTenantDagScheduler*)->add_dag(prepare_dag))
        || OB_FAIL(MTL(ObTenantDagScheduler*)->add_dag(finish_dag))) {
      COMMON_LOG(WARN, "Fail to add dag into dag_scheduler", K(ret));
    } else {
      // add all dags into dag_scheduler
      COMMON_LOG(INFO, "success to add dag into dag_scheduler", K(ret));
    }
    return ret;
  }
};

TEST_F(TestDagScheduler, test_basic_dag_net_with_one_retry_dag)
{
  ObTenantDagScheduler *scheduler = MTL(ObTenantDagScheduler*);
  ASSERT_TRUE(nullptr != scheduler);
  ObDagWarningHistoryManager* manager = MTL(ObDagWarningHistoryManager *);
  ASSERT_TRUE(nullptr != manager);
  EXPECT_EQ(OB_SUCCESS, MTL(ObDagWarningHistoryManager *)->init(true, MTL_ID(), "DagWarnHis"));

  for (int i = 0; i < 1; ++i) {
    EXPECT_EQ(OB_SUCCESS, scheduler->create_and_add_dag_net<ObFatherWithRetryDagNet>(nullptr));
  }

  wait_scheduler();
  EXPECT_EQ(0, MTL(ObDagWarningHistoryManager *)->size());
}

/*
 * check task retry
 * */
static int64_t generate_cnt = 1;
class ObGenerateFailedTask : public ObITask
{
public:
  ObGenerateFailedTask() : ObITask(ObITaskType::TASK_TYPE_NORMAL_MINOR_MERGE), cnt_(0), seq_(0) {}
  virtual ~ObGenerateFailedTask() {}
  virtual int process()
  {
    return OB_SUCCESS;
  }
  void init(int64_t seq) { seq_ = seq; }
  virtual int generate_next_task(ObITask *&next_task)
  {
    int ret = OB_SUCCESS;
    if (seq_ >= MAX_SEQ) {
      ret = OB_ITER_END;
      COMMON_LOG(INFO, "generate task end", K_(seq));
    } else if (generate_cnt++ < 2) {
      ret = OB_ERR_UNEXPECTED;
    } else {
      ObIDag *dag = get_dag();
      ObDagRetryTask *ntask = NULL;
      if (NULL == dag) {
        ret = OB_ERR_UNEXPECTED;
        COMMON_LOG(WARN, "dag is null", K(ret));
      } else if (OB_FAIL(dag->alloc_task(ntask))) {
        COMMON_LOG(WARN, "failed to alloc task", K(ret));
      } else if (NULL == ntask) {
        ret = OB_ERR_UNEXPECTED;
        COMMON_LOG(WARN, "task is null", K(ret));
      } else {
        next_task = ntask;
      }
    }
    return ret;
  }
private:
  const int64_t MAX_SEQ = 2;
  int cnt_;
  int64_t seq_;
};

class ObGenerateFailedDag : public ObBasicDag
{
public:
  virtual int create_first_task() override
  {
    int ret = OB_SUCCESS;
    ObGenerateFailedTask *task = NULL;
    if (OB_FAIL(alloc_task(task))) {
      COMMON_LOG(WARN, "Fail to alloc task", K(ret));
    } else if (OB_FAIL(add_task(*task))) {
      COMMON_LOG(WARN, "Fail to add task", K(ret));
    } else {
      task->init(0);
    }
    return common::OB_SUCCESS;
  }
  int inner_reset_status_for_retry() { return OB_SUCCESS; }
  INHERIT_TO_STRING_KV("ObIDag", ObIDag, K_(is_inited), K_(type), K_(id), K(task_list_.get_size()), K_(dag_ret));
};

TEST_F(TestDagScheduler, test_generage_task_failed)
{
  ObTenantDagScheduler *scheduler = MTL(ObTenantDagScheduler*);
  ASSERT_TRUE(nullptr != scheduler);
  ObDagWarningHistoryManager* manager = MTL(ObDagWarningHistoryManager *);
  ASSERT_TRUE(nullptr != manager);
  ASSERT_EQ(OB_SUCCESS, MTL(ObDagWarningHistoryManager *)->init(true, MTL_ID(), "DagWarnHis"));

  int ret = OB_SUCCESS;
  ObGenerateFailedDag *dag = nullptr;
  for (int i = 0; i < 1; ++i) {
    if (OB_FAIL(scheduler->create_dag(nullptr, dag))) {
      COMMON_LOG(WARN, "failed to create dag", K(ret));
    } else if (FALSE_IT(dag->set_max_retry_times(7))) {
    } else if (OB_FAIL(scheduler->add_dag(dag))) {
      COMMON_LOG(WARN, "failed to add dag", K(ret));
    }
    EXPECT_EQ(OB_SUCCESS, ret);
  }

  wait_scheduler();
  ASSERT_EQ(1, MTL(ObDagWarningHistoryManager *)->size());
}

//generate next dag

class ObGenerateNextDagCtx
{
public:
  ObGenerateNextDagCtx()
    : index_(0)
  {
  }

  virtual ~ObGenerateNextDagCtx() {}

  int get_next_index(int64_t &index)
  {
    int ret = OB_SUCCESS;
    index = 0;
    common::SpinWLockGuard guard(lock_);
    if (index_ >= MAX_INDEX) {
      ret = OB_ITER_END;
    } else {
      index = index_;
      index_++;
    }
    return ret;
  }

  bool is_empty() const
  {
    common::SpinRLockGuard guard(lock_);
    return index_ == MAX_INDEX;
  }

private:
  const int64_t MAX_INDEX = 10;
  common::SpinRWLock lock_;
  int64_t index_;
};

class ObFinishGeneratNextDag : public ObBasicDag
{
public:
  ObFinishGeneratNextDag() :
    ObBasicDag(),
    is_inited_(false),
    ctx_()
  {}

  int init()
  {
    int ret = OB_SUCCESS;
    if (is_inited_) {
      ret = OB_INIT_TWICE;
      COMMON_LOG(WARN, "start generate next dag init twice", K(ret));
    } else {
      id_ = FINISH_DAG_ID;
      is_inited_ = true;
    }
    return ret;
  }

  virtual int create_first_task() override
  {
    int ret = OB_SUCCESS;
    ObFakeTask *task = NULL;
    if (OB_FAIL(alloc_task(task))) {
      COMMON_LOG(WARN, "Fail to alloc task", K(ret));
    } else if (OB_FAIL(add_task(*task))) {
      COMMON_LOG(WARN, "Fail to add task", K(ret));
    }
    return common::OB_SUCCESS;
  }
  ObGenerateNextDagCtx *get_ctx() { return &ctx_; }

private:
  const int64_t FINISH_DAG_ID = 1000001;
  bool is_inited_;
  ObGenerateNextDagCtx ctx_;
  DISALLOW_COPY_AND_ASSIGN(ObFinishGeneratNextDag);
};

class ObDagGenerateNextDag : public ObBasicDag
{
public:
  ObDagGenerateNextDag() :
    ObBasicDag(),
    is_inited_(false),
    ctx_(nullptr)
  {}

  int init(const int64_t id, ObGenerateNextDagCtx *ctx)
  {
    int ret = OB_SUCCESS;
    if (is_inited_) {
      ret = OB_INIT_TWICE;
      COMMON_LOG(WARN, "dag generate next dag init twice", K(ret));
    } else if (id < 0) {
      ret = OB_INVALID_ARGUMENT;
      COMMON_LOG(WARN, "init dag generate next dag get invalid argument", K(ret), K(id));
    } else {
      id_ = id;
      ctx_ = ctx;
      is_inited_ = true;
      COMMON_LOG(INFO, "succeed init next dag", K(id));
    }
    return ret;
  }

  virtual int generate_next_dag(share::ObIDag *&dag)
  {
    int ret = OB_SUCCESS;
    dag = nullptr;
    ObTenantDagScheduler *scheduler = nullptr;
    int64_t next_id = 0;
    ObDagGenerateNextDag *next_dag = nullptr;

    if (!is_inited_) {
      ret = OB_NOT_INIT;
      COMMON_LOG(WARN, "generate next dag do not init", K(ret));
    } else if (OB_FAIL(ctx_->get_next_index(next_id))) {
      if (OB_ITER_END == ret) {
        //do nothing
      } else {
        COMMON_LOG(WARN, "failed to get next index", K(ret));
      }
    } else if (OB_ISNULL(scheduler = MTL(ObTenantDagScheduler*))) {
      ret = OB_ERR_UNEXPECTED;
      COMMON_LOG(WARN, "failed to get ObTenantDagScheduler from MTL", K(ret));
    } else if (OB_FAIL(scheduler->alloc_dag(next_dag))) {
      COMMON_LOG(WARN, "failed to alloc next_dag", K(ret));
    } else if (OB_FAIL(next_dag->init(next_id, ctx_))) {
      COMMON_LOG(WARN, "failed to init tablet migration dag", K(ret));
    } else if (OB_FAIL(next_dag->create_first_task())) {
      COMMON_LOG(WARN, "failed to create first task", K(ret));
    } else {
      dag = next_dag;
      next_dag = nullptr;
    }

    if (OB_NOT_NULL(next_dag)) {
      scheduler->free_dag(*next_dag);
    }
    return ret;
  }

  virtual int create_first_task() override
  {
    int ret = OB_SUCCESS;
    ObFakeTask *task = NULL;
    if (OB_FAIL(alloc_task(task))) {
      COMMON_LOG(WARN, "Fail to alloc task", K(ret));
    } else if (OB_FAIL(add_task(*task))) {
      COMMON_LOG(WARN, "Fail to add task", K(ret));
    }
    return common::OB_SUCCESS;
  }

  INHERIT_TO_STRING_KV("ObIDag", ObIDag, K_(is_inited), K_(type), K_(id), K(task_list_.get_size()), K_(dag_ret));
private:
  bool is_inited_;
  ObGenerateNextDagCtx *ctx_;
  DISALLOW_COPY_AND_ASSIGN(ObDagGenerateNextDag);
};

class ObStartGeneratNextDagTask : public ObITask
{
public:
  ObStartGeneratNextDagTask() : ObITask(ObITaskType::TASK_TYPE_NORMAL_MINOR_MERGE), is_inited_(false), id_(0) {}
  virtual ~ObStartGeneratNextDagTask() {}
  virtual int process()
  {
    int ret = OB_SUCCESS;
    if (!is_inited_) {
      ret = OB_NOT_INIT;
      COMMON_LOG(WARN, "generate next dag task do not init", K(ret));
    } else {
      ObDagGenerateNextDag *next_dag = nullptr;
      ObFinishGeneratNextDag *finish_dag = nullptr;
      ObGenerateNextDagCtx *ctx = nullptr;
      ObTenantDagScheduler *scheduler = nullptr;
      int64_t id = 0;

      if (!is_inited_) {
        ret = OB_NOT_INIT;
        COMMON_LOG(WARN, "start prepare migration task do not init", K(ret));
      } else if (OB_ISNULL(scheduler = MTL(ObTenantDagScheduler*))) {
        ret = OB_ERR_UNEXPECTED;
        COMMON_LOG(WARN, "failed to get ObTenantDagScheduler from MTL", K(ret));
      } else if (OB_FAIL(scheduler->alloc_dag(finish_dag))) {
        COMMON_LOG(WARN, "failed to alloc finish backfill tx migration dag ", K(ret));
      } else if (OB_FAIL(finish_dag->init())) {
        COMMON_LOG(WARN, "failed to init data tablets migration dag", K(ret));
      } else if (OB_ISNULL(ctx = finish_dag->get_ctx())) {
        ret = OB_ERR_UNEXPECTED;
        COMMON_LOG(WARN, "backfill tx ctx should not be NULL", K(ret), KP(ctx));
      } else if (ctx->is_empty()) {
        if (OB_FAIL(this->get_dag()->add_child(*finish_dag))) {
          COMMON_LOG(WARN, "failed to add finish_dag as chilid", K(ret));
        }
      } else {
        if (OB_FAIL(ctx->get_next_index(id))) {
          COMMON_LOG(WARN, "failed to get tablet id", K(ret));
        } else if (OB_FAIL(scheduler->alloc_dag(next_dag))) {
          COMMON_LOG(WARN, "failed to alloc next_dag", K(ret));
        } else if (OB_FAIL(next_dag->init(id, ctx))) {
          COMMON_LOG(WARN, "failed to init next_dag", K(ret));
        } else if (OB_FAIL(this->get_dag()->add_child(*next_dag))) {
          COMMON_LOG(WARN, "failed to add next_dag as chilid", K(ret));
        } else if (OB_FAIL(next_dag->create_first_task())) {
          COMMON_LOG(WARN, "failed to create first task", K(ret));
        } else if (OB_FAIL(next_dag->add_child(*finish_dag))) {
          COMMON_LOG(WARN, "failed to add child dag", K(ret));
        } else if (OB_FAIL(scheduler->add_dag(next_dag))) {
          COMMON_LOG(WARN, "failed to add tablet backfill tx dag", K(ret));
          if (OB_SIZE_OVERFLOW != ret && OB_EAGAIN != ret) {
            COMMON_LOG(WARN, "Fail to add task", K(ret));
            ret = OB_EAGAIN;
          }
        }
      }

      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(finish_dag->create_first_task())) {
        COMMON_LOG(WARN, "failed to create first task", K(ret));
      } else if (OB_FAIL(scheduler->add_dag(finish_dag))) {
        COMMON_LOG(WARN, "failed to add finish_dag", K(ret));
        int tmp_ret = OB_SUCCESS;
        if (OB_SIZE_OVERFLOW != ret && OB_EAGAIN != ret) {
          COMMON_LOG(WARN, "Fail to add task", K(ret));
          ret = OB_EAGAIN;
        }

        if (OB_NOT_NULL(next_dag)) {
          if (OB_SUCCESS != (tmp_ret = scheduler->cancel_dag(next_dag))) {
            COMMON_LOG(WARN, "failed to cancel next_dag", K(ret));
          }
          next_dag = nullptr;
        }
      } else {
        next_dag = nullptr;
        finish_dag = nullptr;
      }

      if (OB_FAIL(ret)) {
        if (OB_NOT_NULL(next_dag)) {
          scheduler->free_dag(*next_dag);
        }

        if (OB_NOT_NULL(finish_dag)) {
          scheduler->free_dag(*finish_dag);
        }
      }
    }

    return ret;
  }
  int init(const int64_t id)
  {
    int ret = OB_SUCCESS;
    if (is_inited_) {
      ret = OB_INIT_TWICE;
      COMMON_LOG(WARN, "generate next dag task init twice", K(ret));
    } else if (id < 0) {
      ret = OB_INVALID_ARGUMENT;
      COMMON_LOG(WARN, "init generate next dag task get invalid argument", K(ret), K(id));
    } else {
      id_ = id;
      is_inited_ = true;
    }
    return ret;
  }
private:
  bool is_inited_;
  int64_t id_;
};

class ObStartGenerateNextDag : public ObBasicDag
{
public:
  ObStartGenerateNextDag() :
    ObBasicDag(),
    is_inited_(false)
  {}

  int init()
  {
    int ret = OB_SUCCESS;
    if (is_inited_) {
      ret = OB_INIT_TWICE;
      COMMON_LOG(WARN, "start generate next dag init twice", K(ret));
    } else {
      id_ = START_DAG_ID;
      is_inited_ = true;
    }
    return ret;
  }

  virtual int create_first_task() override
  {
    int ret = OB_SUCCESS;
    ObStartGeneratNextDagTask *task = NULL;
    if (OB_FAIL(alloc_task(task))) {
      COMMON_LOG(WARN, "Fail to alloc task", K(ret));
    } else if (OB_FAIL(task->init(id_))) {
      COMMON_LOG(WARN, "failed to init task", K(ret));
    } else if (OB_FAIL(add_task(*task))) {
      COMMON_LOG(WARN, "Fail to add task", K(ret));
    }
    return common::OB_SUCCESS;
  }

private:
  const int64_t START_DAG_ID = 1000000;
  bool is_inited_;
  DISALLOW_COPY_AND_ASSIGN(ObStartGenerateNextDag);
};

TEST_F(TestDagScheduler, generate_next_dag)
{
  ObTenantDagScheduler *scheduler = MTL(ObTenantDagScheduler*);
  ASSERT_TRUE(nullptr != scheduler);
  ObDagWarningHistoryManager* manager = MTL(ObDagWarningHistoryManager *);
  ASSERT_TRUE(nullptr != manager);
  EXPECT_EQ(OB_SUCCESS, MTL(ObDagWarningHistoryManager *)->init(true, MTL_ID(), "DagWarnHis"));

  ObStartGenerateNextDag *dag = nullptr;
  EXPECT_EQ(OB_SUCCESS, scheduler->alloc_dag(dag));
  EXPECT_EQ(OB_SUCCESS, dag->init());
  EXPECT_EQ(OB_SUCCESS, dag->create_first_task());
  EXPECT_EQ(OB_SUCCESS, scheduler->add_dag(dag));

  wait_scheduler();
  EXPECT_EQ(0, MTL(ObDagWarningHistoryManager *)->size());
}

class ObCreateChildTask : public ObITask
{
public:
  ObCreateChildTask() : ObITask(ObITaskType::TASK_TYPE_UT), cnt_(0){}
  virtual ~ObCreateChildTask() {}
  virtual int process()
  {
    int ret = OB_SUCCESS;
    int tmp_ret = OB_SUCCESS;
    ObArray<ObIDag *> new_dag_array;
    ObIDag *parent = this->get_dag();
    ObIDagNet *dag_net = get_dag()->get_dag_net();
    ObWaitDag *new_dag = nullptr;
    for (int64_t i = 0; OB_SUCC(ret) && i < TestDagScheduler::MAX_DAG_CNT + 5; ++i) {
      if (OB_FAIL(MTL(ObTenantDagScheduler*)->alloc_dag(new_dag))) {
        COMMON_LOG(WARN, "failed to alloc tablet migration dag ", K(ret));
      } else if (FALSE_IT(new_dag->init(i))) {
      } else if (OB_FAIL(dag_net->add_dag_into_dag_net(*new_dag))) {
        COMMON_LOG(WARN, "failed to add dag into dag net", K(ret), K(i), K(new_dag));
      } else if (OB_FAIL(parent->add_child_without_inheritance(*new_dag))) {
        COMMON_LOG(WARN, "failed to add child dag", K(ret), K(i), K(new_dag));
      } else if (OB_FAIL(new_dag->create_first_task())) {
        COMMON_LOG(WARN, "failed to create first task", K(ret), K(i), K(new_dag));
      } else if (OB_FAIL(MTL(ObTenantDagScheduler*)->add_dag(new_dag))) {
        COMMON_LOG(WARN, "failed to add tablet migration dag", K(ret), K(*new_dag));
        if (OB_SIZE_OVERFLOW != ret && OB_EAGAIN != ret) {
          COMMON_LOG(WARN, "Fail to add task", K(ret));
          ret = OB_EAGAIN;
        }
      } else if (OB_FAIL(new_dag_array.push_back(new_dag))) {
        COMMON_LOG(WARN, "failed to push tablet migration dag into array", K(ret), K(i), K(new_dag));
      }
    }
    if (OB_FAIL(ret)) {
      if (OB_NOT_NULL(new_dag)) {
        new_dag->reset_node();
        if (OB_SUCCESS != (tmp_ret = dag_net->erase_dag_from_dag_net(*new_dag))) {
          COMMON_LOG(WARN, "failed to erase dag from dag net", K(tmp_ret), KPC(new_dag));
        }
        MTL(ObTenantDagScheduler*)->free_dag(*new_dag);
        new_dag = nullptr;
      }

      for (int64_t i = 0; i < new_dag_array.count(); ++i) {
        ObIDag *dag = new_dag_array.at(i);
        dag->reset_node();
        if (OB_SUCCESS != (tmp_ret = dag_net->erase_dag_from_dag_net(*dag))) {
          COMMON_LOG(WARN, "failed to erase dag from dag net", K(tmp_ret), KPC(dag));
        }

        if (OB_SUCCESS != (tmp_ret = MTL(ObTenantDagScheduler*)->cancel_dag(dag))) {
          COMMON_LOG(WARN, "failed to cancel dag", K(tmp_ret), K(*dag));
          ob_abort();
        } else {
          COMMON_LOG(INFO, "success to cancel dag", K(tmp_ret), K(i));
        }
      }
      new_dag_array.reset();
      this->get_dag()->reset_node();
    }
    return ret;
  }
private:
  const static int64_t FINISH_CNT = 5;
  int cnt_;
};

class ObCreateChildDag : public ObBasicDag
{
public:
  ObCreateChildDag() :
    ObBasicDag()
  {}
  virtual int create_first_task() override
  {
    int ret = OB_SUCCESS;
    ObCreateChildTask *task = NULL;
    if (OB_FAIL(alloc_task(task))) {
      COMMON_LOG(WARN, "Fail to alloc task", K(ret));
    } else if (OB_FAIL(add_task(*task))) {
      COMMON_LOG(WARN, "Fail to add task", K(ret));
    }
    return common::OB_SUCCESS;
  }
private:
  const int64_t MAX_RETRY_TIMES = 20;
  const int64_t MAX_CHECK_INTERVAL = 1000L * 100L; // 100ms

  DISALLOW_COPY_AND_ASSIGN(ObCreateChildDag);
};

class ObCreateDagNet : public ObFatherDagNet
{
public:
  virtual int start_running() override
  {
    int ret = OB_SUCCESS;
    ObCreateChildDag *prepare_dag = NULL;
    ObFatherFinishDag *finish_dag = NULL;

    // create dag and connections
    if (OB_FAIL(MTL(ObTenantDagScheduler*)->alloc_dag(prepare_dag))) {
      COMMON_LOG(WARN, "Fail to create dag", K(ret));
    } else if (OB_FAIL(prepare_dag->create_first_task())) {
      COMMON_LOG(WARN, "Fail to create first task", K(ret));
    } else if (OB_FAIL(add_dag_into_dag_net(*prepare_dag))) { // add first dag into this dag_net
      COMMON_LOG(WARN, "Fail to add dag into dag_net", K(ret));
    } else if (OB_FAIL(MTL(ObTenantDagScheduler*)->alloc_dag(finish_dag))) {
      COMMON_LOG(WARN, "Fail to create dag", K(ret));
    } else if (FALSE_IT(finish_dag->init(op_))) {
    } else if (OB_FAIL(finish_dag->create_first_task())) {
      COMMON_LOG(WARN, "Fail to create first task", K(ret));
    } else if (OB_FAIL(prepare_dag->add_child(*finish_dag))) {
      COMMON_LOG(WARN, "Fail to add child", K(ret), KPC(prepare_dag), KPC(finish_dag));
    }
    EXPECT_EQ(OB_SUCCESS, ret);

    if (OB_FAIL(MTL(ObTenantDagScheduler*)->add_dag(prepare_dag))
        || OB_FAIL(MTL(ObTenantDagScheduler*)->add_dag(finish_dag))) {
      COMMON_LOG(WARN, "Fail to add dag into dag_scheduler", K(ret));
    } else {
      // add all dags into dag_scheduler
      COMMON_LOG(INFO, "success to add dag into dag_scheduler", K(ret));
    }

    return ret;
  }
};

TEST_F(TestDagScheduler, test_add_dag_failed_in_generate_dag_net)
{
  ObTenantDagScheduler *scheduler = MTL(ObTenantDagScheduler*);
  ASSERT_TRUE(nullptr != scheduler);
  ObDagWarningHistoryManager* manager = MTL(ObDagWarningHistoryManager *);
  ASSERT_TRUE(nullptr != manager);
  ASSERT_EQ(OB_SUCCESS, MTL(ObDagWarningHistoryManager *)->init(true, MTL_ID(), "DagWarnHis"));

  ASSERT_EQ(OB_SUCCESS, scheduler->create_and_add_dag_net<ObCreateDagNet>(nullptr));

  wait_scheduler();
  ASSERT_EQ(1, MTL(ObDagWarningHistoryManager *)->size());
}

class ObFreeDagNet: public ObFatherDagNet
{
public:
  virtual int start_running() override
  {
    int ret = OB_SUCCESS;
    ObCreateChildDag *prepare_dag = NULL;
    ObFatherFinishDag *finish_dag = NULL;

    // create dag and connections
    if (OB_FAIL(MTL(ObTenantDagScheduler*)->alloc_dag(prepare_dag))) {
      COMMON_LOG(WARN, "Fail to create dag", K(ret));
    } else if (OB_FAIL(prepare_dag->create_first_task())) {
      COMMON_LOG(WARN, "Fail to create first task", K(ret));
    } else if (OB_FAIL(add_dag_into_dag_net(*prepare_dag))) { // add first dag into this dag_net
      COMMON_LOG(WARN, "Fail to add dag into dag_net", K(ret));
    } else if (OB_FAIL(MTL(ObTenantDagScheduler*)->alloc_dag(finish_dag))) {
      COMMON_LOG(WARN, "Fail to create dag", K(ret));
    } else if (FALSE_IT(finish_dag->init(op_))) {
    } else if (OB_FAIL(finish_dag->create_first_task())) {
      COMMON_LOG(WARN, "Fail to create first task", K(ret));
    } else if (OB_FAIL(prepare_dag->add_child(*finish_dag))) {
      COMMON_LOG(WARN, "Fail to add child", K(ret), KPC(prepare_dag), KPC(finish_dag));
    }
    EXPECT_EQ(OB_SUCCESS, ret);
    MTL(ObTenantDagScheduler*)->free_dag(*finish_dag);
    COMMON_LOG(INFO, "free dag", K(ret), KPC(prepare_dag), KPC(this));
    EXPECT_EQ(dag_record_map_.size(), 1);
    EXPECT_EQ(0, prepare_dag->children_.count());

    MTL(ObTenantDagScheduler*)->free_dag(*prepare_dag);
    COMMON_LOG(INFO, "free dag", K(ret), KPC(this));
    EXPECT_EQ(dag_record_map_.size(), 0);

    return OB_ERR_UNEXPECTED;
  }
};

TEST_F(TestDagScheduler, test_free_dag_func)
{
  ObTenantDagScheduler *scheduler = MTL(ObTenantDagScheduler*);
  ASSERT_TRUE(nullptr != scheduler);
  ObDagWarningHistoryManager* manager = MTL(ObDagWarningHistoryManager *);
  ASSERT_TRUE(nullptr != manager);
  EXPECT_EQ(OB_SUCCESS, MTL(ObDagWarningHistoryManager *)->init(true, MTL_ID(), "DagWarnHis"));

  EXPECT_EQ(OB_SUCCESS, scheduler->create_and_add_dag_net<ObFreeDagNet>(nullptr));

  wait_scheduler();
  EXPECT_EQ(0, MTL(ObDagWarningHistoryManager *)->size());
}

class ObCancelDag : public ObBasicDag
{
public:
  ObCancelDag() : can_schedule_(false) {}
  virtual int create_first_task() override
  {
    int ret = OB_SUCCESS;
    ObWaitTask *task = NULL;
    if (OB_FAIL(alloc_task(task))) {
      COMMON_LOG(WARN, "Fail to alloc task", K(ret));
    } else if (OB_FAIL(add_task(*task))) {
      COMMON_LOG(WARN, "Fail to add task", K(ret));
    }
    return common::OB_SUCCESS;
  }
  virtual bool check_can_schedule() override
  {
    return can_schedule_;
  }

  bool can_schedule_;
};

class ObCancelDagNet: public ObFatherDagNet
{
public:
  virtual int start_running() override
  {
    int ret = OB_SUCCESS;
    ObCancelDag *dag = NULL;

    for (int i = 0; OB_SUCC(ret) && i < 3; ++i) {
      // create dag and connections
      if (OB_FAIL(MTL(ObTenantDagScheduler*)->alloc_dag(dag))) {
        COMMON_LOG(WARN, "Fail to create dag", K(ret));
      } else if (OB_FAIL(dag->create_first_task())) {
        COMMON_LOG(WARN, "Fail to create first task", K(ret));
      } else if (OB_FAIL(add_dag_into_dag_net(*dag))) { // add first dag into this dag_net
        COMMON_LOG(WARN, "Fail to add dag into dag_net", K(ret));
      } else if (OB_FAIL(MTL(ObTenantDagScheduler*)->add_dag(dag))) {
        COMMON_LOG(WARN, "failed to add dag", K(ret), K(dag));
      }
      EXPECT_EQ(OB_SUCCESS, ret);
    }
    return ret;
  }
  void get_dag_list(ObIArray<ObIDag *> &dag_array) const
  {
    for (DagRecordMap::const_iterator iter = dag_record_map_.begin();
        iter != dag_record_map_.end(); ++iter) {
      ObDagRecord *dag_record = iter->second;
      dag_array.push_back(dag_record->dag_ptr_);
    }
  }
};

TEST_F(TestDagScheduler, test_cancel_dag_func)
{
  ObTenantDagScheduler *scheduler = MTL(ObTenantDagScheduler*);
  ASSERT_TRUE(nullptr != scheduler);
  ObDagWarningHistoryManager* manager = MTL(ObDagWarningHistoryManager *);
  ASSERT_TRUE(nullptr != manager);
  EXPECT_EQ(OB_SUCCESS, MTL(ObDagWarningHistoryManager *)->init(true, MTL_ID(), "DagWarnHis"));

  EXPECT_EQ(OB_SUCCESS, scheduler->create_and_add_dag_net<ObCancelDagNet>(nullptr));
  ObIDagNet *tmp_dag_net = nullptr;
  EXPECT_EQ(OB_SUCCESS, scheduler->get_first_dag_net(tmp_dag_net));
  EXPECT_NE(nullptr, tmp_dag_net);

  ObCancelDagNet *dag_net = static_cast<ObCancelDagNet*>(tmp_dag_net);

  while (scheduler->get_cur_dag_cnt() < 3) {
    usleep(100);
  }

  ObArray<ObIDag *> dag_array;
  dag_net->get_dag_list(dag_array);

  for (int i = 0; i < dag_array.count(); ++i) {
    scheduler->cancel_dag(dag_array[i]);
  }

  wait_scheduler();
  EXPECT_EQ(0, MTL(ObDagWarningHistoryManager *)->size());
}



TEST_F(TestDagScheduler, test_cancel_dag_net_func)
{
  int ret = OB_SUCCESS;
  ObTenantDagScheduler *scheduler = MTL(ObTenantDagScheduler*);
  ASSERT_TRUE(nullptr != scheduler);
  EXPECT_EQ(OB_SUCCESS, scheduler->create_and_add_dag_net<ObCancelDagNet>(nullptr));
  ObIDagNet *tmp_dag_net = nullptr;
  EXPECT_EQ(OB_SUCCESS, scheduler->get_first_dag_net(tmp_dag_net));
  EXPECT_NE(nullptr, tmp_dag_net);

  ObCancelDagNet *dag_net = static_cast<ObCancelDagNet*>(tmp_dag_net);

  while (scheduler->get_cur_dag_cnt() < 3) {
    usleep(100);
  }

  ObArray<ObIDag *> dag_array;
  dag_net->get_dag_list(dag_array);
  ret = scheduler->cancel_dag_net(dag_net->get_dag_id());

  for (int i = 0; i < dag_array.count(); ++i) {
    ObCancelDag *dag = static_cast<ObCancelDag *>(dag_array[i]);
    dag->can_schedule_ = true;
  }

  EXPECT_EQ(OB_SUCCESS, ret);
  wait_scheduler();
  EXPECT_EQ(0, MTL(ObDagWarningHistoryManager *)->size());
}

class ObCancelWaitingDagInDagNet: public ObCancelDagNet
{
public:
  ObCancelWaitingDagInDagNet()
  : ObCancelDagNet(),
    target_dag_cnt_(4),
    first_dag_(nullptr)
  {}

  virtual int start_running() override
  {
    int ret = OB_SUCCESS;
    ObCancelDag *dag = nullptr;
    ObCancelDag *last_dag = nullptr;
    // Dag1 -> Dag2/Dag3
    // Dag2
    // Dag3 -> Dag2/Dag4
    // Dag4 -> Dag2
    for (int64_t i = 0; i < target_dag_cnt_ && OB_SUCC(ret); i++) {
      if (OB_FAIL(MTL(ObTenantDagScheduler*)->alloc_dag(dag))) {
        COMMON_LOG(WARN, "Fail to create dag", K(ret));
      } else if (OB_ISNULL(first_dag_) && OB_FAIL(add_dag_into_dag_net(*dag))) { // add first dag into this dag_net
        COMMON_LOG(WARN, "Fail to add dag into dag_net", K(ret));
      } else if (OB_NOT_NULL(first_dag_) && i != (target_dag_cnt_ - 1) && OB_FAIL(first_dag_->add_child(*dag))) {
        COMMON_LOG(WARN, "Fail to add child of first_dag", K(ret), KPC(first_dag_), KPC(dag));
      } else if (OB_NOT_NULL(last_dag) && i == (target_dag_cnt_ - 1) && OB_FAIL(last_dag->add_child(*dag))) {
        COMMON_LOG(WARN, "Fail to add child of last_dag", K(ret), KPC(last_dag), KPC(dag));
      } else if (OB_FAIL(dag->create_first_task())) {
        COMMON_LOG(WARN, "Fail to create first task", K(ret));
      } else if (OB_FAIL(MTL(ObTenantDagScheduler*)->add_dag(dag))) {
        COMMON_LOG(WARN, "failed to add dag", K(ret), K(dag));
      } else if (OB_ISNULL(first_dag_)) {
        first_dag_ = dag;
      }
      last_dag = dag;
      EXPECT_EQ(OB_SUCCESS, ret);
    }
    return ret;
  }
public:
  int64_t target_dag_cnt_;
  ObCancelDag *first_dag_;
};

TEST_F(TestDagScheduler, test_cancel_waiting_dag)
{
  int ret = OB_SUCCESS;
  ObTenantDagScheduler *scheduler = MTL(ObTenantDagScheduler*);
  ASSERT_TRUE(nullptr != scheduler);
  EXPECT_EQ(OB_SUCCESS, scheduler->create_and_add_dag_net<ObCancelWaitingDagInDagNet>(nullptr));
  ObIDagNet *tmp_dag_net = nullptr;
  EXPECT_EQ(OB_SUCCESS, scheduler->get_first_dag_net(tmp_dag_net));
  EXPECT_NE(nullptr, tmp_dag_net);

  while (scheduler->get_cur_dag_cnt() < 4 /*target_dag_cnt*/) {
    usleep(100);
  }

  ObCancelWaitingDagInDagNet *dag_net = static_cast<ObCancelWaitingDagInDagNet *>(tmp_dag_net);
  EXPECT_NE(nullptr, dag_net);
  ObArray<ObIDag *> dag_array;
  dag_net->get_dag_list(dag_array);
  EXPECT_EQ(4, dag_array.count());

  ObCancelDag *first_dag = dag_net->first_dag_;
  EXPECT_NE(nullptr, first_dag);
  first_dag->set_force_cancel_flag();
  first_dag->can_schedule_ = true;
  wait_scheduler();
}

TEST_F(TestDagScheduler, test_destroy_when_running) //TODO(renju.rj): fix it
{
//  ObTenantDagScheduler *scheduler = MTL(ObTenantDagScheduler*);
//  ASSERT_TRUE(nullptr != scheduler);
//
//  #ifndef BUILD_COVERAGE
//  // not participate in coverage compilation to fix hang problem
//  ObCancelDagNet *dag_net = nullptr;
//  EXPECT_EQ(OB_SUCCESS, scheduler->create_and_add_dag_net(nullptr));
//
//  while (scheduler->get_cur_dag_cnt() < 3) {
//    usleep(100);
//  }
//  #endif
}


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
  OB_LOGGER.set_enable_async_log(false);
  OB_LOGGER.set_log_level("DEBUG");
  OB_LOGGER.set_max_file_size(256*1024*1024);
  system("rm -f test_dag_net_in_dag_scheduler.log*");
  OB_LOGGER.set_file_name("test_dag_net_in_dag_scheduler.log");
  return RUN_ALL_TESTS();
}
