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

#define USING_LOG_PREFIX SQL

#include <gtest/gtest.h>

#define private public
#define protected public

#include "sql/engine/set/ob_hash_union.h"
#include "sql/engine/set/ob_hash_intersect.h"
#include "sql/engine/set/ob_hash_except.h"
#include "sql/engine/set/ob_merge_union.h"
#include "sql/engine/set/ob_merge_intersect.h"
#include "sql/engine/set/ob_merge_except.h"
#include "share/system_variable/ob_system_variable.h"
#include "storage/blocksstable/ob_data_file_prepare.h"
#include "sql/engine/table/ob_fake_table.h"
#include "set_data_generator.h"
#include "sql/ob_sql_init.h"
#include "share/ob_cluster_version.h"
#include "observer/omt/ob_tenant_config_mgr.h"
#include "observer/ob_server.h"
#include "share/ob_simple_mem_limit_getter.h"
namespace oceanbase
{
namespace sql
{
using namespace common;
using namespace share;
using namespace omt;
static ObSimpleMemLimitGetter getter;

class MockSqlExpression : public ObSqlExpression
{
public:
  MockSqlExpression(ObIAllocator &alloc): ObSqlExpression(alloc)
  {
    set_item_count(10);
  }
};

#define TEST_SET_DUMP_GET_HASH_AREA_SIZE() (get_hash_area_size())
#define TEST_SET_DUMP_SET_HASH_AREA_SIZE(size) (set_hash_area_size(size))

class ObHashSetDumpTest:
  public blocksstable::TestDataFilePrepare, public ::testing::WithParamInterface<ObJoinType>
{
public:
  enum TestAlgo
  {
    UNION = 0,
    INTERSECT,
    EXCEPT
  };
protected:
  struct SetPlan
  {
    explicit SetPlan(ObIAllocator &alloc)
        : exec_ctx_(alloc), set_op_(nullptr), left_(alloc), right_(alloc), expr_(alloc) {}

    int setup_plan(ObSetOperator *set_op);

    ObSQLSessionInfo session_;
    ObPhysicalPlan plan_;
    ObExecContext exec_ctx_;
    ObSetOperator *set_op_;
    SetDataGenerator left_;
    SetDataGenerator right_;
    MockSqlExpression expr_;
  };
public:
  ObHashSetDumpTest()
    : blocksstable::TestDataFilePrepare(&getter,"TestDiskIR", 8<<20, 5000),
        hash_union_(alloc_), merge_union_(alloc_),
        hash_intersect_(alloc_), merge_intersect_(alloc_),
        hash_except_(alloc_), merge_except_(alloc_),
        hash_set_op_(nullptr), merge_set_op_(nullptr),
        hash_plan_(alloc_), merge_plan_(alloc_)
  {
  }

  int init_tenant_mgr();
  virtual void SetUp() override
  {
    ASSERT_EQ(OB_SUCCESS, init_tenant_mgr());
    blocksstable::TestDataFilePrepare::SetUp();
    ASSERT_EQ(OB_SUCCESS, blocksstable::ObTmpFileManager::get_instance().init());
    CHUNK_MGR.set_limit(128L * 1024L * 1024L * 1024L);
    GCONF.enable_sql_operator_dump.set_value("True");
    uint64_t cluster_version = CLUSTER_VERSION_3000;
    common::ObClusterVersion::get_instance().update_cluster_version(cluster_version);
    EXPECT_EQ(cluster_version, common::ObClusterVersion::get_instance().get_cluster_version());
    LOG_INFO("set cluster version", K(cluster_version),
      K(common::ObClusterVersion::get_instance().get_cluster_version()));
    OBSERVER.init_schema();
    OBSERVER.init_tz_info_mgr();
  }
  virtual void TearDown() override
  {
    blocksstable::ObTmpFileManager::get_instance().destroy();
    blocksstable::TestDataFilePrepare::TearDown();
    destroy_tenant_mgr();
  }

  void destroy_tenant_mgr()
  {
  }

  int64_t get_hash_area_size()
  {
    int64_t hash_area_size = 0;
    int ret = OB_SUCCESS;
    ret = ObSqlWorkareaUtil::get_workarea_size(HASH_WORK_AREA, OB_SYS_TENANT_ID, hash_area_size);
    if (OB_FAIL(ret)) {
      LOG_WARN("failed to get hash area size", K(ret), K(hash_area_size));
    }
    return hash_area_size;
  }

  void set_hash_area_size(int64_t size)
  {
    int ret = OB_SUCCESS;
    int64_t tenant_id = OB_SYS_TENANT_ID;
    ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id));
    if (tenant_config.is_valid()) {
      tenant_config->_hash_area_size = size;
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected status: config is invalid", K(tenant_id));
    }
    // ASSERT_EQ(OB_SUCCESS, ret);
  }

  void setup_plan(SetPlan &plan, bool hash_algo, ObHashSetDumpTest::TestAlgo algo);
  void setup_test(TestAlgo algo, int32_t string_size,
      int64_t left_row_count, bool left_reverse, SetDataGenerator::IdxCntFunc left_func,
      int64_t right_row_count, bool right_reverse, SetDataGenerator::IdxCntFunc right_func);

  // iterate hash join result and verify result with merge join.
  void run_test(int64_t print_row_cnt = 0);

protected:
  ObArenaAllocator alloc_;

  ObHashUnion hash_union_;
  ObMergeUnion merge_union_;

  ObHashIntersect hash_intersect_;
  ObMergeIntersect merge_intersect_;

  ObHashExcept hash_except_;
  ObMergeExcept merge_except_;

  ObSetOperator *hash_set_op_;
  ObSetOperator *merge_set_op_;

  SetPlan hash_plan_;
  SetPlan merge_plan_;
};

int ObHashSetDumpTest::SetPlan::setup_plan(ObSetOperator *set_op)
{
  int ret = OB_SUCCESS;
  left_.set_id(0);
  right_.set_id(1);
  set_op_ = set_op;
  set_op_->set_id(2);

  set_op_->set_column_count(SetDataGenerator::CELL_CNT * 2);

  left_.set_phy_plan(&plan_);
  right_.set_phy_plan(&plan_);
  set_op_->set_phy_plan(&plan_);
  set_op_->create_child_array(2);
  if (OB_FAIL(set_op_->set_child(0, right_))) {
  } else if (OB_FAIL(set_op_->set_child(1, left_))) {
  }

  set_op_->set_distinct(true);

  // setup context
  ObString tenant_name("test");
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(session_.test_init(0, 0, 0, NULL))) {
  } else if (OB_FAIL(ObPreProcessSysVars::init_sys_var())) {
  } else if (OB_FAIL(session_.load_default_sys_variable(false, true))) {
  } else if (OB_FAIL(session_.init_tenant(tenant_name, OB_SYS_TENANT_ID))) {
  } else if (FALSE_IT(exec_ctx_.set_my_session(&session_))) {
  } else if (OB_FAIL(exec_ctx_.init_phy_op(3))) {
  } else if (OB_FAIL(exec_ctx_.create_physical_plan_ctx())) {
  }
  return ret;
}

void ObHashSetDumpTest::setup_plan(SetPlan &plan, bool hash_algo, ObHashSetDumpTest::TestAlgo algo)
{
  int ret = OB_SUCCESS;
  ObMergeSetOperator *merge_op = nullptr;
  ObHashSetOperator *hash_op = nullptr;
  switch (algo)
  {
  case UNION:
    if (hash_algo) {
      ASSERT_EQ(OB_SUCCESS, plan.setup_plan(&hash_union_));
      hash_set_op_ = &hash_union_;
      hash_op = &hash_union_;
    } else {
      ASSERT_EQ(OB_SUCCESS, plan.setup_plan(&merge_union_));
      merge_op = &merge_union_;
      merge_set_op_ = &merge_union_;
    }
    break;
  case INTERSECT:
    if (hash_algo) {
      ASSERT_EQ(OB_SUCCESS, plan.setup_plan(&hash_intersect_));
      hash_set_op_ = &hash_intersect_;
      hash_op = &hash_intersect_;
    } else {
      ASSERT_EQ(OB_SUCCESS, plan.setup_plan(&merge_intersect_));
      merge_op = &merge_intersect_;
      merge_set_op_ = &merge_intersect_;
    }
    break;
  case EXCEPT:
    if (hash_algo) {
      ASSERT_EQ(OB_SUCCESS, plan.setup_plan(&hash_except_));
      hash_set_op_ = &hash_except_;
      hash_op = &hash_except_;
    } else {
      ASSERT_EQ(OB_SUCCESS, plan.setup_plan(&merge_except_));
      merge_op = &merge_except_;
      merge_set_op_ = &merge_except_;
    }
    break;
  default:
    break;
  }
  if (nullptr != merge_op) {
    ASSERT_EQ(OB_SUCCESS, merge_op->init(SetDataGenerator::CELL_CNT));
    ObOrderDirection direction = NULLS_FIRST_ASC;
    for (int64_t i = 0; i < SetDataGenerator::CELL_CNT; ++i) {
      ASSERT_EQ(OB_SUCCESS, merge_op->add_set_direction(direction));
    }
    for (int64_t i = 0; i < SetDataGenerator::CELL_CNT && OB_SUCC(ret); ++i) {
      if (OB_FAIL(merge_op->add_collation_type(ObCollationType::CS_TYPE_UTF8MB4_BIN))) {
        LOG_WARN("failed to add collation type", K(ret));
      }
    }
  }
  if (nullptr != hash_op) {
    ASSERT_EQ(OB_SUCCESS, hash_op->init(SetDataGenerator::CELL_CNT));
    for (int64_t i = 0; i < SetDataGenerator::CELL_CNT && OB_SUCC(ret); ++i) {
      if (OB_FAIL(hash_op->add_collation_type(ObCollationType::CS_TYPE_UTF8MB4_BIN))) {
        LOG_WARN("failed to add collation type", K(ret));
      }
    }
  }
}

void ObHashSetDumpTest::setup_test(TestAlgo algo, int32_t string_size,
    int64_t left_row_count, bool left_reverse, SetDataGenerator::IdxCntFunc left_func,
    int64_t right_row_count, bool right_reverse, SetDataGenerator::IdxCntFunc right_func)
{
  SetPlan *plans[] = { &hash_plan_, &merge_plan_ };
  for (int i = 0; i < 2; i++) {
    auto &plan = *plans[i];
    setup_plan(plan, i == 0 ? true : false, algo);

    plan.left_.row_cnt_ = left_row_count;
    plan.right_.row_cnt_ = right_row_count;
    plan.left_.string_size_ = string_size;
    plan.right_.string_size_ = string_size;
    if (&plan != &merge_plan_) {
      plan.left_.reverse_ = left_reverse;
      plan.right_.reverse_ = right_reverse;
    }
    plan.left_.idx_cnt_func_ = left_func;
    plan.right_.idx_cnt_func_ = right_func;

    ASSERT_EQ(OB_SUCCESS, plan.left_.test_init());
    ASSERT_EQ(OB_SUCCESS, plan.right_.test_init());
  }
}

void ObHashSetDumpTest::run_test(int64_t print_row_cnt)
{
  ObArenaAllocator alloc;
  typedef ObArray<int64_t *> ResArray;
  int64_t res_cell_cnt = SetDataGenerator::CELL_CNT * 2;
  auto fun = [&](SetPlan &plan, ResArray &res)->void
  {
    ASSERT_EQ(OB_SUCCESS, plan.set_op_->open(plan.exec_ctx_));
    int ret = OB_SUCCESS;
    const ObNewRow *row = NULL;
    int64_t cnt = 0;
    while (OB_SUCC(ret)) {
      if (OB_FAIL(plan.set_op_->get_next_row(plan.exec_ctx_, row))) {
        ASSERT_EQ(OB_ITER_END, ret);
      } else {
        if (cnt < print_row_cnt) {
          LOG_INFO("join res", K(*row));
        }
        auto r = static_cast<int64_t *>(alloc.alloc(sizeof(int64_t) * res_cell_cnt));
        ASSERT_TRUE(NULL != r);
        for (int64_t i = 0; i < res_cell_cnt; i++) {
          auto &c = row->cells_[i];
          if (i < row->count_ && c.get_type() == ObIntType) {
            r[i] = c.get_int();
          } else {
            r[i] = -1;
          }
        }
        ASSERT_EQ(OB_SUCCESS, res.push_back(r));
      }
      cnt++;
    }
  };

  auto pfunc = [&](int64_t *r)
  {
    ObSqlString s;
    for (int64_t i = 0; i < res_cell_cnt; i++) {
      s.append_fmt("%ld, ", r[i]);
    }
    LOG_INFO("RES:", K(s.ptr()));
  };

  ResArray hash_res;
  fun(hash_plan_, hash_res);
  ASSERT_FALSE(HasFatalFailure());
  ResArray merge_res;
  fun(merge_plan_, merge_res);
  ASSERT_FALSE(HasFatalFailure());

  ASSERT_EQ(hash_res.count(), merge_res.count());
  auto sort_cmp = [&](int64_t *l, int64_t *r)
  {
    for (int64_t i = 0; i < res_cell_cnt; i++) {
      if (l[i] != r[i]) {
        return l[i] < r[i];
      }
    }
    return false;
  };
  std::sort(&hash_res.at(0), &hash_res.at(0) + hash_res.count(), sort_cmp);
  std::sort(&merge_res.at(0), &merge_res.at(0) + merge_res.count(), sort_cmp);
  for (int64_t i = 0; i < hash_res.count(); i++) {
    if (sort_cmp(hash_res.at(i), merge_res.at(i))
        || sort_cmp(merge_res.at(i), hash_res.at(i))) {
      pfunc(hash_res.at(i));
      pfunc(merge_res.at(i));
      ASSERT_FALSE(true);
    }
  }

  hash_set_op_->close(hash_plan_.exec_ctx_);
  merge_set_op_->close(merge_plan_.exec_ctx_);
  // hash_plan_.~SetPlan();
  // merge_plan_.~SetPlan();
  ASSERT_EQ(OB_SUCCESS, blocksstable::ObTmpFileManager::get_instance().files_.map_.size());
}

int ObHashSetDumpTest::init_tenant_mgr()
{
  int ret = OB_SUCCESS;
  ObAddr self;
  oceanbase::rpc::frame::ObReqTransport req_transport(NULL, NULL);
  oceanbase::obrpc::ObSrvRpcProxy rpc_proxy;
  oceanbase::obrpc::ObCommonRpcProxy rs_rpc_proxy;
  oceanbase::share::ObRsMgr rs_mgr;
  uint64_t cluster_version = CLUSTER_VERSION_3000;
  common::ObClusterVersion::get_instance().update_cluster_version(cluster_version);
  EXPECT_EQ(cluster_version, common::ObClusterVersion::get_instance().get_cluster_version());
  int64_t tenant_id = OB_SYS_TENANT_ID;
  self.set_ip_addr("127.0.0.1", 8086);
  ret = ObTenantConfigMgr::get_instance().add_tenant_config(tenant_id);
  EXPECT_EQ(OB_SUCCESS, ret);
  ret = getter.add_tenant(tenant_id,
                          4L * 1024L * 1024L * 1024L,
                          8L * 1024L * 1024L * 1024L);
  EXPECT_EQ(OB_SUCCESS, ret);
  const int64_t ulmt = 256LL << 30;
  const int64_t llmt = 256LL << 30;
  ret = getter.add_tenant(OB_SERVER_TENANT_ID,
                          ulmt,
                          llmt);
  EXPECT_EQ(OB_SUCCESS, ret);
  auto ctx_allocator =
    lib::ObMallocAllocator::get_instance()->get_tenant_ctx_allocator(
          OB_SERVER_TENANT_ID, common::ObCtxIds::DEFAULT_CTX_ID);
  EXPECT_EQ(OB_SUCCESS, ret);
  ret = ctx_allocator->set_limit(8L * 1024L * 1024L * 1024L);
  EXPECT_EQ(OB_SUCCESS, ret);
  oceanbase::lib::set_memory_limit(128LL << 32);
  return ret;
}

TEST_F(ObHashSetDumpTest, test_single)
{
  setup_test(ObHashSetDumpTest::TestAlgo::UNION, 512,
    1000, false, [](int64_t id, int64_t) { return id % 3 == 0 ? 1 : 0; },
    1000, false, [](int64_t id, int64_t) { return id % 5 == 0 ? 1 : 0; });
  ASSERT_FALSE(HasFatalFailure());
  run_test();
  ASSERT_FALSE(HasFatalFailure());
}

TEST_F(ObHashSetDumpTest, test_except)
{
  setup_test(ObHashSetDumpTest::TestAlgo::EXCEPT, 512,
    1000, false, [](int64_t id, int64_t) { return id % 3 == 0 ? 1 : 0; },
    1000, false, [](int64_t id, int64_t) { return id % 5 == 0 ? 1 : 0; });
  ASSERT_FALSE(HasFatalFailure());
  run_test();
  ASSERT_FALSE(HasFatalFailure());
}

TEST_F(ObHashSetDumpTest, test_intersect)
{
  setup_test(ObHashSetDumpTest::TestAlgo::INTERSECT, 512,
    1000, false, [](int64_t id, int64_t) { return id % 3 == 0 ? 1 : 0; },
    1000, false, [](int64_t id, int64_t) { return id % 5 == 0 ? 1 : 0; });
  ASSERT_FALSE(HasFatalFailure());
  run_test();
  ASSERT_FALSE(HasFatalFailure());
}

TEST_F(ObHashSetDumpTest, test_dump_union)
{
  setup_test(ObHashSetDumpTest::TestAlgo::UNION, 2000,
      200000 * 3, false, [](int64_t id, int64_t) { return id % 3 == 0 ? 2 : 0; },
      200000 * 5, false, [](int64_t id, int64_t) { return id % 5 == 0 ? 2 : 0; });
  ASSERT_FALSE(HasFatalFailure());
  run_test();
  ASSERT_FALSE(HasFatalFailure());
}

TEST_F(ObHashSetDumpTest, test_dump_intersect)
{
  setup_test(ObHashSetDumpTest::TestAlgo::INTERSECT, 2000,
      200000 * 3, false, [](int64_t id, int64_t) { return id % 3 == 0 ? 2 : 0; },
      200000 * 5, false, [](int64_t id, int64_t) { return id % 5 == 0 ? 2 : 0; });
  ASSERT_FALSE(HasFatalFailure());
  run_test();
  ASSERT_FALSE(HasFatalFailure());
}

TEST_F(ObHashSetDumpTest, test_dump_except)
{
  setup_test(ObHashSetDumpTest::TestAlgo::EXCEPT, 2000,
      200000 * 3, false, [](int64_t id, int64_t) { return id % 3 == 0 ? 2 : 0; },
      200000 * 5, false, [](int64_t id, int64_t) { return id % 5 == 0 ? 2 : 0; });
  ASSERT_FALSE(HasFatalFailure());
  run_test();
  ASSERT_FALSE(HasFatalFailure());
}

// farm时间比较长，暂时skip掉
// TEST_F(ObHashSetDumpTest, Size20M_union)
// {
//   int64_t hash_mem = 0;
//   hash_mem = TEST_SET_DUMP_GET_HASH_AREA_SIZE();
//   ASSERT_EQ(100 * 1024 * 1024, hash_mem);
//   TEST_SET_DUMP_SET_HASH_AREA_SIZE(20* 1024 * 1024);

//   setup_test(ObHashSetDumpTest::TestAlgo::UNION, 2000,
//       200000 * 3, false, [](int64_t id, int64_t) { return id % 3 == 0 ? 2 : 0; },
//       200000 * 5, false, [](int64_t id, int64_t) { return id % 5 == 0 ? 2 : 0; });
//   ASSERT_FALSE(HasFatalFailure());
//   run_test(10);
//   ASSERT_FALSE(HasFatalFailure());

//   hash_mem = TEST_SET_DUMP_GET_HASH_AREA_SIZE();
//   ASSERT_EQ((20 * 1024 * 1024), hash_mem);
//   TEST_SET_DUMP_SET_HASH_AREA_SIZE(100* 1024 * 1024);
//   hash_mem = TEST_SET_DUMP_GET_HASH_AREA_SIZE();
//   ASSERT_EQ((100 * 1024 * 1024), hash_mem);
// }

// TEST_F(ObHashSetDumpTest, Size20M_intersect)
// {
//   int64_t hash_mem = 0;
//   hash_mem = TEST_SET_DUMP_GET_HASH_AREA_SIZE();
//   ASSERT_EQ(100 * 1024 * 1024, hash_mem);
//   TEST_SET_DUMP_SET_HASH_AREA_SIZE(20* 1024 * 1024);

//   setup_test(ObHashSetDumpTest::TestAlgo::INTERSECT, 2000,
//       200000 * 3, false, [](int64_t id, int64_t) { return id % 3 == 0 ? 2 : 0; },
//       200000 * 5, false, [](int64_t id, int64_t) { return id % 5 == 0 ? 2 : 0; });
//   ASSERT_FALSE(HasFatalFailure());
//   run_test(10);
//   ASSERT_FALSE(HasFatalFailure());

//   hash_mem = TEST_SET_DUMP_GET_HASH_AREA_SIZE();
//   ASSERT_EQ((20 * 1024 * 1024), hash_mem);
//   TEST_SET_DUMP_SET_HASH_AREA_SIZE(100* 1024 * 1024);
//   hash_mem = TEST_SET_DUMP_GET_HASH_AREA_SIZE();
//   ASSERT_EQ((100 * 1024 * 1024), hash_mem);
// }

// TEST_F(ObHashSetDumpTest, Size20M_except)
// {
//   int64_t hash_mem = 0;
//   hash_mem = TEST_SET_DUMP_GET_HASH_AREA_SIZE();
//   ASSERT_EQ(100 * 1024 * 1024, hash_mem);
//   TEST_SET_DUMP_SET_HASH_AREA_SIZE(20* 1024 * 1024);

//   setup_test(ObHashSetDumpTest::TestAlgo::EXCEPT, 2000,
//       200000 * 3, false, [](int64_t id, int64_t) { return id % 3 == 0 ? 2 : 0; },
//       200000 * 5, false, [](int64_t id, int64_t) { return id % 5 == 0 ? 2 : 0; });
//   ASSERT_FALSE(HasFatalFailure());
//   run_test(10);
//   ASSERT_FALSE(HasFatalFailure());

//   hash_mem = TEST_SET_DUMP_GET_HASH_AREA_SIZE();
//   ASSERT_EQ((20 * 1024 * 1024), hash_mem);
//   TEST_SET_DUMP_SET_HASH_AREA_SIZE(100* 1024 * 1024);
//   hash_mem = TEST_SET_DUMP_GET_HASH_AREA_SIZE();
//   ASSERT_EQ((100 * 1024 * 1024), hash_mem);
// }

// TEST_F(ObHashSetDumpTest, test_more_data_union)
// {
//   int64_t hash_mem = 0;
//   hash_mem = TEST_SET_DUMP_GET_HASH_AREA_SIZE();
//   ASSERT_EQ(100 * 1024 * 1024, hash_mem);
//   TEST_SET_DUMP_SET_HASH_AREA_SIZE(20* 1024 * 1024);

//   setup_test(ObHashSetDumpTest::TestAlgo::UNION, 2000,
//       100000 * 3, false, [](int64_t id, int64_t) { return id % 3 == 0 ? 2 : 0; },
//       2000000, false, [](int64_t id, int64_t) { return id % 5 == 0 ? 2 : 0; });
//   ASSERT_FALSE(HasFatalFailure());
//   run_test(10);
//   ASSERT_FALSE(HasFatalFailure());

//   hash_mem = TEST_SET_DUMP_GET_HASH_AREA_SIZE();
//   ASSERT_EQ((20 * 1024 * 1024), hash_mem);
//   TEST_SET_DUMP_SET_HASH_AREA_SIZE(100* 1024 * 1024);
//   hash_mem = TEST_SET_DUMP_GET_HASH_AREA_SIZE();
//   ASSERT_EQ((100 * 1024 * 1024), hash_mem);
// }

// TEST_F(ObHashSetDumpTest, test_more_data_intersect)
// {
//   int64_t hash_mem = 0;
//   hash_mem = TEST_SET_DUMP_GET_HASH_AREA_SIZE();
//   ASSERT_EQ(100 * 1024 * 1024, hash_mem);
//   TEST_SET_DUMP_SET_HASH_AREA_SIZE(20* 1024 * 1024);

//   setup_test(ObHashSetDumpTest::TestAlgo::INTERSECT, 2000,
//       100000 * 3, false, [](int64_t id, int64_t) { return id % 3 == 0 ? 2 : 0; },
//       2000000, false, [](int64_t id, int64_t) { return id % 5 == 0 ? 2 : 0; });
//   ASSERT_FALSE(HasFatalFailure());
//   run_test(10);
//   ASSERT_FALSE(HasFatalFailure());

//   hash_mem = TEST_SET_DUMP_GET_HASH_AREA_SIZE();
//   ASSERT_EQ((20 * 1024 * 1024), hash_mem);
//   TEST_SET_DUMP_SET_HASH_AREA_SIZE(100* 1024 * 1024);
//   hash_mem = TEST_SET_DUMP_GET_HASH_AREA_SIZE();
//   ASSERT_EQ((100 * 1024 * 1024), hash_mem);
// }

// TEST_F(ObHashSetDumpTest, test_more_data_except)
// {
//   int64_t hash_mem = 0;
//   hash_mem = TEST_SET_DUMP_GET_HASH_AREA_SIZE();
//   ASSERT_EQ(100 * 1024 * 1024, hash_mem);
//   TEST_SET_DUMP_SET_HASH_AREA_SIZE(20* 1024 * 1024);

//   setup_test(ObHashSetDumpTest::TestAlgo::EXCEPT, 2000,
//       100000 * 3, false, [](int64_t id, int64_t) { return id % 3 == 0 ? 2 : 0; },
//       2000000, false, [](int64_t id, int64_t) { return id % 5 == 0 ? 2 : 0; });
//   ASSERT_FALSE(HasFatalFailure());
//   run_test(10);
//   ASSERT_FALSE(HasFatalFailure());

//   hash_mem = TEST_SET_DUMP_GET_HASH_AREA_SIZE();
//   ASSERT_EQ((20 * 1024 * 1024), hash_mem);
//   TEST_SET_DUMP_SET_HASH_AREA_SIZE(100* 1024 * 1024);
//   hash_mem = TEST_SET_DUMP_GET_HASH_AREA_SIZE();
//   ASSERT_EQ((100 * 1024 * 1024), hash_mem);
// }

// TEST_F(ObHashSetDumpTest, test_bigger_left_union)
// {
//   int64_t hash_mem = 0;
//   hash_mem = TEST_SET_DUMP_GET_HASH_AREA_SIZE();
//   ASSERT_EQ(100 * 1024 * 1024, hash_mem);
//   TEST_SET_DUMP_SET_HASH_AREA_SIZE(20* 1024 * 1024);

//   setup_test(ObHashSetDumpTest::TestAlgo::UNION, 2000,
//       2000000, false, [](int64_t id, int64_t) { return id % 3 == 0 ? 2 : 0; },
//       2 * 5, false, [](int64_t id, int64_t) { return id % 5 == 0 ? 2 : 0; });
//   ASSERT_FALSE(HasFatalFailure());
//   run_test(10);
//   ASSERT_FALSE(HasFatalFailure());

//   hash_mem = TEST_SET_DUMP_GET_HASH_AREA_SIZE();
//   ASSERT_EQ((20 * 1024 * 1024), hash_mem);
//   TEST_SET_DUMP_SET_HASH_AREA_SIZE(100* 1024 * 1024);
//   hash_mem = TEST_SET_DUMP_GET_HASH_AREA_SIZE();
//   ASSERT_EQ((100 * 1024 * 1024), hash_mem);
// }

// TEST_F(ObHashSetDumpTest, test_bigger_left_intersect)
// {
//   int64_t hash_mem = 0;
//   hash_mem = TEST_SET_DUMP_GET_HASH_AREA_SIZE();
//   ASSERT_EQ(100 * 1024 * 1024, hash_mem);
//   TEST_SET_DUMP_SET_HASH_AREA_SIZE(20* 1024 * 1024);

//   setup_test(ObHashSetDumpTest::TestAlgo::INTERSECT, 2000,
//       2000000, false, [](int64_t id, int64_t) { return id % 3 == 0 ? 2 : 0; },
//       2 * 5, false, [](int64_t id, int64_t) { return id % 5 == 0 ? 2 : 0; });
//   ASSERT_FALSE(HasFatalFailure());
//   run_test(10);
//   ASSERT_FALSE(HasFatalFailure());

//   hash_mem = TEST_SET_DUMP_GET_HASH_AREA_SIZE();
//   ASSERT_EQ((20 * 1024 * 1024), hash_mem);
//   TEST_SET_DUMP_SET_HASH_AREA_SIZE(100* 1024 * 1024);
//   hash_mem = TEST_SET_DUMP_GET_HASH_AREA_SIZE();
//   ASSERT_EQ((100 * 1024 * 1024), hash_mem);
// }

// TEST_F(ObHashSetDumpTest, test_bigger_left_except)
// {
//   int64_t hash_mem = 0;
//   hash_mem = TEST_SET_DUMP_GET_HASH_AREA_SIZE();
//   ASSERT_EQ(100 * 1024 * 1024, hash_mem);
//   TEST_SET_DUMP_SET_HASH_AREA_SIZE(20* 1024 * 1024);

//   setup_test(ObHashSetDumpTest::TestAlgo::EXCEPT, 2000,
//       2000000, false, [](int64_t id, int64_t) { return id % 3 == 0 ? 2 : 0; },
//       2 * 5, false, [](int64_t id, int64_t) { return id % 5 == 0 ? 2 : 0; });
//   ASSERT_FALSE(HasFatalFailure());
//   run_test(10);
//   ASSERT_FALSE(HasFatalFailure());

//   hash_mem = TEST_SET_DUMP_GET_HASH_AREA_SIZE();
//   ASSERT_EQ((20 * 1024 * 1024), hash_mem);
//   TEST_SET_DUMP_SET_HASH_AREA_SIZE(100* 1024 * 1024);
//   hash_mem = TEST_SET_DUMP_GET_HASH_AREA_SIZE();
//   ASSERT_EQ((100 * 1024 * 1024), hash_mem);
// }

// TEST_F(ObHashSetDumpTest, test_bigger_right_union)
// {
//   int64_t hash_mem = 0;
//   hash_mem = TEST_SET_DUMP_GET_HASH_AREA_SIZE();
//   ASSERT_EQ(100 * 1024 * 1024, hash_mem);
//   TEST_SET_DUMP_SET_HASH_AREA_SIZE(20* 1024 * 1024);

//   setup_test(ObHashSetDumpTest::TestAlgo::UNION, 2000,
//     2 * 5, false, [](int64_t id, int64_t) { return id % 5 == 0 ? 2 : 0; },
//     2000000, false, [](int64_t id, int64_t) { return id % 3 == 0 ? 2 : 0; });
//   ASSERT_FALSE(HasFatalFailure());
//   run_test(10);
//   ASSERT_FALSE(HasFatalFailure());

//   hash_mem = TEST_SET_DUMP_GET_HASH_AREA_SIZE();
//   ASSERT_EQ((20 * 1024 * 1024), hash_mem);
//   TEST_SET_DUMP_SET_HASH_AREA_SIZE(100* 1024 * 1024);
//   hash_mem = TEST_SET_DUMP_GET_HASH_AREA_SIZE();
//   ASSERT_EQ((100 * 1024 * 1024), hash_mem);
// }

// TEST_F(ObHashSetDumpTest, test_bigger_right_intersect)
// {
//   int64_t hash_mem = 0;
//   hash_mem = TEST_SET_DUMP_GET_HASH_AREA_SIZE();
//   ASSERT_EQ(100 * 1024 * 1024, hash_mem);
//   TEST_SET_DUMP_SET_HASH_AREA_SIZE(20* 1024 * 1024);

//   setup_test(ObHashSetDumpTest::TestAlgo::INTERSECT, 2000,
//     2 * 5, false, [](int64_t id, int64_t) { return id % 5 == 0 ? 2 : 0; },
//     2000000, false, [](int64_t id, int64_t) { return id % 3 == 0 ? 2 : 0; });
//   ASSERT_FALSE(HasFatalFailure());
//   run_test(10);
//   ASSERT_FALSE(HasFatalFailure());

//   hash_mem = TEST_SET_DUMP_GET_HASH_AREA_SIZE();
//   ASSERT_EQ((20 * 1024 * 1024), hash_mem);
//   TEST_SET_DUMP_SET_HASH_AREA_SIZE(100* 1024 * 1024);
//   hash_mem = TEST_SET_DUMP_GET_HASH_AREA_SIZE();
//   ASSERT_EQ((100 * 1024 * 1024), hash_mem);
// }

// TEST_F(ObHashSetDumpTest, test_bigger_right_except)
// {
//   int64_t hash_mem = 0;
//   hash_mem = TEST_SET_DUMP_GET_HASH_AREA_SIZE();
//   ASSERT_EQ(100 * 1024 * 1024, hash_mem);
//   TEST_SET_DUMP_SET_HASH_AREA_SIZE(20* 1024 * 1024);

//   setup_test(ObHashSetDumpTest::TestAlgo::EXCEPT, 2000,
//     2 * 5, false, [](int64_t id, int64_t) { return id % 5 == 0 ? 2 : 0; },
//     2000000, false, [](int64_t id, int64_t) { return id % 3 == 0 ? 2 : 0; });
//   ASSERT_FALSE(HasFatalFailure());
//   run_test(10);
//   ASSERT_FALSE(HasFatalFailure());

//   hash_mem = TEST_SET_DUMP_GET_HASH_AREA_SIZE();
//   ASSERT_EQ((20 * 1024 * 1024), hash_mem);
//   TEST_SET_DUMP_SET_HASH_AREA_SIZE(100* 1024 * 1024);
//   hash_mem = TEST_SET_DUMP_GET_HASH_AREA_SIZE();
//   ASSERT_EQ((100 * 1024 * 1024), hash_mem);
// }

} // end sql
} // end oceanbase

int main(int argc, char **argv)
{
  oceanbase::sql::init_sql_factories();
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc,argv);
  int ret = RUN_ALL_TESTS();
  OB_LOGGER.disable();
  return ret;
}
