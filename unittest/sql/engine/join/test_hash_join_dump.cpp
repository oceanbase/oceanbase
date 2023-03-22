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

#include "sql/engine/join/ob_hash_join.h"
#include "sql/engine/join/ob_merge_join.h"
#include "share/system_variable/ob_system_variable.h"
#include "storage/blocksstable/ob_data_file_prepare.h"
#include "join_data_generator.h"
#include "sql/ob_sql_init.h"
#include "sql/engine/ob_sql_mem_mgr_processor.h"
#include "observer/omt/ob_tenant_config_mgr.h"
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

#define TEST_HJ_DUMP_GET_HASH_AREA_SIZE() (get_hash_area_size())
#define TEST_HJ_DUMP_SET_HASH_AREA_SIZE(size) (set_hash_area_size(size))

class ObHashJoinDumpTest: public blocksstable::TestDataFilePrepare, public ::testing::WithParamInterface<ObJoinType> 
{
public:
  ObHashJoinDumpTest()
    : blocksstable::TestDataFilePrepare(&getter, "TestDiskIR", 2<<20, 5000),
        hash_join_(alloc_), merge_join_(alloc_),
        hash_plan_(hash_join_, alloc_), merge_plan_(merge_join_, alloc_)
  {
  }

  virtual void SetUp() override
  {
    ASSERT_EQ(OB_SUCCESS, init_tenant_mgr());
    blocksstable::TestDataFilePrepare::SetUp();
    GCONF.enable_sql_operator_dump.set_value("True");
    ObHashJoin::HJ_PROCESSOR_ALGO = ObHashJoin::ENABLE_HJ_NEST_LOOP
                                  | ObHashJoin::ENABLE_HJ_RECURSIVE
                                  | ObHashJoin::ENABLE_HJ_IN_MEMORY;
  }
  virtual void TearDown() override
  {
    blocksstable::TestDataFilePrepare::TearDown();
    destroy_tenant_mgr();
    ObHashJoin::HJ_PROCESSOR_ALGO = 0;
  }
  int init_tenant_mgr();
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

  void setup_test(ObJoinType join_type, int32_t string_size,
      int64_t left_row_count, bool left_reverse, JoinDataGenerator::IdxCntFunc left_func,
      int64_t right_row_count, bool right_reverse, JoinDataGenerator::IdxCntFunc right_func);

  // iterate hash join result and verify result with merge join.
  void run_test(int64_t print_row_cnt = 0);

protected:
  struct JoinPlan
  {
    explicit JoinPlan(ObJoin &join, ObIAllocator &alloc)
        : exec_ctx_(alloc), join_(join), left_(alloc), right_(alloc), expr_(alloc) {}

    int setup_plan(ObJoinType join_type);

    ObSQLSessionInfo session_;
    ObPhysicalPlan plan_;
    ObExecContext exec_ctx_;
    ObJoin &join_;
    JoinDataGenerator left_;
    JoinDataGenerator right_;
    MockSqlExpression expr_;
  };

  ObArenaAllocator alloc_;

  ObHashJoin hash_join_;
  ObMergeJoin merge_join_;

  JoinPlan hash_plan_;
  JoinPlan merge_plan_;
};

int ObHashJoinDumpTest::JoinPlan::setup_plan(ObJoinType join_type)
{
  int ret = OB_SUCCESS;
  left_.set_id(0);
  right_.set_id(1);
  join_.set_id(2);

  join_.set_column_count(JoinDataGenerator::CELL_CNT * (join_type < LEFT_SEMI_JOIN ? 2 : 1));

  left_.set_phy_plan(&plan_);
  right_.set_phy_plan(&plan_);
  join_.set_phy_plan(&plan_);

  if (NULL != dynamic_cast<ObMergeJoin *>(&join_)
      && (RIGHT_SEMI_JOIN == join_type || RIGHT_ANTI_JOIN == join_type)) {
    // right semi and right anti join not supported for merge join,
    // convert to left semi/anti join.
    if (OB_FAIL(join_.set_child(0, right_))) {
    } else if (OB_FAIL(join_.set_child(1, left_))) {
    } else if (OB_FAIL(join_.set_join_type((ObJoinType)(join_type - 1)))) {
    }
  } else {
    if (OB_FAIL(join_.set_child(0, left_))) {
    } else if (OB_FAIL(join_.set_child(1, right_))) {
    } else if (OB_FAIL(join_.set_join_type(join_type))) {
    }
  }

  // setup equal condition
  ObPostExprItem c1;
  ObPostExprItem c2;
  ObPostExprItem op;
  ObExprResType res_type;
  res_type.set_calc_type(ObIntType);

  c1.set_column(0);
  c2.set_column(JoinDataGenerator::CELL_CNT);
  op.set_op(plan_.get_allocator(), "=", 2);
  op.get_expr_operator()->set_result_type(res_type);
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(expr_.add_expr_item(c1))) {
  } else if (OB_FAIL(expr_.add_expr_item(c2))) {
  } else if (OB_FAIL(expr_.add_expr_item(op))) {
  } else if (OB_FAIL(join_.add_equijoin_condition(&expr_))) {
  }

  // setup context
  ObString tenant_name("test");
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(session_.test_init(0, 0, 0, NULL))) {
  } else if (OB_FAIL(ObPreProcessSysVars::init_sys_var())) {
  } else if (OB_FAIL(session_.load_default_sys_variable(false, true))) {
  } else if (OB_FAIL(session_.init_tenant(tenant_name, OB_SYS_TENANT_ID))) {
  } else if (OB_FAIL(exec_ctx_.init_phy_op(3))) {
  } else if (OB_FAIL(exec_ctx_.create_physical_plan_ctx())) {
  } else {
    exec_ctx_.set_my_session(&session_);
  }

  return ret;
}

void ObHashJoinDumpTest::setup_test(ObJoinType join_type, int32_t string_size,
    int64_t left_row_count, bool left_reverse, JoinDataGenerator::IdxCntFunc left_func,
    int64_t right_row_count, bool right_reverse, JoinDataGenerator::IdxCntFunc right_func)
{
  JoinPlan *plans[] = { &hash_plan_, &merge_plan_ };
  for (int i = 0; i < 2; i++) {
    auto &plan = *plans[i];
    ASSERT_EQ(OB_SUCCESS, plan.setup_plan(join_type));

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

  ObSEArray<ObOrderDirection, 1> directions;
  directions.push_back(NULLS_FIRST_ASC);
  ASSERT_EQ(OB_SUCCESS, merge_join_.set_merge_directions(directions));
}

void ObHashJoinDumpTest::run_test(int64_t print_row_cnt)
{
  ObArenaAllocator alloc;
  typedef ObArray<int64_t *> ResArray;
  int64_t res_cell_cnt = JoinDataGenerator::CELL_CNT * 2;
  auto fun = [&](JoinPlan &plan, ResArray &res)->void
  {
    ASSERT_EQ(OB_SUCCESS, plan.join_.open(plan.exec_ctx_));
    int ret = OB_SUCCESS;
    const ObNewRow *row = NULL;
    int64_t cnt = 0;
    while (OB_SUCC(ret)) {
      if (OB_FAIL(plan.join_.get_next_row(plan.exec_ctx_, row))) {
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

  hash_join_.close(hash_plan_.exec_ctx_);
  merge_join_.close(merge_plan_.exec_ctx_);
}

TEST_P(ObHashJoinDumpTest, inmemory)
{
  setup_test(GetParam(), 512,
      1000, false, [](int64_t id, int64_t) { return id % 3 == 0 ? 1 : 0; },
      1000, false, [](int64_t id, int64_t) { return id % 5 == 0 ? 1 : 0; });
  ASSERT_FALSE(HasFatalFailure());
  run_test();
  ASSERT_FALSE(HasFatalFailure());
}

TEST_P(ObHashJoinDumpTest, disk)
{
  setup_test(GetParam(), 2000,
      200000 * 3, false, [](int64_t id, int64_t) { return id % 3 == 0 ? 2 : 0; },
      200000 * 5, false, [](int64_t id, int64_t) { return id % 5 == 0 ? 2 : 0; });
  ASSERT_FALSE(HasFatalFailure());
  run_test();
  ASSERT_FALSE(HasFatalFailure());
}

TEST_P(ObHashJoinDumpTest, disk_reverse)
{
  setup_test(GetParam(), 1999,
      200000 * 3, true, [](int64_t id, int64_t) { return id % 3 == 0 ? 2 : 0; },
      200000 * 5, false, [](int64_t id, int64_t) { return id % 5 == 0 ? 2 : 0; });
  ASSERT_FALSE(HasFatalFailure());
  run_test();
  ASSERT_FALSE(HasFatalFailure());
}

INSTANTIATE_TEST_CASE_P(join, ObHashJoinDumpTest, ::testing::Values(
    INNER_JOIN,
    LEFT_OUTER_JOIN,
    RIGHT_OUTER_JOIN,
    FULL_OUTER_JOIN,
    LEFT_SEMI_JOIN,
    RIGHT_SEMI_JOIN,
    LEFT_ANTI_JOIN,
    RIGHT_ANTI_JOIN));

TEST_F(ObHashJoinDumpTest, test_recursion)
{
  auto part_cnt_bak = ObHashJoin::PART_COUNT;
  auto page_cnt_bak = ObHashJoin::MAX_PAGE_COUNT;
  ObHashJoin::PART_COUNT = 5;
  ObHashJoin::MAX_PAGE_COUNT = (20L << 20) / OB_MALLOC_MIDDLE_BLOCK_SIZE; // 20MB memory
  setup_test(RIGHT_OUTER_JOIN, 2000,
      200000 * 3, false, [](int64_t id, int64_t) { return id % 3 == 0 ? 2 : 0; },
      200000 * 5, false, [](int64_t id, int64_t) { return id % 5 == 0 ? 2 : 0; });
  ASSERT_FALSE(HasFatalFailure());
  run_test(10);
  ASSERT_FALSE(HasFatalFailure());

  ObHashJoin::PART_COUNT = part_cnt_bak;
  ObHashJoin::MAX_PAGE_COUNT = page_cnt_bak;
}

//暂时屏蔽掉这些case，否则farm时间太长，但如果修改join，请打开线下跑下这些test case
// TEST_F(ObHashJoinDumpTest, test_right_outer_recursive)
// {
//   int64_t hash_mem = 0;
//   hash_mem = TEST_HJ_DUMP_GET_HASH_AREA_SIZE();
//   ASSERT_EQ(100 * 1024 * 1024, hash_mem);
//   TEST_HJ_DUMP_SET_HASH_AREA_SIZE(20* 1024 * 1024);

//   auto part_cnt_bak = ObHashJoin::PART_COUNT;
//   auto page_cnt_bak = ObHashJoin::MAX_PAGE_COUNT;
//   ObHashJoin::PART_COUNT = 5;
//   ObHashJoin::MAX_PAGE_COUNT = (20L << 20) / OB_MALLOC_MIDDLE_BLOCK_SIZE; // 20MB memory
//   setup_test(RIGHT_OUTER_JOIN, 2000,
//       200000 * 3, false, [](int64_t id, int64_t) { return id % 3 == 0 ? 2 : 0; },
//       200000 * 5, false, [](int64_t id, int64_t) { return id % 5 == 0 ? 2 : 0; });
//   ASSERT_FALSE(HasFatalFailure());
//   run_test(10);
//   ASSERT_FALSE(HasFatalFailure());

//   hash_mem = TEST_HJ_DUMP_GET_HASH_AREA_SIZE();
//   ASSERT_EQ((20 * 1024 * 1024), hash_mem);
//   TEST_HJ_DUMP_SET_HASH_AREA_SIZE(100* 1024 * 1024);
//   hash_mem = TEST_HJ_DUMP_GET_HASH_AREA_SIZE();
//   ASSERT_EQ((100 * 1024 * 1024), hash_mem);
//   ObHashJoin::PART_COUNT = part_cnt_bak;
//   ObHashJoin::MAX_PAGE_COUNT = page_cnt_bak;
// }

// TEST_F(ObHashJoinDumpTest, test_left_outer_recursive)
// {
//   int64_t hash_mem = 0;
//   hash_mem = TEST_HJ_DUMP_GET_HASH_AREA_SIZE();
//   ASSERT_EQ(100 * 1024 * 1024, hash_mem);
//   TEST_HJ_DUMP_SET_HASH_AREA_SIZE(20* 1024 * 1024);
//   auto part_cnt_bak = ObHashJoin::PART_COUNT;
//   auto page_cnt_bak = ObHashJoin::MAX_PAGE_COUNT;
//   ObHashJoin::PART_COUNT = 5;
//   ObHashJoin::MAX_PAGE_COUNT = (20L << 20) / OB_MALLOC_MIDDLE_BLOCK_SIZE; // 20MB memory
//   setup_test(LEFT_OUTER_JOIN, 2000,
//       200000 * 3, false, [](int64_t id, int64_t) { return id % 3 == 0 ? 2 : 0; },
//       200000 * 5, false, [](int64_t id, int64_t) { return id % 5 == 0 ? 2 : 0; });
//   ASSERT_FALSE(HasFatalFailure());
//   run_test(10);
//   ASSERT_FALSE(HasFatalFailure());

//   hash_mem = TEST_HJ_DUMP_GET_HASH_AREA_SIZE();
//   ASSERT_EQ((20 * 1024 * 1024), hash_mem);
//   TEST_HJ_DUMP_SET_HASH_AREA_SIZE(100* 1024 * 1024);
//   hash_mem = TEST_HJ_DUMP_GET_HASH_AREA_SIZE();
//   ASSERT_EQ((100 * 1024 * 1024), hash_mem);
//   ObHashJoin::PART_COUNT = part_cnt_bak;
//   ObHashJoin::MAX_PAGE_COUNT = page_cnt_bak;
// }

// TEST_F(ObHashJoinDumpTest, test_left_semi)
// {
//   int64_t hash_mem = 0;
//   hash_mem = TEST_HJ_DUMP_GET_HASH_AREA_SIZE();
//   ASSERT_EQ(100 * 1024 * 1024, hash_mem);
//   TEST_HJ_DUMP_SET_HASH_AREA_SIZE(20* 1024 * 1024);
//   auto part_cnt_bak = ObHashJoin::PART_COUNT;
//   auto page_cnt_bak = ObHashJoin::MAX_PAGE_COUNT;
//   ObHashJoin::PART_COUNT = 5;
//   ObHashJoin::MAX_PAGE_COUNT = (20L << 20) / OB_MALLOC_MIDDLE_BLOCK_SIZE; // 20MB memory
//   setup_test(LEFT_SEMI_JOIN, 2000,
//       200000 * 3, false, [](int64_t id, int64_t) { return id % 3 == 0 ? 2 : 0; },
//       200000 * 5, false, [](int64_t id, int64_t) { return id % 5 == 0 ? 2 : 0; });
//   ASSERT_FALSE(HasFatalFailure());
//   run_test(10);
//   ASSERT_FALSE(HasFatalFailure());

//   hash_mem = TEST_HJ_DUMP_GET_HASH_AREA_SIZE();
//   ASSERT_EQ((20 * 1024 * 1024), hash_mem);
//   TEST_HJ_DUMP_SET_HASH_AREA_SIZE(100* 1024 * 1024);
//   hash_mem = TEST_HJ_DUMP_GET_HASH_AREA_SIZE();
//   ASSERT_EQ((100 * 1024 * 1024), hash_mem);
//   ObHashJoin::PART_COUNT = part_cnt_bak;
//   ObHashJoin::MAX_PAGE_COUNT = page_cnt_bak;
// }


// TEST_F(ObHashJoinDumpTest, test_left_anti)
// {
//   int64_t hash_mem = 0;
//   hash_mem = TEST_HJ_DUMP_GET_HASH_AREA_SIZE();
//   ASSERT_EQ(100 * 1024 * 1024, hash_mem);
//   TEST_HJ_DUMP_SET_HASH_AREA_SIZE(20* 1024 * 1024);
//   auto part_cnt_bak = ObHashJoin::PART_COUNT;
//   auto page_cnt_bak = ObHashJoin::MAX_PAGE_COUNT;
//   ObHashJoin::PART_COUNT = 5;
//   ObHashJoin::MAX_PAGE_COUNT = (20L << 20) / OB_MALLOC_MIDDLE_BLOCK_SIZE; // 20MB memory
//   setup_test(LEFT_ANTI_JOIN, 2000,
//       200000 * 3, false, [](int64_t id, int64_t) { return id % 3 == 0 ? 2 : 0; },
//       200000 * 5, false, [](int64_t id, int64_t) { return id % 5 == 0 ? 2 : 0; });
//   ASSERT_FALSE(HasFatalFailure());
//   run_test(10);
//   ASSERT_FALSE(HasFatalFailure());

//   hash_mem = TEST_HJ_DUMP_GET_HASH_AREA_SIZE();
//   ASSERT_EQ((20 * 1024 * 1024), hash_mem);
//   TEST_HJ_DUMP_SET_HASH_AREA_SIZE(100* 1024 * 1024);
//   hash_mem = TEST_HJ_DUMP_GET_HASH_AREA_SIZE();
//   ASSERT_EQ((100 * 1024 * 1024), hash_mem);
//   ObHashJoin::PART_COUNT = part_cnt_bak;
//   ObHashJoin::MAX_PAGE_COUNT = page_cnt_bak;
// }

// TEST_F(ObHashJoinDumpTest, test_right_semi_recursive)
// {
//   int64_t hash_mem = 0;
//   hash_mem = TEST_HJ_DUMP_GET_HASH_AREA_SIZE();
//   ASSERT_EQ(100 * 1024 * 1024, hash_mem);
//   TEST_HJ_DUMP_SET_HASH_AREA_SIZE(20* 1024 * 1024);
//   auto part_cnt_bak = ObHashJoin::PART_COUNT;
//   auto page_cnt_bak = ObHashJoin::MAX_PAGE_COUNT;
//   ObHashJoin::PART_COUNT = 5;
//   ObHashJoin::MAX_PAGE_COUNT = (20L << 20) / OB_MALLOC_MIDDLE_BLOCK_SIZE; // 20MB memory
//   setup_test(RIGHT_SEMI_JOIN, 2000,
//       200000 * 3, false, [](int64_t id, int64_t) { return id % 3 == 0 ? 2 : 0; },
//       200000 * 5, false, [](int64_t id, int64_t) { return id % 5 == 0 ? 2 : 0; });
//   ASSERT_FALSE(HasFatalFailure());
//   run_test(10);
//   ASSERT_FALSE(HasFatalFailure());

//   hash_mem = TEST_HJ_DUMP_GET_HASH_AREA_SIZE();
//   ASSERT_EQ((20 * 1024 * 1024), hash_mem);
//   TEST_HJ_DUMP_SET_HASH_AREA_SIZE(100* 1024 * 1024);
//   hash_mem = TEST_HJ_DUMP_GET_HASH_AREA_SIZE();
//   ASSERT_EQ((100 * 1024 * 1024), hash_mem);
//   ObHashJoin::PART_COUNT = part_cnt_bak;
//   ObHashJoin::MAX_PAGE_COUNT = page_cnt_bak;
// }

// TEST_F(ObHashJoinDumpTest, test_right_anti_recursive)
// {
//   int64_t hash_mem = 0;
//   hash_mem = TEST_HJ_DUMP_GET_HASH_AREA_SIZE();
//   ASSERT_EQ(100 * 1024 * 1024, hash_mem);
//   TEST_HJ_DUMP_SET_HASH_AREA_SIZE(20* 1024 * 1024);
//   auto part_cnt_bak = ObHashJoin::PART_COUNT;
//   auto page_cnt_bak = ObHashJoin::MAX_PAGE_COUNT;
//   ObHashJoin::PART_COUNT = 5;
//   ObHashJoin::MAX_PAGE_COUNT = (20L << 20) / OB_MALLOC_MIDDLE_BLOCK_SIZE; // 20MB memory
//   setup_test(RIGHT_ANTI_JOIN, 2000,
//       200000 * 3, false, [](int64_t id, int64_t) { return id % 3 == 0 ? 2 : 0; },
//       200000 * 5, false, [](int64_t id, int64_t) { return id % 5 == 0 ? 2 : 0; });
//   ASSERT_FALSE(HasFatalFailure());
//   run_test(10);
//   ASSERT_FALSE(HasFatalFailure());

//   hash_mem = TEST_HJ_DUMP_GET_HASH_AREA_SIZE();
//   ASSERT_EQ((20 * 1024 * 1024), hash_mem);
//   TEST_HJ_DUMP_SET_HASH_AREA_SIZE(100* 1024 * 1024);
//   hash_mem = TEST_HJ_DUMP_GET_HASH_AREA_SIZE();
//   ASSERT_EQ((100 * 1024 * 1024), hash_mem);
//   ObHashJoin::PART_COUNT = part_cnt_bak;
//   ObHashJoin::MAX_PAGE_COUNT = page_cnt_bak;
// }

// TEST_F(ObHashJoinDumpTest, test_run_recursive)
// {
//   int64_t hash_mem = 0;
//   hash_mem = TEST_HJ_DUMP_GET_HASH_AREA_SIZE();
//   ASSERT_EQ(100 * 1024 * 1024, hash_mem);
//   TEST_HJ_DUMP_SET_HASH_AREA_SIZE(20* 1024 * 1024);
//   int64_t algo = ObHashJoin::HJ_PROCESSOR_ALGO;
//   ObHashJoin::HJ_PROCESSOR_ALGO = ObHashJoin::ENABLE_HJ_RECURSIVE;
//   auto part_cnt_bak = ObHashJoin::PART_COUNT;
//   auto page_cnt_bak = ObHashJoin::MAX_PAGE_COUNT;
//   ObHashJoin::PART_COUNT = 5;
//   ObHashJoin::MAX_PAGE_COUNT = (20L << 20) / OB_MALLOC_MIDDLE_BLOCK_SIZE; // 20MB memory
//   setup_test(RIGHT_ANTI_JOIN, 2000,
//       200000 * 3, false, [](int64_t id, int64_t) { return id % 3 == 0 ? 2 : 0; },
//       200000 * 5, false, [](int64_t id, int64_t) { return id % 5 == 0 ? 2 : 0; });
//   ASSERT_FALSE(HasFatalFailure());
//   run_test(10);
//   ASSERT_FALSE(HasFatalFailure());

//   ObHashJoin::HJ_PROCESSOR_ALGO = algo;
//   hash_mem = TEST_HJ_DUMP_GET_HASH_AREA_SIZE();
//   ASSERT_EQ((20 * 1024 * 1024), hash_mem);
//   TEST_HJ_DUMP_SET_HASH_AREA_SIZE(100* 1024 * 1024);
//   hash_mem = TEST_HJ_DUMP_GET_HASH_AREA_SIZE();
//   ASSERT_EQ((100 * 1024 * 1024), hash_mem);
//   ObHashJoin::PART_COUNT = part_cnt_bak;
//   ObHashJoin::MAX_PAGE_COUNT = page_cnt_bak;
// }

// TEST_F(ObHashJoinDumpTest, test_run_nest_loop)
// {
//   int64_t hash_mem = 0;
//   hash_mem = TEST_HJ_DUMP_GET_HASH_AREA_SIZE();
//   ASSERT_EQ(100 * 1024 * 1024, hash_mem);
//   TEST_HJ_DUMP_SET_HASH_AREA_SIZE(20* 1024 * 1024);
//   int64_t algo = ObHashJoin::HJ_PROCESSOR_ALGO;
//   ObHashJoin::HJ_PROCESSOR_ALGO = ObHashJoin::ENABLE_HJ_NEST_LOOP;
//   auto part_cnt_bak = ObHashJoin::PART_COUNT;
//   auto page_cnt_bak = ObHashJoin::MAX_PAGE_COUNT;
//   ObHashJoin::PART_COUNT = 5;
//   ObHashJoin::MAX_PAGE_COUNT = (20L << 20) / OB_MALLOC_MIDDLE_BLOCK_SIZE; // 20MB memory
//   setup_test(RIGHT_ANTI_JOIN, 2000,
//       200000 * 3, false, [](int64_t id, int64_t) { return id % 3 == 0 ? 2 : 0; },
//       300000 * 5, false, [](int64_t id, int64_t) { return id % 5 == 0 ? 2 : 0; });
//   ASSERT_FALSE(HasFatalFailure());
//   run_test(10);
//   ASSERT_FALSE(HasFatalFailure());

//   ObHashJoin::HJ_PROCESSOR_ALGO = algo;
//   hash_mem = TEST_HJ_DUMP_GET_HASH_AREA_SIZE();
//   ASSERT_EQ((20 * 1024 * 1024), hash_mem);
//   TEST_HJ_DUMP_SET_HASH_AREA_SIZE(100* 1024 * 1024);
//   hash_mem = TEST_HJ_DUMP_GET_HASH_AREA_SIZE();
//   ASSERT_EQ((100 * 1024 * 1024), hash_mem);
//   ObHashJoin::PART_COUNT = part_cnt_bak;
//   ObHashJoin::MAX_PAGE_COUNT = page_cnt_bak;
// }

// TEST_F(ObHashJoinDumpTest, test_run_nest_loop_to_recursive)
// {
//   int64_t hash_mem = 0;
//   hash_mem = TEST_HJ_DUMP_GET_HASH_AREA_SIZE();
//   ASSERT_EQ(100 * 1024 * 1024, hash_mem);
//   TEST_HJ_DUMP_SET_HASH_AREA_SIZE(20* 1024 * 1024);
//   bool pre_nest_loop_to_recursive = ObHashJoin::TEST_NEST_LOOP_TO_RECURSIVE;
//   ObHashJoin::TEST_NEST_LOOP_TO_RECURSIVE = true;
//   int64_t algo = ObHashJoin::HJ_PROCESSOR_ALGO;
//   ObHashJoin::HJ_PROCESSOR_ALGO = ObHashJoin::ENABLE_HJ_NEST_LOOP;
//   auto part_cnt_bak = ObHashJoin::PART_COUNT;
//   auto page_cnt_bak = ObHashJoin::MAX_PAGE_COUNT;
//   ObHashJoin::PART_COUNT = 5;
//   ObHashJoin::MAX_PAGE_COUNT = (20L << 20) / OB_MALLOC_MIDDLE_BLOCK_SIZE; // 20MB memory
//   setup_test(RIGHT_ANTI_JOIN, 2000,
//       200000 * 3, false, [](int64_t id, int64_t) { return id % 3 == 0 ? 2 : 0; },
//       300000 * 5, false, [](int64_t id, int64_t) { return id % 5 == 0 ? 2 : 0; });
//   ASSERT_FALSE(HasFatalFailure());
//   run_test(10);
//   ASSERT_FALSE(HasFatalFailure());

//   ObHashJoin::TEST_NEST_LOOP_TO_RECURSIVE = pre_nest_loop_to_recursive;
//   pre_nest_loop_to_recursive = ObHashJoin::TEST_NEST_LOOP_TO_RECURSIVE;
//   ASSERT_EQ(false, pre_nest_loop_to_recursive);
//   ObHashJoin::HJ_PROCESSOR_ALGO = algo;
//   hash_mem = TEST_HJ_DUMP_GET_HASH_AREA_SIZE();
//   ASSERT_EQ((20 * 1024 * 1024), hash_mem);
//   TEST_HJ_DUMP_SET_HASH_AREA_SIZE(100* 1024 * 1024);
//   hash_mem = TEST_HJ_DUMP_GET_HASH_AREA_SIZE();
//   ASSERT_EQ((100 * 1024 * 1024), hash_mem);
//   ObHashJoin::PART_COUNT = part_cnt_bak;
//   ObHashJoin::MAX_PAGE_COUNT = page_cnt_bak;
// }

// TEST_F(ObHashJoinDumpTest, test_run_in_memory)
// {
//   int64_t hash_mem = 0;
//   hash_mem = TEST_HJ_DUMP_GET_HASH_AREA_SIZE();
//   ASSERT_EQ(100 * 1024 * 1024, hash_mem);

//   int64_t algo = ObHashJoin::HJ_PROCESSOR_ALGO;
//   ObHashJoin::HJ_PROCESSOR_ALGO = ObHashJoin::ENABLE_HJ_IN_MEMORY;
//   auto part_cnt_bak = ObHashJoin::PART_COUNT;
//   auto page_cnt_bak = ObHashJoin::MAX_PAGE_COUNT;
//   ObHashJoin::PART_COUNT = 5;
//   ObHashJoin::MAX_PAGE_COUNT = (20L << 20) / OB_MALLOC_MIDDLE_BLOCK_SIZE; // 20MB memory
//   setup_test(RIGHT_ANTI_JOIN, 2000,
//       200000 * 3, false, [](int64_t id, int64_t) { return id % 3 == 0 ? 2 : 0; },
//       300000 * 5, false, [](int64_t id, int64_t) { return id % 5 == 0 ? 2 : 0; });
//   ASSERT_FALSE(HasFatalFailure());
//   run_test(10);
//   ASSERT_FALSE(HasFatalFailure());

//   ObHashJoin::HJ_PROCESSOR_ALGO = algo;
//   hash_mem = TEST_HJ_DUMP_GET_HASH_AREA_SIZE();
//   ASSERT_EQ((100 * 1024 * 1024), hash_mem);
//   ObHashJoin::PART_COUNT = part_cnt_bak;
//   ObHashJoin::MAX_PAGE_COUNT = page_cnt_bak;
// }

// TEST_F(ObHashJoinDumpTest, test_in_memory)
// {
//   int64_t hash_mem = 0;
//   hash_mem = TEST_HJ_DUMP_GET_HASH_AREA_SIZE();
//   ASSERT_EQ(100 * 1024 * 1024, hash_mem);
//   TEST_HJ_DUMP_SET_HASH_AREA_SIZE(20* 1024 * 1024);
//   auto part_cnt_bak = ObHashJoin::PART_COUNT;
//   auto page_cnt_bak = ObHashJoin::MAX_PAGE_COUNT;
//   ObHashJoin::PART_COUNT = 5;
//   ObHashJoin::MAX_PAGE_COUNT = (20L << 20) / OB_MALLOC_MIDDLE_BLOCK_SIZE; // 20MB memory
//   setup_test(RIGHT_ANTI_JOIN, 2000,
//       100000 * 3, false, [](int64_t id, int64_t) { return id % 3 == 0 ? 2 : 0; },
//       2000000 * 5, false, [](int64_t id, int64_t) { return id % 5 == 0 ? 2 : 0; });
//   ASSERT_FALSE(HasFatalFailure());
//   run_test(10);
//   ASSERT_FALSE(HasFatalFailure());

//   hash_mem = TEST_HJ_DUMP_GET_HASH_AREA_SIZE();
//   ASSERT_EQ((20 * 1024 * 1024), hash_mem);
//   TEST_HJ_DUMP_SET_HASH_AREA_SIZE(100* 1024 * 1024);
//   hash_mem = TEST_HJ_DUMP_GET_HASH_AREA_SIZE();
//   ASSERT_EQ((100 * 1024 * 1024), hash_mem);
//   ObHashJoin::PART_COUNT = part_cnt_bak;
//   ObHashJoin::MAX_PAGE_COUNT = page_cnt_bak;
// }

// TEST_F(ObHashJoinDumpTest, test_bigger_left)
// {
//   int64_t hash_mem = 0;
//   hash_mem = TEST_HJ_DUMP_GET_HASH_AREA_SIZE();
//   ASSERT_EQ(100 * 1024 * 1024, hash_mem);
//   TEST_HJ_DUMP_SET_HASH_AREA_SIZE(20* 1024 * 1024);
//   auto part_cnt_bak = ObHashJoin::PART_COUNT;
//   auto page_cnt_bak = ObHashJoin::MAX_PAGE_COUNT;
//   ObHashJoin::PART_COUNT = 5;
//   ObHashJoin::MAX_PAGE_COUNT = (20L << 20) / OB_MALLOC_MIDDLE_BLOCK_SIZE; // 20MB memory
//   setup_test(LEFT_OUTER_JOIN, 2000,
//       2000000 * 3, false, [](int64_t id, int64_t) { return id % 3 == 0 ? 2 : 0; },
//       2 * 5, false, [](int64_t id, int64_t) { return id % 5 == 0 ? 2 : 0; });
//   ASSERT_FALSE(HasFatalFailure());
//   run_test(10);
//   ASSERT_FALSE(HasFatalFailure());

//   hash_mem = TEST_HJ_DUMP_GET_HASH_AREA_SIZE();
//   ASSERT_EQ((20 * 1024 * 1024), hash_mem);
//   TEST_HJ_DUMP_SET_HASH_AREA_SIZE(100* 1024 * 1024);
//   hash_mem = TEST_HJ_DUMP_GET_HASH_AREA_SIZE();
//   ASSERT_EQ((100 * 1024 * 1024), hash_mem);
//   ObHashJoin::PART_COUNT = part_cnt_bak;
//   ObHashJoin::MAX_PAGE_COUNT = page_cnt_bak;
// }

// TEST_F(ObHashJoinDumpTest, test_bigger_left_using_recursive)
// {
//   int64_t hash_mem = 0;
//   hash_mem = TEST_HJ_DUMP_GET_HASH_AREA_SIZE();
//   ASSERT_EQ(100 * 1024 * 1024, hash_mem);
//   TEST_HJ_DUMP_SET_HASH_AREA_SIZE(20* 1024 * 1024);
//   int64_t algo = ObHashJoin::HJ_PROCESSOR_ALGO;
//   ObHashJoin::HJ_PROCESSOR_ALGO = ObHashJoin::ENABLE_HJ_RECURSIVE;
//   auto part_cnt_bak = ObHashJoin::PART_COUNT;
//   auto page_cnt_bak = ObHashJoin::MAX_PAGE_COUNT;
//   ObHashJoin::PART_COUNT = 5;
//   ObHashJoin::MAX_PAGE_COUNT = (20L << 20) / OB_MALLOC_MIDDLE_BLOCK_SIZE; // 20MB memory
//   setup_test(LEFT_OUTER_JOIN, 2000,
//       200000 * 3, false, [](int64_t id, int64_t) { return id % 3 == 0 ? 2 : 0; },
//       2 * 5, false, [](int64_t id, int64_t) { return id % 5 == 0 ? 2 : 0; });
//   ASSERT_FALSE(HasFatalFailure());
//   run_test(10);
//   ASSERT_FALSE(HasFatalFailure());

//   ObHashJoin::HJ_PROCESSOR_ALGO = algo;
//   hash_mem = TEST_HJ_DUMP_GET_HASH_AREA_SIZE();
//   ASSERT_EQ((20 * 1024 * 1024), hash_mem);
//   TEST_HJ_DUMP_SET_HASH_AREA_SIZE(100* 1024 * 1024);
//   hash_mem = TEST_HJ_DUMP_GET_HASH_AREA_SIZE();
//   ASSERT_EQ((100 * 1024 * 1024), hash_mem);
//   ObHashJoin::PART_COUNT = part_cnt_bak;
//   ObHashJoin::MAX_PAGE_COUNT = page_cnt_bak;
// }

TEST_F(ObHashJoinDumpTest, test_bad_case)
{
  setup_test(INNER_JOIN, 2000,
      1, false, [](int64_t, int64_t) { return 400000; },
      200000, false, [](int64_t, int64_t) { return 2; });
  ASSERT_FALSE(HasFatalFailure());
  run_test(10);
  ASSERT_FALSE(HasFatalFailure());
}

TEST_F(ObHashJoinDumpTest, test_right_join)
{
  setup_test(RIGHT_SEMI_JOIN, 2000,
      1, false, [](int64_t, int64_t) { return 400000; },
      200000, false, [](int64_t, int64_t) { return 2; });
  ASSERT_FALSE(HasFatalFailure());
  run_test(10);
  ASSERT_FALSE(HasFatalFailure());
}

TEST_F(ObHashJoinDumpTest, test_anti_right_join)
{
  setup_test(RIGHT_ANTI_JOIN, 2000,
      1, false, [](int64_t, int64_t) { return 400000; },
      200000, false, [](int64_t, int64_t) { return 2; });
  ASSERT_FALSE(HasFatalFailure());
  run_test(10);
  ASSERT_FALSE(HasFatalFailure());
}

// see
TEST_F(ObHashJoinDumpTest, test_file_leak)
{
  int ret = OB_SUCCESS;
  setup_test(INNER_JOIN, 2000,
      200000 * 3, false, [](int64_t id, int64_t) { return id % 3 == 0 ? 2 : 0; },
      200000 * 5, false, [](int64_t id, int64_t) { return id % 5 == 0 ? 2 : 0; });
  ASSERT_FALSE(HasFatalFailure());
  hash_plan_.left_.iter_end_ret_ = OB_ERR_SYS;
  hash_plan_.right_.iter_end_ret_ = OB_ERR_SYS;
  ASSERT_EQ(OB_SUCCESS, hash_join_.open(hash_plan_.exec_ctx_));
  const ObNewRow *row = NULL;
  while (OB_SUCC(hash_join_.get_next_row(hash_plan_.exec_ctx_, row))) {
  }
  ASSERT_EQ(OB_ERR_SYS, ret);
  hash_join_.close(hash_plan_.exec_ctx_);
}

TEST_F(ObHashJoinDumpTest, test_conf)
{
  int64_t hash_mem = 0;
  hash_mem = TEST_HJ_DUMP_GET_HASH_AREA_SIZE();
  ASSERT_EQ(100 * 1024 * 1024, hash_mem);

  TEST_HJ_DUMP_SET_HASH_AREA_SIZE(1024 * 1024 * 1024);
  hash_mem = TEST_HJ_DUMP_GET_HASH_AREA_SIZE();
  ASSERT_EQ((1024 * 1024 * 1024), hash_mem);
}

int ObHashJoinDumpTest::init_tenant_mgr()
{
  int ret = OB_SUCCESS;
  ObAddr self;
  oceanbase::rpc::frame::ObReqTransport req_transport(NULL, NULL);
  oceanbase::obrpc::ObSrvRpcProxy rpc_proxy;
  oceanbase::obrpc::ObCommonRpcProxy rs_rpc_proxy;
  oceanbase::share::ObRsMgr rs_mgr;
  int64_t tenant_id = OB_SYS_TENANT_ID;
  self.set_ip_addr("127.0.0.1", 8086);
  ret = ObTenantConfigMgr::get_instance().add_tenant_config(tenant_id);
  EXPECT_EQ(OB_SUCCESS, ret);
  ret = getter.add_tenant(tenant_id,
                          2L * 1024L * 1024L * 1024L, 4L * 1024L * 1024L * 1024L);
  EXPECT_EQ(OB_SUCCESS, ret);
  const int64_t ulmt = 128LL << 30;
  const int64_t llmt = 128LL << 30;
  ret = getter.add_tenant(OB_SERVER_TENANT_ID,
                          ulmt,
                          llmt);
  EXPECT_EQ(OB_SUCCESS, ret);
  oceanbase::lib::set_memory_limit(128LL << 32);
  return ret;
}

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
