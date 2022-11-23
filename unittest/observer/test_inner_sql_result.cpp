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

#define USING_LOG_PREFIX SERVER

#include <gtest/gtest.h>
#include <gmock/gmock.h>
#define private public
#define protected public

#include "observer/ob_inner_sql_result.h"
#include "sql/engine/ob_physical_plan.h"
#include "sql/engine/basic/ob_values.h"
#include "sql/plan_cache/ob_cache_object_factory.h"
#include <limits>
#include <cmath>

namespace oceanbase
{
namespace observer
{
using namespace common;
using namespace share;
using namespace schema;
using namespace sql;

class TestInnerSQLResult : public ::testing::Test
{
public:
  TestInnerSQLResult() : session_(), res_(session_) {}

  virtual void SetUp();
  virtual void TearDown() {}

protected:
  const static int64_t ROW_CNT = 2;
  const static int64_t COL_CNT = 12;

  sql::ObSQLSessionInfo session_;
  ObInnerSQLResult res_;
};

static ObArenaAllocator alloc_;
class FakeValues : public ObNoChildrenPhyOperator
{
public:
  FakeValues() : ObNoChildrenPhyOperator(alloc_), idx_(0) {}

  virtual int get_next_row(ObExecContext &, const common::ObNewRow *&row) const
  {
    int ret = OB_SUCCESS;
    if (idx_ < rows_.count()) {
      row = &rows_.at(idx_++);
    } else {
      ret = OB_ITER_END;
    }
    return ret;
  }

  virtual int inner_open(ObExecContext &) const  { return OB_SUCCESS; }
  virtual int inner_close(ObExecContext &) const { return OB_SUCCESS; }
  virtual ObPhyOperatorType get_type() const { return PHY_VALUES; }
  virtual int init_op_ctx(ObExecContext &) const { return OB_SUCCESS; }
  virtual int inner_get_next_row(ObExecContext &, const common::ObNewRow *&) const { return OB_SUCCESS; }

  mutable int64_t idx_;
  ObArray<ObNewRow> rows_;
};

void TestInnerSQLResult::SetUp()
{
  int ret = OB_SUCCESS;
  ASSERT_TRUE(OB_SUCCESS, res_.init());
  ObArenaGFactory::get_instance()->init();
  session_.load_default_sys_variable(true, true);
  ObResultSet &rs = res_.result_set();
  rs.get_exec_context().create_physical_plan_ctx();
  res_.opened_ = true;
  ObObj *objs = new ObObj[ROW_CNT * COL_CNT];
  FakeValues *values = new FakeValues();
  values->set_id(0);
  for (int64_t i = 0; i < ROW_CNT; i++) {
    ObNewRow row;
    row.cells_ = objs + i * COL_CNT;
    row.count_ = COL_CNT;
    row.projector_ = NULL;

    row.cells_[0].set_int(0);
    row.cells_[1].set_float(1);
    row.cells_[2].set_double(2);
    row.cells_[3].set_varchar("varchar");

    ret = values->rows_.push_back(row);
    ASSERT_EQ(OB_SUCCESS, ret);
  }

  ObPhysicalPlan *plan = NULL;
  ASSERT_EQ(OB_SUCCESS, ObCacheObjectFactory::alloc(plan));
  rs.set_physical_plan(plan);

  rs.get_execute_result().set_root_op(values);

  ObField f;

  f.org_cname_ = ObString::make_string("c0");
  f.cname_ = ObString::make_string("c0");
  rs.add_field_column(f);

  f.org_cname_ = ObString::make_string("c1");
  f.cname_ = ObString::make_string("c1");
  rs.add_field_column(f);

  f.org_cname_ = ObString::make_string("c2");
  f.cname_ = ObString::make_string("c2");
  rs.add_field_column(f);

  f.org_cname_ = ObString::make_string("c3");
  f.cname_ = ObString::make_string("c3");
  rs.add_field_column(f);

  f.org_cname_ = ObString::make_string("c0");
  f.cname_ = ObString::make_string("c0");
  rs.add_field_column(f);
}

TEST_F(TestInnerSQLResult, setup)
{
  int ret = OB_SUCCESS;
  const ObNewRow *row = NULL;
  for (int64_t i = 0; i < ROW_CNT; i++) {
    row = NULL;
    ret = res_.result_set().get_next_row(row);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_TRUE(NULL != row);
    LOG_INFO("row", K(*row));
  }
  ret = res_.result_set().get_next_row(row);
  ASSERT_EQ(OB_ITER_END, ret);
}

TEST_F(TestInnerSQLResult, read_by_index)
{
  int ret = res_.next();
  ASSERT_EQ(OB_SUCCESS, ret);

  int64_t int_val = 1;
  ret = res_.get_int(static_cast<int64_t>(0), int_val);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(0, int_val);

  float float_val = 0;
  ret = res_.get_float(1, float_val);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_LT(std::abs(float_val - 1), std::numeric_limits<float>::epsilon());

  double double_val = 0;
  ret = res_.get_double(2, double_val);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_LT(std::abs(double_val - 2), std::numeric_limits<double>::epsilon());

  ObString varchar;
  ret = res_.get_varchar(3, varchar);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(ObString::make_string("varchar"), varchar);

  ret = res_.get_varchar(4, varchar);
  ASSERT_EQ(OB_ERR_NULL_VALUE, ret);
}

TEST_F(TestInnerSQLResult, read_by_name)
{
  int ret = res_.next();
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = res_.next();
  ASSERT_EQ(OB_SUCCESS, ret);

  int64_t int_val = 1;
  ret = res_.get_int("c0", int_val);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(0, int_val);

  float float_val = 0;
  ret = res_.get_float("c1", float_val);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_LT(std::fabs(float_val - 1), std::numeric_limits<float>::epsilon());

  double double_val = 0;
  ret = res_.get_double("c2", double_val);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_LT(std::abs(double_val - 2), std::numeric_limits<double>::epsilon());

  ObString varchar;
  ret = res_.get_varchar("c3", varchar);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(ObString::make_string("varchar"), varchar);

  ret = res_.get_varchar("c4", varchar);
  ASSERT_NE(OB_ENTRY_NOT_EXIST, ret);
}

} // end namespace share
} // end namespace oceanbase

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  OB_LOGGER.set_log_level("INFO");

  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
