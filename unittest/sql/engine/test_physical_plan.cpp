/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <gtest/gtest.h>
#include "sql/engine/ob_physical_plan.h"
using namespace oceanbase::common;
using namespace oceanbase::share::schema;
namespace oceanbase
{
namespace sql
{
class TestPhysicalPlan : public ::testing::Test
{
public:
  TestPhysicalPlan() {}
  virtual ~TestPhysicalPlan() {}
  virtual void SetUp() {}
  virtual void TearDown() {}
};

TEST_F(TestPhysicalPlan,  test_get_view_table)
{
  ObPhysicalPlan plan;
  const int64_t VIEW_COUNT = 5;
  EXPECT_EQ(OB_SUCCESS, plan.get_dependency_table().init(VIEW_COUNT));
  for (int64_t i = 0; i < VIEW_COUNT; i++) {
    ObSchemaObjVersion version;
    version.object_id_ = i;
    version.object_type_ = DEPENDENCY_VIEW;
    version.version_ = i;
    EXPECT_EQ(OB_SUCCESS, plan.get_dependency_table().push_back(version));
  }
  EXPECT_EQ(VIEW_COUNT, plan.get_dependency_table_size());
}
} //namespace sql
} //namespace oceanbase


int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
