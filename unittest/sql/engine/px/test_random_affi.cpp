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

#include <iostream>
using namespace std;

#define USING_LOG_PREFIX SQL_EXE
#include <gtest/gtest.h>

#include "sql/ob_sql_init.h"
#include "sql/engine/px/ob_px_util.h"

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::sql;
using namespace oceanbase::storage;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;

class ObRandomAffiTaskSplitTest : public ::testing::Test
{
public:

  const static int64_t TEST_PARTITION_COUNT = 5;
  const static int64_t TEST_SPLIT_TASK_COUNT = 8;

  ObRandomAffiTaskSplitTest() = default;
  virtual ~ObRandomAffiTaskSplitTest() = default;
  virtual void SetUp() {};
  virtual void TearDown() {};

private:
  // disallow copy
  ObRandomAffiTaskSplitTest(const ObRandomAffiTaskSplitTest &other);
  ObRandomAffiTaskSplitTest& operator=(const ObRandomAffiTaskSplitTest &other);
};

TEST_F(ObRandomAffiTaskSplitTest, split_task_test) {
  {
    int64_t parallel = 3;
    int64_t tenant_id = 1;
    ObPxTabletInfo px_part_info;
    ObPxAffinityByRandom affinitize_rule(true);
    for (int i = 0; i < 5; ++i) {
      px_part_info.physical_row_count_ = (10 - i) * 100;
      affinitize_rule.add_partition(i,i,parallel,tenant_id,px_part_info);
    }
    affinitize_rule.do_random(true, tenant_id);
    const common::ObIArray<ObPxAffinityByRandom::TabletHashValue>& result = affinitize_rule.get_result();
    for (int i = 0; i < result.count(); ++i) {
      LOG_INFO("result", K(result.at(i).tablet_id_), K(result.at(i).worker_id_), K(result.at(i).partition_info_.physical_row_count_));
    }
    ASSERT_EQ(1, result.at(0).worker_id_);
    ASSERT_EQ(0, result.at(1).worker_id_);
    ASSERT_EQ(2, result.at(2).worker_id_);
    ASSERT_EQ(2, result.at(3).worker_id_);
    ASSERT_EQ(1, result.at(4).worker_id_);
  }

  {
    int64_t parallel = 16;
    int64_t tenant_id = 1;
    ObPxTabletInfo px_part_info;
    ObPxAffinityByRandom affinitize_rule(true);

    px_part_info.physical_row_count_ = 3000;
    affinitize_rule.add_partition(0,0,parallel,tenant_id,px_part_info);
    px_part_info.physical_row_count_ = 1000;
    affinitize_rule.add_partition(1,1,parallel,tenant_id,px_part_info);
    px_part_info.physical_row_count_ = 2500;
    affinitize_rule.add_partition(2,2,parallel,tenant_id,px_part_info);
    px_part_info.physical_row_count_ = 3500;
    affinitize_rule.add_partition(3,3,parallel,tenant_id,px_part_info);
    px_part_info.physical_row_count_ = 2000;
    affinitize_rule.add_partition(4,4,parallel,tenant_id,px_part_info);

    affinitize_rule.do_random(true, tenant_id);

    const common::ObIArray<ObPxAffinityByRandom::TabletHashValue>& result = affinitize_rule.get_result();
    for (int i = 0; i < 5; ++i) {
      LOG_INFO("result", K(result.at(i).tablet_id_), K(result.at(i).worker_id_), K(result.at(i).partition_info_.physical_row_count_));
    }
    ASSERT_EQ(4, result.at(0).worker_id_);
    ASSERT_EQ(2, result.at(1).worker_id_);
    ASSERT_EQ(1, result.at(2).worker_id_);
    ASSERT_EQ(0, result.at(3).worker_id_);
    ASSERT_EQ(3, result.at(4).worker_id_);
  }

  {
    int64_t parallel = 3;
    int64_t tenant_id = 1;
    ObPxTabletInfo px_part_info;
    ObPxAffinityByRandom affinitize_rule(true);

    px_part_info.physical_row_count_ = 3000;
    affinitize_rule.add_partition(0,0,parallel,tenant_id,px_part_info);
    px_part_info.physical_row_count_ = 4000;
    affinitize_rule.add_partition(1,1,parallel,tenant_id,px_part_info);
    px_part_info.physical_row_count_ = 2500;
    affinitize_rule.add_partition(2,2,parallel,tenant_id,px_part_info);
    px_part_info.physical_row_count_ = 1500;
    affinitize_rule.add_partition(3,3,parallel,tenant_id,px_part_info);
    px_part_info.physical_row_count_ = 2000;
    affinitize_rule.add_partition(4,4,parallel,tenant_id,px_part_info);

    affinitize_rule.do_random(true, tenant_id);

    const common::ObIArray<ObPxAffinityByRandom::TabletHashValue>& result = affinitize_rule.get_result();
    for (int i = 0; i < 5; ++i) {
      LOG_INFO("result", K(result.at(i).tablet_id_), K(result.at(i).worker_id_), K(result.at(i).partition_info_.physical_row_count_));
    }

    ASSERT_EQ(1, result.at(0).worker_id_);
    ASSERT_EQ(0, result.at(1).worker_id_);
    ASSERT_EQ(2, result.at(2).worker_id_);
    ASSERT_EQ(2, result.at(3).worker_id_);
    ASSERT_EQ(1, result.at(4).worker_id_);
 }

}

int main(int argc, char **argv)
{
  OB_LOGGER.set_log_level("TRACE");
  //oceanbase::common::ObLogger::get_logger().set_log_level("TRACE");
  init_sql_factories();
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
