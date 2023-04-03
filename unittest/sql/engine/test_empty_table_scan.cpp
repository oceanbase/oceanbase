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

#include <gtest/gtest.h>
#include "sql/ob_sql_init.h"
#include "sql/engine/table/ob_table_scan.h"
#include "sql/optimizer/test_optimizer_utils.h"

#define ObCacheObjectFactory test::MockCacheObjectFactory
using namespace oceanbase::common;
using namespace oceanbase::sql;
using namespace oceanbase::share;

class ObEmptyTableScanTest : public ::testing::Test
{
public:
  const static int64_t TEST_COL_NUM = 2;

  const static int64_t TEST_TABLE_ID = 1;
  const static int64_t TEST_INDEX_ID = 1;
  const static uint64_t COLUMN_ID_1 = 16;
  const static uint64_t COLUMN_ID_2 = 17;

  const static int64_t TEST_LIMIT = 10;
  const static int64_t TEST_OFFSET = 0;


  ObEmptyTableScanTest();
  virtual ~ObEmptyTableScanTest();
  virtual void SetUp();
  virtual void TearDown();
private:
  // disallow copy
  ObEmptyTableScanTest(const ObEmptyTableScanTest &other);
  ObEmptyTableScanTest& operator=(const ObEmptyTableScanTest &other);
private:
  // data members
};
ObEmptyTableScanTest::ObEmptyTableScanTest()
{
}

ObEmptyTableScanTest::~ObEmptyTableScanTest()
{
}

void ObEmptyTableScanTest::SetUp()
{
}

void ObEmptyTableScanTest::TearDown()
{
}

TEST_F(ObEmptyTableScanTest, basic_test)
{
  ObExecContext exec_ctx;
  ObArenaAllocator allocator;
  ObSQLSessionInfo session;
  ObPhyTableLocationFixedArray table_locs;
  ObPhyTableLocation table_loc;
  ObPartitionReplicaLocation partition_loc;
  partition_loc.set_table_id(TEST_TABLE_ID);
  partition_loc.set_partition_id(9);
  table_loc.set_table_location_key(TEST_TABLE_ID, TEST_TABLE_ID);
  ASSERT_EQ(OB_SUCCESS, session.test_init(0, 0, 0, &allocator));
  ASSERT_EQ(OB_SUCCESS, session.load_default_configs_in_pc());
  ASSERT_EQ(OB_SUCCESS, table_loc.add_partition_location(partition_loc));
  ASSERT_EQ(OB_SUCCESS, table_locs.push_back(table_loc));
  exec_ctx.set_my_session(&session);
  exec_ctx.create_physical_plan_ctx();
  ObPhysicalPlanCtx *plan_ctx = exec_ctx.get_physical_plan_ctx();
  ASSERT_TRUE(NULL != plan_ctx);
  plan_ctx->set_timeout_timestamp(::oceanbase::common::ObTimeUtility::current_time() + 2000L * 1000L);
  ObTaskExecutorCtx *task_exe_ctx = exec_ctx.get_task_executor_ctx();
  ASSERT_TRUE(NULL != task_exe_ctx);
  ASSERT_EQ(OB_SUCCESS, task_exe_ctx->init_table_location(table_locs.count()));
  ASSERT_EQ(OB_SUCCESS, task_exe_ctx->set_table_locations(table_locs));

  ObPhysicalPlan *local_phy_plan = NULL;
  ASSERT_EQ(OB_SUCCESS, ObCacheObjectFactory::alloc(local_phy_plan, OB_SYS_TENANT_ID));
  ObPhyOperator *tmp_op = NULL;
  ObTableScan *scan_op = NULL;
  ASSERT_TRUE(NULL != local_phy_plan);
  ASSERT_EQ(OB_SUCCESS, local_phy_plan->alloc_operator_by_type(PHY_TABLE_SCAN, tmp_op));
  //tmp_op->set_column_count(TEST_COL_NUM);
  ASSERT_TRUE(NULL != (scan_op = static_cast<ObTableScan*>(tmp_op)));
  scan_op->set_table_location_key(TEST_TABLE_ID);
  scan_op->set_ref_table_id(TEST_TABLE_ID);
  scan_op->set_index_table_id(TEST_INDEX_ID);
  SQL_ENG_LOG(INFO, "op info", "op_id", scan_op->get_id(), "op_type", scan_op->get_type());

  ObTableScanInput *scan_input = NULL;
  ASSERT_EQ(OB_SUCCESS, exec_ctx.init_phy_op(local_phy_plan->get_phy_operator_size()));
  ASSERT_EQ(OB_SUCCESS, exec_ctx.init_expr_op(local_phy_plan->get_expr_operator_size()));
  ASSERT_EQ(OB_SUCCESS, scan_op->create_operator_input(exec_ctx));
  ASSERT_TRUE(NULL != (scan_input = GET_PHY_OP_INPUT(ObTableScanInput, exec_ctx, scan_op->get_id())));
  scan_input->set_location_idx(OB_INVALID_INDEX);
}

int main(int argc, char **argv)
{
  //TBSYS_LOGGER.setLogLevel("INFO");
  OB_LOGGER.set_log_level("INFO");
  ::oceanbase::sql::init_sql_factories();
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}




