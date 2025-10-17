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
#include <stdarg.h>
#include "sql/executor/ob_executor.h"
#include "sql/executor/ob_distributed_scheduler.h"
#include "sql/engine/sort/ob_sort.h"
#include "sql/engine/table/ob_table_scan.h"
#include "sql/executor/ob_root_transmit.h"
#include "sql/executor/ob_distributed_transmit.h"
#include "sql/executor/ob_direct_receive.h"
#include "sql/executor/ob_fifo_receive.h"
#include "sql/executor/ob_distributed_job_control.h"
#include "sql/executor/ob_job_parser.h"
#include "sql/executor/ob_task_spliter_factory.h"
#include "sql/engine/ob_physical_plan_ctx.h"
#include "sql/engine/ob_physical_plan.h"
#include "sql/ob_sql_init.h"
#include "lib/utility/ob_tracepoint.h"

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::sql;
using namespace oceanbase::storage;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;

class ObTableScanTest : public ::testing::Test
{
public:

  const static int64_t TEST_COL_NUM = 2;
  const static int64_t TEST_SPLIT_TASK_COUNT = 7;
  const static int64_t TEST_PARA_SERVER_COUNT = 2;
  const static int64_t TEST_SERVER_PARA_THREAD_COUNT = 3;

  const static int64_t TEST_TABLE_ID = 1;
  const static int64_t TEST_INDEX_ID = 1;
  const static uint64_t COLUMN_ID_1 = 16;
  const static uint64_t COLUMN_ID_2 = 17;

  const static int64_t TEST_LIMIT = 10;
  const static int64_t TEST_OFFSET = 0;


  ObTableScanTest();
  virtual ~ObTableScanTest();
  virtual void SetUp();
  virtual void TearDown();

  ObPhysicalPlan *local_phy_plan_;

  int create_local_plan_tree(ObExecContext &ctx);
private:
  // disallow copy
  ObTableScanTest(const ObTableScanTest &other);
  ObTableScanTest& operator=(const ObTableScanTest &other);
private:
  // data members
};
ObTableScanTest::ObTableScanTest()
{
}

ObTableScanTest::~ObTableScanTest()
{
}

void ObTableScanTest::SetUp()
{
  int ret = OB_SUCCESS;
  for (int64_t i = 1; OB_SUCC(ret) && i <= 5; ++i) {
    for (int64_t j = 1; OB_SUCC(ret) && j <= 10; ++j) {
      ObFakePartitionKey key;
      key.table_id_ = i;
      key.partition_id_ = j;
      ObPartitionLocation location;
      ObReplicaLocation replica_loc;
      replica_loc.server_.set_ip_addr("127.0.0.1", (int32_t)j);
      replica_loc.role_ = common::LEADER;
      if (OB_SUCCESS != (ret = location.add(replica_loc))) {
        SQL_ENG_LOG(WARN, "fail to add replica location", K(ret), K(i), K(j));
      }
    }
  }
}

void ObTableScanTest::TearDown()
{
}

int ObTableScanTest::create_local_plan_tree(ObExecContext &ctx)
{
  UNUSED(ctx);
  int ret = OB_SUCCESS;

  partition_service_.set_col_num(TEST_COL_NUM);
  for (int64_t i = 0; OB_SUCC(ret) && i < 100; ++i) {
    ObNewRow row;
    ObObj objs[TEST_COL_NUM];
    row.count_ = TEST_COL_NUM;
    row.cells_ = objs;
    for (int64_t j = 0; j < TEST_COL_NUM; ++j) {
      row.cells_[j].set_int(i);
    }
    partition_service_.add_row(row);
  }

  local_phy_plan_ = ObPhysicalPlan::alloc();
  ObPhyOperator *cur_op = NULL;
  ObPhyOperator *tmp_op = NULL;
  int err_code = OB_SUCCESS;

  ObSqlExpression *limit_expr = NULL;
  EXPECT_EQ(OB_SUCCESS, ObSqlExpressionUtil::make_sql_expr(local_phy_plan_, limit_expr));
  EXPECT_FALSE(NULL == limit_expr);
  ObPostExprItem limit_expr_item;
  limit_expr_item.set_int(TEST_LIMIT);
  limit_expr_item.set_item_type(T_INT);
  EXPECT_EQ(OB_SUCCESS, limit_expr->add_expr_item(limit_expr_item));

  ObSqlExpression *offset_expr = NULL;
  EXPECT_EQ(OB_SUCCESS, ObSqlExpressionUtil::make_sql_expr(local_phy_plan_, offset_expr));
  EXPECT_FALSE(NULL == offset_expr);
  ObPostExprItem offset_expr_item;
  offset_expr_item.set_int(TEST_OFFSET);
  offset_expr_item.set_item_type(T_INT);
  EXPECT_EQ(OB_SUCCESS, offset_expr->add_expr_item(offset_expr_item));

  /*
   * calculate c0 % TEST_SPLIT_TASK_COUNT
   * */
  ObSqlExpression *hash_expr = NULL;
  EXPECT_EQ(OB_SUCCESS, ObSqlExpressionUtil::make_sql_expr(local_phy_plan_, hash_expr));
  EXPECT_FALSE(NULL == hash_expr);
  ObPostExprItem expr_item;
  expr_item.set_column(0);
  EXPECT_EQ(OB_SUCCESS, hash_expr->add_expr_item(expr_item));
  expr_item.set_int(TEST_SPLIT_TASK_COUNT);
  expr_item.set_item_type(T_INT);
  EXPECT_EQ(OB_SUCCESS, hash_expr->add_expr_item(expr_item));
  expr_item.set_op("%", 2);
  EXPECT_EQ(OB_SUCCESS, hash_expr->add_expr_item(expr_item));

  /*
   * calculate c0 % 1
   * */
  ObSqlExpression *iden_hash_expr = NULL;
  EXPECT_EQ(OB_SUCCESS, ObSqlExpressionUtil::make_sql_expr(local_phy_plan_, iden_hash_expr));
  EXPECT_FALSE(NULL == iden_hash_expr);
  ObPostExprItem iden_expr_item;
  iden_expr_item.set_column(0);
  EXPECT_EQ(OB_SUCCESS, iden_hash_expr->add_expr_item(iden_expr_item));
  iden_expr_item.set_int(1);
  iden_expr_item.set_item_type(T_INT);
  EXPECT_EQ(OB_SUCCESS, iden_hash_expr->add_expr_item(iden_expr_item));
  iden_expr_item.set_op("%", 2);
  EXPECT_EQ(OB_SUCCESS, iden_hash_expr->add_expr_item(iden_expr_item));

  ObPhysicalPlanCtx *plan_ctx = ctx.get_physical_plan_ctx();
  common::ObIArray<common::ObObj> &param_store = plan_ctx->get_param_store();
  ObObj value1;
  value1.set_int(3);
  param_store.push_back(value1);
  ObObj value2;
  value2.set_int(1);
  param_store.push_back(value2);
  ObObj value3;
  value3.set_int(2);
  param_store.push_back(value3);


  /*
   * calculate a + ?
   * */
  ObSqlExpression *partition_func = NULL;
  EXPECT_EQ(OB_SUCCESS, ObSqlExpressionUtil::make_sql_expr(local_phy_plan_, partition_func));
  EXPECT_FALSE(NULL == partition_func);
  ObPostExprItem partition_func_item;
  partition_func_item.set_column(0);
  EXPECT_EQ(OB_SUCCESS, partition_func->add_expr_item(partition_func_item));
  ObObj index_value;
  index_value.set_int(2);
  partition_func_item.assign(T_QUESTIONMARK, index_value);
  EXPECT_EQ(OB_SUCCESS, partition_func->add_expr_item(partition_func_item));
  partition_func_item.set_op("+", 2);
  EXPECT_EQ(OB_SUCCESS, partition_func->add_expr_item(partition_func_item));

  ObColumnRefRawExpr ref_col(TEST_TABLE_ID, COLUMN_ID_1, T_REF_COLUMN);
  ObArray<ColumnItem> single_range_columns;
  ColumnItem col;
  col.column_id_ = COLUMN_ID_1;
  col.table_id_ = TEST_TABLE_ID;
  col.data_type_ = ObIntType;
  col.column_name_ = ObString::make_string("a");
  EXPECT_EQ(OB_SUCCESS, single_range_columns.push_back(col));
  ref_col.add_flag(IS_COLUMN);
  // 构造 (a = ?)
  ObObj index1;
  index1.set_unknown(0);
  ObConstRawExpr const_col1(index1, T_QUESTIONMARK);
  const_col1.add_flag(IS_PARAM);
  ObOpRawExpr condition1(&ref_col, &const_col1, T_OP_EQ); // a = ?构造完毕
  // 构造 (a > ?)
  ObObj index2;
  index2.set_unknown(1);
  ObConstRawExpr const_col2(index2, T_QUESTIONMARK);
  const_col2.add_flag(IS_PARAM);
  ObOpRawExpr condition2(&ref_col, &const_col2, T_OP_GT); // a > ?构造完毕
  ObQueryRange *scan_query_range = OB_NEW(ObQueryRange, ObModIds::TEST);
  EXPECT_EQ(OB_SUCCESS, scan_query_range->preliminary_extract_query_range(single_range_columns, &condition2));

  ASSERT_EQ(OB_SUCCESS, local_phy_plan_->alloc_operator_by_type(PHY_TABLE_SCAN, tmp_op));
  tmp_op->set_column_count(TEST_COL_NUM);
  static_cast<ObTableScan*>(tmp_op)->set_table_id(TEST_TABLE_ID);
  static_cast<ObTableScan*>(tmp_op)->set_ref_table_id(TEST_TABLE_ID);
  static_cast<ObTableScan*>(tmp_op)->set_index_table_id(TEST_INDEX_ID);
  static_cast<ObTableScan*>(tmp_op)->add_range_column(COLUMN_ID_1);
  static_cast<ObTableScan*>(tmp_op)->add_output_column(COLUMN_ID_1);
  static_cast<ObTableScan*>(tmp_op)->add_output_column(COLUMN_ID_2);
  static_cast<ObTableScan*>(tmp_op)->set_limit_offset(*limit_expr, *offset_expr);
  static_cast<ObTableScan*>(tmp_op)->set_query_range(*scan_query_range);
  cur_op = tmp_op;
  SQL_ENG_LOG(INFO, "op info", "op_id", cur_op->get_id(), "op_type", cur_op->get_type());

  ASSERT_EQ(OB_SUCCESS, local_phy_plan_->alloc_operator_by_type(PHY_ROOT_TRANSMIT, tmp_op));
  tmp_op->set_column_count(TEST_COL_NUM);
  tmp_op->set_child(0, *cur_op);
  static_cast<ObTransmit*>(tmp_op)->get_job_conf().set_scan_table_id(TEST_TABLE_ID, TEST_INDEX_ID);
  static_cast<ObTransmit*>(tmp_op)->get_job_conf().set_task_split_type(ObTaskSpliter::IDENTITY_SPLIT);
  static_cast<ObTransmit*>(tmp_op)->set_split_task_count(TEST_SPLIT_TASK_COUNT);
  static_cast<ObTransmit*>(tmp_op)->set_parallel_server_count(TEST_PARA_SERVER_COUNT);
  static_cast<ObTransmit*>(tmp_op)->set_server_parallel_thread_count(TEST_SERVER_PARA_THREAD_COUNT);
  static_cast<ObTransmit*>(tmp_op)->set_shuffle_func(hash_expr);
  cur_op = tmp_op;
  SQL_ENG_LOG(INFO, "op info", "op_id", cur_op->get_id(), "op_type", cur_op->get_type());

  local_phy_plan_->set_main_query(cur_op);
  local_phy_plan_->set_execute_type(OB_LOCAL_SINGLE_PARTITION_PLAN);

  return ret;
}

TEST_F(ObTableScanTest, basic_test)
{
  int ret = OB_SUCCESS;
  ObPhyTableLocationSEArray table_locs;
  ObPartitionLocation partition_loc;
  partition_loc.set_table_id(TEST_TABLE_ID);
  partition_loc.set_partition_id(9);
  ObAddr server;
  server.set_ip_addr("127.0.0.1", 8888);
  ObExecuteResult exe_result;
  ObExecContext exec_ctx;
  exec_ctx.init_phy_op(100);
  exec_ctx.create_physical_plan_ctx();
  ObPhysicalPlanCtx *plan_ctx = exec_ctx.get_physical_plan_ctx();
  //plan_ctx->set_executor_rpc(rpc);
  //plan_ctx->set_task_response_handler(resp_handler);
  plan_ctx->set_server(server);
  plan_ctx->set_timeout_timestamp(::oceanbase::common::ObTimeUtility::current_time() + 2000L * 1000L);
  ObTaskExecutorCtx *task_exe_ctx = exec_ctx.get_task_executor_ctx();
  task_exe_ctx->set_partition_service(&partition_service_);
  task_exe_ctx->set_execute_result(&exe_result);

  ASSERT_EQ(OB_SUCCESS, create_local_plan_tree(exec_ctx));

  ObExecutor ob_exe;
  ASSERT_EQ(OB_SUCCESS, ob_exe.execute_plan(exec_ctx, local_phy_plan_));
  ASSERT_EQ(OB_SUCCESS, exe_result.open(exec_ctx));
  const ObNewRow *tmp_row = NULL;
  while(OB_SUCCESS == (ret = exe_result.get_next_row(exec_ctx, tmp_row))) {
    SQL_ENG_LOG(INFO, "get a row", K(*tmp_row));
  }
  ASSERT_EQ(OB_ITER_END, ret);
  ASSERT_EQ(OB_SUCCESS, exe_result.close(exec_ctx));
}

int main(int argc, char **argv)
{
  OB_LOGGER.set_log_level("INFO");
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  init_sql_factories();
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
