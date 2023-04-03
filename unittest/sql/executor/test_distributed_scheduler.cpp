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
#include "sql/executor/ob_distributed_scheduler.h"
#include "ob_mock_utils.h"
#include "sql/engine/table/ob_table_scan.h"
#include "sql/engine/sort/ob_sort.h"
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

#define TEST_FAIL(statement) \
    do { \
      if (OB_SUCCESS == ret && OB_SUCCESS != (ret = (statement))) { \
        return ret; \
      } \
    } while(0)

class ObDistributedSchedulerTest : public ::testing::Test
{
public:

  const static int64_t TEST_SPLIT_TASK_COUNT = 7;
  const static int64_t TEST_PARA_SERVER_COUNT = 2;
  const static int64_t TEST_SERVER_PARA_THREAD_COUNT = 3;

  const static int64_t TEST_TABLE_ID = 3003;
  const static int64_t TEST_INDEX_ID = 3004;
  const static uint64_t COLUMN_ID_1 = 16;
  const static uint64_t COLUMN_ID_2 = 17;

  const static int64_t TEST_LIMIT = 10;
  const static int64_t TEST_OFFSET = 0;

  ObDistributedSchedulerTest();
  virtual ~ObDistributedSchedulerTest();
  virtual void SetUp();
  virtual void TearDown();

  ObMockSqlExecutorRpc rpc_;
  ObPhysicalPlan *phy_plan_;

  int create_plan_tree(ObExecContext &ctx);
  int exception_test();
private:
  // disallow copy
  ObDistributedSchedulerTest(const ObDistributedSchedulerTest &other);
  ObDistributedSchedulerTest& operator=(const ObDistributedSchedulerTest &other);
};
ObDistributedSchedulerTest::ObDistributedSchedulerTest()
{
}

ObDistributedSchedulerTest::~ObDistributedSchedulerTest()
{
}

void ObDistributedSchedulerTest::SetUp()
{
}

int ObDistributedSchedulerTest::create_plan_tree(ObExecContext &ctx)
{
  UNUSED(ctx);
  int ret = OB_SUCCESS;

  phy_plan_ = ObPhysicalPlan::alloc();
  ObPhyOperator *cur_op = NULL;
  ObPhyOperator *tmp_op = NULL;
  int err_code = OB_SUCCESS;

  ObSqlExpression *limit_expr = NULL;
  EXPECT_EQ(OB_SUCCESS, ObSqlExpressionUtil::make_sql_expr(phy_plan_, limit_expr));
  EXPECT_FALSE(NULL == limit_expr);
  ObPostExprItem limit_expr_item;
  limit_expr_item.set_int(TEST_LIMIT);
  limit_expr_item.set_item_type(T_INT);
  EXPECT_EQ(OB_SUCCESS, limit_expr->add_expr_item(limit_expr_item));

  ObSqlExpression *offset_expr = NULL;
  EXPECT_EQ(OB_SUCCESS, ObSqlExpressionUtil::make_sql_expr(phy_plan_, offset_expr));
  EXPECT_FALSE(NULL == offset_expr);
  ObPostExprItem offset_expr_item;
  offset_expr_item.set_int(TEST_OFFSET);
  offset_expr_item.set_item_type(T_INT);
  EXPECT_EQ(OB_SUCCESS, offset_expr->add_expr_item(offset_expr_item));

  /*
   * calculate c0 % TEST_SPLIT_TASK_COUNT
   * */
  ObSqlExpression *hash_expr = NULL;
  EXPECT_EQ(OB_SUCCESS, ObSqlExpressionUtil::make_sql_expr(phy_plan_, hash_expr));
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
  EXPECT_EQ(OB_SUCCESS, ObSqlExpressionUtil::make_sql_expr(phy_plan_, iden_hash_expr));
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
  EXPECT_EQ(OB_SUCCESS, ObSqlExpressionUtil::make_sql_expr(phy_plan_, partition_func));
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

  ObColumnRefRawExpr ref_col1(TEST_TABLE_ID, COLUMN_ID_1, T_REF_COLUMN);
  //ObArray<ColumnItem> single_range_columns;
  ColumnItem col1;
  col1.column_id_ = COLUMN_ID_1;
  col1.table_id_ = TEST_TABLE_ID;
  col1.data_type_ = ObIntType;
  col1.column_name_ = ObString::make_string("a");
  //EXPECT_EQ(OB_SUCCESS, single_range_columns.push_back(col1));
  ref_col1.add_flag(IS_COLUMN);
  ObColumnRefRawExpr ref_col2(TEST_TABLE_ID, COLUMN_ID_2, T_REF_COLUMN);
  ColumnItem col2;
  col2.column_id_ = COLUMN_ID_2;
  col2.table_id_ = TEST_TABLE_ID;
  col2.data_type_ = ObIntType;
  col2.column_name_ = ObString::make_string("b");
  //EXPECT_EQ(OB_SUCCESS, single_range_columns.push_back(col2));
  ref_col2.add_flag(IS_COLUMN);

  // 构造 (a = ?)
  ObObj index1;
  index1.set_unknown(0);
  ObConstRawExpr const_col1(index1, T_QUESTIONMARK);
  const_col1.add_flag(IS_STATIC_PARAM);
  ObOpRawExpr condition1(&ref_col1, &const_col1, T_OP_EQ); // a = ?构造完毕
  // 构造 (b > ?)
  ObObj index2;
  index2.set_unknown(1);
  ObConstRawExpr const_col2(index2, T_QUESTIONMARK);
  const_col2.add_flag(IS_STATIC_PARAM);
  ObOpRawExpr condition2(&ref_col2, &const_col2, T_OP_GT); // b > ?构造完毕
  ObOpRawExpr condition3(&condition1, &condition2, T_OP_AND); // a = ? and b > ?构造完毕

  ObArray<ColumnItem> scan_range_columns;
  EXPECT_EQ(OB_SUCCESS, scan_range_columns.push_back(col2));
  ObArray<ColumnItem> partition_range_columns;
  EXPECT_EQ(OB_SUCCESS, partition_range_columns.push_back(col1));
  ObQueryRange *query_range = OB_NEW(ObQueryRange, ObModIds::TEST);
  EXPECT_EQ(OB_SUCCESS, query_range->preliminary_extract_query_range(scan_range_columns, &condition3));
  EXPECT_EQ(OB_SUCCESS, query_range->preliminary_extract_query_range(partition_range_columns, &condition3));

  ASSERT_EQ(OB_SUCCESS, phy_plan_->alloc_operator_by_type(PHY_TABLE_SCAN, tmp_op));
  tmp_op->set_column_count(TEST_MOCK_COL_NUM);
  static_cast<ObTableScan*>(tmp_op)->set_table_id(TEST_TABLE_ID);
  static_cast<ObTableScan*>(tmp_op)->set_ref_table_id(TEST_TABLE_ID);
  static_cast<ObTableScan*>(tmp_op)->set_index_table_id(TEST_INDEX_ID);
  static_cast<ObTableScan*>(tmp_op)->add_range_column(COLUMN_ID_2);
  static_cast<ObTableScan*>(tmp_op)->add_output_column(COLUMN_ID_1);
  static_cast<ObTableScan*>(tmp_op)->add_output_column(COLUMN_ID_2);
  static_cast<ObTableScan*>(tmp_op)->set_limit_offset(*limit_expr, *offset_expr);
  static_cast<ObTableScan*>(tmp_op)->set_query_range(*query_range);
  cur_op = tmp_op;
  SQL_EXE_LOG(INFO, "op info", "op_id", cur_op->get_id(), "op_type", cur_op->get_type());

  ASSERT_EQ(OB_SUCCESS, phy_plan_->alloc_operator_by_type(PHY_DISTRIBUTED_TRANSMIT, tmp_op));
  tmp_op->set_column_count(TEST_MOCK_COL_NUM);
  tmp_op->set_child(0, *cur_op);
  static_cast<ObTransmit*>(tmp_op)->get_job_conf().set_task_split_type(ObTaskSpliter::IDENTITY_SPLIT);
  static_cast<ObTransmit*>(tmp_op)->set_split_task_count(TEST_SPLIT_TASK_COUNT);
  static_cast<ObTransmit*>(tmp_op)->set_parallel_server_count(TEST_PARA_SERVER_COUNT);
  static_cast<ObTransmit*>(tmp_op)->set_server_parallel_thread_count(TEST_SERVER_PARA_THREAD_COUNT);
  static_cast<ObTransmit*>(tmp_op)->set_shuffle_func(hash_expr);
  cur_op = tmp_op;
  SQL_EXE_LOG(INFO, "op info", "op_id", cur_op->get_id(), "op_type", cur_op->get_type());

  ASSERT_EQ(OB_SUCCESS, phy_plan_->alloc_operator_by_type(PHY_FIFO_RECEIVE, tmp_op));
  tmp_op->set_column_count(TEST_MOCK_COL_NUM);
  tmp_op->set_child(0, *cur_op);
  cur_op = tmp_op;
  SQL_EXE_LOG(INFO, "op info", "op_id", cur_op->get_id(), "op_type", cur_op->get_type());

  ASSERT_EQ(OB_SUCCESS, phy_plan_->alloc_operator_by_type(PHY_DISTRIBUTED_TRANSMIT, tmp_op));
  tmp_op->set_column_count(TEST_MOCK_COL_NUM);
  tmp_op->set_child(0, *cur_op);
  static_cast<ObTransmit*>(tmp_op)->get_job_conf().set_task_split_type(ObTaskSpliter::INTERM_SPLIT);
  static_cast<ObTransmit*>(tmp_op)->set_split_task_count(TEST_SPLIT_TASK_COUNT);
  static_cast<ObTransmit*>(tmp_op)->set_parallel_server_count(TEST_PARA_SERVER_COUNT);
  static_cast<ObTransmit*>(tmp_op)->set_server_parallel_thread_count(TEST_SERVER_PARA_THREAD_COUNT);
  static_cast<ObTransmit*>(tmp_op)->set_shuffle_func(iden_hash_expr);
  cur_op = tmp_op;
  SQL_EXE_LOG(INFO, "op info", "op_id", cur_op->get_id(), "op_type", cur_op->get_type());

  ASSERT_EQ(OB_SUCCESS, phy_plan_->alloc_operator_by_type(PHY_FIFO_RECEIVE, tmp_op));
  tmp_op->set_column_count(TEST_MOCK_COL_NUM);
  tmp_op->set_child(0, *cur_op);
  cur_op = tmp_op;
  SQL_EXE_LOG(INFO, "op info", "op_id", cur_op->get_id(), "op_type", cur_op->get_type());

  ASSERT_EQ(OB_SUCCESS, phy_plan_->alloc_operator_by_type(PHY_ROOT_TRANSMIT, tmp_op));
  tmp_op->set_column_count(TEST_MOCK_COL_NUM);
  tmp_op->set_child(0, *cur_op);
  static_cast<ObTransmit*>(tmp_op)->get_job_conf().set_task_split_type(ObTaskSpliter::IDENTITY_SPLIT);
  static_cast<ObTransmit*>(tmp_op)->set_split_task_count(TEST_SPLIT_TASK_COUNT);
  static_cast<ObTransmit*>(tmp_op)->set_parallel_server_count(TEST_PARA_SERVER_COUNT);
  static_cast<ObTransmit*>(tmp_op)->set_server_parallel_thread_count(TEST_SERVER_PARA_THREAD_COUNT);
  static_cast<ObTransmit*>(tmp_op)->set_shuffle_func(hash_expr);
  cur_op = tmp_op;
  SQL_EXE_LOG(INFO, "op info", "op_id", cur_op->get_id(), "op_type", cur_op->get_type());

  phy_plan_->set_main_query(cur_op);

  return ret;
}

void ObDistributedSchedulerTest::TearDown()
{
}

int ObDistributedSchedulerTest::exception_test()
{
  int ret = OB_SUCCESS;
  int64_t remain_time_us = 1000 * 1000;
  uint64_t query_id = OB_INVALID_ID;
  ObDistributedSchedulerManager *sc_manager = ObDistributedSchedulerManager::get_instance();

  ObAddr server;
  server.set_ip_addr("127.0.0.1", 8888);
  ObArenaAllocator allocator(ObModIds::OB_SQL_EXEC_CONTEXT);
  ObPhyTableLocationArray table_locs(allocator);
  ObPhyTableLocation table_loc;
  ObPartitionLocation partition_loc;
  partition_loc.set_table_id(TEST_TABLE_ID);
  partition_loc.set_partition_id(9);
  table_loc.set_table_id(TEST_TABLE_ID);
  EXPECT_EQ(OB_SUCCESS, table_loc.add_partition_location(partition_loc));
  ObMockFetchResultStreamHandle resp_handler(allocator);
  ObExecuteResult exe_result;
  ObExecContext exec_ctx;
  TEST_FAIL(exec_ctx.init_phy_op(100));
  TEST_FAIL(exec_ctx.create_physical_plan_ctx());
  ObTaskExecutorCtx *executor_ctx = exec_ctx.get_task_executor_ctx();
  executor_ctx->set_task_executor_rpc(&rpc_);
  executor_ctx->set_server(server);
  executor_ctx->set_partition_location_cache(&rpc_.partition_loc_cache_);
  executor_ctx->set_partition_service(&rpc_.partition_service_);
  executor_ctx->set_execute_result(&exe_result);
  executor_ctx->init_table_location(1);
  EXPECT_EQ(OB_SUCCESS, table_locs.push_back(table_loc));
  executor_ctx->set_table_locations(table_locs);

  TEST_FAIL(create_plan_tree(exec_ctx));

  TEST_FAIL(sc_manager->alloc_scheduler(query_id, remain_time_us));
  TEST_FAIL(sc_manager->schedule(query_id, exec_ctx, phy_plan_));
  TEST_FAIL(sc_manager->free_scheduler(query_id));
  return ret;
}

TEST_F(ObDistributedSchedulerTest, basic_test)
{
  int ret = OB_SUCCESS;

  //启动模拟收包队列
  ObMockPacketQueueThread::get_instance()->start();

  int64_t remain_time_us = 1000 * 1000;
  uint64_t query_id = OB_INVALID_ID;
  ObDistributedSchedulerManager *sc_manager = ObDistributedSchedulerManager::get_instance();

  ObPhyTableLocationSEArray table_locs;
  ObPhyTableLocation table_loc;
  ObPartitionLocation partition_loc;
  partition_loc.set_table_id(TEST_TABLE_ID);
  partition_loc.set_partition_id(9);
  table_loc.set_table_id(TEST_TABLE_ID);
  ASSERT_EQ(OB_SUCCESS, table_loc.add_partition_location(partition_loc));
  ASSERT_EQ(OB_SUCCESS, table_locs.push_back(table_loc));

  ObAddr server;
  server.set_ip_addr("127.0.0.1", 8888);
  ObArenaAllocator allocator(ObModIds::OB_SQL_EXEC_CONTEXT);
  ObMockFetchResultStreamHandle resp_handler(allocator);
  ObExecuteResult exe_result;
  ObExecContext exec_ctx;
  exec_ctx.init_phy_op(100);
  exec_ctx.create_physical_plan_ctx();
  ObPhysicalPlanCtx *plan_ctx = exec_ctx.get_physical_plan_ctx();
  //plan_ctx->set_task_response_handler(resp_handler);
  plan_ctx->set_server(server);
  plan_ctx->set_timeout_timestamp(::oceanbase::common::ObTimeUtility::current_time() + 2000L * 1000L);
  ObTaskExecutorCtx *executor_ctx = exec_ctx.get_task_executor_ctx();
  executor_ctx->set_task_executor_rpc(&rpc_);
  executor_ctx->set_server(server);
  executor_ctx->set_partition_location_cache(&rpc_.partition_loc_cache_);
  executor_ctx->set_partition_service(&rpc_.partition_service_);
  executor_ctx->set_execute_result(&exe_result);
  executor_ctx->set_table_locations(table_locs);

  create_plan_tree(exec_ctx);

  ASSERT_EQ(OB_SUCCESS, sc_manager->alloc_scheduler(query_id, remain_time_us));
  ASSERT_EQ(OB_SUCCESS, sc_manager->schedule(query_id, exec_ctx, phy_plan_));
  ASSERT_EQ(OB_SUCCESS, sc_manager->free_scheduler(query_id));
  //ObExecutor ob_exe;
  //ASSERT_EQ(OB_SUCCESS, ob_exe.execute_plan(exec_ctx, phy_plan_));
  ASSERT_EQ(OB_SUCCESS, exe_result.open(exec_ctx));
  const ObNewRow *tmp_row = NULL;
  while(OB_SUCCESS == (ret = exe_result.get_next_row(exec_ctx, tmp_row))) {
    SQL_EXE_LOG(INFO, "get a row", K(*tmp_row));
  }
  ASSERT_EQ(OB_ITER_END, ret);
  ASSERT_EQ(OB_SUCCESS, exe_result.close(exec_ctx));
}

#define EXCEPTION_TEST_1(test_name, expected_ret, file, func, key, err) \
          TEST_F(ObDistributedSchedulerTest, test_name) \
          {\
            TP_SET_ERROR(file, func, key, err); \
            int ret = exception_test(); \
            TP_SET_ERROR(file, func, key, NULL); \
            ASSERT_EQ(ret, expected_ret); \
          }

#define EXCEPTION_TEST_2(test_name, expected_ret, file1, func1, key1, err1, file2, func2, key2, err2) \
          TEST_F(ObDistributedSchedulerTest, test_name) \
          {\
            TP_SET_ERROR(file1, func1, key1, err1); \
            TP_SET_ERROR(file2, func2, key2, err2); \
            int ret = exception_test(); \
            TP_SET_ERROR(file1, func1, key1, NULL); \
            TP_SET_ERROR(file2, func2, key2, NULL); \
            ASSERT_EQ(ret, expected_ret); \
          }

#if 0
EXCEPTION_TEST_1(et1_1, OB_NOT_INIT, "executor/ob_task.cpp", "serialize_", "t1", 1)
EXCEPTION_TEST_1(et1_2, OB_NOT_INIT, "executor/ob_task.cpp", "deserialize_", "t1", 1)
EXCEPTION_TEST_1(et1_3, OB_ERR_UNEXPECTED, "executor/ob_task.cpp", "deserialize_", "t2", 1)
EXCEPTION_TEST_1(et1_4, OB_ERROR, "executor/ob_task.cpp", "serialize_tree", "t1", OB_ERROR)
EXCEPTION_TEST_1(et1_5, OB_ERROR, "executor/ob_task.cpp", "serialize_tree", "t2", OB_ERROR)
EXCEPTION_TEST_1(et1_6, OB_ERR_UNEXPECTED, "executor/ob_task.cpp", "serialize_tree", "t3", 1)
EXCEPTION_TEST_1(et1_7, OB_ERROR, "executor/ob_task.cpp", "serialize_tree", "t4", OB_ERROR)
EXCEPTION_TEST_1(et1_8, OB_ERROR, "executor/ob_task.cpp", "deserialize_tree", "t1", OB_ERROR)
EXCEPTION_TEST_1(et1_9, OB_ALLOCATE_MEMORY_FAILED, "executor/ob_task.cpp", "deserialize_tree", "t2", 1)
EXCEPTION_TEST_1(et1_10, OB_ERROR, "executor/ob_task.cpp", "deserialize_tree", "t3", OB_ERROR)
EXCEPTION_TEST_1(et1_11, OB_ERROR, "executor/ob_task.cpp", "deserialize_tree", "t4", OB_ERROR)
EXCEPTION_TEST_1(et1_12, OB_ERR_UNEXPECTED, "executor/ob_task.cpp", "deserialize_tree", "t5", 1)
EXCEPTION_TEST_1(et1_13, OB_ERROR, "executor/ob_task.cpp", "deserialize_tree", "t6", OB_ERROR)
EXCEPTION_TEST_1(et1_14, OB_ERROR, "executor/ob_task.cpp", "deserialize_tree", "t7", OB_ERROR)
EXCEPTION_TEST_1(et1_15, OB_ERROR, "executor/ob_distributed_job_control.cpp", "get_ready_jobs", "t1", OB_ERROR)
EXCEPTION_TEST_1(et1_16, OB_ERROR, "executor/ob_distributed_job_control.cpp", "get_ready_jobs", "t2", OB_ERROR)
EXCEPTION_TEST_1(et1_17, OB_ERR_UNEXPECTED, "executor/ob_distributed_job_executor.cpp", "execute_step", "t1", 1)
EXCEPTION_TEST_1(et1_18, OB_NOT_INIT, "executor/ob_distributed_job_executor.cpp", "get_executable_tasks", "t1", 1)
EXCEPTION_TEST_1(et1_19, OB_ERROR, "executor/ob_distributed_job_executor.cpp", "get_executable_tasks", "t2", OB_ERROR)
EXCEPTION_TEST_1(et1_20, OB_ERROR, "executor/ob_distributed_job_executor.cpp", "get_executable_tasks", "t3", OB_ERROR)
EXCEPTION_TEST_1(et1_21, OB_NOT_INIT, "executor/ob_distributed_task_executor.cpp", "execute", "t1", 1)
EXCEPTION_TEST_1(et1_22, OB_NOT_INIT, "executor/ob_distributed_task_executor.cpp", "execute", "t2", 1)
EXCEPTION_TEST_1(et1_23, OB_ERR_UNEXPECTED, "executor/ob_distributed_task_executor.cpp", "execute", "t3", 1)
EXCEPTION_TEST_1(et1_24, OB_ERR_UNEXPECTED, "executor/ob_distributed_task_executor.cpp", "execute", "t4", 1)
EXCEPTION_TEST_1(et1_25, OB_ERROR, "executor/ob_distributed_task_executor.cpp", "execute", "t5", OB_ERROR)
EXCEPTION_TEST_1(et1_26, OB_ERROR, "executor/ob_distributed_task_executor.cpp", "execute", "t6", OB_ERROR)
EXCEPTION_TEST_1(et1_27, OB_ERROR, "executor/ob_distributed_task_executor.cpp", "build_task", "t1", OB_ERROR)
EXCEPTION_TEST_1(et1_28, OB_NOT_INIT, "executor/ob_distributed_task_runner.cpp", "execute", "t1", 1)
EXCEPTION_TEST_1(et1_29, OB_ERR_UNEXPECTED, "executor/ob_distributed_task_runner.cpp", "execute", "t2", 1)
EXCEPTION_TEST_1(et1_30, OB_ERROR, "executor/ob_distributed_task_runner.cpp", "execute", "t3", OB_ERROR)
EXCEPTION_TEST_1(et1_31, OB_ERROR, "executor/ob_distributed_task_runner.cpp", "execute", "t4", OB_ERROR)
EXCEPTION_TEST_1(et1_32, OB_INVALID_ARGUMENT, "executor/ob_task_spliter.cpp", "init", "t1", 1)
EXCEPTION_TEST_1(et1_33, OB_NOT_INIT, "executor/ob_identity_task_spliter.cpp", "get_next_task", "t1", 1)
EXCEPTION_TEST_1(et1_34, OB_ALLOCATE_MEMORY_FAILED, "executor/ob_identity_task_spliter.cpp", "get_next_task", "t2", 1)
EXCEPTION_TEST_1(et1_35, OB_NOT_INIT, "executor/ob_interm_task_spliter.cpp", "prepare", "t1", 1)
EXCEPTION_TEST_1(et1_36, OB_ERR_UNEXPECTED, "executor/ob_interm_task_spliter.cpp", "prepare", "t2", 1)
EXCEPTION_TEST_1(et1_37, OB_ALLOCATE_MEMORY_FAILED, "executor/ob_interm_task_spliter.cpp", "get_next_task", "t1", 1)
EXCEPTION_TEST_1(et1_38, OB_NOT_INIT, "executor/ob_local_job_executor.cpp", "execute", "t1", 1)
EXCEPTION_TEST_1(et1_39, OB_ERROR, "executor/ob_local_job_executor.cpp", "execute", "t2", OB_ERROR)
EXCEPTION_TEST_1(et1_40, OB_ERROR, "executor/ob_local_job_executor.cpp", "execute", "t3", OB_ERROR)
EXCEPTION_TEST_1(et1_41, OB_ERROR, "executor/ob_local_job_executor.cpp", "get_executable_task", "t1", OB_ERROR)
EXCEPTION_TEST_1(et1_42, OB_ERROR, "executor/ob_local_job_executor.cpp", "get_executable_task", "t2", OB_ERROR)
EXCEPTION_TEST_1(et1_43, OB_ERR_UNEXPECTED, "executor/ob_local_job_executor.cpp", "get_executable_task", "t3", 1)
EXCEPTION_TEST_1(et1_44, OB_ERROR, "executor/ob_local_job_executor.cpp", "get_executable_task", "t4", OB_ERROR)
EXCEPTION_TEST_1(et1_45, OB_NOT_INIT, "executor/ob_job.cpp", "get_task_control", "t1", 1)
EXCEPTION_TEST_1(et1_46, OB_ERROR, "executor/ob_job.cpp", "get_task_control", "t2", OB_ERROR)
EXCEPTION_TEST_1(et1_47, OB_ERROR, "executor/ob_job.cpp", "get_finished_task_locations", "t1", OB_ERROR)
EXCEPTION_TEST_1(et1_48, OB_ERR_UNEXPECTED, "executor/ob_job.cpp", "get_finished_task_locations", "t2", 1)
//EXCEPTION_TEST_1(et1_49, OB_ERROR, "executor/ob_job.cpp", "get_finished_task_locations", "t3", OB_ERROR)
EXCEPTION_TEST_1(et1_50, OB_ERROR, "executor/ob_job.cpp", "get_finished_task_locations", "t4", OB_ERROR)
//EXCEPTION_TEST_1(et1_51, OB_INVALID_ARGUMENT, "executor/ob_job.cpp", "update_job_state", "t1", 1)
//EXCEPTION_TEST_1(et1_52, OB_INVALID_ARGUMENT, "executor/ob_job.cpp", "update_job_state", "t2", 1)
//EXCEPTION_TEST_1(et1_53, OB_ERROR, "executor/ob_job.cpp", "update_job_state", "t3", OB_ERROR)
//EXCEPTION_TEST_1(et1_54, OB_ERROR, "executor/ob_job.cpp", "update_job_state", "t4", OB_ERROR)
EXCEPTION_TEST_1(et1_55, OB_INVALID_ARGUMENT, "executor/ob_job.cpp", "add_child", "t1", 1)
EXCEPTION_TEST_1(et1_56, OB_SIZE_OVERFLOW, "executor/ob_job.cpp", "add_child", "t2", 1)
EXCEPTION_TEST_1(et1_57, OB_ERROR, "executor/ob_task_executor.cpp", "build_op_input", "t1", OB_ERROR)
EXCEPTION_TEST_1(et1_58, OB_ERROR, "executor/ob_task_executor.cpp", "build_op_input", "t2", OB_ERROR)

EXCEPTION_TEST_2(et2_1, OB_ERROR,
                 "executor/ob_distributed_task_runner.cpp", "execute", "t4", OB_ERROR,
                 "executor/ob_distributed_job_executor.cpp", "kill_job", "t1", OB_ERROR)
EXCEPTION_TEST_2(et2_2, OB_ERROR,
                 "executor/ob_distributed_task_runner.cpp", "execute", "t4", OB_ERROR,
                 "executor/ob_distributed_task_executor.cpp", "kill", "t1", 1)
EXCEPTION_TEST_2(et2_3, OB_ERROR,
                 "executor/ob_distributed_task_runner.cpp", "execute", "t4", OB_ERROR,
                 "executor/ob_distributed_task_executor.cpp", "kill", "t2", 1)
EXCEPTION_TEST_2(et2_4, OB_ERROR,
                 "executor/ob_distributed_task_runner.cpp", "execute", "t4", OB_ERROR,
                 "executor/ob_distributed_task_executor.cpp", "kill", "t3", 1)
EXCEPTION_TEST_2(et2_5, OB_ERROR,
                 "executor/ob_distributed_task_runner.cpp", "execute", "t4", OB_ERROR,
                 "executor/ob_distributed_task_executor.cpp", "kill", "t4", OB_ERROR)
EXCEPTION_TEST_2(et2_6, OB_ERROR,
                 "executor/ob_distributed_task_runner.cpp", "execute", "t4", OB_ERROR,
                 "executor/ob_distributed_task_executor.cpp", "kill", "t5", OB_ERROR)
#endif

int main(int argc, char **argv)
{
  OB_LOGGER.set_log_level("INFO");
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  init_sql_factories();
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
