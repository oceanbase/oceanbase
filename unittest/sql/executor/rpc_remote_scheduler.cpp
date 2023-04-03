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
#include "lib/utility/ob_tracepoint.h"
#include "sql/executor/ob_remote_scheduler.h"
//#include "ob_mock_utils.h"
#include "sql/engine/table/ob_table_scan.h"
#include "sql/executor/ob_root_transmit.h"
#include "sql/executor/ob_direct_transmit.h"
#include "sql/executor/ob_direct_receive.h"
#include "sql/executor/ob_remote_job_control.h"
#include "sql/executor/ob_job_parser.h"
#include "sql/executor/ob_task_spliter_factory.h"
#include "sql/executor/ob_executor_rpc_impl.h"
#include "sql/engine/ob_physical_plan_ctx.h"
#include "sql/plan_cache/ob_cache_object_factory.h"
#include "sql/engine/ob_physical_plan.h"
#include "sql/engine/ob_physical_plan_ctx.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/ob_sql_init.h"
#include "sql/optimizer/ob_table_location.h"
#include "sql/executor/ob_executor_rpc_proxy.h"
#include "rpc/obrpc/ob_net_client.h"
#include "storage/tx/ob_trans_define.h"

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::sql;
using namespace oceanbase::obrpc;
using namespace oceanbase::share;
using namespace oceanbase::transaction;

class ObRemoteSchedulerTest : public ::testing::Test
{
public:
  const static int64_t TEST_MOCK_COL_NUM = 3;

  const static int64_t TEST_SPLIT_TASK_COUNT = 7;
  const static int64_t TEST_PARA_SERVER_COUNT = 2;
  const static int64_t TEST_SERVER_PARA_THREAD_COUNT = 3;

  const static int64_t TEST_TABLE_ID = 1099511627778;
  const static int64_t TEST_INDEX_ID = 1099511627778;
  //const static int64_t TEST_TABLE_ID = 1099511627777;//1099511627778;
  //const static int64_t TEST_INDEX_ID = 1099511627777;//1099511627778;
  const static uint64_t COLUMN_ID_1 = 16;
  const static uint64_t COLUMN_ID_2 = 17;

  const static int64_t TEST_LIMIT = 10;
  const static int64_t TEST_OFFSET = 0;

  ObRemoteSchedulerTest();
  virtual ~ObRemoteSchedulerTest();
  virtual void SetUp();
  virtual void TearDown();

  ObPhysicalPlan *phy_plan_;

  int create_plan_tree(ObExecContext &ctx);

private:
  // disallow copy
  ObRemoteSchedulerTest(const ObRemoteSchedulerTest &other);
  ObRemoteSchedulerTest& operator=(const ObRemoteSchedulerTest &other);
private:
  // data members
};
ObRemoteSchedulerTest::ObRemoteSchedulerTest()
{
}

ObRemoteSchedulerTest::~ObRemoteSchedulerTest()
{
}

void ObRemoteSchedulerTest::SetUp()
{
}

void ObRemoteSchedulerTest::TearDown()
{
}

int ObRemoteSchedulerTest::create_plan_tree(ObExecContext &ctx)
{
  UNUSED(ctx);
  int ret = OB_SUCCESS;
  ObCacheObjectFactory::alloc(phy_plan_);
  //phy_plan_ = ObPhysicalPlan::alloc();
  ObPhyOperator *cur_op = NULL;
  ObPhyOperator *tmp_op = NULL;

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
  expr_item.set_op(phy_plan_->get_allocator(), "%", 2);
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
  iden_expr_item.set_op(phy_plan_->get_allocator(), "%", 2);
  EXPECT_EQ(OB_SUCCESS, iden_hash_expr->add_expr_item(iden_expr_item));


  ObPhysicalPlanCtx *plan_ctx = ctx.get_physical_plan_ctx();
  common::ObIArray<common::ObObjParam> &param_store = plan_ctx->get_param_store_for_update();
  ObObjParam value1;
  value1.set_int(3);
  param_store.push_back(value1);
  ObObjParam value2;
  value2.set_int(1);
  param_store.push_back(value2);
  ObObjParam value3;
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
  partition_func_item.assign(index_value);
  EXPECT_EQ(OB_SUCCESS, partition_func->add_expr_item(partition_func_item));
  partition_func_item.set_op(phy_plan_->get_allocator(), "+", 2);
  EXPECT_EQ(OB_SUCCESS, partition_func->add_expr_item(partition_func_item));

  ObColumnRefRawExpr ref_col1(TEST_TABLE_ID, COLUMN_ID_1, T_REF_COLUMN);
  //ObArray<ColumnItem> single_range_columns;
  ColumnItem col1;
  ref_col1.set_data_type(ObIntType);
  ref_col1.set_column_attr(ObString::make_string(""), ObString::make_string("a"));
  col1.table_id_ = ref_col1.get_table_id();
  col1.column_id_ = ref_col1.get_column_id();
  col1.column_name_ = ref_col1.get_column_name();
  col1.expr_ = &ref_col1;
  ref_col1.add_flag(IS_COLUMN);
  ObColumnRefRawExpr ref_col2(TEST_TABLE_ID, COLUMN_ID_2, T_REF_COLUMN);
  ColumnItem col2;
  ref_col2.set_data_type(ObIntType);
  ref_col1.set_column_attr(ObString::make_string(""), ObString::make_string("b"));
  col1.table_id_ = ref_col2.get_table_id();
  col1.column_id_ = ref_col2.get_column_id();
  col2.column_name_ = ref_col2.get_column_name();
  col2.expr_ = &ref_col2;
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
  EXPECT_EQ(OB_SUCCESS, query_range->preliminary_extract_query_range(scan_range_columns, &condition3, NULL));
  EXPECT_EQ(OB_SUCCESS, query_range->preliminary_extract_query_range(partition_range_columns, &condition3, NULL));

  EXPECT_EQ(OB_SUCCESS, phy_plan_->alloc_operator_by_type(PHY_TABLE_SCAN, tmp_op));
  tmp_op->set_column_count(TEST_MOCK_COL_NUM);
  static_cast<ObTableScan*>(tmp_op)->set_table_location_key(TEST_TABLE_ID);
  static_cast<ObTableScan*>(tmp_op)->set_ref_table_id(TEST_TABLE_ID);
  static_cast<ObTableScan*>(tmp_op)->set_index_table_id(TEST_INDEX_ID);
  static_cast<ObTableScan*>(tmp_op)->add_output_column(COLUMN_ID_1);
  static_cast<ObTableScan*>(tmp_op)->add_output_column(COLUMN_ID_2);
  //static_cast<ObTableScan*>(tmp_op)->set_limit_offset(*limit_expr, *offset_expr);
  static_cast<ObTableScan*>(tmp_op)->set_query_range(*query_range);
  cur_op = tmp_op;
  SQL_EXE_LOG(INFO, "op info", "op_id", cur_op->get_id(), "op_type", cur_op->get_type());

  EXPECT_EQ(OB_SUCCESS, phy_plan_->alloc_operator_by_type(PHY_DIRECT_TRANSMIT, tmp_op));
  tmp_op->set_column_count(TEST_MOCK_COL_NUM);
  tmp_op->set_child(0, *cur_op);
  static_cast<ObTransmit*>(tmp_op)->get_job_conf().set_task_split_type(ObTaskSpliter::REMOTE_IDENTITY_SPLIT);
  static_cast<ObTransmit*>(tmp_op)->set_split_task_count(TEST_SPLIT_TASK_COUNT);
  static_cast<ObTransmit*>(tmp_op)->set_parallel_server_count(TEST_PARA_SERVER_COUNT);
  static_cast<ObTransmit*>(tmp_op)->set_server_parallel_thread_count(TEST_SERVER_PARA_THREAD_COUNT);
  //static_cast<ObTransmit*>(tmp_op)->set_shuffle_func(hash_expr);
  cur_op = tmp_op;
  SQL_EXE_LOG(INFO, "op info", "op_id", cur_op->get_id(), "op_type", cur_op->get_type());

  EXPECT_EQ(OB_SUCCESS, phy_plan_->alloc_operator_by_type(PHY_DIRECT_RECEIVE, tmp_op));
  tmp_op->set_column_count(TEST_MOCK_COL_NUM);
  tmp_op->set_child(0, *cur_op);
  cur_op = tmp_op;
  SQL_EXE_LOG(INFO, "op info", "op_id", cur_op->get_id(), "op_type", cur_op->get_type());

  EXPECT_EQ(OB_SUCCESS, phy_plan_->alloc_operator_by_type(PHY_ROOT_TRANSMIT, tmp_op));
  tmp_op->set_column_count(TEST_MOCK_COL_NUM);
  tmp_op->set_child(0, *cur_op);
  static_cast<ObTransmit*>(tmp_op)->get_job_conf().set_task_split_type(ObTaskSpliter::LOCAL_IDENTITY_SPLIT);
  static_cast<ObTransmit*>(tmp_op)->set_split_task_count(TEST_SPLIT_TASK_COUNT);
  static_cast<ObTransmit*>(tmp_op)->set_parallel_server_count(TEST_PARA_SERVER_COUNT);
  static_cast<ObTransmit*>(tmp_op)->set_server_parallel_thread_count(TEST_SERVER_PARA_THREAD_COUNT);
  //static_cast<ObTransmit*>(tmp_op)->set_shuffle_func(hash_expr);
  cur_op = tmp_op;
  SQL_EXE_LOG(INFO, "op info", "op_id", cur_op->get_id(), "op_type", cur_op->get_type());

  phy_plan_->set_main_query(cur_op);

  //EXPECT_EQ(OB_SUCCESS, phy_plan_->add_table_id(TEST_TABLE_ID));
  phy_plan_->set_location_type(OB_PHY_PLAN_REMOTE);

  SQL_EXE_LOG(INFO, "physical plan", K(*phy_plan_));

  return ret;
}

TEST_F(ObRemoteSchedulerTest, basic_test)
{
  int ret = OB_SUCCESS;

  //启动模拟收包队列
  //ObMockPacketQueueThread::get_instance()->start();

  ObRemoteScheduler remote_scheduler;

  ObArenaAllocator allocator(ObModIds::TEST);
  ObAddr dst_server;
  dst_server.set_ip_addr("10.125.224.8", 38455);
  //dst_server.set_ip_addr("10.125.224.9", 60593);
  ObNetClient client;
  ObExecutorRpcProxy proxy;
  ASSERT_EQ(OB_SUCCESS, client.init());
  ASSERT_EQ(OB_SUCCESS, client.get_proxy(proxy));
  const int64_t timeout = 180 * 1000 * 1000; //180s
  proxy.set_timeout(timeout);
  proxy.set_server(dst_server);
  ObExecutorRpcImpl rpc;
  rpc.set_rpc_proxy(&proxy);

  ObPhyTableLocationFixedArray table_locs;
  ObPhyTableLocation table_loc;
  ObPartitionReplicaLocation partition_loc;
  ObReplicaLocation replica_loc;
  replica_loc.server_ = dst_server;
  replica_loc.role_ = LEADER;
  partition_loc.set_table_id(TEST_TABLE_ID);
  partition_loc.set_partition_id(0);
  partition_loc.set_replica_location(replica_loc);
  table_loc.set_table_location_key(TEST_TABLE_ID, TEST_TABLE_ID);
  ASSERT_EQ(OB_SUCCESS, table_loc.add_partition_location(partition_loc));
  ASSERT_EQ(OB_SUCCESS, table_locs.push_back(table_loc));

  ObAddr server;
  server.set_ip_addr("10.125.224.8", 38455);
  ObTransID ob_trans_id;
  ObTransDesc trans_desc;
  trans_desc.set_trans_id(ob_trans_id);
  trans_desc.set_snapshot_version(0);
  //RemoteExecuteStreamHandle resp_handler(allocator);
  ObExecuteResult exe_result;
  ObExecContext exec_ctx;
  exec_ctx.init_phy_op(100);
  exec_ctx.create_physical_plan_ctx();
  ObPhysicalPlanCtx *plan_ctx = exec_ctx.get_physical_plan_ctx();
  ObSQLSessionInfo *my_session = exec_ctx.get_my_session();
  //plan_ctx->set_server(server);
  plan_ctx->set_timeout_timestamp(::oceanbase::common::ObTimeUtility::current_time() + 2000L * 1000L);
  ASSERT_EQ(OB_SUCCESS, my_session->init_tenant(ObString::make_string("t1"), 2));
  ObObj autocommit_obj, min_val, max_val;
  ObObj autocommit_type;
  autocommit_obj.set_varchar("1");
  min_val.set_varchar("");
  max_val.set_varchar("");
  autocommit_type.set_type(ObIntType);
  ObArenaAllocator calc_buf(ObModIds::OB_SQL_SESSION);
  ASSERT_EQ(OB_SUCCESS, my_session->load_sys_variable(calc_buf, ObString::make_string(OB_SV_AUTOCOMMIT), autocommit_type, autocommit_obj, min_val, max_val,
                                                      ObSysVarFlag::GLOBAL_SCOPE | ObSysVarFlag::SESSION_SCOPE | ObSysVarFlag::NEED_SERIALIZE));
  //my_session->set_autocommit(true);
  //plan_ctx->get_trans_desc() = trans_desc;
  ObTaskExecutorCtx *executor_ctx = exec_ctx.get_task_executor_ctx();
  executor_ctx->set_task_executor_rpc(rpc);
  //executor_ctx->set_task_response_handler(resp_handler);
  //executor_ctx->set_partition_location_cache(&rpc.partition_loc_cache_);
  //executor_ctx->set_partition_service(&rpc.partition_service_);
  executor_ctx->set_execute_result(&exe_result);
  executor_ctx->set_table_locations(table_locs);
  executor_ctx->set_self_addr(server);

  create_plan_tree(exec_ctx);

  int64_t sent_task_count = 0;
  ASSERT_EQ(OB_SUCCESS, remote_scheduler.schedule(exec_ctx, phy_plan_, sent_task_count));
  //ObExecutor ob_exe;
  //ASSERT_EQ(OB_SUCCESS, ob_exe.execute_plan(exec_ctx, phy_plan_));
  ASSERT_EQ(OB_SUCCESS, exe_result.open(exec_ctx));
  const ObNewRow *tmp_row = NULL;
  while(OB_SUCCESS == (ret = exe_result.get_next_row(exec_ctx, tmp_row))) {
    SQL_EXE_LOG(INFO, "get a row", K(*tmp_row));
  }
  ASSERT_EQ(OB_ITER_END, ret);
  ASSERT_EQ(OB_SUCCESS, exe_result.close(exec_ctx));


#if 0
  ObArenaAllocator allocator(ObModIds::TEST);
  ObAddr dst_server;
  dst_server.set_ip_addr("10.125.224.8", 60593);
  ObNetClient client;
  ObExecutorRpcProxy proxy;
  ASSERT_EQ(OB_SUCCESS, client.init());
  ASSERT_EQ(OB_SUCCESS, client.get_proxy(proxy));
  const int64_t timeout = 180 * 1000 * 1000; //180s
  proxy.set_timeout(timeout);
  proxy.set_server(dst_server);
  ObTask task;
  RemoteExecuteStreamHandle handler(allocator);
  RemoteExecuteStreamHandle::MyHandle &h = handler.get_handle();
  ASSERT_EQ(OB_SUCCESS, proxy.task_execute(task,  *handler.get_result(), h));
#endif
}

int main(int argc, char **argv)
{
  init_sql_factories();
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
