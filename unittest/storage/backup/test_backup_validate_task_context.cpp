/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE
#include <gtest/gtest.h>

#define private public
#define protected public
#include "storage/backup/ob_backup_fuse_tablet_dag.h"
#include "storage/backup/ob_backup_task.h"
#include "storage/backup/ob_backup_validate_dag_scheduler.h"
#undef protected
#undef private

#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "share/location_cache/ob_location_service.h"

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::share;

namespace oceanbase
{
namespace storage
{

class TestBackupValidateTaskContext : public testing::Test
{
public:
  void SetUp() override
  {
    ASSERT_TRUE(self_.set_ip_addr("127.0.0.1", 8086));
    success_dag_id_.init(self_);
    cancel_dag_id_.init(self_);
    ASSERT_TRUE(success_dag_id_.is_valid());
    ASSERT_TRUE(cancel_dag_id_.is_valid());
  }

protected:
  void assert_selected_result(
      const ObBackupValidateTaskContext &ctx,
      const int result,
      const ObTaskId &dag_id,
      const char *error_msg)
  {
    EXPECT_EQ(ObBackupValidateTaskContext::REPORT_SELECTED, ctx.report_state_);
    EXPECT_EQ(result, ctx.result_);
    EXPECT_TRUE(ctx.result_dag_id_.equals(dag_id));
    EXPECT_STREQ(error_msg, ctx.error_msg_);
  }

  void set_valid_param(ObBackupValidateDagNetInitParam &param)
  {
    param.trace_id_ = success_dag_id_;
    param.job_id_ = 1;
    param.tenant_id_ = 1001;
    param.incarnation_ = 1;
    param.task_id_ = 1;
    param.ls_id_ = ObLSID(1);
    param.task_type_ = ObBackupValidateType::DATABASE;
    param.validate_id_ = 1;
    param.dest_id_ = share::OB_START_DEST_ID;
    param.round_id_ = 1;
    param.validate_level_ = ObBackupValidateLevel::BASIC;
    ASSERT_EQ(OB_SUCCESS, param.validate_path_.assign("file:///tmp"));
    ASSERT_TRUE(param.is_valid());
  }

protected:
  ObAddr self_;
  ObTaskId success_dag_id_;
  ObTaskId cancel_dag_id_;
};

TEST_F(TestBackupValidateTaskContext, prepare_success_wins_cancel)
{
  ObBackupValidateTaskContext ctx;
  ObBackupValidatePrepareDag dag;
  ObBackupValidatePrepareTask task;
  ASSERT_EQ(OB_SUCCESS, ctx.init());
  ASSERT_EQ(OB_SUCCESS, dag.set_dag_id(success_dag_id_));
  task.ctx_ = &ctx;
  task.set_dag(dag);

  ASSERT_EQ(OB_SUCCESS, task.select_validate_succ_());
  ASSERT_EQ(OB_SUCCESS, ctx.set_validate_result(OB_CANCELED, "canceled", cancel_dag_id_));

  assert_selected_result(ctx, OB_SUCCESS, success_dag_id_, "");
}

TEST_F(TestBackupValidateTaskContext, cancel_wins_prepare_success)
{
  ObBackupValidateTaskContext ctx;
  ObBackupValidatePrepareDag dag;
  ObBackupValidatePrepareTask task;
  ASSERT_EQ(OB_SUCCESS, ctx.init());
  ASSERT_EQ(OB_SUCCESS, dag.set_dag_id(success_dag_id_));
  task.ctx_ = &ctx;
  task.set_dag(dag);

  ASSERT_EQ(OB_SUCCESS, ctx.set_validate_result(OB_CANCELED, "canceled", cancel_dag_id_));
  ASSERT_EQ(OB_SUCCESS, task.select_validate_succ_());

  assert_selected_result(ctx, OB_CANCELED, cancel_dag_id_, "canceled");
}

TEST_F(TestBackupValidateTaskContext, finish_success_wins_cancel)
{
  ObBackupValidateTaskContext ctx;
  ObBackupValidateFinishDag dag;
  ObBackupValidateFinishTask task;
  ASSERT_EQ(OB_SUCCESS, ctx.init());
  ASSERT_EQ(OB_SUCCESS, dag.set_dag_id(success_dag_id_));
  task.ctx_ = &ctx;
  task.set_dag(dag);

  ASSERT_EQ(OB_SUCCESS, task.select_validate_succ_());
  ASSERT_EQ(OB_SUCCESS, ctx.set_validate_result(OB_CANCELED, "canceled", cancel_dag_id_));

  assert_selected_result(ctx, OB_SUCCESS, success_dag_id_, "");
}

TEST_F(TestBackupValidateTaskContext, cancel_wins_finish_success)
{
  ObBackupValidateTaskContext ctx;
  ObBackupValidateFinishDag dag;
  ObBackupValidateFinishTask task;
  ASSERT_EQ(OB_SUCCESS, ctx.init());
  ASSERT_EQ(OB_SUCCESS, dag.set_dag_id(success_dag_id_));
  task.ctx_ = &ctx;
  task.set_dag(dag);

  ASSERT_EQ(OB_SUCCESS, ctx.set_validate_result(OB_CANCELED, "canceled", cancel_dag_id_));
  ASSERT_EQ(OB_SUCCESS, task.select_validate_succ_());

  assert_selected_result(ctx, OB_CANCELED, cancel_dag_id_, "canceled");
}

TEST_F(TestBackupValidateTaskContext, report_failure_keeps_selected_terminal_result)
{
  ObBackupValidateTaskContext ctx;
  ObBackupValidateDagNetInitParam param;
  share::ObLocationService location_service;
  common::ObMySQLProxy sql_proxy;
  obrpc::ObSrvRpcProxy rpc_proxy;
  backup::ObBackupReportCtx report_ctx;
  ASSERT_EQ(OB_SUCCESS, ctx.init());
  set_valid_param(param);
  report_ctx.location_service_ = &location_service;
  report_ctx.sql_proxy_ = &sql_proxy;
  report_ctx.rpc_proxy_ = &rpc_proxy;
  ASSERT_TRUE(report_ctx.is_valid());
  ASSERT_EQ(OB_SUCCESS, ctx.set_validate_result(OB_SUCCESS, "", success_dag_id_));

  EXPECT_EQ(OB_NOT_INIT, ctx.report_validate_result(param, self_, report_ctx));
  EXPECT_EQ(ObBackupValidateTaskContext::REPORT_FAILED, ctx.report_state_);
  ASSERT_EQ(OB_SUCCESS, ctx.set_validate_result(OB_CANCELED, "canceled", cancel_dag_id_));
  EXPECT_EQ(OB_SUCCESS, ctx.result_);
  EXPECT_TRUE(ctx.result_dag_id_.equals(success_dag_id_));

  EXPECT_EQ(OB_STATE_NOT_MATCH, ctx.report_validate_result(param, self_, report_ctx));
  EXPECT_EQ(ObBackupValidateTaskContext::REPORT_FAILED, ctx.report_state_);
}

TEST_F(TestBackupValidateTaskContext, backup_dag_net_first_terminal_result_wins)
{
  backup::ObLSBackupDataDagNet dag_net;
  ASSERT_TRUE(dag_net.need_dag_net_finalizer());

  ASSERT_EQ(OB_SUCCESS, dag_net.set_result(OB_IO_ERROR, success_dag_id_));
  ASSERT_EQ(OB_SUCCESS, dag_net.set_result(OB_SUCCESS, cancel_dag_id_));

  EXPECT_TRUE(dag_net.result_selected_);
  EXPECT_EQ(OB_IO_ERROR, dag_net.result_);
  EXPECT_TRUE(dag_net.result_dag_id_.equals(success_dag_id_));
}

TEST_F(TestBackupValidateTaskContext, backup_dag_net_cancel_selects_result)
{
  backup::ObLSBackupDataDagNet dag_net;
  ASSERT_EQ(OB_SUCCESS, dag_net.set_dag_id(cancel_dag_id_));

  ASSERT_EQ(OB_SUCCESS, dag_net.deal_with_cancel());
  ASSERT_EQ(OB_SUCCESS, dag_net.set_result(OB_IO_ERROR, success_dag_id_));

  EXPECT_TRUE(dag_net.result_selected_);
  EXPECT_EQ(OB_CANCELED, dag_net.result_);
  EXPECT_TRUE(dag_net.result_dag_id_.equals(cancel_dag_id_));
}

TEST_F(TestBackupValidateTaskContext, tablet_fuse_uses_dag_net_finalizer)
{
  backup::ObBackupTabletFuseDagNet dag_net;
  EXPECT_TRUE(dag_net.need_dag_net_finalizer());
}

} // namespace storage
} // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_backup_validate_task_context.log*");
  OB_LOGGER.set_file_name("test_backup_validate_task_context.log", true);
  OB_LOGGER.set_log_level("info");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
