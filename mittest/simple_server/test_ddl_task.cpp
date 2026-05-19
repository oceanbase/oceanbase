/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <gtest/gtest.h>
#define USING_LOG_PREFIX SERVER
#define protected public
#define private public

#include "env/ob_simple_cluster_test_base.h"
#include "mittest/env/ob_simple_server_helper.h"
#include "rootserver/ob_root_service.h"
#include "rootserver/ob_snapshot_info_manager.h"

namespace oceanbase
{
namespace unittest
{

using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::storage;
using namespace oceanbase::rootserver;


class ObDDLTaskTest : public ObSimpleClusterTestBase
{
public:
  ObDDLTaskTest() : ObSimpleClusterTestBase("test_ddl_task_") {}
};

TEST_F(ObDDLTaskTest, create_tenant)
{
  uint64_t tenant_id;
  ASSERT_EQ(OB_SUCCESS, create_tenant());
  ASSERT_EQ(OB_SUCCESS, get_tenant_id(tenant_id));
  ASSERT_NE(0, tenant_id);
  ASSERT_EQ(OB_SUCCESS, get_curr_simple_server().init_sql_proxy2());
}

class MockDDLTask : public ObDDLTask
{
public:
  MockDDLTask() : ObDDLTask(DDL_INVALID) {  }
  virtual int process() { return OB_SUCCESS; }
  virtual int cleanup_impl() { return OB_SUCCESS; }
  virtual void flt_set_task_span_tag() const {  }
  virtual void flt_set_status_span_tag() const {  }
};

TEST_F(ObDDLTaskTest, switch_status)
{
  common::ObMySQLProxy &tenant_sql_proxy = get_curr_simple_server().get_sql_proxy2();
  ObArenaAllocator arena;
  uint64_t tenant_id = 0;
  ASSERT_EQ(OB_SUCCESS, get_tenant_id(tenant_id));
  MockDDLTask task;
  ObDDLTaskRecord task_record;
  task.ddl_stmt_str_  = "create index xxx";
  task.gmt_create_ = ObTimeUtility::current_time();
  task.tenant_id_ = tenant_id;
  task.object_id_ = 1;
  task.target_object_id_ = 1;
  task.schema_version_ = 1;
  task.task_type_ = DDL_CREATE_INDEX;
  task.task_status_ = ObDDLTaskStatus::PREPARE;
  task.task_id_ = 1;
  task.parent_task_id_ = 0;
  task.task_version_ = 1;
  task.execution_id_ = 1;
  task.ret_code_ = OB_SUCCESS;
  task.trace_id_.id_.seq_ = 1;
  ASSERT_EQ(OB_SUCCESS, task.convert_to_record(task_record, arena));
  ASSERT_EQ(OB_SUCCESS, ObDDLTaskRecordOperator::insert_record(*GCTX.sql_proxy_, task_record));
  ASSERT_EQ(OB_SUCCESS, task.switch_status(ObDDLTaskStatus::DROP_SCHEMA, false/*enable_flt*/, OB_SUCCESS));
}

// Regression for SNAPSHOT_FOR_CREATE_INDEX leak (DIMA-2026030900114516370).
// Covers the scn-based release path used when task schema no longer has tablets.
TEST_F(ObDDLTaskTest, batch_release_snapshot_by_scn)
{
  uint64_t tenant_id = 0;
  ASSERT_EQ(OB_SUCCESS, get_tenant_id(tenant_id));
  ASSERT_NE(0, tenant_id);
  ASSERT_NE(GCTX.root_service_, nullptr);
  ASSERT_NE(GCTX.sql_proxy_, nullptr);
  while (!GCTX.root_service_->is_full_service()) {
    ob_usleep(100 * 1000);
  }

  ObSnapshotInfoManager &snapshot_mgr =
      GCTX.root_service_->get_ddl_service().get_snapshot_mgr();
  ObMySQLProxy &sql_proxy = *GCTX.sql_proxy_;

  ObSEArray<ObTabletID, 2> tablet_ids;
  ASSERT_EQ(OB_SUCCESS, tablet_ids.push_back(ObTabletID(900000001)));
  ASSERT_EQ(OB_SUCCESS, tablet_ids.push_back(ObTabletID(900000002)));

  const int64_t schema_version = 200;
  SCN snapshot_scn;
  ASSERT_EQ(OB_SUCCESS, snapshot_scn.convert_for_tx(ObTimeUtility::current_time_ns()));

  ASSERT_EQ(OB_SUCCESS, snapshot_mgr.batch_acquire_snapshot_in_trans(
      sql_proxy, SNAPSHOT_FOR_DDL, tenant_id,
      schema_version, snapshot_scn, "ut_release_fallback", tablet_ids));

  ObSqlString check_sql;
  ASSERT_EQ(OB_SUCCESS, check_sql.assign_fmt(
      "select count(*) val from oceanbase.__all_acquired_snapshot "
      "where snapshot_type=%d and schema_version=%ld and snapshot_scn=%lu",
      static_cast<int>(SNAPSHOT_FOR_DDL), schema_version,
      snapshot_scn.get_val_for_inner_table_field()));

  int64_t cnt = -1;
  ASSERT_EQ(OB_SUCCESS, SSH::g_select_int64(tenant_id, check_sql.ptr(), cnt));
  ASSERT_EQ(2, cnt);

  ObSEArray<ObTabletID, 2> empty_ids;
  MockDDLTask invalid_task;
  invalid_task.tenant_id_ = tenant_id;
  invalid_task.schema_version_ = 0;
  invalid_task.task_id_ = 2;
  invalid_task.snapshot_version_ = snapshot_scn.get_val_for_tx();
  ASSERT_EQ(OB_INVALID_ARGUMENT,
      invalid_task.batch_release_snapshot(invalid_task.snapshot_version_, empty_ids));
  invalid_task.schema_version_ = schema_version;
  invalid_task.snapshot_version_ = 0;
  ASSERT_EQ(OB_INVALID_ARGUMENT,
      invalid_task.batch_release_snapshot(invalid_task.snapshot_version_, empty_ids));
  invalid_task.snapshot_version_ = snapshot_scn.get_val_for_tx();
  ASSERT_EQ(OB_SUCCESS, SSH::g_select_int64(tenant_id, check_sql.ptr(), cnt));
  ASSERT_EQ(2, cnt);

  ObArenaAllocator arena;
  ObDDLTaskRecord task_record;
  MockDDLTask task;
  task.ddl_stmt_str_  = "create index xxx";
  task.gmt_create_ = ObTimeUtility::current_time();
  task.tenant_id_ = tenant_id;
  task.object_id_ = 1;
  task.target_object_id_ = 1;
  task.schema_version_ = schema_version;
  task.task_id_ = 2;
  task.task_type_ = DDL_CREATE_INDEX;
  task.task_status_ = ObDDLTaskStatus::PREPARE;
  task.parent_task_id_ = 0;
  task.task_version_ = 1;
  task.execution_id_ = 1;
  task.ret_code_ = OB_SUCCESS;
  task.trace_id_.id_.seq_ = 2;
  task.snapshot_version_ = snapshot_scn.get_val_for_tx();
  ASSERT_EQ(OB_SUCCESS, task.convert_to_record(task_record, arena));
  ASSERT_EQ(OB_SUCCESS, ObDDLTaskRecordOperator::insert_record(*GCTX.sql_proxy_, task_record));
  ASSERT_EQ(OB_SUCCESS,
      task.batch_release_snapshot(task.snapshot_version_, empty_ids));
  ASSERT_EQ(0, task.snapshot_version_);
  ASSERT_EQ(OB_SUCCESS, SSH::g_select_int64(tenant_id, check_sql.ptr(), cnt));
  ASSERT_EQ(0, cnt);

  ObSqlString task_sql;
  ASSERT_EQ(OB_SUCCESS, task_sql.assign_fmt(
      "select snapshot_version val from oceanbase.__all_ddl_task_status "
      "where task_id=%ld", task.task_id_));
  int64_t persisted_snapshot = -1;
  ASSERT_EQ(OB_SUCCESS, SSH::g_select_int64(tenant_id, task_sql.ptr(), persisted_snapshot));
  ASSERT_EQ(0, persisted_snapshot);
}

TEST_F(ObDDLTaskTest, end)
{
  // used for debug
//  ::sleep(3600);
}

}
}

int main(int argc, char **argv)
{
  oceanbase::unittest::init_log_and_gtest(argc, argv);
  OB_LOGGER.set_log_level("INFO");

  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
