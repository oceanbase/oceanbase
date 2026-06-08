/**
 * Copyright (c) 2024 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX RS

#include <gtest/gtest.h>
#include "lib/ob_define.h"
#include "lib/container/ob_se_array.h"
#include "rootserver/mview/ob_mview_pending_task_manager.h"
#include "share/ob_rpc_struct.h"
#include "share/rc/ob_tenant_base.h"
#include "share/schema/ob_schema_struct.h"

using namespace oceanbase;
using namespace oceanbase::rootserver;
using namespace oceanbase::common;

static int init_task(ObMViewPendingTask &task,
                     const uint64_t mview_id,
                     uint64_t *dep_ids = NULL,
                     const int64_t dep_cnt = 0)
{
  int ret = OB_SUCCESS;
  task.tenant_id_ = OB_SYS_TENANT_ID;
  task.refresh_id_ = 100L;
  task.mview_id_ = mview_id;
  task.seq_ = 0;
  task.target_data_sync_scn_ = 0;
  task.status_ = MV_TASK_PENDING;
  task.skip_cnt_ = 0;
  task.retry_count_ = 0;
  task.next_retry_ts_ = 0;
  task.flags_ = 0;
  task.dep_mview_id_cnt_ = dep_cnt;
  task.dep_mview_ids_ = dep_ids;
  task.gmt_create_ = 0;
  task.gmt_modified_ = 0;
  task.svr_addr_.reset();
  task.session_id_ = 0;
  return ret;
}

// ===========================================================================
// Manager Pre-Init Contract Tests
// ===========================================================================
//
// Every public method of ObMViewPendingTaskManager checks is_inited_ first.
// These tests exercise that contract without needing MTL context, SQL proxy,
// or RPC proxy — the early-return path is exercised in isolation.
//
// DISABLED_ tests are full-stack cases that require:
//   - MTL context (MTL_ID() must return a valid tenant_id)
//   - GCTX.sql_proxy_ pointing at a live test database
//   - GCTX.srv_rpc_proxy_ for run_task()
// ===========================================================================

class ObMViewPendingTaskManagerTest : public ::testing::Test
{
public:
  static constexpr uint64_t MGR_TENANT_ID  = OB_SYS_TENANT_ID;
  static constexpr int64_t  MGR_REFRESH_ID = 100L;
  static constexpr uint64_t MGR_MVIEW_ID   = 1001UL;

  ObMViewPendingTaskManagerTest()
    : tenant_base_(MGR_TENANT_ID)
  {
  }

  void SetUp() override
  {
    share::ObTenantEnv::set_tenant(&tenant_base_);
  }

  void TearDown() override
  {
    share::ObTenantEnv::set_tenant(NULL);
  }

  ObMViewPendingTaskManager manager_;
  share::ObTenantBase tenant_base_;
};

// ---------------------------------------------------------------------------
// 1. Every public method before init() returns OB_NOT_INIT
// ---------------------------------------------------------------------------

TEST_F(ObMViewPendingTaskManagerTest, AllOpsBeforeInitReturnNotInit)
{
  ObMViewPendingTask task;
  ObMViewTaskStatus  status;
  int64_t            refresh_id_out = 0;

  EXPECT_EQ(OB_NOT_INIT, manager_.peek_task(task));
  EXPECT_EQ(OB_NOT_INIT,
            manager_.get_refresh_status(MGR_TENANT_ID, MGR_REFRESH_ID, status));
  EXPECT_EQ(OB_NOT_INIT,
            manager_.mark_task_running(MGR_TENANT_ID, MGR_REFRESH_ID, MGR_MVIEW_ID,
                                       common::ObAddr()));
  EXPECT_EQ(OB_NOT_INIT,
            manager_.mark_task_success(MGR_TENANT_ID, MGR_REFRESH_ID, MGR_MVIEW_ID));
  EXPECT_EQ(OB_NOT_INIT,
            manager_.mark_task_retry_wait(MGR_TENANT_ID, MGR_REFRESH_ID, MGR_MVIEW_ID));
  EXPECT_EQ(OB_NOT_INIT,
            manager_.mark_task_failed(MGR_TENANT_ID, MGR_REFRESH_ID, MGR_MVIEW_ID));
  EXPECT_EQ(OB_NOT_INIT,
            manager_.mark_all_tasks_canceled(MGR_TENANT_ID, MGR_REFRESH_ID));
  EXPECT_EQ(OB_NOT_INIT,
            manager_.recycle_refresh(MGR_TENANT_ID, MGR_REFRESH_ID));
  EXPECT_EQ(OB_NOT_INIT,
            manager_.run_task(MGR_TENANT_ID,
                              MGR_REFRESH_ID,
                              MGR_MVIEW_ID,
                              0,
                              share::schema::ObMVRefreshMethod::MAX,
                              0,
                              0,
                              false,
                              0,
                              common::ObAddr()));
  obrpc::ObScheduleMViewRefreshArg arg;
  arg.tenant_id_ = MGR_TENANT_ID;
  arg.mview_id_ = MGR_MVIEW_ID;
  arg.is_nested_ = false;
  arg.refresh_method_ = share::schema::ObMVRefreshMethod::MAX;
  arg.refresh_parallel_ = 0;
  EXPECT_EQ(OB_NOT_INIT, manager_.schedule_task(arg, refresh_id_out));
}

// ---------------------------------------------------------------------------
// 2. New public methods added in kill/cancel feature also return OB_NOT_INIT
// ---------------------------------------------------------------------------

TEST_F(ObMViewPendingTaskManagerTest, KillMethodsBeforeInitReturnNotInit)
{
  obrpc::ObKillMViewRefreshArg kill_arg;
  kill_arg.tenant_id_ = MGR_TENANT_ID;
  kill_arg.refresh_id_ = MGR_REFRESH_ID;
  kill_arg.is_kill_by_mview_id_ = false;
  EXPECT_EQ(OB_NOT_INIT, manager_.kill_refresh(kill_arg));
  EXPECT_EQ(OB_NOT_INIT, manager_.kill_refresh_local(MGR_TENANT_ID, MGR_REFRESH_ID));
  EXPECT_EQ(OB_NOT_INIT, manager_.mark_all_tasks_canceled(MGR_TENANT_ID, MGR_REFRESH_ID));
}

// ---------------------------------------------------------------------------
// 3. New columns svr_addr_ / session_id_ are zero-initialised by init_task
// ---------------------------------------------------------------------------

TEST(ObMViewPendingTaskTest, NewColumnFieldsDefaultZero)
{
  ObMViewPendingTask task;
  ASSERT_EQ(OB_SUCCESS, init_task(task, 1001UL));
  EXPECT_FALSE(task.svr_addr_.is_valid());
  EXPECT_EQ(0U, task.session_id_);
}

TEST_F(ObMViewPendingTaskManagerTest, CheckReloadTasksReachable)
{
  ObMViewPendingTask leaf_task;
  ObMViewPendingTask mid_task;
  ObMViewPendingTask orphan_task;
  ObMViewPendingTask root_task;
  uint64_t mid_deps[] = {1001UL};
  uint64_t root_deps[] = {1002UL};
  bool is_valid = false;
  ObSEArray<ObMViewPendingTask *, 4> group;

  ASSERT_EQ(OB_SUCCESS, init_task(leaf_task, 1001UL));
  // Simulate a leaf task that was RUNNING when RS restarted: svr_addr and
  // session_id are loaded from disk and must survive the reachability check.
  leaf_task.status_    = MV_TASK_RUNNING;
  leaf_task.session_id_ = 12345;
  ASSERT_TRUE(leaf_task.svr_addr_.set_ip_addr("10.0.0.1", 2881));

  ASSERT_EQ(OB_SUCCESS, init_task(mid_task, 1002UL, mid_deps, 1));
  ASSERT_EQ(OB_SUCCESS, init_task(orphan_task, 1004UL));
  ASSERT_EQ(OB_SUCCESS, init_task(root_task, 1003UL, root_deps, 1));
  ASSERT_EQ(OB_SUCCESS, group.push_back(&leaf_task));
  ASSERT_EQ(OB_SUCCESS, group.push_back(&mid_task));
  ASSERT_EQ(OB_SUCCESS, group.push_back(&root_task));
  EXPECT_EQ(OB_SUCCESS, manager_.check_reload_tasks_reachable(group, is_valid));
  EXPECT_TRUE(is_valid);
  // check_reload_tasks_reachable must not modify task fields
  EXPECT_EQ(MV_TASK_RUNNING,      leaf_task.status_);
  EXPECT_EQ(12345U,               leaf_task.session_id_);
  EXPECT_TRUE(leaf_task.svr_addr_.is_valid());
  EXPECT_EQ(2881,                 leaf_task.svr_addr_.get_port());

  group.reuse();
  ASSERT_EQ(OB_SUCCESS, group.push_back(&leaf_task));
  ASSERT_EQ(OB_SUCCESS, group.push_back(&root_task));
  EXPECT_EQ(OB_SUCCESS, manager_.check_reload_tasks_reachable(group, is_valid));
  EXPECT_FALSE(is_valid);

  group.reuse();
  ASSERT_EQ(OB_SUCCESS, group.push_back(&leaf_task));
  ASSERT_EQ(OB_SUCCESS, group.push_back(&mid_task));
  ASSERT_EQ(OB_SUCCESS, group.push_back(&orphan_task));
  ASSERT_EQ(OB_SUCCESS, group.push_back(&root_task));
  EXPECT_EQ(OB_SUCCESS, manager_.check_reload_tasks_reachable(group, is_valid));
  EXPECT_FALSE(is_valid);

  group.reuse();
  EXPECT_EQ(OB_SUCCESS, manager_.check_reload_tasks_reachable(group, is_valid));
  EXPECT_FALSE(is_valid);
}

int main(int argc, char **argv)
{
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
