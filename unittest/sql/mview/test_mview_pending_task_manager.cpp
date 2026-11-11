/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX RS

#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include "lib/ob_define.h"
#include "lib/container/ob_se_array.h"
#define private public
#include "rootserver/mview/ob_mview_pending_task_manager.h"
#include "rootserver/mview/ob_mview_pending_task_inspection_task.h"
#undef private
#include "rootserver/ddl_task/ob_build_mview_task.h"
#include "share/ob_rpc_struct.h"
#include "share/rc/ob_tenant_base.h"
#include "share/schema/ob_schema_struct.h"
#include "share/mock_mysql_proxy.h"
#include "fake_mysql_result.h"

using ::testing::_;
using ::testing::Invoke;

using namespace oceanbase;
using namespace oceanbase::rootserver;
using namespace oceanbase::common;
using namespace oceanbase::unittest;

class ObMViewCancelTestSQLConnection : public sqlclient::ObISQLConnection
{
public:
  ObMViewCancelTestSQLConnection(ObSqlString &last_write_sql, int64_t &write_count)
    : last_write_sql_(last_write_sql), write_count_(write_count)
  {
  }

  int execute_read(const uint64_t,
                   const ObString &,
                   ObISQLClient::ReadResult &,
                   bool,
                   const ObAddr *) override
  {
    return OB_NOT_SUPPORTED;
  }

  int execute_read(const int64_t,
                   const uint64_t,
                   const ObString &,
                   ObISQLClient::ReadResult &,
                   bool,
                   const ObAddr *) override
  {
    return OB_NOT_SUPPORTED;
  }

  int execute_write(const uint64_t,
                    const ObString &sql,
                    int64_t &affected_rows,
                    bool,
                    const ObAddr *) override
  {
    int ret = forced_write_ret_;
    if (OB_SUCC(ret)) {
      ret = last_write_sql_.assign(sql);
      if (OB_SUCC(ret)) {
        ++write_count_;
        affected_rows = 1;
      }
    }
    return ret;
  }

  int execute_proc(const uint64_t,
                   ObIAllocator &,
                   ParamStore &,
                   ObString &,
                   const share::schema::ObRoutineInfo &,
                   const ObIArray<const pl::ObUserDefinedType *> &,
                   const ObTimeZoneInfo *,
                   ObObj *,
                   bool) override
  {
    return OB_NOT_SUPPORTED;
  }

  int start_transaction(const uint64_t &, bool) override { return OB_SUCCESS; }
  int rollback() override { return OB_SUCCESS; }
  int commit() override { return OB_SUCCESS; }
  int get_session_variable(const ObString &, int64_t &) override { return OB_NOT_SUPPORTED; }
  int set_session_variable(const ObString &, int64_t) override { return OB_NOT_SUPPORTED; }
  int set_session_variable(const ObString &, const ObString &) override { return OB_NOT_SUPPORTED; }
  sqlclient::ObCommonServerConnectionPool *get_common_server_pool() override { return NULL; }

  int forced_write_ret_ = OB_SUCCESS;

private:
  ObSqlString &last_write_sql_;
  int64_t &write_count_;
};

class ObMViewCancelTestSQLConnectionPool : public sqlclient::ObISQLConnectionPool
{
public:
  explicit ObMViewCancelTestSQLConnectionPool(sqlclient::ObISQLConnection &connection)
    : connection_(connection)
  {
  }

  int escape(const char *from,
             const int64_t from_size,
             char *to,
             const int64_t to_size,
             int64_t &out_size) override
  {
    int ret = OB_SUCCESS;
    out_size = 0;
    if (OB_UNLIKELY(NULL == from || NULL == to || from_size < 0 || to_size < from_size)) {
      ret = OB_INVALID_ARGUMENT;
    } else {
      MEMCPY(to, from, from_size);
      out_size = from_size;
    }
    return ret;
  }

  int acquire(const uint64_t,
              sqlclient::ObISQLConnection *&connection,
              ObISQLClient *,
              const int32_t) override
  {
    connection = &connection_;
    return OB_SUCCESS;
  }

  int release(sqlclient::ObISQLConnection *, const bool) override { return OB_SUCCESS; }
  int on_client_inactive(ObISQLClient *) override { return OB_SUCCESS; }
  sqlclient::ObSQLConnPoolType get_type() override { return sqlclient::MYSQL_POOL; }
  sqlclient::DblinkDriverProto get_pool_link_driver_proto() override
  {
    return sqlclient::DBLINK_UNKNOWN;
  }

  int create_dblink_pool(const sqlclient::dblink_param_ctx &,
                         const ObString &,
                         int32_t,
                         const ObString &,
                         const ObString &,
                         const ObString &,
                         const ObString &,
                         const ObString &,
                         const ObString &) override
  {
    return OB_NOT_SUPPORTED;
  }

  int acquire_dblink(const sqlclient::dblink_param_ctx &,
                     sqlclient::ObISQLConnection *&) override
  {
    return OB_NOT_SUPPORTED;
  }

  int release_dblink(sqlclient::ObISQLConnection *) override { return OB_NOT_SUPPORTED; }
  int do_acquire_dblink(const sqlclient::dblink_param_ctx &,
                        sqlclient::ObISQLConnection *&) override
  {
    return OB_NOT_SUPPORTED;
  }

  int try_connect_dblink(sqlclient::ObISQLConnection *, int64_t) override
  {
    return OB_NOT_SUPPORTED;
  }

  int clean_dblink_connection(uint64_t) override { return OB_NOT_SUPPORTED; }

private:
  sqlclient::ObISQLConnection &connection_;
};

class ObMViewCancelTestSQLClient : public ObISQLClient
{
public:
  ObMViewCancelTestSQLClient()
    : last_write_sql_(),
      last_read_sql_(),
      write_count_(0),
      read_count_(0),
      empty_result_(),
      connection_(last_write_sql_, write_count_),
      pool_(connection_)
  {
  }

  int escape(const char *from,
             const int64_t from_size,
             char *to,
             const int64_t to_size,
             int64_t &out_size) override
  {
    return pool_.escape(from, from_size, to, to_size, out_size);
  }

  int read(ReadResult &result, const uint64_t, const char *sql) override
  {
    int ret = last_read_sql_.assign(sql);
    FakeResultHandler *handler = NULL;
    FakeMySQLResult *fake_result = &empty_result_;
    if (OB_SUCC(ret)) {
      ++read_count_;
      empty_result_.reset_cursor();
      ret = result.create_handler(handler, fake_result);
    }
    return ret;
  }

  int read(ReadResult &result,
           const int64_t,
           const uint64_t tenant_id,
           const char *sql) override
  {
    return read(result, tenant_id, sql);
  }

  int read(ReadResult &result,
           const uint64_t tenant_id,
           const char *sql,
           const int32_t) override
  {
    return read(result, tenant_id, sql);
  }

  int write(const uint64_t tenant_id, const char *sql, int64_t &affected_rows) override
  {
    return connection_.execute_write(tenant_id,
                                     ObString::make_string(sql),
                                     affected_rows,
                                     false,
                                     NULL);
  }

  int write(const uint64_t tenant_id,
            const char *sql,
            const int32_t,
            int64_t &affected_rows) override
  {
    return write(tenant_id, sql, affected_rows);
  }

  sqlclient::ObISQLConnectionPool *get_pool() override { return &pool_; }
  sqlclient::ObISQLConnection *get_connection() override { return &connection_; }
  bool is_oracle_mode() const override { return false; }

  int load_rows(const FakeRow *rows, int64_t count)
  {
    return empty_result_.load_rows(rows, count);
  }

  void force_write_error(int ret) { connection_.forced_write_ret_ = ret; }

  ObSqlString last_write_sql_;
  ObSqlString last_read_sql_;
  int64_t write_count_;
  int64_t read_count_;

private:
  FakeMySQLResult empty_result_;
  ObMViewCancelTestSQLConnection connection_;
  ObMViewCancelTestSQLConnectionPool pool_;
};

// ===========================================================================
// Manager local-operation pre-init contract tests
// ===========================================================================
//
// Operational methods of ObMViewPendingTaskManager check is_inited_ first.
// Monitoring readers are the exception: an unready manager is exposed as an
// empty result. These tests exercise both contracts without needing MTL
// context, SQL proxy, or RPC proxy.
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
// 1. Public methods before init() follow their operational/monitoring contracts
// ---------------------------------------------------------------------------

TEST_F(ObMViewPendingTaskManagerTest, OpsBeforeInitFollowContracts)
{
  ObMViewPendingTaskGroup::GroupInfo info;
  ObMViewPendingTask task;

  EXPECT_EQ(OB_NOT_INIT, manager_.find_next_ready_task(info, task));
  EXPECT_EQ(OB_NOT_INIT,
            manager_.mark_task_running(MGR_TENANT_ID, MGR_REFRESH_ID, MGR_MVIEW_ID,
                                       common::ObAddr()));
  EXPECT_EQ(OB_NOT_INIT,
            manager_.mark_task_retry_wait(MGR_TENANT_ID, MGR_REFRESH_ID, MGR_MVIEW_ID));
  EXPECT_EQ(OB_NOT_INIT,
            manager_.mark_task_pending(MGR_TENANT_ID, MGR_REFRESH_ID, MGR_MVIEW_ID));
  EXPECT_EQ(OB_NOT_INIT,
            manager_.finalize_task(MGR_TENANT_ID, MGR_REFRESH_ID, MGR_MVIEW_ID, OB_SUCCESS));
  EXPECT_EQ(OB_NOT_INIT,
            manager_.recycle_refresh(MGR_TENANT_ID, MGR_REFRESH_ID));
  EXPECT_EQ(OB_NOT_INIT,
            manager_.resync_task_from_disk(MGR_TENANT_ID, MGR_REFRESH_ID, MGR_MVIEW_ID));
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
  EXPECT_EQ(OB_NOT_INIT,
            manager_.push_task_result(MGR_TENANT_ID,
                                      MGR_REFRESH_ID,
                                      MGR_MVIEW_ID,
                                      0,
                                      OB_SUCCESS));
  EXPECT_EQ(OB_NOT_INIT, manager_.cleanup_stale_drop_blocks());

  EXPECT_EQ(OB_NOT_INIT, manager_.reload_tasks());

  obrpc::ObScheduleMViewRefreshArg refresh_arg;
  refresh_arg.tenant_id_ = MGR_TENANT_ID;
  refresh_arg.run_user_id_ = OB_SYS_USER_ID;
  refresh_arg.mview_id_ = MGR_MVIEW_ID;
  refresh_arg.refresh_method_ = share::schema::ObMVRefreshMethod::FAST;
  obrpc::ObScheduleMViewRefreshResult refresh_result;
  EXPECT_EQ(OB_NOT_INIT, manager_.schedule_mview_refresh_local(refresh_arg, refresh_result));

  obrpc::ObNotifyMViewTimerArg notify_arg;
  notify_arg.tenant_id_ = MGR_TENANT_ID;
  notify_arg.mview_id_ = MGR_MVIEW_ID;
  notify_arg.op_type_ = obrpc::ObNotifyMViewTimerArg::REGISTER;
  EXPECT_EQ(OB_NOT_INIT, manager_.notify_mview_timer_local(notify_arg));

  ObSEArray<ObMViewSchedulerTimerSnapshot, 1> timer_snapshots;
  EXPECT_EQ(OB_SUCCESS, manager_.get_scheduler_timer_snapshot(timer_snapshots));
  EXPECT_TRUE(timer_snapshots.empty());
}

// ---------------------------------------------------------------------------
// 2. Local cancel method before init also returns OB_NOT_INIT
// ---------------------------------------------------------------------------

TEST_F(ObMViewPendingTaskManagerTest, LocalCancelBeforeInitReturnsNotInit)
{
  bool refresh_exists = true;
  obrpc::ObCancelMViewRefreshArg cancel_arg;
  cancel_arg.tenant_id_ = MGR_TENANT_ID;
  cancel_arg.refresh_id_ = MGR_REFRESH_ID;
  cancel_arg.is_cancel_by_mview_id_ = false;
  EXPECT_EQ(OB_NOT_INIT, manager_.cancel_refresh_local(cancel_arg, refresh_exists));
  EXPECT_FALSE(refresh_exists);
}

// ---------------------------------------------------------------------------
// 3. Default-constructed ObMViewPendingTask leaves svr fields invalid
// ---------------------------------------------------------------------------

TEST(ObMViewPendingTaskTest, PendingTaskSvrFieldsDefaultInvalid)
{
  ObMViewPendingTask task;
  EXPECT_EQ(MV_TASK_PENDING, task.status_);
  EXPECT_EQ(0, task.start_time_);
  EXPECT_FALSE(task.svr_addr_.is_valid());
  EXPECT_EQ(0U, task.session_id_);
}

// ---------------------------------------------------------------------------
// 4. ObScheduleMViewTaskParam::is_valid accepts mview_id, group_id, or both
// ---------------------------------------------------------------------------

TEST(ObScheduleMViewTaskParamTest, IsValidAcceptsMviewOrGroupId)
{
  ObScheduleMViewTaskParam param;
  EXPECT_FALSE(param.is_valid());
  // Only a timer-triggered refresh carries a trigger timestamp.
  EXPECT_EQ(OB_INVALID_TIMESTAMP, param.trigger_ts_);

  param.tenant_id_ = OB_SYS_TENANT_ID;
  EXPECT_FALSE(param.is_valid());

  param.mview_id_ = 1001UL;
  EXPECT_TRUE(param.is_valid());

  param.group_id_ = 2001UL;
  EXPECT_TRUE(param.is_valid());

  param.mview_id_ = OB_INVALID_ID;
  param.group_id_ = 2001UL;
  EXPECT_TRUE(param.is_valid());

  param.tenant_id_ = OB_INVALID_TENANT_ID;
  EXPECT_FALSE(param.is_valid());
}

TEST(ObScheduleMViewRefreshArgTest, InitialRefreshRequiresRunUser)
{
  obrpc::ObScheduleMViewRefreshArg arg;
  arg.tenant_id_ = OB_SYS_TENANT_ID;
  arg.mview_id_ = 1001UL;
  EXPECT_TRUE(arg.is_valid());

  arg.is_initial_refresh_ = true;
  EXPECT_FALSE(arg.is_valid());

  arg.run_user_id_ = OB_SYS_USER_ID;
  EXPECT_TRUE(arg.is_valid());
}

TEST(ObNotifyMViewTimerArgTest, ValidatesExclusiveScopeAndDatabaseOperation)
{
  obrpc::ObNotifyMViewTimerArg arg;
  arg.tenant_id_ = OB_SYS_TENANT_ID;
  arg.op_type_ = obrpc::ObNotifyMViewTimerArg::REGISTER;
  EXPECT_FALSE(arg.is_valid());

  arg.mview_id_ = 1001UL;
  EXPECT_TRUE(arg.is_valid());

  arg.op_type_ = obrpc::ObNotifyMViewTimerArg::ACTIVATE;
  EXPECT_TRUE(arg.is_valid());

  arg.database_id_ = 2001UL;
  EXPECT_FALSE(arg.is_valid());

  arg.mview_id_ = OB_INVALID_ID;
  arg.op_type_ = obrpc::ObNotifyMViewTimerArg::REGISTER;
  EXPECT_FALSE(arg.is_valid());

  arg.op_type_ = obrpc::ObNotifyMViewTimerArg::ACTIVATE;
  EXPECT_TRUE(arg.is_valid());

  arg.database_id_ = OB_INVALID_ID;
  arg.mview_id_ = 1001UL;
  arg.op_type_ = obrpc::ObNotifyMViewTimerArg::UNREGISTER;
  EXPECT_TRUE(arg.is_valid());
}

TEST(ObBuildMViewTaskTest, RetryableStatusesIncludeTakeEffect)
{
  ObBuildMViewTask task;

  task.set_task_status(share::ObDDLTaskStatus::BUILD_MLOG);
  EXPECT_TRUE(task.task_can_retry());

  task.set_task_status(share::ObDDLTaskStatus::TAKE_EFFECT);
  EXPECT_TRUE(task.task_can_retry());

  task.set_task_status(share::ObDDLTaskStatus::SUCCESS);
  EXPECT_FALSE(task.task_can_retry());
}

class ObMViewPendingTaskManagerDropCancelTest : public ::testing::Test
{
public:
  static constexpr uint64_t TENANT_ID = OB_SYS_TENANT_ID;
  static constexpr int64_t REFRESH_ID = 100L;
  static constexpr uint64_t DEPENDENCY_MVIEW_ID = 1001UL;
  static constexpr uint64_t TARGET_MVIEW_ID = 1002UL;

  ObMViewPendingTaskManagerDropCancelTest()
    : tenant_base_(TENANT_ID),
      manager_(),
      sql_client_(),
      sql_proxy_(),
      empty_result_(),
      old_sql_proxy_(NULL),
      proxy_read_count_(0),
      proxy_base_info_sql_(),
      proxy_dep_info_sql_()
  {
  }

  void SetUp() override
  {
    share::ObTenantEnv::set_tenant(&tenant_base_);
    old_sql_proxy_ = GCTX.sql_proxy_;
    GCTX.sql_proxy_ = &sql_proxy_;
    EXPECT_CALL(sql_proxy_, read(_, _, _))
        .WillRepeatedly(Invoke(
            this, &ObMViewPendingTaskManagerDropCancelTest::handle_empty_read));
    ASSERT_EQ(OB_SUCCESS, manager_.sched_info_mgr_.init(TENANT_ID, 64L));
    ASSERT_EQ(OB_SUCCESS, manager_.queue_.init(TENANT_ID));
    ASSERT_EQ(OB_SUCCESS, manager_.table_operator_.init(&sql_client_));
    ASSERT_EQ(OB_SUCCESS, manager_.reload_cond_.init(ObWaitEventIds::THREAD_IDLING_COND_WAIT));
    ASSERT_EQ(OB_SUCCESS,
              manager_.mview_context_map_.create(64L, "MVCtxTest", "MVCtxTest", TENANT_ID));
    ASSERT_EQ(OB_SUCCESS,
              manager_.mview_ctx_alloc_.init(lib::ObMallocAllocator::get_instance(),
                                             OB_MALLOC_NORMAL_BLOCK_SIZE,
                                             ObMemAttr(TENANT_ID, "MVCtxTest")));
    ATOMIC_STORE(&manager_.reload_state_,
                 static_cast<int64_t>(ObMViewPendingTaskManager::RS_READY));
    manager_.is_inited_ = true;
  }

  void TearDown() override
  {
    manager_.destroy();
    GCTX.sql_proxy_ = old_sql_proxy_;
    share::ObTenantEnv::set_tenant(NULL);
  }

  int handle_empty_read(ObISQLClient::ReadResult &res,
                        const uint64_t,
                        const char *sql)
  {
    int ret = OB_SUCCESS;
    const bool is_dep_query = NULL != strstr(sql, "dep_mview_id");
    ObSqlString &recorded_sql = is_dep_query ? proxy_dep_info_sql_ : proxy_base_info_sql_;
    FakeResultHandler *handler = NULL;
    FakeMySQLResult *result = &empty_result_;
    ++proxy_read_count_;
    empty_result_.clear();
    if (OB_FAIL(recorded_sql.assign(sql))) {
    } else if (OB_FAIL(res.create_handler(handler, result))) {
    }
    return ret;
  }

  int add_sched_info(const uint64_t mview_id,
                     const uint64_t *dep_mview_ids,
                     const int64_t dep_count)
  {
    int ret = OB_SUCCESS;
    ObMViewSchedInfo *sched_info = OB_NEWx(ObMViewSchedInfo,
                                           &manager_.sched_info_mgr_.alloc_,
                                           manager_.sched_info_mgr_.alloc_);
    if (OB_ISNULL(sched_info)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      sched_info->mview_id_ = mview_id;
      sched_info->group_id_ = mview_id;
      sched_info->specified_refresh_interval_ = ObMViewTieredIntervalCalc::BASE_UNIT_US;
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < dep_count; ++i) {
      if (OB_FAIL(sched_info->dep_mview_ids_.push_back(dep_mview_ids[i]))) {
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(manager_.sched_info_mgr_.info_map_.set_refactored(mview_id, sched_info))) {
    }
    if (OB_FAIL(ret) && OB_NOT_NULL(sched_info)) {
      OB_DELETEx(ObMViewSchedInfo, &manager_.sched_info_mgr_.alloc_, sched_info);
    }
    return ret;
  }

  int publish_group(const uint64_t group_id,
                    const ObIArray<uint64_t> &top_mview_ids,
                    const ObIArray<uint64_t> &all_mview_ids)
  {
    ObMVGroupKey group_key;
    return manager_.sched_info_mgr_.build_and_publish_sched_group(group_id,
                                                                   top_mview_ids,
                                                                   all_mview_ids,
                                                                   group_key);
  }

  int enqueue_nested_refresh()
  {
    int ret = OB_SUCCESS;
    ObArenaAllocator allocator;
    ObSEArray<ObMViewPendingTask, 2> tasks;
    ObMViewPendingTaskGroup *group = NULL;
    ObMViewPendingTask dependency_task;
    dependency_task.mview_id_ = DEPENDENCY_MVIEW_ID;
    ObMViewPendingTask target_task;
    target_task.mview_id_ = TARGET_MVIEW_ID;
    ObMViewPendingTaskGroup::GroupInfo group_info;
    group_info.tenant_id_ = TENANT_ID;
    group_info.refresh_id_ = REFRESH_ID;
    group_info.target_data_sync_scn_ = 1;
    if (OB_FAIL(tasks.push_back(dependency_task))) {
    } else if (OB_FAIL(tasks.push_back(target_task))) {
    } else if (OB_FAIL(ObMViewPendingTaskGroup::alloc_and_build(
                   allocator, group_info, tasks, group))) {
    } else if (OB_FAIL(manager_.queue_.restore_pending_task_group(*group))) {
    }
    return ret;
  }

  int enqueue_refresh(const int64_t refresh_id,
                      const uint64_t mview_id,
                      const uint64_t target_scn,
                      const bool publish)
  {
    int ret = OB_SUCCESS;
    ObArenaAllocator allocator;
    ObSEArray<ObMViewPendingTask, 1> tasks;
    ObMViewPendingTaskGroup *group = NULL;
    ObMViewPendingTask task;
    task.mview_id_ = mview_id;
    ObMViewPendingTaskGroup::GroupInfo group_info;
    group_info.tenant_id_ = TENANT_ID;
    group_info.refresh_id_ = refresh_id;
    group_info.target_data_sync_scn_ = target_scn;
    if (OB_FAIL(tasks.push_back(task))) {
    } else if (OB_FAIL(ObMViewPendingTaskGroup::alloc_and_build(
                   allocator, group_info, tasks, group))) {
    } else if (publish) {
      ret = manager_.queue_.restore_pending_task_group(*group);
    } else {
      ObLatchWGuard guard(manager_.queue_.rw_lock_, ObLatchIds::MVIEW_TASK_QUEUE_LOCK);
      ret = manager_.queue_.enqueue_pending_task_group(*group, false /*publish_group*/);
    }
    return ret;
  }

  share::ObTenantBase tenant_base_;
  ObMViewPendingTaskManager manager_;
  ObMViewCancelTestSQLClient sql_client_;
  MockMySQLProxy sql_proxy_;
  FakeMySQLResult empty_result_;
  ObMySQLProxy *old_sql_proxy_;
  int64_t proxy_read_count_;
  ObSqlString proxy_base_info_sql_;
  ObSqlString proxy_dep_info_sql_;
};

TEST_F(ObMViewPendingTaskManagerDropCancelTest,
       DropCancelsOnlyTargetAndPreservesRunningDependency)
{
  ASSERT_EQ(OB_SUCCESS, enqueue_nested_refresh());
  ASSERT_EQ(OB_SUCCESS,
            manager_.queue_.set_task_running(TENANT_ID,
                                             REFRESH_ID,
                                             DEPENDENCY_MVIEW_ID));

  obrpc::ObCancelMViewRefreshArg cancel_arg;
  cancel_arg.tenant_id_ = TENANT_ID;
  cancel_arg.mview_id_ = TARGET_MVIEW_ID;
  cancel_arg.is_cancel_by_mview_id_ = true;
  cancel_arg.is_drop_ = true;
  ASSERT_TRUE(cancel_arg.is_valid());
  bool refresh_exists = false;
  ASSERT_EQ(OB_SUCCESS, manager_.cancel_refresh_local(cancel_arg, refresh_exists));

  EXPECT_EQ(1, sql_client_.write_count_);
  EXPECT_NE(nullptr, strstr(sql_client_.last_write_sql_.ptr(), "mview_id = 1002"));
  EXPECT_EQ(nullptr, strstr(sql_client_.last_write_sql_.ptr(), "refresh_id"));
  EXPECT_EQ(1, sql_client_.read_count_);
  EXPECT_NE(nullptr, strstr(sql_client_.last_read_sql_.ptr(), "mview_id = 1002"));
  EXPECT_EQ(nullptr, strstr(sql_client_.last_read_sql_.ptr(), "refresh_id"));

  int64_t status = -1;
  ASSERT_EQ(OB_SUCCESS,
            manager_.queue_.get_task_status(TENANT_ID,
                                            REFRESH_ID,
                                            TARGET_MVIEW_ID,
                                            status));
  EXPECT_EQ(MV_TASK_CANCELLED, status);
  ASSERT_EQ(OB_SUCCESS,
            manager_.queue_.get_task_status(TENANT_ID,
                                            REFRESH_ID,
                                            DEPENDENCY_MVIEW_ID,
                                            status));
  EXPECT_EQ(MV_TASK_RUNNING, status);
  EXPECT_EQ(1, manager_.queue_.get_total_running_cnt());

  bool refresh_cancelled = true;
  ASSERT_EQ(OB_SUCCESS,
            manager_.queue_.is_refresh_cancelled(TENANT_ID, REFRESH_ID,
                                                  DEPENDENCY_MVIEW_ID, refresh_cancelled));
  EXPECT_FALSE(refresh_cancelled);

  ASSERT_EQ(OB_SUCCESS,
            manager_.mark_task_retry_wait(TENANT_ID, REFRESH_ID, DEPENDENCY_MVIEW_ID, OB_TIMEOUT));
  ASSERT_EQ(OB_SUCCESS,
            manager_.queue_.get_task_status(TENANT_ID, REFRESH_ID, DEPENDENCY_MVIEW_ID, status));
  EXPECT_EQ(MV_TASK_PENDING, status);
  int64_t retry_count = -1;
  ASSERT_EQ(OB_SUCCESS,
            manager_.queue_.get_task_retry_count(TENANT_ID, REFRESH_ID,
                                                  DEPENDENCY_MVIEW_ID, retry_count));
  EXPECT_EQ(1, retry_count);
  EXPECT_EQ(0, manager_.queue_.get_total_running_cnt());

  ASSERT_EQ(OB_SUCCESS,
            manager_.queue_.set_task_running(TENANT_ID, REFRESH_ID, DEPENDENCY_MVIEW_ID));
  bool refresh_finished = false;
  ASSERT_EQ(OB_SUCCESS,
            manager_.queue_.set_task_success(TENANT_ID,
                                             REFRESH_ID,
                                             DEPENDENCY_MVIEW_ID,
                                             refresh_finished));
  EXPECT_TRUE(refresh_finished);
  EXPECT_EQ(0, manager_.queue_.get_total_running_cnt());
}

TEST_F(ObMViewPendingTaskManagerDropCancelTest, DropRunningTargetDoesNotRetry)
{
  ASSERT_EQ(OB_SUCCESS, sql_proxy_.init(sql_client_.get_pool()));
  ASSERT_EQ(OB_SUCCESS, enqueue_nested_refresh());
  ASSERT_EQ(OB_SUCCESS,
            manager_.queue_.set_task_running(TENANT_ID, REFRESH_ID, TARGET_MVIEW_ID));

  obrpc::ObCancelMViewRefreshArg cancel_arg;
  cancel_arg.tenant_id_ = TENANT_ID;
  cancel_arg.mview_id_ = TARGET_MVIEW_ID;
  cancel_arg.is_cancel_by_mview_id_ = true;
  cancel_arg.is_drop_ = true;
  bool refresh_exists = false;
  ASSERT_EQ(OB_SUCCESS, manager_.cancel_refresh_local(cancel_arg, refresh_exists));

  bool refresh_cancelled = false;
  ASSERT_EQ(OB_SUCCESS,
            manager_.queue_.is_refresh_cancelled(TENANT_ID, REFRESH_ID,
                                                  TARGET_MVIEW_ID, refresh_cancelled));
  EXPECT_TRUE(refresh_cancelled);
  ASSERT_EQ(OB_SUCCESS,
            manager_.mark_task_retry_wait(TENANT_ID, REFRESH_ID, TARGET_MVIEW_ID, OB_TIMEOUT));

  int64_t status = -1;
  ASSERT_EQ(OB_SUCCESS,
            manager_.queue_.get_task_status(TENANT_ID, REFRESH_ID, TARGET_MVIEW_ID, status));
  EXPECT_EQ(MV_TASK_FAILED, status);
  int64_t retry_count = -1;
  ASSERT_EQ(OB_SUCCESS,
            manager_.queue_.get_task_retry_count(TENANT_ID, REFRESH_ID,
                                                  TARGET_MVIEW_ID, retry_count));
  EXPECT_EQ(0, retry_count);
  ASSERT_EQ(OB_SUCCESS,
            manager_.queue_.get_task_status(TENANT_ID, REFRESH_ID, DEPENDENCY_MVIEW_ID, status));
  EXPECT_EQ(MV_TASK_PENDING, status);
  EXPECT_EQ(0, manager_.queue_.get_total_running_cnt());
  EXPECT_TRUE(manager_.inspection_task_.retry_list_.empty());
}

TEST_F(ObMViewPendingTaskManagerDropCancelTest, InitialRefreshReusesActiveRefresh)
{
  const int64_t active_refresh_id = 123L;
  FakeRow row = {};
  row.refresh_id = active_refresh_id;
  ASSERT_EQ(OB_SUCCESS, sql_client_.load_rows(&row, 1));

  obrpc::ObScheduleMViewRefreshArg arg;
  arg.tenant_id_ = TENANT_ID;
  arg.run_user_id_ = OB_SYS_USER_ID;
  arg.mview_id_ = TARGET_MVIEW_ID;
  arg.is_initial_refresh_ = true;
  ASSERT_TRUE(arg.is_valid());

  obrpc::ObScheduleMViewRefreshResult result;
  ASSERT_EQ(OB_SUCCESS, manager_.schedule_mview_refresh_local(arg, result));
  EXPECT_EQ(active_refresh_id, result.refresh_id_);
  EXPECT_EQ(1, sql_client_.read_count_);
  EXPECT_EQ(0, sql_client_.write_count_);
  EXPECT_NE(nullptr, strstr(sql_client_.last_read_sql_.ptr(), "mview_id = 1002"));
  // An initial refresh may only be served by a COMPLETE or FORCE refresh already
  // in flight, and the newest one has to be picked first.
  EXPECT_NE(nullptr, strstr(sql_client_.last_read_sql_.ptr(), "refresh_method IN (1, 3)"));
  EXPECT_EQ(nullptr, strstr(sql_client_.last_read_sql_.ptr(), "target_data_sync_scn <"));
  EXPECT_NE(nullptr,
            strstr(sql_client_.last_read_sql_.ptr(), "ORDER BY target_data_sync_scn DESC"));
  EXPECT_EQ(0, manager_.queue_.get_total_running_cnt());
}

TEST_F(ObMViewPendingTaskManagerDropCancelTest, ForceCancelRecyclesPrecedingPublishedGroups)
{
  const int64_t REFRESH_OLD = 301L;
  const int64_t REFRESH_NEW = 302L;
  ASSERT_EQ(OB_SUCCESS, enqueue_refresh(REFRESH_OLD, TARGET_MVIEW_ID, 1, true));
  ASSERT_EQ(OB_SUCCESS, enqueue_refresh(REFRESH_NEW, TARGET_MVIEW_ID, 2, true));

  ASSERT_EQ(OB_SUCCESS,
            manager_.cancel_refresh_groups_for_force_refresh(TENANT_ID,
                                                             TARGET_MVIEW_ID,
                                                             REFRESH_NEW));

  int64_t old_status = -1;
  EXPECT_EQ(OB_ENTRY_NOT_EXIST,
            manager_.queue_.get_task_status(TENANT_ID,
                                            REFRESH_OLD,
                                            TARGET_MVIEW_ID,
                                            old_status));
  EXPECT_EQ(-1, old_status);
  EXPECT_NE(nullptr, strstr(sql_client_.last_write_sql_.ptr(), "refresh_id = 301"));

  int64_t new_status = -1;
  ASSERT_EQ(OB_SUCCESS,
            manager_.queue_.get_task_status(TENANT_ID,
                                            REFRESH_NEW,
                                            TARGET_MVIEW_ID,
                                            new_status));
  EXPECT_EQ(MV_TASK_PENDING, new_status);
}

TEST_F(ObMViewPendingTaskManagerDropCancelTest, LocalCancelMissingGroupReturnsFalseWithoutSql)
{
  obrpc::ObCancelMViewRefreshArg cancel_arg;
  cancel_arg.tenant_id_ = TENANT_ID;
  cancel_arg.refresh_id_ = REFRESH_ID;
  bool refresh_exists = true;

  ASSERT_EQ(OB_SUCCESS, manager_.cancel_refresh_local(cancel_arg, refresh_exists));
  EXPECT_FALSE(refresh_exists);
  EXPECT_EQ(0, sql_client_.write_count_);
  EXPECT_EQ(0, sql_client_.read_count_);
  EXPECT_EQ(0, proxy_read_count_);
}

TEST_F(ObMViewPendingTaskManagerDropCancelTest, LocalCancelUnpublishedGroupReturnsFalseWithoutSql)
{
  ASSERT_EQ(OB_SUCCESS, enqueue_refresh(REFRESH_ID, TARGET_MVIEW_ID, 1, false));
  obrpc::ObCancelMViewRefreshArg cancel_arg;
  cancel_arg.tenant_id_ = TENANT_ID;
  cancel_arg.refresh_id_ = REFRESH_ID;
  bool refresh_exists = true;

  ASSERT_EQ(OB_SUCCESS, manager_.cancel_refresh_local(cancel_arg, refresh_exists));
  EXPECT_FALSE(refresh_exists);
  EXPECT_EQ(0, sql_client_.write_count_);
  EXPECT_EQ(0, sql_client_.read_count_);
  EXPECT_EQ(0, proxy_read_count_);

  ObMViewPendingTaskGroup *group = NULL;
  const ObMViewRefreshKey key(TENANT_ID, REFRESH_ID);
  ASSERT_EQ(OB_SUCCESS, manager_.queue_.group_map_.get_refactored(key, group));
  ASSERT_TRUE(OB_NOT_NULL(group));
  EXPECT_FALSE(group->is_published());
  EXPECT_FALSE(group->cancelled_);
  EXPECT_EQ(MV_TASK_PENDING, group->tasks_.at(0).status_);
}

TEST_F(ObMViewPendingTaskManagerDropCancelTest, LocalCancelPublishedGroupReturnsTrueAndCancelsTasks)
{
  ASSERT_EQ(OB_SUCCESS, enqueue_nested_refresh());
  ASSERT_EQ(OB_SUCCESS,
            manager_.queue_.set_task_running(TENANT_ID, REFRESH_ID, DEPENDENCY_MVIEW_ID));
  obrpc::ObCancelMViewRefreshArg cancel_arg;
  cancel_arg.tenant_id_ = TENANT_ID;
  cancel_arg.refresh_id_ = REFRESH_ID;
  bool refresh_exists = false;

  ASSERT_EQ(OB_SUCCESS, manager_.cancel_refresh_local(cancel_arg, refresh_exists));
  EXPECT_TRUE(refresh_exists);
  EXPECT_EQ(1, sql_client_.write_count_);
  EXPECT_NE(nullptr, strstr(sql_client_.last_write_sql_.ptr(), "refresh_id = 100"));
  int64_t status = -1;
  ASSERT_EQ(OB_SUCCESS,
            manager_.queue_.get_task_status(TENANT_ID, REFRESH_ID, TARGET_MVIEW_ID, status));
  EXPECT_EQ(MV_TASK_CANCELLED, status);
}

TEST_F(ObMViewPendingTaskManagerDropCancelTest, CancelRefreshGroupSkipsUnpublishedBeforeSql)
{
  ASSERT_EQ(OB_SUCCESS, enqueue_refresh(REFRESH_ID, TARGET_MVIEW_ID, 1, false));

  bool refresh_exists = true;
  ASSERT_EQ(OB_SUCCESS, manager_.cancel_refresh_group(TENANT_ID, REFRESH_ID, refresh_exists));
  EXPECT_FALSE(refresh_exists);
  EXPECT_EQ(0, sql_client_.write_count_);
  EXPECT_EQ(0, sql_client_.read_count_);
  EXPECT_EQ(0, proxy_read_count_);

  ObMViewPendingTaskGroup *group = NULL;
  const ObMViewRefreshKey key(TENANT_ID, REFRESH_ID);
  ASSERT_EQ(OB_SUCCESS, manager_.queue_.group_map_.get_refactored(key, group));
  ASSERT_TRUE(OB_NOT_NULL(group));
  EXPECT_FALSE(group->is_published());
  EXPECT_FALSE(group->cancelled_);
  EXPECT_EQ(MV_TASK_PENDING, group->tasks_.at(0).status_);
  EXPECT_EQ(OB_SUCCESS, manager_.queue_.publish_pending_task_group(TENANT_ID, REFRESH_ID));
}

TEST_F(ObMViewPendingTaskManagerDropCancelTest, ForceCancelMarksUnpublishedReservationCanceled)
{
  const int64_t REFRESH_OLD = 311L;
  const int64_t REFRESH_NEW = 312L;
  ASSERT_EQ(OB_SUCCESS, enqueue_refresh(REFRESH_OLD, TARGET_MVIEW_ID, 1, false));
  ASSERT_EQ(OB_SUCCESS, enqueue_refresh(REFRESH_NEW, TARGET_MVIEW_ID, 2, true));

  ASSERT_EQ(OB_SUCCESS,
            manager_.cancel_refresh_groups_for_force_refresh(TENANT_ID,
                                                             TARGET_MVIEW_ID,
                                                             REFRESH_NEW));

  ObMViewPendingTaskGroup *group_old = NULL;
  const ObMViewRefreshKey old_key(TENANT_ID, REFRESH_OLD);
  ASSERT_EQ(OB_SUCCESS, manager_.queue_.group_map_.get_refactored(old_key, group_old));
  ASSERT_TRUE(OB_NOT_NULL(group_old));
  EXPECT_FALSE(group_old->is_published());
  EXPECT_TRUE(group_old->cancelled_);
  EXPECT_EQ(MV_TASK_CANCELLED, group_old->tasks_.at(0).status_);

  int64_t old_status = -1;
  EXPECT_EQ(OB_EAGAIN,
            manager_.queue_.get_task_status(TENANT_ID,
                                            REFRESH_OLD,
                                            TARGET_MVIEW_ID,
                                            old_status));

  ASSERT_EQ(OB_CANCELED,
            manager_.queue_.publish_pending_task_group(TENANT_ID, REFRESH_OLD));
  EXPECT_FALSE(group_old->is_published());

  int64_t new_status = -1;
  ASSERT_EQ(OB_SUCCESS,
            manager_.queue_.get_task_status(TENANT_ID,
                                            REFRESH_NEW,
                                            TARGET_MVIEW_ID,
                                            new_status));
  EXPECT_EQ(MV_TASK_PENDING, new_status);
}

// ACTIVATE scoped to a database loads every mview of that database through the
// sql proxy (base info + dep info) instead of touching a single mview.
TEST_F(ObMViewPendingTaskManagerDropCancelTest, DatabaseActivateLoadsMviewsOfDatabase)
{
  obrpc::ObNotifyMViewTimerArg arg;
  arg.tenant_id_ = TENANT_ID;
  arg.database_id_ = 2001UL;
  arg.op_type_ = obrpc::ObNotifyMViewTimerArg::ACTIVATE;
  ASSERT_TRUE(arg.is_valid());

  EXPECT_EQ(OB_SUCCESS, manager_.notify_mview_timer_local(arg));
  ASSERT_EQ(2, proxy_read_count_);
  ASSERT_NE(nullptr, proxy_base_info_sql_.ptr());
  ASSERT_NE(nullptr, proxy_dep_info_sql_.ptr());
  EXPECT_NE(nullptr, strstr(proxy_base_info_sql_.ptr(), " WHERE t.database_id = 2001"));
  EXPECT_EQ(nullptr, strstr(proxy_base_info_sql_.ptr(), "WHERE a.mview_id"));
  EXPECT_NE(nullptr, strstr(proxy_dep_info_sql_.ptr(), "mv.database_id = 2001"));
  EXPECT_EQ(nullptr, strstr(proxy_dep_info_sql_.ptr(), "WHERE dep.mview_id"));
  EXPECT_EQ(0, sql_client_.read_count_);
  EXPECT_EQ(0, sql_client_.write_count_);
  EXPECT_EQ(0, manager_.sched_info_mgr_.info_map_.size());
  EXPECT_EQ(0, manager_.scheduler_timer_.heap_.get_heap_data().count());
}

TEST_F(ObMViewPendingTaskManagerDropCancelTest, MviewActivateDoesNotUseDatabaseFilter)
{
  ASSERT_EQ(OB_SUCCESS, add_sched_info(TARGET_MVIEW_ID, NULL, 0));
  obrpc::ObNotifyMViewTimerArg arg;
  arg.tenant_id_ = TENANT_ID;
  arg.mview_id_ = TARGET_MVIEW_ID;
  arg.op_type_ = obrpc::ObNotifyMViewTimerArg::ACTIVATE;
  ASSERT_TRUE(arg.is_valid());

  EXPECT_EQ(OB_SUCCESS, manager_.notify_mview_timer_local(arg));
  EXPECT_EQ(0, proxy_read_count_);
  ASSERT_NE(nullptr, manager_.sched_info_mgr_.get_sched_group_info(TARGET_MVIEW_ID));
  EXPECT_EQ(1, manager_.scheduler_timer_.heap_.get_heap_data().count());
}

TEST_F(ObMViewPendingTaskManagerDropCancelTest, NotifyTimerRejectsNullSqlProxy)
{
  GCTX.sql_proxy_ = NULL;
  obrpc::ObNotifyMViewTimerArg arg;
  arg.tenant_id_ = TENANT_ID;
  arg.mview_id_ = TARGET_MVIEW_ID;
  arg.op_type_ = obrpc::ObNotifyMViewTimerArg::ACTIVATE;
  ASSERT_TRUE(arg.is_valid());

  EXPECT_EQ(OB_ERR_UNEXPECTED, manager_.notify_mview_timer_local(arg));
  arg.mview_id_ = OB_INVALID_ID;
  arg.database_id_ = 2001UL;
  ASSERT_TRUE(arg.is_valid());
  EXPECT_EQ(OB_ERR_UNEXPECTED, manager_.notify_mview_timer_local(arg));
  EXPECT_EQ(0, proxy_read_count_);
  GCTX.sql_proxy_ = &sql_proxy_;
}

// The timer heap only accepts keys of the currently published group incarnation:
// a stale modified_ts_ is silently skipped and an unknown group is rejected.
TEST_F(ObMViewPendingTaskManagerDropCancelTest, PushTimerHeapSkipsStaleAndUnknownGroupKeys)
{
  ASSERT_EQ(OB_SUCCESS, add_sched_info(TARGET_MVIEW_ID, NULL, 0));
  ObSEArray<uint64_t, 1> top_mview_ids;
  ObSEArray<uint64_t, 1> all_mview_ids;
  ASSERT_EQ(OB_SUCCESS, top_mview_ids.push_back(TARGET_MVIEW_ID));
  ASSERT_EQ(OB_SUCCESS, all_mview_ids.push_back(TARGET_MVIEW_ID));
  ASSERT_EQ(OB_SUCCESS, publish_group(TARGET_MVIEW_ID, top_mview_ids, all_mview_ids));
  const ObMViewSchedGroupInfo *group_info =
      manager_.sched_info_mgr_.get_sched_group_info(TARGET_MVIEW_ID);
  ASSERT_NE(nullptr, group_info);
  const ObIArray<ObMViewTimerHeapNode> &heap_data =
      manager_.scheduler_timer_.heap_.get_heap_data();

  ObSEArray<ObMVGroupKey, 1> stale_groups;
  ASSERT_EQ(OB_SUCCESS,
            stale_groups.push_back(ObMVGroupKey(TARGET_MVIEW_ID, group_info->modified_ts_ - 1)));
  ASSERT_EQ(OB_SUCCESS, manager_.scheduler_timer_.push_timer_heap(stale_groups));
  EXPECT_EQ(0, heap_data.count());

  ObSEArray<ObMVGroupKey, 1> unknown_groups;
  ASSERT_EQ(OB_SUCCESS, unknown_groups.push_back(ObMVGroupKey(9999UL, group_info->modified_ts_)));
  EXPECT_EQ(OB_ENTRY_NOT_EXIST, manager_.scheduler_timer_.push_timer_heap(unknown_groups));
  EXPECT_EQ(0, heap_data.count());

  ObSEArray<ObMVGroupKey, 1> current_groups;
  ASSERT_EQ(OB_SUCCESS,
            current_groups.push_back(ObMVGroupKey(TARGET_MVIEW_ID, group_info->modified_ts_)));
  ASSERT_EQ(OB_SUCCESS, manager_.scheduler_timer_.push_timer_heap(current_groups));
  ASSERT_EQ(1, heap_data.count());
  EXPECT_EQ(TARGET_MVIEW_ID, heap_data.at(0).group_id_);
  EXPECT_EQ(group_info->modified_ts_, heap_data.at(0).modified_ts_);
  EXPECT_NE(OB_INVALID_TIMESTAMP, heap_data.at(0).next_trigger_ts_);
}

// A timer-driven refresh carries the trigger timestamp it was due at, and that
// timestamp becomes the new last start time so the schedule grid does not drift.
// Refreshes without a trigger timestamp fall back to the target data sync scn.
TEST_F(ObMViewPendingTaskManagerDropCancelTest, MarkRefreshStartedPrefersTriggerTsOverSyncScn)
{
  ASSERT_EQ(OB_SUCCESS, add_sched_info(DEPENDENCY_MVIEW_ID, NULL, 0));
  ASSERT_EQ(OB_SUCCESS, add_sched_info(TARGET_MVIEW_ID, NULL, 0));
  static const uint64_t UNSCHEDULED_MVIEW_ID = 1003UL;
  ASSERT_EQ(OB_SUCCESS, add_sched_info(UNSCHEDULED_MVIEW_ID, NULL, 0));
  ObSEArray<uint64_t, 4> sched_mview_ids;
  ASSERT_EQ(OB_SUCCESS, sched_mview_ids.push_back(DEPENDENCY_MVIEW_ID));
  ASSERT_EQ(OB_SUCCESS, sched_mview_ids.push_back(9999UL));
  ASSERT_EQ(OB_SUCCESS, sched_mview_ids.push_back(TARGET_MVIEW_ID));
  ObMViewPendingTaskGroup::GroupInfo group_info;
  group_info.target_data_sync_scn_ = 9876000UL;
  ObScheduleMViewTaskParam param;
  ASSERT_EQ(OB_INVALID_TIMESTAMP, param.trigger_ts_);

  manager_.mark_mview_refresh_started(sched_mview_ids, param, group_info);
  EXPECT_EQ(9876, manager_.sched_info_mgr_.get_sched_info(DEPENDENCY_MVIEW_ID)->last_start_time_);
  EXPECT_EQ(9876, manager_.sched_info_mgr_.get_sched_info(TARGET_MVIEW_ID)->last_start_time_);
  EXPECT_EQ(0, manager_.sched_info_mgr_.get_sched_info(UNSCHEDULED_MVIEW_ID)->last_start_time_);

  param.trigger_ts_ = 12345;
  manager_.mark_mview_refresh_started(sched_mview_ids, param, group_info);
  EXPECT_EQ(12345, manager_.sched_info_mgr_.get_sched_info(DEPENDENCY_MVIEW_ID)->last_start_time_);
  EXPECT_EQ(12345, manager_.sched_info_mgr_.get_sched_info(TARGET_MVIEW_ID)->last_start_time_);
  EXPECT_EQ(0, manager_.sched_info_mgr_.get_sched_info(UNSCHEDULED_MVIEW_ID)->last_start_time_);
}

TEST_F(ObMViewPendingTaskManagerDropCancelTest, NotifyActivatePublishesMergedGroupTimer)
{
  ASSERT_EQ(OB_SUCCESS, add_sched_info(DEPENDENCY_MVIEW_ID, NULL, 0));
  ASSERT_EQ(OB_SUCCESS, add_sched_info(TARGET_MVIEW_ID, NULL, 0));
  ObSEArray<uint64_t, 1> first_top_ids;
  ObSEArray<uint64_t, 1> first_all_ids;
  ObSEArray<uint64_t, 1> second_top_ids;
  ObSEArray<uint64_t, 1> second_all_ids;
  ASSERT_EQ(OB_SUCCESS, first_top_ids.push_back(DEPENDENCY_MVIEW_ID));
  ASSERT_EQ(OB_SUCCESS, first_all_ids.push_back(DEPENDENCY_MVIEW_ID));
  ASSERT_EQ(OB_SUCCESS, second_top_ids.push_back(TARGET_MVIEW_ID));
  ASSERT_EQ(OB_SUCCESS, second_all_ids.push_back(TARGET_MVIEW_ID));
  ASSERT_EQ(OB_SUCCESS,
            publish_group(DEPENDENCY_MVIEW_ID, first_top_ids, first_all_ids));
  ASSERT_EQ(OB_SUCCESS,
            publish_group(TARGET_MVIEW_ID, second_top_ids, second_all_ids));
  static const uint64_t MERGED_MVIEW_ID = 1003UL;
  const uint64_t dep_mview_ids[] = {DEPENDENCY_MVIEW_ID, TARGET_MVIEW_ID};
  ASSERT_EQ(OB_SUCCESS, add_sched_info(MERGED_MVIEW_ID, dep_mview_ids, 2));
  obrpc::ObNotifyMViewTimerArg arg;
  arg.tenant_id_ = TENANT_ID;
  arg.mview_id_ = MERGED_MVIEW_ID;
  arg.op_type_ = obrpc::ObNotifyMViewTimerArg::ACTIVATE;
  ASSERT_TRUE(arg.is_valid());

  ASSERT_EQ(OB_SUCCESS, manager_.notify_mview_timer_local(arg));
  EXPECT_EQ(MERGED_MVIEW_ID,
            manager_.sched_info_mgr_.get_sched_info(DEPENDENCY_MVIEW_ID)->group_id_);
  EXPECT_EQ(MERGED_MVIEW_ID,
            manager_.sched_info_mgr_.get_sched_info(TARGET_MVIEW_ID)->group_id_);
  EXPECT_EQ(MERGED_MVIEW_ID,
            manager_.sched_info_mgr_.get_sched_info(MERGED_MVIEW_ID)->group_id_);
  EXPECT_EQ(nullptr,
            manager_.sched_info_mgr_.get_sched_group_info(DEPENDENCY_MVIEW_ID));
  EXPECT_EQ(nullptr,
            manager_.sched_info_mgr_.get_sched_group_info(TARGET_MVIEW_ID));
  const ObMViewSchedGroupInfo *merged_group =
      manager_.sched_info_mgr_.get_sched_group_info(MERGED_MVIEW_ID);
  ASSERT_NE(nullptr, merged_group);
  const ObIArray<ObMViewTimerHeapNode> &heap_data =
      manager_.scheduler_timer_.heap_.get_heap_data();
  ASSERT_EQ(1, heap_data.count());
  EXPECT_EQ(MERGED_MVIEW_ID, heap_data.at(0).group_id_);
  EXPECT_EQ(merged_group->modified_ts_, heap_data.at(0).modified_ts_);
}

TEST_F(ObMViewPendingTaskManagerDropCancelTest, NotifyReschedulePushesRepublishedGroupTimer)
{
  ASSERT_EQ(OB_SUCCESS, add_sched_info(TARGET_MVIEW_ID, NULL, 0));
  ObSEArray<uint64_t, 1> top_mview_ids;
  ObSEArray<uint64_t, 1> all_mview_ids;
  ASSERT_EQ(OB_SUCCESS, top_mview_ids.push_back(TARGET_MVIEW_ID));
  ASSERT_EQ(OB_SUCCESS, all_mview_ids.push_back(TARGET_MVIEW_ID));
  ASSERT_EQ(OB_SUCCESS, publish_group(TARGET_MVIEW_ID, top_mview_ids, all_mview_ids));
  const int64_t old_modified_ts =
      manager_.sched_info_mgr_.get_sched_group_info(TARGET_MVIEW_ID)->modified_ts_;
  obrpc::ObNotifyMViewTimerArg arg;
  arg.tenant_id_ = TENANT_ID;
  arg.mview_id_ = TARGET_MVIEW_ID;
  arg.op_type_ = obrpc::ObNotifyMViewTimerArg::RESCHEDULE;
  ASSERT_TRUE(arg.is_valid());

  ASSERT_EQ(OB_SUCCESS, manager_.notify_mview_timer_local(arg));
  const ObMViewSchedGroupInfo *group_info =
      manager_.sched_info_mgr_.get_sched_group_info(TARGET_MVIEW_ID);
  ASSERT_NE(nullptr, group_info);
  EXPECT_LE(old_modified_ts, group_info->modified_ts_);
  const ObIArray<ObMViewTimerHeapNode> &heap_data =
      manager_.scheduler_timer_.heap_.get_heap_data();
  ASSERT_EQ(1, heap_data.count());
  EXPECT_EQ(TARGET_MVIEW_ID, heap_data.at(0).group_id_);
  EXPECT_EQ(group_info->modified_ts_, heap_data.at(0).modified_ts_);
}

TEST_F(ObMViewPendingTaskManagerDropCancelTest, NotifyUnregisterSplitPushesEveryGroupTimer)
{
  ASSERT_EQ(OB_SUCCESS, add_sched_info(DEPENDENCY_MVIEW_ID, NULL, 0));
  ASSERT_EQ(OB_SUCCESS, add_sched_info(TARGET_MVIEW_ID, NULL, 0));
  static const uint64_t TOP_MVIEW_ID = 1003UL;
  const uint64_t dep_mview_ids[] = {DEPENDENCY_MVIEW_ID, TARGET_MVIEW_ID};
  ASSERT_EQ(OB_SUCCESS, add_sched_info(TOP_MVIEW_ID, dep_mview_ids, 2));
  ObSEArray<uint64_t, 1> top_mview_ids;
  ObSEArray<uint64_t, 3> all_mview_ids;
  ASSERT_EQ(OB_SUCCESS, top_mview_ids.push_back(TOP_MVIEW_ID));
  ASSERT_EQ(OB_SUCCESS, all_mview_ids.push_back(DEPENDENCY_MVIEW_ID));
  ASSERT_EQ(OB_SUCCESS, all_mview_ids.push_back(TARGET_MVIEW_ID));
  ASSERT_EQ(OB_SUCCESS, all_mview_ids.push_back(TOP_MVIEW_ID));
  ASSERT_EQ(OB_SUCCESS, publish_group(TOP_MVIEW_ID, top_mview_ids, all_mview_ids));
  obrpc::ObNotifyMViewTimerArg arg;
  arg.tenant_id_ = TENANT_ID;
  arg.mview_id_ = TOP_MVIEW_ID;
  arg.op_type_ = obrpc::ObNotifyMViewTimerArg::UNREGISTER;
  ASSERT_TRUE(arg.is_valid());

  ASSERT_EQ(OB_SUCCESS, manager_.notify_mview_timer_local(arg));
  EXPECT_EQ(nullptr, manager_.sched_info_mgr_.get_sched_info(TOP_MVIEW_ID));
  EXPECT_EQ(nullptr, manager_.sched_info_mgr_.get_sched_group_info(TOP_MVIEW_ID));
  const ObIArray<ObMViewTimerHeapNode> &heap_data =
      manager_.scheduler_timer_.heap_.get_heap_data();
  ASSERT_EQ(2, heap_data.count());
  bool has_dependency_group = false;
  bool has_target_group = false;
  for (int64_t i = 0; i < heap_data.count(); ++i) {
    has_dependency_group = has_dependency_group
                           || DEPENDENCY_MVIEW_ID == heap_data.at(i).group_id_;
    has_target_group = has_target_group || TARGET_MVIEW_ID == heap_data.at(i).group_id_;
  }
  EXPECT_TRUE(has_dependency_group);
  EXPECT_TRUE(has_target_group);
}

// ===========================================================================
// Recovery error handling: entries must survive transient internal-table
// failures, and memory must be aligned to disk before any entry leaves the
// recovery list.
// ===========================================================================

TEST(ObMViewPendingTaskInspectionTaskTest, TransientRecoveryErrorClassification)
{
  EXPECT_TRUE(ObMViewPendingTaskInspectionTask::is_transient_recovery_error(OB_TRANS_TIMEOUT));
  EXPECT_TRUE(ObMViewPendingTaskInspectionTask::is_transient_recovery_error(OB_NOT_MASTER));
  EXPECT_FALSE(ObMViewPendingTaskInspectionTask::is_transient_recovery_error(OB_EAGAIN));
}

class ObMViewRecoveryErrorHandleTest : public ::testing::Test
{
public:
  static constexpr uint64_t TENANT_ID = OB_SYS_TENANT_ID;
  static constexpr int64_t REFRESH_ID = 100L;
  static constexpr uint64_t TARGET_MVIEW_ID = 1002UL;

  ObMViewRecoveryErrorHandleTest()
    : tenant_base_(TENANT_ID), manager_(), inspection_task_(), sql_client_()
  {
  }

  void SetUp() override
  {
    share::ObTenantEnv::set_tenant(&tenant_base_);
    ASSERT_EQ(OB_SUCCESS, manager_.sched_info_mgr_.init(TENANT_ID, 64L));
    ASSERT_EQ(OB_SUCCESS, manager_.queue_.init(TENANT_ID));
    ASSERT_EQ(OB_SUCCESS, manager_.table_operator_.init(&sql_client_));
    ASSERT_EQ(OB_SUCCESS, manager_.reload_cond_.init(ObWaitEventIds::THREAD_IDLING_COND_WAIT));
    ASSERT_EQ(OB_SUCCESS,
              manager_.mview_context_map_.create(64L, "MVCtxTest", "MVCtxTest", TENANT_ID));
    ASSERT_EQ(OB_SUCCESS,
              manager_.mview_ctx_alloc_.init(lib::ObMallocAllocator::get_instance(),
                                             OB_MALLOC_NORMAL_BLOCK_SIZE,
                                             ObMemAttr(TENANT_ID, "MVCtxTest")));
    ATOMIC_STORE(&manager_.reload_state_,
                 static_cast<int64_t>(ObMViewPendingTaskManager::RS_READY));
    ASSERT_EQ(OB_SUCCESS,
              manager_.result_alloc_.init(lib::ObMallocAllocator::get_instance(),
                                          OB_MALLOC_NORMAL_BLOCK_SIZE,
                                          ObMemAttr(TENANT_ID, "MVResTest")));
    manager_.is_inited_ = true;
    ASSERT_EQ(OB_SUCCESS, inspection_task_.init(manager_, TENANT_ID));
    ASSERT_EQ(OB_SUCCESS, manager_.inspection_task_.init(manager_, TENANT_ID));
  }

  void TearDown() override
  {
    inspection_task_.destroy();
    manager_.destroy();
    share::ObTenantEnv::set_tenant(NULL);
  }

  int enqueue_single_running_task()
  {
    int ret = OB_SUCCESS;
    ObArenaAllocator allocator;
    ObSEArray<ObMViewPendingTask, 1> tasks;
    ObMViewPendingTaskGroup *group = NULL;
    ObMViewPendingTask target_task;
    target_task.mview_id_ = TARGET_MVIEW_ID;
    ObMViewPendingTaskGroup::GroupInfo group_info;
    group_info.tenant_id_ = TENANT_ID;
    group_info.refresh_id_ = REFRESH_ID;
    group_info.target_data_sync_scn_ = 1;
    if (OB_FAIL(tasks.push_back(target_task))) {
    } else if (OB_FAIL(ObMViewPendingTaskGroup::alloc_and_build(
                   allocator, group_info, tasks, group))) {
    } else if (OB_FAIL(manager_.queue_.restore_pending_task_group(*group))) {
    } else if (OB_FAIL(manager_.queue_.set_task_running(TENANT_ID,
                                                        REFRESH_ID,
                                                        TARGET_MVIEW_ID))) {
    }
    return ret;
  }

  share::ObTenantBase tenant_base_;
  ObMViewPendingTaskManager manager_;
  ObMViewPendingTaskInspectionTask inspection_task_;
  ObMViewCancelTestSQLClient sql_client_;
};

TEST_F(ObMViewRecoveryErrorHandleTest, DeterministicErrorRunsFallbackAndCleansMemory)
{
  ASSERT_EQ(OB_SUCCESS, enqueue_single_running_task());
  ASSERT_EQ(1, manager_.queue_.get_total_running_cnt());

  // 单测环境无 schema service，recover_single_entry 必报 OB_ERR_UNEXPECTED
  //（确定性错误）：走 finalize 兜底把任务标记失败，内存 RUNNING 计数清零，
  // entry 正常离开恢复列表。
  ASSERT_EQ(OB_SUCCESS,
            inspection_task_.register_for_recovery(TENANT_ID, REFRESH_ID,
                                                   TARGET_MVIEW_ID, 1));
  ASSERT_EQ(OB_SUCCESS, inspection_task_.process_recovery_list());

  EXPECT_EQ(0, inspection_task_.recovery_list_.count());
  EXPECT_LT(0, sql_client_.write_count_);
  EXPECT_EQ(0, manager_.queue_.get_total_running_cnt());
}

TEST_F(ObMViewRecoveryErrorHandleTest, TransientFallbackFailureKeepsEntryForNextRound)
{
  ASSERT_EQ(OB_SUCCESS, enqueue_single_running_task());
  ASSERT_EQ(1, manager_.queue_.get_total_running_cnt());

  // 恢复失败（确定性）进入兜底，但兜底写内部表也超时（如磁盘满）：
  // entry 必须保留到下轮巡检，丢弃会留下孤儿 RUNNING 占住并发计数。
  sql_client_.force_write_error(OB_TIMEOUT);
  ASSERT_EQ(OB_SUCCESS,
            inspection_task_.register_for_recovery(TENANT_ID, REFRESH_ID,
                                                   TARGET_MVIEW_ID, 1));
  ASSERT_EQ(OB_SUCCESS, inspection_task_.process_recovery_list());

  EXPECT_EQ(1, inspection_task_.recovery_list_.count());
  EXPECT_EQ(1, manager_.queue_.get_total_running_cnt());

  // 内部表写恢复后，下一轮兜底成功：计数清零、entry 正常离列。
  sql_client_.force_write_error(OB_SUCCESS);
  ASSERT_EQ(OB_SUCCESS, inspection_task_.process_recovery_list());
  EXPECT_EQ(0, inspection_task_.recovery_list_.count());
  EXPECT_EQ(0, manager_.queue_.get_total_running_cnt());
}

TEST_F(ObMViewRecoveryErrorHandleTest, ResultProcessFailureRegistersRecoveryThenHeals)
{
  ASSERT_EQ(OB_SUCCESS, enqueue_single_running_task());
  ASSERT_EQ(1, manager_.queue_.get_total_running_cnt());

  // Internal-table writes keep timing out: the returned task result cannot be
  // processed and must be handed over to the recovery list instead of being
  // dropped (dropping leaves an orphaned RUNNING task with a dead session).
  sql_client_.force_write_error(OB_TIMEOUT);
  ASSERT_EQ(OB_SUCCESS,
            manager_.push_task_result(TENANT_ID, REFRESH_ID, TARGET_MVIEW_ID,
                                      1 /*target_data_sync_scn*/, OB_TIMEOUT));
  ASSERT_EQ(OB_SUCCESS, manager_.process_task_results(10));
  EXPECT_EQ(1, manager_.inspection_task_.recovery_list_.count());
  EXPECT_EQ(1, manager_.queue_.get_total_running_cnt());

  // Writes recover: the next inspection round drains the recovery entry and
  // aligns memory to disk (row gone -> CANCELLED -> recycle).
  sql_client_.force_write_error(OB_SUCCESS);
  ASSERT_EQ(OB_SUCCESS, manager_.inspection_task_.process_recovery_list());
  EXPECT_EQ(0, manager_.inspection_task_.recovery_list_.count());
  EXPECT_EQ(0, manager_.queue_.get_total_running_cnt());
}

TEST_F(ObMViewRecoveryErrorHandleTest, NonReadyManagerDoesNotExposeRunningJobs)
{
  ASSERT_EQ(OB_SUCCESS, enqueue_single_running_task());
  int64_t callback_count = 0;
  auto count_job = [&callback_count](const ObMViewPendingRunningJobInfo &) {
    ++callback_count;
    return OB_SUCCESS;
  };

  EXPECT_EQ(OB_SUCCESS, manager_.foreach_running_job(count_job));
  EXPECT_EQ(1, callback_count);
  EXPECT_EQ(1, sql_client_.read_count_);

  callback_count = 0;
  ATOMIC_STORE(&manager_.reload_state_,
               static_cast<int64_t>(ObMViewPendingTaskManager::RS_NOT_READY));
  EXPECT_EQ(OB_SUCCESS, manager_.foreach_running_job(count_job));
  EXPECT_EQ(0, callback_count);
  EXPECT_EQ(1, sql_client_.read_count_);
  EXPECT_EQ(1, manager_.queue_.get_total_running_cnt());

  ATOMIC_STORE(&manager_.reload_state_,
               static_cast<int64_t>(ObMViewPendingTaskManager::RS_RELOADING));
  EXPECT_EQ(OB_SUCCESS, manager_.foreach_running_job(count_job));
  EXPECT_EQ(0, callback_count);
  EXPECT_EQ(1, sql_client_.read_count_);

  ATOMIC_STORE(&manager_.reload_state_,
               static_cast<int64_t>(ObMViewPendingTaskManager::RS_READY));
  EXPECT_EQ(OB_SUCCESS, manager_.foreach_running_job(count_job));
  EXPECT_EQ(1, callback_count);
  EXPECT_EQ(2, sql_client_.read_count_);
}

int main(int argc, char **argv)
{
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
