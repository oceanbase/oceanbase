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
#ifndef USING_LOG_PREFIX
#define USING_LOG_PREFIX STORAGETEST
#endif
#include <gtest/gtest.h>

#define protected public
#define private public
#include "lib/utility/ob_test_util.h"
#include "storage/shared_storage/storage_cache_policy/ob_storage_cache_service.h"
#include "mittest/mtlenv/mock_tenant_module_env.h"
#include "mittest/shared_storage/clean_residual_data.h"
#include "mittest/simple_server/env/ob_simple_cluster_test_base.h"
#include "storage/shared_storage/prewarm/ob_storage_cache_policy_prewarmer.h"
#include "storage/shared_storage/storage_cache_policy/ob_storage_cache_tablet_scheduler.h"

using namespace oceanbase::transaction;
using namespace oceanbase::storage;

namespace oceanbase
{
char *shared_storage_info = nullptr;
namespace unittest
{

struct TestRunCtx
{
  uint64_t tenant_id_ = 1;
  int64_t tenant_epoch_ = 0;
  ObLSID ls_id_;
  int64_t ls_epoch_;
  ObTabletID tablet_id_;

  TO_STRING_KV(K(tenant_id_), K(tenant_epoch_), K(ls_id_), K(ls_epoch_), K(tablet_id_));
};

class ObStorageCachePolicyPrewarmerTest : public ObSimpleClusterTestBase
{
public:
  ObStorageCachePolicyPrewarmerTest()
      : ObSimpleClusterTestBase("test_storage_cache_policy_prewarmer_", "50G", "50G", "50G"),
        tenant_created_(false),
        run_ctx_()
  {}

  virtual void SetUp() override
  {
    ObSimpleClusterTestBase::SetUp();
    if (!tenant_created_) {
      OK(create_tenant("tt1", "5G", "10G", false/*oracle_mode*/, 8));
      OK(get_tenant_id(run_ctx_.tenant_id_));
      ASSERT_NE(0, run_ctx_.tenant_id_);
      OK(get_curr_simple_server().init_sql_proxy2());
      tenant_created_ = true;
    }
  }

  static void TearDownTestCase()
  {
    ResidualDataCleanerHelper::clean_in_mock_env();
    ObSimpleClusterTestBase::TearDownTestCase();
  }

  int exe_sql(const char *sql_str)
  {
    int ret = OB_SUCCESS;
    ObSqlString sql;
    int64_t affected_rows = 0;
    if (OB_ISNULL(sql_str)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("sql str is null", KR(ret), KP(sql_str));
    } else if (OB_FAIL(sql.assign(sql_str))) {
      LOG_WARN("fail to assign sql", KR(ret), K(sql_str));
    } else if (OB_FAIL(get_curr_simple_server().get_sql_proxy2().write(sql.ptr(), affected_rows))) {
      LOG_WARN("fail to write sql", KR(ret), K(sql_str), K(sql));
    }
    return ret;
  }

  int sys_exe_sql(const char *sql_str)
  {
    int ret = OB_SUCCESS;
    ObSqlString sql;
    int64_t affected_rows = 0;
    if (OB_ISNULL(sql_str)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("sql str is null", KR(ret), KP(sql_str));
    } else if (OB_FAIL(sql.assign(sql_str))) {
      LOG_WARN("fail to assign sql", KR(ret), K(sql_str));
    } else if (OB_FAIL(get_curr_simple_server().get_sql_proxy().write(sql.ptr(), affected_rows))) {
      LOG_WARN("fail to write sql", KR(ret), K(sql_str), K(sql));
    }
    return ret;
  }

  void wait_major_finish();
  void set_ls_and_tablet_id_for_run_ctx();
  void add_task(SCPTabletTaskMap &tablet_tasks, ObStorageCacheTaskStatusType status, int64_t end_time, const int64_t tablet_id);


private:
  bool tenant_created_;
  TestRunCtx run_ctx_;
};

void ObStorageCachePolicyPrewarmerTest::wait_major_finish()
{
  int ret = OB_SUCCESS;
  LOG_INFO("wait major begin");
  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy();

  ObSqlString sql;
  int64_t affected_rows = 0;
  static int64_t old_major_scn = 1;
  int64_t new_major_scn = 1;
  int64_t scn = 0;
  do {
    OK(sql.assign_fmt(
        "select frozen_scn, (frozen_scn - last_scn) as result"
        " from oceanbase.CDB_OB_MAJOR_COMPACTION where tenant_id=%lu;",
        run_ctx_.tenant_id_));
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      OK(sql_proxy.read(res, sql.ptr()));
      sqlclient::ObMySQLResult *result = res.get_result();
      ASSERT_NE(nullptr, result);
      OK(result->next());
      OK(result->get_int("result", scn));
      OK(result->get_int("frozen_scn", new_major_scn));
    }
    LOG_INFO("major result", K(scn), K(new_major_scn));
    ob_usleep(100 * 1000); // 100_ms
  } while (0 != scn || old_major_scn == new_major_scn);
  
  old_major_scn = new_major_scn;
  LOG_INFO("major finished", K(new_major_scn));
}

void ObStorageCachePolicyPrewarmerTest::set_ls_and_tablet_id_for_run_ctx()
{
  int ret = OB_SUCCESS;
  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();

  ObSqlString sql;
  int64_t affected_rows = 0;
  uint64_t tablet_id = 0;
  OK(sql.assign("select tablet_id from oceanbase.__all_virtual_table where table_name='test_table';"));
  SMART_VAR(ObMySQLProxy::MySQLResult, res1) {
    ASSERT_EQ(OB_SUCCESS, sql_proxy.read(res1, sql.ptr()));
    sqlclient::ObMySQLResult *result = res1.get_result();
    ASSERT_NE(nullptr, result);
    OK(result->next());
    OK(result->get_uint("tablet_id", tablet_id));
  }
  run_ctx_.tablet_id_ = tablet_id;

  sql.reset();
  int64_t id = 0;
  OK(sql.assign_fmt("select ls_id from oceanbase.__all_tablet_to_ls where tablet_id=%ld;", tablet_id));
  SMART_VAR(ObMySQLProxy::MySQLResult, res2) {
    ASSERT_EQ(OB_SUCCESS, sql_proxy.read(res2, sql.ptr()));
    sqlclient::ObMySQLResult *result = res2.get_result();
    ASSERT_NE(nullptr, result);
    OK(result->next());
    OK(result->get_int("ls_id", id));
  }
  ObLSID ls_id(id);

  ObLSHandle ls_handle;
  OK(MTL(ObLSService*)->get_ls(ls_id, ls_handle, ObLSGetMod::TXSTORAGE_MOD));
  ObLS *ls = ls_handle.get_ls();
  ASSERT_NE(nullptr, ls);
  run_ctx_.ls_id_ = ls->get_ls_id();
  run_ctx_.ls_epoch_ = ls->get_ls_epoch();
  run_ctx_.tenant_epoch_ = MTL_EPOCH_ID();
  LOG_INFO("finish set run ctx", K(run_ctx_));
}

void ObStorageCachePolicyPrewarmerTest::add_task(SCPTabletTaskMap &tablet_tasks, ObStorageCacheTaskStatusType status, int64_t end_time, const int64_t tablet_id)
{
  ObStorageCacheTabletTask *task = nullptr;
  task = OB_NEW(ObStorageCacheTabletTask, "ObStorageCache");
  ASSERT_NE(nullptr, task);
  ObStorageCacheTabletTaskHandle handle;
  ASSERT_EQ(OB_SUCCESS, task->init(run_ctx_.tenant_id_, 2, tablet_id, PolicyStatus::HOT));
  task->status_ = status;

  task->set_end_time(end_time);
  handle.set_ptr(task);
  ASSERT_EQ(OB_SUCCESS, tablet_tasks.set_refactored(task->get_tablet_id(), handle));
}

TEST_F(ObStorageCachePolicyPrewarmerTest, basic)
{
  share::ObTenantSwitchGuard tguard;
  OK(tguard.switch_to(run_ctx_.tenant_id_));
  OK(sys_exe_sql("alter system set ob_compaction_schedule_interval = '3s' tenant tt1;"));
  OK(exe_sql("create table test_table (a int)"));
  set_ls_and_tablet_id_for_run_ctx();

  OK(exe_sql("insert into test_table values (1)"));
  sleep(1);
  OK(sys_exe_sql("alter system major freeze tenant tt1;"));
  wait_major_finish();

  ObSSMicroCache *micro_cache = MTL(ObSSMicroCache *);
  ASSERT_NE(nullptr, micro_cache);

  ObStorageCacheTabletTask *task = static_cast<ObStorageCacheTabletTask *>(ob_malloc(
      sizeof(ObStorageCacheTabletTask),
      ObMemAttr(run_ctx_.tenant_id_, "ObStorageCache")));
  ASSERT_NE(nullptr, task);
  new (task) ObStorageCacheTabletTask();
  PolicyStatus policy_status;
  OK(task->init(
      run_ctx_.tenant_id_,
      run_ctx_.ls_id_.id(),
      run_ctx_.tablet_id_.id(),
      policy_status));
  task->inc_ref_count();

  // read init major
  ObStorageCachePolicyPrewarmer prewarmer;
  TabletMajorPrewarmStat first_stat;
  const int64_t MAX_RETRY_TIMES = 3;
  bool succeed = false;

  task->status_ = ObStorageCacheTaskStatus::OB_STORAGE_CACHE_TASK_DOING;
  for (int i = 0; i < MAX_RETRY_TIMES && !succeed; i++) {
    micro_cache->clear_micro_cache();
    OK(prewarmer.prewarm_hot_tablet(run_ctx_.ls_id_, run_ctx_.tablet_id_, task));
    first_stat = prewarmer.get_tablet_major_prewarm_stat();
    LOG_INFO("read init major", K(i), K(first_stat));
    succeed = first_stat.macro_block_fail_cnt_ == 0
        && first_stat.micro_block_fail_cnt_ == 0
        && first_stat.micro_block_hit_cnt_ == 0
        && first_stat.micro_block_num_ == first_stat.micro_block_add_cnt_;
  }
  ASSERT_TRUE(succeed);

  // read init major again, micro blocks should all be hit
  succeed = false;
  TabletMajorPrewarmStat second_stat;
  task->status_ = ObStorageCacheTaskStatus::OB_STORAGE_CACHE_TASK_DOING;
  for (int i = 0; i < MAX_RETRY_TIMES && !succeed; i++) {
    OK(prewarmer.prewarm_hot_tablet(run_ctx_.ls_id_, run_ctx_.tablet_id_, task));
    second_stat = prewarmer.get_tablet_major_prewarm_stat();
    LOG_INFO("read init major again", K(i), K(second_stat));
    succeed = second_stat.macro_block_fail_cnt_ == 0
        && second_stat.micro_block_fail_cnt_ == 0
        && second_stat.micro_block_hit_cnt_ == first_stat.micro_block_add_cnt_
        && second_stat.micro_block_add_cnt_ == 0;
  }
  ASSERT_TRUE(succeed);

  // generate a new version of tablet
  // micro blocks num should remain unchanged
  OK(exe_sql("insert into test_table values (2)"));
  sleep(1);
  OK(sys_exe_sql("alter system major freeze tenant tt1;"));
  wait_major_finish();

  // read second version major
  succeed = false;
  TabletMajorPrewarmStat third_stat;
  task->status_ = ObStorageCacheTaskStatus::OB_STORAGE_CACHE_TASK_DOING;
  for (int i = 0; i < MAX_RETRY_TIMES && !succeed; i++) {
    OK(prewarmer.prewarm_hot_tablet(run_ctx_.ls_id_, run_ctx_.tablet_id_, task));
    third_stat = prewarmer.get_tablet_major_prewarm_stat();
    LOG_INFO("read second major", K(i), K(third_stat));
    succeed = third_stat.macro_block_fail_cnt_ == 0
        && third_stat.micro_block_fail_cnt_ == 0
        && third_stat.micro_block_add_cnt_ + third_stat.micro_block_hit_cnt_ == third_stat.micro_block_num_;
  }
  ASSERT_TRUE(succeed);

  task->dec_ref_count();
  LOG_INFO("Finish basic");
  
  // test clean history
  ObStorageCachePolicyService *policy_service = MTL(ObStorageCachePolicyService *);
  ASSERT_NE(nullptr, policy_service);
  SCPTabletTaskMap &tablet_tasks = policy_service->get_tablet_tasks();
  ASSERT_EQ(OB_SUCCESS, tablet_tasks.clear());
  ASSERT_EQ(0, tablet_tasks.size());
  TG_CANCEL_TASK(policy_service->refresh_policy_scheduler_.tg_id_, policy_service->refresh_policy_scheduler_.clean_history_task_);
  TG_WAIT_TASK(policy_service->refresh_policy_scheduler_.tg_id_, policy_service->refresh_policy_scheduler_.clean_history_task_);
  TG_SCHEDULE(policy_service->refresh_policy_scheduler_.tg_id_, policy_service->refresh_policy_scheduler_.clean_history_task_,
      2 * 1000 * 1000, true);
  for (int i = 0; i < 1100; ++i) {
    // add tasks to be cleaned
    const int64_t tablet_id = i + 200001;
    if (i < 200) {
      add_task(tablet_tasks, ObStorageCacheTaskStatus::OB_STORAGE_CACHE_TASK_FINISHED, ObTimeUtility::current_time() - 2 * 3600 * 1000LL * 1000LL, tablet_id);
    } else if (i < 400) {
      add_task(tablet_tasks, ObStorageCacheTaskStatus::OB_STORAGE_CACHE_TASK_FAILED, ObTimeUtility::current_time() - 12 * 3600 * 1000LL * 1000LL, tablet_id);
    } else if (i < 600) {
      add_task(tablet_tasks, ObStorageCacheTaskStatus::OB_STORAGE_CACHE_TASK_CANCELED, ObTimeUtility::current_time() - 12 * 3600 * 1000LL * 1000LL, tablet_id);
    } else if (i < 800) {
      add_task(tablet_tasks, ObStorageCacheTaskStatus::OB_STORAGE_CACHE_TASK_SUSPENDED, ObTimeUtility::current_time() - 12 * 3600 * 1000LL * 1000LL, tablet_id);
    } else {
      add_task(tablet_tasks, ObStorageCacheTaskStatus::OB_STORAGE_CACHE_TASK_DOING, ObTimeUtility::current_time(), tablet_id);
    }
  }
  sleep(5);
  ASSERT_EQ(500, tablet_tasks.size()); // OB_STORAGE_CACHE_TASK_DOING + OB_STORAGE_CACHE_TASK_SUSPENDED
}

} // end unittest
} // end oceanbase
int main(int argc, char **argv)
{
  char buf[1000] = {0};
  const int64_t cur_time_ns = ObTimeUtility::current_time_ns();
  databuff_printf(buf, sizeof(buf),
      "%s/%lu?host=%s&access_id=%s&access_key=%s&s3_region=%s&max_iops=10000&max_bandwidth=200000000B&scope=region",
      oceanbase::unittest::S3_BUCKET, cur_time_ns, oceanbase::unittest::S3_ENDPOINT,
      oceanbase::unittest::S3_AK, oceanbase::unittest::S3_SK, oceanbase::unittest::S3_REGION);
  oceanbase::shared_storage_info = buf;
  oceanbase::unittest::init_log_and_gtest(argc, argv);
  OB_LOGGER.set_log_level("INFO");
  GCONF.ob_startup_mode.set_value("shared_storage");
  GCONF.datafile_size.set_value("100G");
  GCONF.memory_limit.set_value("20G");
  GCONF.system_memory.set_value("5G");

  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
