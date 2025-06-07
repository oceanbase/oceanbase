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
#include "mittest/shared_storage/test_ss_common_util.h"
#include "storage/shared_storage/prewarm/ob_storage_cache_policy_prewarmer.h"
#include "storage/shared_storage/storage_cache_policy/ob_storage_cache_tablet_scheduler.h"


using namespace oceanbase::transaction;
using namespace oceanbase::storage;

namespace oceanbase
{
char *shared_storage_info = nullptr;
namespace unittest
{
bool scp_tenant_created = false;

struct TestRunCtx
{
  uint64_t tenant_id_ = 1;
  int64_t tenant_epoch_ = 0;
  ObLSID ls_id_;
  int64_t ls_epoch_;
  ObTabletID tablet_id_;

  TO_STRING_KV(K(tenant_id_), K(tenant_epoch_), K(ls_id_), K(ls_epoch_), K(tablet_id_));
};

TestRunCtx run_ctx_;


class ObStorageCachePolicyPrewarmerTest : public ObSimpleClusterTestBase
{
public:
  ObStorageCachePolicyPrewarmerTest()
      : ObSimpleClusterTestBase("test_storage_cache_policy_prewarmer_", "50G", "50G", "50G")
  {}
  virtual void SetUp() override
  {
    if (!scp_tenant_created) {
      ObSimpleClusterTestBase::SetUp();
      OK(create_tenant_with_retry("tt1", "5G", "10G", false/*oracle_mode*/, 8));
      OK(get_tenant_id(run_ctx_.tenant_id_));
      ASSERT_NE(0, run_ctx_.tenant_id_);
      OK(get_curr_simple_server().init_sql_proxy2());
      scp_tenant_created = true;
    }
  }

  static void TearDownTestCase()
  {
    ResidualDataCleanerHelper::clean_in_mock_env();
    ObSimpleClusterTestBase::TearDownTestCase();
  }

  int exe_sql(const char *sql_str);
  int sys_exe_sql(const char *sql_str);
  int medium_compact(const int64_t tablet_id);
  void wait_major_finish();
  void wait_minor_finish();
  void wait_ss_minor_compaction_finish();
  void wait_task_finished(int64_t tablet_id);
  void set_ls_and_tablet_id_for_run_ctx(const char* table_name);
  void add_task(SCPTabletTaskMap &tablet_tasks, ObStorageCacheTaskStatusType status, int64_t end_time, const int64_t tablet_id);
  void check_macro_cache_exist();
  void exe_prepare_sql();
  void check_macro_blocks_type(const ObSSMacroCacheType expected_type);

};

int ObStorageCachePolicyPrewarmerTest::exe_sql(const char *sql_str)
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

int ObStorageCachePolicyPrewarmerTest::sys_exe_sql(const char *sql_str)
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

int ObStorageCachePolicyPrewarmerTest::medium_compact(const int64_t tablet_id)
{
  int ret = OB_SUCCESS;
  int64_t last_major_snapshot = 0;
  int64_t finish_count = 0;
  ObSqlString sql1;
  ObSqlString sql2;
  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();

  if(OB_FAIL(sql1.assign_fmt("select ifnull(max(end_log_scn),0) as mx from oceanbase.__all_virtual_table_mgr"
                     " where tenant_id = %lu and tablet_id = %lu and ls_id = %lu"
                     " and table_type = 10;",run_ctx_.tenant_id_, run_ctx_.tablet_id_.id(), run_ctx_.ls_id_.id()))) {
    LOG_WARN("[TEST] fail to select last_major_snapshot", KR(ret), K(tablet_id), K(sql1), K(run_ctx_));
  } else {
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      if(OB_FAIL(sql_proxy.read(res, sql1.ptr()))) {
        LOG_WARN("[TEST] fail to read sql result", KR(ret), K(sql1));
      } else {
        sqlclient::ObMySQLResult *result = res.get_result();
        if (OB_ISNULL(result)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("[TEST] fail to get result", KR(ret), K(sql1));
        } else if (OB_FAIL(result->next())) {
          LOG_WARN("[TEST] fail to get next result", KR(ret), K(sql1));
        } else if (OB_FAIL(result->get_int("mx", last_major_snapshot))) {
          LOG_WARN("[TEST] fail to get int result", KR(ret), K(sql1));
        }
      }
    }
  }
  FLOG_INFO("[TEST] start medium compact", K(run_ctx_), K(sql1));
  sleep(10);
  std::string medium_compact_str = "alter system major freeze tenant tt1 tablet_id=";
  medium_compact_str += std::to_string(tablet_id);
  if(OB_FAIL(sys_exe_sql(medium_compact_str.c_str()))) {
    LOG_WARN("[TEST] fail to exe sys sql", KR(ret), K(medium_compact_str.c_str()));
  }
  FLOG_INFO("[TEST] wait medium compact finished", K(run_ctx_), K(medium_compact_str.c_str()));

  do {
    if(OB_FAIL(sql2.assign_fmt("select count(*) as count from oceanbase.__all_virtual_table_mgr"
                    " where tenant_id = %lu and tablet_id = %lu and ls_id = %lu"
                    " and table_type = 10 and end_log_scn > %lu;", run_ctx_.tenant_id_, run_ctx_.tablet_id_.id(),
                    run_ctx_.ls_id_.id(), last_major_snapshot))) {
      LOG_WARN("[TEST] fail to select last_major_snapshot", KR(ret), K(tablet_id), K(sql1), K(run_ctx_), K(last_major_snapshot));
    } else {
      SMART_VAR(ObMySQLProxy::MySQLResult, res2) {
        if(OB_FAIL(sql_proxy.read(res2, sql2.ptr()))) {
          LOG_WARN("[TEST] fail to read sql result", KR(ret), K(sql2));
        } else {
          sqlclient::ObMySQLResult *result = res2.get_result();
          if (OB_ISNULL(result)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("[TEST] fail to get result", KR(ret), K(sql2));
          } else if (OB_FAIL(result->next())) {
            LOG_WARN("[TEST] fail to get next result", KR(ret), K(sql2));
          } else if (OB_FAIL(result->get_int("count", finish_count))) {
            LOG_WARN("[TEST] fail to get int result", KR(ret), K(sql2));
          }
        }
      }
    }
  } while (finish_count == 0);
  FLOG_INFO("[TEST] medium compact finished", K(run_ctx_), K(sql2));
  return ret;
}
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

void ObStorageCachePolicyPrewarmerTest::wait_minor_finish()
{
  int ret = OB_SUCCESS;
  LOG_INFO("wait minor begin");
  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();

  ObSqlString sql;
  int64_t row_cnt = 0;
  do {
    ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("select count(*) as row_cnt from oceanbase.__all_virtual_table_mgr where tenant_id=%lu and tablet_id=%lu and table_type=0;",
              run_ctx_.tenant_id_, run_ctx_.tablet_id_.id()));
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      ASSERT_EQ(OB_SUCCESS, sql_proxy.read(res, sql.ptr()));
      sqlclient::ObMySQLResult *result = res.get_result();
      ASSERT_NE(nullptr, result);
      ASSERT_EQ(OB_SUCCESS, result->next());
      ASSERT_EQ(OB_SUCCESS, result->get_int("row_cnt", row_cnt));
    }
    usleep(100 * 1000);
    LOG_INFO("minor result", K(row_cnt));
  } while (row_cnt > 0);
  LOG_INFO("minor finished", K(row_cnt));
}

void ObStorageCachePolicyPrewarmerTest::wait_ss_minor_compaction_finish()
{
  int ret = OB_SUCCESS;
  LOG_INFO("wait ss minor compaction begin");
  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();

  ObSqlString sql;
  int64_t row_cnt = 0;
  do {
    ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("select count(*) as row_cnt from oceanbase.__all_virtual_table_mgr where tenant_id=%lu and tablet_id=%lu and table_type=12;",
              run_ctx_.tenant_id_, run_ctx_.tablet_id_.id()));
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      ASSERT_EQ(OB_SUCCESS, sql_proxy.read(res, sql.ptr()));
      sqlclient::ObMySQLResult *result = res.get_result();
      ASSERT_NE(nullptr, result);
      ASSERT_EQ(OB_SUCCESS, result->next());
      ASSERT_EQ(OB_SUCCESS, result->get_int("row_cnt", row_cnt));
    }
    usleep(100 * 1000);
    LOG_INFO("minor result", K(row_cnt));
  } while (row_cnt > 0);

  row_cnt = 0;
  do {
    ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("select count(*) as row_cnt from oceanbase.__all_virtual_table_mgr where tenant_id=%lu and tablet_id=%lu and table_type=11 and table_flag = 1;",
              run_ctx_.tenant_id_, run_ctx_.tablet_id_.id()));
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      ASSERT_EQ(OB_SUCCESS, sql_proxy.read(res, sql.ptr()));
      sqlclient::ObMySQLResult *result = res.get_result();
      ASSERT_NE(nullptr, result);
      ASSERT_EQ(OB_SUCCESS, result->next());
      ASSERT_EQ(OB_SUCCESS, result->get_int("row_cnt", row_cnt));
    }
    usleep(100 * 1000);
    LOG_INFO("minor result", K(row_cnt));
  } while (row_cnt <= 0);
  LOG_INFO("wait ss minor compaction finished");
}

void ObStorageCachePolicyPrewarmerTest::wait_task_finished(int64_t tablet_id)
{
  int ret = OB_SUCCESS;
  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy();

  ObSqlString sql;
  int64_t count = 0;
  FLOG_INFO("[TEST] start to wait task finished", K(run_ctx_), K(tablet_id));
  do {
    OK(sql.assign_fmt(
        "select count(*) as count from oceanbase.__all_virtual_storage_cache_task where tablet_id=%lu and status = 'FINISHED';",
        run_ctx_.tablet_id_.id()));
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      OK(sql_proxy.read(res, sql.ptr()));
      sqlclient::ObMySQLResult *result = res.get_result();
      ASSERT_NE(nullptr, result);
      OK(result->next());
      OK(result->get_int("count", count));
    }
    ob_usleep(100 * 1000); // 100_ms
  } while (0 == count);
  FLOG_INFO("[TEST] storage cache task finished", K(tablet_id), K(run_ctx_));
}

void ObStorageCachePolicyPrewarmerTest::set_ls_and_tablet_id_for_run_ctx(const char* table_name)
{
  int ret = OB_SUCCESS;
  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();

  ObSqlString sql;
  int64_t affected_rows = 0;
  uint64_t tablet_id = 0;
  OK(sql.assign_fmt("select tablet_id from oceanbase.__all_virtual_table where table_name='%s';", table_name));
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
  LOG_INFO("[TEST] finish set run ctx", K(run_ctx_));
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

void ObStorageCachePolicyPrewarmerTest::check_macro_cache_exist()
{
  ObSEArray<MacroBlockId, 64> data_block_ids;
  ObSEArray<MacroBlockId, 16> meta_block_ids;
  FLOG_INFO("[TEST] start to check macro cache", K(run_ctx_));
  data_block_ids.set_attr(ObMemAttr(run_ctx_.tenant_id_, "DataBlockIDs"));
  meta_block_ids.set_attr(ObMemAttr(run_ctx_.tenant_id_, "MetaBlockIDs"));

  OK(get_macro_blocks(run_ctx_.ls_id_, run_ctx_.tablet_id_, data_block_ids, meta_block_ids));
  FLOG_INFO("[TEST] current info", K(run_ctx_), K(data_block_ids), K(meta_block_ids));

  ObSSMacroCacheMgr *ss_cache_mgr = MTL(ObSSMacroCacheMgr*);
  ASSERT_NE(nullptr, ss_cache_mgr);
  bool is_exist = false;
  int64_t retry_times = 0;
  wait_task_finished(run_ctx_.tablet_id_.id());
  for (int i = 0; i < data_block_ids.count(); i++) {
    is_exist = false;
    FLOG_INFO("[TEST] macro id", K(data_block_ids.at(i)), K(i));
    if (data_block_ids.at(i).is_data()) {
      OK(ss_cache_mgr->exist(data_block_ids.at(i), is_exist));
      if (!is_exist) {
        FLOG_INFO("macro is not exist", K(i), K(run_ctx_), K(data_block_ids.at(i)));
      }
      ASSERT_TRUE(is_exist);
    }
  }
  LOG_INFO("[TEST] finish to check macro cache exist", K(run_ctx_));
}

void ObStorageCachePolicyPrewarmerTest::exe_prepare_sql()
{
  OK(exe_sql("alter system set _ss_schedule_upload_interval = '1s';"));
  OK(exe_sql("alter system set inc_sstable_upload_thread_score = 20;"));
  OK(exe_sql("alter system set _ss_garbage_collect_interval = '10s';"));
  OK(exe_sql("alter system set _ss_garbage_collect_file_expiration_time = '10s';"));
  OK(sys_exe_sql("alter system set ob_compaction_schedule_interval = '3s' tenant tt1;"));
  OK(sys_exe_sql("alter system set merger_check_interval = '10s' tenant tt1;"));
  OK(sys_exe_sql("alter system set minor_compact_trigger = 2 tenant tt1;"));
  OK(sys_exe_sql("alter system set _ss_major_compaction_prewarm_level = 0 tenant tt1;"));
  FLOG_INFO("set _ss_major_compaction_prewarm_level = 0");
}

void ObStorageCachePolicyPrewarmerTest::check_macro_blocks_type(const ObSSMacroCacheType expected_type)
{
  ObSEArray<MacroBlockId, 128> data_block_ids;
  ObSEArray<MacroBlockId, 128> meta_block_ids;
  data_block_ids.set_attr(ObMemAttr(run_ctx_.tenant_id_, "DataBlockIDs"));
  meta_block_ids.set_attr(ObMemAttr(run_ctx_.tenant_id_, "BlockIDs"));
  OK(get_macro_blocks(run_ctx_.ls_id_, run_ctx_.tablet_id_, data_block_ids, meta_block_ids));
  FLOG_INFO("[TEST] start to check macro blocks type", K(run_ctx_), K(expected_type), K(data_block_ids), K(meta_block_ids));
  bool is_exist = false;
  ObSSMacroCacheMgr *ss_cache_mgr = MTL(ObSSMacroCacheMgr*);
  ASSERT_NE(nullptr, ss_cache_mgr);
  ObSSMacroCacheMetaHandle meta_handle;
  for (int i = 0; i < data_block_ids.count(); i++) {
    is_exist = false;
    meta_handle.reset();
    OK(ss_cache_mgr->get_macro_cache_meta(data_block_ids.at(i), is_exist, meta_handle));
    if (is_exist) {
      if (expected_type != meta_handle()->cache_type_) {
        FLOG_INFO("[TEST] data macro id", K(data_block_ids.at(i)), K(i), K(expected_type));
      }
      ASSERT_EQ(expected_type, meta_handle()->cache_type_);
    }
  }
  FLOG_INFO("[TEST] finish check all macro blocks type", K(run_ctx_), K(expected_type));
}

TEST_F(ObStorageCachePolicyPrewarmerTest, test_clean_task_history)
{
  // test clean history
  share::ObTenantSwitchGuard tguard;
  OK(tguard.switch_to(run_ctx_.tenant_id_));

  ObStorageCachePolicyService *policy_service = MTL(ObStorageCachePolicyService *);
  ASSERT_NE(nullptr, policy_service);
  SCPTabletTaskMap &tablet_tasks = policy_service->get_tablet_tasks();
  ASSERT_EQ(OB_SUCCESS, tablet_tasks.clear());
  ASSERT_EQ(0, tablet_tasks.size());
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
  ASSERT_EQ(1100, tablet_tasks.size());
  TG_CANCEL_TASK(policy_service->refresh_policy_scheduler_.tg_id_, policy_service->refresh_policy_scheduler_.clean_history_task_);
  TG_WAIT_TASK(policy_service->refresh_policy_scheduler_.tg_id_, policy_service->refresh_policy_scheduler_.clean_history_task_);
  TG_SCHEDULE(policy_service->refresh_policy_scheduler_.tg_id_, policy_service->refresh_policy_scheduler_.clean_history_task_,
      2 * 1000 * 1000, true);
  sleep(10);
  ASSERT_EQ(500, tablet_tasks.size()); // OB_STORAGE_CACHE_TASK_DOING + OB_STORAGE_CACHE_TASK_SUSPENDED
  FLOG_INFO("[TEST] test_clean_task_history end");
}

TEST_F(ObStorageCachePolicyPrewarmerTest, basic)
{
  share::ObTenantSwitchGuard tguard;
  OK(tguard.switch_to(run_ctx_.tenant_id_));
  exe_prepare_sql();

  ObStorageCachePolicyPrewarmer prewarmer;
  ObSSMicroCache *micro_cache = MTL(ObSSMicroCache *);
  ASSERT_NE(nullptr, micro_cache);

  OK(exe_sql("create table test_table (a int)"));
  set_ls_and_tablet_id_for_run_ctx("test_table");

  OK(exe_sql("insert into test_table values (1)"));
  sleep(1);
  OK(medium_compact(run_ctx_.tablet_id_.id()));

  // 1.test basic hot retention prewarm
  ObStorageCacheTabletTask *task = static_cast<ObStorageCacheTabletTask *>(ob_malloc(
      sizeof(ObStorageCacheTabletTask),
      ObMemAttr(run_ctx_.tenant_id_, "ObStorageCache")));
  ASSERT_NE(nullptr, task);
  new (task) ObStorageCacheTabletTask();
  PolicyStatus policy_status = PolicyStatus::HOT;
  OK(task->init(
      run_ctx_.tenant_id_,
      run_ctx_.ls_id_.id(),
      run_ctx_.tablet_id_.id(),
      policy_status));
  task->inc_ref_count();
  // read init major
  ObSSPrewarmStat first_stat;
  const int64_t MAX_RETRY_TIMES = 3;
  bool succeed = false;

  task->status_ = ObStorageCacheTaskStatus::OB_STORAGE_CACHE_TASK_DOING;
  for (int i = 0; i < MAX_RETRY_TIMES && !succeed; i++) {
    micro_cache->clear_micro_cache();
    OK(prewarmer.prewarm_hot_tablet(task));
    first_stat = prewarmer.get_hot_retention_prewarm_stat();
    LOG_INFO("read init major", K(i), K(first_stat));
    ASSERT_TRUE(first_stat.macro_block_fail_cnt_== 0);
    ASSERT_TRUE(first_stat.micro_block_fail_cnt_ == 0);
    // ASSERT_TRUE(first_stat.macro_block_hit_cnt_ == 0);
    ASSERT_TRUE(first_stat.micro_block_hit_cnt_ == 0);
    ASSERT_TRUE(first_stat.macro_block_add_cnt_ == first_stat.macro_data_block_num_);
    ASSERT_TRUE(first_stat.micro_block_add_cnt_ == first_stat.micro_block_num_);
    succeed = first_stat.macro_block_fail_cnt_ == 0
        && first_stat.micro_block_fail_cnt_ == 0
        // && first_stat.macro_block_hit_cnt_ == 0
        && first_stat.micro_block_hit_cnt_ == 0
        && first_stat.macro_block_add_cnt_ == first_stat.macro_data_block_num_
        && first_stat.micro_block_add_cnt_ == first_stat.micro_block_num_;
  }
  ASSERT_TRUE(succeed);

  task->dec_ref_count();
  LOG_INFO("[TEST] basic end");
}

TEST_F(ObStorageCachePolicyPrewarmerTest, test_shared_major_for_macro_cache)
{
  share::ObTenantSwitchGuard tguard;
  OK(tguard.switch_to(run_ctx_.tenant_id_));
  exe_prepare_sql();
  FLOG_INFO("[TEST] start test_shared_major_for_macro_cache ");
  ObSSMacroCacheMgr *macro_cache_mgr = MTL(ObSSMacroCacheMgr *);
  ASSERT_NE(nullptr, macro_cache_mgr);

  ObSSMicroCache *micro_cache = MTL(ObSSMicroCache *);
  ASSERT_NE(nullptr, micro_cache);

  ObTenantDiskSpaceManager *tnt_disk_space_mgr = MTL(ObTenantDiskSpaceManager*);
  ASSERT_NE(tnt_disk_space_mgr, nullptr);
  // 2.test prewarm shared major macro blocks into local cache
  OK(exe_sql("create table test_macro_cache (a int)"));
  set_ls_and_tablet_id_for_run_ctx("test_macro_cache");

  OK(exe_sql("insert into test_macro_cache values (1)"));
  sleep(1);
  OK(medium_compact(run_ctx_.tablet_id_.id()));
  OK(exe_sql("alter table test_macro_cache storage_cache_policy (global = 'hot');"));
  FLOG_INFO("[TEST] finish alter storage cache policy");
  check_macro_cache_exist();
  FLOG_INFO("[TEST] finish test_shared_major_for_macro_cache");
}

TEST_F(ObStorageCachePolicyPrewarmerTest, test_shared_increase_for_macro_cache)
{
  share::ObTenantSwitchGuard tguard;
  OK(tguard.switch_to(run_ctx_.tenant_id_));
  exe_prepare_sql();
  FLOG_INFO("[TEST] start test_shared_increase_for_macro_cache");

  ObSSMacroCacheMgr *macro_cache_mgr = MTL(ObSSMacroCacheMgr *);
  ASSERT_NE(nullptr, macro_cache_mgr);

  ObSSMicroCache *micro_cache = MTL(ObSSMicroCache *);
  ASSERT_NE(nullptr, micro_cache);

  ObTenantDiskSpaceManager *tnt_disk_space_mgr = MTL(ObTenantDiskSpaceManager*);
  ASSERT_NE(tnt_disk_space_mgr, nullptr);

   // 3. test prewarm shared minor macro blocks into local cache
  // trigger minor compaction by minor freeze 3 times.
  // Check whether the hot_retention prewarm SHARED_MINI_DATA_MACRO/SHARED_MINOR_DATA_MACRO/SHARED_MDS_MINI_DATA_MACRO/SHARED_MDS_MINOR_DATA_MACRO
  // into macro cache
  OK(exe_sql("create table test_shared_increase (a int)"));
  set_ls_and_tablet_id_for_run_ctx("test_shared_increase");
  OK(exe_sql("insert into test_shared_increase values (2);"));
  sleep(1);
  OK(exe_sql("alter system minor freeze;"));
  wait_minor_finish();

  OK(exe_sql("insert into test_shared_increase values (3);"));
  sleep(1);
  OK(exe_sql("alter system minor freeze;"));
  wait_minor_finish();

  OK(exe_sql("insert into test_shared_increase values (4);"));
  sleep(1);
  OK(exe_sql("alter system minor freeze;"));
  wait_minor_finish();

  wait_ss_minor_compaction_finish();
  OK(exe_sql("alter table test_shared_increase storage_cache_policy (global = 'auto');"));
  OK(exe_sql("alter table test_shared_increase storage_cache_policy (global = 'hot');"));
  check_macro_cache_exist();
  LOG_INFO("[TEST] finish to test shared minor");

  // 4. test private macro blocks in macro cache
  OK(exe_sql("insert into test_shared_increase values (5)"));
  sleep(1);
  OK(exe_sql("alter system minor freeze;"));
  wait_minor_finish();
  check_macro_cache_exist();
  FLOG_INFO("[TEST] finish test_shared_increase_for_macro_cache");
}

TEST_F(ObStorageCachePolicyPrewarmerTest, test_macro_cache_space_occupy)
{
  share::ObTenantSwitchGuard tguard;
  OK(tguard.switch_to(run_ctx_.tenant_id_));
  exe_prepare_sql();
  FLOG_INFO("[TEST] start test_macro_cache_space_occupy");

  ObSSMacroCacheMgr *macro_cache_mgr = MTL(ObSSMacroCacheMgr *);
  ASSERT_NE(nullptr, macro_cache_mgr);

  ObSSMicroCache *micro_cache = MTL(ObSSMicroCache *);
  ASSERT_NE(nullptr, micro_cache);

  ObTenantDiskSpaceManager *tnt_disk_space_mgr = MTL(ObTenantDiskSpaceManager*);
  ASSERT_NE(tnt_disk_space_mgr, nullptr);
  OK(exe_sql("create table test_space_occupy (a int)"));
  set_ls_and_tablet_id_for_run_ctx("test_space_occupy");

  // 6. test whether occupy space happens when hot tablet macro block space is not enough
  const int64_t macro_free_size = tnt_disk_space_mgr->get_macro_cache_free_size();
  OK(tnt_disk_space_mgr->alloc_file_size((macro_free_size), ObSSMacroCacheType::MACRO_BLOCK));


  ObSSMacroCacheStat macro_cache_stat1;
  ASSERT_EQ(OB_SUCCESS, tnt_disk_space_mgr->get_macro_cache_stat(ObSSMacroCacheType::MACRO_BLOCK, macro_cache_stat1));
  ObSSMacroCacheStat hot_macro_cache_stat1;
  ASSERT_EQ(OB_SUCCESS, tnt_disk_space_mgr->get_macro_cache_stat(ObSSMacroCacheType::HOT_TABLET_MACRO_BLOCK, hot_macro_cache_stat1));

  OK(exe_sql("insert into test_space_occupy values (7)"));
  OK(exe_sql("insert into test_space_occupy values (8)"));
  OK(exe_sql("insert into test_space_occupy values (9)"));

  OK(medium_compact(run_ctx_.tablet_id_.id()));
  OK(exe_sql("alter table test_space_occupy storage_cache_policy (global = 'auto');"));
  OK(exe_sql("alter table test_space_occupy storage_cache_policy (global = 'hot');"));
  check_macro_cache_exist();

  ObSSMacroCacheStat macro_cache_stat2;
  ASSERT_EQ(OB_SUCCESS, tnt_disk_space_mgr->get_macro_cache_stat(ObSSMacroCacheType::MACRO_BLOCK, macro_cache_stat2));
  ObSSMacroCacheStat hot_macro_cache_stat2;
  ASSERT_EQ(OB_SUCCESS, tnt_disk_space_mgr->get_macro_cache_stat(ObSSMacroCacheType::HOT_TABLET_MACRO_BLOCK, hot_macro_cache_stat2));

  LOG_INFO("[TEST] compare hot_tablet_space with macro_cache_space", K(hot_macro_cache_stat1.used_), K(macro_cache_stat1.used_), K(hot_macro_cache_stat2.used_), K(macro_cache_stat2.used_));
  ASSERT_TRUE(hot_macro_cache_stat1.used_ < hot_macro_cache_stat2.used_);
  ASSERT_TRUE(macro_cache_stat1.used_ >  macro_cache_stat2.used_);

  ASSERT_EQ(OB_SUCCESS, tnt_disk_space_mgr->free_file_size(macro_free_size, ObSSMacroCacheType::MACRO_BLOCK));
  FLOG_INFO("[TEST] test_macro_cache_space_occupy end", K(tnt_disk_space_mgr->get_macro_cache_free_size()));
}

TEST_F(ObStorageCachePolicyPrewarmerTest, test_macro_cache_full)
{
  share::ObTenantSwitchGuard tguard;
  OK(tguard.switch_to(run_ctx_.tenant_id_));
  exe_prepare_sql();

  ObSSMacroCacheMgr *macro_cache_mgr = MTL(ObSSMacroCacheMgr *);
  ASSERT_NE(nullptr, macro_cache_mgr);

  ObSSMicroCache *micro_cache = MTL(ObSSMicroCache *);
  ASSERT_NE(nullptr, micro_cache);

  checkpoint::ObTabletGCService *gc_service = MTL(checkpoint::ObTabletGCService*);
  ASSERT_NE(nullptr, gc_service);
  gc_service->stop();
  sleep(10);

  ObTenantDiskSpaceManager *tnt_disk_space_mgr = MTL(ObTenantDiskSpaceManager*);
  ASSERT_NE(tnt_disk_space_mgr, nullptr);
  macro_cache_mgr->evict_task_.is_inited_ = false;
  OK(exe_sql("create table test_macro_cache_full (a int)"));
  set_ls_and_tablet_id_for_run_ctx("test_macro_cache_full");

  // 7. Test if the space of hot tablet macro cache is full
  int64_t max_hot_tablet_size = tnt_disk_space_mgr->get_macro_cache_free_size();
  OK(tnt_disk_space_mgr->alloc_file_size(max_hot_tablet_size, ObSSMacroCacheType::HOT_TABLET_MACRO_BLOCK));
  int64_t after_alloc_hot_tablet_size = tnt_disk_space_mgr->get_macro_cache_free_size();
  ASSERT_EQ(0, after_alloc_hot_tablet_size);
  OK(exe_sql("insert into test_macro_cache_full values (7)"));
  OK(exe_sql("insert into test_macro_cache_full values (8)"));
  OK(exe_sql("insert into test_macro_cache_full values (9)"));

  OK(medium_compact(run_ctx_.tablet_id_.id()));
  OK(exe_sql("alter table test_macro_cache_full storage_cache_policy (global = 'auto');"));
  OK(exe_sql("alter table test_macro_cache_full storage_cache_policy (global = 'hot');"));
  ObStorageCachePolicyPrewarmer prewarmer2;
  ObStorageCacheTabletTask *task2 = static_cast<ObStorageCacheTabletTask *>(ob_malloc(
      sizeof(ObStorageCacheTabletTask),
      ObMemAttr(run_ctx_.tenant_id_, "TestPrewarm2")));
  ASSERT_NE(nullptr, task2);
  new (task2) ObStorageCacheTabletTask();
  OK(task2->init(
      run_ctx_.tenant_id_,
      run_ctx_.ls_id_.id(),
      run_ctx_.tablet_id_.id(),
      PolicyStatus::HOT));
  task2->inc_ref_count();

  micro_cache->clear_micro_cache();
  OK(prewarmer2.prewarm_hot_tablet(task2));

  ObSEArray<MacroBlockId, 64> data_block_ids;
  ObSEArray<MacroBlockId, 16> meta_block_ids;
  FLOG_INFO("[TEST] start to check macro cache", K(run_ctx_), K(max_hot_tablet_size));
  data_block_ids.set_attr(ObMemAttr(run_ctx_.tenant_id_, "DataBlockIDs"));
  meta_block_ids.set_attr(ObMemAttr(run_ctx_.tenant_id_, "MetaBlockIDs"));

  OK(get_macro_blocks(run_ctx_.ls_id_, run_ctx_.tablet_id_, data_block_ids, meta_block_ids));
  FLOG_INFO("[TEST] get macro blocks", K(prewarmer2.tablet_id_), K(prewarmer2.ls_id_), K(data_block_ids), K(meta_block_ids));

  ObSSPrewarmStat second_stat;
  second_stat = prewarmer2.get_hot_retention_prewarm_stat();
  ASSERT_GE(second_stat.get_macro_block_bytes(), second_stat.get_macro_cache_max_available_size());
  ASSERT_LT(second_stat.get_micro_block_bytes(), second_stat.get_micro_cache_max_available_size());
  ObSSMicroCacheStat &after_cache_stat = micro_cache->cache_stat_;
  OK(task2->generate_comment());

  FLOG_INFO("[TEST] test macro cache full, micro cache is not full", K(second_stat), K(second_stat.get_macro_cache_max_available_size()),
      K(second_stat.get_micro_cache_max_available_size()), K(after_cache_stat.micro_stat()), KPC(task2), K(task2->get_comment()));

  const char *macro_cache_warn_comment = "current macro cache available size";
  ASSERT_NE(nullptr, strstr(task2->get_comment().ptr(), macro_cache_warn_comment));
  const char *size_comment = "size:";
  ASSERT_NE(nullptr, strstr(task2->get_comment().ptr(), size_comment));
  ASSERT_TRUE(data_block_ids.count() + meta_block_ids.count() > second_stat.macro_data_block_num_ + second_stat.macro_block_hit_cnt_);
  ASSERT_EQ(OB_SUCCESS, tnt_disk_space_mgr->free_file_size(max_hot_tablet_size, ObSSMacroCacheType::HOT_TABLET_MACRO_BLOCK));
  FLOG_INFO("[TEST] test_prewarm_macro_block end", K(tnt_disk_space_mgr->get_macro_cache_free_size()));
}

TEST_F(ObStorageCachePolicyPrewarmerTest, test_micro_cache_full)
{
  FLOG_INFO("[TEST] start test_micro_cache_full");

  share::ObTenantSwitchGuard tguard;
  OK(tguard.switch_to(run_ctx_.tenant_id_));
  exe_prepare_sql();

  OK(exe_sql("create table test_micro_cache (a int)"));
  set_ls_and_tablet_id_for_run_ctx("test_micro_cache");
  OK(exe_sql("insert into test_micro_cache values (1)"));
  OK(exe_sql("insert into test_micro_cache values (2)"));
  OK(medium_compact(run_ctx_.tablet_id_.id()));
  FLOG_INFO("[TEST] finish to wait major freeze");

  ObSSMicroCache *micro_cache = MTL(ObSSMicroCache *);
  ASSERT_NE(nullptr, micro_cache);
  ObTenantDiskSpaceManager *tnt_disk_space_mgr = MTL(ObTenantDiskSpaceManager*);
  ASSERT_NE(tnt_disk_space_mgr, nullptr);

  ObTenantFileManager *tnt_file_mgr = MTL(ObTenantFileManager *);
  ASSERT_NE(nullptr, tnt_file_mgr);
  tnt_file_mgr->persist_disk_space_task_.enable_adjust_size_ = false;
  ob_usleep(5 * 1000 * 1000);

  const int64_t block_size = micro_cache->phy_blk_size_;
  ObSSMicroCacheStat &cache_stat = micro_cache->cache_stat_;
  ObSSARCInfo &arc_info = micro_cache->micro_meta_mgr_.arc_info_;
  const int64_t micro_cache_file_size = micro_cache->cache_file_size_;

  const int64_t max_micro_size = 16 * 1024; // 16KB

  const int64_t WRITE_BLK_CNT = 300;
  ObArenaAllocator allocator;
  char *data_buf = static_cast<char*>(allocator.alloc(max_micro_size));
  ASSERT_NE(nullptr, data_buf);
  MEMSET(data_buf, 'a', max_micro_size);
  ObSSPhyBlockCommonHeader common_header;
  ObSSMicroDataBlockHeader data_blk_header;
  const int64_t payload_offset = common_header.get_serialize_size() + data_blk_header.get_serialize_size();

  int64_t prev_add_micro_cnt = 0;
  int64_t prev_reorgan_cnt = 0;
  const int64_t total_cnt = micro_cache_file_size / block_size;
  const int64_t total_data_blk_cnt = micro_cache->phy_blk_mgr_.blk_cnt_info_.micro_data_blk_max_cnt();
  const int64_t TOTAL_ROUND = (total_cnt / WRITE_BLK_CNT) + 1;
  int64_t before_max_available_size = 0;
  micro_cache->get_available_space_for_prewarm(before_max_available_size);
  FLOG_INFO("[TEST] start to write to micro cache", K(block_size), K(micro_cache_file_size), K(TOTAL_ROUND), K(total_data_blk_cnt), K(total_cnt), K(before_max_available_size));
  micro_cache->task_runner_.disable_release_cache(); // Stop micro cache release task to ensure filling up micro cache
  for (int64_t round = 0; round < TOTAL_ROUND; ++round) {
    for (int64_t i = WRITE_BLK_CNT * round; i < WRITE_BLK_CNT * (round + 1); ++i) {
      MacroBlockId macro_id = TestSSCommonUtil::gen_macro_block_id(i + 1);
      int32_t offset = payload_offset;
      do {
        const int32_t micro_size = max_micro_size;
        ObSSMicroBlockCacheKey micro_key = TestSSCommonUtil::gen_phy_micro_key(macro_id, offset, micro_size);
        micro_cache->add_micro_block_cache(micro_key, data_buf, micro_size, macro_id.second_id(), ObSSMicroCacheAccessType::COMMON_IO_TYPE);
        offset += micro_size;
      } while (offset < block_size);
      ob_usleep((i / 50 + 1) * 5 * 1000);
    }
  }
  int64_t after_max_available_size = 0;
  micro_cache->get_available_space_for_prewarm(after_max_available_size);
  ASSERT_TRUE(before_max_available_size > after_max_available_size);

  FLOG_INFO("[TEST] micro cache has no space now", K(before_max_available_size), K(after_max_available_size), K(cache_stat));
  OK(exe_sql("alter table test_micro_cache storage_cache_policy (global = 'auto');"));
  OK(exe_sql("alter table test_micro_cache storage_cache_policy (global = 'hot');"));
  ObStorageCachePolicyPrewarmer micro_cache_prewarmer;
  ObStorageCacheTabletTask *micro_cache_task = static_cast<ObStorageCacheTabletTask *>(ob_malloc(
      sizeof(ObStorageCacheTabletTask),
      ObMemAttr(run_ctx_.tenant_id_, "TestPrewarm2")));
  ASSERT_NE(nullptr, micro_cache_task);
  new (micro_cache_task) ObStorageCacheTabletTask();
  OK(micro_cache_task->init(
      run_ctx_.tenant_id_,
      run_ctx_.ls_id_.id(),
      run_ctx_.tablet_id_.id(),
      PolicyStatus::HOT));
  micro_cache_task->inc_ref_count();
  FLOG_INFO("[TEST] start to prewarm micro_cache_task", KPC(micro_cache_task));

  OK(micro_cache_prewarmer.prewarm_hot_tablet(micro_cache_task));
  ObSSPrewarmStat second_stat;
  second_stat = micro_cache_prewarmer.get_hot_retention_prewarm_stat();
  ObSSMicroCacheStat &after_cache_stat = micro_cache->cache_stat_;
  int64_t after_max_available_size2 = 0;
  micro_cache->get_available_space_for_prewarm(after_max_available_size2);
  FLOG_INFO("[TEST] second stat", K(second_stat), K(after_cache_stat.micro_stat()), K(after_cache_stat), K(after_max_available_size2));
  ASSERT_LT(second_stat.get_macro_block_bytes(), second_stat.get_macro_cache_max_available_size());
  ASSERT_GE(second_stat.get_micro_block_bytes(), second_stat.get_micro_cache_max_available_size());
  OK(micro_cache_task->generate_comment());
  const char *macro_cache_warn_comment = "current micro cache available size";
  ASSERT_NE(nullptr, strstr(micro_cache_task->get_comment().ptr(), macro_cache_warn_comment));
  const char *size_comment = "size:";
  ASSERT_NE(nullptr, strstr(micro_cache_task->get_comment().ptr(), size_comment));

  FLOG_INFO("[TEST] test micro cache full, macro cache is not full", K(second_stat), K(second_stat.get_macro_cache_max_available_size()),
      K(second_stat.get_micro_cache_max_available_size()), K(after_cache_stat.micro_stat()), KPC(micro_cache_task), K(micro_cache_task->get_comment()));
  FLOG_INFO("[TEST] finish test_micro_cache_full");
}

TEST_F(ObStorageCachePolicyPrewarmerTest, test_comment)
{
  FLOG_INFO("[TEST] finish test_comment");
  share::ObTenantSwitchGuard tguard;
  OK(tguard.switch_to(run_ctx_.tenant_id_));
  OK(exe_sql("create table test_comment (a int)"));
  set_ls_and_tablet_id_for_run_ctx("test_comment");

  ObSCPTraceIdGuard scp_trace_id_guard;
  ObStorageCacheTabletTask *task = static_cast<ObStorageCacheTabletTask *>(ob_malloc(
      sizeof(ObStorageCacheTabletTask),
      ObMemAttr(run_ctx_.tenant_id_, "TestPrewarm2")));
  ASSERT_NE(nullptr, task);
  new (task) ObStorageCacheTabletTask();
  OK(task->init(
      run_ctx_.tenant_id_,
      run_ctx_.ls_id_.id(),
      run_ctx_.tablet_id_.id(),
      PolicyStatus::HOT));
  task->inc_ref_count();
  task->result_ = OB_INVALID_ARGUMENT;
  OK(task->generate_comment());
  const char *ret_err = "OB_INVALID_ARGUMENT";
  FLOG_INFO("[TEST] finish test_comment", K(task->get_comment()), KPC(task));
  ASSERT_NE(nullptr, strstr(task->get_comment().ptr(), ret_err));
}

TEST_F(ObStorageCachePolicyPrewarmerTest, test_convert_hot_to_auto)
{
  FLOG_INFO("[TEST] start test_convert_hot_to_auto");
  share::ObTenantSwitchGuard tguard;
  OK(tguard.switch_to(run_ctx_.tenant_id_));
  exe_prepare_sql();

  OK(exe_sql("create table test_convert_hot_to_auto (a int)"));
  set_ls_and_tablet_id_for_run_ctx("test_convert_hot_to_auto");

  OK(exe_sql("insert into test_convert_hot_to_auto values (1)"));
  OK(exe_sql("insert into test_convert_hot_to_auto values (2)"));
  OK(exe_sql("insert into test_convert_hot_to_auto values (3)"));

  OK(medium_compact(run_ctx_.tablet_id_.id()));
  OK(exe_sql("alter table test_convert_hot_to_auto storage_cache_policy (global = 'hot');"));
  wait_task_finished(run_ctx_.tablet_id_.id());
  check_macro_blocks_type(ObSSMacroCacheType::HOT_TABLET_MACRO_BLOCK);

  OK(exe_sql("alter table test_convert_hot_to_auto storage_cache_policy (global = 'auto');"));
  sleep(30);
  check_macro_blocks_type(ObSSMacroCacheType::MACRO_BLOCK);
  FLOG_INFO("[TEST] finish test_convert_hot_to_auto");
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