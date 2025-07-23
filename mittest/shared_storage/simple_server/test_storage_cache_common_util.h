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
#ifndef TEST_SHARED_STORAGE_SIMPLE_SERVER_STORAGE_CAHCE_COMMON_DEFINE_H_
#define TEST_SHARED_STORAGE_SIMPLE_SERVER_STORAGE_CAHCE_COMMON_DEFINE_H_
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
#include "storage/compaction/ob_tenant_freeze_info_mgr.h"

using namespace oceanbase::transaction;
using namespace oceanbase::storage;

#ifdef __cplusplus
extern "C" const char *get_per_file_test_name();
#endif
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
      : ObSimpleClusterTestBase(get_per_file_test_name(), "50G", "50G", "50G")
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
  sleep(20);
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
  LOG_INFO("[TEST] finish set run ctx", K(run_ctx_), K(table_name));
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
}
}