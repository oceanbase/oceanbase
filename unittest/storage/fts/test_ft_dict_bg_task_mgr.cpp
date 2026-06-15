/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "mtlenv/mock_tenant_module_env.h"
#include "share/ob_tenant_mem_limit_getter.h"
#include "storage/fts/dict/ob_ft_cache.h"
#define private public
#include "storage/fts/dict/ob_ft_dict_mgr.h"
#undef private
#include "share/rc/ob_tenant_base.h"

#include <cstring>
#include <gtest/gtest.h>

#define USING_LOG_PREFIX STORAGE_FTS

namespace oceanbase
{
namespace storage
{

class FTDictBgTaskMgrTest : public ::testing::Test
{
protected:
  FTDictBgTaskMgrTest() : tenant_id_(1), tenant_base_(tenant_id_), dict_mgr_() {}
  virtual ~FTDictBgTaskMgrTest() {}

  static void SetUpTestCase()
  {
    EXPECT_EQ(OB_SUCCESS, MockTenantModuleEnv::get_instance().init());
  }

  static void TearDownTestCase()
  {
    MockTenantModuleEnv::get_instance().destroy();
  }

  virtual void SetUp()
  {
    const int64_t kv_cache_wash_timer_interval_us = 60 * 1000L * 1000L;
    const int64_t default_bucket_num = 10000000L;
    const int64_t default_max_cache_size = 1024L * 1024L * 1024L * 1024L;
    int ret = OB_SUCCESS;
    ASSERT_EQ(OB_SUCCESS, ObTimerService::get_instance().start());
    if (OB_FAIL(ObKVGlobalCache::get_instance().init(&(ObTenantMemLimitGetter::get_instance()),
                                                     default_bucket_num,
                                                     default_max_cache_size,
                                                     lib::ACHUNK_SIZE,
                                                     kv_cache_wash_timer_interval_us))) {
      if (OB_INIT_TWICE == ret) {
        ret = OB_SUCCESS;
      }
    }
    ObDictCache::get_instance().init("dict cache");
    ASSERT_EQ(OB_SUCCESS, dict_mgr_.init());
    tenant_base_.set(&dict_mgr_);
    ObTenantEnv::set_tenant(&tenant_base_);
    ASSERT_EQ(OB_SUCCESS, tenant_base_.init());
    ASSERT_EQ(tenant_id_, MTL_ID());
  }

  virtual void TearDown()
  {
    ObTenantEnv::set_tenant(nullptr);
    ObDictCache::get_instance().destroy();
    ObKVGlobalCache::get_instance().destroy();
    ObClockGenerator::destroy();
    ObTimerService::get_instance().stop();
    ObTimerService::get_instance().wait();
    ObTimerService::get_instance().destroy();
  }

protected:
  void set_snapshot(const uint64_t table_id,
                    const int64_t row_scn,
                    const int64_t row_count,
                    const int64_t snapshot_version)
  {
    ObFTDictMgr::DictTableSnapshot snapshot;
    snapshot.reset();
    snapshot.row_scn_ = row_scn;
    snapshot.row_count_ = row_count;
    snapshot.snapshot_version_ = snapshot_version;
    ASSERT_EQ(OB_SUCCESS, dict_mgr_.update_table_snapshot(table_id, snapshot));
  }

protected:
  const uint64_t tenant_id_;
  ObTenantBase tenant_base_;
  ObFTDictMgr dict_mgr_;
};

TEST_F(FTDictBgTaskMgrTest, test_collect_table_ids)
{
  set_snapshot(1001, 1, 2, 3);
  set_snapshot(1002, 4, 5, 6);

  uint64_t table_ids[ObFTDictMgr::ROW_SCN_CACHE_SIZE];
  int64_t table_id_cnt = 0;
  ASSERT_EQ(OB_SUCCESS, dict_mgr_.collect_table_ids(table_ids, table_id_cnt));
  ASSERT_EQ(2, table_id_cnt);
  bool has_first = false;
  bool has_second = false;
  for (int64_t i = 0; i < table_id_cnt; ++i) {
    if (1001 == table_ids[i]) {
      has_first = true;
    } else if (1002 == table_ids[i]) {
      has_second = true;
    }
  }
  ASSERT_TRUE(has_first);
  ASSERT_TRUE(has_second);
}

TEST_F(FTDictBgTaskMgrTest, test_collect_table_ids_empty)
{
  uint64_t table_ids[ObFTDictMgr::ROW_SCN_CACHE_SIZE];
  int64_t table_id_cnt = -1;
  ASSERT_EQ(OB_SUCCESS, dict_mgr_.collect_table_ids(table_ids, table_id_cnt));
  ASSERT_EQ(0, table_id_cnt);
}

TEST_F(FTDictBgTaskMgrTest, test_collect_table_ids_invalid_argument)
{
  int64_t table_id_cnt = 0;
  ASSERT_EQ(OB_INVALID_ARGUMENT, dict_mgr_.collect_table_ids(nullptr, table_id_cnt));
}

TEST_F(FTDictBgTaskMgrTest, test_get_sql_statement_empty)
{
  ObFTDictRefreshTask task;
  common::ObSqlString sql;
  ASSERT_EQ(OB_SUCCESS, task.init());
  ASSERT_EQ(OB_SUCCESS, task.get_sql_statement(sql));
  ASSERT_TRUE(sql.empty());
  task.destroy();
}

TEST_F(FTDictBgTaskMgrTest, test_get_sql_statement_with_table_ids)
{
  ObFTDictRefreshTask task;
  common::ObSqlString sql;
  set_snapshot(1001, 11, 12, 13);
  set_snapshot(1002, 21, 22, 23);
  ASSERT_EQ(OB_SUCCESS, task.init());
  ASSERT_EQ(OB_SUCCESS, task.get_sql_statement(sql));
  ASSERT_FALSE(sql.empty());
  ASSERT_NE(nullptr, strstr(sql.ptr(), "1001"));
  ASSERT_NE(nullptr, strstr(sql.ptr(), "1002"));
  ASSERT_EQ(nullptr, strstr(sql.ptr(), share::OB_ALL_RECYCLEBIN_TNAME));
  ASSERT_NE(nullptr, strstr(sql.ptr(), "db.in_recyclebin = 0"));
  ASSERT_NE(nullptr, strstr(sql.ptr(), "WHERE t.tenant_id = 0 AND t.table_id IN"));
  task.destroy();
}

}  // namespace storage
}  // namespace oceanbase

int main(int argc, char **argv)
{
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
