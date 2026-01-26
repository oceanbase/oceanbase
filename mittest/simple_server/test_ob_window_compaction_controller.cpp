// owner: ouyanghongrong.oyh
// owner group: storage

/**
 * Copyright (c) 2025 OceanBase
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
#define USING_LOG_PREFIX STORAGE
#define protected public
#define private public

#include "env/ob_simple_cluster_test_base.h"
#include "storage/compaction/ob_window_loop.h"
#include "storage/compaction/ob_tenant_tablet_scheduler.h"

namespace oceanbase
{
namespace compaction
{
namespace unittest
{
class ObWindowCompactionControllerTest : public ::oceanbase::unittest::ObSimpleClusterTestBase
{
public:
  struct ReportStat {
    ReportStat() = default;
    ReportStat(
      const char* tname,
      const ObTableModeFlag mode,
      const int64_t ls_id,
      const uint64_t tablet_id,
      const uint32_t query_cnt,
      const uint64_t scan_physical_row_cnt,
      const uint64_t inc_row_cnt)
    : tname_(tname),
      mode_(mode)
    {
      stat_.ls_id_ = ls_id;
      stat_.tablet_id_ = tablet_id;
      stat_.query_cnt_ = query_cnt;
      stat_.scan_physical_row_cnt_ = scan_physical_row_cnt;
      stat_.insert_row_cnt_ = inc_row_cnt;
    }
    TO_STRING_KV(K_(tname), K_(mode), K_(stat));
public:
    const char* tname_;
    ObTableModeFlag mode_;
    ObTabletStat stat_;
  };
public:
  ObWindowCompactionControllerTest() : ObSimpleClusterTestBase("test_ob_window_compaction_controller") {}
  void execute_stmt(int &ret,const char* label, const char* stmt);
  void create_table_and_fetch_infomations(
    int &ret,
    const char *tname,
    const ObTableModeFlag mode,
    int64_t &ls_id,
    uint64_t &tablet_id);
  void insert_data(
    int &ret,
    const char* tname,
    const uint64_t row_cnt);
  void wait_refresh();
  void check_report_stats(ObIArray<ReportStat> &report_stats);
  void mock_one_table(
    ObIArray<ReportStat> &report_stats,
    const char* tname,
    uint32_t &base_query_cnt,
    uint64_t &base_scan_physical_row_cnt,
    const uint64_t inc_row_cnt,
    const ObTableModeFlag mode);
  void alter_table_mode(const char*tname, const ObTableModeFlag new_mode);
  void alter_compaction_params();
};

void ObWindowCompactionControllerTest::execute_stmt(int &ret, const char* label, const char* stmt)
{
  ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  LOG_INFO("start execute", K(label), K(stmt));
  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy();
  ASSERT_EQ(OB_SUCCESS, sql_proxy.write(stmt, affected_rows));
  LOG_INFO("finish execute", K(label), K(stmt));
}

void ObWindowCompactionControllerTest::create_table_and_fetch_infomations(
  int &ret,
  const char *tname,
  const ObTableModeFlag mode,
  int64_t &ls_id,
  uint64_t &tablet_id)
{
  ret = OB_SUCCESS;
  // 1. Create table
  {
    common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy();
    LOG_INFO("start create table", K(tname));
    ObSqlString sql;
    int64_t affected_rows = 0;
    ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("create table %s (k int primary key, v int) table_mode='%s'", tname, table_mode_flag_to_str(mode)));
    ASSERT_EQ(OB_SUCCESS, sql_proxy.write(sql.ptr(), affected_rows));
    LOG_INFO("finish create table", K(tname));
  }

  static bool need_init = true;
  if (need_init) {
    need_init = false;
    ASSERT_EQ(OB_SUCCESS, get_curr_simple_server().init_sql_proxy2("sys", "oceanbase"));
  }
  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();
  {
    LOG_INFO("start query table_id", K(tname));
    ObSqlString sql;
    ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("select tablet_id from __all_virtual_table where table_name='%s'", tname));
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      ASSERT_EQ(OB_SUCCESS, sql_proxy.read(res, sql.ptr()));
      sqlclient::ObMySQLResult *result = res.get_result();
      ASSERT_NE(nullptr, result);
      ASSERT_EQ(OB_SUCCESS, result->next());
      ASSERT_EQ(OB_SUCCESS, result->get_uint("tablet_id", tablet_id));
      ASSERT_NE(0, tablet_id);
    }
    LOG_INFO("finish query table_id", K(tname), K(tablet_id));
  }
  {
    LOG_INFO("start query ls_id", K(tname));
    ObSqlString sql;
    ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("select ls_id from __all_virtual_tablet_to_ls where tablet_id=%ld", tablet_id));
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      ASSERT_EQ(OB_SUCCESS, sql_proxy.read(res, sql.ptr()));
      sqlclient::ObMySQLResult *result = res.get_result();
      ASSERT_NE(nullptr, result);
      ASSERT_EQ(OB_SUCCESS, result->next());
      ASSERT_EQ(OB_SUCCESS, result->get_int("ls_id", ls_id));
    }
    LOG_INFO("finish query ls_id", K(tname), K(ls_id));
  }
  LOG_INFO("Success to create table", K(tname), "mode", table_mode_flag_to_str(mode), K(ls_id), K(tablet_id));
}


void ObWindowCompactionControllerTest::insert_data(
  int &ret,
  const char* tname,
  const uint64_t row_cnt)
{
  ret = OB_SUCCESS;
  // 1. Insert data
  {
    common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy();
    int64_t affected_rows = 0;
    LOG_INFO("start insert data", K(tname), K(row_cnt));
    ObSqlString sql;
    ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("insert into %s select COLUMN_VALUE, COLUMN_VALUE from (select * from  table(generator(%ld))) ", tname, row_cnt));
    ASSERT_EQ(OB_SUCCESS, sql_proxy.write(sql.ptr(), affected_rows));
    ASSERT_EQ(row_cnt, affected_rows);
    LOG_INFO("finish insert data", K(tname), K(row_cnt));
  }
}

void ObWindowCompactionControllerTest::wait_refresh()
{
  ObTenantTabletStatMgr *stat_mgr = MTL(ObTenantTabletStatMgr *);
  ASSERT_NE(nullptr, stat_mgr);

  const int64_t current_time = ObTimeUtility::current_time();
  const int64_t last_update_time = stat_mgr->get_last_update_time();
  const int64_t TIMEOUT_INTERVAL = 5 * ObTenantTabletStatMgr::CHECK_INTERVAL;
  LOG_INFO("Wait until ObTenantTabletStatMgr refresh", K(current_time), K(last_update_time));

  while (last_update_time == stat_mgr->get_last_update_time()) {
    LOG_INFO("sleep for a while");
    usleep(ObTenantTabletStatMgr::CHECK_INTERVAL / 2);
    if ((ObTimeUtility::current_time() - current_time) > TIMEOUT_INTERVAL) {
      ASSERT_TRUE(false) << "Waiting stat mgr update timeout";
    }
  }
  ASSERT_GT(stat_mgr->get_last_update_time(), last_update_time);
  LOG_INFO("Finsih waiting ObTenantTabletStatMgr refresh");
}

void ObWindowCompactionControllerTest::mock_one_table(
  ObIArray<ReportStat> &report_stats,
  const char* tname,
  uint32_t &base_query_cnt,
  uint64_t &base_scan_physical_row_cnt,
  const uint64_t inc_row_cnt,
  const ObTableModeFlag mode)
{
  int ret = OB_SUCCESS;
  bool succ_report = false;
  int64_t cur_ls_id = OB_INVALID_ID;
  uint64_t cur_tablet_id = OB_INVALID_ID;
  ObTenantTabletStatMgr *stat_mgr = MTL(ObTenantTabletStatMgr *);

  ASSERT_NE(nullptr, stat_mgr);
  create_table_and_fetch_infomations(ret, tname, mode, cur_ls_id, cur_tablet_id);
  ASSERT_EQ(OB_SUCCESS, ret);
  insert_data(ret, tname, inc_row_cnt);
  ASSERT_EQ(OB_SUCCESS, ret);
  ReportStat rep_stat(tname, mode, cur_ls_id, cur_tablet_id, base_query_cnt++, base_scan_physical_row_cnt++, inc_row_cnt);
  ASSERT_EQ(OB_SUCCESS, report_stats.push_back(rep_stat));
  ASSERT_EQ(OB_SUCCESS, stat_mgr->report_stat(rep_stat.stat_, succ_report));
  ASSERT_TRUE(succ_report);
}

TEST_F(ObWindowCompactionControllerTest, test_basic_init)
{
  LOG_INFO("ObWindowCompactionControllerTest::test_basic_init");
  int ret = OB_SUCCESS;
  share::ObTenantSwitchGuard tenant_guard;
  ret = tenant_guard.switch_to(OB_SYS_TENANT_ID);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObTenantTabletStatMgr *stat_mgr = MTL(ObTenantTabletStatMgr *);
  ASSERT_NE(nullptr, stat_mgr);

  uint32_t base_query_cnt = 100000;
  uint64_t base_scan_physical_row_cnt = 10001;
  ObSEArray<ReportStat, 10> report_stats;
  mock_one_table(report_stats, "qt0", base_query_cnt, base_scan_physical_row_cnt, 1,     ObTableModeFlag::TABLE_MODE_NORMAL);
  mock_one_table(report_stats, "qt1", base_query_cnt, base_scan_physical_row_cnt, 10,    ObTableModeFlag::TABLE_MODE_NORMAL);
  mock_one_table(report_stats, "qt2", base_query_cnt, base_scan_physical_row_cnt, 100,   ObTableModeFlag::TABLE_MODE_NORMAL);
  mock_one_table(report_stats, "qt3", base_query_cnt, base_scan_physical_row_cnt, 1000,  ObTableModeFlag::TABLE_MODE_NORMAL);

  execute_stmt(ret, "minor freeze", "alter system minor freeze");

  wait_refresh();

  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2(); // has been inited in mock_one_table
  {
    ObSqlString sql;
    int64_t affected_rows = 0;
    ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("alter system set enable_window_compaction = true tenant = all"));
    ASSERT_EQ(OB_SUCCESS, sql_proxy.write(sql.ptr(), affected_rows));
    LOG_INFO("finish set merger_check_interval", K(affected_rows));
    ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("alter system set ob_compaction_schedule_interval = '10s' tenant = all"));
    ASSERT_EQ(OB_SUCCESS, sql_proxy.write(sql.ptr(), affected_rows));
    LOG_INFO("finish set ob_compaction_schedule_interval", K(affected_rows));
    ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("alter system set merger_check_interval = '10s' tenant = all"));
    ASSERT_EQ(OB_SUCCESS, sql_proxy.write(sql.ptr(), affected_rows));
    LOG_INFO("finish set merger_check_interval", K(affected_rows));
  }

  // in mittest, tenant is sys tenant, so WindowLoop thread will not be started
  ObTenantTabletScheduler *scheduler = MTL(ObTenantTabletScheduler *);
  ASSERT_NE(nullptr, scheduler);
  ObWindowLoop &window_loop = scheduler->get_window_loop();
  const int64_t merge_start_time = ObTimeUtility::current_time();
  ASSERT_EQ(OB_SUCCESS, scheduler->update_merge_info(share::ObGlobalMergeInfo::MERGE_MODE_WINDOW, share::ObZoneMergeInfo::MERGE_STATUS_MERGING, merge_start_time));
  ASSERT_EQ(OB_SUCCESS, window_loop.start_window_compaction(merge_start_time));
  ASSERT_TRUE(window_loop.is_active());
  ObWindowCompactionPriorityQueue &prio_queue = window_loop.score_prio_queue_;
  ObWindowCompactionReadyList &ready_list = window_loop.ready_list_;

  ASSERT_EQ(OB_SUCCESS, window_loop.loop_tablet_stats());
  int64_t tablet_cnt = prio_queue.score_map_.size();
  ASSERT_EQ(OB_SUCCESS, window_loop.loop_tablet_stats());
  ASSERT_EQ(tablet_cnt, prio_queue.score_prio_queue_.count());
  ASSERT_EQ(tablet_cnt, prio_queue.score_map_.size());

  ASSERT_EQ(OB_SUCCESS, window_loop.loop_priority_queue());
  ASSERT_EQ(OB_SUCCESS, window_loop.loop_ready_list(0 /*ts_threshold*/));
  ASSERT_EQ(tablet_cnt, ready_list.candidate_map_.size());

  for (int64_t i = 0; i < 4; i++) {
    bool exist = false;
    const int64_t tablet_id = 200001 + i;
    ObTabletCompactionScore *candidate = nullptr;
    ready_list.inner_check_exist_and_get_candidate(ObTabletCompactionScoreKey(1, tablet_id), exist, candidate);
    ASSERT_TRUE(exist);
    ASSERT_NE(nullptr, candidate);
    ASSERT_EQ(tablet_id, candidate->get_key().tablet_id_.id());
    ASSERT_EQ(1, candidate->get_key().ls_id_.id());
  }
}

} // end compaction
} // end unittest
} // end oceanbase

int main(int argc, char **argv)
{
  oceanbase::unittest::init_log_and_gtest(argc, argv);
  OB_LOGGER.set_log_level("TRACE");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}