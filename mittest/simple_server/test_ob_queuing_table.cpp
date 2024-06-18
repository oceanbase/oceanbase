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

#include <gtest/gtest.h>
#define USING_LOG_PREFIX STORAGE
#define protected public
#define private public

#include "env/ob_simple_cluster_test_base.h"
#include "storage/ob_tenant_tablet_stat_mgr.h"

namespace oceanbase
{
namespace unittest
{
class ObQueuingTableTest : public ObSimpleClusterTestBase
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
      const uint64_t scan_physical_row_cnt)
    : tname_(tname),
      mode_(mode)
    {
      stat_.ls_id_ = ls_id;
      stat_.tablet_id_ = tablet_id;
      stat_.query_cnt_ = query_cnt;
      stat_.scan_physical_row_cnt_ = scan_physical_row_cnt;
    }
    TO_STRING_KV(K_(tname), K_(mode), K_(stat));
public:
    const char* tname_;
    ObTableModeFlag mode_;
    ObTabletStat stat_;
  };
public:
  ObQueuingTableTest() : ObSimpleClusterTestBase("test_ob_queuing_table") {}
  void create_table_and_fetch_infomations(
    int &ret,
    const char *tname,
    const ObTableModeFlag mode,
    int64_t &ls_id,
    uint64_t &tablet_id);
  void wait_refresh();
  void check_report_stats(ObIArray<ReportStat> &report_stats);
  void mock_one_table(
    ObIArray<ReportStat> &report_stats,
    const char* tname,
    uint32_t &base_query_cnt,
    uint64_t &base_scan_physical_row_cnt,
    const ObTableModeFlag mode);
  void alter_table_mode(const char*tname, const ObTableModeFlag new_mode);
};

void ObQueuingTableTest::create_table_and_fetch_infomations(
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


void ObQueuingTableTest::wait_refresh()
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

void ObQueuingTableTest::check_report_stats(ObIArray<ReportStat> &report_stats)
{
  ObTenantTabletStatMgr *stat_mgr = MTL(ObTenantTabletStatMgr *);
  ASSERT_NE(nullptr, stat_mgr);
  FOREACH_CNT(it, report_stats) {
    ObTabletStat res;
    ObLSID ls_id(it->stat_.ls_id_);
    ObTabletID tablet_id(it->stat_.tablet_id_);
    ObTabletStat stat;
    ObTableModeFlag mode = ObTableModeFlag::TABLE_MODE_MAX;
    ASSERT_EQ(OB_SUCCESS, stat_mgr->get_latest_tablet_stat(ls_id, tablet_id, res, stat, mode));
    ASSERT_EQ(mode, it->mode_);
    ObTableQueuingModeCfg cfg;
    ASSERT_EQ(OB_SUCCESS, stat_mgr->get_queuing_cfg(ls_id, tablet_id, cfg));
    ASSERT_EQ(cfg.mode_, it->mode_);
    ASSERT_EQ(stat.query_cnt_, it->stat_.query_cnt_);
    ASSERT_EQ(stat.scan_physical_row_cnt_, it->stat_.scan_physical_row_cnt_);
  }
}

void ObQueuingTableTest::mock_one_table(
  ObIArray<ReportStat> &report_stats,
  const char* tname,
  uint32_t &base_query_cnt,
  uint64_t &base_scan_physical_row_cnt,
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
  ReportStat rep_stat(tname, mode, cur_ls_id, cur_tablet_id, base_query_cnt++, base_scan_physical_row_cnt++);
  ASSERT_EQ(OB_SUCCESS, report_stats.push_back(rep_stat));
  ASSERT_EQ(OB_SUCCESS, stat_mgr->report_stat(rep_stat.stat_, succ_report));
  ASSERT_TRUE(succ_report);
}

void ObQueuingTableTest::alter_table_mode(const char*tname, const ObTableModeFlag new_mode)
{
  int ret = OB_SUCCESS;
  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy();
  LOG_INFO("start alter table mode", K(tname), "new_mode", table_mode_flag_to_str(new_mode));
  ObSqlString sql;
  int64_t affected_rows = 0;
  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("alter table %s set table_mode='%s'", tname, table_mode_flag_to_str(new_mode)));
  ASSERT_EQ(OB_SUCCESS, sql_proxy.write(sql.ptr(), affected_rows));
  LOG_INFO("finish alter table mode", K(tname), "new_mode", table_mode_flag_to_str(new_mode));
}

TEST_F(ObQueuingTableTest, refresh_queuing_mode)
{
  LOG_INFO("ObQueuingTableTest::refresh_queuing_mode");
  int ret = OB_SUCCESS;
  share::ObTenantSwitchGuard tenant_guard;
  ret = tenant_guard.switch_to(OB_SYS_TENANT_ID);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObTenantTabletStatMgr *stat_mgr = MTL(ObTenantTabletStatMgr *);
  ASSERT_NE(nullptr, stat_mgr);

  uint32_t base_query_cnt = 100000;
  uint64_t base_scan_physical_row_cnt = 10000;
  ObSEArray<ReportStat, 10> report_stats;

  mock_one_table(report_stats, "qt0", base_query_cnt, base_scan_physical_row_cnt, ObTableModeFlag::TABLE_MODE_NORMAL);
  mock_one_table(report_stats, "qt1", base_query_cnt, base_scan_physical_row_cnt, ObTableModeFlag::TABLE_MODE_QUEUING);
  mock_one_table(report_stats, "qt2", base_query_cnt, base_scan_physical_row_cnt, ObTableModeFlag::TABLE_MODE_QUEUING_MODERATE);
  mock_one_table(report_stats, "qt3", base_query_cnt, base_scan_physical_row_cnt, ObTableModeFlag::TABLE_MODE_QUEUING_SUPER);
  mock_one_table(report_stats, "qt4", base_query_cnt, base_scan_physical_row_cnt, ObTableModeFlag::TABLE_MODE_QUEUING_EXTREME);
  wait_refresh();
  check_report_stats(report_stats);

  int64_t report_cnt = report_stats.count();
  ASSERT_GT(report_cnt, 0);
  ObTableModeFlag base_mode = ObTableModeFlag::TABLE_MODE_MAX;
  ObTableModeFlag new_mode = ObTableModeFlag::TABLE_MODE_MAX;
  int64_t idx = 0;
  bool succ_report = false;
  for (int64_t round = 0; round < 3; round++) {
    LOG_INFO("Checking alter table mode and report again", K(round));
    base_mode = report_stats.at(0).mode_;
    for (idx = 0; idx < report_cnt; idx++) {
      new_mode = idx == report_cnt - 1 ? base_mode : report_stats.at(idx+1).mode_;
      alter_table_mode(report_stats.at(idx).tname_, new_mode);
      ObTabletStat new_stat = report_stats.at(idx).stat_;
      new_stat.query_cnt_ += idx;
      new_stat.scan_physical_row_cnt_ += idx;
      ASSERT_EQ(OB_SUCCESS, stat_mgr->report_stat(new_stat, succ_report));
      ASSERT_TRUE(succ_report);
      report_stats.at(idx).stat_ += new_stat;
      report_stats.at(idx).mode_ = new_mode;
    }
    wait_refresh();
    check_report_stats(report_stats);
  }
}

} // end unittest
} // end oceanbase

int main(int argc, char **argv)
{
  oceanbase::unittest::init_log_and_gtest(argc, argv);
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}