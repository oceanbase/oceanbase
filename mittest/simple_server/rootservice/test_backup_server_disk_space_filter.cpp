/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX RS

#include "env/ob_simple_cluster_test_base.h"

#define private public
#include "rootserver/backup/ob_backup_server_disk_space_filter.h"

namespace oceanbase
{
using namespace unittest;
namespace rootserver
{
using namespace common;
using namespace share;

class TestBackupServerDiskSpaceFilter : public ObSimpleClusterTestBase
{
public:
  TestBackupServerDiskSpaceFilter() : ObSimpleClusterTestBase("test_backup_server_disk_space_filter") {}
};

// query_all_disk_stats returns OB_NOT_INIT before init() is called
TEST_F(TestBackupServerDiskSpaceFilter, test_not_init)
{
  ObBackupServerDiskSpaceFilter filter;
  ObArray<ObBackupServerDiskSpaceFilter::RawDiskStat> raw_stats;
  ASSERT_EQ(OB_NOT_INIT, filter.query_all_disk_stats(raw_stats));
}

// apply_disk_filter on empty input returns success with empty output
TEST_F(TestBackupServerDiskSpaceFilter, test_empty_input)
{
  ObArray<ObBackupServer> input, output;
  ObArray<ObBackupServerDiskSpaceFilter::RawDiskStat> raw_stats;
  ASSERT_EQ(OB_SUCCESS, ObBackupServerDiskSpaceFilter::apply_disk_filter(input, raw_stats, output));
  ASSERT_EQ(0, output.count());
}

// ServerWithDiskInfo sorts by (priority asc, used_pct asc)
TEST_F(TestBackupServerDiskSpaceFilter, test_server_sort_order)
{
  ObBackupServerDiskSpaceFilter::ServerWithDiskInfo a, b;
  ObAddr addr1, addr2;
  addr1.set_ip_addr("127.0.0.1", 2882);
  addr2.set_ip_addr("127.0.0.2", 2882);
  ASSERT_EQ(OB_SUCCESS, a.server_.set(addr1, 1));
  a.used_percentage_ = 60;
  ASSERT_EQ(OB_SUCCESS, b.server_.set(addr2, 2));
  b.used_percentage_ = 40;
  // lower priority value is preferred: a(priority=1) before b(priority=2)
  ASSERT_TRUE(a < b);
  ASSERT_FALSE(b < a);

  // same priority: lower used_pct preferred
  ASSERT_EQ(OB_SUCCESS, b.server_.set(addr2, 1));
  b.used_percentage_ = 40;
  ASSERT_FALSE(a < b);  // a.used_pct(60) > b.used_pct(40)
  ASSERT_TRUE(b < a);
}

// With a real cluster server, query+apply passes the server through at default threshold
TEST_F(TestBackupServerDiskSpaceFilter, test_batch_query_and_filter)
{
  ObMySQLProxy &sql_proxy = get_curr_observer().get_mysql_proxy();
  ObAddr svr_addr = get_curr_simple_server().get_addr();
  ObBackupServerDiskSpaceFilter filter;
  ASSERT_EQ(OB_SUCCESS, filter.init(&sql_proxy));

  ObArray<ObBackupServer> input, output;
  ObBackupServer bs;
  ASSERT_EQ(OB_SUCCESS, bs.set(svr_addr, 1));
  ASSERT_EQ(OB_SUCCESS, input.push_back(bs));

  ObArray<ObBackupServerDiskSpaceFilter::RawDiskStat> raw_stats;
  ASSERT_EQ(OB_SUCCESS, filter.query_all_disk_stats(raw_stats));
  // With default threshold (85%), the test environment server should pass
  ASSERT_EQ(OB_SUCCESS, ObBackupServerDiskSpaceFilter::apply_disk_filter(input, raw_stats, output));
  ASSERT_EQ(1, output.count());
  ASSERT_EQ(svr_addr, output.at(0).server_);
}

// A server not present in __all_virtual_server is treated as ok (used_pct=0) and passes through
TEST_F(TestBackupServerDiskSpaceFilter, test_absent_server_passes)
{
  ObMySQLProxy &sql_proxy = get_curr_observer().get_mysql_proxy();
  ObBackupServerDiskSpaceFilter filter;
  ASSERT_EQ(OB_SUCCESS, filter.init(&sql_proxy));

  ObArray<ObBackupServer> input, output;
  ObBackupServer bs;
  ObAddr nonexistent;
  // Use a non-routable IP guaranteed not to be in __all_virtual_server
  ASSERT_TRUE(nonexistent.set_ip_addr("192.0.2.1", 2882));
  ASSERT_EQ(OB_SUCCESS, bs.set(nonexistent, 1));
  ASSERT_EQ(OB_SUCCESS, input.push_back(bs));

  ObArray<ObBackupServerDiskSpaceFilter::RawDiskStat> raw_stats;
  ASSERT_EQ(OB_SUCCESS, filter.query_all_disk_stats(raw_stats));
  ASSERT_EQ(OB_SUCCESS, ObBackupServerDiskSpaceFilter::apply_disk_filter(input, raw_stats, output));
  // Absent server is treated as disk-ok and passes through
  ASSERT_EQ(1, output.count());
  ASSERT_EQ(nonexistent, output.at(0).server_);
}

// query_all_disk_stats returns disk stats for every server in __all_virtual_server.
// Verify the current observer is present with non-negative usage.
TEST_F(TestBackupServerDiskSpaceFilter, test_disk_stats_populated)
{
  ObMySQLProxy &sql_proxy = get_curr_observer().get_mysql_proxy();
  ObAddr svr_addr = get_curr_simple_server().get_addr();
  ObBackupServerDiskSpaceFilter filter;
  ASSERT_EQ(OB_SUCCESS, filter.init(&sql_proxy));

  ObArray<ObBackupServerDiskSpaceFilter::RawDiskStat> raw_stats;
  ASSERT_EQ(OB_SUCCESS, filter.query_all_disk_stats(raw_stats));
  ASSERT_GT(raw_stats.count(), 0);

  bool found = false;
  for (int64_t i = 0; i < raw_stats.count(); ++i) {
    if (raw_stats.at(i).server_ == svr_addr) {
      ASSERT_GT(raw_stats.at(i).capacity_, 0);
      ASSERT_GE(raw_stats.at(i).in_use_, 0);
      found = true;
      break;
    }
  }
  ASSERT_TRUE(found);
}

} // namespace rootserver
} // namespace oceanbase

int main(int argc, char **argv)
{
  oceanbase::unittest::init_log_and_gtest(argc, argv);
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
