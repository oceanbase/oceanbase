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

#include <cstdint>
#include <gtest/gtest.h>
#include "cluster/simple_server/env/ob_simple_cluster_test_base.h"
#include "lib/mysqlclient/ob_isql_client.h"
#include "lib/net/ob_addr.h"
#include "lib/ob_define.h"
#include "lib/string/ob_string.h"
#include "lib/time/ob_time_utility.h"
#include "observer/ob_server_struct.h"
#include "share/backup/ob_backup_struct.h"
#include "share/restore/ob_log_archive_source_mgr.h"

namespace oceanbase
{
namespace unittest
{
using namespace oceanbase::share;
class TestLogArchiveSource : public ObSimpleClusterTestBase
{
public:
  int init(const uint64_t tenant_id, ObISQLClient *proxy)
  {
    return mgr_.init(tenant_id, proxy);
  }
  int insert_service_source(const int64_t recovery_until_ts_ns, const ObAddr &addr)
  {
    return mgr_.add_service_source(recovery_until_ts_ns, addr);
  }
  int insert_location_source(const int64_t recovery_until_ts_ns, const ObString &dest)
  {
    return mgr_.add_location_source(recovery_until_ts_ns, dest);
  }
  int insert_piece_array(const int64_t recovery_until_ts_ns, DirArray &array)
  {
    return mgr_.add_rawpath_source(recovery_until_ts_ns, array);
  }
  int update_until_ts(const int64_t ts)
  {
    return mgr_.update_recovery_until_ts(ts);
  }
  int get_source(ObLogArchiveSourceItem &item)
  {
    return mgr_.get_source(item);
  }
  int delete_source()
  {
    return mgr_.delete_source();
  }
  int get_backup_dest(ObLogArchiveSourceItem &item, ObBackupDest &dest)
  {
    return mgr_.get_backup_dest(item, dest);
  }
private:
  share::ObLogArchiveSourceMgr mgr_;
};
TEST_F(TestLogArchiveSource, insert_source)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = OB_INVALID_TENANT_ID;
  int64_t ts = common::ObTimeUtility::current_time_ns();
  ObLogArchiveSourceItem item;
  ret = create_tenant();
  EXPECT_EQ(OB_SUCCESS, ret);

  while (true) {
    if (OB_SUCC(get_tenant_id(tenant_id))) {
      break;
    } else {
      sleep(1);
    }
  }

  ret = init(tenant_id, GCTX.sql_proxy_);
  EXPECT_EQ(OB_SUCCESS, ret);

  /*
  ret = insert_service_source(ts, GCONF.self_addr_);
  EXPECT_EQ(OB_SUCCESS, ret);
  ret = get_source(item);
  EXPECT_EQ(OB_SUCCESS, ret);
  */

  /*
  ret = insert_location_source(ts, ObString("oss://backup_dir/?host=xxx.com&access_id=111&access_key=222"));
  EXPECT_EQ(OB_SUCCESS, ret);
  ret = get_source(item);
  EXPECT_EQ(OB_SUCCESS, ret);
  */

  ObString path("file:///data/1/");
  ret = insert_location_source(ts, path);
  EXPECT_EQ(OB_SUCCESS, ret);
  ret = get_source(item);
  EXPECT_EQ(OB_SUCCESS, ret);
  ret = path.case_compare(item.value_);
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_EQ(ts, item.until_ts_);

  ts = common::ObTimeUtility::current_time_ns();
  ret = update_until_ts(ts);
  ret = get_source(item);
  EXPECT_EQ(OB_SUCCESS, ret);
  ret = path.case_compare(item.value_);
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_EQ(ts, item.until_ts_);

  ObString oss_path("oss://backup_dir/?host=xxx.com&access_id=111&access_key=222");
  ret = insert_location_source(ts, oss_path);
  EXPECT_EQ(OB_SUCCESS, ret);
  ret = get_source(item);
  EXPECT_EQ(OB_SUCCESS, ret);
  ObBackupDest dest;
  ret = get_backup_dest(item, dest);
  EXPECT_EQ(OB_SUCCESS, ret);

  ret = delete_source();
  EXPECT_EQ(OB_SUCCESS, ret);
  ret = get_source(item);
  EXPECT_EQ(OB_ERR_NULL_VALUE, ret);
}

} // namespace unittest
} // namespace oceanbase


int main(int argc, char **argv)
{
  OB_LOGGER.set_file_name("test_log_archive_source.log", true, false, "test_log_archive_source_rs.log", "test_archive_election.log");
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
