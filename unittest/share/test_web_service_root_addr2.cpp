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

#define USING_LOG_PREFIX SHARE

#include <gtest/gtest.h>
#include "share/ob_web_service_root_addr.h"
#include "schema/db_initializer.h"

namespace oceanbase
{
namespace share
{
using namespace common;

class TestWebServiceRootAddr : public ::testing::Test
{
public:
  TestWebServiceRootAddr() : service_pid_(0) {}

  virtual void SetUp();
  virtual void TearDown();

  void set_response_json(const char *json);

protected:
  pid_t service_pid_;
  schema::DBInitializer initer_;
  schema::DBInitializer initer2_;
  ObWebServiceRootAddr ws_;
};

void TestWebServiceRootAddr::SetUp()
{
  int ret = OB_SUCCESS;
  initer2_.get_config().obconfig_url.set_value("http://127.0.0.1:8658/obconfig/region1");
  initer2_.get_config().cluster_id.set_value("1");

  pid_t pid = fork();
  if (0 == pid) {
    ret = setpgid(pid, pid);
    if (ret < 0) {
      LOG_ERROR("setpgid failed", K(errno));
    } else if (-1 == (ret = execl(
                "/bin/bash", "./fake_ob_config-sh2", "./fake_ob_config-sh2", "8658", NULL))) {
      LOG_ERROR("execl failed", K(errno));
    }
    exit(1);
  } else if (-1 == pid) {
    LOG_ERROR("fork failed", K(errno));
  } else {
    LOG_INFO("create child", K(pid));
    service_pid_ = pid;

    // wait ./fake_ob_config-sh execute.
    usleep(100000);
  }
}

void TestWebServiceRootAddr::TearDown()
{
  int status = 0;
  kill(-service_pid_, SIGINT);
  pid_t pid = wait(&status);
  LOG_INFO("child exit", K(pid));
}

void TestWebServiceRootAddr::set_response_json(const char *json)
{
  usleep(50000);
  ObSqlString cmd;
  int ret = cmd.assign_fmt("echo -n 'POST / HTTP/1.1\r\nContent-Length: %ld\r\n%s' | nc 127.0.0.1 8658",
      strlen(json), json);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = system(cmd.ptr());
  LOG_INFO("set response json", K(cmd), K(ret), K(errno), K(json));
  usleep(50000);
}

TEST_F(TestWebServiceRootAddr, fetch_version2)
{
  ObSEArray<ObRootAddr, 16> rs_list;
  ObSEArray<ObRootAddr, 16> readonly_rs_list;

  //No master
  set_response_json("[{\"ObRegion\":\"ob2.rongxuan.lc\",\"ObRegionId\":2,\"Type\":\"SLAVE\",\"RsList\":[{\"address\":\"127.0.0.1:16825\",\"role\":\"LEADER\",\"sql_port\":16860},{\"address\":\"127.0.0.2:16826\",\"role\":\"FOLLOWER\",\"sql_port\":16861}],\"ReadonlyRsList\":[]}]");
  int ret = ws_.fetch_master_cluster_info(&(initer_.get_config()), rs_list, readonly_rs_list);
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, ret);

  //Have master
  set_response_json("[{\"ObRegion\":\"ob2.rongxuan.lc\",\"ObRegionId\":2,\"Type\":\"MASTER\",\"RsList\":[{\"address\":\"127.0.0.1:16825\",\"role\":\"LEADER\",\"sql_port\":16860}],\"ReadonlyRsList\":[]}]");
  ret = ws_.fetch_master_cluster_info(&(initer_.get_config()), rs_list, readonly_rs_list);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(rs_list.count(), 1);
  ASSERT_EQ(readonly_rs_list.count(), 0);
  ASSERT_EQ(16860, rs_list.at(0).sql_port_);

  //Active and standby two clusters
  set_response_json("[{\"ObRegion\":\"ob2.rongxuan.lc\",\"ObRegionId\":2,\"Type\":\"SLAVE\",\"RsList\":[{\"address\":\"127.0.0.1:16825\",\"role\":\"LEADER\",\"sql_port\":16860},{\"address\":\"127.0.0.2:16826\",\"role\":\"FOLLOWER\",\"sql_port\":16861}],\"ReadonlyRsList\":[]}"
                    ",{\"ObRegion\":\"ob2.rongxuan.lc\",\"ObRegionId\":3,\"Type\":\"MASTER\",\"RsList\":[{\"address\":\"10.101.67.160:16820\",\"role\":\"LEADER\",\"sql_port\":16870},"
                    "{\"address\":\"127.0.0.3:16825\",\"role\":\"FOLLOWER\",\"sql_port\":16865}],\"ReadonlyRsList\":[]}]");
  ret = ws_.fetch_master_cluster_info(&(initer_.get_config()), rs_list, readonly_rs_list);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(rs_list.count(), 2);
  ASSERT_EQ(readonly_rs_list.count(), 0);
}
} // end namespace share
} // end namespace oceanbase

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
