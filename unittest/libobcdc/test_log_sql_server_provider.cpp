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

#define USING_LOG_PREFIX OBLOG

#include <gtest/gtest.h>
#include "share/ob_web_service_root_addr.h" // to_json
#include "lib/oblog/ob_log_module.h"
#include "lib/string/ob_sql_string.h"       // ObSqlString

#include "ob_log_sql_server_provider.h"

namespace oceanbase
{
using namespace common;
using namespace share;

namespace libobcdc
{
#define CONFIG_SERVER_PORT 6789
#define CONFIG_SERVER_PORT_STR "6789"
#define CONFIG_SERVER_IP "127.0.0.1"
#define CONFIG_SERVER_PROGRAM "./fake_config_server"

class TestLogSQLServerProvider : public ::testing::Test
{
  static const int64_t MAX_JASON_BUFFER_SIZE = 1 << 10;
  static const int64_t MAX_CONFIG_URL_LENGTH = 1 << 10;
  static const int64_t MAX_APPNAME_LENGTH = 1 << 10;

public:
  TestLogSQLServerProvider() : rs_leader_(),
                               rs_follower_1_(),
                               rs_follower_2_(),
                               service_pid_(0),
                               server_provider_()
  {}

  virtual void SetUp();
  virtual void TearDown();

  void set_rs_list(const ObRootAddrList &rs_list);

protected:
  ObRootAddr              rs_leader_;
  ObRootAddr              rs_follower_1_;
  ObRootAddr              rs_follower_2_;

  pid_t                   service_pid_;
  ObLogSQLServerProvider  server_provider_;

  char                    appname_[MAX_APPNAME_LENGTH];
  char                    config_url_[MAX_CONFIG_URL_LENGTH];
  char                    json_buffer_[MAX_JASON_BUFFER_SIZE];
};

void TestLogSQLServerProvider::SetUp()
{
  int ret = OB_SUCCESS;
  const char *config_url_arbitrary_str = "i_am_an_arbitrary_string/versin=1";
  const char *appname_str = "test";

  // Constructing the ConfigURL
  // Note that the URL is actually only accessible as "http::/IP:PORT", the subsequent string is an arbitrary string
  (void)snprintf(config_url_, sizeof(config_url_), "http://%s:%d/%s",
      CONFIG_SERVER_IP, CONFIG_SERVER_PORT, config_url_arbitrary_str);

  (void)snprintf(appname_, sizeof(appname_), "%s", appname_str);

  // Create configure server simulation process
  pid_t pid = fork();
  if (0 == pid) {
    // Set the child process to a new child process group to facilitate KILL
    ret = setpgid(pid, pid);
    if (ret < 0) {
      LOG_ERROR("setpgid failed", K(errno));
    } else if (-1 == (ret = execl(
        "/bin/bash", CONFIG_SERVER_PROGRAM, CONFIG_SERVER_PROGRAM, CONFIG_SERVER_PORT_STR, (char *)NULL))) {
      LOG_ERROR("execl failed", K(errno));
    }
    exit(1);
  } else if (-1 == pid) {
    LOG_ERROR("fork failed", K(errno));
  } else {
    LOG_INFO("create child", K(pid));
    service_pid_ = pid;

    // wait child process execute.
    usleep(100000);
  }

  // init rs addr list
  ObSEArray<ObRootAddr, 3> rs_list;

  rs_leader_.server_.set_ip_addr("10.210.170.11", 100);
  rs_leader_.role_ = LEADER;
  rs_leader_.sql_port_ = 2828;
  rs_list.push_back(rs_leader_);

  rs_follower_1_.server_.set_ip_addr("10.210.170.16", 200);
  rs_follower_1_.role_ = FOLLOWER;
  rs_follower_1_.sql_port_ = 3838;
  rs_list.push_back(rs_follower_1_);

  rs_follower_2_.server_.set_ip_addr("10.210.180.96", 300);
  rs_follower_2_.role_ = FOLLOWER;
  rs_follower_2_.sql_port_ = 4848;
  rs_list.push_back(rs_follower_2_);

  // Setting up the Rootserver list
  set_rs_list(rs_list);

  // init Server Provider
  ret = server_provider_.init(config_url_, appname_);
  ASSERT_EQ(OB_SUCCESS, ret);

  // init 3 rootserver
  ObAddr server;
  EXPECT_EQ(3, server_provider_.get_server_count());
  EXPECT_EQ(OB_SUCCESS, server_provider_.get_server(0, server));
  EXPECT_EQ(rs_leader_.server_.get_ipv4(), server.get_ipv4());
  EXPECT_EQ(rs_leader_.sql_port_, server.get_port());
  EXPECT_EQ(OB_SUCCESS, server_provider_.get_server(1, server));
  EXPECT_EQ(rs_follower_1_.server_.get_ipv4(), server.get_ipv4());
  EXPECT_EQ(rs_follower_1_.sql_port_, server.get_port());
  EXPECT_EQ(OB_SUCCESS, server_provider_.get_server(2, server));
  EXPECT_EQ(rs_follower_2_.server_.get_ipv4(), server.get_ipv4());
  EXPECT_EQ(rs_follower_2_.sql_port_, server.get_port());
  EXPECT_EQ(OB_ENTRY_NOT_EXIST, server_provider_.get_server(3, server));
}

void TestLogSQLServerProvider::TearDown()
{
  int status = 0;
  int64_t orig_server_count = server_provider_.get_server_count();

  // Sends SIGINT to all processes in the process group of the child process
  kill(-service_pid_, SIGINT);

  pid_t pid = wait(&status);
  LOG_INFO("child exit", K(pid));

  // Refresh error if Configure Server does not exist
  EXPECT_NE(OB_SUCCESS, server_provider_.refresh_server_list());

  // Refresh the error without modifying the previous Server list
  EXPECT_EQ(orig_server_count, server_provider_.get_server_count());
}

void TestLogSQLServerProvider::set_rs_list(const ObRootAddrList &rs_list)
{
  int ret = OB_SUCCESS;
  ObSqlString cmd;
  ObSqlString json;

  usleep(50000);
  const int64_t cluster_id = 100;
  ret = ObWebServiceRootAddr::to_json(rs_list, appname_, cluster_id, json);
  ASSERT_EQ(OB_SUCCESS, ret);
  LOG_INFO("to_json", K(json));

  ret = cmd.assign_fmt("echo -n 'POST / HTTP/1.1\r\nContent-Length: %ld\r\n%s' | nc %s %d &> /dev/null",
      json.length(), json.ptr(), CONFIG_SERVER_IP, CONFIG_SERVER_PORT);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = system(cmd.ptr());
  usleep(50000);
}

TEST_F(TestLogSQLServerProvider, fetch)
{
  ObAddr server;
  ObSEArray<ObRootAddr, 16> rs_list;

  // Test zero RS
  rs_list.reuse();
  set_rs_list(rs_list); // Set up a new RS list
  ASSERT_EQ(OB_SUCCESS, server_provider_.refresh_server_list()); // Refresh RS list
  EXPECT_EQ(0, server_provider_.get_server_count());
  EXPECT_EQ(OB_ENTRY_NOT_EXIST, server_provider_.get_server(0, server));

  // Test one RS
  rs_list.reuse();
  rs_list.push_back(rs_leader_);
  set_rs_list(rs_list); // Set up a new RS list
  ASSERT_EQ(OB_SUCCESS, server_provider_.refresh_server_list()); // Refresh RS list
  EXPECT_EQ(1, server_provider_.get_server_count());
  EXPECT_EQ(OB_SUCCESS, server_provider_.get_server(0, server));
  EXPECT_EQ(rs_leader_.server_.get_ipv4(), server.get_ipv4());
  EXPECT_EQ(rs_leader_.sql_port_, server.get_port());     // Server的端口应该是SQL端口

  EXPECT_EQ(OB_ENTRY_NOT_EXIST, server_provider_.get_server(1, server));
  EXPECT_EQ(OB_INVALID_ARGUMENT, server_provider_.get_server(-1, server));

  // Test two RS
  rs_list.reuse();
  rs_list.push_back(rs_leader_);
  rs_list.push_back(rs_follower_1_);
  set_rs_list(rs_list); // Set up a new RS list
  ASSERT_EQ(OB_SUCCESS, server_provider_.refresh_server_list()); // Refresh RS list
  EXPECT_EQ(2, server_provider_.get_server_count());
  EXPECT_EQ(OB_SUCCESS, server_provider_.get_server(0, server));
  EXPECT_EQ(rs_leader_.server_.get_ipv4(), server.get_ipv4());
  EXPECT_EQ(rs_leader_.sql_port_, server.get_port());
  EXPECT_EQ(OB_SUCCESS, server_provider_.get_server(1, server));
  EXPECT_EQ(rs_follower_1_.server_.get_ipv4(), server.get_ipv4());
  EXPECT_EQ(rs_follower_1_.sql_port_, server.get_port());
  EXPECT_EQ(OB_ENTRY_NOT_EXIST, server_provider_.get_server(2, server));

  // Test three RS
  rs_list.reuse();
  rs_list.push_back(rs_leader_);
  rs_list.push_back(rs_follower_1_);
  rs_list.push_back(rs_follower_2_);
  set_rs_list(rs_list); // Set up a new RS list
  ASSERT_EQ(OB_SUCCESS, server_provider_.refresh_server_list()); // Refresh RS list
  EXPECT_EQ(3, server_provider_.get_server_count());
  EXPECT_EQ(OB_SUCCESS, server_provider_.get_server(0, server));
  EXPECT_EQ(rs_leader_.server_.get_ipv4(), server.get_ipv4());
  EXPECT_EQ(rs_leader_.sql_port_, server.get_port());
  EXPECT_EQ(OB_SUCCESS, server_provider_.get_server(1, server));
  EXPECT_EQ(rs_follower_1_.server_.get_ipv4(), server.get_ipv4());
  EXPECT_EQ(rs_follower_1_.sql_port_, server.get_port());
  EXPECT_EQ(OB_SUCCESS, server_provider_.get_server(2, server));
  EXPECT_EQ(rs_follower_2_.server_.get_ipv4(), server.get_ipv4());
  EXPECT_EQ(rs_follower_2_.sql_port_, server.get_port());
  EXPECT_EQ(OB_ENTRY_NOT_EXIST, server_provider_.get_server(3, server));
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
