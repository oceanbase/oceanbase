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
  void start_new_shell();

  void set_response_json(const char *json);
  void set_response_json2(const char *json);

protected:
  pid_t service_pid_;
  schema::DBInitializer initer_;
  schema::DBInitializer initer2_;
  ObWebServiceRootAddr ws_;
};

void TestWebServiceRootAddr::start_new_shell()
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
void TestWebServiceRootAddr::SetUp()
{
  initer_.get_config().obconfig_url.set_value("http://127.0.0.1:8657/obconfig/region1");
  initer_.get_config().cluster_id.set_value("1");
  int ret = ws_.init(initer_.get_config());
  ASSERT_EQ(OB_SUCCESS, ret);

  pid_t pid = fork();
  if (0 == pid) {
    ret = setpgid(pid, pid);
    if (ret < 0) {
      LOG_ERROR("setpgid failed", K(errno));
    } else if (-1 == (ret = execl(
        "/bin/bash", "./fake_ob_config-sh", "./fake_ob_config-sh", "8657", NULL))) {
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

void TestWebServiceRootAddr::set_response_json2(const char *json)
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

void TestWebServiceRootAddr::set_response_json(const char *json)
{
  usleep(50000);
  ObSqlString cmd;
  int ret = cmd.assign_fmt("echo -n 'POST / HTTP/1.1\r\nContent-Length: %ld\r\n%s' | nc 127.0.0.1 8657",
      strlen(json), json);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = system(cmd.ptr());
  LOG_INFO("set response json", K(cmd), K(ret), K(errno), K(json));
  usleep(50000);
}

TEST_F(TestWebServiceRootAddr, store)
{
  ObSEArray<ObRootAddr, 16> rs_list;
  ObSEArray<ObRootAddr, 16> readonly_rs_list;
  ObRootAddr rs;
  rs.server_.set_ip_addr("127.0.0.1", 9988);
  rs.sql_port_ = 1;
  rs_list.push_back(rs);
  rs.server_.set_ip_addr("127.0.0.2", 9988);
  rs.role_ = LEADER;
  rs.sql_port_ = 1111;
  rs_list.push_back(rs);
  ObRootAddr readonly_rs;
  readonly_rs.server_.set_ip_addr("127.0.0.3", 9988);
  readonly_rs.sql_port_ = 1;
  readonly_rs_list.push_back(readonly_rs);
  readonly_rs.server_.set_ip_addr("127.0.0.4", 9988);
  readonly_rs.sql_port_ = 1111;
  readonly_rs_list.push_back(readonly_rs);
  int ret = ws_.store(rs_list, readonly_rs_list, true, LEADER);

  usleep(100000);

  rs_list.reset();
  readonly_rs_list.reset();
  ret = ws_.fetch(rs_list, readonly_rs_list);
  ASSERT_EQ(OB_SUCCESS, ret);

  ASSERT_EQ(2, rs_list.count());
  ObAddr rs1(ObAddr::IPV4, "127.0.0.1", 9988);
  LOG_WARN("get server", K(rs1), K_(rs_list.at(0).server));
  ASSERT_EQ(rs1, rs_list.at(0).server_);
  ASSERT_EQ(FOLLOWER, rs_list.at(0).role_);
  ASSERT_EQ(1, rs_list.at(0).sql_port_);
  ObAddr rs2(ObAddr::IPV4, "127.0.0.2", 9988);
  ASSERT_EQ(rs2, rs_list.at(1).server_);
  ASSERT_EQ(LEADER, rs_list.at(1).role_);
  ASSERT_EQ(1111, rs_list.at(1).sql_port_);

  ASSERT_EQ(2, readonly_rs_list.count());
  ObAddr rs3(ObAddr::IPV4, "127.0.0.3", 9988);
  ASSERT_EQ(rs3, readonly_rs_list.at(0).server_);
  ASSERT_EQ(FOLLOWER, readonly_rs_list.at(0).role_);
  ASSERT_EQ(1, readonly_rs_list.at(0).sql_port_);
  ObAddr rs4(ObAddr::IPV4, "127.0.0.4", 9988);
  ASSERT_EQ(rs4, readonly_rs_list.at(1).server_);
  ASSERT_EQ(FOLLOWER, readonly_rs_list.at(1).role_);
  ASSERT_EQ(1111, readonly_rs_list.at(1).sql_port_);
}

TEST_F(TestWebServiceRootAddr, fetch)
{
  ObSEArray<ObRootAddr, 16> rs_list;
  ObSEArray<ObRootAddr, 16> readonly_rs_list;
  set_response_json("{\"RsList\":[{\"address\":\"127.0.0.1:99\",\"role\":\"FOLLOWER\",\"sql_port\": 1234},{\"sql_port\": 1234,\"address\":\"127.0.0.2:99\",\"role\":\"LEADER\"}]}");
  int ret = ws_.fetch(rs_list, readonly_rs_list);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(2, rs_list.count());
  ASSERT_EQ(0, readonly_rs_list.count());
  ASSERT_EQ(1234, rs_list[0].sql_port_);

  set_response_json("{\"RsList\":[{\"address\":\"127.0.0.1:99\",\"role\":\"FOLLOWER\", \"sql_port\": 1234},{\"sql_port\": 1234,\"address\":\"127.0.0.2:99\",\"role\":\"LEADER\"},{\"sql_port\": 1234,\"address\":\"127.0.0.3:99\",\"role\":\"FOLLOWER\"}]}");
  ret = ws_.fetch(rs_list, readonly_rs_list);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(3, rs_list.count());
  ASSERT_EQ(0, readonly_rs_list.count());

  set_response_json("{}");
  ret = ws_.fetch(rs_list, readonly_rs_list);
  ASSERT_NE(OB_SUCCESS, ret);

  set_response_json("{\"RsList\":[]}");
  ret = ws_.fetch(rs_list, readonly_rs_list);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(0, rs_list.count());
  ASSERT_EQ(0, readonly_rs_list.count());

  set_response_json("{\"RsList\":[{\"address_NO\":\"127.0.0.1:99\",\"role_NO\":\"FOLLOWER\"}]}");
  ret = ws_.fetch(rs_list, readonly_rs_list);
  ASSERT_NE(OB_SUCCESS, ret);

  set_response_json("{\"RsList\":{}}");
  ret = ws_.fetch(rs_list, readonly_rs_list);
  ASSERT_NE(OB_SUCCESS, ret);

  set_response_json("{\"RsList\":[{\"address\":\"127.0.0.1:99\",\"role\":\"FOLLOWER\",\"sql_port\":\"abc\"}]}");
  ret = ws_.fetch(rs_list, readonly_rs_list);
  ASSERT_NE(OB_SUCCESS, ret);

  set_response_json("{\"RsList\":[{\"address\":123,\"role\":\"FOLLOWER\",\"sql_port\":88}]}");
  ret = ws_.fetch(rs_list, readonly_rs_list);
  ASSERT_NE(OB_SUCCESS, ret);

  set_response_json("{\"RsList\":[{\"address\":\"127.0.0.1:99\",\"role\":0,\"sql_port\":88}]}");
  ret = ws_.fetch(rs_list, readonly_rs_list);
  ASSERT_NE(OB_SUCCESS, ret);

  //check readonly_rs_list
  set_response_json("{\"RsList\":[{\"address\":\"127.0.0.1:99\",\"role\":\"FOLLOWER\", \"sql_port\": 1234},{\"sql_port\": 1234,\"address\":\"127.0.0.2:99\",\"role\":\"LEADER\"},{\"sql_port\": 1234,\"address\":\"127.0.0.3:99\",\"role\":\"FOLLOWER\"}], \"ReadonlyRsList\":[{\"address\":\"127.0.0.4:99\",\"role\":\"FOLLOWER\", \"sql_port\": 1234},{\"sql_port\": 1234,\"address\":\"127.0.0.5:99\",\"role\":\"FOLLOWER\"}]}");
  ret = ws_.fetch(rs_list, readonly_rs_list);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(3, rs_list.count());
  ASSERT_EQ(2, readonly_rs_list.count());

  set_response_json("{\"RsList\":[{\"address\":\"127.0.0.1:99\",\"role\":\"FOLLOWER\", \"sql_port\": 1234},{\"sql_port\": 1234,\"address\":\"127.0.0.2:99\",\"role\":\"LEADER\"},{\"sql_port\": 1234,\"address\":\"127.0.0.3:99\",\"role\":\"FOLLOWER\"}], \"ReadonlyRsList\":[{\"address\":\"127.0.0.4:99\",\"role\":\"FOLLOWER\", \"sql_port\": 1234}]}");
  ret = ws_.fetch(rs_list, readonly_rs_list);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(3, rs_list.count());
  ASSERT_EQ(1, readonly_rs_list.count());

  set_response_json("{\"RsList\":[{\"address\":\"127.0.0.1:99\",\"role\":\"FOLLOWER\", \"sql_port\": 1234},{\"sql_port\": 1234,\"address\":\"127.0.0.2:99\",\"role\":\"LEADER\"},{\"sql_port\": 1234,\"address\":\"127.0.0.3:99\",\"role\":\"FOLLOWER\"}], \"ReadonlyRsList\":[]}");
  ret = ws_.fetch(rs_list, readonly_rs_list);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(3, rs_list.count());
  ASSERT_EQ(0, readonly_rs_list.count());

  set_response_json("{\"RsList\":[], \"ReadonlyRsList\":[{\"address\":\"127.0.0.4:99\",\"role\":\"FOLLOWER\", \"sql_port\": 1234}]}");
  ret = ws_.fetch(rs_list, readonly_rs_list);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(0, rs_list.count());
  ASSERT_EQ(1, readonly_rs_list.count());

  set_response_json("{\"ReadonlyRsList\":[{\"address\":\"127.0.0.4:99\",\"role\":\"FOLLOWER\", \"sql_port\": 1234}]}");
  ret = ws_.fetch(rs_list, readonly_rs_list);
  ASSERT_NE(OB_SUCCESS, ret);

  set_response_json("{\"RsList\":[{\"address\":123,\"role\":\"FOLLOWER\", \"sql_port\": 1234}], \"ReadonlyRsList\":[{\"address\":\"127.0.0.4:99\",\"role\":\"FOLLOWER\", \"sql_port\": 1234}]}");
  ret = ws_.fetch(rs_list, readonly_rs_list);
  ASSERT_NE(OB_SUCCESS, ret);

  set_response_json("{\"RsList\":[{\"address\":\"127.0.0.1:99\",\"role_NO\":\"FOLLOWER\", \"sql_port\": 1234}], \"ReadonlyRsList\":[{\"address\":\"127.0.0.4:99\",\"role\":\"FOLLOWER\", \"sql_port\": 1234}]}");
  ret = ws_.fetch(rs_list, readonly_rs_list);
  ASSERT_NE(OB_SUCCESS, ret);

  set_response_json("{\"RsList\":[{\"address\":\"127.0.0.1:99\",\"role\":\"FOLLOWER\", \"sql_port\": 1234},{\"sql_port\": 1234,\"address\":\"127.0.0.2:99\",\"role\":\"LEADER\"},{\"sql_port\": 1234,\"address\":\"127.0.0.3:99\",\"role\":\"FOLLOWER\"}], \"ReadonlyRsList\":{}}");
  ret = ws_.fetch(rs_list, readonly_rs_list);
  ASSERT_NE(OB_SUCCESS, ret);

  set_response_json("{\"RsList\":[{\"address\":\"127.0.0.1:99\",\"role\":\"FOLLOWER\", \"sql_port\": 1234},{\"sql_port\": 1234,\"address\":\"127.0.0.2:99\",\"role\":\"LEADER\"},{\"sql_port\": 1234,\"address\":\"127.0.0.3:99\",\"role\":\"FOLLOWER\"}], \"ReadonlyRsList\":[{\"address_NO\":\"127.0.0.4:99\",\"role\":\"FOLLOWER\", \"sql_port\": 1234}]}");
  ret = ws_.fetch(rs_list, readonly_rs_list);
  ASSERT_NE(OB_SUCCESS, ret);

  set_response_json("{\"RsList\":[{\"address\":\"127.0.0.1:99\",\"role\":\"FOLLOWER\", \"sql_port\": 1234},{\"sql_port\": 1234,\"address\":\"127.0.0.2:99\",\"role\":\"LEADER\"},{\"sql_port\": 1234,\"address\":\"127.0.0.3:99\",\"role\":\"FOLLOWER\"}], \"ReadonlyRsList\":[{\"address\":\"127.0.0.4:99\",\"role_NO\":\"FOLLOWER\", \"sql_port\": 1234}]}");
  ret = ws_.fetch(rs_list, readonly_rs_list);
  ASSERT_NE(OB_SUCCESS, ret);

  set_response_json("{\"RsList\":[{\"address\":\"127.0.0.1:99\",\"role\":\"FOLLOWER\", \"sql_port\": 1234},{\"sql_port\": 1234,\"address\":\"127.0.0.2:99\",\"role\":\"LEADER\"},{\"sql_port\": 1234,\"address\":\"127.0.0.3:99\",\"role\":\"FOLLOWER\"}], \"ReadonlyRsList\":[{\"address\":\"127.0.0.4:99\",\"role\":\"FOLLOWER\", \"sql_port_NO\": 1234}]}");
  ret = ws_.fetch(rs_list, readonly_rs_list);
  ASSERT_NE(OB_SUCCESS, ret);

  set_response_json("{\"RsList\":[{\"address\":\"127.0.0.1:99\",\"role\":\"FOLLOWER\", \"sql_port\": 1234},{\"sql_port\": 1234,\"address\":\"127.0.0.2:99\",\"role\":\"LEADER\"},{\"sql_port\": 1234,\"address\":\"127.0.0.3:99\",\"role\":\"FOLLOWER\"}], \"ReadonlyRsList\":[{\"address\":123,\"role\":\"FOLLOWER\", \"sql_port\": 1234}]}");
  ret = ws_.fetch(rs_list, readonly_rs_list);
  ASSERT_NE(OB_SUCCESS, ret);

  set_response_json("{\"RsList\":[{\"address\":\"127.0.0.1:99\",\"role\":\"FOLLOWER\", \"sql_port\": 1234},{\"sql_port\": 1234,\"address\":\"127.0.0.2:99\",\"role\":\"LEADER\"},{\"sql_port\": 1234,\"address\":\"127.0.0.3:99\",\"role\":\"FOLLOWER\"}], \"ReadonlyRsList\":[{\"address\":\"127.0.0.4:99\",\"role\":0, \"sql_port\": 1234}]}");
  ret = ws_.fetch(rs_list, readonly_rs_list);
  ASSERT_NE(OB_SUCCESS, ret);

  set_response_json("{\"RsList\":[{\"address\":\"127.0.0.1:99\",\"role\":\"FOLLOWER\", \"sql_port\": 1234},{\"sql_port\": 1234,\"address\":\"127.0.0.2:99\",\"role\":\"LEADER\"},{\"sql_port\": 1234,\"address\":\"127.0.0.3:99\",\"role\":\"FOLLOWER\"}], \"ReadonlyRsList\":[{\"address\":\"127.0.0.4:99\",\"role\":\"FOLLOWER\", \"sql_port\": \"abc\"}]}");
  ret = ws_.fetch(rs_list, readonly_rs_list);
  ASSERT_NE(OB_SUCCESS, ret);
}

TEST_F(TestWebServiceRootAddr, invalid)
{
  ObWebServiceRootAddr ws;
  ObSEArray<ObRootAddr, 16> rs;
  ObSEArray<ObRootAddr, 16> readonly_rs;
  ASSERT_NE(OB_SUCCESS, ws.fetch(rs, readonly_rs));
  ASSERT_NE(OB_SUCCESS, ws.store(rs, readonly_rs, true, LEADER));

  ASSERT_EQ(OB_SUCCESS, ws.init(initer_.get_config()));
  initer_.get_config().obconfig_url.set_value("http://127.127.127.127:1/obconfig/region1");
  ASSERT_NE(OB_SUCCESS, ws.fetch(rs, readonly_rs));
  ASSERT_NE(OB_SUCCESS, ws.store(rs, readonly_rs, true, LEADER));

  ObRootAddr ra;
  ra.server_.set_ip_addr("127.0.0.1", 9988);
  rs.push_back(ra);
  ASSERT_NE(OB_SUCCESS, ws.store(rs, readonly_rs, true, LEADER));
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
