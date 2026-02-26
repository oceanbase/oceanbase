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

#define USING_LOG_PREFIX COMMON
#include <gtest/gtest.h>

#include "src/share/ob_define.h"
#include "deps/oblib/src/lib/container/ob_se_array.h"
#include "src/sql/engine/basic/ob_consistent_hashing_load_balancer.h"

#define private public

using namespace std;
namespace oceanbase
{
namespace sql
{
class ObTestConsistentHashingLoadBalancer : public ::testing::Test
{
public:
  ObTestConsistentHashingLoadBalancer()
  {}
  ~ObTestConsistentHashingLoadBalancer()
  {}
  virtual void SetUp()
  {}
  virtual void TearDown()
  {}

private:
  DISALLOW_COPY_AND_ASSIGN(ObTestConsistentHashingLoadBalancer);
};

TEST(ObTestConsistentHashingLoadBalancer, select_server)
{
  int ret = OB_SUCCESS;
  int64_t PORT = 5010;
  ObDefaultLoadBalancer load_balancer;
  common::ObSEArray<ObAddr, 8> target_servers;
  common::ObSEArray<ObString, 32> files;
  OZ (target_servers.push_back(ObAddr(ObAddr::IPV4, "127.0.0.1", PORT)));
  OZ (target_servers.push_back(ObAddr(ObAddr::IPV4, "127.0.0.2", PORT)));
  OZ (target_servers.push_back(ObAddr(ObAddr::IPV4, "127.0.0.3", PORT)));

  OZ (files.push_back(ObString(53, "part-00000-2c66b58b-7753-45e7-ab1c-8703fc929481-c000")));
  OZ (files.push_back(ObString(53, "part-00001-2c66b58b-7753-45e7-ab1c-8703fc929481-c000")));
  OZ (files.push_back(ObString(53, "part-00002-2c66b58b-7753-45e7-ab1c-8703fc929481-c000")));
  OZ (files.push_back(ObString(53, "part-00003-2c66b58b-7753-45e7-ab1c-8703fc929481-c000")));
  OZ (files.push_back(ObString(53, "part-00004-2c66b58b-7753-45e7-ab1c-8703fc929481-c000")));
  OZ (files.push_back(ObString(53, "part-00005-2c66b58b-7753-45e7-ab1c-8703fc929481-c000")));
  OZ (files.push_back(ObString(53, "part-00006-2c66b58b-7753-45e7-ab1c-8703fc929481-c000")));
  OZ (files.push_back(ObString(53, "part-00007-2c66b58b-7753-45e7-ab1c-8703fc929481-c000")));
  OZ (files.push_back(ObString(53, "part-00008-2c66b58b-7753-45e7-ab1c-8703fc929481-c000")));
  OZ (files.push_back(ObString(53, "part-00009-2c66b58b-7753-45e7-ab1c-8703fc929481-c000")));
  OZ (files.push_back(ObString(53, "part-00010-2c66b58b-7753-45e7-ab1c-8703fc929481-c000")));
  OZ (files.push_back(ObString(53, "part-00011-2c66b58b-7753-45e7-ab1c-8703fc929481-c000")));
  OZ (files.push_back(ObString(53, "part-00012-2c66b58b-7753-45e7-ab1c-8703fc929481-c000")));
  OZ (files.push_back(ObString(53, "part-00013-2c66b58b-7753-45e7-ab1c-8703fc929481-c000")));
  OZ (files.push_back(ObString(53, "part-00014-2c66b58b-7753-45e7-ab1c-8703fc929481-c000")));
  OZ (files.push_back(ObString(53, "part-00015-2c66b58b-7753-45e7-ab1c-8703fc929481-c000")));
  OZ (files.push_back(ObString(53, "part-00016-2c66b58b-7753-45e7-ab1c-8703fc929481-c000")));
  OZ (files.push_back(ObString(53, "part-00017-2c66b58b-7753-45e7-ab1c-8703fc929481-c000")));
  OZ (files.push_back(ObString(53, "part-00018-2c66b58b-7753-45e7-ab1c-8703fc929481-c000")));
  OZ (files.push_back(ObString(53, "part-00019-2c66b58b-7753-45e7-ab1c-8703fc929481-c000")));
  OZ (files.push_back(ObString(53, "part-00020-2c66b58b-7753-45e7-ab1c-8703fc929481-c000")));

  OZ (load_balancer.add_server_list(target_servers));
  for (int64_t i = 0; OB_SUCC(ret) && i < files.count(); ++i) {
    ObAddr addr;
    if (OB_FAIL(load_balancer.select_server(files.at(i), addr))) {
      LOG_WARN("failed to select server", K(ret));
    }
  }
}

TEST(ObTestConsistentHashingLoadBalancer, remove_server)
{
  int ret = OB_SUCCESS;
  int64_t PORT = 5010;
  ObDefaultLoadBalancer load_balancer;
  common::ObSEArray<ObAddr, 8> target_servers;
  common::ObSEArray<ObString, 32> files;
  OZ (target_servers.push_back(ObAddr(ObAddr::IPV4, "127.0.0.1", PORT)));
  OZ (target_servers.push_back(ObAddr(ObAddr::IPV4, "127.0.0.2", PORT)));

  OZ (files.push_back(ObString(53, "part-00000-2c66b58b-7753-45e7-ab1c-8703fc929481-c000")));
  OZ (files.push_back(ObString(53, "part-00001-2c66b58b-7753-45e7-ab1c-8703fc929481-c000")));
  OZ (files.push_back(ObString(53, "part-00002-2c66b58b-7753-45e7-ab1c-8703fc929481-c000")));
  OZ (files.push_back(ObString(53, "part-00003-2c66b58b-7753-45e7-ab1c-8703fc929481-c000")));
  OZ (files.push_back(ObString(53, "part-00004-2c66b58b-7753-45e7-ab1c-8703fc929481-c000")));
  OZ (files.push_back(ObString(53, "part-00005-2c66b58b-7753-45e7-ab1c-8703fc929481-c000")));
  OZ (files.push_back(ObString(53, "part-00006-2c66b58b-7753-45e7-ab1c-8703fc929481-c000")));
  OZ (files.push_back(ObString(53, "part-00007-2c66b58b-7753-45e7-ab1c-8703fc929481-c000")));
  OZ (files.push_back(ObString(53, "part-00008-2c66b58b-7753-45e7-ab1c-8703fc929481-c000")));
  OZ (files.push_back(ObString(53, "part-00009-2c66b58b-7753-45e7-ab1c-8703fc929481-c000")));
  OZ (files.push_back(ObString(53, "part-00010-2c66b58b-7753-45e7-ab1c-8703fc929481-c000")));
  OZ (files.push_back(ObString(53, "part-00011-2c66b58b-7753-45e7-ab1c-8703fc929481-c000")));
  OZ (files.push_back(ObString(53, "part-00012-2c66b58b-7753-45e7-ab1c-8703fc929481-c000")));
  OZ (files.push_back(ObString(53, "part-00013-2c66b58b-7753-45e7-ab1c-8703fc929481-c000")));
  OZ (files.push_back(ObString(53, "part-00014-2c66b58b-7753-45e7-ab1c-8703fc929481-c000")));
  OZ (files.push_back(ObString(53, "part-00015-2c66b58b-7753-45e7-ab1c-8703fc929481-c000")));
  OZ (files.push_back(ObString(53, "part-00016-2c66b58b-7753-45e7-ab1c-8703fc929481-c000")));
  OZ (files.push_back(ObString(53, "part-00017-2c66b58b-7753-45e7-ab1c-8703fc929481-c000")));
  OZ (files.push_back(ObString(53, "part-00018-2c66b58b-7753-45e7-ab1c-8703fc929481-c000")));
  OZ (files.push_back(ObString(53, "part-00019-2c66b58b-7753-45e7-ab1c-8703fc929481-c000")));
  OZ (files.push_back(ObString(53, "part-00020-2c66b58b-7753-45e7-ab1c-8703fc929481-c000")));

  OZ (load_balancer.add_server_list(target_servers));
  LOG_INFO("remove server......");
  for (int64_t i = 0; OB_SUCC(ret) && i < files.count(); ++i) {
    ObAddr addr;
    if (OB_FAIL(load_balancer.select_server(files.at(i), addr))) {
      LOG_WARN("failed to select server", K(ret));
    }
  }
}

}
}

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  OB_LOGGER.set_log_level("INFO");
  int ret = RUN_ALL_TESTS();
  return ret;
}
