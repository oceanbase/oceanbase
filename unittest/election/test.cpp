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

#include <gtest/gtest.h>
#include "election/ob_election_server.h"
#include "election/ob_election_client.h"
#include "election/ob_election_base.h"
#include "election_test_env.h"
#include "election_tester.h"

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::election;
using namespace oceanbase::tests;
using namespace oceanbase::tests::election;

#define TEST_CASE_NAME Test
#define MAKE_STR(name) #name
#define TEST_CASE_NAME_STR MAKE_STR(Test)

#define TEST_LOG "./test.log"

static const char* dev_name = "bond0";
static const int timeout = 300000; /*us*/

class TEST_CASE_NAME : public ::testing::Test {
public:
  virtual void SetUp()
  {}
  virtual void TearDown()
  {}

  int routine(int loop);

protected:
  ObServer self_;
};

TEST_F(TEST_CASE_NAME, test_election_interface)
{

  ObElectionTesterCluster cluster;
  ObElectionTester t1, t2, t3;

  t1.set_candidate().add_to(cluster);
  t2.set_candidate().add_to(cluster);
  t3.set_candidate().add_to(cluster);

  cluster.init(dev_name).start();
  msleep(2 * T_CYCLE + T_ELECT2);

  printf("%p\n", cluster.get_one_slave());
  printf("%p\n", cluster.get_one_slave());
  printf("%p\n", cluster.get_one_slave());
  printf("%p\n", cluster.get_one_slave());
  printf("%p\n", cluster.get_one_slave());
  printf("%p\n", cluster.get_one_slave());
  printf("%p\n", cluster.get_one_slave());
  printf("%p\n", cluster.get_one_slave());
}

int main(int argc, char** argv)
{
  oceanbase::tests::election::ElectionTestEnv env;
  env.init(TEST_CASE_NAME_STR);
  // init async log
  ASYNC_LOG_INIT(TEST_LOG, INFO, true);
  // ASYNC_LOG_SET_LEVEL(INFO);
  // close TBSYS_LOG
  TBSYS_LOGGER.setFileName("test_log.log", true, false);

  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
