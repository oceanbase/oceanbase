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
#include <gmock/gmock.h>
#include "share/ob_alive_server_tracer.h"
#include "rpc/mock_ob_common_rpc_proxy.h"
#include "partition_table/fake_part_property_getter.h"
#include "lib/container/ob_array_iterator.h"

using ::testing::_;
using ::testing::Invoke;
namespace oceanbase
{
namespace share
{
using namespace common;
using namespace host;
using namespace obrpc;

TEST(TestAliveServerMap, all)
{
  ObArray<ObAddr> server_list;
  ObAliveServerMap server_map;
  bool alive = false;
  int64_t trace_time = 0;
  ASSERT_EQ(OB_NOT_INIT, server_map.is_alive(A, alive, trace_time));
  ASSERT_EQ(OB_NOT_INIT, server_map.refresh(server_list));

  ASSERT_EQ(OB_SUCCESS, server_map.init());
  // before refresh, all alive
  ASSERT_EQ(OB_SUCCESS, server_map.is_alive(A, alive, trace_time));
  ASSERT_TRUE(alive);
  ASSERT_EQ(0, trace_time);
  ASSERT_EQ(OB_SUCCESS, server_map.is_alive(D, alive, trace_time));
  ASSERT_TRUE(alive);
  ASSERT_EQ(0, trace_time);

  int64_t t = ObTimeUtility::current_time();
  usleep(1);

  // refreshed A, B, C
  ASSERT_EQ(OB_SUCCESS, server_list.push_back(A));
  ASSERT_EQ(OB_SUCCESS, server_list.push_back(B));
  ASSERT_EQ(OB_SUCCESS, server_list.push_back(C));

  ASSERT_EQ(OB_SUCCESS, server_map.refresh(server_list));
  ASSERT_EQ(OB_SUCCESS, server_map.is_alive(A, alive, trace_time));
  ASSERT_TRUE(alive);
  ASSERT_GT(trace_time, t);
  ASSERT_EQ(OB_SUCCESS, server_map.is_alive(D, alive, trace_time));
  ASSERT_FALSE(alive);
  ASSERT_GT(trace_time, t);

  t = ObTimeUtility::current_time();
  usleep(1);

  // refreshed B, C, D
  server_list.reuse();
  ASSERT_EQ(OB_SUCCESS, server_list.push_back(D));
  ASSERT_EQ(OB_SUCCESS, server_list.push_back(B));
  ASSERT_EQ(OB_SUCCESS, server_list.push_back(C));

  ASSERT_EQ(OB_SUCCESS, server_map.refresh(server_list));
  ASSERT_EQ(OB_SUCCESS, server_map.is_alive(A, alive, trace_time));
  ASSERT_FALSE(alive);
  ASSERT_GT(trace_time, t);
  ASSERT_EQ(OB_SUCCESS, server_map.is_alive(D, alive, trace_time));
  ASSERT_TRUE(alive);
  ASSERT_GT(trace_time, t);
}

class ServerList
{
public:
  int fetch_alive_server(const ObFetchAliveServerArg &, ObFetchAliveServerResult &res,
      const ObRpcOpts &)
  {
    return res.server_list_.assign(server_list_);
  }
  ObArray<ObAddr> server_list_;
};

TEST(TestAliveServerTracer, all)
{
  ObTimer timer;
  ASSERT_EQ(OB_SUCCESS, timer.init());
  obrpc::MockObCommonRpcProxy rpc_proxy;
  ServerList server_list;

  ObAliveServerTracer tracer;
  bool alive = false;
  int64_t trace_time = 0;
  ASSERT_EQ(OB_NOT_INIT, tracer.is_alive(A, alive, trace_time));
  ASSERT_EQ(OB_NOT_INIT, tracer.refresh());

  ON_CALL(rpc_proxy, fetch_alive_server(_, _, _))
      .WillByDefault(Invoke(&server_list, &ServerList::fetch_alive_server));

  ASSERT_EQ(OB_SUCCESS, tracer.init(timer, rpc_proxy));
  // empty server list, refresh fail
  ASSERT_NE(OB_SUCCESS, tracer.refresh());

  ASSERT_EQ(OB_SUCCESS, tracer.is_alive(A, alive, trace_time));
  ASSERT_TRUE(alive);

  // refreshed A, B
  ASSERT_EQ(OB_SUCCESS, server_list.server_list_.push_back(A));
  ASSERT_EQ(OB_SUCCESS, server_list.server_list_.push_back(B));

  usleep(ObAliveServerRefreshTask::REFRESH_INTERVAL_US * 3 / 2);
  ASSERT_EQ(OB_SUCCESS, tracer.is_alive(A, alive, trace_time));
  ASSERT_TRUE(alive);
  ASSERT_EQ(OB_SUCCESS, tracer.is_alive(C, alive, trace_time));
  ASSERT_FALSE(alive);
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
