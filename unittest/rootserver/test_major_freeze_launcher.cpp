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

#define USING_LOG_PREFIX RS

#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include "lib/stat/ob_session_stat.h"
#define private public
#include "rootserver/ob_major_freeze_launcher.h"
#undef private
#include "../share/schema/db_initializer.h"
#include "rpc/mock_ob_common_rpc_proxy.h"
#include "lib/container/ob_array_iterator.h"
#include "rootserver/ob_freeze_info_manager.h"

using ::testing::_;
using ::testing::Assign;
using ::testing::Return;
using ::testing::SetArgReferee;

namespace oceanbase
{
using namespace common;
using namespace obrpc;
using namespace share;
using namespace share::schema;
namespace rootserver
{
class TestMajorFreezeLauncher : public ::testing::Test
{
public:
  TestMajorFreezeLauncher()
  {
    self_addr_.set_ip_addr("127.0.0.1", 8888);
  }
protected:
  DBInitializer db_initer_;
  MockObCommonRpcProxy common_proxy_;
  common::ObAddr self_addr_;
};

TEST_F(TestMajorFreezeLauncher, basic)
{
  ObServerConfig &server_config = db_initer_.get_config();
  time_t cur_time;
  time(&cur_time);
  struct tm human_time;
  struct tm *time_ptr = NULL;
  time_ptr = localtime_r(&cur_time, &human_time);
  if (time_ptr->tm_sec > 50) {
    usleep(10 * 1000 * 1000);
    time(&cur_time);
    time_ptr = localtime_r(&cur_time, &human_time);
  }
  char time_str[50];
  strftime(time_str, 50,  "%H:%M", &human_time);
  server_config.major_freeze_duty_time.set(time_str);

  ObFreezeInfoManager freeze_info_manager_;
  ObMajorFreezeLauncher launcher;
  ASSERT_EQ(OB_SUCCESS, launcher.init(common_proxy_, server_config, self_addr_, freeze_info_manager_));
  int64_t major_freeze_count = 0;
  ON_CALL(common_proxy_, get_frozen_version(_,_))
      .WillByDefault(DoAll(SetArgReferee<0>(1), Return(OB_SUCCESS)));
  ON_CALL(common_proxy_, root_major_freeze(_,_))
      .WillByDefault(DoAll(Assign(&major_freeze_count, major_freeze_count + 1),
                           Return(OB_SUCCESS)));
  for (int64_t i = 0; i < 10; ++i) {
    ASSERT_EQ(OB_SUCCESS, launcher.try_launch_major_freeze());
  }
  ASSERT_EQ(1, major_freeze_count);

  ObMajorFreezeLauncher launcher1;
  ASSERT_EQ(OB_SUCCESS, launcher1.init(common_proxy_, server_config, self_addr_, freeze_info_manager_));
  launcher1.start();
  major_freeze_count = 0;
  // run one minute
  usleep(60 * 1000 * 1000);
  launcher1.stop();
  launcher1.wait();
  ASSERT_EQ(1, major_freeze_count);
}
}//end namespace rootserver
}//end namespace oceanbase

int main(int argc, char **argv)
{
  OB_LOGGER.set_log_level("WARN");
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
