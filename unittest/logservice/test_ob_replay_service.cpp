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
#include <iostream>
#include "lib/time/ob_time_utility.h"
#include "logservice/replayservice/ob_replay_status.h"
#include "logservice/replayservice/ob_log_replay_service.h"
#include "logservice/ob_log_service.h"
#include "mock_palf.h"
#include "mock_ls_service.h"

using namespace std;
using namespace oceanbase::common;
namespace oceanbase
{
using namespace common;
using namespace logservice;
using namespace share;
using namespace palf;
using namespace storage;
namespace unittest
{
LSN initial_lsn(1, LogGroupEntryHeader::HEADER_SER_SIZE);
int64_t initial_log_ts = 1;
ObLogReplayService replay_service;
MockPalfEnv palf_env;
MockLSService ls_service;
ObLSID id(1);
TEST(TestObReplayService, test_ob_replay_service)
{
  replay_service.init(&palf_env, &ls_service);
  // add_ls接口会直接开始回放已有的未回放已确认日志
  replay_service.add_ls(id, ObReplicaType::REPLICA_TYPE_FULL, initial_lsn, initial_log_ts);
  sleep(1);
  bool is_done = false;
  while (!is_done) {
    replay_service.is_replay_done(id, initial_lsn, is_done);
    sleep(1);
  }
  sleep(5);
} // end TEST
} // end of unittest
} // end of oceanbase

int main(int argc, char **argv)
{
  UNUSED(argc);
  UNUSED(argv);

  OB_LOGGER.set_file_name("test_ob_replay_service.log", true);
  OB_LOGGER.set_log_level("TRACE");
  PALF_LOG(INFO, "begin unittest::test_ob_replay_service");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
