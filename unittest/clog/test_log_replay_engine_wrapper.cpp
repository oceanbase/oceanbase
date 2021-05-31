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

#include "clog/ob_log_replay_engine_wrapper.h"

#include <gtest/gtest.h>
#include "share/ob_define.h"
#include "storage/replayengine/ob_log_replay_engine.h"
#include "clog/ob_log_entry.h"

namespace oceanbase {
using namespace clog;
using namespace common;
namespace unittest {
TEST(ob_log_replay_engine_wrapper, test1)
{
  replayengine::ObLogReplayEngine log_replay_engine;
  ObLogReplayEngineWrapper re_wrapper;
  common::ObPartitionKey partition_key;
  clog::ObLogEntry log_entry;
  // const bool batch_committed = false;
  // EXPECT_EQ(OB_NOT_INIT, re_wrapper.submit_replay_task(partition_key, log_entry, 0, batch_committed));

  EXPECT_EQ(OB_SUCCESS, re_wrapper.init(&log_replay_engine));
  // EXPECT_EQ(OB_NOT_INIT, re_wrapper.submit_replay_task(partition_key, log_entry, 0, batch_committed));
}

}  // namespace unittest
}  // namespace oceanbase

int main(int argc, char** argv)
{
  OB_LOGGER.set_file_name("test_log_replay_engine_wrapper.log", true);
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
