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

#include "storage/tx/ob_trans_version_mgr.h"
#include "common/ob_clock_generator.h"

#include <gtest/gtest.h>
#include "share/ob_errno.h"
#include "lib/oblog/ob_log.h"

namespace oceanbase
{

using namespace common;
using namespace transaction;

namespace unittest
{

class TestObTransVersionMgr : public ::testing::Test
{
public :
  virtual void SetUp() {}
  virtual void TearDown() {}
};

TEST_F(TestObTransVersionMgr, local_trans_version)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());
  int64_t local_trans_version = 0;
  int64_t tmp_trans_version = 0;
  ObTransVersionMgr mgr;

  EXPECT_EQ(OB_SUCCESS, mgr.get_and_update_local_trans_version(local_trans_version));
  EXPECT_TRUE(local_trans_version > 0);
  EXPECT_EQ(OB_SUCCESS, mgr.get_and_update_local_trans_version(tmp_trans_version));
  EXPECT_TRUE(tmp_trans_version > local_trans_version);

  EXPECT_EQ(OB_SUCCESS, mgr.update_local_trans_version(local_trans_version + 1000000));
  EXPECT_EQ(OB_SUCCESS, mgr.get_and_update_local_trans_version(tmp_trans_version));
  EXPECT_TRUE(tmp_trans_version >= local_trans_version + 1000000);
}

TEST_F(TestObTransVersionMgr, local_trans_version_publish_version)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());
  int64_t local_trans_version = 0;
  int64_t tmp_trans_version = 0;
  int64_t publish_version = 0;
  ObTransVersionMgr mgr;

  EXPECT_EQ(OB_SUCCESS, mgr.get_and_update_local_trans_version(local_trans_version));
  EXPECT_EQ(OB_SUCCESS, mgr.update_publish_version(local_trans_version + 1000000));
  EXPECT_EQ(OB_SUCCESS, mgr.get_and_update_local_trans_version(tmp_trans_version));
  EXPECT_TRUE(tmp_trans_version >= local_trans_version + 1000000);
  EXPECT_EQ(OB_SUCCESS, mgr.get_publish_version(publish_version));
  EXPECT_TRUE(publish_version >= local_trans_version + 1000000);
}

TEST_F(TestObTransVersionMgr, destroy)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());
  ObTransVersionMgr mgr;
  mgr.destroy();
  mgr.reset();
}

TEST_F(TestObTransVersionMgr, invalid_argument)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());

  int64_t local_trans_version = ObTransVersion::INVALID_TRANS_VERSION;
  int64_t publish_version = ObTransVersion::INVALID_TRANS_VERSION;
  ObTransVersionMgr mgr;

  EXPECT_EQ(OB_INVALID_ARGUMENT, mgr.update_local_trans_version(local_trans_version));
  EXPECT_EQ(OB_INVALID_ARGUMENT, mgr.update_publish_version(publish_version));
}

}//end of unittest
}//end of oceanbase

using namespace oceanbase;
using namespace oceanbase::common;

int main(int argc, char **argv)
{
  int ret = 1;
  ObLogger &logger = ObLogger::get_logger();
  logger.set_file_name("test_ob_trans_version_mgr.log", true);
  logger.set_log_level(OB_LOG_LEVEL_INFO);
  if (OB_SUCCESS != ObClockGenerator::init()) {
    TRANS_LOG(WARN, "init ob_clock_generator error");
  } else {
    testing::InitGoogleTest(&argc, argv);
    ret = RUN_ALL_TESTS();
  }
  return ret;
}
