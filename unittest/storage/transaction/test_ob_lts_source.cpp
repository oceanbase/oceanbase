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

#include "storage/transaction/ob_lts_source.h"
#include "storage/transaction/ob_ts_mgr.h"
#include <gtest/gtest.h>
#include "share/ob_errno.h"
#include "lib/oblog/ob_log.h"

namespace oceanbase {
using namespace common;
using namespace transaction;
namespace unittest {

class TestObTsCbTask : public ObTsCbTask {
public:
  TestObTsCbTask()
  {}
  ~TestObTsCbTask()
  {}

public:
  int get_gts_callback(const MonotonicTs srr, const int64_t ts, const MonotonicTs receive_gts_ts)
  {
    UNUSED(srr);
    UNUSED(ts);
    UNUSED(receive_gts_ts);
    return OB_SUCCESS;
  }
  int gts_elapse_callback(const MonotonicTs srr, const int64_t ts)
  {
    UNUSED(srr);
    UNUSED(ts);
    return OB_SUCCESS;
  }
  MonotonicTs get_stc() const
  {
    return MonotonicTs(100);
  }
  uint64_t hash() const
  {
    return 100;
  }
  int64_t get_request_ts() const
  {
    return 100;
  }
  uint64_t get_tenant_id() const
  {
    return 100;
  }
};

class TestObLtsSource : public ::testing::Test {
public:
  virtual void SetUp()
  {}
  virtual void TearDown()
  {}
};

//////////////////////basic function test//////////////////////////////////////////
TEST_F(TestObLtsSource, get_gts)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());
  ObLtsSource lts;
  bool update = false;
  int64_t t1 = 0;
  int64_t t2 = 0;
  MonotonicTs stc(100);
  MonotonicTs receive_gts_ts;
  TestObTsCbTask cb_task;
  EXPECT_EQ(OB_SUCCESS, lts.update_gts(100, update));
  EXPECT_EQ(false, update);
  EXPECT_EQ(OB_SUCCESS, lts.get_gts(NULL, t1));
  EXPECT_EQ(OB_SUCCESS, lts.get_gts(NULL, t2));
  EXPECT_TRUE(t1 <= t2);

  t1 = 0;
  t2 = 0;
  EXPECT_EQ(OB_SUCCESS, lts.get_gts(&cb_task, t1));
  EXPECT_EQ(OB_SUCCESS, lts.get_gts(&cb_task, t2));
  EXPECT_TRUE(t1 <= t2);

  t1 = 0;
  t2 = 0;
  EXPECT_EQ(OB_SUCCESS, lts.get_gts(stc, &cb_task, t1, receive_gts_ts));
  EXPECT_EQ(OB_SUCCESS, lts.get_gts(stc, &cb_task, t2, receive_gts_ts));
  EXPECT_TRUE(t1 <= t2);

  t1 = 0;
  t2 = 0;
  EXPECT_EQ(OB_SUCCESS, lts.get_gts(stc, NULL, t1, receive_gts_ts));
  EXPECT_EQ(OB_SUCCESS, lts.get_gts(stc, NULL, t2, receive_gts_ts));
  EXPECT_TRUE(t1 <= t2);
}

TEST_F(TestObLtsSource, wait_gts_elapse)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());
  ObLtsSource lts;
  int64_t t1 = 0;
  int64_t t2 = 0;
  TestObTsCbTask cb_task;
  bool need_wait = false;
  t1 = ObTimeUtility::current_time() + 1000000;
  EXPECT_EQ(OB_SUCCESS, lts.wait_gts_elapse(t1, &cb_task, need_wait));
  EXPECT_EQ(false, need_wait);
  t2 = 0;
  EXPECT_EQ(OB_SUCCESS, lts.get_gts(NULL, t2));
  EXPECT_TRUE(t1 <= t2);

  t1 = ObTimeUtility::current_time() + 2000000;
  EXPECT_EQ(OB_SUCCESS, lts.wait_gts_elapse(t1));
  t2 = 0;
  EXPECT_EQ(OB_SUCCESS, lts.get_gts(NULL, t2));
  EXPECT_TRUE(t1 <= t2);
}

TEST_F(TestObLtsSource, refresh_gts)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());
  ObLtsSource lts;
  bool need_refresh = true;
  EXPECT_EQ(OB_SUCCESS, lts.refresh_gts(need_refresh));
  need_refresh = false;
  EXPECT_EQ(OB_SUCCESS, lts.refresh_gts(need_refresh));
}

TEST_F(TestObLtsSource, update_and_get_base_ts)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());
  ObLtsSource lts;
  const int64_t base_ts = ObTimeUtility::current_time() + 1000000;
  const int64_t publish_version = ObTimeUtility::current_time() + 1000000;
  int64_t t1 = 0;
  int64_t t2 = 0;
  EXPECT_EQ(OB_SUCCESS, lts.update_base_ts(base_ts, publish_version));
  EXPECT_EQ(OB_SUCCESS, lts.get_gts(NULL, t1));
  EXPECT_TRUE(base_ts <= t1);
  t1 = 0;
  EXPECT_EQ(OB_SUCCESS, lts.get_base_ts(t1, t2));
  EXPECT_TRUE(base_ts <= t1);
  EXPECT_TRUE(publish_version <= t1);
}

TEST_F(TestObLtsSource, update_and_get_publish_version)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());
  ObLtsSource lts;
  const int64_t publish_version = ObTimeUtility::current_time() + 1000000;
  int64_t t1 = 0;
  EXPECT_EQ(OB_SUCCESS, lts.update_publish_version(publish_version));
  EXPECT_EQ(OB_SUCCESS, lts.get_publish_version(t1));
  EXPECT_TRUE(publish_version <= t1);
}

//////////////////////////boundary test/////////////////////////////////////////
TEST_F(TestObLtsSource, invalid_argument)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());
  ObLtsSource lts;
  bool update = false;
  bool need_wait = false;
  MonotonicTs receive_gts_ts;
  int64_t t1 = 0;
  MonotonicTs stc;
  EXPECT_EQ(OB_INVALID_ARGUMENT, lts.update_gts(0, update));
  EXPECT_EQ(OB_INVALID_ARGUMENT, lts.get_gts(stc, NULL, t1, receive_gts_ts));
  EXPECT_EQ(OB_INVALID_ARGUMENT, lts.wait_gts_elapse(0, NULL, need_wait));
  EXPECT_EQ(OB_INVALID_ARGUMENT, lts.wait_gts_elapse(0));
  EXPECT_EQ(OB_INVALID_ARGUMENT, lts.update_base_ts(-1, -1));
  EXPECT_EQ(OB_SUCCESS, lts.update_base_ts(0, 0));
  EXPECT_EQ(OB_INVALID_ARGUMENT, lts.update_publish_version(-1));
  EXPECT_EQ(OB_SUCCESS, lts.update_publish_version(0));
}

}  // namespace unittest
}  // namespace oceanbase

using namespace oceanbase;
using namespace oceanbase::common;

int main(int argc, char** argv)
{
  int ret = 1;
  ObLogger& logger = ObLogger::get_logger();
  logger.set_file_name("test_ob_lts_source.log", true);
  logger.set_log_level(OB_LOG_LEVEL_INFO);
  testing::InitGoogleTest(&argc, argv);
  ret = RUN_ALL_TESTS();
  return ret;
}
