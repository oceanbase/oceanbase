/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "share/deadlock/ob_deadlock_detector_common_define.h"

#include <gmock/gmock.h>
#define private public
#define protected public
#include "share/deadlock/ob_lcl_scheme/ob_lcl_node.h"
#include "share/deadlock/ob_lcl_scheme/ob_lcl_message.h"
#undef protected
#undef private

namespace oceanbase {
namespace unittest {

using namespace common;
using namespace share::detector;
using namespace std;


class TestDeadLockUtility : public ::testing::Test {
public:
  TestDeadLockUtility() {}
  ~TestDeadLockUtility() {}
  virtual void SetUp() {}
  virtual void TearDown() {}
};

// 序列化和反序列化的功能
TEST_F(TestDeadLockUtility, interface) {
  ObDetectorUserReportInfo info;
  ObSharedGuard<char> ptr;
  ptr.assign((char*)("a"), [](char*){});
  ASSERT_EQ(OB_SUCCESS, info.set_module_name(ptr));
  ptr.assign((char*)("b"), [](char*){});
  ASSERT_EQ(OB_SUCCESS, info.set_visitor(ptr));
  ptr.assign((char*)("c"), [](char*){});
  ASSERT_EQ(OB_SUCCESS, info.set_resource(ptr));
  ASSERT_EQ(OB_SUCCESS, info.set_extra_info("1","2"));// OK
  // ASSERT_EQ(OB_SUCCESS, info.set_extra_info(ObString("1"),"2"));// OK
  // ASSERT_EQ(OB_SUCCESS, info.set_extra_info("1",ObString("2")));// OK
  // ASSERT_EQ(OB_SUCCESS, info.set_extra_info(ObString("1"),ObString("2")));// OK
  // ASSERT_EQ(OB_SUCCESS, info.set_extra_info("1",ObString("2"),ObString("3"),"4"));// OK
  // info.set_extra_info("1",ObString("2"),ObString("3"));// compile error, number of args not even
  // info.set_extra_info(string("1"), "2");// compile error, arg types neither ObString nor char*
  // info.set_extra_info("1","2","3","4","5","6","7","8","9","10","11","12");// compile error, number of args reach column's limit
}

class TestLCLNode : public ::testing::Test
{
public:
  TestLCLNode()
    : node_(UserBinaryKey(), 2,
            DetectCallBack([](const ObIArray<ObDetectorInnerReportInfo> &, const int64_t) {
              return OB_SUCCESS;
            }),
            CollectCallBack([](const ObDependencyHolder &, ObDetectorUserReportInfo &) {
              return OB_SUCCESS;
            }),
            FillVirtualInfoCallBack(DummyFillCallBack()),
            0, ObDetectorPriority(1), 0, 0, false)
  {}

  void SetUp() override
  {
    node_.lclv_ = 10;
    node_.lcl_period_ = 1;
    node_.last_send_collect_info_period_ = 0;
    msg_.label_ = ObLCLLabel(1, ObDetectorPriority(1));
  }

  int process_message_in_phase(const bool lcls)
  {
    const int64_t deadline = ObClockGenerator::getRealClock() + 5 * 1000 * 1000;
    const int64_t original_lclv = node_.lclv_;
    while (ObClockGenerator::getRealClock() < deadline) {
      const int64_t now = ObClockGenerator::getClock();
      const int64_t phase = now / PHASE_TIME;
      if ((phase % 2 == 1) == lcls && now % PHASE_TIME < PHASE_TIME / 2) {
        node_.lclv_ = original_lclv;
        msg_.send_ts_ = now;
        const int ret = node_.process_lcl_message(msg_);
        // Retry if scheduling crossed a phase boundary during the call.
        if (ObClockGenerator::getClock() / PHASE_TIME == phase) {
          return ret;
        }
      }
      ObClockGenerator::usleep(10 * 1000);
    }
    return OB_TIMEOUT;
  }

  // Set the wire value directly so invalid values can reach the handlers.
  ObLCLNode node_;
  ObLCLMessage msg_;
};

TEST_F(TestLCLNode, lclp_valid_clock_values)
{
  const struct {
    uint64_t input;
    int64_t expected;
  } cases[] = {
    {0, 10},
    {9, 10},
    {10, 11},
    {11, 12},
    {static_cast<uint64_t>(INT64_MAX - 2), INT64_MAX - 1},
  };
  for (const auto &test_case : cases) {
    SCOPED_TRACE(test_case.input);
    node_.lclv_ = 10;
    msg_.lclv_ = test_case.input;
    ASSERT_EQ(OB_SUCCESS, node_.process_msg_as_lclp_msg_(msg_));
    EXPECT_EQ(test_case.expected, node_.lclv_);
  }
}

TEST_F(TestLCLNode, lclp_rejects_invalid_clock_without_poisoning_state)
{
  const uint64_t invalid_values[] = {
    static_cast<uint64_t>(INT64_MAX - 1),
    static_cast<uint64_t>(INT64_MAX),
    static_cast<uint64_t>(INT64_MAX) + 1,
    UINT64_MAX - 1,
    UINT64_MAX,
  };
  const ObLCLLabel original_label = node_.public_label_;
  for (const uint64_t value : invalid_values) {
    SCOPED_TRACE(value);
    node_.lclv_ = 10;
    msg_.lclv_ = value;
    ASSERT_EQ(OB_INVALID_ARGUMENT, node_.process_msg_as_lclp_msg_(msg_));
    EXPECT_EQ(10, node_.lclv_);
    EXPECT_TRUE(original_label == node_.public_label_);

    msg_.lclv_ = 11;
    ASSERT_EQ(OB_SUCCESS, node_.process_msg_as_lclp_msg_(msg_));
    EXPECT_EQ(12, node_.lclv_);
  }
}

TEST_F(TestLCLNode, lcls_valid_clock_values)
{
  const struct {
    uint64_t input;
    int64_t expected;
    bool updates_label;
  } cases[] = {
    {0, 10, false},
    {9, 10, false},
    {10, 10, true},
    {11, 11, true},
    {static_cast<uint64_t>(INT64_MAX - 2), INT64_MAX - 2, true},
    {static_cast<uint64_t>(INT64_MAX - 1), INT64_MAX - 1, true},
  };
  ASSERT_TRUE(msg_.label_ < node_.private_label_);
  for (const auto &test_case : cases) {
    SCOPED_TRACE(test_case.input);
    node_.lclv_ = 10;
    node_.public_label_ = node_.private_label_;
    msg_.lclv_ = test_case.input;
    bool detected = false;
    ASSERT_EQ(OB_SUCCESS, node_.process_msg_as_lcls_msg_(msg_, detected));
    EXPECT_EQ(test_case.expected, node_.lclv_);
    EXPECT_FALSE(detected);
    EXPECT_TRUE(node_.public_label_ ==
                (test_case.updates_label ? msg_.label_ : node_.private_label_));
  }
}

TEST_F(TestLCLNode, lcls_rejects_invalid_clock_without_poisoning_state)
{
  const uint64_t invalid_values[] = {
    static_cast<uint64_t>(INT64_MAX),
    static_cast<uint64_t>(INT64_MAX) + 1,
    UINT64_MAX - 1,
    UINT64_MAX,
  };
  const ObLCLLabel lower_label = msg_.label_;
  // Exercise both label propagation and cycle detection paths.
  for (const ObLCLLabel &label : {lower_label, node_.private_label_}) {
    for (const uint64_t value : invalid_values) {
      SCOPED_TRACE(value);
      node_.lclv_ = 10;
      node_.public_label_ = node_.private_label_;
      msg_.label_ = label;
      msg_.lclv_ = value;
      bool detected = false;
      ASSERT_EQ(OB_INVALID_ARGUMENT, node_.process_msg_as_lcls_msg_(msg_, detected));
      EXPECT_EQ(10, node_.lclv_);
      EXPECT_TRUE(node_.private_label_ == node_.public_label_);
      EXPECT_EQ(0, node_.last_send_collect_info_period_);
      EXPECT_FALSE(detected);

      msg_.label_ = lower_label;
      msg_.lclv_ = 11;
      ASSERT_EQ(OB_SUCCESS, node_.process_msg_as_lcls_msg_(msg_, detected));
      EXPECT_EQ(11, node_.lclv_);
      EXPECT_TRUE(lower_label == node_.public_label_);
      EXPECT_FALSE(detected);
    }
  }
}

TEST_F(TestLCLNode, lcls_valid_clock_still_detects_cycle)
{
  msg_.lclv_ = 10;
  msg_.label_ = node_.private_label_;
  bool detected = false;
  ASSERT_EQ(OB_SUCCESS, node_.process_msg_as_lcls_msg_(msg_, detected));
  EXPECT_TRUE(detected);
  EXPECT_EQ(10, node_.lclv_);
  EXPECT_EQ(node_.lcl_period_, node_.last_send_collect_info_period_);
}

TEST_F(TestLCLNode, entry_propagates_invalid_lclp_clock)
{
  msg_.lclv_ = UINT64_MAX - 1;
  ASSERT_EQ(OB_INVALID_ARGUMENT, process_message_in_phase(false));
  EXPECT_EQ(10, node_.lclv_);
  EXPECT_EQ(0, node_.last_send_collect_info_period_);
}

TEST_F(TestLCLNode, entry_propagates_invalid_lcls_clock)
{
  msg_.lclv_ = UINT64_MAX;
  ASSERT_EQ(OB_INVALID_ARGUMENT, process_message_in_phase(true));
  EXPECT_EQ(10, node_.lclv_);
  EXPECT_EQ(0, node_.last_send_collect_info_period_);
}

TEST_F(TestLCLNode, entry_ignores_already_collected_cycle)
{
  // Keep the recorded period stable throughout the bounded phase wait.
  node_.lcl_period_ = ObClockGenerator::getRealClock() / PERIOD + 10;
  node_.last_send_collect_info_period_ = node_.lcl_period_;
  msg_.lclv_ = 11;
  msg_.label_ = node_.private_label_;
  bool detected = false;
  ASSERT_EQ(OB_EAGAIN, node_.process_msg_as_lcls_msg_(msg_, detected));
  ASSERT_FALSE(detected);
  node_.lclv_ = 10;

  ASSERT_EQ(OB_SUCCESS, process_message_in_phase(true));
  EXPECT_EQ(11, node_.lclv_);
  EXPECT_EQ(node_.lcl_period_, node_.last_send_collect_info_period_);
}

}// namespace unittest
}// namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -rf test_deadlock_utility.log");
  oceanbase::common::ObLogger &logger = oceanbase::common::ObLogger::get_logger();
  logger.set_file_name("test_deadlock_utility.log", false);
  logger.set_log_level(OB_LOG_LEVEL_DEBUG);
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
