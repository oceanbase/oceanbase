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

#define private public
#include "sql/engine/expr/ob_expr_operator.h"

#define ADAPTIVE_SLIDE_WINDOW_SIZE 4096

using namespace std;
namespace oceanbase
{
namespace sql
{
class AdapitveSlideWindowTest : public ::testing::Test
{
public:
  AdapitveSlideWindowTest() = default;
  virtual ~AdapitveSlideWindowTest() = default;
  virtual void SetUp(){};
  virtual void TearDown(){};

public:
  int64_t total_count_{0};
  ObAdaptiveFilterSlideWindow slide_window_{total_count_};

private:
  DISALLOW_COPY_AND_ASSIGN(AdapitveSlideWindowTest);
};

static void mock_filter(ObAdaptiveFilterSlideWindow &slide_window, int64_t &total_count)
{
  bool reopen_flag = false;
  int64_t partial_total_count = common::ObRandom::rand(1, ADAPTIVE_SLIDE_WINDOW_SIZE);
  int64_t partial_filter_count = common::ObRandom::rand(0, partial_total_count);
  total_count += partial_total_count;
  if (slide_window.dynamic_disable()) {
    if (slide_window.cur_pos_ >= slide_window.next_check_start_pos_) {
      reopen_flag = true;
    }
    partial_filter_count = 0;
    slide_window.update_slide_window_info(partial_filter_count, partial_total_count);
    EXPECT_EQ(reopen_flag, !slide_window.dynamic_disable());
    EXPECT_EQ(0, slide_window.partial_filter_count_);
    EXPECT_EQ(0, slide_window.partial_total_count_);
  } else {
    int64_t acc_partial_total_count = partial_total_count + slide_window.partial_total_count_;
    int64_t acc_partial_filter_count = partial_filter_count + slide_window.partial_filter_count_;
    double filter_rate = acc_partial_filter_count / (double)acc_partial_total_count;
    bool need_check_filter_rate =
        total_count >= slide_window.next_check_start_pos_ + ADAPTIVE_SLIDE_WINDOW_SIZE;
    slide_window.update_slide_window_info(partial_filter_count, partial_total_count);
    if (need_check_filter_rate) {
      if (filter_rate >= 0.5) {
        EXPECT_EQ(false, slide_window.dynamic_disable());
        EXPECT_EQ(0, slide_window.window_cnt_);
        EXPECT_EQ(total_count, slide_window.next_check_start_pos_);
      } else {
        EXPECT_EQ(true, slide_window.dynamic_disable());
      }
      EXPECT_EQ(0, slide_window.partial_filter_count_);
      EXPECT_EQ(0, slide_window.partial_total_count_);
    } else {
      EXPECT_EQ(acc_partial_filter_count, slide_window.partial_filter_count_);
      EXPECT_EQ(acc_partial_total_count, slide_window.partial_total_count_);
    }
  }
}

TEST_F(AdapitveSlideWindowTest, test_adaptive_slide_window)
{
  slide_window_.start_to_work();
  for (int64_t i = 0; i < 100; ++i) { mock_filter(slide_window_, total_count_); }
}

} // namespace sql
} // namespace oceanbase
int main(int argc, char **argv)
{
  OB_LOGGER.set_log_level("TRACE");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
