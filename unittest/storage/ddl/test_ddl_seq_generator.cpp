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

#define USING_LOG_PREFIX STORAGE

#include <gmock/gmock.h>
#define private public
#include "storage/ddl/ob_ddl_seq_generator.h"

#define ASSERT_SUCC(expr) ASSERT_EQ(common::OB_SUCCESS, (expr))
#define ASSERT_FAIL(expr) ASSERT_NE(common::OB_SUCCESS, (expr))
using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::storage;

TEST(ddl_seq_generator, init)
{
  ObDDLSeqGenerator seq_generator;
  ASSERT_FALSE(seq_generator.is_inited_);
  ASSERT_FAIL(seq_generator.init(-1, 5, 100));
  ASSERT_FAIL(seq_generator.init(0, 0, 100));
  ASSERT_FAIL(seq_generator.init(0, 5, -100));
  ASSERT_FAIL(seq_generator.init(0, 100, 5));
  ASSERT_SUCC(seq_generator.init(0, 5, 100));
  ASSERT_FAIL(seq_generator.init(0, 5, 100));
  ASSERT_TRUE(seq_generator.is_inited_);
  seq_generator.reset();
  ASSERT_FALSE(seq_generator.is_inited_);
}

TEST(ddl_seq_generator, get_next)
{
  ObDDLSeqGenerator seq_generator;
  ASSERT_SUCC(seq_generator.init(10, 5, 100));
  int64_t seq_val = -1;
  int64_t preview_next_val = -1;
  bool is_step_over = false;
  ASSERT_SUCC(seq_generator.get_next(seq_val, is_step_over)); ASSERT_EQ(10, seq_val); ASSERT_FALSE(is_step_over);
  ASSERT_SUCC(seq_generator.get_next(seq_val, is_step_over)); ASSERT_EQ(11, seq_val); ASSERT_FALSE(is_step_over);
  ASSERT_SUCC(seq_generator.get_next(seq_val, is_step_over)); ASSERT_EQ(12, seq_val); ASSERT_FALSE(is_step_over);
  ASSERT_SUCC(seq_generator.get_next(seq_val, is_step_over)); ASSERT_EQ(13, seq_val); ASSERT_FALSE(is_step_over);
  ASSERT_SUCC(seq_generator.get_next(seq_val, is_step_over)); ASSERT_EQ(14, seq_val); ASSERT_TRUE(is_step_over);
  ASSERT_SUCC(seq_generator.get_next(seq_val, is_step_over)); ASSERT_EQ(110, seq_val); ASSERT_FALSE(is_step_over);
  ASSERT_SUCC(seq_generator.get_next(seq_val, is_step_over)); ASSERT_EQ(111, seq_val); ASSERT_FALSE(is_step_over);
  ASSERT_SUCC(seq_generator.get_next(seq_val, is_step_over)); ASSERT_EQ(112, seq_val); ASSERT_FALSE(is_step_over);
  ASSERT_SUCC(seq_generator.get_next(seq_val, is_step_over)); ASSERT_EQ(113, seq_val); ASSERT_FALSE(is_step_over);
  ASSERT_SUCC(seq_generator.get_next(seq_val, is_step_over)); ASSERT_EQ(114, seq_val); ASSERT_TRUE(is_step_over);
  ASSERT_SUCC(seq_generator.get_next(seq_val, is_step_over)); ASSERT_EQ(210, seq_val); ASSERT_FALSE(is_step_over);

  ASSERT_SUCC(seq_generator.preview_next(seq_val, preview_next_val));
  ASSERT_EQ(211, preview_next_val);
  ASSERT_EQ(seq_val, seq_generator.get_current());
}

TEST(ddl_seq_generator, corner)
{
  ObDDLSeqGenerator seq_generator;
  ASSERT_SUCC(seq_generator.init(1, 1, 1));
  seq_generator.reset();
  ASSERT_SUCC(seq_generator.init(10, 1, 100));
  int64_t seq_val = -1;
  bool is_step_over = false;
  ASSERT_SUCC(seq_generator.get_next(seq_val, is_step_over)); ASSERT_EQ(10, seq_val); ASSERT_TRUE(is_step_over);
  ASSERT_SUCC(seq_generator.get_next(seq_val, is_step_over)); ASSERT_EQ(110, seq_val); ASSERT_TRUE(is_step_over);
  ASSERT_SUCC(seq_generator.get_next(seq_val, is_step_over)); ASSERT_EQ(210, seq_val); ASSERT_TRUE(is_step_over);

  seq_generator.reset();
  ASSERT_SUCC(seq_generator.init(0, 1, 1));
  ASSERT_SUCC(seq_generator.get_next(seq_val, is_step_over)); ASSERT_EQ(0, seq_val); ASSERT_TRUE(is_step_over);
  ASSERT_SUCC(seq_generator.get_next(seq_val, is_step_over)); ASSERT_EQ(1, seq_val); ASSERT_TRUE(is_step_over);
  ASSERT_SUCC(seq_generator.get_next(seq_val, is_step_over)); ASSERT_EQ(2, seq_val); ASSERT_TRUE(is_step_over);

  seq_generator.reset();
  ASSERT_SUCC(seq_generator.init(10, 1, 11));
  ASSERT_SUCC(seq_generator.get_next(seq_val, is_step_over)); ASSERT_EQ(10, seq_val); ASSERT_TRUE(is_step_over);
  ASSERT_SUCC(seq_generator.get_next(seq_val, is_step_over)); ASSERT_EQ(21, seq_val); ASSERT_TRUE(is_step_over);
  ASSERT_SUCC(seq_generator.get_next(seq_val, is_step_over)); ASSERT_EQ(32, seq_val); ASSERT_TRUE(is_step_over);
}

TEST(ddl_seq_generator, get_next_interval)
{
  ObDDLSeqGenerator seq_generator;
  ASSERT_SUCC(seq_generator.init(10, 10, 100));
  int64_t interval_start = -1;
  int64_t interval_end = -1;
  ASSERT_SUCC(seq_generator.get_next_interval(interval_start, interval_end)); ASSERT_EQ(10, interval_start); ASSERT_EQ(19, interval_end); ASSERT_TRUE(seq_generator.is_step_over(seq_generator.get_current()));
  ASSERT_SUCC(seq_generator.get_next_interval(interval_start, interval_end)); ASSERT_EQ(110, interval_start); ASSERT_EQ(119, interval_end); ASSERT_TRUE(seq_generator.is_step_over(seq_generator.get_current()));
  ASSERT_SUCC(seq_generator.get_next_interval(interval_start, interval_end)); ASSERT_EQ(210, interval_start); ASSERT_EQ(219, interval_end); ASSERT_TRUE(seq_generator.is_step_over(seq_generator.get_current()));
  ASSERT_SUCC(seq_generator.get_next_interval(interval_start, interval_end)); ASSERT_EQ(310, interval_start); ASSERT_EQ(319, interval_end); ASSERT_TRUE(seq_generator.is_step_over(seq_generator.get_current()));

  seq_generator.reset();
  ASSERT_SUCC(seq_generator.init(11, 8, 123));
  ASSERT_SUCC(seq_generator.get_next_interval(interval_start, interval_end)); ASSERT_EQ(11, interval_start); ASSERT_EQ(18, interval_end); ASSERT_TRUE(seq_generator.is_step_over(seq_generator.get_current()));
  ASSERT_SUCC(seq_generator.get_next_interval(interval_start, interval_end)); ASSERT_EQ(134, interval_start); ASSERT_EQ(141, interval_end); ASSERT_TRUE(seq_generator.is_step_over(seq_generator.get_current()));
  ASSERT_SUCC(seq_generator.get_next_interval(interval_start, interval_end)); ASSERT_EQ(257, interval_start); ASSERT_EQ(264, interval_end); ASSERT_TRUE(seq_generator.is_step_over(seq_generator.get_current()));

}

TEST(ddl_seq_generator, big_start)
{
  ObDDLSeqGenerator seq_generator;
  ASSERT_SUCC(seq_generator.init(320, 10, 100));
  int64_t seq_val = -1;
  bool is_step_over = false;
  ASSERT_SUCC(seq_generator.get_next(seq_val, is_step_over)); ASSERT_EQ(320, seq_val); ASSERT_FALSE(is_step_over);
  ASSERT_SUCC(seq_generator.get_next(seq_val, is_step_over)); ASSERT_EQ(321, seq_val); ASSERT_FALSE(is_step_over);
  ASSERT_SUCC(seq_generator.get_next(seq_val, is_step_over)); ASSERT_EQ(322, seq_val); ASSERT_FALSE(is_step_over);
  ASSERT_SUCC(seq_generator.get_next(seq_val, is_step_over)); ASSERT_EQ(323, seq_val); ASSERT_FALSE(is_step_over);
  ASSERT_SUCC(seq_generator.get_next(seq_val, is_step_over)); ASSERT_EQ(324, seq_val); ASSERT_FALSE(is_step_over);
  ASSERT_SUCC(seq_generator.get_next(seq_val, is_step_over)); ASSERT_EQ(325, seq_val); ASSERT_FALSE(is_step_over);
  ASSERT_SUCC(seq_generator.get_next(seq_val, is_step_over)); ASSERT_EQ(326, seq_val); ASSERT_FALSE(is_step_over);
  ASSERT_SUCC(seq_generator.get_next(seq_val, is_step_over)); ASSERT_EQ(327, seq_val); ASSERT_FALSE(is_step_over);
  ASSERT_SUCC(seq_generator.get_next(seq_val, is_step_over)); ASSERT_EQ(328, seq_val); ASSERT_FALSE(is_step_over);
  ASSERT_SUCC(seq_generator.get_next(seq_val, is_step_over)); ASSERT_EQ(329, seq_val); ASSERT_TRUE(is_step_over);
  ASSERT_SUCC(seq_generator.get_next(seq_val, is_step_over)); ASSERT_EQ(420, seq_val); ASSERT_FALSE(is_step_over);

  seq_generator.reset();
  ASSERT_SUCC(seq_generator.init(320, 10, 100));
  int64_t interval_start = -1;
  int64_t interval_end = -1;
  ASSERT_SUCC(seq_generator.get_next_interval(interval_start, interval_end)); ASSERT_EQ(320, interval_start); ASSERT_EQ(329, interval_end); ASSERT_TRUE(seq_generator.is_step_over(seq_generator.get_current()));
  ASSERT_SUCC(seq_generator.get_next_interval(interval_start, interval_end)); ASSERT_EQ(420, interval_start); ASSERT_EQ(429, interval_end); ASSERT_TRUE(seq_generator.is_step_over(seq_generator.get_current()));

}


#define LOG_FILE_PATH "./test_ddl_seq_generator.log"

int main(int argc, char **argv)
{
  system("rm -rf " LOG_FILE_PATH "*");
  oceanbase::common::ObLogger::get_logger().set_log_level("WDIAG");
  oceanbase::common::ObLogger::get_logger().set_file_name(LOG_FILE_PATH, true);
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
