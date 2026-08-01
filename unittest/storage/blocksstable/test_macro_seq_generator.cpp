/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE

#include <gmock/gmock.h>
#define private public
#include "storage/blocksstable/ob_macro_seq_generator.h"

#define ASSERT_SUCC(expr) ASSERT_EQ(common::OB_SUCCESS, (expr))
#define ASSERT_FAIL(expr) ASSERT_NE(common::OB_SUCCESS, (expr))
using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::blocksstable;

TEST(macro_seq_generator, param)
{
  ObMacroSeqParam seq_param;
  ASSERT_FALSE(seq_param.is_valid());
  seq_param.seq_type_ = ObMacroSeqParam::SEQ_TYPE_INC;
  ASSERT_TRUE(seq_param.is_valid());
  seq_param.start_ = -1;
  ASSERT_FALSE(seq_param.is_valid());
  seq_param.start_ = 0;
  ASSERT_TRUE(seq_param.is_valid());
  seq_param.reset();
  ASSERT_FALSE(seq_param.is_valid());
}

TEST(macro_seq_generator, inc_generator)
{
  ObMacroIncSeqGenerator inc_generator;
  ASSERT_FALSE(inc_generator.is_inited_);
  ObMacroSeqParam seq_param;
  ASSERT_FAIL(inc_generator.init(seq_param));
  seq_param.seq_type_ = ObMacroSeqParam::SEQ_TYPE_INC;
  seq_param.start_ = 11;
  ASSERT_SUCC(inc_generator.init(seq_param));
  ASSERT_TRUE(inc_generator.is_inited_);
  int64_t seq_val = -1;
  ASSERT_SUCC(inc_generator.get_next(seq_val)); ASSERT_EQ(seq_val, 11);
  ASSERT_SUCC(inc_generator.get_next(seq_val)); ASSERT_EQ(seq_val, 12);
  ASSERT_SUCC(inc_generator.get_next(seq_val)); ASSERT_EQ(seq_val, 13);
  ASSERT_SUCC(inc_generator.get_next(seq_val)); ASSERT_EQ(seq_val, 14);
  ASSERT_SUCC(inc_generator.get_next(seq_val)); ASSERT_EQ(seq_val, 15);
  ASSERT_SUCC(inc_generator.get_next(seq_val)); ASSERT_EQ(seq_val, 16);
  ASSERT_EQ(seq_val, inc_generator.get_current());
  int64_t preview_next_val = -1;
  ASSERT_SUCC(inc_generator.preview_next(seq_val, preview_next_val));
  ASSERT_EQ(seq_val + 1, preview_next_val);
  ASSERT_EQ(seq_val, inc_generator.get_current());
}

TEST(macro_seq_generator, inc_generator_threshold)
{
  ObMacroIncSeqGenerator inc_generator;
  ObMacroSeqParam seq_param;
  seq_param.seq_type_ = ObMacroSeqParam::SEQ_TYPE_INC;
  seq_param.start_ = 11;
  ASSERT_SUCC(inc_generator.init(seq_param));

  inc_generator.current_ = inc_generator.seq_threshold_ - 2;
  int64_t seq_val = -1;
  ASSERT_SUCC(inc_generator.get_next(seq_val));
  ASSERT_EQ(inc_generator.seq_threshold_ - 1, seq_val);
  ASSERT_EQ(seq_val, inc_generator.get_current());

  ASSERT_EQ(OB_SIZE_OVERFLOW, inc_generator.get_next(seq_val));
  ASSERT_EQ(inc_generator.seq_threshold_, seq_val);
  ASSERT_EQ(inc_generator.seq_threshold_ - 1, inc_generator.get_current());
}

#define LOG_FILE_PATH "./test_macro_seq_generator.log"

int main(int argc, char **argv)
{
  system("rm -rf " LOG_FILE_PATH "*");
  oceanbase::common::ObLogger::get_logger().set_log_level("WDIAG");
  oceanbase::common::ObLogger::get_logger().set_file_name(LOG_FILE_PATH, true);
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
