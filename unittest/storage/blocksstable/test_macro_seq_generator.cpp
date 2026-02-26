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
#include "storage/blocksstable/ob_macro_seq_generator.h"

#define ASSERT_SUCC(expr) ASSERT_EQ(common::OB_SUCCESS, (expr))
#define ASSERT_FAIL(expr) ASSERT_NE(common::OB_SUCCESS, (expr))
using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::storage;
using namespace oceanbase::blocksstable;

TEST(macro_seq_generator, param)
{
  ObMacroSeqParam seq_param;
  ASSERT_FALSE(seq_param.is_valid());
  seq_param.seq_type_ = ObMacroSeqParam::SEQ_TYPE_INC;
  ASSERT_TRUE(seq_param.is_valid());
  seq_param.seq_type_ = ObMacroSeqParam::SEQ_TYPE_SKIP;
  ASSERT_FALSE(seq_param.is_valid());
  seq_param.interval_ = 1;
  ASSERT_FALSE(seq_param.is_valid());
  seq_param.step_ = 1;
  ASSERT_TRUE(seq_param.is_valid());
}

TEST(macro_seq_generator, inc_generator)
{
  ObMacroIncSeqGenerator inc_generator;
  ASSERT_FALSE(inc_generator.is_inited_);
  ObMacroSeqParam seq_param;
  seq_param.seq_type_ = ObMacroSeqParam::SEQ_TYPE_SKIP;
  seq_param.start_ = 11;
  seq_param.interval_ = 10;
  seq_param.step_ = 100;
  ASSERT_FAIL(inc_generator.init(seq_param));
  seq_param.seq_type_ = ObMacroSeqParam::SEQ_TYPE_INC;
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

TEST(macro_seq_generator, skip_generator)
{
  ObMacroSkipSeqGenerator skip_generator;
  ASSERT_FALSE(skip_generator.ddl_seq_generator_.is_inited_);
  ObMacroSeqParam seq_param;
  seq_param.seq_type_ = ObMacroSeqParam::SEQ_TYPE_INC;
  ASSERT_FAIL(skip_generator.init(seq_param));
  seq_param.seq_type_ = ObMacroSeqParam::SEQ_TYPE_SKIP;
  seq_param.start_ = 22;
  seq_param.interval_ = 3;
  seq_param.step_ = 100;
  ASSERT_SUCC(skip_generator.init(seq_param));
  ASSERT_TRUE(skip_generator.ddl_seq_generator_.is_inited_);
  int64_t seq_val = -1;
  ASSERT_SUCC(skip_generator.get_next(seq_val)); ASSERT_EQ(seq_val, 22);
  ASSERT_SUCC(skip_generator.get_next(seq_val)); ASSERT_EQ(seq_val, 23);
  ASSERT_SUCC(skip_generator.get_next(seq_val)); ASSERT_EQ(seq_val, 24);
  ASSERT_SUCC(skip_generator.get_next(seq_val)); ASSERT_EQ(seq_val, 122);
  ASSERT_SUCC(skip_generator.get_next(seq_val)); ASSERT_EQ(seq_val, 123);
  ASSERT_SUCC(skip_generator.get_next(seq_val)); ASSERT_EQ(seq_val, 124);
  ASSERT_EQ(seq_val, skip_generator.get_current());
  int64_t preview_next_val = -1;
  ASSERT_SUCC(skip_generator.preview_next(seq_val, preview_next_val));
  ASSERT_GT(preview_next_val, seq_val);
  ASSERT_EQ(seq_val, skip_generator.get_current());
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
