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

#include "gtest/gtest.h"
#include "ob_log_trans_id_filter.h"

using namespace oceanbase::common;
namespace oceanbase
{
namespace libobcdc
{

class TestObLogTransIDFilter : public ::testing::Test
{
public:
  TestObLogTransIDFilter() {}
  ~TestObLogTransIDFilter() {}
  virtual void SetUp() {}
  virtual void TearDown() {}
};

// NULL and empty string: init succeeds, nothing is filtered
TEST_F(TestObLogTransIDFilter, init_empty)
{
  ObLogTransIDFilter f1;
  EXPECT_EQ(OB_SUCCESS, f1.init(NULL));
  EXPECT_FALSE(f1.should_filter(1001, transaction::ObTransID(100)));

  ObLogTransIDFilter f2;
  EXPECT_EQ(OB_SUCCESS, f2.init(""));
  EXPECT_FALSE(f2.should_filter(1001, transaction::ObTransID(100)));
}

// Single tenant, single trans_id
TEST_F(TestObLogTransIDFilter, single_tenant_single_tx)
{
  ObLogTransIDFilter f;
  EXPECT_EQ(OB_SUCCESS, f.init("1001:100"));
  EXPECT_TRUE(f.should_filter(1001, transaction::ObTransID(100)));
  EXPECT_FALSE(f.should_filter(1001, transaction::ObTransID(200)));
  EXPECT_FALSE(f.should_filter(1002, transaction::ObTransID(100)));
}

// Single tenant, multiple trans_ids
TEST_F(TestObLogTransIDFilter, single_tenant_multi_tx)
{
  ObLogTransIDFilter f;
  EXPECT_EQ(OB_SUCCESS, f.init("1001:100,200,300"));
  EXPECT_TRUE(f.should_filter(1001, transaction::ObTransID(100)));
  EXPECT_TRUE(f.should_filter(1001, transaction::ObTransID(200)));
  EXPECT_TRUE(f.should_filter(1001, transaction::ObTransID(300)));
  EXPECT_FALSE(f.should_filter(1001, transaction::ObTransID(400)));
}

// Multiple tenants
TEST_F(TestObLogTransIDFilter, multi_tenant)
{
  ObLogTransIDFilter f;
  EXPECT_EQ(OB_SUCCESS, f.init("1001:100,200|1002:300,400"));
  EXPECT_TRUE(f.should_filter(1001, transaction::ObTransID(100)));
  EXPECT_TRUE(f.should_filter(1001, transaction::ObTransID(200)));
  EXPECT_FALSE(f.should_filter(1001, transaction::ObTransID(300)));
  EXPECT_TRUE(f.should_filter(1002, transaction::ObTransID(300)));
  EXPECT_TRUE(f.should_filter(1002, transaction::ObTransID(400)));
  EXPECT_FALSE(f.should_filter(1002, transaction::ObTransID(100)));
  EXPECT_FALSE(f.should_filter(9999, transaction::ObTransID(100)));
}

// Duplicate tenant segments are merged
TEST_F(TestObLogTransIDFilter, duplicate_tenant_merge)
{
  ObLogTransIDFilter f;
  EXPECT_EQ(OB_SUCCESS, f.init("1001:100|1001:200"));
  EXPECT_TRUE(f.should_filter(1001, transaction::ObTransID(100)));
  EXPECT_TRUE(f.should_filter(1001, transaction::ObTransID(200)));
}

// Duplicate trans_ids within same segment
TEST_F(TestObLogTransIDFilter, duplicate_tx_id)
{
  ObLogTransIDFilter f;
  EXPECT_EQ(OB_SUCCESS, f.init("1001:100,100,200"));
  EXPECT_TRUE(f.should_filter(1001, transaction::ObTransID(100)));
  EXPECT_TRUE(f.should_filter(1001, transaction::ObTransID(200)));
}

// Invalid segments are skipped, valid ones still work
TEST_F(TestObLogTransIDFilter, invalid_segment_skipped)
{
  // Missing ':' in first segment
  ObLogTransIDFilter f1;
  EXPECT_EQ(OB_SUCCESS, f1.init("bad_segment|1001:100"));
  EXPECT_TRUE(f1.should_filter(1001, transaction::ObTransID(100)));

  // Invalid tenant_id (non-numeric)
  ObLogTransIDFilter f2;
  EXPECT_EQ(OB_SUCCESS, f2.init("abc:100|1002:200"));
  EXPECT_TRUE(f2.should_filter(1002, transaction::ObTransID(200)));
  EXPECT_FALSE(f2.should_filter(0, transaction::ObTransID(100)));

  // Empty trans_id list
  ObLogTransIDFilter f3;
  EXPECT_EQ(OB_SUCCESS, f3.init("1001:|1002:300"));
  EXPECT_FALSE(f3.should_filter(1001, transaction::ObTransID(0)));
  EXPECT_TRUE(f3.should_filter(1002, transaction::ObTransID(300)));
}

// Init twice returns OB_INIT_TWICE
TEST_F(TestObLogTransIDFilter, init_twice)
{
  ObLogTransIDFilter f;
  EXPECT_EQ(OB_SUCCESS, f.init("1001:100"));
  EXPECT_EQ(OB_INIT_TWICE, f.init("1002:200"));
  // First init's data is intact
  EXPECT_TRUE(f.should_filter(1001, transaction::ObTransID(100)));
}

// should_filter before init returns false (no crash)
TEST_F(TestObLogTransIDFilter, filter_before_init)
{
  ObLogTransIDFilter f;
  EXPECT_FALSE(f.should_filter(1001, transaction::ObTransID(100)));
}

// Destroy and re-init
TEST_F(TestObLogTransIDFilter, destroy_and_reinit)
{
  ObLogTransIDFilter f;
  EXPECT_EQ(OB_SUCCESS, f.init("1001:100"));
  EXPECT_TRUE(f.should_filter(1001, transaction::ObTransID(100)));
  f.destroy();
  EXPECT_FALSE(f.should_filter(1001, transaction::ObTransID(100)));
  EXPECT_EQ(OB_SUCCESS, f.init("1002:200"));
  EXPECT_TRUE(f.should_filter(1002, transaction::ObTransID(200)));
  EXPECT_FALSE(f.should_filter(1001, transaction::ObTransID(100)));
}

// Only pipe delimiter — produces empty segments, no filter entries
TEST_F(TestObLogTransIDFilter, pipe_only)
{
  ObLogTransIDFilter f;
  EXPECT_EQ(OB_SUCCESS, f.init("|"));
  EXPECT_FALSE(f.should_filter(1, transaction::ObTransID(1)));
}

// Large number of trans_ids
TEST_F(TestObLogTransIDFilter, many_trans_ids)
{
  // Build "1001:1,2,3,...,1000"
  const int64_t N = 1000;
  char buf[16 * N];
  int64_t pos = snprintf(buf, sizeof(buf), "1001:");
  for (int64_t i = 1; i <= N; ++i) {
    if (i > 1) { pos += snprintf(buf + pos, sizeof(buf) - pos, ","); }
    pos += snprintf(buf + pos, sizeof(buf) - pos, "%ld", i);
  }

  ObLogTransIDFilter f;
  EXPECT_EQ(OB_SUCCESS, f.init(buf));
  for (int64_t i = 1; i <= N; ++i) {
    EXPECT_TRUE(f.should_filter(1001, transaction::ObTransID(i)));
  }
  EXPECT_FALSE(f.should_filter(1001, transaction::ObTransID(N + 1)));
}

} // namespace libobcdc
} // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_ob_cdc_trans_id_filter.log");
  ObLogger &logger = ObLogger::get_logger();
  bool not_output_obcdc_log = true;
  logger.set_file_name("test_ob_cdc_trans_id_filter.log", not_output_obcdc_log, false);
  logger.set_log_level(OB_LOG_LEVEL_DEBUG);
  logger.set_mod_log_levels("ALL.*:DEBUG;TLOG.*:DEBUG");
  logger.set_enable_async_log(false);
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
