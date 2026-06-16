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

#define USING_LOG_PREFIX OBLOG

#include "gtest/gtest.h"
#include "lib/string/ob_string.h"
#include "lib/utility/ob_print_utils.h"
#include "logservice/palf/lsn.h"
#include "logservice/libobcdc/src/ob_log_part_trans_task.h"

using namespace oceanbase::common;
using namespace oceanbase::palf;

namespace oceanbase
{
namespace libobcdc
{

class TestDmlUniqueID : public ::testing::Test
{
public:
  TestDmlUniqueID() {}
  ~TestDmlUniqueID() {}
};

// Simulate what to_string_part_trans_info_ does and verify no embedded null bytes
static void build_part_trans_info(const char *tls_str, const int64_t trans_id,
    char *buf, const int64_t buf_size, ObString &result)
{
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, databuff_printf(buf, buf_size, pos, "%s" DELIMITER_STR "%ld", tls_str, trans_id));
  result.assign_ptr(buf, static_cast<int32_t>(pos));
}

static void verify_unique_id_no_null_bytes(const ObString &part_trans_info,
    const uint64_t lsn_val, const uint64_t row_index_in_redo)
{
  LSN lsn(lsn_val);
  DmlStmtUniqueID uid(part_trans_info, lsn, row_index_in_redo);
  ASSERT_TRUE(uid.is_valid());

  const int64_t buf_len = uid.get_dml_unique_id_length();
  char buf[buf_len];
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, uid.customized_to_string(buf, buf_len, pos));

  ASSERT_GT(pos, 0);
  for (int64_t i = 0; i < pos; ++i) {
    ASSERT_NE('\0', buf[i]) << "found null byte at position " << i
        << " in unique_id of length " << pos;
  }

  char expected[256];
  int64_t expected_len = snprintf(expected, sizeof(expected), "%.*s_%lu_%lu",
      static_cast<int>(part_trans_info.length()), part_trans_info.ptr(),
      lsn_val, row_index_in_redo);
  ASSERT_EQ(expected_len, pos);
  ASSERT_EQ(0, memcmp(expected, buf, pos));
}

// Verify get_dml_unique_id_length returns a buffer size >= actual serialized length
TEST_F(TestDmlUniqueID, unique_id_length_sufficient)
{
  const char *info_str = "1042_1002_52267214708";
  ObString part_info(static_cast<int32_t>(strlen(info_str)), info_str);
  LSN lsn(112495476554643UL);
  DmlStmtUniqueID uid(part_info, lsn, 42);
  ASSERT_TRUE(uid.is_valid());

  const int64_t buf_len = uid.get_dml_unique_id_length();
  ASSERT_GT(buf_len, 0);
  char buf[buf_len];
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, uid.customized_to_string(buf, buf_len, pos));
  ASSERT_GT(pos, 0);
  ASSERT_LE(pos, buf_len);
}

// Basic case: small trans_id (no over-estimation in compute_str_length_base_num)
TEST_F(TestDmlUniqueID, basic_small_trans_id)
{
  char buf[128];
  ObString part_info;
  build_part_trans_info("1042_1002", 12345, buf, sizeof(buf), part_info);
  ASSERT_EQ(static_cast<int32_t>(strlen("1042_1002_12345")), part_info.length());
  verify_unique_id_no_null_bytes(part_info, 112495476554643UL, 42);
}

// Key regression case: large trans_id (>= 10^10) where compute_str_length_base_num
// returns MAX_ROW_INDEX_LENGTH=20, which is larger than actual digits.
// Before the fix, to_string_part_trans_info_ used buf_size instead of pos,
// causing trailing null bytes in part_trans_info_str_.
TEST_F(TestDmlUniqueID, large_trans_id_no_trailing_nulls)
{
  char buf[128];
  ObString part_info;

  // trans_id = 52267214708 has 11 digits, but compute_str_length_base_num returns 20
  build_part_trans_info("1042_1002", 52267214708LL, buf, sizeof(buf), part_info);

  const char *expected_str = "1042_1002_52267214708";
  ASSERT_EQ(static_cast<int32_t>(strlen(expected_str)), part_info.length());
  ASSERT_EQ(0, memcmp(expected_str, part_info.ptr(), part_info.length()));

  verify_unique_id_no_null_bytes(part_info, 112495476554643UL, 42);
}

// Demonstrate what the old buggy code would produce: using buf_size causes null bytes
TEST_F(TestDmlUniqueID, buggy_buf_size_causes_null_bytes)
{
  const char *tls_str = "1042_1002";
  const int64_t trans_id = 52267214708L;

  // Replicate the old buf_size calculation from to_string_part_trans_info_:
  // strlen(tls_str) + MAX_ROW_INDEX_LENGTH(20) + sizeof("_")(2) + 1
  // trans_id has 11 digits but MAX_ROW_INDEX_LENGTH is 20, so buf_size > actual length
  const int64_t buf_size = strlen(tls_str) + 20 + sizeof(DELIMITER_STR) + 1;
  char buf[buf_size];
  memset(buf, 0, buf_size);
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, databuff_printf(buf, buf_size, pos, "%s" DELIMITER_STR "%ld", tls_str, trans_id));

  // pos is the actual string length, buf_size is strictly larger
  ASSERT_LT(pos, buf_size);

  // The OLD code did: assign_ptr(buf, buf_size) — includes trailing null bytes
  ObString buggy_str;
  buggy_str.assign_ptr(buf, static_cast<int32_t>(buf_size));
  bool has_null = false;
  for (int32_t i = 0; i < buggy_str.length(); ++i) {
    if (buggy_str.ptr()[i] == '\0') { has_null = true; break; }
  }
  EXPECT_TRUE(has_null) << "buf_size version should contain null bytes";

  // The FIXED code does: assign_ptr(buf, pos) — no trailing null bytes
  ObString fixed_str;
  fixed_str.assign_ptr(buf, static_cast<int32_t>(pos));
  for (int32_t i = 0; i < fixed_str.length(); ++i) {
    ASSERT_NE('\0', fixed_str.ptr()[i]) << "found null byte at position " << i;
  }
}

// Boundary: trans_id exactly at 10^10 boundary
TEST_F(TestDmlUniqueID, boundary_trans_id)
{
  char buf[128];
  ObString part_info;

  // 9999999999 has 10 digits, compute_str_length_base_num returns 10 (exact)
  build_part_trans_info("1_1", 9999999999LL, buf, sizeof(buf), part_info);
  ASSERT_EQ(static_cast<int32_t>(strlen("1_1_9999999999")), part_info.length());
  verify_unique_id_no_null_bytes(part_info, 100, 0);

  // 10000000000 has 11 digits, compute_str_length_base_num returns 20 (over-estimate)
  build_part_trans_info("1_1", 10000000000LL, buf, sizeof(buf), part_info);
  ASSERT_EQ(static_cast<int32_t>(strlen("1_1_10000000000")), part_info.length());
  verify_unique_id_no_null_bytes(part_info, 100, 0);
}

// Multiple varying trans_id sizes
TEST_F(TestDmlUniqueID, various_trans_ids)
{
  const int64_t trans_ids[] = {1, 42, 999, 100000, 9999999999LL, 10000000000LL,
      99999999999LL, 999999999999LL, 52267214708LL};
  char buf[128];
  ObString part_info;

  for (size_t i = 0; i < sizeof(trans_ids) / sizeof(trans_ids[0]); ++i) {
    SCOPED_TRACE(::testing::Message() << "trans_id=" << trans_ids[i]);
    build_part_trans_info("1042_1002", trans_ids[i], buf, sizeof(buf), part_info);

    char expected[128];
    snprintf(expected, sizeof(expected), "1042_1002_%ld", trans_ids[i]);
    ASSERT_EQ(static_cast<int32_t>(strlen(expected)), part_info.length());

    verify_unique_id_no_null_bytes(part_info, 500, 7);
  }
}

} // namespace libobcdc
} // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_dml_unique_id.log");
  ObLogger &logger = ObLogger::get_logger();
  logger.set_file_name("test_dml_unique_id.log", true, false);
  logger.set_log_level(OB_LOG_LEVEL_DEBUG);
  logger.set_enable_async_log(false);
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
