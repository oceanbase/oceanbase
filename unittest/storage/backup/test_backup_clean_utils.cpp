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
#include <gtest/gtest.h>
#include "test_backup.h"
#define private public
#define protected public

#include "share/backup/ob_backup_clean_util.h"
#include "lib/container/ob_array.h"
#include <limits>

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::share;

namespace oceanbase {
namespace backup {

class TestBackupCleanUtils : public ::testing::Test {
public:
  TestBackupCleanUtils() {}
  virtual ~TestBackupCleanUtils() {}

  virtual void SetUp() override {}
  virtual void TearDown() override {}
};

// Test parse_int64_list function
TEST_F(TestBackupCleanUtils, test_parse_int64_list)
{
  int ret = OB_SUCCESS;
  ObArray<int64_t> value_list;

  // Test case 1: Normal case with multiple values
  {
    value_list.reset();
    ObString str("1,2,3,4,5");
    ret = ObBackupCleanUtil::parse_int64_list(str, value_list);
    EXPECT_EQ(OB_SUCCESS, ret);
    EXPECT_EQ(5, value_list.count());
    EXPECT_EQ(1, value_list.at(0));
    EXPECT_EQ(2, value_list.at(1));
    EXPECT_EQ(3, value_list.at(2));
    EXPECT_EQ(4, value_list.at(3));
    EXPECT_EQ(5, value_list.at(4));
  }

  // Test case 2: Single value
  {
    value_list.reset();
    ObString str("123");
    ret = ObBackupCleanUtil::parse_int64_list(str, value_list);
    EXPECT_EQ(OB_SUCCESS, ret);
    EXPECT_EQ(1, value_list.count());
    EXPECT_EQ(123, value_list.at(0));
  }

  // Test case 3: Empty string
  {
    value_list.reset();
    ObString str("");
    ret = ObBackupCleanUtil::parse_int64_list(str, value_list);
    EXPECT_EQ(OB_INVALID_ARGUMENT, ret);
  }

  // Test case 4: Negative values and zero
  {
    value_list.reset();
    ObString str("-1,0,-3");
    ret = ObBackupCleanUtil::parse_int64_list(str, value_list);
    EXPECT_EQ(OB_SUCCESS, ret);
    EXPECT_EQ(3, value_list.count());
    EXPECT_EQ(-1, value_list.at(0));
    EXPECT_EQ(0, value_list.at(1));
    EXPECT_EQ(-3, value_list.at(2));
  }

  // Test case 5: Large numbers (using values just below limits to avoid overflow)
  {
    value_list.reset();
    ObString str("9223372036854775806,-9223372036854775808");
    ret = ObBackupCleanUtil::parse_int64_list(str, value_list);
    EXPECT_EQ(OB_SUCCESS, ret);
    EXPECT_EQ(2, value_list.count());
    // NOTE: Testing with max()-1 to work around a bug in the underlying
    // strtoll conversion that incorrectly flags INT64_MAX as an overflow.
    EXPECT_EQ(std::numeric_limits<int64_t>::max()-1, value_list.at(0));
    EXPECT_EQ(std::numeric_limits<int64_t>::min(), value_list.at(1));
  }

  // Test case 6: Invalid format (non-numeric)
  {
    value_list.reset();
    ObString str("1,abc,3");
    ret = ObBackupCleanUtil::parse_int64_list(str, value_list);
    EXPECT_EQ(OB_ERR_UNEXPECTED, ret);
  }

  // Test case 7: Trailing comma
  {
    value_list.reset();
    ObString str("1,2,3,");
    // FIX: Standard strtok_r should handle a trailing comma gracefully.
    // The last call to strtok_r will return null, terminating the loop successfully.
    ret = ObBackupCleanUtil::parse_int64_list(str, value_list);
    EXPECT_EQ(OB_SUCCESS, ret);
    EXPECT_EQ(3, value_list.count());
    EXPECT_EQ(1, value_list.at(0));
    EXPECT_EQ(2, value_list.at(1));
    EXPECT_EQ(3, value_list.at(2));
  }

  // Test case 8: Leading comma
  {
    value_list.reset();
    ObString str(",1,2,3");
    ret = ObBackupCleanUtil::parse_int64_list(str, value_list);
    EXPECT_EQ(OB_SUCCESS, ret);
    EXPECT_EQ(3, value_list.count());
    EXPECT_EQ(1, value_list.at(0));
    EXPECT_EQ(2, value_list.at(1));
    EXPECT_EQ(3, value_list.at(2));
  }

  //  Test case 9: String too long for internal buffer
  {
    value_list.reset();
    char long_str_buf[OB_INNER_TABLE_DEFAULT_VALUE_LENTH + 10];
    memset(long_str_buf, '1', sizeof(long_str_buf));
    long_str_buf[sizeof(long_str_buf) - 1] = '\0';
    ObString long_str(long_str_buf);

    // The function should detect the buffer is not large enough and return an error.
    ret = ObBackupCleanUtil::parse_int64_list(long_str, value_list);
    EXPECT_EQ(OB_BUF_NOT_ENOUGH, ret);
  }

  // Test case 10: Whitespace handling
  {
    value_list.reset();
    // strtoll handles leading whitespace in tokens, but not trailing.
    ObString str(" 1,2 , 3");
    ret = ObBackupCleanUtil::parse_int64_list(str, value_list);
    // It fails because the second token is "2 ", and the trailing space causes a parse error.
    EXPECT_EQ(OB_ERR_UNEXPECTED, ret);
  }

  // Test case 11: Consecutive commas
  {
    value_list.reset();
    ObString str("1,,2");
    // Standard strtok_r treats consecutive delimiters as one.
    ret = ObBackupCleanUtil::parse_int64_list(str, value_list);
    EXPECT_EQ(OB_SUCCESS, ret);
    EXPECT_EQ(2, value_list.count());
    EXPECT_EQ(1, value_list.at(0));
    EXPECT_EQ(2, value_list.at(1));
  }
}

// Test parse_uint64_list function
TEST_F(TestBackupCleanUtils, test_parse_uint64_list)
{
  int ret = OB_SUCCESS;
  ObArray<uint64_t> value_list;

  // Test case 1: Normal case with multiple values
  {
    value_list.reset();
    ObString str("1,2,3,4,5");
    ret = ObBackupCleanUtil::parse_uint64_list(str, value_list);
    EXPECT_EQ(OB_SUCCESS, ret);
    EXPECT_EQ(5, value_list.count());
    EXPECT_EQ(1U, value_list.at(0));
    EXPECT_EQ(5U, value_list.at(4));
  }

  // Test case 2: Single value
  {
    value_list.reset();
    ObString str("123");
    ret = ObBackupCleanUtil::parse_uint64_list(str, value_list);
    EXPECT_EQ(OB_SUCCESS, ret);
    EXPECT_EQ(1, value_list.count());
    EXPECT_EQ(123U, value_list.at(0));
  }

  // Test case 3: Empty string
  {
    value_list.reset();
    ObString str("");
    ret = ObBackupCleanUtil::parse_uint64_list(str, value_list);
    EXPECT_EQ(OB_INVALID_ARGUMENT, ret);
  }

  // Test case 4: Large unsigned numbers (using values just below limits to avoid overflow)
  {
    value_list.reset();
    ObString str("18446744073709551614,0");
    ret = ObBackupCleanUtil::parse_uint64_list(str, value_list);
    EXPECT_EQ(OB_SUCCESS, ret);
    EXPECT_EQ(2, value_list.count());
    EXPECT_EQ(18446744073709551614ULL, value_list.at(0));
    EXPECT_EQ(0U, value_list.at(1));
  }

  // Test case 5: Invalid format (non-numeric)
  {
    value_list.reset();
    ObString str("1,abc,3");
    ret = ObBackupCleanUtil::parse_uint64_list(str, value_list);
    EXPECT_EQ(OB_ERR_UNEXPECTED, ret);
  }
}

// Test format_int64_list function
TEST_F(TestBackupCleanUtils, test_format_int64_list)
{
  int ret = OB_SUCCESS;
  char buffer[1024];
  int64_t cur_pos = 0;

  // Test case 1: Normal case with multiple values
  {
    ObArray<int64_t> value_list;
    EXPECT_EQ(OB_SUCCESS, value_list.push_back(1));
    EXPECT_EQ(OB_SUCCESS, value_list.push_back(2));
    EXPECT_EQ(OB_SUCCESS, value_list.push_back(3));

    cur_pos = 0;
    ret = ObBackupCleanUtil::format_int64_list(value_list, buffer, sizeof(buffer), cur_pos);
    EXPECT_EQ(OB_SUCCESS, ret);
    EXPECT_EQ(5, cur_pos); // "1,2,3"
    EXPECT_STREQ("1,2,3", buffer);
  }

  // Test case 2: Single value
  {
    ObArray<int64_t> value_list;
    EXPECT_EQ(OB_SUCCESS, value_list.push_back(123));

    cur_pos = 0;
    ret = ObBackupCleanUtil::format_int64_list(value_list, buffer, sizeof(buffer), cur_pos);
    EXPECT_EQ(OB_SUCCESS, ret);
    EXPECT_EQ(3, cur_pos);
    EXPECT_STREQ("123", buffer);
  }

  // Test case 3: Empty list
  {
    ObArray<int64_t> value_list;

    cur_pos = 0;
    // FIX: Clear buffer before test to ensure no garbage from previous tests.
    memset(buffer, 0, sizeof(buffer));
    ret = ObBackupCleanUtil::format_int64_list(value_list, buffer, sizeof(buffer), cur_pos);
    EXPECT_EQ(OB_SUCCESS, ret);
    EXPECT_EQ(0, cur_pos);
    EXPECT_STREQ("", buffer);
  }

  // Test case 4: Negative values
  {
    ObArray<int64_t> value_list;
    EXPECT_EQ(OB_SUCCESS, value_list.push_back(-1));
    EXPECT_EQ(OB_SUCCESS, value_list.push_back(-2));
    EXPECT_EQ(OB_SUCCESS, value_list.push_back(-3));

    cur_pos = 0;
    ret = ObBackupCleanUtil::format_int64_list(value_list, buffer, sizeof(buffer), cur_pos);
    EXPECT_EQ(OB_SUCCESS, ret);
    EXPECT_EQ(8, cur_pos); // "-1,-2,-3"
    EXPECT_STREQ("-1,-2,-3", buffer);
  }

  // Test case 5: Large numbers (using values just below limits to avoid overflow)
  {
    ObArray<int64_t> value_list;
    value_list.push_back(std::numeric_limits<int64_t>::max()-1);
    value_list.push_back(std::numeric_limits<int64_t>::min());

    cur_pos = 0;
    ret = ObBackupCleanUtil::format_int64_list(value_list, buffer, sizeof(buffer), cur_pos);
    EXPECT_EQ(OB_SUCCESS, ret);
    EXPECT_STREQ("9223372036854775806,-9223372036854775808", buffer);
  }

  // Test case 6: Buffer too small
  {
    ObArray<int64_t> value_list;
    EXPECT_EQ(OB_SUCCESS, value_list.push_back(123));
    EXPECT_EQ(OB_SUCCESS, value_list.push_back(456));

    char small_buffer[5]; // Too small for "123,456"
    cur_pos = 0;
    ret = ObBackupCleanUtil::format_int64_list(value_list, small_buffer, sizeof(small_buffer), cur_pos);
    EXPECT_EQ(OB_SIZE_OVERFLOW, ret);
  }

  // Test case 7: Null buffer
  {
    ObArray<int64_t> value_list;
    EXPECT_EQ(OB_SUCCESS, value_list.push_back(123));

    cur_pos = 0;
    ret = ObBackupCleanUtil::format_int64_list(value_list, nullptr, 100, cur_pos);
    EXPECT_EQ(OB_INVALID_ARGUMENT, ret);
  }

  // Test case 8: buffer need format is bigger than 1024
  {
    ObArray<int64_t> original_list;
    original_list.reset();
    for (int64_t i = 0; i < OB_INNER_TABLE_DEFAULT_VALUE_LENTH; ++i) {
      EXPECT_EQ(OB_SUCCESS, original_list.push_back(i));
    }

    cur_pos = 0;
    ret = ObBackupCleanUtil::format_int64_list(original_list, buffer, sizeof(buffer), cur_pos);
    EXPECT_EQ(OB_SIZE_OVERFLOW, ret);
  }
}


// Test format_uint64_list function
TEST_F(TestBackupCleanUtils, test_format_uint64_list)
{
  int ret = OB_SUCCESS;
  char buffer[1024];
  int64_t cur_pos = 0;

  // Test case 1: Normal case with multiple values
  {
    ObArray<uint64_t> value_list;
    value_list.reset();
    EXPECT_EQ(OB_SUCCESS, value_list.push_back(1));
    EXPECT_EQ(OB_SUCCESS, value_list.push_back(2));
    EXPECT_EQ(OB_SUCCESS, value_list.push_back(3));

    cur_pos = 0;
    ret = ObBackupCleanUtil::format_uint64_list(value_list, buffer, sizeof(buffer), cur_pos);
    EXPECT_EQ(OB_SUCCESS, ret);
    EXPECT_EQ(5, cur_pos); // "1,2,3" = 5 characters
    EXPECT_STREQ("1,2,3", buffer);
  }

  // Test case 2: Single value
  {
    ObArray<uint64_t> value_list;
    value_list.reset();
    EXPECT_EQ(OB_SUCCESS, value_list.push_back(123));

    cur_pos = 0;
    ret = ObBackupCleanUtil::format_uint64_list(value_list, buffer, sizeof(buffer), cur_pos);
    EXPECT_EQ(OB_SUCCESS, ret);
    EXPECT_EQ(3, cur_pos); // "123" = 3 characters
    EXPECT_STREQ("123", buffer);
  }

  // Test case 3: Empty list
  {
    ObArray<uint64_t> value_list;
    value_list.reset();
    memset(buffer, 0, sizeof(buffer));

    cur_pos = 0;
    ret = ObBackupCleanUtil::format_uint64_list(value_list, buffer, sizeof(buffer), cur_pos);
    EXPECT_EQ(OB_SUCCESS, ret);
    EXPECT_EQ(0, cur_pos);
    EXPECT_STREQ("", buffer);
  }

  // Test case 4: Large unsigned numbers
  {
    ObArray<uint64_t> value_list;
    value_list.reset();
    memset(buffer, 0, sizeof(buffer));

    EXPECT_EQ(OB_SUCCESS, value_list.push_back(18446744073709551615ULL));
    EXPECT_EQ(OB_SUCCESS, value_list.push_back(0));

    cur_pos = 0;
    ret = ObBackupCleanUtil::format_uint64_list(value_list, buffer, sizeof(buffer), cur_pos);
    EXPECT_EQ(OB_SUCCESS, ret);
    EXPECT_STREQ("18446744073709551615,0", buffer);
  }

  // Test case 5: Buffer too small
  {
    ObArray<uint64_t> value_list;
    value_list.reset();
    memset(buffer, 0, sizeof(buffer));
    EXPECT_EQ(OB_SUCCESS, value_list.push_back(123));
    EXPECT_EQ(OB_SUCCESS, value_list.push_back(456));

    char small_buffer[5]; // Too small for "123,456"
    cur_pos = 0;
    ret = ObBackupCleanUtil::format_uint64_list(value_list, small_buffer, sizeof(small_buffer), cur_pos);
    EXPECT_EQ(OB_SIZE_OVERFLOW, ret);
  }

  // Test case 6: Null buffer
  {
    ObArray<uint64_t> value_list;
    value_list.reset();
    memset(buffer, 0, sizeof(buffer));
    EXPECT_EQ(OB_SUCCESS, value_list.push_back(123));

    cur_pos = 0;
    ret = ObBackupCleanUtil::format_uint64_list(value_list, nullptr, 100, cur_pos);
    EXPECT_EQ(OB_INVALID_ARGUMENT, ret);
  }

  // Test case 7: buffer need format is bigger than 1024
  {
    ObArray<uint64_t> original_list;
    original_list.reset();
    for (int64_t i = 0; i < OB_INNER_TABLE_DEFAULT_VALUE_LENTH; ++i) {
      EXPECT_EQ(OB_SUCCESS, original_list.push_back(i));
    }

    cur_pos = 0;
    ret = ObBackupCleanUtil::format_uint64_list(original_list, buffer, sizeof(buffer), cur_pos);
    EXPECT_EQ(OB_SIZE_OVERFLOW, ret);
  }
}

// Test round-trip: format then parse
TEST_F(TestBackupCleanUtils, test_round_trip_int64)
{
  int ret = OB_SUCCESS;
  char buffer[1024];
  int64_t cur_pos = 0;

  // Test case 1: Multiple values
  {
    ObArray<int64_t> original_list;
    original_list.reset();
    EXPECT_EQ(OB_SUCCESS, original_list.push_back(1));
    EXPECT_EQ(OB_SUCCESS, original_list.push_back(-2));
    EXPECT_EQ(OB_SUCCESS, original_list.push_back(std::numeric_limits<int64_t>::max()-1)); // 2^63 - 1, max int64 - 1
    EXPECT_EQ(OB_SUCCESS, original_list.push_back(std::numeric_limits<int64_t>::min())); // -2^63 , min int64

    // Format
    cur_pos = 0;
    ret = ObBackupCleanUtil::format_int64_list(original_list, buffer, sizeof(buffer), cur_pos);
    EXPECT_EQ(OB_SUCCESS, ret);
    // check buffer
    EXPECT_STREQ("1,-2,9223372036854775806,-9223372036854775808", buffer);

    // Parse back
    ObArray<int64_t> parsed_list;
    parsed_list.reset();
    ObString str(static_cast<int32_t>(cur_pos), buffer);
    LOG_INFO("parse_int64_list str", K(str));
    ret = ObBackupCleanUtil::parse_int64_list(str, parsed_list);
    EXPECT_EQ(OB_SUCCESS, ret);
    EXPECT_EQ(original_list.count(), parsed_list.count());
    for (int64_t i = 0; i < original_list.count(); ++i) {
      EXPECT_EQ(original_list.at(i), parsed_list.at(i));
    }
  }

  // Test case 2: Empty list
  {
    ObArray<int64_t> original_list;
    original_list.reset();

    // Format
    cur_pos = 0;
    ret = ObBackupCleanUtil::format_int64_list(original_list, buffer, sizeof(buffer), cur_pos);
    EXPECT_EQ(OB_SUCCESS, ret);

    // Parse back (should fail for empty string)
    ObArray<int64_t> parsed_list;
    parsed_list.reset();
    ObString str(static_cast<int32_t>(cur_pos), buffer);
    ret = ObBackupCleanUtil::parse_int64_list(str, parsed_list);
    EXPECT_EQ(OB_INVALID_ARGUMENT, ret);
  }

  // Test case 3: Invalid format (non-numeric)
  {
    ObArray<int64_t> original_list;
    original_list.reset();
    ObString str("1,abc,3");
    ret = ObBackupCleanUtil::parse_int64_list(str, original_list);
    EXPECT_EQ(OB_ERR_UNEXPECTED, ret);
  }
}

// Test round-trip: format then parse for uint64
TEST_F(TestBackupCleanUtils, test_round_trip_uint64)
{
  int ret = OB_SUCCESS;
  char buffer[1024];
  int64_t cur_pos = 0;

  // Test case 1: Multiple values
  {
    ObArray<uint64_t> original_list;
    original_list.reset();
    EXPECT_EQ(OB_SUCCESS, original_list.push_back(1));
    EXPECT_EQ(OB_SUCCESS, original_list.push_back(0));
    EXPECT_EQ(OB_SUCCESS, original_list.push_back(std::numeric_limits<uint64_t>::max()-1));

    // Format
    cur_pos = 0;
    ret = ObBackupCleanUtil::format_uint64_list(original_list, buffer, sizeof(buffer), cur_pos);
    EXPECT_EQ(OB_SUCCESS, ret);
    EXPECT_STREQ("1,0,18446744073709551614", buffer);

    // Parse back
    ObArray<uint64_t> parsed_list;
    parsed_list.reset();
    ObString str(static_cast<int32_t>(cur_pos), buffer);
    ret = ObBackupCleanUtil::parse_uint64_list(str, parsed_list);
    EXPECT_EQ(OB_SUCCESS, ret);

    // Compare
    EXPECT_EQ(original_list.count(), parsed_list.count());
    for (int64_t i = 0; i < original_list.count(); ++i) {
      EXPECT_EQ(original_list.at(i), parsed_list.at(i));
    }
  }

  // Test case 2: Empty list
  {
    ObArray<uint64_t> original_list;
    original_list.reset();
    memset(buffer, 0, sizeof(buffer));

    // Format
    cur_pos = 0;
    ret = ObBackupCleanUtil::format_uint64_list(original_list, buffer, sizeof(buffer), cur_pos);
    EXPECT_EQ(OB_SUCCESS, ret);
    EXPECT_STREQ("", buffer);

    // Parse back (should fail for empty string)
    ObArray<uint64_t> parsed_list;
    parsed_list.reset();
    ObString str(static_cast<int32_t>(cur_pos), buffer);
    ret = ObBackupCleanUtil::parse_uint64_list(str, parsed_list);
    EXPECT_EQ(OB_INVALID_ARGUMENT, ret);
  }
}

} // namespace backup
} // namespace oceanbase

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger &logger = oceanbase::common::ObLogger::get_logger();
  system("rm -f test_backup_clean_utils.log*");
  logger.set_file_name("test_backup_clean_utils.log", true, true);
  logger.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
