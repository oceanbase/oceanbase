/**
* Copyright (c) 2024 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define UNITTEST_DEBUG
#define USING_LOG_PREFIX SHARE
#include "lib/oblog/ob_log_module.h"
#include "lib/string/ob_string.h"
#include "sql/optimizer/file_prune/ob_iceberg_file_pruner.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>


using namespace oceanbase;

class TestPartitionTransform : public ::testing::Test {

};

TEST_F(TestPartitionTransform, bucket_hash) {
  ObNewRange range;
  ObObj start;
  ObObj end;
  range.start_key_ = ObRowkey(&start, 1);
  range.end_key_ = ObRowkey(&end, 1);
  range.border_flag_.set_inclusive_start();
  range.border_flag_.set_inclusive_end();

  // int
  {
    // hash value 1392991556
    start.set_int32(1);
    end.set_int32(1);
    ASSERT_EQ(OB_SUCCESS, sql::ObIcebergFilePrunner::transform_bucket_range(range, 5));
    ASSERT_EQ(1, start.get_int32());
  }

  // date
  {
    // hash value 1176478452
    start.set_date(2);
    end.set_date(2);
    ASSERT_EQ(OB_SUCCESS, sql::ObIcebergFilePrunner::transform_bucket_range(range, 5));
    ASSERT_EQ(2, start.get_int32());
  }

  // long
  {
    // hash value 2017239379
    start.set_int(34);
    end.set_int(34);
    EXPECT_EQ(OB_SUCCESS, sql::ObIcebergFilePrunner::transform_bucket_range(range, 5));
    ASSERT_EQ(4, start.get_int32());
  }

  // time
  {
    // hash value 1066781525
    start.set_time(35);
    end.set_time(35);
    EXPECT_EQ(OB_SUCCESS, sql::ObIcebergFilePrunner::transform_bucket_range(range, 5));
    ASSERT_EQ(0, start.get_int32());
  }

  // datetime
  {
    // hash value 1497025390
    start.set_datetime(36);
    end.set_datetime(36);
    EXPECT_EQ(OB_SUCCESS, sql::ObIcebergFilePrunner::transform_bucket_range(range, 5));
    ASSERT_EQ(0, start.get_int32());
  }

  // timestamp
  {
    // hash value 978785182
    start.set_time(37);
    end.set_time(37);
    EXPECT_EQ(OB_SUCCESS, sql::ObIcebergFilePrunner::transform_bucket_range(range, 5));
    ASSERT_EQ(2, start.get_int32());
  }

  // binary
  {
    // hash value 1210000089
    ObString s = "iceberg";
    start.set_varchar(s);
    end.set_varchar(s);
    EXPECT_EQ(OB_SUCCESS, sql::ObIcebergFilePrunner::transform_bucket_range(range, 5));
    ASSERT_EQ(4, start.get_int32());
  }

  // decimal(14.20)
  {
    // hash value 1646729059
    ObArenaAllocator allocator;
    size_t buf_size = 4;
    char *buf = static_cast<char *>(allocator.alloc(buf_size));
    memset(buf, 0, buf_size);
    buf[0] = 0x8c;
    buf[1] = 0x05;
    ObDecimalInt *decint = reinterpret_cast<ObDecimalInt *>(buf);
    start.set_decimal_int(buf_size, 2, decint);
    end.set_decimal_int(buf_size, 2, decint);
    EXPECT_EQ(OB_SUCCESS, sql::ObIcebergFilePrunner::transform_bucket_range(range, 5));
    EXPECT_EQ(4, start.get_int32());
  }

  // decimal(0.00)
  {
    // hash value 1364076727
    ObArenaAllocator allocator;
    size_t buf_size = 4;
    char *buf = static_cast<char *>(allocator.alloc(buf_size));
    memset(buf, 0, buf_size);
    // buf[0] = 0x8c;
    // buf[1] = 0x05;
    ObDecimalInt *decint = reinterpret_cast<ObDecimalInt *>(buf);
    start.set_decimal_int(buf_size, 2, decint);
    end.set_decimal_int(buf_size, 2, decint);
    EXPECT_EQ(OB_SUCCESS, sql::ObIcebergFilePrunner::transform_bucket_range(range, 5));
    EXPECT_EQ(2, start.get_int32());
  }

  // decimal(-14.20)
  {
    // hash value 667775751
    ObArenaAllocator allocator;
    size_t buf_size = 4;
    char *buf = static_cast<char *>(allocator.alloc(buf_size));
    memset(buf, 0, buf_size);
    buf[0] = 0x74;
    buf[1] = 0xFA;
    buf[2] = 0xFF;
    buf[3] = 0xFF;
    ObDecimalInt *decint = reinterpret_cast<ObDecimalInt *>(buf);
    start.set_decimal_int(buf_size, 2, decint);
    end.set_decimal_int(buf_size, 2, decint);
    EXPECT_EQ(OB_SUCCESS, sql::ObIcebergFilePrunner::transform_bucket_range(range, 5));
    EXPECT_EQ(1, start.get_int32());
  }

  {
    ObArenaAllocator allocator;
    size_t buf_size = 4;
    char *buf = static_cast<char *>(allocator.alloc(buf_size));
    memset(buf, 0, buf_size);
    buf[0] = 0x00;
    buf[1] = 0xFF;
    buf[2] = 0xFF;
    buf[3] = 0xFF;
    ObDecimalInt *decint = reinterpret_cast<ObDecimalInt *>(buf);
    start.set_decimal_int(buf_size, 2, decint);
    end.set_decimal_int(buf_size, 2, decint);
    EXPECT_EQ(OB_SUCCESS, sql::ObIcebergFilePrunner::transform_bucket_range(range, 5));
    EXPECT_EQ(0, start.get_int32());
  }
}

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  OB_LOGGER.set_log_level("INFO");
  return RUN_ALL_TESTS();
}