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
#include "lib/compress/ob_compressor_pool.h"


namespace oceanbase
{
namespace common
{
void test_normal(const char *compressor_name)
{
  int ret = OB_SUCCESS;
  ObCompressorPool &cp = ObCompressorPool::get_instance();
  ObCompressor *compressor = NULL;

  cp.get_compressor(compressor_name, compressor);
  ASSERT_EQ(OB_SUCCESS, ret);
  EXPECT_TRUE(NULL != compressor);
  EXPECT_TRUE(0 == strcmp(compressor_name, compressor->get_compressor_name()));
  ObCompressorType compressor_type;
  cp.get_compressor_type(compressor_name, compressor_type);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(compressor_type, compressor->get_compressor_type());
}

void test_stream(const char *compressor_name)
{
  int ret = OB_SUCCESS;
  ObCompressorPool &cp = ObCompressorPool::get_instance();
  ObStreamCompressor *compressor = NULL;

  cp.get_stream_compressor(compressor_name, compressor);
  ASSERT_EQ(OB_SUCCESS, ret);
  EXPECT_TRUE(NULL != compressor);
  EXPECT_TRUE(0 == strcmp(compressor_name, compressor->get_compressor_name()));
  ObCompressorType compressor_type;
  cp.get_compressor_type(compressor_name, compressor_type);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(compressor_type, compressor->get_compressor_type());
}

TEST(ObCompressorPool, test_invalid)
{
  int ret = OB_SUCCESS;
  ObCompressorPool &cp = ObCompressorPool::get_instance();
  ObCompressor *compressor = NULL;

  //test invalid argument
  cp.get_compressor(NULL, compressor);
  ASSERT_EQ(OB_INVALID, ret);
  EXPECT_EQ(NULL, compressor);

  //test not exist compressor
  cp.get_compressor("oceanbase", compressor);
  ASSERT_EQ(OB_SUCCESS, ret);
  EXPECT_EQ(NULL, compressor);
}

TEST(ObCompressorPool, test_normal_compressor)
{
  test_normal("none");
  test_normal("lz4_1.0");
  test_normal("snappy_1.0");
  test_normal("zlib_1.0");
  test_normal("zstd_1.0");
  test_normal("zstd_1.3.8");
  test_normal("lz4_1.9.1");
}

TEST(ObCompressorPool, test_stream_compressor)
{
  test_stream("stream_lz4_1.0");
  test_stream("stream_zstd_1.0");
  test_stream("stream_zstd_1.3.8");
}
}
}


int main(int argc, char** argv)
{
  system("rm -f test_compress_pool.log*");
  OB_LOGGER.set_file_name("test_compress_pool.log", true, true);
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

