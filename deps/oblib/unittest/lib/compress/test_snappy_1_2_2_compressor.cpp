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
#include <string.h>
#include "lib/compress/snappy_1_2_2/ob_snappy_compressor_1_2_2.h"
#include "lib/allocator/ob_malloc.h"
#include "lib/compress/snappy/snappy_src/snappy.h"

using namespace oceanbase::common;
using namespace oceanbase::common::snappy_1_2_2;

namespace oceanbase
{
namespace unittest
{

class TestSnappy1_2_2Compressor : public ::testing::Test
{
public:
  TestSnappy1_2_2Compressor() : allocator_(ObModIds::TEST) {}
  virtual ~TestSnappy1_2_2Compressor() {}

  virtual void SetUp()
  {
    compressor_ = new (std::nothrow) ObSnappyCompressor1_2_2(allocator_);
    ASSERT_NE(nullptr, compressor_);
  }

  virtual void TearDown()
  {
    if (compressor_) {
      delete compressor_;
      compressor_ = nullptr;
    }
  }

protected:
  ObMalloc allocator_;
  ObSnappyCompressor1_2_2 *compressor_;
};

TEST_F(TestSnappy1_2_2Compressor, test_1_2_2_compressor)
{
  // First compress some data to get compressed buffer
  const char *src_data = "This is a test string for snappy 1.1.8 compression. "
                         "It contains some repeated patterns like: test test test.";
  int64_t src_data_size = strlen(src_data);

  // Compress the data
  int64_t max_overflow_size = 0;
  ASSERT_EQ(OB_SUCCESS, compressor_->get_max_overflow_size(src_data_size, max_overflow_size));

  int64_t compress_buffer_size = src_data_size + max_overflow_size;
  char *compress_buffer = (char*)allocator_.alloc(compress_buffer_size);
  ASSERT_NE(nullptr, compress_buffer);

  int64_t compressed_size = 0;
  int ret = compressor_->compress(src_data, src_data_size,
                                  compress_buffer, compress_buffer_size,
                                  compressed_size);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_GT(compressed_size, 0);

  // Now test decompression
  char *decompress_buffer = (char*)allocator_.alloc(src_data_size);
  ASSERT_NE(nullptr, decompress_buffer);

  int64_t decompressed_size = 0;
  ret = compressor_->decompress(compress_buffer, compressed_size,
                                decompress_buffer, src_data_size,
                                decompressed_size);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(decompressed_size, src_data_size);
  ASSERT_EQ(0, memcmp(src_data, decompress_buffer, src_data_size));

  // Cleanup
  allocator_.free(compress_buffer);
  allocator_.free(decompress_buffer);
}

TEST_F(TestSnappy1_2_2Compressor, test_normal_decompress)
{
  const char *src_data = "This is a test string for snappy normal decompression. ";
  int64_t src_data_size = strlen(src_data);
  int64_t max_overflow_size = snappy::MaxCompressedLength(src_data_size);
  char *compress_buffer = (char*)allocator_.alloc(max_overflow_size);
  ASSERT_NE(nullptr, compress_buffer);
  size_t compressed_size = 0;
  snappy::RawCompress(src_data, src_data_size, compress_buffer, &compressed_size);
  char *decompress_buffer = (char*)allocator_.alloc(src_data_size);
  ASSERT_NE(nullptr, decompress_buffer);

  bool tmp_ret = snappy::RawUncompress(compress_buffer, compressed_size, decompress_buffer);
  ASSERT_EQ(true, tmp_ret);
  ASSERT_EQ(0, memcmp(src_data, decompress_buffer, src_data_size));

  allocator_.free(compress_buffer);
  allocator_.free(decompress_buffer);
}
} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  OB_LOGGER.set_log_level("INFO");
  return RUN_ALL_TESTS();
}
