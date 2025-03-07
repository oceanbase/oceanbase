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
#define private public
#define protected public

#include "storage/backup/ob_backup_index_compressor.h"

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::backup;
using namespace oceanbase::blocksstable;

namespace oceanbase
{
namespace backup
{

TEST(ObBackupIndexBlockCompressorTest, Compress)
{
  int ret = OB_SUCCESS;

  ObBackupIndexBlockCompressor compressor;
  const int64_t block_size = 16 * 1024;
  const ObCompressorType compressor_type = ObCompressorType::ZSTD_COMPRESSOR;

  ret = compressor.init(block_size, compressor_type);
  EXPECT_EQ(OB_SUCCESS, ret);

  int buffer_size = 16 * 1024;
  char buffer[buffer_size];

  for (int i = 0; i < buffer_size; ++i) {
      buffer[i] = std::rand() % 8;
  }

  const char* in = buffer;
  int64_t in_size = buffer_size;

  const char* out = NULL;
  int64_t out_size = 0;

  ret = compressor.compress(in, in_size, out, out_size);
  EXPECT_EQ(OB_SUCCESS, ret);

  ObBackupIndexBlockCompressor decompressor;

  ret = decompressor.init(block_size, compressor_type);
  EXPECT_EQ(OB_SUCCESS, ret);

  const char *decomp_out = NULL;
  int64_t decomp_size = 0;

  ret = decompressor.decompress(out, out_size, block_size, decomp_out, decomp_size);
  EXPECT_EQ(OB_SUCCESS, ret);

  EXPECT_EQ(in_size, decomp_size);
  EXPECT_LE(out_size, in_size);

  LOG_INFO("compress info", K(in_size), K(out_size), K(decomp_size));
}

}
}

int main(int argc, char **argv)
{
  system("rm -f test_backup_index_compressor.log*");
  ObLogger &logger = ObLogger::get_logger();
  logger.set_file_name("test_backup_index_compressor.log", true);
  logger.set_log_level("info");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
