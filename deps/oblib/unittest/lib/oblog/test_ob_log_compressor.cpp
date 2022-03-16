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
#include "lib/oblog/ob_log_compressor.h"
#include "lib/compress/ob_compressor_pool.h"
#include "lib/ob_errno.h"
#include "lib/string/ob_string.h"

using namespace oceanbase::lib;

namespace oceanbase {
namespace common {

TEST(ObLogCompressor, normal)
{
  int ret = OB_SUCCESS;
  int test_count = 1000;
  int test_size = test_count * sizeof(int);
  ObLogCompressor log_compressor;
  ObCompressor *compressor;

  // normal init
  ret = log_compressor.init();
  ASSERT_EQ(OB_SUCCESS, ret);

  // repeat init
  ret = log_compressor.init();
  ASSERT_EQ(OB_INIT_TWICE, ret);

  // prepare data
  ObString file_name = "test_ob_log_compressor_file";
  FILE *input_file = fopen(file_name.ptr(), "w");
  ASSERT_EQ(true, NULL != input_file);
  int data[test_count];
  for (int i = 0; i < test_count; i++) {
    data[i] = i;
  }
  ret = fwrite(data, 1, test_size, input_file);
  ASSERT_EQ(test_size, ret);
  fclose(input_file);

  // normal append
  ret = log_compressor.append_log(file_name);
  ASSERT_EQ(OB_SUCCESS, ret);

  // get compression result
  sleep(2);
  ObString compression_file_name = log_compressor.get_compression_file_name(file_name);
  ASSERT_EQ(0, access(compression_file_name.ptr(), F_OK));
  FILE *output_file = fopen(compression_file_name.ptr(), "r");
  ASSERT_EQ(true, NULL != output_file);
  int buf_size = test_size + 512;
  int read_size = 0;
  void *buf = malloc(buf_size);
  ASSERT_EQ(true, NULL != buf);
  read_size = fread(buf, 1, buf_size, output_file);
  ASSERT_GT(read_size, 0);
  fclose(output_file);

  // check decompression result
  int64_t decomp_size = 0;
  int decomp_buf_size = buf_size;
  int *decomp_buf = (int *)malloc(decomp_buf_size);
  ASSERT_EQ(true, NULL != decomp_buf);
  compressor = (ObCompressor *)log_compressor.get_compressor();
  ASSERT_EQ(true, NULL != compressor);
  ret = compressor->decompress((char *)buf, read_size, (char *)decomp_buf, decomp_buf_size, decomp_size);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(test_size, decomp_size);
  for (int i = 0; i < test_count; i++) {
    ASSERT_EQ(*(decomp_buf + i), i);
  }

  // clear environment
  free(buf);
  free(decomp_buf);
  ASSERT_NE(0, access(file_name.ptr(), F_OK));
  ASSERT_EQ(0, access(compression_file_name.ptr(), F_OK));
  unlink(compression_file_name.ptr());

  // destroy and init
  log_compressor.destroy();
  ret = log_compressor.init();
  ASSERT_EQ(OB_SUCCESS, ret);

  // repeat destroy
  log_compressor.destroy();
  log_compressor.destroy();
}

}  // namespace common
}  // namespace oceanbase

int main(int argc, char **argv)
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
