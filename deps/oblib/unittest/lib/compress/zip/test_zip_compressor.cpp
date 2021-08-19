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

#include "lib/compress/zip/ob_zip_compressor.h"

#include "gtest/gtest.h"

using namespace oceanbase::common;
namespace oceanbase {
namespace unittest {
class TestObZipCompressor : public ::testing::Test {
public:
  virtual void SetUp()
  {}
  virtual void TearDown()
  {}
};


TEST_F(TestObZipCompressor, test_compress)
{
  int ret = OB_SUCCESS;
  ObZipCompressor compressor;
  int test_count = 1000;
  int test_size  = test_count * sizeof(int);
  int src_size   = 2 * 1024 * 1024;
  int dest_size  = src_size;
  
  char *src_buf  = (char *)malloc(src_size);
  ASSERT_EQ(true, NULL != src_buf);

  char *dest_buf = (char *)malloc(dest_size);
  ASSERT_EQ(true, NULL != dest_buf);

  // prepare src file data
  const char* file_name = "test_zip_compressor_file";
  FILE *input_file = NULL;
  input_file = fopen(file_name, "w");
  ASSERT_EQ(true, NULL != input_file);
  int data[test_count];
  for (int i = 0; i < test_count; i++) {
    data[i] = i;
  }
  ret = fwrite(data, 1, test_size, input_file);
  ASSERT_EQ(test_size, ret);
  fclose(input_file);
  input_file = NULL;

  const char* compression_file_name = "test_zip_compressor_file.zip";
  FILE *output_file = NULL;
  output_file = fopen(compression_file_name, "w");
  ASSERT_EQ(true, NULL != output_file);

  input_file = fopen(file_name, "r");
  ASSERT_EQ(true, NULL != input_file);

  int64_t read_size = 0;
  int64_t write_size = 0;
  // get a fake file header and the file name
  ret = compressor.compress(src_buf, read_size, dest_buf, write_size, ObZipCompressFlag::FAKE_FILE_HEADER, file_name); 
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(write_size, fwrite(dest_buf, 1, write_size, output_file));


  while(OB_SUCC(ret) && !feof(input_file)) {
    if ((read_size = fread(src_buf, 1, src_size, input_file)) > 0) {
      // compress every part of data
      ret = compressor.compress(src_buf, read_size, dest_buf, write_size, ObZipCompressFlag::DATA, nullptr);
      ASSERT_EQ(OB_SUCCESS, ret);
      ASSERT_EQ(write_size, fwrite(dest_buf, 1, write_size, output_file));
    }
  }

  // get an extra compress data
  ret = compressor.compress(src_buf, read_size, dest_buf, write_size, ObZipCompressFlag::LAST_DATA, file_name);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(write_size, fwrite(dest_buf, 1, write_size, output_file));

  // get the real file header
  fseek(output_file, 0, SEEK_SET);
  ret = compressor.compress(src_buf, read_size, dest_buf, write_size, ObZipCompressFlag::FILE_HEADER, file_name);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(write_size, fwrite(dest_buf, 1, write_size, output_file));

  // get the tail, include the central directory file header and the end of central directory record
  fseek(output_file, 0, SEEK_END);
  ret = compressor.compress(src_buf, read_size, dest_buf, write_size, ObZipCompressFlag::TAIL, file_name);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(write_size, fwrite(dest_buf, 1, write_size, output_file));

  fclose(input_file);
  fclose(output_file);
  
  free(src_buf);
  free(dest_buf);
}

}  // end namespace unittest
}  // end namespace oceanbase

int main(int argc, char** argv)
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}