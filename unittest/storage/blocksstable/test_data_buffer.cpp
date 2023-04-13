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
#include <signal.h>
#include "storage/blocksstable/ob_data_buffer.h"
#include "common/object/ob_object.h"
#include "storage/blocksstable/ob_block_sstable_struct.h"
#include "share/redolog/ob_log_definition.h"

namespace oceanbase
{
using namespace common;
namespace blocksstable
{

class TestDataBuffer : public ::testing::Test
{
public:
  TestDataBuffer();
  virtual ~TestDataBuffer();
  virtual void SetUp();
  virtual void TearDown();
protected:
  char *buf_;
  int64_t buf_size_;
  static constexpr int64_t ALIGNED_SIZE = ObLogConstants::LOG_FILE_ALIGN_SIZE;
};

TestDataBuffer::TestDataBuffer()
  : buf_(NULL),
    buf_size_(0)
{

}

TestDataBuffer::~TestDataBuffer()
{

}

void TestDataBuffer::SetUp()
{
  buf_size_ = 2L * 1024 * 1024;
  buf_ = static_cast<char *>(malloc(buf_size_));
  ASSERT_FALSE(NULL == buf_);
}

void TestDataBuffer::TearDown()
{
  if(NULL != buf_){
    free(buf_);
  }
}

TEST_F(TestDataBuffer, test_ObSelfBufferWriter)
{
  int ret = OB_SUCCESS;
  int64_t big_size = 256L * 1024L * 1024L * 1024L * 1024L * 1024L;//256TB
  ObSelfBufferWriter buf_align(ObModIds::TEST, 4096, true);
  ObSelfBufferWriter buf_not_align(ObModIds::TEST, 0, false);
  ret = buf_align.ensure_space(ALIGNED_SIZE);
  ASSERT_EQ(ret, OB_SUCCESS);

  ret = buf_align.ensure_space(4097);
  ASSERT_EQ(ret, OB_INVALID_ARGUMENT);

  ret = buf_not_align.ensure_space(big_size);
  ASSERT_EQ(ret, OB_ALLOCATE_MEMORY_FAILED);

  ret = buf_align.ensure_space(big_size);
  ASSERT_EQ(ret, OB_ALLOCATE_MEMORY_FAILED);

  ret = buf_align.ensure_space(buf_size_);
  ASSERT_EQ(ret, OB_SUCCESS);

  ret = buf_align.expand(buf_size_);
  ASSERT_EQ(ret, OB_SUCCESS);

  ret = buf_not_align.ensure_space(buf_size_);
  ASSERT_EQ(ret, OB_SUCCESS);

  ret = buf_align.ensure_space(big_size);
  ASSERT_EQ(ret, OB_ALLOCATE_MEMORY_FAILED);
}

TEST_F(TestDataBuffer, test_ObBufferHolder)
{
  //rollback and advance test
  ObBufferWriter buffer_writer(buf_, buf_size_);
  const int64_t tmp = 1234;
  ASSERT_EQ(OB_BUF_NOT_ENOUGH, buffer_writer.rollback(tmp));
  ASSERT_EQ(OB_SUCCESS, buffer_writer.set_pos(buf_size_));
  ASSERT_EQ(OB_BUF_NOT_ENOUGH, buffer_writer.advance(sizeof(int64_t)));
  ASSERT_EQ(OB_SUCCESS, buffer_writer.rollback(tmp));
  ASSERT_EQ(OB_SUCCESS, buffer_writer.advance(sizeof(int64_t)));
  ASSERT_EQ(buffer_writer.length(), buf_size_);
  //write_pod and read_pod get
  int64_t pod = INT64_MAX;
  int64_t read_pod = 0;
  const int64_t *get_pod = NULL;
  buffer_writer.assign(buf_, buf_size_);
  ObBufferReader buffer_reader(buf_, buf_size_);
  memset(buf_, 0, buf_size_);
  ASSERT_EQ(OB_SUCCESS, buffer_writer.write(pod));
  ASSERT_EQ(OB_SUCCESS, buffer_reader.read(read_pod));
  ASSERT_EQ(pod, read_pod);
  ASSERT_EQ(buffer_writer.length(), buffer_reader.length());
  ASSERT_EQ(OB_SUCCESS, buffer_writer.write(pod));
  ASSERT_EQ(OB_SUCCESS, buffer_reader.get(get_pod));
  ASSERT_FALSE(NULL == get_pod);
  ASSERT_EQ(pod, *get_pod);
  ASSERT_EQ(buffer_writer.length(), buffer_reader.length());
  //write and read buf test
  char write_buf[10] = "test";
  char read_buf[10];
  ASSERT_EQ(OB_SUCCESS, buffer_writer.write(write_buf, 10));
  ASSERT_EQ(OB_SUCCESS, buffer_reader.read(read_buf, 10));
  ASSERT_EQ(0, memcmp(write_buf, read_buf, 10));
  ASSERT_EQ(buffer_writer.length(), buffer_reader.length());
  //write_serialize and read_serizlize
  ObObj write_key;
  ObObj read_key;
  write_key.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
  write_key.set_varchar("test");
  ASSERT_TRUE(write_key != read_key);
  ASSERT_EQ(OB_SUCCESS, buffer_writer.write_serialize(write_key));
  ASSERT_EQ(OB_SUCCESS, buffer_reader.read_serialize(read_key));
  ASSERT_TRUE(write_key == read_key);
  ASSERT_EQ(buffer_writer.length(), buffer_reader.length());
  // write and read
  pod = INT64_MAX;
  ASSERT_EQ(OB_SUCCESS, buffer_writer.write(pod));
  ASSERT_EQ(OB_SUCCESS, buffer_reader.read(read_pod));
  ASSERT_EQ(pod, read_pod);
  ASSERT_EQ(buffer_writer.length(), buffer_reader.length());
  //append_fmt
  char *buf_read = NULL;
  ASSERT_EQ(OB_SUCCESS, buffer_writer.append_fmt("%s", "oceanbase"));
  ASSERT_EQ(OB_SUCCESS, buffer_reader.read_cstr(buf_read));
  ASSERT_EQ(0, memcmp(buf_read, "oceanbase", sizeof("oceanbase")));
}

TEST_F(TestDataBuffer, test_data_buffer_append_fmt_need_size)
{
  common::ObArenaAllocator allocator;
  const char *str="123";
  char *buf = static_cast<char *>(allocator.alloc(4));
  ObBufferWriter buffer_writer(buf, 4);
  ASSERT_NE(nullptr, buf);
  ASSERT_EQ(OB_SUCCESS, buffer_writer.append_fmt("%s", str));
}

TEST_F(TestDataBuffer, test_data_buffer_reader_read_str)
{
  common::ObArenaAllocator allocator;
  const char *str="123";
  char *buf = static_cast<char *>(allocator.alloc(4));
  MEMCPY(buf, "123", 4);
  ObBufferReader buffer_reader(buf, 4);
  char *read_str = nullptr;
  ASSERT_EQ(OB_SUCCESS, buffer_reader.read_cstr(read_str));
  ASSERT_EQ(4, buffer_reader.pos());
}

TEST_F(TestDataBuffer, test_ObBufferReader)
{
  int64_t buf_len = 1024*1024;
  char *buf = (char*)malloc(buf_len);
  int64_t pos = 0;

  ObBufferReader buffer_reader(buf_, 16384, 16384);
  ASSERT_EQ(OB_SUCCESS, buffer_reader.serialize(buf, buf_len, pos));
  COMMON_LOG(INFO, "dump", "need_size", buffer_reader.get_serialize_size(), K(buffer_reader), K(pos));

  ObBufferReader buffer_reader2;
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, buffer_reader2.deserialize(buf, buf_len, pos));
  COMMON_LOG(INFO, "dump", K(buffer_reader), K(pos), K(buffer_reader2));


}
}//blocksstable
}//oceanbase
int main(int argc, char** argv)
{
  system("rm -f test_data_buffer.log");
  OB_LOGGER.set_file_name("test_data_buffer.log");
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  oceanbase::lib::set_memory_limit(40UL << 30);
  signal(49, SIG_IGN);
  return RUN_ALL_TESTS();
}

