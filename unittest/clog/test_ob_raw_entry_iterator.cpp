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

#include "clog/ob_raw_entry_iterator.h"
#include "clog/ob_log_file_trailer.h"
#include "clog/ob_log_file_pool.h"
#include "clog/ob_log_direct_reader.h"
#include "clog/ob_log_common.h"
#include "common/ob_partition_key.h"
#include "clog/ob_clog_mgr.h"

#include <libaio.h>
#include <gtest/gtest.h>
#include <gmock/gmock.h>

using namespace oceanbase::common;
namespace oceanbase {
using namespace clog;
namespace unittest {
class TestObRawLogIterator : public ::testing::Test {
public:
  virtual void SetUp();
  virtual void TearDown();
  void prepare_entry();
  int write_data(const int fd, const char* buf, const int64_t data_len, const int64_t offset);
  int submit_aio(struct iocb* p);
  int wait_event(struct iocb* p);
  int write_file();

  static const int64_t data_len1 = 1 << 6;  // 64
  static const int64_t data_len2 = 1 << 7;  // 128
  static const int64_t data_len3 = 1 << 8;  // 256

  int64_t size;
  int64_t align_size;
  int64_t trailer_offset;
  offset_t header_offset;
  file_id_t file_id;
  const char* path;
  const char* shm_path;

  io_context_t ctx_;
  ObLogBlockMetaV2 meta;
  ObAlignedBuffer buffer;
  clog::ObLogEntry entry1;
  clog::ObLogEntry entry2;
  clog::ObLogEntry entry3;
  char buf1[data_len1];
  char buf2[data_len2];
  char buf3[data_len3];
  ObLogCache cache;
  ObLogDirectReader reader;
  ObRawLogIterator iterator;
};

void TestObRawLogIterator::SetUp()
{
  int task_num = 1024;
  const char* label = "3";

  size = 1 << 10;
  align_size = 512;
  trailer_offset = CLOG_TRAILER_OFFSET;
  header_offset = 0;
  file_id = 31;
  path = "readers/";
  shm_path = "readers/shm_buf";

  memset(buf1, 'A', sizeof(buf1));
  memset(buf2, 'B', sizeof(buf2));
  memset(buf3, 'C', sizeof(buf3));

  memset(&ctx_, 0, sizeof(ctx_));

  EXPECT_LE(0, system("rm -rf readers"));
  EXPECT_LE(0, system("mkdir readers"));

  EXPECT_EQ(OB_SUCCESS, buffer.init(size, align_size, label));
  EXPECT_EQ(0, io_setup(task_num, &ctx_));

  const int64_t bucket_num = 1024;
  const int64_t max_cache_size = 1024 * 1024 * 512;
  const int64_t block_size = common::OB_MALLOC_BIG_BLOCK_SIZE;
  ObKVGlobalCache::get_instance().init(bucket_num, max_cache_size, block_size);

  const int64_t hot_cache_size = 1L << 28;
  common::ObAddr addr(ObAddr::VER::IPV4, "100.81.152.48", 2828);
  EXPECT_EQ(OB_SUCCESS, cache.init(addr, "TestObLogCache", 5, hot_cache_size));
}

void TestObRawLogIterator::TearDown()
{
  if (NULL != ctx_) {
    io_destroy(ctx_);
  }
  buffer.destroy();
  cache.destroy();
  ObKVGlobalCache::get_instance().destroy();
}

void TestObRawLogIterator::prepare_entry()
{
  ObLogType ltype = OB_LOG_SUBMIT;
  int64_t gts = ::oceanbase::common::ObTimeUtility::current_time();
  int64_t tts = ::oceanbase::common::ObTimeUtility::current_time();
  ObProposalID rts;
  rts.ts_ = ::oceanbase::common::ObTimeUtility::current_time();
  uint64_t log_id1 = 3;
  uint64_t log_id2 = 4;
  uint64_t log_id3 = 5;
  ObPartitionKey partition_key1;
  ObPartitionKey partition_key2;
  ObPartitionKey partition_key3;
  ObLogEntryHeader header1;
  ObLogEntryHeader header2;
  ObLogEntryHeader header3;

  EXPECT_EQ(OB_SUCCESS, partition_key1.init(3, 8, 512));
  EXPECT_EQ(OB_SUCCESS, partition_key2.init(4, 8, 512));
  EXPECT_EQ(OB_SUCCESS, partition_key3.init(5, 7, 512));

  EXPECT_EQ(OB_SUCCESS,
      header1.generate_header(ltype, partition_key1, log_id1, buf1, data_len1, gts, tts, rts, gts, ObVersion(0), true));
  EXPECT_EQ(OB_SUCCESS,
      header2.generate_header(ltype, partition_key2, log_id2, buf2, data_len2, gts, tts, rts, gts, ObVersion(0), true));
  EXPECT_EQ(OB_SUCCESS,
      header3.generate_header(ltype, partition_key3, log_id3, buf3, data_len3, gts, tts, rts, gts, ObVersion(0), true));
  EXPECT_EQ(OB_SUCCESS, entry1.generate_entry(header1, buf1));
  EXPECT_EQ(OB_SUCCESS, entry2.generate_entry(header2, buf2));
  EXPECT_EQ(OB_SUCCESS, entry3.generate_entry(header3, buf3));
}

int TestObRawLogIterator::write_data(const int fd, const char* buf, const int64_t data_len, const int64_t offset)
{
  int ret = OB_SUCCESS;
  OB_ASSERT(-1 != fd);
  OB_ASSERT(NULL != buf);
  OB_ASSERT(data_len > 0);
  OB_ASSERT(offset >= 0);
  struct iocb io;
  struct iocb* p = &io;
  memset(&io, 0x0, sizeof(io));
  io_prep_pwrite(&io, fd, (void*)buf, data_len, offset);
  EXPECT_EQ(1, io_submit(ctx_, 1, &p));
  EXPECT_EQ(OB_SUCCESS, wait_event(p));
  return ret;
}

int TestObRawLogIterator::wait_event(struct iocb* p)
{
  int ret = OB_SUCCESS;
  struct io_event e;
  struct timespec timeout;
  OB_ASSERT(p->u.c.offset != -1);
  OB_ASSERT(static_cast<int64_t>(p->u.c.nbytes) >= 0);

  timeout.tv_sec = 100;
  timeout.tv_nsec = 0;  // 100s
  EXPECT_EQ(1, io_getevents(ctx_, 1, 1, &e, &timeout));
  EXPECT_EQ(0U, e.res2);
  EXPECT_EQ(p->u.c.nbytes, e.res);
  EXPECT_EQ(p->data, e.data);
  return ret;
}

int TestObRawLogIterator::write_file()
{
  int ret = OB_SUCCESS;
  ObLogDir log_dir;
  ObLogWriteFilePool write_pool;
  ObLogFileTrailer trailer;
  int fd = -1;
  int64_t pos = 0;
  int64_t file_size = CLOG_FILE_SIZE;
  int64_t meta_len = meta.get_serialize_size();
  int64_t block_num = 5;
  int64_t info_len = 512;

  prepare_entry();

  EXPECT_EQ(OB_SUCCESS, log_dir.init(path));
  EXPECT_EQ(OB_SUCCESS, write_pool.init(&log_dir, file_size, CLOG_WRITE_POOL));

  //------------------test case 1: file_id = 31, correct file-----------------------
  pos = meta_len;
  EXPECT_EQ(OB_SUCCESS, write_pool.get_fd(file_id, fd));
  EXPECT_EQ(OB_SUCCESS, entry1.serialize(buffer.get_align_buf(), size, pos));
  EXPECT_EQ(OB_SUCCESS, entry2.serialize(buffer.get_align_buf(), size, pos));
  EXPECT_EQ(OB_SUCCESS, entry3.serialize(buffer.get_align_buf(), size, pos));
  pos = 0;
  EXPECT_EQ(OB_SUCCESS,
      meta.build_serialized_block(
          buffer.get_align_buf(), meta_len, buffer.get_align_buf() + meta_len, pos - meta_len, OB_DATA_BLOCK, pos));

  for (int64_t i = 0; i < block_num; i++) {
    EXPECT_EQ(OB_SUCCESS, write_data(fd, buffer.get_align_buf(), size, header_offset + i * size));
  }

  buffer.reuse();
  pos = 0;
  memset(buffer.get_align_buf() + meta_len, 'I', static_cast<int>(info_len));
  EXPECT_EQ(OB_SUCCESS,
      meta.build_serialized_block(
          buffer.get_align_buf(), meta_len, buffer.get_align_buf() + meta_len, info_len, OB_INFO_BLOCK, pos));
  EXPECT_EQ(OB_SUCCESS, write_data(fd, buffer.get_align_buf(), size, header_offset + block_num * size));

  buffer.reuse();
  pos = 0;
  EXPECT_EQ(OB_SUCCESS,
      trailer.build_serialized_trailer(buffer.get_align_buf(),
          size,
          header_offset + static_cast<offset_t>((block_num + 1) * size),
          file_id + 1,
          pos));
  EXPECT_EQ(OB_SUCCESS, write_data(fd, buffer.get_align_buf(), align_size, trailer_offset));

  if (-1 != fd) {
    close(fd);
  }

  //------------------test case 2: file_id = 32, correct file, EOF-----------------------
  fd = -1;
  file_id++;
  pos = meta_len;
  buffer.reuse();
  EXPECT_EQ(OB_SUCCESS, write_pool.get_fd(file_id, fd));
  EXPECT_EQ(OB_SUCCESS, entry1.serialize(buffer.get_align_buf(), size, pos));
  pos = 0;
  EXPECT_EQ(OB_SUCCESS,
      meta.build_serialized_block(
          buffer.get_align_buf(), meta_len, buffer.get_align_buf() + meta_len, pos - meta_len, OB_DATA_BLOCK, pos));
  // The EOF block should be placed at the end of a block, at this time you need to know the length of a block
  // memcpy(buffer.get_align_buf() + meta.get_total_len(), ObEOFBuf::eof_flag_buf_, align_size);
  EXPECT_EQ(OB_SUCCESS, write_data(fd, buffer.get_align_buf(), size, header_offset));
  if (-1 != fd) {
    close(fd);
  }

  //------------------test case 3: file_id = 33, INFO block missing-----------------------
  fd = -1;
  file_id++;
  pos = meta_len;
  buffer.reuse();
  pos = meta_len;
  EXPECT_EQ(OB_SUCCESS, write_pool.get_fd(file_id, fd));
  EXPECT_EQ(OB_SUCCESS, entry1.serialize(buffer.get_align_buf(), size, pos));
  EXPECT_EQ(OB_SUCCESS, entry2.serialize(buffer.get_align_buf(), size, pos));
  EXPECT_EQ(OB_SUCCESS, entry3.serialize(buffer.get_align_buf(), size, pos));
  pos = 0;
  EXPECT_EQ(OB_SUCCESS,
      meta.build_serialized_block(
          buffer.get_align_buf(), meta_len, buffer.get_align_buf() + meta_len, pos - meta_len, OB_DATA_BLOCK, pos));
  EXPECT_EQ(OB_SUCCESS, write_data(fd, buffer.get_align_buf(), size, header_offset));

  buffer.reuse();
  pos = 0;
  EXPECT_EQ(OB_SUCCESS,
      trailer.build_serialized_trailer(
          buffer.get_align_buf(), size, header_offset + static_cast<offset_t>(size), file_id + 1, pos));
  EXPECT_EQ(OB_SUCCESS, write_data(fd, buffer.get_align_buf(), align_size, trailer_offset));

  if (-1 != fd) {
    close(fd);
  }

  //------------------test case 4: file_id = 34, Trailer block missing-----------------------
  fd = -1;
  file_id++;
  pos = meta_len;
  buffer.reuse();
  pos = meta_len;
  EXPECT_EQ(OB_SUCCESS, write_pool.get_fd(file_id, fd));
  EXPECT_EQ(OB_SUCCESS, entry1.serialize(buffer.get_align_buf(), size, pos));
  EXPECT_EQ(OB_SUCCESS, entry2.serialize(buffer.get_align_buf(), size, pos));
  EXPECT_EQ(OB_SUCCESS, entry3.serialize(buffer.get_align_buf(), size, pos));
  pos = 0;
  EXPECT_EQ(OB_SUCCESS,
      meta.build_serialized_block(
          buffer.get_align_buf(), meta_len, buffer.get_align_buf() + meta_len, pos - meta_len, OB_DATA_BLOCK, pos));
  EXPECT_EQ(OB_SUCCESS, write_data(fd, buffer.get_align_buf(), size, header_offset));

  buffer.reuse();
  pos = 0;
  memset(buffer.get_align_buf() + meta_len, 'I', static_cast<int>(info_len));
  EXPECT_EQ(OB_SUCCESS,
      meta.build_serialized_block(
          buffer.get_align_buf(), meta_len, buffer.get_align_buf() + meta_len, info_len, OB_INFO_BLOCK, pos));
  EXPECT_EQ(OB_SUCCESS, write_data(fd, buffer.get_align_buf(), size, header_offset + size));

  if (-1 != fd) {
    close(fd);
  }

  //------------------test case 5: block check fail-----------------------
  fd = -1;
  file_id++;
  pos = meta_len;
  buffer.reuse();
  pos = meta_len;
  EXPECT_EQ(OB_SUCCESS, write_pool.get_fd(file_id, fd));
  EXPECT_EQ(OB_SUCCESS, entry1.serialize(buffer.get_align_buf(), buffer.get_size(), pos));
  EXPECT_EQ(OB_SUCCESS, entry2.serialize(buffer.get_align_buf(), buffer.get_size(), pos));
  EXPECT_EQ(OB_SUCCESS, entry3.serialize(buffer.get_align_buf(), buffer.get_size(), pos));
  pos = 0;
  EXPECT_EQ(OB_SUCCESS,
      meta.build_serialized_block(
          buffer.get_align_buf(), meta_len, buffer.get_align_buf() + meta_len, pos - meta_len, OB_DATA_BLOCK, pos));

  *(buffer.get_align_buf() + 143) = 'M';
  EXPECT_EQ(OB_SUCCESS, write_data(fd, buffer.get_align_buf(), buffer.get_size(), header_offset));

  if (-1 != fd) {
    close(fd);
  }

  //------------------test case 6: block check fail-----------------------
  fd = -1;
  file_id++;
  buffer.reuse();
  pos = meta_len;
  EXPECT_EQ(OB_SUCCESS, write_pool.get_fd(file_id, fd));
  EXPECT_EQ(OB_SUCCESS, entry1.serialize(buffer.get_align_buf(), buffer.get_size(), pos));
  EXPECT_EQ(OB_SUCCESS, entry2.serialize(buffer.get_align_buf(), buffer.get_size(), pos));
  EXPECT_EQ(OB_SUCCESS, entry3.serialize(buffer.get_align_buf(), buffer.get_size(), pos));
  *(buffer.get_align_buf() + 81) = 'M';
  pos = 0;
  EXPECT_EQ(OB_SUCCESS,
      meta.build_serialized_block(
          buffer.get_align_buf(), meta_len, buffer.get_align_buf() + meta_len, pos - meta_len, OB_DATA_BLOCK, pos));
  EXPECT_EQ(OB_SUCCESS, write_data(fd, buffer.get_align_buf(), buffer.get_size(), header_offset));

  if (-1 != fd) {
    close(fd);
  }

  //------------------test case 7: only info block-----------------------
  fd = -1;
  file_id++;
  buffer.reuse();

  buffer.reuse();
  pos = 0;
  EXPECT_EQ(OB_SUCCESS, write_pool.get_fd(file_id, fd));
  memset(buffer.get_align_buf() + meta_len, 'I', info_len);
  EXPECT_EQ(OB_SUCCESS,
      meta.build_serialized_block(
          buffer.get_align_buf(), meta_len, buffer.get_align_buf() + meta_len, info_len, OB_INFO_BLOCK, pos));
  EXPECT_EQ(OB_SUCCESS, write_data(fd, buffer.get_align_buf(), size, header_offset));

  buffer.reuse();
  pos = 0;
  EXPECT_EQ(
      OB_SUCCESS, trailer.build_serialized_trailer(buffer.get_align_buf(), size, header_offset, file_id + 1, pos));
  EXPECT_EQ(OB_SUCCESS, write_data(fd, buffer.get_align_buf(), align_size, trailer_offset));

  if (-1 != fd) {
    close(fd);
  }

  //------------------test case 8: unknown block-----------------------
  fd = -1;
  file_id++;
  buffer.reuse();

  buffer.reuse();
  pos = 0;
  EXPECT_EQ(OB_SUCCESS, write_pool.get_fd(file_id, fd));
  EXPECT_EQ(OB_SUCCESS,
      meta.build_serialized_block(
          buffer.get_align_buf(), meta_len, buffer.get_align_buf() + meta_len, info_len, OB_TRAILER_BLOCK, pos));
  EXPECT_EQ(OB_SUCCESS, write_data(fd, buffer.get_align_buf(), size, header_offset));

  if (-1 != fd) {
    close(fd);
  }

  file_id = 31;
  return ret;
}

TEST_F(TestObRawLogIterator, test_read_data)
{
  ObReadParam param;
  ObReadParam read_param;
  ObReadRes res;
  clog::ObLogEntry tmp_entry;
  ObAlignedBuffer buffer;
  int64_t block_num = 5;
  int64_t entry1_len;
  int64_t entry2_len;
  int64_t entry3_len;
  int64_t header_len = meta.get_serialize_size() + header_offset;
  ObLogDir log_dir;

  ObTailCursor* tail = new ObTailCursor();
  EXPECT_EQ(OB_SUCCESS, log_dir.init(path));
  EXPECT_EQ(OB_SUCCESS, reader.init(path, shm_path, true, &cache, tail, ObLogWritePoolType::CLOG_WRITE_POOL));
  EXPECT_EQ(OB_SUCCESS, write_file());

  // Test init failed
  param.reset();
  EXPECT_EQ(OB_INVALID_ARGUMENT, iterator.init(NULL, param.file_id_, 0, param.file_id_, param.timeout_));
  EXPECT_EQ(OB_INVALID_ARGUMENT, iterator.init(&reader, param.file_id_, 0, param.file_id_, param.timeout_));

  param.file_id_ = file_id;
  param.read_len_ = OB_MAX_LOG_BUFFER_SIZE;
  param.timeout_ = 5000000;  // 5s
  int64_t persist_len = 0;
  EXPECT_EQ(OB_NOT_INIT, iterator.next_entry(tmp_entry, read_param, persist_len));

  // Start to test read
  entry1_len = entry1.get_serialize_size();
  entry2_len = entry2.get_serialize_size();
  entry3_len = entry3.get_serialize_size();

  (void)entry3_len;

  CLOG_LOG(INFO, "begin unittest::test_ob_raw_log_iterator");
  EXPECT_EQ(OB_SUCCESS, iterator.init(&reader, param.file_id_, 0, param.file_id_, param.timeout_));
  EXPECT_EQ(OB_INIT_TWICE, iterator.init(&reader, param.file_id_, 0, param.file_id_, param.timeout_));

  //------------------test case 1: file_id = 31, correct file----------------------------
  for (int64_t i = 0; i < block_num; i++) {
    CLOG_LOG(WARN, "warning", "param", to_cstring(read_param));
    EXPECT_EQ(OB_SUCCESS, iterator.next_entry(tmp_entry, read_param, persist_len));
    EXPECT_EQ(header_len + i * size, read_param.offset_);
    EXPECT_TRUE(tmp_entry == entry1);

    EXPECT_EQ(OB_SUCCESS, iterator.next_entry(tmp_entry, read_param, persist_len));
    EXPECT_EQ(entry1_len + header_len + i * size, read_param.offset_);
    EXPECT_TRUE(tmp_entry == entry2);

    EXPECT_EQ(OB_SUCCESS, iterator.next_entry(tmp_entry, read_param, persist_len));
    EXPECT_EQ(entry1_len + entry2_len + header_len + i * size, read_param.offset_);
    EXPECT_TRUE(tmp_entry == entry3);
  }
  EXPECT_EQ(OB_SUCCESS, iterator.next_entry(tmp_entry, read_param, persist_len));
  EXPECT_EQ(header_len, read_param.offset_);
  EXPECT_TRUE(tmp_entry == entry1);
  EXPECT_EQ(OB_ITER_END, iterator.next_entry(tmp_entry, read_param, persist_len));
  CLOG_LOG(INFO, "test case 1 success");

  /*
    //------------------test case 2: file_id = 32, correct file, EOF-----------------------
    param.file_id_ = param.file_id_ + 1;
    param.read_len_ = OB_MAX_LOG_BUFFER_SIZE;
    param.timeout_ = 5000000;              //5s
    EXPECT_EQ(OB_NOT_SUPPORTED, iterator.reuse(param.file_id_, 0, param.timeout_));

    EXPECT_EQ(OB_SUCCESS, iterator.next_entry(tmp_entry, read_param, persist_len));
    EXPECT_EQ(header_len, read_param.offset_);
    EXPECT_TRUE(tmp_entry == entry1);
    EXPECT_EQ(OB_ITER_END, iterator.next_entry(tmp_entry, read_param, persist_len));
    CLOG_LOG(INFO, "test case 2 success");

    //------------------test case 3: file_id = 33, INFO block missing-----------------------
    param.file_id_ = param.file_id_ + 1;
    param.read_len_ = OB_MAX_LOG_BUFFER_SIZE;
    param.timeout_ = 5000000;              //5s
    EXPECT_EQ(OB_NOT_SUPPORTED, iterator.reuse(param.file_id_, 0, param.timeout_));

    EXPECT_EQ(OB_SUCCESS, iterator.next_entry(tmp_entry, read_param, persist_len));
    EXPECT_EQ(header_len, read_param.offset_);
    EXPECT_TRUE(tmp_entry == entry1);

    EXPECT_EQ(OB_SUCCESS, iterator.next_entry(tmp_entry, read_param, persist_len));
    EXPECT_EQ(entry1_len + header_len, read_param.offset_);
    EXPECT_TRUE(tmp_entry == entry2);

    EXPECT_EQ(OB_SUCCESS, iterator.next_entry(tmp_entry, read_param, persist_len));
    EXPECT_EQ(entry1_len + entry2_len + header_len, read_param.offset_);
    EXPECT_TRUE(tmp_entry == entry3);

    // next_entry() do not rewrite ret now
    EXPECT_EQ(OB_INVALID_DATA, iterator.next_entry(tmp_entry, read_param, persist_len));
    CLOG_LOG(INFO, "test case 3 success");

    //------------------test case 4: file_id = 34, Trailer block missing-----------------------
    param.file_id_ = param.file_id_ + 1;
    param.read_len_ = OB_MAX_LOG_BUFFER_SIZE;
    param.timeout_ = 5000000;              //5s
    EXPECT_EQ(OB_NOT_SUPPORTED, iterator.reuse(param.file_id_, 0, param.timeout_));

    EXPECT_EQ(OB_SUCCESS, iterator.next_entry(tmp_entry, read_param, persist_len));
    EXPECT_EQ(header_len, read_param.offset_);
    EXPECT_TRUE(tmp_entry == entry1);

    EXPECT_EQ(OB_SUCCESS, iterator.next_entry(tmp_entry, read_param, persist_len));
    EXPECT_EQ(entry1_len + header_len, read_param.offset_);
    EXPECT_TRUE(tmp_entry == entry2);

    EXPECT_EQ(OB_SUCCESS, iterator.next_entry(tmp_entry, read_param, persist_len));
    EXPECT_EQ(entry1_len + entry2_len + header_len, read_param.offset_);
    EXPECT_TRUE(tmp_entry == entry3);

    EXPECT_EQ(OB_INVALID_DATA, iterator.next_entry(tmp_entry, read_param, persist_len));
    CLOG_LOG(INFO, "test case 4 success");

    //------------------test case 5: block check fail-----------------------
    param.file_id_ = param.file_id_ + 1;
    param.read_len_ = OB_MAX_LOG_BUFFER_SIZE;
    param.timeout_ = 5000000;              //5s
    EXPECT_EQ(OB_NOT_SUPPORTED, iterator.reuse(param.file_id_, 0, param.timeout_));

    EXPECT_EQ(OB_INVALID_DATA, iterator.next_entry(tmp_entry, read_param, persist_len));
    CLOG_LOG(INFO, "test case 5 success");

    //------------------test case 6: block check fail-----------------------
    param.file_id_ = param.file_id_ + 1;
    param.read_len_ = OB_MAX_LOG_BUFFER_SIZE;
    param.timeout_ = 5000000;              //5s
    EXPECT_EQ(OB_NOT_SUPPORTED, iterator.reuse(param.file_id_, 0, param.timeout_));

    EXPECT_EQ(OB_INVALID_DATA, iterator.next_entry(tmp_entry, read_param, persist_len));
    CLOG_LOG(INFO, "test case 6 success");

    //------------------test case 7: switch file fail-----------------------
    param.file_id_ = param.file_id_ + 1;
    param.read_len_ = OB_MAX_LOG_BUFFER_SIZE;
    param.timeout_ = 5000000;              //5s
    EXPECT_EQ(OB_SUCCESS, iterator.reuse(param.file_id_, 0, param.timeout_));

    EXPECT_EQ(OB_INVALID_DATA, iterator.next_entry(tmp_entry, read_param, persist_len));
    CLOG_LOG(INFO, "test case 7 success");

    //------------------test case 8: unknown block-----------------------
    param.file_id_ = param.file_id_ + 1;
    param.read_len_ = OB_MAX_LOG_BUFFER_SIZE;
    param.timeout_ = 5000000;              //5s
    EXPECT_EQ(OB_SUCCESS, iterator.reuse(param.file_id_, 0, param.timeout_));

    EXPECT_EQ(OB_INVALID_DATA, iterator.next_entry(tmp_entry, read_param, persist_len));
    CLOG_LOG(INFO, "test case 8 success");


    param.reset();
    EXPECT_EQ(OB_INVALID_ARGUMENT, iterator.reuse(param.file_id_, 0, param.timeout_));
    */
}

}  // end namespace unittest
}  // end namespace oceanbase

int main(int argc, char** argv)
{
  OB_LOGGER.set_file_name("test_ob_raw_entry_iterator.log", true);
  OB_LOGGER.set_log_level("DEBUG");
  CLOG_LOG(INFO, "begin unittest::test_ob_raw_entry_iterator");
  testing::InitGoogleTest(&argc, argv);
  // return RUN_ALL_TESTS();
  return 0;
}
