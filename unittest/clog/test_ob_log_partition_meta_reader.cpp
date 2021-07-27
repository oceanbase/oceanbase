// Copyright 2014 Alibaba Inc. All Rights Reserved.
// Author:
//     qiaoli.xql@alibaba-inc.com
// Owner:
//     lujun.wlj@alibaba-inc.com
//
// This file tests ObLogPartitionMetaReader.
//
//l1: Write the correct trailer information and read the correct PartitionMeta information
//l2: Write the correct trailer information and read the incorrect PartitionMeta information
//l3: Write the correct trailer information and can not read PartitionMeta information
//14: not write trailer
//

#include "clog/ob_log_partition_meta_reader.h"
#include "clog/ob_log_file_trailer.h"
#include "clog/ob_log_direct_reader.h"
#include "clog/ob_log_file_pool.h"
#include "clog/ob_log_common.h"

#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include "lib/tbsys.h"
#include <libaio.h>

using namespace oceanbase::common;

namespace oceanbase
{
using namespace clog;
namespace unittest
{
class MyMinUsingFileIDGetter: public ObIGetMinUsingFID
{
public:
  MyMinUsingFileIDGetter() {}
  virtual ~MyMinUsingFileIDGetter() {}
  int on_leader_revoke(const common::ObPartitionKey &partition_key)
  { UNUSED(partition_key); return OB_SUCCESS; }
  int on_leader_takeover(const common::ObPartitionKey &partition_key)
  { UNUSED(partition_key); return OB_SUCCESS; }
  int on_member_change_success(
      const common::ObPartitionKey &partition_key,
      const int64_t mc_timestamp,
      const common::ObMemberList &prev_member_list,
      const common::ObMemberList &curr_member_list)
  { UNUSED(partition_key); UNUSED(mc_timestamp); UNUSED(prev_member_list); UNUSED(curr_member_list); return OB_SUCCESS; }
  int64_t get_min_using_file_id() const
  { return 1; }
};

class TestObLogPartitionMetaReader: public ::testing::Test
{
public:
  virtual void SetUp();
  virtual void TearDown();
  int write_data(const int fd, const char *buf, const int64_t buf_len, const int64_t offset);
  int submit_aio(struct iocb *p);
  int wait_event(struct iocb *p);
  int write_file();

  static const int64_t data_size = 1 << 9;       //512
  static const int64_t align_size = 1 << 10;    //1K
  static const char *label = "2";
  static const char *label2 = "3";

  io_context_t ctx_;
  ObAlignedBuffer buffer;
  ObAlignedBuffer buffer2;
  ObLogFileTrailer trailer;
  ObLogCache cache;
  const char *path;
  int64_t blk_len;
  char buf[align_size];
};

void TestObLogPartitionMetaReader::SetUp()
{
  path = "readers";

  EXPECT_LE(0, system("mkdir readers"));
  EXPECT_LE(0, system("rm readers/*"));

  memset(&ctx_, 0, sizeof(ctx_));
  memset(buf, 0, sizeof(buf));

  EXPECT_EQ(0, io_setup(1, &ctx_));
  EXPECT_EQ(OB_SUCCESS, buffer.init(align_size, align_size, label));
  EXPECT_EQ(OB_SUCCESS, buffer2.init(data_size, data_size, label2));

  const int64_t bucket_num = 1024;
  const int64_t max_cache_size = 1024 * 1024 * 512;
  const int64_t block_size = common::OB_MALLOC_BIG_BLOCK_SIZE;
  ObKVGlobalCache::get_instance().init(bucket_num, max_cache_size, block_size);

  EXPECT_EQ(OB_SUCCESS, cache.init("TestObLogCache", 5));
}

void TestObLogPartitionMetaReader::TearDown()
{
  if (NULL != ctx_) {
    io_destroy(ctx_);
  }
  buffer.destroy();
  cache.destroy();
  ObKVGlobalCache::get_instance().destroy();
}

int TestObLogPartitionMetaReader::write_data(const int fd, const char *buf, const int64_t buf_len,
                                             const int64_t offset)
{
  int ret = OB_SUCCESS;
  struct iocb io;
  struct iocb *p = &io;
  memset(&io, 0x0, sizeof(io));
  io_prep_pwrite(&io, fd, (void *)buf, buf_len, offset);
  EXPECT_EQ(1, io_submit(ctx_, 1, &p));
  EXPECT_EQ(OB_SUCCESS, wait_event(p));
  return ret;
}

int TestObLogPartitionMetaReader::wait_event(struct iocb *p)
{
  int ret = OB_SUCCESS;
  struct io_event e;
  struct timespec timeout;
  OB_ASSERT(p->u.c.offset != -1);
  OB_ASSERT(static_cast<int64_t>(p->u.c.nbytes) >= 0);

  timeout.tv_sec = 100;
  timeout.tv_nsec = 0; //100s
  EXPECT_EQ(1, io_getevents(ctx_, 1, 1, &e, &timeout));
  EXPECT_EQ(0U, e.res2);
  EXPECT_EQ(p->u.c.nbytes, e.res);
  EXPECT_EQ(p->data, e.data);
  return ret;
}

int TestObLogPartitionMetaReader::write_file()
{
  int ret = OB_SUCCESS;
  int fd = -1;
  int64_t pos = 0;
  int64_t num = 2;
  int64_t file_size = 1 << 22;     //4K
  int64_t percent = 100;
  file_id_t start_file_id = 11; //11-14 are test file
  ObLogBlockMetaV2::MetaContent block;
  ObLogWriteFilePool write_pool;
  MyMinUsingFileIDGetter min_getter;

  EXPECT_EQ(OB_SUCCESS, write_pool.init(path, file_size, percent, &min_getter));
  EXPECT_EQ(OB_SUCCESS, block.generate_block(buf, data_size, OB_DATA_BLOCK));
  CLOG_LOG(INFO, "block", "meta", to_cstring(block), "buf", buf, "data_size", (int64_t)data_size);

  EXPECT_EQ(OB_SUCCESS, block.serialize(buffer.get_align_buf(), align_size, pos));
  memcpy((buffer.get_align_buf() + pos), buf, data_size);
  offset_t start_pos = -1;
  file_id_t next_file_id = 0;
  blk_len = block.get_total_len();

  //--------------test case 1-----------------------
  //Write file which file id is 11, content is correct, write tow block
  EXPECT_EQ(OB_SUCCESS, write_pool.get_fd(start_file_id, fd));
  for (int64_t i = 0; i < num; i++) {
    if (OB_SUCCESS != (ret = write_data(fd, buffer.get_align_buf(), blk_len, i * blk_len))) {
      CLOG_LOG(ERROR, "write_data fail", KERRMSG);
    }
  }

  pos = 0;
  start_pos = 1024;
  next_file_id = start_file_id + 1;

  //Write the trailer of file which file id is 11, include start address and next file id
  EXPECT_EQ(OB_SUCCESS, trailer.build_serialized_trailer(buffer2.get_align_buf(), data_size, start_pos, next_file_id, pos));
  EXPECT_EQ(OB_SUCCESS, write_data(fd, buffer2.get_align_buf(), data_size, CLOG_TRAILER_OFFSET));
  if (fd >= 0) {
    close(fd);
  }

  //--------------test case 2-----------------------
  // Write file whild file id is 13, write data failed, check block integrity fail
  start_file_id = next_file_id;
  EXPECT_EQ(OB_SUCCESS, write_pool.get_fd(start_file_id, fd));
  buffer.get_align_buf()[512] = 'B';
  for (int64_t i = 0; i < num; i++) {
    if (OB_SUCCESS != (ret = write_data(fd, buffer.get_align_buf(), blk_len, i * blk_len))) {
      CLOG_LOG(ERROR, "write_data fail", KERRMSG);
    }
  }

  pos = 0;
  start_pos = 0;
  next_file_id = start_file_id + 1;

  //Write the trailer of file which file id is 12, include start address and next file id
  EXPECT_EQ(OB_SUCCESS, trailer.build_serialized_trailer(buffer2.get_align_buf(), data_size, start_pos, next_file_id, pos));
  EXPECT_EQ(OB_SUCCESS, write_data(fd, buffer2.get_align_buf(), data_size, CLOG_TRAILER_OFFSET));
  if (fd >= 0) {
    close(fd);
  }

  //--------------test case 3-----------------------
  // Write file whild file id is 13, write one block, data correct,
  // check meat checksum fail
  start_file_id = next_file_id;
  EXPECT_EQ(OB_SUCCESS, write_pool.get_fd(start_file_id, fd));
  EXPECT_EQ(OB_SUCCESS, write_data(fd, buffer.get_align_buf(), blk_len, 0));

  pos = 0;
  start_pos = 512;
  next_file_id = start_file_id + 1;

  //Write the trailer of file which file id is 13, include start address and next file id
  EXPECT_EQ(OB_SUCCESS, trailer.build_serialized_trailer(buffer2.get_align_buf(), data_size, start_pos, next_file_id, pos));
  EXPECT_EQ(OB_SUCCESS, write_data(fd, buffer2.get_align_buf(), data_size, CLOG_TRAILER_OFFSET));
  if (fd >= 0) {
    close(fd);
  }

  //--------------test case 4-----------------------
  // Write file whild file id is 14, write one block, data correct
  start_file_id = next_file_id;
  EXPECT_EQ(OB_SUCCESS, write_pool.get_fd(start_file_id, fd));
  EXPECT_EQ(OB_SUCCESS, write_data(fd, buffer.get_align_buf(), blk_len, 0));

  if (fd >= 0) {
    close(fd);
  }

  return ret;
}

TEST_F(TestObLogPartitionMetaReader, test_read_partition_meta)
{
  ObLogReadFilePool reader_pool;
  ObLogDirectReader dreader;
  ObLogPartitionMetaReader reader;
  ObReadParam param;
  ObReadRes res;
  file_id_t start_file_id = 11;

  param.timeout_ = 5000000;  //5s
  param.read_len_ = OB_MAX_LOG_BUFFER_SIZE;
  param.file_id_ = start_file_id;
  EXPECT_EQ(OB_NOT_INIT, reader.read_partition_meta(param, res));
  EXPECT_EQ(OB_INVALID_ARGUMENT, reader.init(NULL));
  EXPECT_EQ(OB_NOT_INIT, reader.read_partition_meta(param, res));

  //Test read
  CLOG_LOG(INFO, "begin unittest::test_ob_log_partition_meta_reader");
  EXPECT_EQ(OB_SUCCESS, reader_pool.init(path));
  EXPECT_EQ(OB_SUCCESS, dreader.init(&reader_pool, &cache));
  EXPECT_EQ(OB_SUCCESS, reader.init(&dreader));
  EXPECT_EQ(OB_INIT_TWICE, reader.init(&dreader));
  EXPECT_EQ(OB_SUCCESS, write_file());

  //Test read first block
  param.file_id_ = start_file_id;
  EXPECT_EQ(OB_SUCCESS, reader.read_partition_meta(param, res));
  EXPECT_EQ((int64_t)data_size, res.data_len_);

  param.file_id_ = param.file_id_ + 1;
  EXPECT_EQ(OB_INVALID_DATA, reader.read_partition_meta(param, res));

  param.file_id_ = param.file_id_ + 1;
  EXPECT_EQ(OB_INVALID_DATA, reader.read_partition_meta(param, res));

  param.file_id_ = param.file_id_ + 1;
  EXPECT_EQ(OB_READ_NOTHING, reader.read_partition_meta(param, res));

  //Test failure
  param.reset();
  EXPECT_EQ(OB_INVALID_ARGUMENT, reader.read_partition_meta(param, res));

  //Test deserialize failed
  param.file_id_ = start_file_id;
  param.read_len_ = 1;
  param.timeout_ = 5000000; //5s
  EXPECT_EQ(OB_DESERIALIZE_ERROR, reader.read_partition_meta(param, res));

  //Test invalid argument
  param.file_id_ = start_file_id;
  param.read_len_ = 1;
  param.timeout_ = 0;
  res.reset();
  EXPECT_EQ(OB_INVALID_ARGUMENT, reader.read_partition_meta(param, res));
}

} // end namespace unittest
} // end namespace oceanbase

int main(int argc, char **argv)
{
  OB_LOGGER.set_file_name("test_ob_log_partition_meta_reader.log", true);
  OB_LOGGER.set_log_level("INFO");
  CLOG_LOG(INFO, "begin unittest::test_ob_log_partition_meta_reader");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
