// Copyright 2014 Alibaba Inc. All Rights Reserved.
// Author:
//     qiaoli.xql@alibaba-inc.com
// Owner:
//     lujun.wlj@alibaba-inc.com
//
// This file tests ObLogWriter.
//
// Testing method
//1. Write 60 data blocks, each log saves the same 1K data buffer, each block 1000 logs,
//   log ID is (1-1000)*i
//2. Write a confirm block after every 10,000 logs, which contains the confirm log of
//   the first 10,000 logs
//3. The file is written by ObLogWriter, and the log is read iteratively by ObLogIterator
//
//Testing scenarios
//1. Write start file id is 1
//2. After writing 10 data blocks, submit a sync_flush_task task and submit a confirm block
//3. After the last confirm log data block, switch_file_task is called once
//4. Write the first 60 data blocks, at least 64M, ObLogWriter will actively cut the file once
//5. Test ObLogFileLocator:
//  1)Create the first file when the directory is empty
//  2)Find the end of the file when there is a file
//  3)The file with the largest ID happens to be the first newly created file
//6. Use ObLogReader to read the log in turn for the correctness of comparison
//

#include "clog/ob_log_writer.h"
#include "clog/ob_log_file_trailer.h"
#include "clog/ob_info_block_handler.h"
#include "clog/ob_log_common.h"
#include "clog/ob_log_type.h"
#include "lib/utility/ob_tracepoint.h"
#include "share/ob_proposal_id.h"

#include <libaio.h>
#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include "lib/tbsys.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace clog
{
//class MyObLogScannerUtilityImp: public ObILogScannerUtilityImp
//{
//public:
//  int scan_partition_log_range(const ObPartitionKey &partition_key,
//                               const char *buf,
//                               const int64_t data_len,
//                               uint64_t &min_log_id,
//                               uint64_t &max_log_id);
//};
//
//int MyObLogScannerUtilityImp::scan_partition_log_range(const ObPartitionKey &partition_key,
//                                                       const char *buf,
//                                                       const int64_t data_len,
//                                                       uint64_t &min_log_id,
//                                                       uint64_t &max_log_id)
//{
//  int ret = OB_SUCCESS;
//  UNUSED(partition_key);
//  UNUSED(buf);
//  UNUSED(data_len);
//  min_log_id = 1;
//  max_log_id = 60000;
//  return ret;
//}
//
//ObILogScannerUtilityImp *get_instance()
//{
//  static MyObLogScannerUtilityImp instance;
//  return &instance;
//}

class FlushLogTask: public IFlushLogTask
{
public:
  FlushLogTask(): is_done_(false) {}
  virtual ~FlushLogTask() { is_done_ = false; }
  int after_flushed(const file_id_t file_id, const offset_t offset, const int error_code);
  void init(char *buf, const int64_t data_len);
  void reset();
  bool is_done() { return is_done_; }
private:
  volatile bool is_done_;
};

int FlushLogTask::after_flushed(const file_id_t file_id,
                                const offset_t offset,
                                const int error_code)
{
  UNUSED(file_id);
  UNUSED(offset);
  UNUSED(error_code);
  is_done_ = true;
  return OB_SUCCESS;
}

void FlushLogTask::init(char *buf, int64_t data_len)
{
  IFlushLogTask::init(buf, data_len);
  is_done_ = false;
}

void FlushLogTask::reset()
{
  buf_ = NULL;
  data_len_ = 0;
  is_done_ = false;
}

} //namespace clog

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
  { return 10000; }
};

class MyMetaInfoGenerator: public ObIInfoBlockHandler
{
public:
  MyMetaInfoGenerator() {}
  virtual ~MyMetaInfoGenerator() {}
  virtual int build_info_block(char *buf, const int64_t buf_len, int64_t &pos);
  virtual int resolve_info_block(const char *buf, const int64_t buf_len, int64_t &pos);
  virtual int update_info(const ObVersion &version);
};

int MyMetaInfoGenerator::build_info_block(char *buf, const int64_t buf_len, int64_t &pos)
{
  UNUSED(buf_len);
  memset(buf, 'Z', buf_len);
  pos = 1023;
  return OB_SUCCESS;
}

int MyMetaInfoGenerator::resolve_info_block(const char *buf, const int64_t buf_len, int64_t &pos)
{
  UNUSED(buf);
  UNUSED(buf_len);
  UNUSED(pos);
  return OB_SUCCESS;
}

int MyMetaInfoGenerator::update_info(const ObVersion &version)
{
  UNUSED(version);
  return OB_SUCCESS;
}

class TestObLogWriter: public ::testing::Test
{
public:
  virtual void SetUp();
  virtual void TearDown();
  int write_data(const int fd, const char *buf, const int64_t data_len, const offset_t offset);
  int submit_aio(struct iocb *p);
  int wait_event(struct iocb *p);

  int prepare_data_block(const uint64_t max_id,
                         char *&blk_buf,
                         int64_t &blk_len);
  int prepare_confirm_block(const uint64_t max_id,
                            const uint64_t num,
                            char *&blk_buf,
                            int64_t &blk_len);
  int submit_task(ObLogWriter &writer, uint64_t &max_log_id);

  static const ObLogType ctype = OB_LOG_CONFIRMED;
  static const ObLogType ltype = OB_LOG_SUBMIT;
  static const common::ObProposalID rts;
  static const int64_t gts = 201;
  static const int64_t tts = 202;
  static const uint64_t table_id = 5;
  static const int64_t partition_idx = 20;
  ObPartitionKey partition_key;
  static const int64_t data_len = 1024 - 200;
  char buf[data_len];

  static const int64_t size = 1 << 21;
  static const int64_t align_size = 512;
  const char *path;
  const char *dir;
  int64_t file_size;
  int64_t percent;

  offset_t start_offset;
  int64_t queue_size;
  file_id_t file_id;
  int64_t timeout;

  io_context_t ctx_;
  ObAlignedBuffer buffer;
  ObAlignedBuffer confirm_buffer;
  MyMetaInfoGenerator meta_gen;
  ObLogReadFilePool reader_pool;
  ObLogWriteFilePool write_pool;
  MyMinUsingFileIDGetter min_getter;
  ObReadParam param;
  ObLogFileLocator<ObLogEntry, ObIRawLogIterator> locator;
  ObLogReadFilePool rpool;
  ObLogWriteFilePool wpool;
  ObLogDirectReader dreader;
  ObLogCache cache;
};

const common::ObProposalID TestObLogWriter::rts;

void TestObLogWriter::SetUp()
{
  CLOG_LOG(INFO, "SetUp");
  //TestObLogWriter::rts.ts_ = 200;
  int task_num = 1024;
  const char *label1 = "1";
  const char *label2 = "2";
  timeout = 10000000; //10s
  file_size = CLOG_FILE_SIZE; //64M
  percent = 100;

  param.timeout_ = timeout;

  EXPECT_EQ(OB_SUCCESS, partition_key.init(3, 8, 9));
  memset(buf, 'A', data_len);
  memset(&ctx_, 0, sizeof(ctx_));
  path = "writer/";
  dir = "writer_error";

  EXPECT_EQ(OB_SUCCESS, buffer.init(size, align_size, label1));
  EXPECT_EQ(OB_SUCCESS, confirm_buffer.init(size, align_size, label2));
  EXPECT_EQ(0, io_setup(task_num, &ctx_));
  EXPECT_LE(0, system("rm -rf writer"));
  EXPECT_LE(0, system("mkdir writer"));
  EXPECT_LE(0, system("rm -rf writer_error"));
  EXPECT_LE(0, system("mkdir writer_error"));

  EXPECT_EQ(OB_SUCCESS, reader_pool.init(path));
  EXPECT_EQ(OB_SUCCESS, write_pool.init(path, file_size, percent, &min_getter));
  //oceanbase::clog::log_scanner_utility_ptr = get_instance();

  int fd = 0;
  file_id = 1;
  start_offset = 0;
  queue_size = 1024;
  EXPECT_EQ(OB_SUCCESS, rpool.init(dir));
  EXPECT_EQ(OB_SUCCESS, wpool.init(dir, file_size, percent, &min_getter));
  EXPECT_EQ(OB_SUCCESS, wpool.get_fd(file_id, fd));
  if (fd >= 0) {
    close(fd);
  }

  const int64_t bucket_num = 1024;
  const int64_t max_cache_size = 1024 * 1024 * 512;
  const int64_t block_size = common::OB_MALLOC_BIG_BLOCK_SIZE;
  ObKVGlobalCache::get_instance().init(bucket_num, max_cache_size, block_size);

  EXPECT_EQ(OB_SUCCESS, cache.init("TestObLogCache", 5));

  EXPECT_EQ(OB_SUCCESS, dreader.init(&reader_pool, &cache));
}

void TestObLogWriter::TearDown()
{
  CLOG_LOG(INFO, "TearDown");
  if (NULL != ctx_) {
    io_destroy(ctx_);
  }
  buffer.destroy();
  confirm_buffer.destroy();
  cache.destroy();
  dreader.destroy();

  ObKVGlobalCache::get_instance().destroy();
}

int TestObLogWriter::write_data(const int fd, const char *buf, const int64_t data_len,
                                const offset_t offset)
{
  int ret = OB_SUCCESS;
  OB_ASSERT(-1 != fd);
  OB_ASSERT(NULL != buf);
  OB_ASSERT(data_len > 0);
  OB_ASSERT(offset >= 0);
  struct iocb io;
  struct iocb *p = &io;
  memset(&io, 0x0, sizeof(io));
  io_prep_pwrite(&io, fd, (void *)buf, data_len, offset);
  EXPECT_EQ(1, io_submit(ctx_, 1, &p));
  EXPECT_EQ(OB_SUCCESS, wait_event(p));
  return ret;
}

int TestObLogWriter::wait_event(struct iocb *p)
{
  int ret = OB_SUCCESS;
  struct io_event e;
  struct timespec timeout;
  OB_ASSERT(p->u.c.offset != -1);
  OB_ASSERT(static_cast<int64_t>(p->u.c.nbytes) >= 0);

  timeout.tv_sec = 100;
  timeout.tv_nsec = 0; //100s
  EXPECT_EQ(1, io_getevents(ctx_, 1, 1, &e, &timeout));
  EXPECT_TRUE(0 == e.res2);
  EXPECT_EQ(p->u.c.nbytes, e.res);
  EXPECT_EQ(p->data, e.data);
  return ret;
}

int TestObLogWriter::prepare_data_block(const uint64_t max_id,
                                        char *&blk_buf,
                                        int64_t &blk_len)
{
  ObLogEntryHeader header;
  ObLogEntry entry;
  ObLogBlockMetaV2::MetaContent meta;
  uint64_t log_id = OB_INVALID_ID;
  int64_t meta_len = meta.get_serialize_size();
  int64_t pos = meta_len;

  for (uint64_t i = 1; i <= max_id; i++) {
    EXPECT_EQ(OB_SUCCESS, header.generate_header(ltype, partition_key, log_id, buf, data_len, gts, tts,
                                                 rts, 0, 1, 1, ObVersion()));
    EXPECT_EQ(OB_SUCCESS, entry.generate_entry(header, buf));
    EXPECT_EQ(OB_SUCCESS, entry.serialize(buffer.get_align_buf(), size, pos));
  }
  blk_len = pos - meta_len;
  blk_buf = buffer.get_align_buf() + meta_len;
  return OB_SUCCESS;
}

int TestObLogWriter::prepare_confirm_block(const uint64_t max_id,
                                           const uint64_t num,
                                           char *&blk_buf,
                                           int64_t &blk_len)
{
  ObLogEntryHeader header;
  ObLogEntry entry;
  ObLogBlockMetaV2::MetaContent meta;
  ObConfirmedLog clog;
  uint64_t log_id = OB_INVALID_ID;
  int64_t meta_len = meta.get_serialize_size();
  int64_t pos = meta_len;
  int64_t confirm_pos = 0;
  int64_t clog_pos = 0;
  int64_t clog_len = 1024;
  char clog_buf[clog_len];

  for (uint64_t i = 1; i <= max_id; i++) {
    clog_pos = 0;
    log_id = i + max_id * num;
    EXPECT_EQ(OB_SUCCESS, header.generate_header(ltype, partition_key, log_id, buf, data_len, gts, tts,
                                                 rts, 0, 1, 1, ObVersion()));
    EXPECT_EQ(OB_SUCCESS, entry.generate_entry(header, buf));
    EXPECT_EQ(OB_SUCCESS, entry.serialize(buffer.get_align_buf(), size, pos));

    clog.set_log_id(log_id);
    clog.set_data_checksum(header.get_data_checksum());
    clog.set_epoch_id(header.get_epoch_id());

    EXPECT_EQ(OB_SUCCESS, clog.serialize(clog_buf, clog_len, clog_pos));
    EXPECT_EQ(OB_SUCCESS, header.generate_header(ltype, partition_key, log_id, buf, data_len, gts, tts,
                                                 rts, 0, 1, 1, ObVersion()));
    EXPECT_EQ(OB_SUCCESS, entry.generate_entry(header, clog_buf));
    EXPECT_EQ(OB_SUCCESS, entry.serialize(confirm_buffer.get_align_buf(), size, confirm_pos));
  }
  memcpy(buffer.get_align_buf() + pos, confirm_buffer.get_align_buf(), confirm_pos);
  blk_len = pos + confirm_pos - meta_len;
  blk_buf = buffer.get_align_buf() + meta_len;
  return OB_SUCCESS;
}

int TestObLogWriter::submit_task(ObLogWriter &writer, uint64_t &max_log_id)
{
  int ret = OB_SUCCESS;
  uint64_t write_times = 1;
  uint64_t entry_num_in_block = 60;
  FlushLogTask flush_task;
  char *f_buf = NULL;
  int64_t f_dl = 0;

  max_log_id = write_times * entry_num_in_block;

  for (uint64_t i = 0; i < write_times; i++) {
    flush_task.reset();
    EXPECT_EQ(OB_SUCCESS, prepare_confirm_block(entry_num_in_block, i, f_buf, f_dl));
    flush_task.init(f_buf, f_dl);
    EXPECT_EQ(OB_SUCCESS, writer.submit_flush_log_task(&flush_task));
    while (!flush_task.is_done()) {
      PAUSE();
    }
  }
  return ret;
}

TEST_F(TestObLogWriter, test_log_file_locator)
{
  ObLogBlockMetaV2::MetaContent t_meta;
  int fd = -1;
  file_id_t file_id = OB_INVALID_FILE_ID;
  offset_t offset = OB_INVALID_OFFSET;
  int64_t pos = 0;
  int64_t num = 1000;
  int64_t meta_len = t_meta.get_serialize_size();
  char *t_buf = NULL;
  int64_t t_data_len = 0;;
  int64_t t_blk_len = 0;

  //Test invalid arguments
  EXPECT_EQ(OB_INVALID_ARGUMENT, locator.get_cursor(param.timeout_, NULL, &dreader, file_id, offset));

  EXPECT_LE(0, system("rm -rf writer"));
  EXPECT_LE(0, system("mkdir writer"));
  EXPECT_EQ(OB_SUCCESS, locator.get_cursor(param.timeout_, &reader_pool, &dreader, file_id, offset));
  EXPECT_EQ(1UL, file_id);
  EXPECT_EQ(0, offset);

  file_id_t t_file_id = 4;
  offset_t t_offset = 0;
  EXPECT_EQ(OB_SUCCESS, write_pool.get_fd(t_file_id, fd));
  //Test exist files, and content is valid
  pos = 0;
  EXPECT_EQ(OB_SUCCESS, prepare_data_block(num, t_buf, t_data_len));
  EXPECT_EQ(OB_SUCCESS, t_meta.generate_block(t_buf, t_data_len, OB_DATA_BLOCK));
  EXPECT_EQ(OB_SUCCESS, t_meta.serialize(t_buf - meta_len, meta_len, pos));
  t_blk_len = t_meta.get_total_len();
  memcpy(t_buf - meta_len + t_blk_len, ObEOFBuf::eof_flag_buf_, DIO_ALIGN_SIZE);
  EXPECT_EQ(OB_SUCCESS, write_data(fd, t_buf - meta_len, t_blk_len + DIO_ALIGN_SIZE , t_offset));

  EXPECT_EQ(OB_SUCCESS, locator.get_cursor(param.timeout_, &reader_pool, &dreader, file_id, offset));
  EXPECT_EQ(t_file_id, file_id);
  EXPECT_EQ(t_offset + t_blk_len, offset);
  if (fd >= 0) {
    close(fd);
  }

  //Test only a empty file
  t_file_id = 30;
  EXPECT_EQ(OB_SUCCESS, write_pool.get_fd(t_file_id, fd));
  EXPECT_EQ(OB_SUCCESS, locator.get_cursor(param.timeout_, &reader_pool, &dreader, file_id, offset));
  EXPECT_EQ(t_file_id, file_id);
  EXPECT_EQ(0, offset);
  if (fd >= 0) {
    close(fd);
  }
}

TEST_F(TestObLogWriter, test_ob_write_param)
{
  int t_fd = 4;
  file_id_t t_file_id = 7;
  offset_t t_offset = 123;
  int64_t t_write_len = 3333;
  int64_t t_timeout = 9000;
  char buf[20];
  ObWriteParam param1;
  ObWriteParam param2;

  param1.fd_ = t_fd;
  param1.file_id_ = t_file_id;
  param1.offset_ = t_offset;
  param1.buf_ = buf;
  param1. write_len_ = t_write_len;
  param1.timeout_ = t_timeout;

  param2.reset();
  param2.shallow_copy(param1);
  EXPECT_TRUE(param1 == param2);

  param1.reset();
  param2.reset();
  EXPECT_TRUE(param1 == param2);

  CLOG_LOG(INFO, "param to string", "param", to_cstring(param1));
}

TEST_F(TestObLogWriter, test_write_data)
{
  ObReadParam param;
  ObLogWriter writer;
  ObLogEntryHeader t_header;
  ObLogEntry t_entry;
  ObLogEntry r_entry;
  uint64_t max_log_id = 60;
  uint64_t times = 2 * 1000; //1K * 60K * 2 = 120M, switch file
  int fd = -1;

  param.read_len_ = OB_MAX_LOG_BUFFER_SIZE;
  param.file_id_ = 1;
  param.log_id_ = 1;
  param.timeout_ = 10000000; //5s
  param.partition_key_ = partition_key;

  FlushLogTask flush_task;
  flush_task.init(buffer.get_align_buf() + 100, 1 << 20);
  EXPECT_EQ(OB_NOT_INIT, writer.submit_flush_log_task(&flush_task));

  // Return the first file, file_id = 1
  EXPECT_EQ(OB_SUCCESS, locator.get_cursor(param.timeout_, &reader_pool, &dreader, file_id, start_offset));
  EXPECT_EQ(OB_SUCCESS, writer.init(file_id, start_offset, param.timeout_, queue_size, &dreader, &write_pool, &cache,
                                    &meta_gen));
  EXPECT_EQ(OB_SUCCESS, submit_task(writer, max_log_id));

  CLOG_LOG(INFO, "write success");

  for (uint64_t i = 1; i <= times; i++) {
    param.log_id_ = i;
    param.file_id_ = OB_INVALID_FILE_ID;
    EXPECT_EQ(OB_SUCCESS, t_header.generate_header(ltype, partition_key, i, buf, data_len, gts, tts,
                                                   rts, 0, 1, 1, ObVersion()));
    EXPECT_EQ(OB_SUCCESS, t_entry.generate_entry(t_header, buf));
    EXPECT_EQ(OB_SUCCESS, submit_task(writer, max_log_id));
  }

  if (fd >= 0) {
    close(fd);
  }
  writer.destroy();
}

TEST_F(TestObLogWriter, test_possible_init_b_error)
{
  ObLogWriter twriter;
  file_id_t file_id = 1;
  int64_t timeout = 10000000; //1s

  // Init failed
  EXPECT_EQ(OB_SUCCESS, twriter.init(file_id, start_offset, timeout, queue_size, &dreader, &wpool, &cache, &meta_gen));
  EXPECT_EQ(OB_INIT_TWICE, twriter.init(file_id, start_offset, param.timeout_, queue_size, &dreader, &write_pool, &cache, &meta_gen));
  twriter.destroy();
}

TEST_F(TestObLogWriter, test_possible_init_t_error)
{
  ObLogWriter twriter;
  file_id_t file_id = 1;
  int64_t timeout = 10000000; //1s

  // Init failed
  TP_SET_ERROR("ob_log_writer.cpp", "init", "tinit", OB_ERROR);
  EXPECT_EQ(OB_ERROR, twriter.init(file_id, start_offset, timeout, queue_size, &dreader, &wpool, &cache, &meta_gen));
  TP_SET("ob_log_writer.cpp", "init", "tinit", NULL);
  twriter.destroy();
}

TEST_F(TestObLogWriter, test_possible_init_iosetup_error)
{
  ObLogWriter twriter;
  file_id_t file_id = 1;
  int64_t timeout = 10000000; //1s

  // Init failed
  TP_SET_ERROR("ob_log_writer.cpp", "init", "iosetup", OB_ERROR);
  EXPECT_EQ(OB_IO_ERROR, twriter.init(file_id, start_offset, timeout, queue_size, &dreader, &wpool, &cache, &meta_gen));
  TP_SET("ob_log_writer.cpp", "init", "iosetup", NULL);
  twriter.destroy();
}

TEST_F(TestObLogWriter, test_possible_init_getfd_error)
{
  ObLogWriter twriter;
  file_id_t file_id = 1;
  int64_t timeout = 10000000; //1s

  // Init failed
  TP_SET_ERROR("ob_log_writer.cpp", "init", "getfd", OB_ERROR);
  EXPECT_EQ(OB_ERROR, twriter.init(file_id, start_offset, timeout, queue_size, &dreader, &wpool, &cache, &meta_gen));
  TP_SET("ob_log_writer.cpp", "init", "getfd", NULL);
  twriter.destroy();
}

TEST_F(TestObLogWriter, test_possible_submit_tasks_errors)
{
  // Submit flush task failed
  ObLogWriter ewriter;
  EXPECT_EQ(OB_SUCCESS, ewriter.init(file_id, start_offset, timeout, queue_size, &dreader, &wpool, &cache, &meta_gen));

  FlushLogTask flush_task;
  flush_task.init(buffer.get_align_buf(), -1);
  EXPECT_EQ(OB_INVALID_ARGUMENT, ewriter.submit_flush_log_task(NULL));
  EXPECT_EQ(OB_INVALID_ARGUMENT, ewriter.submit_flush_log_task(&flush_task));
  flush_task.init(buffer.get_align_buf(), 1 << 20);

  TP_SET_ERROR("ob_log_writer.cpp", "submit_flush_log_task", "push", OB_SIZE_OVERFLOW);
  EXPECT_EQ(OB_EAGAIN, ewriter.submit_flush_log_task(&flush_task));
  TP_SET("ob_log_writer.cpp", "submit_flush_log_task", "push", NULL);

  ewriter.destroy();
}

TEST_F(TestObLogWriter, test_possible_handle_flush_g_errors)
{
  ObLogWriter ewriter;
  EXPECT_EQ(OB_SUCCESS, ewriter.init(file_id, start_offset, timeout, queue_size, &dreader, &wpool, &cache, &meta_gen));

  FlushLogTask flush_task;
  flush_task.init(buffer.get_align_buf() + 100, 1 << 20);
  //Handle flush task
  TP_SET_ERROR("ob_log_writer.cpp", "handle_flush_log_task", "g", OB_ERROR);
  EXPECT_EQ(OB_SUCCESS, ewriter.submit_flush_log_task(&flush_task));
  while (!flush_task.is_done()) {
    PAUSE();
  }
  TP_SET("ob_log_writer.cpp", "handle_flush_log_task", "g", NULL);

  ewriter.destroy();
}

TEST_F(TestObLogWriter, test_possible_handle_flush_s_errors)
{
  ObLogWriter ewriter;
  EXPECT_EQ(OB_SUCCESS, ewriter.init(file_id, start_offset, timeout, queue_size, &dreader, &wpool, &cache, &meta_gen));

  FlushLogTask flush_task;
  flush_task.init(buffer.get_align_buf() + 100, 1 << 20);
  TP_SET_ERROR("ob_log_writer.cpp", "handle_flush_log_task", "s", OB_ERROR);
  EXPECT_EQ(OB_SUCCESS, ewriter.submit_flush_log_task(&flush_task));
  while (!flush_task.is_done()) {
    PAUSE();
  }
  TP_SET("ob_log_writer.cpp", "handle_flush_log_task", "s", NULL);

  ewriter.destroy();
}

TEST_F(TestObLogWriter, test_possible_handle_flush_w_errors)
{
  ObLogWriter ewriter;
  EXPECT_EQ(OB_SUCCESS, ewriter.init(file_id, start_offset, timeout, queue_size, &dreader, &wpool, &cache, &meta_gen));

  FlushLogTask flush_task;
  flush_task.init(buffer.get_align_buf() + 100, 1 << 20);
  TP_SET_ERROR("ob_log_writer.cpp", "handle_flush_log_task", "w", OB_ERROR);
  EXPECT_EQ(OB_SUCCESS, ewriter.submit_flush_log_task(&flush_task));
  while (!flush_task.is_done()) {
    PAUSE();
  }
  TP_SET("ob_log_writer.cpp", "handle_flush_log_task", "w", NULL);

  ewriter.destroy();
}

TEST_F(TestObLogWriter, test_possible_write_data_s_errors)
{
  ObLogWriter ewriter;
  EXPECT_EQ(OB_SUCCESS, ewriter.init(file_id, start_offset, timeout, queue_size, &dreader, &wpool, &cache, &meta_gen));

  FlushLogTask flush_task;
  flush_task.init(buffer.get_align_buf() + 100, 1 << 10);
  TP_SET_ERROR("ob_log_writer.cpp", "write_data", "submit", OB_ERROR);
  EXPECT_EQ(OB_SUCCESS, ewriter.submit_flush_log_task(&flush_task));
  while (!flush_task.is_done()) {
    PAUSE();
  }
  TP_SET("ob_log_writer.cpp", "write_data", "submit", NULL);

  ewriter.destroy();
}

TEST_F(TestObLogWriter, test_possible_write_data_w_errors)
{
  ObLogWriter ewriter;
  EXPECT_EQ(OB_SUCCESS, ewriter.init(file_id, start_offset, timeout, queue_size, &dreader, &wpool, &cache, &meta_gen));

  FlushLogTask flush_task;
  flush_task.init(buffer.get_align_buf() + 200, 1 << 10);
  TP_SET_ERROR("ob_log_writer.cpp", "write_data", "wait", OB_ERROR);
  EXPECT_EQ(OB_SUCCESS, ewriter.submit_flush_log_task(&flush_task));
  while (!flush_task.is_done()) {
    PAUSE();
  }
  TP_SET("ob_log_writer.cpp", "write_data", "wait", NULL);

  ewriter.destroy();
}

} // end namespace unittest
} // end namespace oceanbase

int main(int argc, char **argv)
{
  OB_LOGGER.set_file_name("test_ob_log_writer.log", true);
  OB_LOGGER.set_log_level("DEBUG");
  CLOG_LOG(INFO, "begin unittest::test_ob_log_writer");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
  EXPECT_EQ(OB_INVALID_ARGUMENT, twriter.init(file_id, -1, timeout, queue_size, &dreader, &wpool, NULL, &meta_gen));
  EXPECT_EQ(OB_INVALID_ARGUMENT, twriter.init(OB_INVALID_FILE_ID, start_offset, timeout, queue_size, &dreader, &wpool, NULL, &meta_gen));
  EXPECT_EQ(OB_INVALID_ARGUMENT, twriter.init(file_id, start_offset, 0, queue_size, &dreader, &wpool, NULL, &meta_gen));

  TP_SET_ERROR("ob_log_writer.cpp", "init", "binit", OB_ERROR);
  EXPECT_EQ(OB_ERROR, twriter.init(file_id, start_offset, timeout, queue_size, &dreader, &wpool, &cache, &meta_gen));
  TP_SET("ob_log_writer.cpp", "init", "binit", NULL);

  // Init success
  EXPECT_EQ(OB_SUCCESS, twriter.init(file_id, start_offset, timeout, queue_size, &dreader, &wpool, &cache, &meta_gen));
  EXPECT_EQ(OB_INIT_TWICE, twriter.init(file_id, start_offset, param.timeout_, queue_size, &dreader, &write_pool, &cache, &meta_gen));
  twriter.destroy();
}

TEST_F(TestObLogWriter, test_possible_init_t_error)
{
  ObLogWriter twriter;
  file_id_t file_id = 1;
  int64_t timeout = 10000000; //1s

  // Init failed
  TP_SET_ERROR("ob_log_writer.cpp", "init", "tinit", OB_ERROR);
  EXPECT_EQ(OB_ERROR, twriter.init(file_id, start_offset, timeout, queue_size, &dreader, &wpool, &cache, &meta_gen));
  TP_SET("ob_log_writer.cpp", "init", "tinit", NULL);
  twriter.destroy();
}

TEST_F(TestObLogWriter, test_possible_init_iosetup_error)
{
  ObLogWriter twriter;
  file_id_t file_id = 1;
  int64_t timeout = 10000000; //1s

  // Init failed
  TP_SET_ERROR("ob_log_writer.cpp", "init", "iosetup", OB_ERROR);
  EXPECT_EQ(OB_IO_ERROR, twriter.init(file_id, start_offset, timeout, queue_size, &dreader, &wpool, &cache, &meta_gen));
  TP_SET("ob_log_writer.cpp", "init", "iosetup", NULL);
  twriter.destroy();
}

TEST_F(TestObLogWriter, test_possible_init_getfd_error)
{
  ObLogWriter twriter;
  file_id_t file_id = 1;
  int64_t timeout = 10000000; //1s

  // Init failed
  TP_SET_ERROR("ob_log_writer.cpp", "init", "getfd", OB_ERROR);
  EXPECT_EQ(OB_ERROR, twriter.init(file_id, start_offset, timeout, queue_size, &dreader, &wpool, &cache, &meta_gen));
  TP_SET("ob_log_writer.cpp", "init", "getfd", NULL);
  twriter.destroy();
}

TEST_F(TestObLogWriter, test_possible_submit_tasks_errors)
{
  // Submit flush task failed
  ObLogWriter ewriter;
  EXPECT_EQ(OB_SUCCESS, ewriter.init(file_id, start_offset, timeout, queue_size, &dreader, &wpool, &cache, &meta_gen));

  FlushLogTask flush_task;
  flush_task.init(buffer.get_align_buf(), -1);
  EXPECT_EQ(OB_INVALID_ARGUMENT, ewriter.submit_flush_log_task(NULL));
  EXPECT_EQ(OB_INVALID_ARGUMENT, ewriter.submit_flush_log_task(&flush_task));
  flush_task.init(buffer.get_align_buf(), 1 << 20);

  TP_SET_ERROR("ob_log_writer.cpp", "submit_flush_log_task", "push", OB_SIZE_OVERFLOW);
  EXPECT_EQ(OB_EAGAIN, ewriter.submit_flush_log_task(&flush_task));
  TP_SET("ob_log_writer.cpp", "submit_flush_log_task", "push", NULL);

  ewriter.destroy();
}

TEST_F(TestObLogWriter, test_possible_handle_flush_g_errors)
{
  ObLogWriter ewriter;
  EXPECT_EQ(OB_SUCCESS, ewriter.init(file_id, start_offset, timeout, queue_size, &dreader, &wpool, &cache, &meta_gen));

  FlushLogTask flush_task;
  flush_task.init(buffer.get_align_buf() + 100, 1 << 20);
  //Handle flush task
  TP_SET_ERROR("ob_log_writer.cpp", "handle_flush_log_task", "g", OB_ERROR);
  EXPECT_EQ(OB_SUCCESS, ewriter.submit_flush_log_task(&flush_task));
  while (!flush_task.is_done()) {
    PAUSE();
  }
  TP_SET("ob_log_writer.cpp", "handle_flush_log_task", "g", NULL);

  ewriter.destroy();
}

TEST_F(TestObLogWriter, test_possible_handle_flush_s_errors)
{
  ObLogWriter ewriter;
  EXPECT_EQ(OB_SUCCESS, ewriter.init(file_id, start_offset, timeout, queue_size, &dreader, &wpool, &cache, &meta_gen));

  FlushLogTask flush_task;
  flush_task.init(buffer.get_align_buf() + 100, 1 << 20);
  TP_SET_ERROR("ob_log_writer.cpp", "handle_flush_log_task", "s", OB_ERROR);
  EXPECT_EQ(OB_SUCCESS, ewriter.submit_flush_log_task(&flush_task));
  while (!flush_task.is_done()) {
    PAUSE();
  }
  TP_SET("ob_log_writer.cpp", "handle_flush_log_task", "s", NULL);

  ewriter.destroy();
}

TEST_F(TestObLogWriter, test_possible_handle_flush_w_errors)
{
  ObLogWriter ewriter;
  EXPECT_EQ(OB_SUCCESS, ewriter.init(file_id, start_offset, timeout, queue_size, &dreader, &wpool, &cache, &meta_gen));

  FlushLogTask flush_task;
  flush_task.init(buffer.get_align_buf() + 100, 1 << 20);
  TP_SET_ERROR("ob_log_writer.cpp", "handle_flush_log_task", "w", OB_ERROR);
  EXPECT_EQ(OB_SUCCESS, ewriter.submit_flush_log_task(&flush_task));
  while (!flush_task.is_done()) {
    PAUSE();
  }
  TP_SET("ob_log_writer.cpp", "handle_flush_log_task", "w", NULL);

  ewriter.destroy();
}

TEST_F(TestObLogWriter, test_possible_write_data_s_errors)
{
  ObLogWriter ewriter;
  EXPECT_EQ(OB_SUCCESS, ewriter.init(file_id, start_offset, timeout, queue_size, &dreader, &wpool, &cache, &meta_gen));

  FlushLogTask flush_task;
  flush_task.init(buffer.get_align_buf() + 100, 1 << 10);
  TP_SET_ERROR("ob_log_writer.cpp", "write_data", "submit", OB_ERROR);
  EXPECT_EQ(OB_SUCCESS, ewriter.submit_flush_log_task(&flush_task));
  while (!flush_task.is_done()) {
    PAUSE();
  }
  TP_SET("ob_log_writer.cpp", "write_data", "submit", NULL);

  ewriter.destroy();
}

TEST_F(TestObLogWriter, test_possible_write_data_w_errors)
{
  ObLogWriter ewriter;
  EXPECT_EQ(OB_SUCCESS, ewriter.init(file_id, start_offset, timeout, queue_size, &dreader, &wpool, &cache, &meta_gen));

  FlushLogTask flush_task;
  flush_task.init(buffer.get_align_buf() + 200, 1 << 10);
  TP_SET_ERROR("ob_log_writer.cpp", "write_data", "wait", OB_ERROR);
  EXPECT_EQ(OB_SUCCESS, ewriter.submit_flush_log_task(&flush_task));
  while (!flush_task.is_done()) {
    PAUSE();
  }
  TP_SET("ob_log_writer.cpp", "write_data", "wait", NULL);

  ewriter.destroy();
}

} // end namespace unittest
} // end namespace oceanbase

int main(int argc, char **argv)
{
  OB_LOGGER.set_file_name("test_ob_log_writer.log", true);
  OB_LOGGER.set_log_level("DEBUG");
  CLOG_LOG(INFO, "begin unittest::test_ob_log_writer");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
