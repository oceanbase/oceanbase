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
#include <thread>
#include "lib/file/file_directory_utils.h"
#include "lib/utility/ob_tracepoint.h"
#include "storage/blocksstable/slog/ob_storage_log_reader.h"
#include "storage/blocksstable/slog/ob_storage_log_writer.h"
#include "share/redolog/ob_log_disk_manager.h"

using namespace oceanbase::common;
using namespace ::testing;

namespace oceanbase {
namespace blocksstable {
class TestStorageLogReaderWriter : public ::testing::Test {
public:
  TestStorageLogReaderWriter()
  {}
  virtual ~TestStorageLogReaderWriter()
  {}
  virtual void SetUp();
  virtual void TearDown();
};

void TestStorageLogReaderWriter::SetUp()
{
  system("rm -rf ./test_storage_log_rw");
  FileDirectoryUtils::create_full_path("./test_storage_log_rw/");
}

void TestStorageLogReaderWriter::TearDown()
{
  OB_SLOG_DISK_MGR.destroy();
  system("rm -rf ./test_storage_log_rw");
}

TEST_F(TestStorageLogReaderWriter, normal)
{
  int ret = OB_SUCCESS;
  const char LOG_DIR[512] = "./test_storage_log_rw";
  const int64_t LOG_FILE_SIZE = 64 << 20;  // 64MB
  const int64_t CONCURRENT_TRANS_CNT = 128;
  const int64_t LOG_BUFFER_SIZE = 1966080L;  // 1.875MB

  // write part
  ObLogCursor start_cursor;
  start_cursor.file_id_ = 1;
  start_cursor.log_id_ = 1;
  start_cursor.offset_ = 0;

  char write_data[1024] = "this is slog read write test.";
  ObBaseStorageLogBuffer log_buf;
  ret = log_buf.assign(write_data, 1024);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = log_buf.set_pos(strlen(write_data));
  ASSERT_EQ(OB_SUCCESS, ret);

  ObStorageLogWriter writer;
  writer.init(LOG_DIR, LOG_FILE_SIZE, LOG_BUFFER_SIZE, CONCURRENT_TRANS_CNT);

  ret = writer.start_log(start_cursor);
  ASSERT_EQ(OB_SUCCESS, ret);

  // write dummy log
  start_cursor.reset();
  ret = writer.flush_log(LogCommand::OB_LOG_DUMMY_LOG, log_buf, start_cursor);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(1, start_cursor.file_id_);
  ASSERT_EQ(1, start_cursor.log_id_);

  // write checkpoint log
  start_cursor.reset();
  ret = writer.flush_log(LogCommand::OB_LOG_CHECKPOINT, log_buf, start_cursor);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(1, start_cursor.file_id_);
  ASSERT_EQ(3, start_cursor.log_id_);

  // read part
  LogCommand cmd = LogCommand::OB_LOG_UNKNOWN;
  uint64_t seq = 0;
  int64_t read_len = 0;
  char* read_data = NULL;
  ObLogCursor read_cursor;

  ObStorageLogReader reader;
  ret = reader.init(LOG_DIR, start_cursor.file_id_, 0);
  ASSERT_EQ(OB_SUCCESS, ret);

  // read dummy log
  ret = reader.read_log(cmd, seq, read_data, read_len);
  ASSERT_EQ(OB_SUCCESS, ret);

  ASSERT_EQ(LogCommand::OB_LOG_DUMMY_LOG, cmd);
  ASSERT_EQ(1, seq);
  ASSERT_EQ(strlen(write_data), read_len);
  ASSERT_TRUE(0 == strncmp(write_data, read_data, read_len - 1));

  ret = reader.get_cursor(read_cursor);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(1, read_cursor.file_id_);
  ASSERT_EQ(1, read_cursor.log_id_);
  OB_LOG(INFO, "read log ", K(read_cursor));

  // read NOP log
  ret = reader.read_log(cmd, seq, read_data, read_len);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(LogCommand::OB_LOG_NOP, cmd);
  ASSERT_EQ(2, seq);

  ret = reader.get_cursor(read_cursor);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(1, read_cursor.file_id_);
  ASSERT_EQ(2, read_cursor.log_id_);
  OB_LOG(INFO, "read log ", K(read_cursor));
  ASSERT_TRUE(0 == read_cursor.offset_ % OB_DIRECT_IO_ALIGN);

  // read checkpoint log
  read_data = NULL;
  ret = reader.read_log(cmd, seq, read_data, read_len);
  ASSERT_EQ(OB_SUCCESS, ret);

  ASSERT_EQ(LogCommand::OB_LOG_CHECKPOINT, cmd);
  ASSERT_EQ(3, seq);
  ASSERT_EQ(strlen(write_data), read_len);
  ASSERT_TRUE(0 == strncmp(write_data, read_data, read_len - 1));

  ret = reader.get_cursor(read_cursor);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(1, read_cursor.file_id_);
  ASSERT_EQ(3, read_cursor.log_id_);
  OB_LOG(INFO, "read log ", K(read_cursor));

  // read NOP log
  read_data = NULL;
  ret = reader.read_log(cmd, seq, read_data, read_len);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(LogCommand::OB_LOG_NOP, cmd);
  ASSERT_EQ(4, seq);

  ret = reader.get_cursor(read_cursor);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(1, read_cursor.file_id_);
  ASSERT_EQ(4, read_cursor.log_id_);
  OB_LOG(INFO, "read log ", K(read_cursor));
  ASSERT_TRUE(0 == read_cursor.offset_ % OB_DIRECT_IO_ALIGN);

  // read end of file
  read_data = NULL;
  ret = reader.read_log(cmd, seq, read_data, read_len);
  ASSERT_EQ(OB_READ_NOTHING, ret);
  ret = reader.get_cursor(read_cursor);
  ASSERT_EQ(OB_SUCCESS, ret);
  OB_LOG(INFO, "read log ", K(read_cursor));
}

TEST_F(TestStorageLogReaderWriter, large_buf)
{
  int ret = OB_SUCCESS;
  const char LOG_DIR[512] = "./test_storage_log_rw";
  const int64_t LOG_FILE_SIZE = 64 << 20;  // 64MB
  const int64_t CONCURRENT_TRANS_CNT = 128;
  const int64_t LOG_BUFFER_SIZE = 1966080L;  // 1.875MB
  const int64_t LEN_1MB = 1048578;
  const int64_t LEN_1500K = 1536000;

  // prepare writer
  ObLogCursor start_cursor;
  start_cursor.file_id_ = 1;
  start_cursor.log_id_ = 1;
  start_cursor.offset_ = 0;

  ObMemAttr attr(OB_SERVER_TENANT_ID, ObModIds::TEST);
  char* data_1M = (char*)ob_malloc(LEN_1MB, attr);
  MEMSET(data_1M, 1, LEN_1MB);
  ObBaseStorageLogBuffer buf_1K;
  ret = buf_1K.assign(data_1M, LEN_1MB);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = buf_1K.set_pos(LEN_1MB);
  ASSERT_EQ(OB_SUCCESS, ret);

  char* data_1500K = (char*)ob_malloc(LEN_1500K, attr);
  MEMSET(data_1500K, 2, LEN_1500K);
  ObBaseStorageLogBuffer buf_1500K;
  ret = buf_1500K.assign(data_1500K, LEN_1500K);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = buf_1500K.set_pos(LEN_1500K);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObStorageLogWriter writer;
  writer.init(LOG_DIR, LOG_FILE_SIZE, LOG_BUFFER_SIZE, CONCURRENT_TRANS_CNT);

  ret = writer.start_log(start_cursor);
  ASSERT_EQ(OB_SUCCESS, ret);

  // write 1M log and 1500K log
  ret = writer.flush_log(LogCommand::OB_LOG_DUMMY_LOG, buf_1K, start_cursor);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = writer.flush_log(LogCommand::OB_LOG_DUMMY_LOG, buf_1500K, start_cursor);
  ASSERT_EQ(OB_SUCCESS, ret);

  // prepare reader
  LogCommand cmd = LogCommand::OB_LOG_UNKNOWN;
  uint64_t seq = 0;
  int64_t read_len = 0;
  char* read_data = NULL;
  ObLogCursor read_cursor;

  ObStorageLogReader reader;
  ret = reader.init(LOG_DIR, start_cursor.file_id_, 0);
  ASSERT_EQ(OB_SUCCESS, ret);

  // read to the end
  while (OB_SUCC(ret)) {
    read_data = NULL;
    ret = reader.read_log(cmd, seq, read_data, read_len);
  }

  ASSERT_EQ(OB_READ_NOTHING, ret);
  ASSERT_EQ(LogCommand::OB_LOG_NOP, cmd);
  ASSERT_EQ(4, seq);

  ob_free(data_1M);
  ob_free(data_1500K);
}

TEST_F(TestStorageLogReaderWriter, over_2MB)
{
  int ret = OB_SUCCESS;
  const char LOG_DIR[512] = "./test_storage_log_rw";
  const int64_t LOG_FILE_SIZE = 64 << 20;  // 64MB
  const int64_t CONCURRENT_TRANS_CNT = 128;
  const int64_t LOG_BUFFER_SIZE = 1966080L;  // 1.875MB
  const int64_t LEN_2MB = (1 << 21) + 64;    // 2MB + 64 Bytes

  // prepare writer
  ObLogCursor start_cursor;
  start_cursor.file_id_ = 1;
  start_cursor.log_id_ = 1;
  start_cursor.offset_ = 0;

  ObMemAttr attr(OB_SERVER_TENANT_ID, ObModIds::TEST);
  char* data_2M = (char*)ob_malloc(LEN_2MB, attr);
  MEMSET(data_2M, 1, LEN_2MB);
  ObBaseStorageLogBuffer buf_2M;
  ret = buf_2M.assign(data_2M, LEN_2MB);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = buf_2M.set_pos(LEN_2MB);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObStorageLogWriter writer;
  ret = writer.init(LOG_DIR, LOG_FILE_SIZE, LOG_BUFFER_SIZE, CONCURRENT_TRANS_CNT);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = writer.start_log(start_cursor);
  ASSERT_EQ(OB_SUCCESS, ret);

  // write 2M log
  ret = writer.flush_log(LogCommand::OB_LOG_DUMMY_LOG, buf_2M, start_cursor);
  ASSERT_EQ(OB_SUCCESS, ret);

  // prepare reader
  LogCommand cmd = LogCommand::OB_LOG_UNKNOWN;
  uint64_t seq = 0;
  int64_t read_len = 0;
  char* read_data = NULL;
  ObLogCursor read_cursor;

  ObStorageLogReader reader;
  ret = reader.init(LOG_DIR, start_cursor.file_id_, 0);
  ASSERT_EQ(OB_SUCCESS, ret);

  // read to the end
  while (OB_SUCC(ret)) {
    read_data = NULL;
    ret = reader.read_log(cmd, seq, read_data, read_len);
    if (LogCommand::OB_LOG_DUMMY_LOG == cmd) {
      ASSERT_TRUE(0 == MEMCMP(data_2M, read_data, LEN_2MB));
      _OB_LOG(INFO, "compare data success");
    }
  }

  ASSERT_EQ(OB_READ_NOTHING, ret);
  ASSERT_EQ(LogCommand::OB_LOG_NOP, cmd);
  ASSERT_EQ(2, seq);

  ob_free(data_2M);
}

TEST_F(TestStorageLogReaderWriter, large_item_with_switch_log)
{
  int ret = OB_SUCCESS;
  const char LOG_DIR[512] = "./test_storage_log_rw";
  const int64_t LOG_FILE_SIZE = 51200;
  const int64_t CONCURRENT_TRANS_CNT = 128;
  const int64_t LOG_BUFFER_SIZE = 51200;

  // ObLogEntry size is 44. Left 2 byte for the NOP entry after ObLogEntry + DATA_LEN, so
  // that NOP entry will consumed another 512 byte for alignment.
  const int64_t DATA_LEN = 549842;

  // prepare writer
  ObLogCursor start_cursor;
  start_cursor.file_id_ = 1;
  start_cursor.log_id_ = 1;
  start_cursor.offset_ = 0;

  ObMemAttr attr(OB_SERVER_TENANT_ID, ObModIds::TEST);
  char* data = (char*)ob_malloc(DATA_LEN, attr);
  MEMSET(data, 1, DATA_LEN);
  ObBaseStorageLogBuffer buf;
  ret = buf.assign(data, DATA_LEN);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = buf.set_pos(DATA_LEN);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObStorageLogWriter writer;
  ret = writer.init(LOG_DIR, LOG_FILE_SIZE, LOG_BUFFER_SIZE, CONCURRENT_TRANS_CNT);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = writer.start_log(start_cursor);
  ASSERT_EQ(OB_SUCCESS, ret);

  // write 2M log
  ret = writer.flush_log(LogCommand::OB_LOG_DUMMY_LOG, buf, start_cursor);
  ASSERT_EQ(OB_SUCCESS, ret);

  // prepare reader
  LogCommand cmd = LogCommand::OB_LOG_UNKNOWN;
  uint64_t seq = 0;
  int64_t read_len = 0;
  char* read_data = NULL;
  ObLogCursor read_cursor;

  ObStorageLogReader reader;
  ret = reader.init(LOG_DIR, start_cursor.file_id_, 0);
  ASSERT_EQ(OB_SUCCESS, ret);

  // read to the end
  while (OB_SUCC(ret)) {
    read_data = NULL;
    ret = reader.read_log(cmd, seq, read_data, read_len);
    OB_LOG(INFO, "read log", K(ret), K(cmd), K(seq), K(read_len));
    if (LogCommand::OB_LOG_DUMMY_LOG == cmd) {
      ASSERT_TRUE(0 == MEMCMP(data, read_data, DATA_LEN));
      OB_LOG(INFO, "compare data success");
    }
  }

  ASSERT_EQ(OB_READ_NOTHING, ret);
  ASSERT_EQ(LogCommand::OB_LOG_SWITCH_LOG, cmd);
  ASSERT_EQ(3, seq);

  ob_free(data);
}

TEST_F(TestStorageLogReaderWriter, large_item_batch_write)
{
  int ret = OB_SUCCESS;
  const char LOG_DIR[512] = "./test_storage_log_rw";
  const int64_t LOG_FILE_SIZE = 12 * 1024;  // 12K
  const int64_t CONCURRENT_TRANS_CNT = 128;
  const int64_t LOG_BUFFER_SIZE = 512 * 1024;  // 512K

  const int64_t DATA_LEN = 4 << 10;  // 4K, let each batch contain 1 item

  // prepare writer
  ObLogCursor start_cursor;
  start_cursor.file_id_ = 1;
  start_cursor.log_id_ = 1;
  start_cursor.offset_ = 0;

  ObMemAttr attr(OB_SERVER_TENANT_ID, ObModIds::TEST);
  char* data = (char*)ob_malloc(DATA_LEN, attr);
  MEMSET(data, 1, DATA_LEN);
  ObBaseStorageLogBuffer buf;
  ret = buf.assign(data, DATA_LEN);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = buf.set_pos(DATA_LEN);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObStorageLogWriter writer;
  ret = writer.init(LOG_DIR, LOG_FILE_SIZE, LOG_BUFFER_SIZE, CONCURRENT_TRANS_CNT);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = writer.start_log(start_cursor);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObLogCursor flush_cursor_1;
  ObLogCursor flush_cursor_2;
  ObLogCursor flush_cursor_3;
  std::thread t1(std::bind(&ObStorageLogWriter::flush_log, &writer, LogCommand::OB_LOG_DUMMY_LOG, buf, flush_cursor_1));
  std::thread t2(std::bind(&ObStorageLogWriter::flush_log, &writer, LogCommand::OB_LOG_DUMMY_LOG, buf, flush_cursor_2));
  std::thread t3(std::bind(&ObStorageLogWriter::flush_log, &writer, LogCommand::OB_LOG_DUMMY_LOG, buf, flush_cursor_3));
  t1.detach();
  t2.detach();
  t3.detach();

  sleep(5);

  // prepare reader
  LogCommand cmd = LogCommand::OB_LOG_UNKNOWN;
  uint64_t seq = 0;
  int64_t read_len = 0;
  char* read_data = NULL;
  ObLogCursor read_cursor;

  ObStorageLogReader reader;
  ret = reader.init(LOG_DIR, start_cursor.file_id_, 0);
  ASSERT_EQ(OB_SUCCESS, ret);

  // read to the end
  while (OB_SUCC(ret)) {
    read_data = NULL;
    ret = reader.read_log(cmd, seq, read_data, read_len);
    OB_LOG(INFO, "read log", K(ret), K(cmd), K(seq), K(read_len));
    if (LogCommand::OB_LOG_DUMMY_LOG == cmd) {
      ASSERT_TRUE(0 == MEMCMP(data, read_data, DATA_LEN));
      OB_LOG(INFO, "compare data success");
    }
  }

  ASSERT_EQ(OB_READ_NOTHING, ret);
  ASSERT_EQ(LogCommand::OB_LOG_SWITCH_LOG, cmd);
  ASSERT_EQ(9, seq);

  ob_free(data);
}

TEST_F(TestStorageLogReaderWriter, revise)
{
  int ret = OB_SUCCESS;
  const char LOG_DIR[512] = "./test_storage_log_rw";
  const int64_t LOG_FILE_SIZE = 2 << 20;  // 2MB
  const int64_t CONCURRENT_TRANS_CNT = 8;
  const int64_t LOG_BUFFER_SIZE = 1966080L;  // 1.875MB

  // write part
  ObLogCursor start_cursor;
  start_cursor.file_id_ = 1;
  start_cursor.log_id_ = 1;
  start_cursor.offset_ = 0;

  const int data_size = 5000;
  char write_data[data_size];
  MEMSET(write_data, 1, data_size);
  ObBaseStorageLogBuffer log_buf;
  ret = log_buf.assign(write_data, data_size);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = log_buf.set_pos(data_size);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObStorageLogWriter writer;
  writer.init(LOG_DIR, LOG_FILE_SIZE, LOG_BUFFER_SIZE, CONCURRENT_TRANS_CNT);

  ret = writer.start_log(start_cursor);
  ASSERT_EQ(OB_SUCCESS, ret);

  // write 3 logs so that valid data length is 4K * 3 = 12288
  for (int64_t i = 0; i < 3; ++i) {
    start_cursor.reset();
    ret = writer.flush_log(LogCommand::OB_LOG_DUMMY_LOG, log_buf, start_cursor);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(1, start_cursor.file_id_);
    // data entry + nop entry
    ASSERT_EQ((i * 2) + 1, start_cursor.log_id_);
  }

  // truncate the file so that last log is incomplete
  ASSERT_TRUE(0 == ::truncate("./test_storage_log_rw/1", 20480));

  // revise log
  ObStorageLogReader reader;
  ret = reader.init(LOG_DIR, start_cursor.file_id_, 0);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = reader.revise_log(false);
  ASSERT_EQ(OB_SUCCESS, ret);

  int64_t revise_size = 0;
  ret = FileDirectoryUtils::get_file_size("./test_storage_log_rw/1", revise_size);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(16384, revise_size);
}

// the last log file has switch file entry at the end
// revise should still succeed even the next file not exist
TEST_F(TestStorageLogReaderWriter, switch_file_revise)
{
  int ret = OB_SUCCESS;
  const char LOG_DIR[512] = "./test_storage_log_rw";
  const int64_t LOG_FILE_SIZE = 16 * 1024;  // 16KB
  const int64_t CONCURRENT_TRANS_CNT = 8;
  const int64_t LOG_BUFFER_SIZE = 1966080L;  // 1.875MB

  // prepare data with 800 Bytes length
  char write_data[800];
  MEMSET(write_data, 1, 800);
  ObBaseStorageLogBuffer log_buf;
  ret = log_buf.assign(write_data, 800);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = log_buf.set_pos(800);
  ASSERT_EQ(OB_SUCCESS, ret);

  // prepare log writer
  ObStorageLogWriter writer;
  writer.init(LOG_DIR, LOG_FILE_SIZE, LOG_BUFFER_SIZE, CONCURRENT_TRANS_CNT);
  ObLogCursor start_cursor;
  start_cursor.file_id_ = 1;
  start_cursor.log_id_ = 1;
  start_cursor.offset_ = 0;
  ret = writer.start_log(start_cursor);
  ASSERT_EQ(OB_SUCCESS, ret);

  // write log, the length after padding should be 2KB
  start_cursor.reset();
  ret = writer.flush_log(LogCommand::OB_LOG_DUMMY_LOG, log_buf, start_cursor);
  ASSERT_EQ(OB_SUCCESS, ret);
  // log start cursor check
  ASSERT_EQ(1, start_cursor.file_id_);
  ASSERT_EQ(1, start_cursor.log_id_);
  // after flush, should start new file
  ObLogCursor cur_cursor = writer.get_cur_cursor();
  ASSERT_EQ(2, cur_cursor.file_id_);
  ASSERT_EQ(0, cur_cursor.offset_);
  ASSERT_EQ(4, cur_cursor.log_id_);  // data entry + nop entry + switch entry

  // revise log
  ObStorageLogReader reader;
  ret = reader.init(LOG_DIR, start_cursor.file_id_, 0);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = reader.revise_log(false);
  ASSERT_EQ(OB_SUCCESS, ret);

  // post check file size
  int64_t revise_size = 0;
  ret = FileDirectoryUtils::get_file_size("./test_storage_log_rw/1", revise_size);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(3 * 4096, revise_size);  // truncate last 4k
}

TEST_F(TestStorageLogReaderWriter, errsim_io_hung)
{
  int ret = OB_SUCCESS;
  const char LOG_DIR[512] = "./test_storage_log_rw";
  const int64_t LOG_FILE_SIZE = 16 * 1024;  // 16KB
  const int64_t CONCURRENT_TRANS_CNT = 8;
  const int64_t LOG_BUFFER_SIZE = 1966080L;  // 1.875MB

  // prepare data with 800 Bytes length
  char write_data[800];
  MEMSET(write_data, 1, 800);
  ObBaseStorageLogBuffer log_buf;
  ret = log_buf.assign(write_data, 800);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = log_buf.set_pos(800);
  ASSERT_EQ(OB_SUCCESS, ret);

  // prepare log writer
  ObStorageLogWriter writer;
  writer.init(LOG_DIR, LOG_FILE_SIZE, LOG_BUFFER_SIZE, CONCURRENT_TRANS_CNT);
  ObLogCursor start_cursor;
  start_cursor.file_id_ = 1;
  start_cursor.log_id_ = 1;
  start_cursor.offset_ = 0;
  ret = writer.start_log(start_cursor);
  ASSERT_EQ(OB_SUCCESS, ret);

  // write log, make flush log io timeout
#ifdef ERRSIM
  TP_SET_EVENT(EventTable::EN_SLOG_WAIT_FLUSH_LOG, OB_TIMEOUT, 0, 1);
#endif
  start_cursor.reset();
  ret = writer.flush_log(LogCommand::OB_LOG_DUMMY_LOG, log_buf, start_cursor);
#ifdef ERRSIM
  TP_SET_EVENT(EventTable::EN_SLOG_WAIT_FLUSH_LOG, OB_TIMEOUT, 0, 0);
  ASSERT_EQ(OB_STATE_NOT_MATCH, ret);
  bool is_frozen = writer.is_frozen();
  ASSERT_TRUE(is_frozen);
#else
  ASSERT_EQ(OB_SUCCESS, ret);
#endif
}

// when the target log_seq occurs at the first log, reading logs should still succeed
TEST_F(TestStorageLogReaderWriter, seek_first_log)
{
  int ret = OB_SUCCESS;
  const char LOG_DIR[512] = "./test_storage_log_rw";
  const int64_t LOG_FILE_SIZE = 64 << 20;  // 64MB
  const int64_t CONCURRENT_TRANS_CNT = 128;
  const int64_t LOG_BUFFER_SIZE = 1966080L;  // 1.875MB
  const int64_t LOG_FILE_ID = 615;
  const int64_t LOG_START_SEQ = 320168236;

  // write part
  ObLogCursor start_cursor;
  start_cursor.file_id_ = LOG_FILE_ID;
  start_cursor.log_id_ = LOG_START_SEQ;
  start_cursor.offset_ = 0;

  char write_data[1024] = "some checkpoint data";
  ObBaseStorageLogBuffer log_buf;
  ret = log_buf.assign(write_data, 1024);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = log_buf.set_pos(strlen(write_data));
  ASSERT_EQ(OB_SUCCESS, ret);

  ObStorageLogWriter writer;
  writer.init(LOG_DIR, LOG_FILE_SIZE, LOG_BUFFER_SIZE, CONCURRENT_TRANS_CNT);

  ret = writer.start_log(start_cursor);
  ASSERT_EQ(OB_SUCCESS, ret);

  // write checkpoint log
  start_cursor.reset();
  ret = writer.flush_log(LogCommand::OB_LOG_CHECKPOINT, log_buf, start_cursor);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(LOG_FILE_ID, start_cursor.file_id_);
  ASSERT_EQ(LOG_START_SEQ, start_cursor.log_id_);

  // read part
  LogCommand cmd = LogCommand::OB_LOG_UNKNOWN;
  uint64_t seq = 0;
  int64_t read_len = 0;
  char* read_data = nullptr;
  ObLogCursor read_cursor;

  // let's reader start from LOG_START_SEQ, the first log
  ObStorageLogReader reader;
  ret = reader.init(LOG_DIR, LOG_FILE_ID, LOG_START_SEQ - 1);
  ASSERT_EQ(OB_SUCCESS, ret);

  // read checkpoint log
  ret = reader.read_log(cmd, seq, read_data, read_len);
  ASSERT_EQ(OB_SUCCESS, ret);

  ASSERT_EQ(LogCommand::OB_LOG_CHECKPOINT, cmd);
  ASSERT_EQ(LOG_START_SEQ, seq);
  ASSERT_EQ(strlen(write_data), read_len);
  ASSERT_TRUE(0 == strncmp(write_data, read_data, read_len - 1));

  ret = reader.get_cursor(read_cursor);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(LOG_FILE_ID, read_cursor.file_id_);
  ASSERT_EQ(LOG_START_SEQ, read_cursor.log_id_);

  // read NOP log
  read_data = nullptr;
  ret = reader.read_log(cmd, seq, read_data, read_len);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(LogCommand::OB_LOG_NOP, cmd);
  ASSERT_EQ(LOG_START_SEQ + 1, seq);

  ret = reader.get_cursor(read_cursor);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(LOG_FILE_ID, read_cursor.file_id_);
  ASSERT_EQ(LOG_START_SEQ + 1, read_cursor.log_id_);
  ASSERT_TRUE(0 == read_cursor.offset_ % OB_DIRECT_IO_ALIGN);
  OB_LOG(INFO, "read NOP log", K(read_cursor));

  // read end of file
  read_data = nullptr;
  ret = reader.read_log(cmd, seq, read_data, read_len);
  ASSERT_EQ(OB_READ_NOTHING, ret);
  ret = reader.get_cursor(read_cursor);
  ASSERT_EQ(OB_SUCCESS, ret);
  OB_LOG(INFO, "read finish ", K(read_cursor));
}

TEST_F(TestStorageLogReaderWriter, seek_second_log)
{
  int ret = OB_SUCCESS;
  const char LOG_DIR[512] = "./test_storage_log_rw";
  const int64_t LOG_FILE_SIZE = 64 << 20;  // 64MB
  const int64_t CONCURRENT_TRANS_CNT = 128;
  const int64_t LOG_BUFFER_SIZE = 1966080L;  // 1.875MB
  const int64_t LOG_FILE_ID = 615;
  const int64_t LOG_START_SEQ = 320168236;

  // write part
  ObLogCursor start_cursor;
  start_cursor.file_id_ = LOG_FILE_ID;
  start_cursor.log_id_ = LOG_START_SEQ;
  start_cursor.offset_ = 0;

  char write_data[1024] = "some checkpoint data";
  ObBaseStorageLogBuffer log_buf;
  ret = log_buf.assign(write_data, 1024);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = log_buf.set_pos(strlen(write_data));
  ASSERT_EQ(OB_SUCCESS, ret);

  ObStorageLogWriter writer;
  writer.init(LOG_DIR, LOG_FILE_SIZE, LOG_BUFFER_SIZE, CONCURRENT_TRANS_CNT);

  ret = writer.start_log(start_cursor);
  ASSERT_EQ(OB_SUCCESS, ret);

  // write checkpoint log
  start_cursor.reset();
  ret = writer.flush_log(LogCommand::OB_LOG_CHECKPOINT, log_buf, start_cursor);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(LOG_FILE_ID, start_cursor.file_id_);
  ASSERT_EQ(LOG_START_SEQ, start_cursor.log_id_);

  // read part
  LogCommand cmd = LogCommand::OB_LOG_UNKNOWN;
  uint64_t seq = 0;
  int64_t read_len = 0;
  char* read_data = nullptr;
  ObLogCursor read_cursor;

  // let's reader start from LOG_START_SEQ + 1, the second log
  ObStorageLogReader reader;
  ret = reader.init(LOG_DIR, LOG_FILE_ID, LOG_START_SEQ);
  ASSERT_EQ(OB_SUCCESS, ret);

  // read NOP log
  read_data = nullptr;
  ret = reader.read_log(cmd, seq, read_data, read_len);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(LogCommand::OB_LOG_NOP, cmd);
  ASSERT_EQ(LOG_START_SEQ + 1, seq);

  ret = reader.get_cursor(read_cursor);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(LOG_FILE_ID, read_cursor.file_id_);
  ASSERT_EQ(LOG_START_SEQ + 1, read_cursor.log_id_);
  ASSERT_TRUE(0 == read_cursor.offset_ % OB_DIRECT_IO_ALIGN);
  OB_LOG(INFO, "read NOP log", K(read_cursor));

  // read end of file
  read_data = nullptr;
  ret = reader.read_log(cmd, seq, read_data, read_len);
  ASSERT_EQ(OB_READ_NOTHING, ret);
  ret = reader.get_cursor(read_cursor);
  ASSERT_EQ(OB_SUCCESS, ret);
  OB_LOG(INFO, "read finish ", K(read_cursor));
}
}  // namespace blocksstable
}  // namespace oceanbase

int main(int argc, char** argv)
{
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
