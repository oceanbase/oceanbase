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
#include <gmock/gmock.h>
#define private public
#include "simple_ob_partition_image.h"
#include "lib/io/ob_io_manager.h"
#undef private

using namespace oceanbase::obsys;
using namespace oceanbase::lib;
using namespace oceanbase::common;
using namespace ::testing;

namespace oceanbase {
namespace blocksstable {
class TestSlogDiskError : public ::testing::Test {
public:
  TestSlogDiskError()
  {}
  virtual ~TestSlogDiskError()
  {}
  virtual void SetUp();
  virtual void TearDown();
  virtual void prepare_conf(const int start_fail_index, const int fail_cnt, const int reset_call_cnt, const int error);
  virtual int prepare_log_file(ObLogCursor& last_log_cursor);
  virtual int make_log_not_complete(const ObLogCursor& last_log_cursor);
  virtual int make_log_ruined(const ObLogCursor& last_log_cursor);
  virtual int replay_log();

protected:
  ObLogCursor replay_start_cursor_;
};

void TestSlogDiskError::SetUp()
{
  prepare_conf(1000000, 0, 1, 0);

  ASSERT_EQ(OB_SUCCESS, ObIOManager::get_instance().init());
  system("rm -rf ./redo_log_test/*");
  FileDirectoryUtils::create_full_path("./redo_log_test/data/");
  FileDirectoryUtils::create_full_path("./redo_log_test/log/");
  replay_start_cursor_.file_id_ = 1;
  replay_start_cursor_.log_id_ = 0;
  replay_start_cursor_.offset_ = 0;
}

void TestSlogDiskError::TearDown()
{
  system("rm -rf ./redo_log_test/*");
  ObIOManager::get_instance().destroy();
}

void TestSlogDiskError::prepare_conf(
    const int start_fail_index, const int fail_cnt, const int reset_call_cnt, const int error)
{
  int fd = ::open("pwrite_conf", O_CREAT | O_TRUNC | O_RDWR, S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH);
  if (fd < 0) {
    STORAGE_REDO_LOG(WARN, "open failed", KERRMSG);
  }
  ASSERT_TRUE(fd > 0);
  char buf[64];
  memset(buf, 0, sizeof(buf));
  snprintf(buf, sizeof(buf), "%d,%d,%d,%d", start_fail_index, fail_cnt, reset_call_cnt, error);
  ::write(fd, buf, strlen(buf));
  ::close(fd);
  sleep(3);
}

int TestSlogDiskError::prepare_log_file(ObLogCursor& last_log_cursor)
{
  int ret = OB_SUCCESS;
  static const int64_t max_log_size = 64 * 1024 * 1024;
  SimpleObPartitionImage partition_image;
  ObStorageLogCommittedTransGetter committed_trans_getter;

  if (OB_FAIL(partition_image.init("./redo_log_test/data/", &SLOGGER))) {
    STORAGE_REDO_LOG(WARN, "init partition_image failed", K(ret));
  } else if (OB_FAIL(SLOGGER.init("./redo_log_test/log/", max_log_size))) {
    STORAGE_REDO_LOG(WARN, "int redo_log failed", K(ret));
  } else if (OB_FAIL(committed_trans_getter.init(SLOGGER.get_log_dir(), replay_start_cursor_))) {
    STORAGE_REDO_LOG(WARN, "fail to init committed trans getter", K(ret));
  } else if (OB_FAIL(SLOGGER.replay(replay_start_cursor_, committed_trans_getter))) {
    STORAGE_REDO_LOG(WARN, "replay failed", K(ret));
  }

  if (OB_SUCC(ret)) {
    SimpleObPartition partition;
    for (int64_t i = 0; i < 100; ++i) {
      partition.table_id_ = 50001 + i;
      partition.partition_id_ = 0;
      partition.macro_block_cnt_ = 10;
      for (int64_t i = 0; i < partition.macro_block_cnt_; ++i) {
        partition.macro_blocks_[i] = ObRandom::rand(0, 100);
      }
      if (OB_FAIL(partition_image.add_partition(partition))) {
        STORAGE_REDO_LOG(WARN, "add_partition failed", K(ret));
      } else {
        if (98 == i) {
          last_log_cursor = SLOGGER.log_writer_.get_cur_cursor();
        }
      }
    }
  }
  return ret;
}

int TestSlogDiskError::make_log_not_complete(const ObLogCursor& last_log_cursor)
{
  int ret = OB_SUCCESS;
  const int64_t new_file_size = last_log_cursor.offset_ + sizeof(ObLogEntry) + 5;
  int fd = open("./redo_log_test/log/1", O_WRONLY);
  if (fd < 0) {
    STORAGE_REDO_LOG(WARN, "open file failed", K(fd), K(errno));
  } else if (ftruncate(fd, new_file_size) < 0) {
    ret = OB_IO_ERROR;
    STORAGE_REDO_LOG(WARN, "ftruncate failed", K(ret));
  } else if (fsync(fd) < 0) {
    ret = OB_IO_ERROR;
    STORAGE_REDO_LOG(WARN, "fsync failed", K(ret), K(fd));
  }
  return ret;
}

int TestSlogDiskError::make_log_ruined(const ObLogCursor& last_log_cursor)
{
  int ret = OB_SUCCESS;
  const int64_t start_write_offset = last_log_cursor.offset_ + sizeof(ObLogEntry) + 5;
  int fd = open("./redo_log_test/log/1", O_WRONLY);
  if (fd < 0) {
    STORAGE_REDO_LOG(WARN, "open file failed", K(fd), K(errno));
  } else if (pwrite(fd, "0123456789", 10, start_write_offset) != 10) {
    ret = OB_IO_ERROR;
    STORAGE_REDO_LOG(WARN, "pwrite failed", K(ret));
  } else if (fsync(fd) < 0) {
    ret = OB_IO_ERROR;
    STORAGE_REDO_LOG(WARN, "fsync failed", K(ret), K(fd));
  }
  return ret;
}

int TestSlogDiskError::replay_log()
{
  int ret = OB_SUCCESS;
  static const int64_t max_log_size = 64 * 1024 * 1024;
  SimpleObPartitionImage partition_image;
  ObStorageLogCommittedTransGetter committed_trans_getter;
  if (OB_FAIL(partition_image.init("./redo_log_test/data/", &SLOGGER))) {
    STORAGE_REDO_LOG(WARN, "init partition_image failed", K(ret));
  } else if (OB_FAIL(SLOGGER.init("./redo_log_test/log/", max_log_size))) {
    STORAGE_REDO_LOG(WARN, "int redo_log failed", K(ret));
  } else if (OB_FAIL(committed_trans_getter.init(SLOGGER.get_log_dir(), replay_start_cursor_))) {
    STORAGE_REDO_LOG(WARN, "fail to init committed trans getter", K(ret));
  } else if (OB_FAIL(SLOGGER.replay(replay_start_cursor_, committed_trans_getter))) {
    STORAGE_REDO_LOG(WARN, "replay failed", K(ret));
  }
  return ret;
}

TEST_F(TestSlogDiskError, test_fatal_error_freeze)
{
  int ret = OB_SUCCESS;
  static const int64_t max_log_size = 64 * 1024 * 1024;
  SimpleObPartitionImage partition_image;
  ObStorageLogCommittedTransGetter committed_trans_getter;

  ret = partition_image.init("./redo_log_test/data/", &SLOGGER);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = SLOGGER.init("./redo_log_test/log/", max_log_size);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(OB_SUCCESS, committed_trans_getter.init(SLOGGER.get_log_dir(), replay_start_cursor_));
  ASSERT_EQ(OB_SUCCESS, SLOGGER.replay(replay_start_cursor_, committed_trans_getter));

  prepare_conf(100, 10, 1, EIO);

  SimpleObPartition partition;
  for (int64_t i = 0; i < 100; ++i) {
    partition.table_id_ = 50001 + i;
    partition.partition_id_ = 0;
    partition.macro_block_cnt_ = 10;
    for (int64_t i = 0; i < partition.macro_block_cnt_; ++i) {
      partition.macro_blocks_[i] = ObRandom::rand(0, 100);
    }
    ASSERT_EQ(OB_SUCCESS, partition_image.add_partition(partition));
  }

  // fatal error occur, slog writer will be frozen
  partition.table_id_ += 1;
  ASSERT_EQ(OB_IO_ERROR, partition_image.add_partition(partition));

  // frozen, retrun OB_STATE_NOT_MATCH
  ASSERT_EQ(OB_STATE_NOT_MATCH, partition_image.add_partition(partition));
}

TEST_F(TestSlogDiskError, test_disk_full)
{
  int ret = OB_SUCCESS;
  static const int64_t max_log_size = 64 * 1024 * 1024;
  SimpleObPartitionImage partition_image;
  ObStorageLogCommittedTransGetter committed_trans_getter;

  ret = partition_image.init("./redo_log_test/data/", &SLOGGER);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = SLOGGER.init("./redo_log_test/log/", max_log_size);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(OB_SUCCESS, committed_trans_getter.init(SLOGGER.get_log_dir(), replay_start_cursor_));
  ASSERT_EQ(OB_SUCCESS, SLOGGER.replay(replay_start_cursor_, committed_trans_getter));

  prepare_conf(100, 30, 1, ENOSPC);

  SimpleObPartition partition;
  for (int64_t i = 0; i < 100; ++i) {
    partition.table_id_ = 50001 + i;
    partition.partition_id_ = 0;
    partition.macro_block_cnt_ = 10;
    for (int64_t i = 0; i < partition.macro_block_cnt_; ++i) {
      partition.macro_blocks_[i] = ObRandom::rand(0, 100);
    }
    ASSERT_EQ(OB_SUCCESS, partition_image.add_partition(partition));
  }

  // disk full, retry 10 times
  partition.table_id_ += 1;
  for (int64_t i = 0; i < 10; ++i) {
    ASSERT_EQ(OB_CS_OUTOF_DISK_SPACE, partition_image.add_partition(partition));
  }

  // retry succeed, following will succeed
  for (int64_t i = 0; i < 10; ++i) {
    partition.table_id_ += 1;
    ASSERT_EQ(OB_SUCCESS, partition_image.add_partition(partition));
  }
}

TEST_F(TestSlogDiskError, recover_when_log_write_half)
{
  ObLogCursor last_log_cursor;
  ASSERT_EQ(OB_SUCCESS, prepare_log_file(last_log_cursor));

  // make last log not complete
  ASSERT_EQ(OB_SUCCESS, make_log_not_complete(last_log_cursor));

  // try replay not complete log file
  ASSERT_EQ(OB_SUCCESS, replay_log());

  // make last log ruined
  ASSERT_EQ(OB_SUCCESS, make_log_ruined(last_log_cursor));
  ASSERT_NE(OB_SUCCESS, replay_log());
}

}  // end namespace blocksstable
}  // end namespace oceanbase

int main(int argc, char** argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  // g++ -shared -o pwrite_preload.so pwrite_preload.c -fPIC
  // export LD_PRELOAD="./slog/pwrite_preload.so"
  // return RUN_ALL_TESTS();
  return 0;
}
