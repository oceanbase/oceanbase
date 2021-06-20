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
#include "share/redolog/ob_log_file_store.h"
#include "lib/file/file_directory_utils.h"

namespace oceanbase {
namespace common {

class TestLogDiskMgr : public ::testing::Test {
public:
  TestLogDiskMgr()
  {}
  virtual ~TestLogDiskMgr()
  {}
  virtual void SetUp();
  virtual void TearDown();

public:
  const static int64_t LOG_FILE_SIZE = 4 * 1024 * 1024;
  const static int DIO_WRITE_ALIGN_SIZE = 512;

  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(TestLogDiskMgr);
};

void TestLogDiskMgr::SetUp()
{
  system("rm -rf ./log_disk_mgr");
  FileDirectoryUtils::create_full_path("./log_disk_mgr/");
}

void TestLogDiskMgr::TearDown()
{
  system("rm -rf ./log_disk_mgr");
}

TEST_F(TestLogDiskMgr, simple)
{
  int ret = OB_SUCCESS;

  FileDirectoryUtils::create_full_path("./log_disk_mgr/disk1/");
  FileDirectoryUtils::create_full_path("./log_disk_mgr/disk2/");

  ObLogDiskManager disk_mgr;
  ret = disk_mgr.init("./log_disk_mgr/", TestLogDiskMgr::LOG_FILE_SIZE, clog::ObLogWritePoolType::SLOG_WRITE_POOL);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObLogFileDescriptor log_fd;
  ret = log_fd.init(&disk_mgr, ObLogFileDescriptor::WRITE_FLAG, 0);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);

  log_fd.reset();
  ret = log_fd.init(&disk_mgr, ObLogFileDescriptor::WRITE_FLAG, 2);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = log_fd.sync();
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(2, log_fd.count());

  log_fd.reset();
  ret = log_fd.init(&disk_mgr, ObLogFileDescriptor::WRITE_FLAG, 2);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = log_fd.sync();
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(2, log_fd.count());

  log_fd.reset();
  ret = log_fd.init(&disk_mgr, ObLogFileDescriptor::WRITE_FLAG, 3);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = log_fd.sync();
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(2, log_fd.count());
}

TEST_F(TestLogDiskMgr, single)
{
  int ret = OB_SUCCESS;

  ObLogDiskManager disk_mgr;
  ret = disk_mgr.init("./log_disk_mgr/", TestLogDiskMgr::LOG_FILE_SIZE, clog::ObLogWritePoolType::CLOG_WRITE_POOL);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObLogFileDescriptor log_fd;
  ret = log_fd.init(&disk_mgr, ObLogFileDescriptor::WRITE_FLAG, 1);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = log_fd.sync();
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(1, log_fd.count());
}

TEST_F(TestLogDiskMgr, over_limit)
{
  int ret = OB_SUCCESS;

  FileDirectoryUtils::create_full_path("./log_disk_mgr/disk1/");
  FileDirectoryUtils::create_full_path("./log_disk_mgr/disk2/");
  FileDirectoryUtils::create_full_path("./log_disk_mgr/disk3/");
  FileDirectoryUtils::create_full_path("./log_disk_mgr/disk4/");
  FileDirectoryUtils::create_full_path("./log_disk_mgr/disk5/");
  FileDirectoryUtils::create_full_path("./log_disk_mgr/disk6/");
  FileDirectoryUtils::create_full_path("./log_disk_mgr/disk7/");

  ObLogDiskManager disk_mgr;
  ret = disk_mgr.init("./log_disk_mgr/", TestLogDiskMgr::LOG_FILE_SIZE, clog::ObLogWritePoolType::CLOG_WRITE_POOL);
  ASSERT_EQ(OB_SIZE_OVERFLOW, ret);
}

TEST_F(TestLogDiskMgr, iterator)
{
  int ret = OB_SUCCESS;

  FileDirectoryUtils::create_full_path("./log_disk_mgr/disk1/");
  FileDirectoryUtils::create_full_path("./log_disk_mgr/disk2/");
  FileDirectoryUtils::create_full_path("./log_disk_mgr/disk3/");

  ObLogDiskManager disk_mgr;
  ret = disk_mgr.init("./log_disk_mgr/", TestLogDiskMgr::LOG_FILE_SIZE, clog::ObLogWritePoolType::CLOG_WRITE_POOL);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObLogDiskManager::ReadWriteDiskIterator iter = disk_mgr.begin();
  int64_t expect_disk_id = 0;
  for (; iter != disk_mgr.end(); ++iter) {
    ASSERT_EQ(expect_disk_id++, iter->get_disk_id());
    ASSERT_EQ(ObLogDiskState::OB_LDS_GOOD, iter->get_state());
  }
  ASSERT_EQ(3, expect_disk_id);

  ret = disk_mgr.set_bad_disk(1);
  ASSERT_EQ(OB_SUCCESS, ret);

  iter = disk_mgr.begin();
  ASSERT_EQ(0, iter->get_disk_id());
  ASSERT_EQ(ObLogDiskState::OB_LDS_GOOD, iter->get_state());
  ++iter;
  ASSERT_EQ(2, iter->get_disk_id());
  ASSERT_EQ(ObLogDiskState::OB_LDS_GOOD, iter->get_state());
  ++iter;
  ASSERT_TRUE(disk_mgr.end() == iter);
}

TEST_F(TestLogDiskMgr, last_bad_disk)
{
  int ret = OB_SUCCESS;

  FileDirectoryUtils::create_full_path("./log_disk_mgr/disk1/");
  FileDirectoryUtils::create_full_path("./log_disk_mgr/disk2/");
  FileDirectoryUtils::create_full_path("./log_disk_mgr/disk3/");

  ObLogDiskManager disk_mgr;
  ret = disk_mgr.init("./log_disk_mgr/", TestLogDiskMgr::LOG_FILE_SIZE, clog::ObLogWritePoolType::CLOG_WRITE_POOL);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObLogDiskManager::ReadWriteDiskIterator iter = disk_mgr.begin();
  int64_t expect_disk_id = 0;
  for (; iter != disk_mgr.end(); ++iter) {
    ASSERT_EQ(expect_disk_id++, iter->get_disk_id());
    ASSERT_EQ(ObLogDiskState::OB_LDS_GOOD, iter->get_state());
    ret = disk_mgr.set_bad_disk(iter->get_disk_id());
    ASSERT_EQ(OB_SUCCESS, ret);
  }
  ASSERT_EQ(3, expect_disk_id);

  // re-init disk manager, the last disk shouldn't be renamed
  disk_mgr.destroy();
  ret = disk_mgr.init("./log_disk_mgr/", TestLogDiskMgr::LOG_FILE_SIZE, clog::ObLogWritePoolType::CLOG_WRITE_POOL);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObLogFileDescriptor log_fd;
  ret = log_fd.init(&disk_mgr, ObLogFileDescriptor::WRITE_FLAG, 1);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = log_fd.sync();
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(1, log_fd.count());
}

TEST_F(TestLogDiskMgr, keep_last_disk)
{
  int ret = OB_SUCCESS;

  FileDirectoryUtils::create_full_path("./log_disk_mgr/disk1/");
  FileDirectoryUtils::create_full_path("./log_disk_mgr/disk2/");
  FileDirectoryUtils::create_full_path("./log_disk_mgr/disk3/");

  ObLogDiskManager disk_mgr;
  ret = disk_mgr.init("./log_disk_mgr/", TestLogDiskMgr::LOG_FILE_SIZE, clog::ObLogWritePoolType::CLOG_WRITE_POOL);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObLogDiskManager::ReadWriteDiskIterator iter = disk_mgr.begin();
  int64_t expect_disk_id = 0;
  for (; iter != disk_mgr.end(); ++iter) {
    ASSERT_EQ(expect_disk_id++, iter->get_disk_id());
    ASSERT_EQ(ObLogDiskState::OB_LDS_GOOD, iter->get_state());
    ret = disk_mgr.set_bad_disk(iter->get_disk_id());
    ASSERT_EQ(OB_SUCCESS, ret);
  }
  ASSERT_EQ(3, expect_disk_id);

  ObLogFileDescriptor log_fd;
  ret = log_fd.init(&disk_mgr, ObLogFileDescriptor::WRITE_FLAG, 1);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = log_fd.sync();
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(1, log_fd.count());

  uint32_t min_log_id = -1;
  uint32_t max_log_id = -1;
  ret = disk_mgr.get_file_id_range(min_log_id, max_log_id);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(1, min_log_id);
  ASSERT_EQ(1, max_log_id);
}

TEST_F(TestLogDiskMgr, restart_bad_disk)
{
  int ret = OB_SUCCESS;

  FileDirectoryUtils::create_full_path("./log_disk_mgr/disk1/");
  FileDirectoryUtils::create_full_path("./log_disk_mgr/disk2/");
  FileDirectoryUtils::create_full_path("./log_disk_mgr/disk3/");

  ObLogDiskManager disk_mgr;
  ret = disk_mgr.init("./log_disk_mgr/", TestLogDiskMgr::LOG_FILE_SIZE, clog::ObLogWritePoolType::CLOG_WRITE_POOL);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = disk_mgr.set_bad_disk(1);
  ASSERT_EQ(OB_SUCCESS, ret);

  disk_mgr.destroy();

  // restart
  ret = disk_mgr.init("./log_disk_mgr/", TestLogDiskMgr::LOG_FILE_SIZE, clog::ObLogWritePoolType::CLOG_WRITE_POOL);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObLogDiskManager::ReadWriteDiskIterator iter = disk_mgr.begin();
  int64_t disk_cnt = 0;
  for (; iter != disk_mgr.end(); ++iter) {
    ASSERT_EQ(ObLogDiskState::OB_LDS_GOOD, iter->get_state());
    disk_cnt++;
  }
  ASSERT_EQ(2, disk_cnt);
}

TEST_F(TestLogDiskMgr, set_bad_disk)
{
  int ret = OB_SUCCESS;

  system("rm -rf ./log_disk_disk3");
  FileDirectoryUtils::create_full_path("./log_disk_mgr/disk1/");
  FileDirectoryUtils::create_full_path("./log_disk_mgr/disk2/");
  FileDirectoryUtils::create_full_path("./log_disk_disk3");
  FileDirectoryUtils::symlink("../log_disk_disk3", "./log_disk_mgr/disk3");

  ObLogDiskManager disk_mgr;
  ret = disk_mgr.init("./log_disk_mgr/", TestLogDiskMgr::LOG_FILE_SIZE, clog::ObLogWritePoolType::CLOG_WRITE_POOL);
  ASSERT_EQ(OB_SUCCESS, ret);

  int64_t disk_cnt = 0;
  for (auto iter = disk_mgr.begin(); iter != disk_mgr.end(); ++iter) {
    ASSERT_EQ(ObLogDiskState::OB_LDS_GOOD, iter->get_state());
    disk_cnt++;
  }
  ASSERT_EQ(3, disk_cnt);

  // make disk 3 bad
  system("rm -rf ./log_disk_disk3");

  ObLogFileDescriptor log_fd;
  ret = log_fd.init(&disk_mgr, ObLogFileDescriptor::WRITE_FLAG, 1);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = log_fd.sync();
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(2, log_fd.count());

  disk_cnt = 0;
  for (auto iter = disk_mgr.begin(); iter != disk_mgr.end(); ++iter) {
    ASSERT_EQ(ObLogDiskState::OB_LDS_GOOD, iter->get_state());
    disk_cnt++;
  }
  ASSERT_EQ(2, disk_cnt);
}

TEST_F(TestLogDiskMgr, fd_sync)
{
  int ret = OB_SUCCESS;

  FileDirectoryUtils::create_full_path("./log_disk_mgr/disk1/");
  FileDirectoryUtils::create_full_path("./log_disk_mgr/disk2/");
  FileDirectoryUtils::create_full_path("./log_disk_mgr/disk3/");

  ObLogDiskManager disk_mgr;
  ret = disk_mgr.init("./log_disk_mgr/", TestLogDiskMgr::LOG_FILE_SIZE, clog::ObLogWritePoolType::CLOG_WRITE_POOL);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObLogFileDescriptor log_fd;
  ret = log_fd.init(&disk_mgr, ObLogFileDescriptor::WRITE_FLAG, 1);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = log_fd.sync();
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(3, log_fd.count());

  ret = disk_mgr.set_bad_disk(0);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = log_fd.sync();
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(2, log_fd.count());

  ret = disk_mgr.set_bad_disk(1);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = log_fd.sync();
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(1, log_fd.count());

  ret = disk_mgr.set_bad_disk(2);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = log_fd.sync();
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(1, log_fd.count());
}

TEST_F(TestLogDiskMgr, startup_all_empty)
{
  int ret = OB_SUCCESS;

  ret = FileDirectoryUtils::create_full_path("./log_disk_mgr/disk1/");
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = FileDirectoryUtils::create_full_path("./log_disk_mgr/disk2/");
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = FileDirectoryUtils::create_full_path("./log_disk_mgr/disk3/");
  ASSERT_EQ(OB_SUCCESS, ret);

  ObLogDiskManager disk_mgr;
  ret = disk_mgr.init("./log_disk_mgr/", TestLogDiskMgr::LOG_FILE_SIZE, clog::ObLogWritePoolType::CLOG_WRITE_POOL);
  ASSERT_EQ(OB_SUCCESS, ret);
}

TEST_F(TestLogDiskMgr, startup_incomplete)
{
  int ret = OB_SUCCESS;
  const int OPEN_FLAG = O_WRONLY | O_DIRECT | O_SYNC | O_CREAT;
  const int OPEN_MODE = S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH;
  int fd = -1;
  bool is_exist = false;

  // create 3 disks:
  // disk1 contains all log files
  // disk2 is discontinuous
  // disk3 is empty
  ret = FileDirectoryUtils::create_full_path("./log_disk_mgr/disk1/");
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = FileDirectoryUtils::create_full_path("./log_disk_mgr/disk2/");
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = FileDirectoryUtils::create_full_path("./log_disk_mgr/disk3/");
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = FileDirectoryUtils::open("./log_disk_mgr/disk1/1", OPEN_FLAG, OPEN_MODE, fd);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = FileDirectoryUtils::close(fd);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = FileDirectoryUtils::open("./log_disk_mgr/disk1/2", OPEN_FLAG, OPEN_MODE, fd);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = FileDirectoryUtils::close(fd);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = FileDirectoryUtils::open("./log_disk_mgr/disk1/3", OPEN_FLAG, OPEN_MODE, fd);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = FileDirectoryUtils::close(fd);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = FileDirectoryUtils::open("./log_disk_mgr/disk2/1", OPEN_FLAG, OPEN_MODE, fd);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = FileDirectoryUtils::close(fd);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = FileDirectoryUtils::open("./log_disk_mgr/disk2/3", OPEN_FLAG, OPEN_MODE, fd);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = FileDirectoryUtils::close(fd);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObLogDiskManager disk_mgr;
  ret = disk_mgr.init("./log_disk_mgr/", TestLogDiskMgr::LOG_FILE_SIZE, clog::ObLogWritePoolType::CLOG_WRITE_POOL);
  ASSERT_EQ(OB_SUCCESS, ret);

  // disk2, disk3 should all be purged
  ret = FileDirectoryUtils::is_exists("./log_disk_mgr/disk2/1", is_exist);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_FALSE(is_exist);
  ret = FileDirectoryUtils::is_exists("./log_disk_mgr/disk2/3", is_exist);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_FALSE(is_exist);
  ret = FileDirectoryUtils::is_exists("./log_disk_mgr/disk3/3", is_exist);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_FALSE(is_exist);
}

TEST_F(TestLogDiskMgr, startup_clone)
{
  int ret = OB_SUCCESS;
  const int OPEN_FLAG = O_WRONLY | O_DIRECT | O_SYNC | O_CREAT;
  const int OPEN_MODE = S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH;
  const int OPEN_FLAG_READ = O_RDONLY | O_DIRECT;
  int fd = -1;
  const int64_t buf_size = 4096;
  char* buffer = reinterpret_cast<char*>(ob_malloc_align(CLOG_DIO_ALIGN_SIZE, buf_size, ObModIds::OB_LOG_DISK_MANAGER));
  char* read_buf1 =
      reinterpret_cast<char*>(ob_malloc_align(CLOG_DIO_ALIGN_SIZE, buf_size, ObModIds::OB_LOG_DISK_MANAGER));
  char* read_buf2 =
      reinterpret_cast<char*>(ob_malloc_align(CLOG_DIO_ALIGN_SIZE, buf_size, ObModIds::OB_LOG_DISK_MANAGER));
  int64_t p_ret = 0;

  ret = FileDirectoryUtils::create_full_path("./log_disk_mgr/disk1/");
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = FileDirectoryUtils::create_full_path("./log_disk_mgr/disk2/");
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = FileDirectoryUtils::open("./log_disk_mgr/disk1/1", OPEN_FLAG, OPEN_MODE, fd);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = FileDirectoryUtils::close(fd);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = FileDirectoryUtils::open("./log_disk_mgr/disk1/2", OPEN_FLAG, OPEN_MODE, fd);
  ASSERT_EQ(OB_SUCCESS, ret);
  strcpy(buffer, "this is startup_clone disk 1");
  p_ret = ob_pwrite(fd, buffer, buf_size, 0);
  ASSERT_EQ(buf_size, p_ret);
  ret = FileDirectoryUtils::close(fd);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = FileDirectoryUtils::open("./log_disk_mgr/disk2/1", OPEN_FLAG, OPEN_MODE, fd);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = FileDirectoryUtils::close(fd);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = FileDirectoryUtils::open("./log_disk_mgr/disk2/2", OPEN_FLAG, OPEN_MODE, fd);
  strcpy(buffer, "That are startup_clone disk 2");
  p_ret = ob_pwrite(fd, buffer, buf_size, 0);
  ASSERT_EQ(buf_size, p_ret);
  ret = FileDirectoryUtils::close(fd);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObLogDiskManager disk_mgr;
  ret = disk_mgr.init("./log_disk_mgr/", TestLogDiskMgr::LOG_FILE_SIZE, clog::ObLogWritePoolType::CLOG_WRITE_POOL);
  ASSERT_EQ(OB_SUCCESS, ret);

  // disk2 and disk3
  ret = FileDirectoryUtils::open("./log_disk_mgr/disk1/2", OPEN_FLAG_READ, OPEN_MODE, fd);
  ASSERT_EQ(OB_SUCCESS, ret);
  p_ret = ob_pread(fd, read_buf1, buf_size, 0);
  ASSERT_EQ(buf_size, p_ret);
  ret = FileDirectoryUtils::close(fd);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = FileDirectoryUtils::open("./log_disk_mgr/disk2/2", OPEN_FLAG_READ, OPEN_MODE, fd);
  ASSERT_EQ(OB_SUCCESS, ret);
  p_ret = ob_pread(fd, read_buf2, buf_size, 0);
  ASSERT_EQ(buf_size, p_ret);
  ret = FileDirectoryUtils::close(fd);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = STRCMP(read_buf1, read_buf2);
  ASSERT_EQ(0, ret);

  ob_free_align(buffer);
  ob_free_align(read_buf1);
  ob_free_align(read_buf2);
}

TEST_F(TestLogDiskMgr, startup_discontinuous)
{
  int ret = OB_SUCCESS;
  int fd = -1;
  const int OPEN_FLAG = O_WRONLY | O_DIRECT | O_SYNC | O_CREAT;
  const int OPEN_MODE = S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH;

  ret = FileDirectoryUtils::create_full_path("./log_disk_mgr/disk1/");
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = FileDirectoryUtils::open("./log_disk_mgr/disk1/1", OPEN_FLAG, OPEN_MODE, fd);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = FileDirectoryUtils::close(fd);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = FileDirectoryUtils::open("./log_disk_mgr/disk1/4", OPEN_FLAG, OPEN_MODE, fd);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = FileDirectoryUtils::close(fd);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = FileDirectoryUtils::open("./log_disk_mgr/disk1/5", OPEN_FLAG, OPEN_MODE, fd);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = FileDirectoryUtils::close(fd);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObLogDiskManager disk_mgr;
  ret = disk_mgr.init("./log_disk_mgr/", TestLogDiskMgr::LOG_FILE_SIZE, clog::ObLogWritePoolType::CLOG_WRITE_POOL);
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, ret);
}

TEST_F(TestLogDiskMgr, startup_single_disk)
{
  int ret = OB_SUCCESS;
  int fd = -1;
  const int OPEN_FLAG = O_WRONLY | O_DIRECT | O_SYNC | O_CREAT;
  const int OPEN_MODE = S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH;

  ret = FileDirectoryUtils::create_full_path("./log_disk_mgr/disk1/");
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = FileDirectoryUtils::open("./log_disk_mgr/disk1/1", OPEN_FLAG, OPEN_MODE, fd);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = FileDirectoryUtils::close(fd);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = FileDirectoryUtils::open("./log_disk_mgr/disk1/2", OPEN_FLAG, OPEN_MODE, fd);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = FileDirectoryUtils::close(fd);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObLogDiskManager disk_mgr;
  ret = disk_mgr.init("./log_disk_mgr/", TestLogDiskMgr::LOG_FILE_SIZE, clog::ObLogWritePoolType::CLOG_WRITE_POOL);
  ASSERT_EQ(OB_SUCCESS, ret);
}

TEST_F(TestLogDiskMgr, startup_clear_tmp)
{
  int ret = OB_SUCCESS;
  int fd = -1;
  bool is_exist;
  const int OPEN_FLAG = O_WRONLY | O_DIRECT | O_SYNC | O_CREAT;
  const int OPEN_MODE = S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH;

  ret = FileDirectoryUtils::create_full_path("./log_disk_mgr/disk1/");
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = FileDirectoryUtils::open("./log_disk_mgr/disk1/1", OPEN_FLAG, OPEN_MODE, fd);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = FileDirectoryUtils::close(fd);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = FileDirectoryUtils::open("./log_disk_mgr/disk1/2", OPEN_FLAG, OPEN_MODE, fd);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = FileDirectoryUtils::close(fd);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = FileDirectoryUtils::open("./log_disk_mgr/disk1/2.tmp", OPEN_FLAG, OPEN_MODE, fd);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = FileDirectoryUtils::close(fd);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = FileDirectoryUtils::open("./log_disk_mgr/disk1/3.tmp", OPEN_FLAG, OPEN_MODE, fd);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = FileDirectoryUtils::close(fd);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObLogDiskManager disk_mgr;
  ret = disk_mgr.init("./log_disk_mgr/", TestLogDiskMgr::LOG_FILE_SIZE, clog::ObLogWritePoolType::CLOG_WRITE_POOL);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = FileDirectoryUtils::is_exists("./log_disk_mgr/disk1/1", is_exist);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(is_exist);
  ret = FileDirectoryUtils::is_exists("./log_disk_mgr/disk1/2", is_exist);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(is_exist);
  ret = FileDirectoryUtils::is_exists("./log_disk_mgr/disk1/2.tmp", is_exist);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_FALSE(is_exist);
  ret = FileDirectoryUtils::is_exists("./log_disk_mgr/disk1/3.tmp", is_exist);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_FALSE(is_exist);
}

TEST_F(TestLogDiskMgr, disk_load_only_catchup)
{
  int ret = OB_SUCCESS;
  ret = FileDirectoryUtils::create_full_path("./log_disk_mgr/disk1/");
  ASSERT_EQ(OB_SUCCESS, ret);

  ObLogDiskManager disk_mgr;
  ret = disk_mgr.init("./log_disk_mgr/", TestLogDiskMgr::LOG_FILE_SIZE, clog::ObLogWritePoolType::CLOG_WRITE_POOL);
  ASSERT_EQ(OB_SUCCESS, ret);
  sleep(2);

  ret = FileDirectoryUtils::create_full_path("./log_disk_mgr/disk2/");
  ASSERT_EQ(OB_SUCCESS, ret);
  sleep(2);
  ObLogFileDescriptor write_fd;
  ret = write_fd.init(&disk_mgr, ObLogFileDescriptor::WRITE_FLAG, 1);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = write_fd.sync(0);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(2, write_fd.count());
  sleep(3);

  ObLogFileDescriptor read_fd;
  ret = read_fd.init(&disk_mgr, ObLogFileDescriptor::READ_FLAG, 1);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = read_fd.sync();
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(2, read_fd.count());

  bool exist = false;
  ret = FileDirectoryUtils::is_exists("./log_disk_mgr/disk1/1", exist);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(exist);
  ret = FileDirectoryUtils::is_exists("./log_disk_mgr/disk2/1", exist);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(exist);
  ret = FileDirectoryUtils::is_exists("./log_disk_mgr/disk1/LOG_RESTORE", exist);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_FALSE(exist);
  ret = FileDirectoryUtils::is_exists("./log_disk_mgr/disk2/LOG_RESTORE", exist);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_FALSE(exist);
}

TEST_F(TestLogDiskMgr, disk_load_content)
{
  int ret = OB_SUCCESS;
  const int OPEN_FLAG = O_WRONLY | O_DIRECT | O_SYNC | O_CREAT;
  const int OPEN_MODE = S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH;
  const int OPEN_FLAG_READ = O_RDONLY | O_DIRECT;
  const char* file1_content = "this is disk_load_content file 1";
  const char* file2_content = "those are disk_load_content 2 files";
  int fd = -1;
  const int64_t buf_size = CLOG_DIO_ALIGN_SIZE;
  char* buffer = reinterpret_cast<char*>(ob_malloc_align(CLOG_DIO_ALIGN_SIZE, buf_size, ObModIds::OB_LOG_DISK_MANAGER));
  char* read_buf =
      reinterpret_cast<char*>(ob_malloc_align(CLOG_DIO_ALIGN_SIZE, buf_size, ObModIds::OB_LOG_DISK_MANAGER));
  int64_t p_ret = 0;

  // create disk 1 with 2 log files
  ret = FileDirectoryUtils::create_full_path("./log_disk_mgr/disk1/");
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = FileDirectoryUtils::open("./log_disk_mgr/disk1/1", OPEN_FLAG, OPEN_MODE, fd);
  ASSERT_EQ(OB_SUCCESS, ret);
  strcpy(buffer, file1_content);
  p_ret = ob_pwrite(fd, buffer, buf_size, 0);
  ASSERT_EQ(buf_size, p_ret);
  ret = FileDirectoryUtils::close(fd);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = FileDirectoryUtils::open("./log_disk_mgr/disk1/2", OPEN_FLAG, OPEN_MODE, fd);
  ASSERT_EQ(OB_SUCCESS, ret);
  strcpy(buffer, file2_content);
  p_ret = ob_pwrite(fd, buffer, buf_size, 0);
  ASSERT_EQ(buf_size, p_ret);
  ret = FileDirectoryUtils::close(fd);
  ASSERT_EQ(OB_SUCCESS, ret);

  // start disk manager
  ObLogDiskManager disk_mgr;
  ret = disk_mgr.init("./log_disk_mgr/", TestLogDiskMgr::LOG_FILE_SIZE, clog::ObLogWritePoolType::CLOG_WRITE_POOL);
  ASSERT_EQ(OB_SUCCESS, ret);
  sleep(2);

  // load disk2 and sync write position to file 2, offset CLOG_DIO_ALIGN_SIZE
  ret = FileDirectoryUtils::create_full_path("./log_disk_mgr/disk2/");
  ASSERT_EQ(OB_SUCCESS, ret);
  sleep(5);
  ObLogFileDescriptor write_fd;
  ret = write_fd.init(&disk_mgr, ObLogFileDescriptor::WRITE_FLAG, 2);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = write_fd.sync(CLOG_DIO_ALIGN_SIZE);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(2, write_fd.count());

  // write content at offset CLOG_DIO_ALIGN_SIZE, ensure catch up won't overwrite it
  ret = FileDirectoryUtils::open("./log_disk_mgr/disk2/2", O_WRONLY | O_DIRECT | O_SYNC, OPEN_MODE, fd);
  ASSERT_EQ(OB_SUCCESS, ret);
  strcpy(buffer, file2_content);
  p_ret = ob_pwrite(fd, buffer, buf_size, CLOG_DIO_ALIGN_SIZE);
  ASSERT_EQ(buf_size, p_ret);
  ret = FileDirectoryUtils::close(fd);
  ASSERT_EQ(OB_SUCCESS, ret);
  sleep(5);

  // check disk 2 is GOOD
  ObLogFileDescriptor read_fd;
  ret = read_fd.init(&disk_mgr, ObLogFileDescriptor::READ_FLAG, 2);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = read_fd.sync();
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(2, read_fd.count());

  // check log sync complete
  bool exist = true;
  ret = FileDirectoryUtils::is_exists("./log_disk_mgr/disk1/LOG_RESTORE", exist);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_FALSE(exist);
  ret = FileDirectoryUtils::is_exists("./log_disk_mgr/disk2/LOG_RESTORE", exist);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_FALSE(exist);

  // check disk2 log file 1 and 2
  ret = FileDirectoryUtils::open("./log_disk_mgr/disk2/1", OPEN_FLAG_READ, OPEN_MODE, fd);
  ASSERT_EQ(OB_SUCCESS, ret);
  p_ret = ob_pread(fd, read_buf, buf_size, 0);
  ASSERT_EQ(buf_size, p_ret);
  ASSERT_EQ(0, STRCMP(read_buf, file1_content));
  ret = FileDirectoryUtils::close(fd);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = FileDirectoryUtils::open("./log_disk_mgr/disk2/2", OPEN_FLAG_READ, OPEN_MODE, fd);
  ASSERT_EQ(OB_SUCCESS, ret);
  p_ret = ob_pread(fd, read_buf, buf_size, 0);
  ASSERT_EQ(buf_size, p_ret);
  ASSERT_EQ(0, STRCMP(read_buf, file2_content));
  p_ret = ob_pread(fd, read_buf, buf_size, CLOG_DIO_ALIGN_SIZE);
  ASSERT_EQ(buf_size, p_ret);
  ASSERT_EQ(0, STRCMP(read_buf, file2_content));
  ret = FileDirectoryUtils::close(fd);
  ASSERT_EQ(OB_SUCCESS, ret);

  ob_free_align(buffer);
  ob_free_align(read_buf);
}

TEST_F(TestLogDiskMgr, disk_info_sync)
{
  ObLogDiskInfo disk;
  ASSERT_EQ(OB_LDS_INVALID, disk.get_state());
  disk.set_state(OB_LDS_NEW);
  ASSERT_EQ(OB_LDS_NEW, disk.get_state());
  disk.restore_start(3, 109);
  ASSERT_EQ(-1, disk.get_restore_start_file_id());
  ASSERT_EQ(-1, disk.get_restore_start_offset());

  disk.set_state(OB_LDS_INVALID, OB_LDS_RESTORE);
  ASSERT_EQ(OB_LDS_NEW, disk.get_state());
  disk.set_state(OB_LDS_NEW, OB_LDS_RESTORE);
  ASSERT_EQ(OB_LDS_RESTORE, disk.get_state());

  disk.restore_start(3, 109);
  ASSERT_EQ(3, disk.get_restore_start_file_id());
  ASSERT_EQ(109, disk.get_restore_start_offset());

  disk.restore_start(1, 99);
  ASSERT_EQ(3, disk.get_restore_start_file_id());
  ASSERT_EQ(109, disk.get_restore_start_offset());

  disk.restore_start(5, 199);
  ASSERT_EQ(3, disk.get_restore_start_file_id());
  ASSERT_EQ(109, disk.get_restore_start_offset());
}

TEST_F(TestLogDiskMgr, restore_serialization)
{
  int ret = OB_SUCCESS;
  const int buf_size = DIO_ALIGN_SIZE;
  int64_t pos = 0;
  char* buf = reinterpret_cast<char*>(ob_malloc_align(DIO_ALIGN_SIZE, buf_size, ObModIds::OB_LOG_DISK_MANAGER));

  ObLogDiskManager::LogRestoreProgress progress;
  progress.copy_complete_ = 1;
  progress.catchup_complete_ = 1;
  progress.catchup_file_id_ = 100;
  progress.catchup_offset_ = 3900;
  progress.copy_start_file_id_ = 93;
  progress.copied_file_id_ = 87;
  ret = progress.serialize(buf, buf_size, pos);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObLogDiskManager::LogRestoreProgress new_progress;
  pos = 0;
  ret = new_progress.deserialize(buf, buf_size, pos);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(progress.copy_complete_, new_progress.copy_complete_);
  ASSERT_EQ(progress.catchup_complete_, new_progress.catchup_complete_);
  ASSERT_EQ(progress.catchup_file_id_, new_progress.catchup_file_id_);
  ASSERT_EQ(progress.catchup_offset_, new_progress.catchup_offset_);
  ASSERT_EQ(progress.copy_start_file_id_, new_progress.copy_start_file_id_);
  ASSERT_EQ(progress.copied_file_id_, new_progress.copied_file_id_);

  ob_free_align(buf);
}

TEST_F(TestLogDiskMgr, restore_normal)
{
  int ret = OB_SUCCESS;
  const int OPEN_FLAG = O_WRONLY | O_DIRECT | O_SYNC | O_CREAT;
  const int OPEN_MODE = S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH;
  const int OPEN_FLAG_READ = O_RDONLY | O_DIRECT;
  int fd = -1;

  // disk1 contains 3 log files
  ret = FileDirectoryUtils::create_full_path("./log_disk_mgr/disk1/");
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = FileDirectoryUtils::open("./log_disk_mgr/disk1/1", OPEN_FLAG, OPEN_MODE, fd);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = FileDirectoryUtils::close(fd);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = FileDirectoryUtils::open("./log_disk_mgr/disk1/2", OPEN_FLAG, OPEN_MODE, fd);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = FileDirectoryUtils::close(fd);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = FileDirectoryUtils::open("./log_disk_mgr/disk1/3", OPEN_FLAG, OPEN_MODE, fd);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = FileDirectoryUtils::close(fd);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObLogDiskManager disk_mgr;
  ret = disk_mgr.init("./log_disk_mgr/", TestLogDiskMgr::LOG_FILE_SIZE, clog::ObLogWritePoolType::CLOG_WRITE_POOL);
  ASSERT_EQ(OB_SUCCESS, ret);

  // add new disk2
  ret = FileDirectoryUtils::create_full_path("./log_disk_mgr/disk2/");
  ASSERT_EQ(OB_SUCCESS, ret);
  sleep(5);

  // check LOG_RESTORE content
  bool exist = true;
  ret = FileDirectoryUtils::is_exists("./log_disk_mgr/disk1/LOG_RESTORE", exist);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_FALSE(exist);
  ret = FileDirectoryUtils::is_exists("./log_disk_mgr/disk2/LOG_RESTORE", exist);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(exist);

  const int64_t buf_size = DIO_ALIGN_SIZE;
  char* read_buf = reinterpret_cast<char*>(ob_malloc_align(DIO_ALIGN_SIZE, buf_size, ObModIds::OB_LOG_DISK_MANAGER));
  int64_t p_ret = 0;
  int64_t pos = 0;

  ret = FileDirectoryUtils::open("./log_disk_mgr/disk2/LOG_RESTORE", OPEN_FLAG_READ, OPEN_MODE, fd);
  ASSERT_EQ(OB_SUCCESS, ret);
  p_ret = ob_pread(fd, read_buf, buf_size, 0);
  ASSERT_EQ(buf_size, p_ret);

  ObLogDiskManager::LogRestoreProgress progress;
  ret = progress.deserialize(read_buf, buf_size, pos);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(sizeof(ObLogDiskManager::LogRestoreProgress), progress.length_);
  ASSERT_EQ(0, progress.catchup_complete_);
  ASSERT_EQ(1, progress.copy_complete_);
  ASSERT_EQ(-1, progress.catchup_file_id_);
  ASSERT_EQ(-1, progress.catchup_offset_);
  ASSERT_EQ(2, progress.copy_start_file_id_);
  ASSERT_EQ(1, progress.copied_file_id_);
  ret = FileDirectoryUtils::close(fd);
  ASSERT_EQ(OB_SUCCESS, ret);

  // sync write position at file 4
  ObLogFileDescriptor write_fd;
  ret = write_fd.init(&disk_mgr, ObLogFileDescriptor::WRITE_FLAG, 4);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = write_fd.sync(CLOG_DIO_ALIGN_SIZE);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(2, write_fd.count());
  sleep(5);

  // Check LOG_RESTORE again
  ret = FileDirectoryUtils::is_exists("./log_disk_mgr/disk2/LOG_RESTORE", exist);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_FALSE(exist);
  ret = FileDirectoryUtils::is_exists("./log_disk_mgr/disk2/1", exist);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(exist);
  ret = FileDirectoryUtils::is_exists("./log_disk_mgr/disk2/2", exist);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(exist);
  ret = FileDirectoryUtils::is_exists("./log_disk_mgr/disk2/3", exist);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(exist);
  ret = FileDirectoryUtils::is_exists("./log_disk_mgr/disk2/4", exist);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(exist);

  ob_free_align(read_buf);
}

TEST_F(TestLogDiskMgr, restore_restart)
{
  int ret = OB_SUCCESS;
  const int OPEN_FLAG = O_WRONLY | O_DIRECT | O_SYNC | O_CREAT;
  const int OPEN_MODE = S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH;
  const int OPEN_FLAG_READ = O_RDONLY | O_DIRECT;
  int fd = -1;

  // disk1 contains 3 log files
  ret = FileDirectoryUtils::create_full_path("./log_disk_mgr/disk1/");
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = FileDirectoryUtils::open("./log_disk_mgr/disk1/1", OPEN_FLAG, OPEN_MODE, fd);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = FileDirectoryUtils::close(fd);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = FileDirectoryUtils::open("./log_disk_mgr/disk1/2", OPEN_FLAG, OPEN_MODE, fd);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = FileDirectoryUtils::close(fd);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = FileDirectoryUtils::open("./log_disk_mgr/disk1/3", OPEN_FLAG, OPEN_MODE, fd);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = FileDirectoryUtils::close(fd);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObLogDiskManager disk_mgr;
  ret = disk_mgr.init("./log_disk_mgr/", TestLogDiskMgr::LOG_FILE_SIZE, clog::ObLogWritePoolType::CLOG_WRITE_POOL);
  ASSERT_EQ(OB_SUCCESS, ret);

  // add new disk2
  ret = FileDirectoryUtils::create_full_path("./log_disk_mgr/disk2/");
  ASSERT_EQ(OB_SUCCESS, ret);
  sleep(5);

  // restart disk_mgr
  disk_mgr.destroy();
  ret = disk_mgr.init("./log_disk_mgr/", TestLogDiskMgr::LOG_FILE_SIZE, clog::ObLogWritePoolType::CLOG_WRITE_POOL);
  ASSERT_EQ(OB_SUCCESS, ret);

  // check LOG_RESTORE content
  bool exist = true;
  ret = FileDirectoryUtils::is_exists("./log_disk_mgr/disk1/LOG_RESTORE", exist);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_FALSE(exist);
  ret = FileDirectoryUtils::is_exists("./log_disk_mgr/disk2/LOG_RESTORE", exist);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(exist);

  const int64_t buf_size = DIO_ALIGN_SIZE;
  char* read_buf = reinterpret_cast<char*>(ob_malloc_align(DIO_ALIGN_SIZE, buf_size, ObModIds::OB_LOG_DISK_MANAGER));
  int64_t p_ret = 0;
  int64_t pos = 0;

  ret = FileDirectoryUtils::open("./log_disk_mgr/disk2/LOG_RESTORE", OPEN_FLAG_READ, OPEN_MODE, fd);
  ASSERT_EQ(OB_SUCCESS, ret);
  p_ret = ob_pread(fd, read_buf, buf_size, 0);
  ASSERT_EQ(buf_size, p_ret);

  ObLogDiskManager::LogRestoreProgress progress;
  ret = progress.deserialize(read_buf, buf_size, pos);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(sizeof(ObLogDiskManager::LogRestoreProgress), progress.length_);
  ASSERT_EQ(0, progress.catchup_complete_);
  ASSERT_EQ(1, progress.copy_complete_);
  ASSERT_EQ(-1, progress.catchup_file_id_);
  ASSERT_EQ(-1, progress.catchup_offset_);
  ASSERT_EQ(2, progress.copy_start_file_id_);
  ASSERT_EQ(1, progress.copied_file_id_);
  ret = FileDirectoryUtils::close(fd);
  ASSERT_EQ(OB_SUCCESS, ret);

  // restart disk_mgr
  disk_mgr.destroy();
  ret = disk_mgr.init("./log_disk_mgr/", TestLogDiskMgr::LOG_FILE_SIZE, clog::ObLogWritePoolType::CLOG_WRITE_POOL);
  ASSERT_EQ(OB_SUCCESS, ret);

  // sync write position at file 4
  ObLogFileDescriptor write_fd;
  ret = write_fd.init(&disk_mgr, ObLogFileDescriptor::WRITE_FLAG, 4);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = write_fd.sync(CLOG_DIO_ALIGN_SIZE);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(2, write_fd.count());
  sleep(5);

  // Check LOG_RESTORE again
  ret = FileDirectoryUtils::is_exists("./log_disk_mgr/disk2/LOG_RESTORE", exist);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_FALSE(exist);
  ret = FileDirectoryUtils::is_exists("./log_disk_mgr/disk2/1", exist);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(exist);
  ret = FileDirectoryUtils::is_exists("./log_disk_mgr/disk2/2", exist);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(exist);
  ret = FileDirectoryUtils::is_exists("./log_disk_mgr/disk2/3", exist);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(exist);
  ret = FileDirectoryUtils::is_exists("./log_disk_mgr/disk2/4", exist);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(exist);

  ob_free_align(read_buf);
}

/// test restore only 1 file which start offset is 0
TEST_F(TestLogDiskMgr, restore_zero_offset)
{
  int ret = OB_SUCCESS;
  ret = FileDirectoryUtils::create_full_path("./log_disk_mgr/disk1/");
  ASSERT_EQ(OB_SUCCESS, ret);

  ObLogDiskManager disk_mgr;
  ret = disk_mgr.init("./log_disk_mgr/", TestLogDiskMgr::LOG_FILE_SIZE, clog::ObLogWritePoolType::CLOG_WRITE_POOL);
  ASSERT_EQ(OB_SUCCESS, ret);
  sleep(2);

  ObLogFileDescriptor write_fd;
  ret = write_fd.init(&disk_mgr, ObLogFileDescriptor::WRITE_FLAG, 1);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = write_fd.sync(0);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(1, write_fd.count());
  sleep(3);

  ret = FileDirectoryUtils::create_full_path("./log_disk_mgr/disk2/");
  ASSERT_EQ(OB_SUCCESS, ret);
  sleep(2);

  ret = write_fd.sync(0);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(2, write_fd.count());
  sleep(2);

  bool exist = false;
  ret = FileDirectoryUtils::is_exists("./log_disk_mgr/disk1/1", exist);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(exist);
  ret = FileDirectoryUtils::is_exists("./log_disk_mgr/disk2/1", exist);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(exist);
  ret = FileDirectoryUtils::is_exists("./log_disk_mgr/disk1/LOG_RESTORE", exist);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_FALSE(exist);
  ret = FileDirectoryUtils::is_exists("./log_disk_mgr/disk2/LOG_RESTORE", exist);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_FALSE(exist);
}
}  // namespace common
}  // namespace oceanbase

int main(int argc, char** argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
