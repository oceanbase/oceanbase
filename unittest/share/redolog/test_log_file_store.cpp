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
#include "share/redolog/ob_log_local_store.h"
#include "share/redolog/ob_log_file_reader.h"
#include "lib/file/file_directory_utils.h"
#include "lib/file/ob_file.h"
#include "lib/container/ob_se_array.h"

namespace oceanbase
{
using namespace share;
namespace common
{

class TestLogFileStore: public ::testing::Test
{
public:
  TestLogFileStore()
  {
  }
  virtual ~TestLogFileStore()
  {
  }
  virtual void SetUp();
  virtual void TearDown();

public:
  const static int64_t LOG_FILE_SIZE = 4 * 1024 * 1024;
  const static int DIO_WRITE_ALIGN_SIZE = 512;

  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(TestLogFileStore);
};

void TestLogFileStore::SetUp()
{
  system("rm -rf ./log_file_test");
  FileDirectoryUtils::create_full_path("./log_file_test/");
  OB_LOG_FILE_READER.init();
#ifdef ERRSIM
  TP_SET_EVENT(EventTable::EN_IO_GETEVENTS, OB_SUCCESS, 0, 1);
  TP_SET_EVENT(EventTable::EN_IO_SUBMIT, OB_SUCCESS, 0, 1);
  usleep(100 * 1000);
#endif
}

void TestLogFileStore::TearDown()
{
  system("rm -rf ./log_file_test");
  OB_SLOG_DISK_MGR.destroy();
  OB_CLOG_DISK_MGR.destroy();
  OB_ILOG_DISK_MGR.destroy();
  OB_LOG_FILE_READER.destroy();
#ifdef ERRSIM
  TP_SET_EVENT(EventTable::EN_IO_GETEVENTS, OB_SUCCESS, 0, 1);
  TP_SET_EVENT(EventTable::EN_IO_SUBMIT, OB_SUCCESS, 0, 1);
  usleep(100 * 1000);
#endif
}

TEST_F(TestLogFileStore, simple)
{
  int ret = OB_SUCCESS;
  FileDirectoryUtils::create_full_path("./log_file_test/disk1/");
  FileDirectoryUtils::create_full_path("./log_file_test/disk2/");

  ObLogLocalStore file_store;
  file_store.init("./log_file_test/", TestLogFileStore::LOG_FILE_SIZE,
      clog::ObLogWritePoolType::CLOG_WRITE_POOL);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = file_store.open(1);
  ASSERT_EQ(OB_SUCCESS, ret);

  char *buf = nullptr;
  ret = posix_memalign((void **) &buf, TestLogFileStore::DIO_WRITE_ALIGN_SIZE,
      TestLogFileStore::DIO_WRITE_ALIGN_SIZE);
  ASSERT_EQ(0, ret);
  ASSERT_TRUE(NULL != buf);
  strcpy(buf, "this is simple test");

  ret = file_store.write((void *) buf, TestLogFileStore::DIO_WRITE_ALIGN_SIZE, 0);
  ASSERT_EQ(OB_SUCCESS, ret);
  free(buf);

  ret = file_store.fsync();
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = file_store.close();
  ASSERT_EQ(OB_SUCCESS, ret);
}

TEST_F(TestLogFileStore, rd_wr)
{
  int ret = OB_SUCCESS;
  int64_t read_size = 0;
  int64_t count = TestLogFileStore::DIO_WRITE_ALIGN_SIZE;
  FileDirectoryUtils::create_full_path("./log_file_test/disk1/");
  FileDirectoryUtils::create_full_path("./log_file_test/disk2/");

  ObLogLocalStore file_store;
  file_store.init("./log_file_test/", TestLogFileStore::LOG_FILE_SIZE,
      clog::ObLogWritePoolType::SLOG_WRITE_POOL);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = file_store.open(1);
  ASSERT_EQ(OB_SUCCESS, ret);

  char *buf;
  ret = posix_memalign((void **) &buf, TestLogFileStore::DIO_WRITE_ALIGN_SIZE, count);
  ASSERT_EQ(0, ret);
  ASSERT_TRUE(NULL != buf);
  strcpy(buf, "this is rd_wr test");
  size_t len = strlen(buf);
  ASSERT_NE(0, len);

  ret = file_store.write((void *) buf, count, 0);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = file_store.close();
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = file_store.open(1);
  ASSERT_EQ(OB_SUCCESS, ret);

  char *rd_buf;
  ret = posix_memalign((void **) &rd_buf, TestLogFileStore::DIO_WRITE_ALIGN_SIZE, count);

  ret = file_store.read((void *) rd_buf, count, 0, read_size);
  OB_LOG(INFO, "buf value", K(buf), K(rd_buf), KP(buf), KP(rd_buf), K(len));
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(count, read_size);
  ASSERT_EQ(len, strlen(rd_buf));
  ASSERT_EQ(0, strcmp(buf, rd_buf));

  free(buf);
  free(rd_buf);

  ret = file_store.close();
  ASSERT_EQ(OB_SUCCESS, ret);
}

TEST_F(TestLogFileStore, read_nothing)
{
  int ret = OB_SUCCESS;
  int64_t read_size = 0;
  int64_t count = TestLogFileStore::DIO_WRITE_ALIGN_SIZE;
  ObLogLocalStore file_store;
  file_store.init("./log_file_test/", TestLogFileStore::LOG_FILE_SIZE,
      clog::ObLogWritePoolType::CLOG_WRITE_POOL);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = file_store.open(1);
  ASSERT_EQ(OB_SUCCESS, ret);

  char *buf;
  ret = posix_memalign((void **) &buf, TestLogFileStore::DIO_WRITE_ALIGN_SIZE, count);
  ASSERT_EQ(0, ret);
  ASSERT_TRUE(NULL != buf);
  strcpy(buf, "this is rd_wr test");
  size_t len = strlen(buf);
  ASSERT_NE(0, len);

  ret = file_store.write((void *) buf, count, 0);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = file_store.close();
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = file_store.open(1);
  ASSERT_EQ(OB_SUCCESS, ret);

  char *rd_buf;
  rd_buf = static_cast<char *>(ob_malloc_align(
                               TestLogFileStore::DIO_WRITE_ALIGN_SIZE,
                               TestLogFileStore::LOG_FILE_SIZE,
                               "TestFileStore"));

  ret = file_store.read((void *) rd_buf, count, 0, read_size);
  OB_LOG(INFO, "buf value", K(buf), K(rd_buf), KP(buf), KP(rd_buf), K(len));
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(count, read_size);
  ASSERT_EQ(len, strlen(rd_buf));
  ASSERT_EQ(0, strcmp(buf, rd_buf));

  // read nothing
  read_size = count;
  ret = file_store.read((void *) rd_buf, count, TestLogFileStore::LOG_FILE_SIZE, read_size);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(0, read_size);

  // read oversize
  read_size = count;
  ret = file_store.read((void *) rd_buf, count, TestLogFileStore::LOG_FILE_SIZE + count, read_size);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(0, read_size);

  // read tail part
  read_size = count;
  ret = file_store.read((void *) rd_buf, count * 2, TestLogFileStore::LOG_FILE_SIZE - count, read_size);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(count, read_size);

  free(buf);
  ob_free_align(rd_buf);
}

TEST_F(TestLogFileStore, file_read)
{
  int ret = OB_SUCCESS;
  FileDirectoryUtils::create_full_path("./log_file_test/disk1/");
  FileDirectoryUtils::create_full_path("./log_file_test/disk2/");

  ObLogLocalStore file_store;
  file_store.init("./log_file_test/", TestLogFileStore::LOG_FILE_SIZE,
      clog::ObLogWritePoolType::SLOG_WRITE_POOL);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = file_store.open(1);
  ASSERT_EQ(OB_SUCCESS, ret);

  char *buf;
  ret = posix_memalign((void **) &buf, TestLogFileStore::DIO_WRITE_ALIGN_SIZE,
      TestLogFileStore::DIO_WRITE_ALIGN_SIZE);
  ASSERT_EQ(0, ret);
  ASSERT_TRUE(NULL != buf);
  strcpy(buf, "this is file_read test");
  size_t len = strlen(buf);
  ASSERT_NE(0, len);

  ret = file_store.write((void *) buf, TestLogFileStore::DIO_WRITE_ALIGN_SIZE, 0);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = file_store.close();
  ASSERT_EQ(OB_SUCCESS, ret);

  char prd_buf[1024] = "\0";
  ObString fname("./log_file_test/disk2/1");
  int64_t read_size = 0;
  ObFileReader file_reader;

  ret = file_reader.open(fname, true, TestLogFileStore::DIO_WRITE_ALIGN_SIZE);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = file_reader.pread(prd_buf, TestLogFileStore::DIO_WRITE_ALIGN_SIZE, 0, read_size);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(read_size > 0);
  OB_LOG(INFO, "buf value", K(buf), K(prd_buf), K(len));
  ASSERT_EQ(len, strlen(prd_buf));
  ASSERT_EQ(0, strcmp(buf, prd_buf));

  free(buf);
}

TEST_F(TestLogFileStore, write_file_fd_read)
{
  int ret = OB_SUCCESS;
  const int64_t file_id = 1;
  int64_t read_size = 0;
  int64_t count = TestLogFileStore::DIO_WRITE_ALIGN_SIZE;
  FileDirectoryUtils::create_full_path("./log_file_test/disk1/");
  FileDirectoryUtils::create_full_path("./log_file_test/disk2/");

  ObLogLocalStore file_store;
  file_store.init("./log_file_test/", TestLogFileStore::LOG_FILE_SIZE,
      clog::ObLogWritePoolType::SLOG_WRITE_POOL);
  ASSERT_EQ(OB_SUCCESS, ret);

  char *buf;
  ret = posix_memalign((void **) &buf, TestLogFileStore::DIO_WRITE_ALIGN_SIZE, count);
  ASSERT_EQ(0, ret);
  ASSERT_TRUE(NULL != buf);
  strcpy(buf, "this is rd_wr test");
  size_t len = strlen(buf);
  ASSERT_NE(0, len);

  ret = file_store.write_file(file_id, (void *)buf, count);
  ASSERT_EQ(OB_SUCCESS, ret);

  bool b_exist = false;
  FileDirectoryUtils::is_exists("./log_file_test/disk1/1", b_exist);
  ASSERT_TRUE(b_exist);
  FileDirectoryUtils::is_exists("./log_file_test/disk2/1", b_exist);
  ASSERT_TRUE(b_exist);

  char *rd_buf;
  ret = posix_memalign((void **) &rd_buf, TestLogFileStore::DIO_WRITE_ALIGN_SIZE, count);
  ObLogFileDescriptor log_fd;
  ret = ObLogFileReader::get_fd("./log_file_test", file_id, file_store.get_redo_log_type(), log_fd);
  ASSERT_EQ(OB_SUCCESS, ret);
  read_size = ObLogFileReader::pread(log_fd, (void *) rd_buf, count, 0);
  OB_LOG(INFO, "buf value", K(buf), K(rd_buf), KP(buf), KP(rd_buf), K(count), K(read_size));
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(count, read_size);
  ASSERT_EQ(len, strlen(rd_buf));
  ASSERT_EQ(0, strcmp(buf, rd_buf));
  ret = ObLogFileReader::close_fd(log_fd);
  ASSERT_EQ(OB_SUCCESS, ret);

  free(buf);
  free(rd_buf);
}

TEST_F(TestLogFileStore, log_file_reader2)
{
  int ret = OB_SUCCESS;
  clog::file_id_t file_id = 1;
  int64_t read_size = 0;
  int64_t count = TestLogFileStore::DIO_WRITE_ALIGN_SIZE;
  FileDirectoryUtils::create_full_path("./log_file_test/");

  ObLogLocalStore file_store;
  file_store.init("./log_file_test/", TestLogFileStore::LOG_FILE_SIZE,
      clog::ObLogWritePoolType::SLOG_WRITE_POOL);
  ASSERT_EQ(OB_SUCCESS, ret);

  char *buf;
  ret = posix_memalign((void **) &buf, TestLogFileStore::DIO_WRITE_ALIGN_SIZE, count);
  ASSERT_EQ(0, ret);
  ASSERT_TRUE(NULL != buf);
  STRCPY(buf, "this is rd_wr test");
  size_t len = STRLEN(buf);
  ASSERT_NE(0, len);

  ret = file_store.write_file(file_id, (void *)buf, count);
  ASSERT_EQ(OB_SUCCESS, ret);

  bool b_exist = false;
  FileDirectoryUtils::is_exists("./log_file_test/1", b_exist);
  ASSERT_TRUE(b_exist);

  char *rd_buf;
  ret = posix_memalign((void **) &rd_buf, TestLogFileStore::DIO_WRITE_ALIGN_SIZE, count);
  for (int64_t i = 0; i < 10; ++i) {
    ObLogReadFdHandle log_fd;
    MEMSET(rd_buf, 0, TestLogFileStore::DIO_WRITE_ALIGN_SIZE);
    ret = OB_LOG_FILE_READER.get_fd("./log_file_test", file_id, log_fd);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = OB_LOG_FILE_READER.pread(log_fd, (void *) rd_buf, count, 0, read_size);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(count, read_size);
    ASSERT_EQ(len, STRLEN(rd_buf));
    ASSERT_EQ(0, STRCMP(buf, rd_buf));
  }

  // evict fd and read again
  ret = FileDirectoryUtils::delete_file("./log_file_test/1");
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = OB_LOG_FILE_READER.evict_fd("./log_file_test", file_id);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObLogReadFdHandle log_fd;
  ret = OB_LOG_FILE_READER.get_fd("./log_file_test", file_id, log_fd);
  ASSERT_EQ(OB_FILE_NOT_EXIST, ret);

  free(buf);
  free(rd_buf);
}

TEST_F(TestLogFileStore, delete_file)
{
  int ret = OB_SUCCESS;
  FileDirectoryUtils::create_full_path("./log_file_test/disk1/");
  FileDirectoryUtils::create_full_path("./log_file_test/disk2/");

  ObLogLocalStore file_store;
  file_store.init("./log_file_test/", TestLogFileStore::LOG_FILE_SIZE,
      clog::ObLogWritePoolType::CLOG_WRITE_POOL);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = file_store.open(1);
  ASSERT_EQ(OB_SUCCESS, ret);

  bool is_exist = false;
  FileDirectoryUtils::is_exists("./log_file_test/disk1/1", is_exist);
  ASSERT_TRUE(is_exist);
  FileDirectoryUtils::is_exists("./log_file_test/disk2/1", is_exist);
  ASSERT_TRUE(is_exist);

  ret = file_store.delete_file(1);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = file_store.close();
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = file_store.delete_file(1);
  ASSERT_EQ(OB_SUCCESS, ret);

  FileDirectoryUtils::is_exists("./log_file_test/disk1/1", is_exist);
  ASSERT_FALSE(is_exist);
  FileDirectoryUtils::is_exists("./log_file_test/disk2/1", is_exist);
  ASSERT_FALSE(is_exist);
}

TEST_F(TestLogFileStore, slog_empty_file)
{
  int ret = OB_SUCCESS;

  ObLogLocalStore file_store;
  file_store.init("./log_file_test/", TestLogFileStore::LOG_FILE_SIZE,
      clog::ObLogWritePoolType::SLOG_WRITE_POOL);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = file_store.open(1);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = file_store.close();
  ASSERT_EQ(OB_SUCCESS, ret);

  int64_t file_size = 1;
  FileDirectoryUtils::get_file_size("./log_file_test/1", file_size);
  ASSERT_EQ(0, file_size);
}

TEST_F(TestLogFileStore, open_exist)
{
  int ret = OB_SUCCESS;
  bool is_exist = false;

  ObLogLocalStore file_store;
  file_store.init("./log_file_test/", TestLogFileStore::LOG_FILE_SIZE,
      clog::ObLogWritePoolType::SLOG_WRITE_POOL);
  ASSERT_EQ(OB_SUCCESS, ret);

  ASSERT_FALSE(file_store.is_opened());
  ASSERT_EQ(OB_SUCCESS, file_store.exist(1, is_exist));
  ASSERT_FALSE(is_exist);

  ret = file_store.open(1);
  ASSERT_EQ(OB_SUCCESS, ret);

  ASSERT_TRUE(file_store.is_opened());
  ASSERT_EQ(OB_SUCCESS, file_store.exist(1, is_exist));
  ASSERT_TRUE(is_exist);

  ret = file_store.close();
  ASSERT_EQ(OB_SUCCESS, ret);

  ASSERT_FALSE(file_store.is_opened());
  ASSERT_EQ(OB_SUCCESS, file_store.exist(1, is_exist));
  ASSERT_TRUE(is_exist);

  ret = file_store.delete_file(1);
  ASSERT_EQ(OB_SUCCESS, ret);

  ASSERT_FALSE(file_store.is_opened());
  ASSERT_EQ(OB_SUCCESS, file_store.exist(1, is_exist));
  ASSERT_FALSE(is_exist);
}

TEST_F(TestLogFileStore, truncate)
{
  int ret = OB_SUCCESS;
  const int64_t original_file_size = TestLogFileStore::LOG_FILE_SIZE;
  const int64_t truncate_file_size = 2 * 1024 * 1024;
  int64_t file_size = 0;

  ObLogLocalStore file_store;
  file_store.init("./log_file_test/", original_file_size,
      clog::ObLogWritePoolType::CLOG_WRITE_POOL);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = file_store.open(1);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = file_store.close();
  ASSERT_EQ(OB_SUCCESS, ret);

  FileDirectoryUtils::get_file_size("./log_file_test/1", file_size);
  ASSERT_EQ(original_file_size, file_size);

  ret = file_store.ftruncate(1, truncate_file_size);
  ASSERT_EQ(OB_SUCCESS, ret);

  FileDirectoryUtils::get_file_size("./log_file_test/1", file_size);
  ASSERT_EQ(truncate_file_size, file_size);
}

TEST_F(TestLogFileStore, set_bad_disk)
{
  int ret = OB_SUCCESS;
  int64_t read_size = 0;
  int64_t count = TestLogFileStore::DIO_WRITE_ALIGN_SIZE;
  FileDirectoryUtils::create_full_path("./log_file_test/disk1/");
  FileDirectoryUtils::create_full_path("./log_file_test/disk2/");
  FileDirectoryUtils::create_full_path("./log_file_test/disk3/");

  ObLogLocalStore file_store;
  file_store.init("./log_file_test/", TestLogFileStore::LOG_FILE_SIZE,
      clog::ObLogWritePoolType::CLOG_WRITE_POOL);
  ASSERT_EQ(OB_SUCCESS, ret);

  // make 2 disk bad
  ret = OB_CLOG_DISK_MGR.set_bad_disk(0);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = OB_CLOG_DISK_MGR.set_bad_disk(2);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = file_store.open(1);
  ASSERT_EQ(OB_SUCCESS, ret);

  char *buf;
  ret = posix_memalign((void **) &buf, TestLogFileStore::DIO_WRITE_ALIGN_SIZE, count);
  ASSERT_EQ(0, ret);
  ASSERT_TRUE(NULL != buf);
  strcpy(buf, "this is rd_wr test");
  size_t len = strlen(buf);
  ASSERT_NE(0, len);

  ret = file_store.write((void *) buf, count, 0);
  ASSERT_EQ(OB_SUCCESS, ret);

  char *rd_buf;
  ret = posix_memalign((void **) &rd_buf, TestLogFileStore::DIO_WRITE_ALIGN_SIZE, count);

  ret = file_store.read((void *) rd_buf, count, 0, read_size);
  OB_LOG(INFO, "buf value", K(buf), K(rd_buf), KP(buf), KP(rd_buf), K(len));
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(count, read_size);
  ASSERT_EQ(len, strlen(rd_buf));
  ASSERT_EQ(0, strcmp(buf, rd_buf));

  free(buf);
  free(rd_buf);

  ret = file_store.close();
  ASSERT_EQ(OB_SUCCESS, ret);
}

TEST_F(TestLogFileStore, errsim_single_disk_failure)
{
  int ret = OB_SUCCESS;
  int64_t count = TestLogFileStore::DIO_WRITE_ALIGN_SIZE;
  FileDirectoryUtils::create_full_path("./log_file_test/disk1/");

  ObLogLocalStore file_store;
  file_store.init("./log_file_test/", TestLogFileStore::LOG_FILE_SIZE,
      clog::ObLogWritePoolType::CLOG_WRITE_POOL);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = file_store.open(1);
  ASSERT_EQ(OB_SUCCESS, ret);

  // make write fail
#ifdef ERRSIM
  TP_SET_EVENT(EventTable::EN_IO_GETEVENTS, OB_IO_ERROR, 0, 1);
  usleep(100 * 1000);
#endif

  char *buf = nullptr;
  ret = posix_memalign((void **) &buf, TestLogFileStore::DIO_WRITE_ALIGN_SIZE, count);
  ASSERT_EQ(0, ret);
  ASSERT_TRUE(NULL != buf);
  strcpy(buf, "this is write_bad_disk test");
  size_t len = strlen(buf);
  ASSERT_NE(0, len);

  ret = file_store.write((void *) buf, count, 0);

#ifdef ERRSIM
  ASSERT_EQ(OB_IO_ERROR, ret);
#else
  ASSERT_EQ(OB_SUCCESS, ret);
#endif

  free(buf);

  ret = file_store.close();
  ASSERT_EQ(OB_SUCCESS, ret);
}

TEST_F(TestLogFileStore, errsim_disk_full)
{
  int ret = OB_SUCCESS;
  int64_t count = TestLogFileStore::DIO_WRITE_ALIGN_SIZE;
  FileDirectoryUtils::create_full_path("./log_file_test/disk1/");

  ObLogLocalStore file_store;
  file_store.init("./log_file_test/", TestLogFileStore::LOG_FILE_SIZE,
      clog::ObLogWritePoolType::CLOG_WRITE_POOL);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = file_store.open(1);
  ASSERT_EQ(OB_SUCCESS, ret);

  // make write return disk full
#ifdef ERRSIM
  // OB_SERVER_OUTOF_DISK_SPACE is not defined in lib/ob_errno.h, use OB_RESOURCE_OUT to represent
  TP_SET_EVENT(EventTable::EN_IO_GETEVENTS, OB_RESOURCE_OUT, 0, 1);
  usleep(100 * 1000);
#endif

  char *buf = nullptr;
  ret = posix_memalign((void **) &buf, TestLogFileStore::DIO_WRITE_ALIGN_SIZE, count);
  ASSERT_EQ(0, ret);
  ASSERT_TRUE(NULL != buf);
  strcpy(buf, "this is write_bad_disk test");
  size_t len = strlen(buf);
  ASSERT_NE(0, len);

  ret = file_store.write((void *) buf, count, 0);

#ifdef ERRSIM
  ASSERT_EQ(OB_SERVER_OUTOF_DISK_SPACE, ret);
#else
  ASSERT_EQ(OB_SUCCESS, ret);
#endif

  free(buf);

  ret = file_store.close();
  ASSERT_EQ(OB_SUCCESS, ret);
}

TEST_F(TestLogFileStore, errsim_aio_timeout)
{
  int ret = OB_SUCCESS;
  int64_t count = TestLogFileStore::DIO_WRITE_ALIGN_SIZE;
  FileDirectoryUtils::create_full_path("./log_file_test/disk1/");

  ObLogLocalStore file_store;
  file_store.init("./log_file_test/", TestLogFileStore::LOG_FILE_SIZE,
      clog::ObLogWritePoolType::CLOG_WRITE_POOL);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = file_store.open(1);
  ASSERT_EQ(OB_SUCCESS, ret);

#ifdef ERRSIM
  TP_SET_EVENT(EventTable::EN_IO_GETEVENTS, OB_AIO_TIMEOUT, 0, 1);
  usleep(100 * 1000);
#endif

  char *buf = nullptr;
  ret = posix_memalign((void **) &buf, TestLogFileStore::DIO_WRITE_ALIGN_SIZE, count);
  ASSERT_EQ(0, ret);
  ASSERT_TRUE(NULL != buf);
  strcpy(buf, "this is write_bad_disk test");
  size_t len = strlen(buf);
  ASSERT_NE(0, len);

  ret = file_store.write((void *) buf, count, 0);

#ifdef ERRSIM
  ASSERT_EQ(OB_TIMEOUT, ret);
#else
  ASSERT_EQ(OB_SUCCESS, ret);
#endif

  free(buf);

  ret = file_store.close();
  ASSERT_EQ(OB_SUCCESS, ret);
}

TEST_F(TestLogFileStore, errsim_bad_disk)
{
  int ret = OB_SUCCESS;
  int64_t read_size = 0;
  int64_t count = TestLogFileStore::DIO_WRITE_ALIGN_SIZE;
  FileDirectoryUtils::create_full_path("./log_file_test/disk1/");
  FileDirectoryUtils::create_full_path("./log_file_test/disk2/");
  FileDirectoryUtils::create_full_path("./log_file_test/disk3/");

  ObLogLocalStore file_store;
  file_store.init("./log_file_test/", TestLogFileStore::LOG_FILE_SIZE,
      clog::ObLogWritePoolType::CLOG_WRITE_POOL);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = file_store.open(1);
  ASSERT_EQ(OB_SUCCESS, ret);

  // random make 1 disk fail
#ifdef ERRSIM
  TP_SET_EVENT(EventTable::EN_IO_GETEVENTS, OB_EAGAIN, 0, 1);
  usleep(100 * 1000);
#endif

  // write should still succeed since 2 disk are good
  char *buf;
  ret = posix_memalign((void **) &buf, TestLogFileStore::DIO_WRITE_ALIGN_SIZE, count);
  ASSERT_EQ(0, ret);
  ASSERT_TRUE(NULL != buf);
  strcpy(buf, "this is write_bad_disk test");
  size_t len = strlen(buf);
  ASSERT_NE(0, len);

  ret = file_store.write((void *) buf, count, 0);
  ASSERT_EQ(OB_SUCCESS, ret);

#ifdef ERRSIM
  TP_SET_EVENT(EventTable::EN_IO_GETEVENTS, OB_SUCCESS, 0, 1);
  usleep(100 * 1000);
#endif

  // read buf and check
  char *rd_buf;
  ret = posix_memalign((void **) &rd_buf, TestLogFileStore::DIO_WRITE_ALIGN_SIZE, count);
  ret = file_store.read((void *) rd_buf, count, 0, read_size);
  OB_LOG(INFO, "buf value", K(buf), K(rd_buf), KP(buf), KP(rd_buf), K(len));
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(count, read_size);
  ASSERT_EQ(len, strlen(rd_buf));
  ASSERT_EQ(0, strcmp(buf, rd_buf));

  // write and read again
  strcpy(buf, "test write_bad_disk is this");
  len = strlen(buf);
  ASSERT_NE(0, len);

  ret = file_store.write((void *) buf, count, 0);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = file_store.read((void *) rd_buf, count, 0, read_size);
  OB_LOG(INFO, "buf value", K(buf), K(rd_buf), KP(buf), KP(rd_buf), K(len));
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(count, read_size);
  ASSERT_EQ(len, strlen(rd_buf));
  ASSERT_EQ(0, strcmp(buf, rd_buf));

  ObLogFileDescriptor log_fd;
  ret = log_fd.init(&OB_CLOG_DISK_MGR, ObLogFileDescriptor::READ_FLAG, 1);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = log_fd.sync();
  ASSERT_EQ(OB_SUCCESS, ret);

#ifdef ERRSIM
  ASSERT_EQ(2, log_fd.count());
#else
  ASSERT_EQ(3, log_fd.count());
#endif

  free(buf);
  free(rd_buf);

  ret = file_store.close();
  ASSERT_EQ(OB_SUCCESS, ret);
}

TEST_F(TestLogFileStore, errsim_partial_submit)
{
  int ret = OB_SUCCESS;
  const int64_t file_id = 1;
  int64_t read_size = 0;
  int64_t count = TestLogFileStore::DIO_WRITE_ALIGN_SIZE;
  FileDirectoryUtils::create_full_path("./log_file_test/disk1/");
  FileDirectoryUtils::create_full_path("./log_file_test/disk2/");
  FileDirectoryUtils::create_full_path("./log_file_test/disk3/");
  FileDirectoryUtils::create_full_path("./log_file_test/disk4/");

  ObLogLocalStore file_store;
  file_store.init("./log_file_test/", TestLogFileStore::LOG_FILE_SIZE,
      clog::ObLogWritePoolType::CLOG_WRITE_POOL);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = file_store.open(file_id);
  ASSERT_EQ(OB_SUCCESS, ret);

  // each time submit 1 disk event
#ifdef ERRSIM
  TP_SET_EVENT(EventTable::EN_IO_SUBMIT, OB_EAGAIN, 0, 1);
  usleep(100 * 1000);
#endif

  // write should still succeed because three disks write succeeded
  char *buf;
  ret = posix_memalign((void **) &buf, TestLogFileStore::DIO_WRITE_ALIGN_SIZE, count);
  ASSERT_EQ(0, ret);
  ASSERT_TRUE(NULL != buf);
  strcpy(buf, "this is write_bad_disk test");
  size_t len = strlen(buf);
  ASSERT_NE(0, len);

  ret = file_store.write((void *) buf, count, 0);
  ASSERT_EQ(OB_SUCCESS, ret);

  // read buf and check
  char *rd_buf;
  ret = posix_memalign((void **) &rd_buf, TestLogFileStore::DIO_WRITE_ALIGN_SIZE, count);
  ret = file_store.read((void *) rd_buf, count, 0, read_size);
  OB_LOG(INFO, "buf value", K(buf), K(rd_buf), KP(buf), KP(rd_buf), K(len));
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(count, read_size);
  ASSERT_EQ(len, strlen(rd_buf));
  ASSERT_EQ(0, strcmp(buf, rd_buf));

  ObLogFileDescriptor log_fd;
  ret = log_fd.init(&OB_CLOG_DISK_MGR, ObLogFileDescriptor::READ_FLAG, 1);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = log_fd.sync();
  ASSERT_EQ(OB_SUCCESS, ret);

#ifdef ERRSIM
  ASSERT_EQ(3, log_fd.count());
#else
  ASSERT_EQ(4, log_fd.count());
#endif

  free(buf);
  free(rd_buf);

  ret = file_store.close();
  ASSERT_EQ(OB_SUCCESS, ret);
}

TEST_F(TestLogFileStore, errsim_partial_complete)
{
  int ret = OB_SUCCESS;
  const int64_t file_id = 1;
  int64_t read_size = 0;
  int64_t count = DIO_READ_ALIGN_SIZE * 2;
  FileDirectoryUtils::create_full_path("./log_file_test/disk1/");
  FileDirectoryUtils::create_full_path("./log_file_test/disk2/");
  FileDirectoryUtils::create_full_path("./log_file_test/disk3/");

  ObLogLocalStore file_store;
  file_store.init("./log_file_test/", TestLogFileStore::LOG_FILE_SIZE,
      clog::ObLogWritePoolType::CLOG_WRITE_POOL);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = file_store.open(file_id);
  ASSERT_EQ(OB_SUCCESS, ret);

  char *buf;
  ret = posix_memalign((void **) &buf, TestLogFileStore::DIO_WRITE_ALIGN_SIZE, count);
  ASSERT_EQ(0, ret);
  ASSERT_TRUE(NULL != buf);
  strcpy(buf, "this is errsim_partial_complete test");
  strcpy(buf + DIO_READ_ALIGN_SIZE, "errsim_partial_complete again");

  // simulate partial complete
#ifdef ERRSIM
  TP_SET_EVENT(EventTable::EN_IO_SUBMIT, OB_NEED_RETRY, 0, 1);
  usleep(100 * 1000);
#endif

  ret = file_store.write((void *) buf, count, 0);
  ASSERT_EQ(OB_SUCCESS, ret);

#ifdef ERRSIM
  TP_SET_EVENT(EventTable::EN_IO_SUBMIT, OB_SUCCESS, 0, 1);
  usleep(100 * 1000);
#endif

  // read buf and check
  char *rd_buf;
  ret = posix_memalign((void **) &rd_buf, TestLogFileStore::DIO_WRITE_ALIGN_SIZE, count);
  ret = file_store.read((void *) rd_buf, count, 0, read_size);
  OB_LOG(INFO, "buf value", K(buf), K(rd_buf), K(buf + DIO_READ_ALIGN_SIZE), K(rd_buf + DIO_READ_ALIGN_SIZE), K(read_size));
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(count, read_size);
  ASSERT_EQ(0, strcmp(buf, rd_buf));
  ASSERT_EQ(0, strcmp(buf + DIO_READ_ALIGN_SIZE, rd_buf + DIO_READ_ALIGN_SIZE));

  ObLogFileDescriptor log_fd;
  ret = log_fd.init(&OB_CLOG_DISK_MGR, ObLogFileDescriptor::READ_FLAG, 1);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = log_fd.sync();
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(3, log_fd.count());

  free(buf);
  free(rd_buf);

  ret = file_store.close();
  ASSERT_EQ(OB_SUCCESS, ret);
}
} // namespace common
} // namespace oceanbase

int main(int argc, char** argv)
{
  system("rm -f test_log_file_store.log");
  OB_LOGGER.set_file_name("test_log_file_store.log", true);
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
