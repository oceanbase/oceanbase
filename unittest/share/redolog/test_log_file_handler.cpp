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
#include "share/redolog/ob_log_file_reader.h"
#include "share/redolog/ob_log_definition.h"
#include "common/log/ob_log_constants.h"
#include "common/storage/ob_io_device.h"
#include "share/ob_local_device.h"
#include "storage/blocksstable/ob_data_file_prepare.h"
#include "lib/file/ob_file.h"
#include "share/ob_simple_mem_limit_getter.h"

#define private public
#include "share/redolog/ob_log_file_handler.h"
#undef private

namespace oceanbase
{
using namespace share;

namespace common
{
static ObSimpleMemLimitGetter getter;
class TestLogFileHandler: public blocksstable::TestDataFilePrepare
{
public:
  TestLogFileHandler()
    : blocksstable::TestDataFilePrepare(&getter,
                                        "TestLogFileHandler")
  {
  }
  virtual ~TestLogFileHandler()
  {
  }
  virtual void SetUp();
  virtual void TearDown();

public:
  static const int64_t LOG_FILE_SIZE;
  static const int DIO_WRITE_ALIGN_SIZE;
  static const mode_t DIR_CREATE_MODE;

  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(TestLogFileHandler);
};

const int64_t TestLogFileHandler::LOG_FILE_SIZE = 4 * 1024 * 1024;
const int TestLogFileHandler::DIO_WRITE_ALIGN_SIZE = ObLogConstants::LOG_FILE_ALIGN_SIZE;
const mode_t TestLogFileHandler::DIR_CREATE_MODE = S_IRWXU | S_IRGRP | S_IXGRP | S_IROTH | S_IXOTH;

void TestLogFileHandler::SetUp()
{
  system("rm -rf ./data_TestLogFileHandler");
  TestDataFilePrepare::SetUp(); // init dir and io device
  FileDirectoryUtils::create_full_path("./data_TestLogFileHandler/clog/");

  ASSERT_EQ(OB_SUCCESS, OB_LOG_FILE_READER.init());

#ifdef ERRSIM
  TP_SET_EVENT(EventTable::EN_IO_GETEVENTS, OB_SUCCESS, 0, 1);
  TP_SET_EVENT(EventTable::EN_IO_SUBMIT, OB_SUCCESS, 0, 1);
  usleep(100 * 1000);
#endif
}

void TestLogFileHandler::TearDown()
{
  //system("rm -rf ./log_file_test");
  THE_IO_DEVICE->destroy();
  OB_LOG_FILE_READER.destroy();
  TestDataFilePrepare::TearDown();
#ifdef ERRSIM
  TP_SET_EVENT(EventTable::EN_IO_GETEVENTS, OB_SUCCESS, 0, 1);
  TP_SET_EVENT(EventTable::EN_IO_SUBMIT, OB_SUCCESS, 0, 1);
  usleep(100 * 1000);
#endif
}

TEST_F(TestLogFileHandler, simple)
{
  int ret = OB_SUCCESS;
  ObLogFileHandler file_handler;
  file_handler.init(util_.get_storage_env().clog_dir_, TestLogFileHandler::LOG_FILE_SIZE, OB_SERVER_TENANT_ID);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = file_handler.open(1);
  ASSERT_EQ(OB_SUCCESS, ret);

  char *buf = nullptr;
  ret = posix_memalign((void **) &buf, TestLogFileHandler::DIO_WRITE_ALIGN_SIZE,
      TestLogFileHandler::DIO_WRITE_ALIGN_SIZE);
  ASSERT_EQ(0, ret);
  ASSERT_TRUE(NULL != buf);
  strcpy(buf, "this is simple test");

  // no need to sync
  ret = file_handler.write((void *) buf, TestLogFileHandler::DIO_WRITE_ALIGN_SIZE, 0);
  ASSERT_EQ(OB_SUCCESS, ret);
  free(buf);

  ret = file_handler.close();
  ASSERT_EQ(OB_SUCCESS, ret);
}

TEST_F(TestLogFileHandler, rd_wr)
{
  int ret = OB_SUCCESS;
  int64_t read_size = 0;
  int64_t count = TestLogFileHandler::DIO_WRITE_ALIGN_SIZE;
  const char *log_dir = util_.get_storage_env().log_spec_.log_dir_;

  ObLogFileHandler file_handler;
  file_handler.init(log_dir, TestLogFileHandler::LOG_FILE_SIZE, OB_SERVER_TENANT_ID);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = file_handler.open(1);
  ASSERT_EQ(OB_SUCCESS, ret);

  char *buf;
  ret = posix_memalign((void **) &buf, TestLogFileHandler::DIO_WRITE_ALIGN_SIZE, count);
  ASSERT_EQ(0, ret);
  ASSERT_TRUE(NULL != buf);
  strcpy(buf, "this is rd_wr test");
  size_t len = strlen(buf);
  ASSERT_NE(0, len);

  ret = file_handler.write((void *) buf, count, 0);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = file_handler.close();
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = file_handler.open(1);
  ASSERT_EQ(OB_SUCCESS, ret);

  char *rd_buf;
  ret = posix_memalign((void **) &rd_buf, TestLogFileHandler::DIO_WRITE_ALIGN_SIZE, count);

  ret = file_handler.read((void *) rd_buf, count, 0, read_size);
  OB_LOG(INFO, "buf value", K(buf), K(rd_buf), KP(buf), KP(rd_buf), K(len));
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(count, read_size);
  ASSERT_EQ(len, strlen(rd_buf));
  ASSERT_EQ(0, strcmp(buf, rd_buf));

  // read again
  MEMSET(rd_buf, 0, len);
  read_size = 0;
  ret = file_handler.read((void *) rd_buf, count, 0, read_size);
  OB_LOG(INFO, "buf value", K(buf), K(rd_buf), KP(buf), KP(rd_buf), K(len));
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(count, read_size);
  ASSERT_EQ(len, strlen(rd_buf));
  ASSERT_EQ(0, strcmp(buf, rd_buf));

  free(buf);
  free(rd_buf);

  ret = file_handler.close();
  ASSERT_EQ(OB_SUCCESS, ret);
}

TEST_F(TestLogFileHandler, read_nothing)
{
  int ret = OB_SUCCESS;
  int64_t read_size = 0;
  int64_t count = TestLogFileHandler::DIO_WRITE_ALIGN_SIZE;
  ObLogFileHandler file_handler;
  file_handler.init(util_.get_storage_env().clog_dir_, TestLogFileHandler::LOG_FILE_SIZE, OB_SERVER_TENANT_ID);
  ASSERT_EQ(OB_SUCCESS, ret);

  char file_path[MAX_PATH_SIZE] = { 0 };
  snprintf(file_path, sizeof(file_path), "%s/%d", util_.get_storage_env().clog_dir_, 1);
  const int64_t CLOG_FILE_ACTURAL_SIZE = 4 * 1024 * 1024;

  ret = file_handler.open(1);
  ASSERT_EQ(OB_SUCCESS, ret);

  char *buf;
  ret = posix_memalign((void **) &buf, TestLogFileHandler::DIO_WRITE_ALIGN_SIZE, count);
  ASSERT_EQ(0, ret);
  ASSERT_TRUE(NULL != buf);
  strcpy(buf, "this is rd_wr test");
  size_t len = strlen(buf);
  ASSERT_NE(0, len);

  ret = file_handler.close();
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = file_handler.open(1);
  ASSERT_EQ(OB_SUCCESS, ret);

  char rd_buf[TestLogFileHandler::DIO_WRITE_ALIGN_SIZE * 2] = { 0 };
  ret = file_handler.read((void *) rd_buf, count, 0, read_size);
  OB_LOG(INFO, "buf value", K(buf), K(rd_buf), KP(buf), KP(rd_buf), K(len));
  ASSERT_EQ(OB_SUCCESS, ret);

  // read nothing
  read_size = count;
  ret = file_handler.read((void *) rd_buf, count, CLOG_FILE_ACTURAL_SIZE, read_size);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(0, read_size);

  // read oversize, fail
  read_size = count;
  ret = file_handler.read((void *) rd_buf, count, CLOG_FILE_ACTURAL_SIZE + count, read_size);
  ASSERT_NE(OB_SUCCESS, ret);

  ret = file_handler.close();
  ASSERT_EQ(OB_SUCCESS, ret);

  free(buf);
}

TEST_F(TestLogFileHandler, file_read)
{
  int ret = OB_SUCCESS;
  ObLogFileHandler file_handler;
  file_handler.init(util_.get_storage_env().log_spec_.log_dir_, TestLogFileHandler::LOG_FILE_SIZE, OB_SERVER_TENANT_ID);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = file_handler.open(1);
  ASSERT_EQ(OB_SUCCESS, ret);

  char *buf;
  ret = posix_memalign((void **) &buf, TestLogFileHandler::DIO_WRITE_ALIGN_SIZE,
      TestLogFileHandler::DIO_WRITE_ALIGN_SIZE);
  ASSERT_EQ(0, ret);
  ASSERT_TRUE(NULL != buf);
  strcpy(buf, "this is file_read test");
  size_t len = strlen(buf);
  ASSERT_NE(0, len);

  ret = file_handler.write((void *) buf, TestLogFileHandler::DIO_WRITE_ALIGN_SIZE, 0);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = file_handler.close();
  ASSERT_EQ(OB_SUCCESS, ret);

  char file_path[MAX_PATH_SIZE] = { 0 };
  snprintf(file_path, sizeof(file_path), "%s/%d", util_.get_storage_env().log_spec_.log_dir_, 1);
  ObString fname(file_path);
  ObFileReader file_reader;
  ret = file_reader.open(fname, true, TestLogFileHandler::DIO_WRITE_ALIGN_SIZE);
  ASSERT_EQ(OB_SUCCESS, ret);

  char prd_buf[4096] = "\0";
  int64_t read_size = 0;
  ret = file_reader.pread(prd_buf, TestLogFileHandler::DIO_WRITE_ALIGN_SIZE, 0, read_size);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(read_size > 0);
  OB_LOG(INFO, "buf value", K(buf), K(prd_buf), K(len));
  ASSERT_EQ(len, strlen(prd_buf));
  ASSERT_EQ(0, strcmp(buf, prd_buf));

  free(buf);
}

TEST_F(TestLogFileHandler, write_file_fd_read)
{
  int ret = OB_SUCCESS;
  const int64_t file_id = 1;
  int64_t read_size = 0;
  int64_t count = TestLogFileHandler::DIO_WRITE_ALIGN_SIZE;

  ObLogFileHandler file_handler;
  file_handler.init(util_.get_storage_env().log_spec_.log_dir_, TestLogFileHandler::LOG_FILE_SIZE, OB_SERVER_TENANT_ID);
  ASSERT_EQ(OB_SUCCESS, ret);

  char *buf;
  ret = posix_memalign((void **) &buf, TestLogFileHandler::DIO_WRITE_ALIGN_SIZE, count);
  ASSERT_EQ(0, ret);
  ASSERT_TRUE(NULL != buf);
  strcpy(buf, "this is rd_wr test");
  size_t len = strlen(buf);
  ASSERT_NE(0, len);

  ret = file_handler.open(1);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = file_handler.write((void *)buf, count, 0);
  ASSERT_EQ(OB_SUCCESS, ret);

  bool b_exist = false;
  char file_path[MAX_PATH_SIZE] = { 0 };
  snprintf(file_path, sizeof(file_path), "%s/%d", util_.get_storage_env().log_spec_.log_dir_, 1);
  FileDirectoryUtils::is_exists(file_path, b_exist);
  ASSERT_TRUE(b_exist);

  char *rd_buf;
  ret = posix_memalign((void **) &rd_buf, TestLogFileHandler::DIO_WRITE_ALIGN_SIZE, count);
  ObLogReadFdHandle fd_handle;
  ret = OB_LOG_FILE_READER.get_fd(util_.get_storage_env().log_spec_.log_dir_, file_id, fd_handle);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = OB_LOG_FILE_READER.pread(fd_handle, (void *) rd_buf, count, 0, read_size);
  OB_LOG(INFO, "buf value", K(buf), K(rd_buf), KP(buf), KP(rd_buf), K(count), K(read_size));
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(count, read_size);
  ASSERT_EQ(len, strlen(rd_buf));
  ASSERT_EQ(0, strcmp(buf, rd_buf));
  //ret = OB_LOG_FILE_READER.close_fd(log_fd);
  //ASSERT_EQ(OB_SUCCESS, ret);

  ret = file_handler.close();
  ASSERT_EQ(OB_SUCCESS, ret);

  free(buf);
  free(rd_buf);
}

TEST_F(TestLogFileHandler, log_file_reader2)
{
  int ret = OB_SUCCESS;
  // clog::file_id_t file_id = 1;
  const int64_t file_id = 1;
  int64_t read_size = 0;
  int64_t count = TestLogFileHandler::DIO_WRITE_ALIGN_SIZE;

  ObLogFileHandler file_handler;
  file_handler.init(util_.get_storage_env().log_spec_.log_dir_, TestLogFileHandler::LOG_FILE_SIZE, OB_SERVER_TENANT_ID);
  ASSERT_EQ(OB_SUCCESS, ret);

  char *buf;
  ret = posix_memalign((void **) &buf, TestLogFileHandler::DIO_WRITE_ALIGN_SIZE, count);
  ASSERT_EQ(0, ret);
  ASSERT_TRUE(NULL != buf);
  STRCPY(buf, "this is rd_wr test");
  size_t len = STRLEN(buf);
  ASSERT_NE(0, len);

  ret = file_handler.open(1);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = file_handler.write((void *)buf, count, 0);
  ASSERT_EQ(OB_SUCCESS, ret);

  bool b_exist = false;
  char file_path[MAX_PATH_SIZE] = { 0 };
  snprintf(file_path, sizeof(file_path), "%s/%ld", util_.get_storage_env().log_spec_.log_dir_, file_id);
  FileDirectoryUtils::is_exists(file_path, b_exist);
  ASSERT_TRUE(b_exist);

  char *rd_buf;
  ret = posix_memalign((void **) &rd_buf, TestLogFileHandler::DIO_WRITE_ALIGN_SIZE, count);
  for (int64_t i = 0; i < 10; ++i) {
    ObLogReadFdHandle log_fd;
    MEMSET(rd_buf, 0, TestLogFileHandler::DIO_WRITE_ALIGN_SIZE);
    ret = OB_LOG_FILE_READER.get_fd(util_.get_storage_env().log_spec_.log_dir_, file_id, log_fd);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = OB_LOG_FILE_READER.pread(log_fd, (void *) rd_buf, count, 0, read_size);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(count, read_size);
    ASSERT_EQ(len, STRLEN(rd_buf));
    ASSERT_EQ(0, STRCMP(buf, rd_buf));
  }

  // evict fd and read again
  //ret = FileDirectoryUtils::delete_file(file_path);
  ret = file_handler.delete_file(file_id);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = OB_LOG_FILE_READER.evict_fd(util_.get_storage_env().log_spec_.log_dir_, file_id);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObLogReadFdHandle log_fd;
  ret = OB_LOG_FILE_READER.get_fd(util_.get_storage_env().log_spec_.log_dir_, file_id, log_fd);
  ASSERT_EQ(OB_NO_SUCH_FILE_OR_DIRECTORY, ret); // file does not exist

  ret = file_handler.close();
  ASSERT_EQ(OB_SUCCESS, ret);

  free(buf);
  free(rd_buf);
}

TEST_F(TestLogFileHandler, delete_file)
{
  int ret = OB_SUCCESS;

  ObLogFileHandler file_handler;
  file_handler.init(util_.get_storage_env().log_spec_.log_dir_, TestLogFileHandler::LOG_FILE_SIZE, OB_SERVER_TENANT_ID);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = file_handler.open(1);
  ASSERT_EQ(OB_SUCCESS, ret);

  bool is_exist = false;
  char file_path[MAX_PATH_SIZE] = { 0 };
  snprintf(file_path, sizeof(file_path), "%s/%d", util_.get_storage_env().log_spec_.log_dir_, 1);
  FileDirectoryUtils::is_exists(file_path, is_exist);
  ASSERT_TRUE(is_exist);

  ret = file_handler.delete_file(1);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = file_handler.close();
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = file_handler.delete_file(1);
  ASSERT_EQ(OB_SUCCESS, ret); // file does not exist

  FileDirectoryUtils::is_exists(file_path, is_exist);
  ASSERT_FALSE(is_exist);
}

TEST_F(TestLogFileHandler, clean_tmp_files)
{
  int ret = OB_SUCCESS;
  const char* log_dir = util_.get_storage_env().log_spec_.log_dir_;

  // create 5 normal file
  for (int i = 1; i <= 10; ++i) {
    common::ObIOFd io_fd;
    char full_path[MAX_PATH_SIZE] = { 0 };
    snprintf(full_path, sizeof(full_path), "%s/%d", log_dir, i);
    ret = THE_IO_DEVICE->open(full_path, ObLogDefinition::LOG_APPEND_FLAG, ObLogDefinition::FILE_OPEN_MODE, io_fd);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_TRUE(io_fd.is_normal_file());
    ret = THE_IO_DEVICE->close(io_fd);
    ASSERT_EQ(OB_SUCCESS, ret);
  }

  // create 5 tmp file
  for (int i = 1; i <= 10; ++i) {
    common::ObIOFd io_fd;
    char full_path[MAX_PATH_SIZE] = { 0 };
    snprintf(full_path, sizeof(full_path), "%s/%d.tmp", log_dir, i);
    ret = THE_IO_DEVICE->open(full_path, ObLogDefinition::LOG_APPEND_FLAG, ObLogDefinition::FILE_OPEN_MODE, io_fd);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_TRUE(io_fd.is_normal_file());
    ret = THE_IO_DEVICE->close(io_fd);
    ASSERT_EQ(OB_SUCCESS, ret);
  }

  ObLogFileHandler file_handler;
  ret = file_handler.init(log_dir, TestLogFileHandler::LOG_FILE_SIZE, OB_SERVER_TENANT_ID);
  ASSERT_EQ(OB_SUCCESS, ret);

  // check files
  for (int i = 1; i <= 10; ++i) {
    bool b_exist = false;
    char full_path[MAX_PATH_SIZE] = { 0 };
    snprintf(full_path, sizeof(full_path), "%s/%d", log_dir, i);
    ret = THE_IO_DEVICE->exist(full_path, b_exist);
    ASSERT_TRUE(b_exist);
  }
}

} // namespace common
} // namespace oceanbase

int main(int argc, char** argv)
{
  system("rm -f test_log_file_handler.log*");
  OB_LOGGER.set_file_name("test_log_file_handler.log", true);
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
