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

#define protected public
#include "lib/restore/ob_storage.h"
#include "lib/restore/ob_storage_oss_base.cpp"
#undef protected

#include "lib/profile/ob_trace_id.h"
#include <gtest/gtest.h>
#include "lib/utility/ob_test_util.h"
#include "lib/thread/ob_dynamic_thread_pool.h"
#include "share/backup/ob_backup_io_adapter.h"
#include "common/storage/ob_fd_simulator.h"
#include "share/ob_device_manager.h"
#include "share/backup/ob_backup_struct.h"

using namespace oceanbase::common;
using namespace oceanbase::share;
class TestStorageFile: public ::testing::Test
{
public:
  TestStorageFile() {}
  virtual ~TestStorageFile(){}
  virtual void SetUp()
  {
    ASSERT_EQ(OB_SUCCESS,
        databuff_printf(test_dir_, sizeof(test_dir_), "%s/test_storage",
            get_current_dir_name()));
    ASSERT_EQ(OB_SUCCESS,
        databuff_printf(test_dir_uri_, sizeof(test_dir_uri_), "file://%s", test_dir_));
    STORAGE_LOG(INFO, "clean test_storageg dir");
    ASSERT_EQ(0, ::system("rm -fr test_storage"));
  }
  virtual void TearDown()
  {
    STORAGE_LOG(INFO, "clean test_storageg dir");
    ASSERT_EQ(0, ::system("rm -fr test_storage"));
  }

  static void SetUpTestCase()
  {
  }

  static void TearDownTestCase()
  {
  }
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(TestStorageFile);
protected:
  // function members
protected:
  char test_dir_[OB_MAX_URI_LENGTH];
  char test_dir_uri_[OB_MAX_URI_LENGTH];
};

TEST_F(TestStorageFile, test_util)
{
  ObBackupIoAdapter util;
  char uri[OB_MAX_URI_LENGTH];
  ObBackupStorageInfo storage_info;
  storage_info.device_type_ = ObStorageType::OB_STORAGE_FILE;
  bool is_exist = false;
  bool is_empty_dir = false;
  char test_content[OB_MAX_URI_LENGTH] = "just_for_test";
  char read_buf[OB_MAX_URI_LENGTH];
  int64_t read_size = 0;
  ASSERT_EQ(OB_SUCCESS, util.mkdir(test_dir_uri_, &storage_info));
  ASSERT_EQ(OB_SUCCESS, util.is_empty_directory(test_dir_uri_, &storage_info, is_empty_dir));
  ASSERT_TRUE(is_empty_dir);

  ASSERT_EQ(OB_SUCCESS, databuff_printf(uri, sizeof(uri), "file://%s/test_file", test_dir_));
  ASSERT_EQ(OB_SUCCESS, util.is_exist(uri, &storage_info, is_exist));
  ASSERT_FALSE(is_exist);
  ASSERT_EQ(OB_SUCCESS, util.write_single_file(uri, &storage_info, test_content, strlen(test_content)));
  ASSERT_EQ(OB_SUCCESS, util.get_file_length(uri, &storage_info, read_size));
  ASSERT_EQ(strlen(test_content), read_size);
  ASSERT_EQ(OB_SUCCESS, util.is_exist(uri, &storage_info, is_exist));
  ASSERT_TRUE(is_exist);

  ASSERT_EQ(OB_SUCCESS, util.is_empty_directory(test_dir_uri_, &storage_info, is_empty_dir));
  ASSERT_FALSE(is_empty_dir);

  ASSERT_EQ(OB_SUCCESS, util.del_file(uri, &storage_info));
  ASSERT_EQ(OB_SUCCESS, util.is_exist(uri, &storage_info, is_exist));
  ASSERT_FALSE(is_exist);

  ASSERT_EQ(OB_SUCCESS, util.write_single_file(uri, &storage_info, test_content, strlen(test_content)));
  ASSERT_EQ(OB_SUCCESS, util.read_single_file(uri, &storage_info, read_buf, sizeof(read_buf), read_size));
  ASSERT_EQ(strlen(test_content), read_size);
  ASSERT_EQ(0, strncmp(test_content, read_buf, read_size));
  ASSERT_EQ(OB_SUCCESS, util.read_single_text_file(uri, &storage_info, read_buf, sizeof(read_buf)));
  ASSERT_EQ(strlen(test_content), strlen(read_buf));
  ASSERT_EQ(0, strcmp(test_content, read_buf));

  ASSERT_EQ(OB_INVALID_BACKUP_DEST, util.is_exist("bad://", &storage_info, is_exist));
  ASSERT_EQ(OB_INVALID_BACKUP_DEST, util.get_file_length("bad://", &storage_info, read_size));
  ASSERT_EQ(OB_INVALID_BACKUP_DEST, util.del_file("bad://", &storage_info));
  ASSERT_EQ(OB_INVALID_BACKUP_DEST, util.read_single_file("bad://", &storage_info, read_buf, sizeof(read_buf), read_size));
  ASSERT_EQ(OB_INVALID_BACKUP_DEST, util.read_single_text_file("bad://", &storage_info, read_buf, sizeof(read_buf)));
  ASSERT_EQ(OB_INVALID_BACKUP_DEST, util.write_single_file("bad://", &storage_info, test_content, strlen(test_content)));
}

TEST_F(TestStorageFile, test_reader)
{
  ObBackupIoAdapter util;
  char uri[OB_MAX_URI_LENGTH];
  ObBackupStorageInfo storage_info;
  storage_info.device_type_ = ObStorageType::OB_STORAGE_FILE;
  char test_content[OB_MAX_URI_LENGTH] = "just_for_test";
  char read_buf[OB_MAX_URI_LENGTH];
  int64_t read_size = 0;

  ASSERT_EQ(OB_SUCCESS, util.mkdir(test_dir_uri_, &storage_info));
  ASSERT_EQ(OB_SUCCESS, databuff_printf(uri, sizeof(uri), "file://%s/test_file", test_dir_));

  ObIODevice* dev_handle = NULL;
  ObIOFd fd;
  ObIOFd fd_2;

  ASSERT_EQ(OB_INVALID_BACKUP_DEST, util.open_with_access_type(dev_handle, fd, &storage_info, "bad://",  OB_STORAGE_ACCESS_READER));
  ASSERT_EQ(OB_BACKUP_FILE_NOT_EXIST, util.open_with_access_type(dev_handle, fd, &storage_info, uri, OB_STORAGE_ACCESS_READER));
  ASSERT_EQ(OB_SUCCESS, util.write_single_file(uri, &storage_info, test_content, strlen(test_content)));

  ASSERT_EQ(OB_SUCCESS, util.open_with_access_type(dev_handle, fd, &storage_info, uri, OB_STORAGE_ACCESS_READER));
  ASSERT_EQ(OB_INIT_TWICE, util.open_with_access_type(dev_handle, fd, &storage_info, uri, OB_STORAGE_ACCESS_READER));
  //in the old version, use a valid fd to open twice, fd will be closed automatic, this behavir has changed.
  //in such scenario, just return error. so here should success.
  //difference: OB_NOT_INIT -> OB_SUCCESS
  //ASSERT_EQ(OB_NOT_INIT, dev_handle->pread(fd, 0, sizeof(read_buf), read_buf, read_size));
  ASSERT_EQ(OB_SUCCESS, dev_handle->pread(fd, 0, sizeof(read_buf), read_buf, read_size));
  ASSERT_EQ(OB_SUCCESS, dev_handle->close(fd));

  ASSERT_EQ(OB_SUCCESS, util.open_with_access_type(dev_handle, fd_2, &storage_info, uri, OB_STORAGE_ACCESS_READER));
  ASSERT_EQ(OB_SUCCESS, dev_handle->pread(fd_2, 0, sizeof(read_buf), read_buf, read_size));
  ASSERT_EQ(strlen(test_content), read_size);
  ASSERT_EQ(0, strncmp(test_content, read_buf, read_size));

  ASSERT_EQ(OB_INVALID_ARGUMENT, dev_handle->pread(fd_2, read_size + 1, sizeof(read_buf), read_buf, read_size));
  ASSERT_EQ(OB_INVALID_ARGUMENT, dev_handle->pread(fd_2, -1, sizeof(read_buf), read_buf, read_size));
  ASSERT_EQ(OB_INVALID_ARGUMENT, dev_handle->pread(fd_2, -1, sizeof(read_buf), NULL, read_size));
  ASSERT_EQ(OB_INVALID_ARGUMENT, dev_handle->pread(fd_2, 0, 0, read_buf, read_size));

  ASSERT_EQ(OB_SUCCESS, dev_handle->close(fd_2));
  ASSERT_EQ(OB_NOT_INIT, dev_handle->close(fd_2));
}

TEST_F(TestStorageFile, test_writer)
{
  ObBackupIoAdapter util;
  char uri[OB_MAX_URI_LENGTH];
  ObBackupStorageInfo storage_info;
  storage_info.device_type_ = ObStorageType::OB_STORAGE_FILE;
  char test_content[OB_MAX_URI_LENGTH] = "just_for_test";
  int64_t write_size = -1;

  ObIODevice* dev_handle = NULL;
  ObIOFd fd;
  ObIOFd fd_2;
  ASSERT_EQ(OB_SUCCESS, util.mkdir(test_dir_uri_, &storage_info));
  ASSERT_EQ(OB_SUCCESS, databuff_printf(uri, sizeof(uri), "file://%s/test_file", test_dir_));

  ASSERT_EQ(OB_INVALID_BACKUP_DEST, util.open_with_access_type(dev_handle, fd, &storage_info, "bad://", OB_STORAGE_ACCESS_OVERWRITER));
  ASSERT_EQ(OB_SUCCESS, util.open_with_access_type(dev_handle, fd, &storage_info, uri, OB_STORAGE_ACCESS_OVERWRITER));
  ASSERT_EQ(OB_INIT_TWICE, util.open_with_access_type(dev_handle, fd, &storage_info, uri,  OB_STORAGE_ACCESS_OVERWRITER));
  ASSERT_EQ(OB_SUCCESS, dev_handle->close(fd));
  ASSERT_EQ(OB_SUCCESS, util.open_with_access_type(dev_handle, fd_2, &storage_info, uri, OB_STORAGE_ACCESS_OVERWRITER));
  ASSERT_EQ(OB_SUCCESS, dev_handle->write(fd_2, test_content, strlen(test_content), write_size));
  ASSERT_EQ(OB_SUCCESS, dev_handle->close(fd_2));
  ASSERT_EQ(OB_NOT_INIT,  dev_handle->close(fd_2));
}

TEST_F(TestStorageFile, test_file_writer_fail)
{
  ObBackupIoAdapter util;
  char uri[OB_MAX_URI_LENGTH];
  ObBackupStorageInfo storage_info;
  storage_info.device_type_ = ObStorageType::OB_STORAGE_FILE;
  char test_content[OB_MAX_URI_LENGTH] = "just_for_test";
  char test_content2[OB_MAX_URI_LENGTH] = "just_for_test2";
  int64_t write_size = -1;

  ObIODevice* dev_handle = NULL;
  ObIOFd fd;
  ObIOFd fd_2;
  ASSERT_EQ(OB_SUCCESS, util.mkdir(test_dir_uri_, &storage_info));
  ASSERT_EQ(OB_SUCCESS, databuff_printf(uri, sizeof(uri), "file://%s/test_file", test_dir_));

  ASSERT_EQ(OB_SUCCESS, util.open_with_access_type(dev_handle, fd, &storage_info, uri,  OB_STORAGE_ACCESS_OVERWRITER));
  ASSERT_EQ(OB_SUCCESS, dev_handle->write(fd, test_content, strlen(test_content) , write_size));
  ASSERT_EQ(OB_SUCCESS, dev_handle->close(fd));

  ASSERT_EQ(OB_SUCCESS, util.open_with_access_type(dev_handle, fd_2, &storage_info, uri,  OB_STORAGE_ACCESS_OVERWRITER));
  ASSERT_EQ(OB_SUCCESS, dev_handle->write(fd_2, test_content2, strlen(test_content2), write_size));
  ASSERT_EQ(OB_SUCCESS, dev_handle->close(fd_2));

  char read_buf[OB_MAX_URI_LENGTH];
  int64_t read_size = 0;
  ASSERT_EQ(OB_SUCCESS, util.read_single_file(uri, &storage_info,
      read_buf, sizeof(read_buf), read_size));
  COMMON_LOG(INFO, "dump buf", K(test_content), K(test_content2), K(read_buf));
  //comment by zhangyi, now we can not just used storage api to change has_error_;
  //ASSERT_EQ(0, MEMCMP(test_content, read_buf, read_size));
}

TEST_F(TestStorageFile, test_mkdir)
{
  ObBackupIoAdapter util;
  char uri[OB_MAX_URI_LENGTH];
  ObBackupStorageInfo storage_info;
  storage_info.device_type_ = ObStorageType::OB_STORAGE_FILE;
  bool is_exist = false;

  ASSERT_EQ(OB_SUCCESS, databuff_printf(uri, sizeof(uri), "file://%s/nfs/test/ob/7/1001/113415412451/0", test_dir_));
  ASSERT_EQ(OB_SUCCESS, util.mkdir(uri, &storage_info));
  ASSERT_EQ(OB_SUCCESS, util.is_exist(uri, &storage_info, is_exist));
  ASSERT_TRUE(is_exist);
  ASSERT_EQ(OB_SUCCESS, util.del_dir(uri, &storage_info));
}

TEST_F(TestStorageFile, test_parallel_mkdir)
{
  class MakedirTask: public ObDynamicThreadTask
  {
  public:
    int init(const char *dir)
    {
      EXPECT_FALSE(NULL == dir);
      int64_t timestamp = ObTimeUtility::current_time();
      EXPECT_EQ(OB_SUCCESS, databuff_printf(uri_, OB_MAX_URI_LENGTH,
          "file://%s/%ld/nfs/test/parallel/mkdir/1/2/3/4/5/6/7/8",
          dir, timestamp));
      return OB_SUCCESS;
    }
    int process(const bool &is_stop)
    {
      UNUSED(is_stop);
      ObBackupIoAdapter util;
      ObBackupStorageInfo storage_info;
      storage_info.device_type_ = ObStorageType::OB_STORAGE_FILE;
      bool is_exist = false;
      EXPECT_EQ(OB_SUCCESS, util.mkdir(uri_, &storage_info));
      EXPECT_EQ(OB_SUCCESS, util.is_exist(uri_, &storage_info, is_exist));
      EXPECT_TRUE(is_exist);
      return OB_SUCCESS;
    }
  private:
    char uri_[OB_MAX_URI_LENGTH];
  };

  ObDynamicThreadPool pool;
  const int64_t task_count = 30;
  ASSERT_EQ(OB_SUCCESS, pool.init());
  ASSERT_EQ(OB_SUCCESS, pool.set_task_thread_num(30));
  MakedirTask task;
  task.init(test_dir_);
  for (int64_t i = 0; i < task_count; ++i) {
    ASSERT_EQ(OB_SUCCESS, pool.add_task(&task));
  }
  sleep(10);
  ASSERT_EQ(0, pool.get_task_count());
  pool.stop();
  pool.destroy();
}

int main(int argc, char **argv)
{
  system("rm -f test_storage_file.log");
  OB_LOGGER.set_file_name("test_storage_file.log");
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
