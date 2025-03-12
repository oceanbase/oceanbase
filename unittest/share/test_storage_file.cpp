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
#undef protected

#include <gtest/gtest.h>
#include "share/backup/ob_backup_io_adapter.h"
#include "share/ob_device_manager.h"
#include "share/io/ob_io_manager.h"

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
    ObTenantBase *tenant_base = new ObTenantBase(OB_SYS_TENANT_ID);
    auto malloc = ObMallocAllocator::get_instance();
    if (NULL == malloc->get_tenant_ctx_allocator(OB_SYS_TENANT_ID, 0)) {
      malloc->create_and_add_tenant_allocator(OB_SYS_TENANT_ID);
    }
    tenant_base->init();
    ObTenantEnv::set_tenant(tenant_base);
    ASSERT_EQ(OB_SUCCESS, ObDeviceManager::get_instance().init_devices_env());
    ASSERT_EQ(OB_SUCCESS, ObIOManager::get_instance().init());
    ASSERT_EQ(OB_SUCCESS, ObIOManager::get_instance().start());
    ObTenantIOManager *io_service = nullptr;
    EXPECT_EQ(OB_SUCCESS, ObTenantIOManager::mtl_new(io_service));
    EXPECT_EQ(OB_SUCCESS, ObTenantIOManager::mtl_init(io_service));
    EXPECT_EQ(OB_SUCCESS, io_service->start());
    tenant_base->set(io_service);
    ObTenantEnv::set_tenant(tenant_base);
  }

  static void TearDownTestCase()
  {
    ObIOManager::get_instance().stop();
    ObIOManager::get_instance().destroy();
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
  ASSERT_EQ(OB_SUCCESS, util.write_single_file(uri, &storage_info, test_content, strlen(test_content),
                                               ObStorageIdMod::get_default_id_mod()));
  ASSERT_EQ(OB_SUCCESS, util.get_file_length(uri, &storage_info, read_size));
  ASSERT_EQ(strlen(test_content), read_size);
  ASSERT_EQ(OB_SUCCESS, util.is_exist(uri, &storage_info, is_exist));
  ASSERT_TRUE(is_exist);

  ASSERT_EQ(OB_SUCCESS, util.is_empty_directory(test_dir_uri_, &storage_info, is_empty_dir));
  ASSERT_FALSE(is_empty_dir);

  ASSERT_EQ(OB_SUCCESS, util.del_file(uri, &storage_info));
  ASSERT_EQ(OB_SUCCESS, util.is_exist(uri, &storage_info, is_exist));
  ASSERT_FALSE(is_exist);

  ASSERT_EQ(OB_SUCCESS, util.write_single_file(uri, &storage_info, test_content, strlen(test_content),
                                               ObStorageIdMod::get_default_id_mod()));
  ASSERT_EQ(OB_SUCCESS, util.read_single_file(uri, &storage_info, read_buf, sizeof(read_buf), read_size,
                                              ObStorageIdMod::get_default_id_mod()));
  ASSERT_EQ(strlen(test_content), read_size);
  ASSERT_EQ(0, strncmp(test_content, read_buf, read_size));
  ASSERT_EQ(OB_SUCCESS, util.read_single_text_file(uri, &storage_info, read_buf, sizeof(read_buf),
                                                   ObStorageIdMod::get_default_id_mod()));
  ASSERT_EQ(strlen(test_content), strlen(read_buf));
  ASSERT_EQ(0, strcmp(test_content, read_buf));

  ASSERT_EQ(OB_INVALID_BACKUP_DEST, util.is_exist("bad://", &storage_info, is_exist));
  ASSERT_EQ(OB_INVALID_BACKUP_DEST, util.get_file_length("bad://", &storage_info, read_size));
  ASSERT_EQ(OB_INVALID_BACKUP_DEST, util.del_file("bad://", &storage_info));
  ASSERT_EQ(OB_INVALID_BACKUP_DEST, util.read_single_file("bad://", &storage_info, read_buf,
                                    sizeof(read_buf), read_size, ObStorageIdMod::get_default_id_mod()));
  ASSERT_EQ(OB_INVALID_BACKUP_DEST, util.read_single_text_file("bad://", &storage_info, read_buf,
                                    sizeof(read_buf), ObStorageIdMod::get_default_id_mod()));
  ASSERT_EQ(OB_INVALID_BACKUP_DEST, util.write_single_file("bad://", &storage_info, test_content,
                                    strlen(test_content), ObStorageIdMod::get_default_id_mod()));
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

  ASSERT_EQ(OB_INVALID_BACKUP_DEST, util.open_with_access_type(dev_handle, fd, &storage_info, "bad://",  OB_STORAGE_ACCESS_READER,
                                                               ObStorageIdMod::get_default_id_mod()));
  ASSERT_EQ(OB_OBJECT_NOT_EXIST, util.open_with_access_type(dev_handle, fd, &storage_info, uri, OB_STORAGE_ACCESS_READER,
                                                            ObStorageIdMod::get_default_id_mod()));
  ASSERT_EQ(OB_SUCCESS, util.write_single_file(uri, &storage_info, test_content, strlen(test_content),
                                               ObStorageIdMod::get_default_id_mod()));

  ASSERT_EQ(OB_SUCCESS, util.open_with_access_type(dev_handle, fd, &storage_info, uri, OB_STORAGE_ACCESS_READER,
                                                   ObStorageIdMod::get_default_id_mod()));
  ASSERT_EQ(OB_INIT_TWICE, util.open_with_access_type(dev_handle, fd, &storage_info, uri, OB_STORAGE_ACCESS_READER,
                                                      ObStorageIdMod::get_default_id_mod()));
  //in the old version, use a valid fd to open twice, fd will be closed automatic, this behavir has changed.
  //in such scenario, just return error. so here should success.
  //difference: OB_NOT_INIT -> OB_SUCCESS
  //ASSERT_EQ(OB_NOT_INIT, dev_handle->pread(fd, 0, sizeof(read_buf), read_buf, read_size));
  ASSERT_EQ(OB_SUCCESS, dev_handle->pread(fd, 0, sizeof(read_buf), read_buf, read_size));
  ASSERT_EQ(OB_SUCCESS, dev_handle->close(fd));

  ASSERT_EQ(OB_SUCCESS, util.open_with_access_type(dev_handle, fd_2, &storage_info, uri, OB_STORAGE_ACCESS_READER,
                                                   ObStorageIdMod::get_default_id_mod()));
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

  ASSERT_EQ(OB_INVALID_BACKUP_DEST, util.open_with_access_type(dev_handle, fd, &storage_info, "bad://", OB_STORAGE_ACCESS_OVERWRITER,
                                                               ObStorageIdMod::get_default_id_mod()));
  ASSERT_EQ(OB_SUCCESS, util.open_with_access_type(dev_handle, fd, &storage_info, uri, OB_STORAGE_ACCESS_OVERWRITER,
                                                   ObStorageIdMod::get_default_id_mod()));
  ASSERT_EQ(OB_INIT_TWICE, util.open_with_access_type(dev_handle, fd, &storage_info, uri,  OB_STORAGE_ACCESS_OVERWRITER,
                                                      ObStorageIdMod::get_default_id_mod()));
  ASSERT_EQ(OB_SUCCESS, dev_handle->close(fd));
  ASSERT_EQ(OB_SUCCESS, util.open_with_access_type(dev_handle, fd_2, &storage_info, uri, OB_STORAGE_ACCESS_OVERWRITER,
                                                   ObStorageIdMod::get_default_id_mod()));
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

  ASSERT_EQ(OB_SUCCESS, util.open_with_access_type(dev_handle, fd, &storage_info, uri,  OB_STORAGE_ACCESS_OVERWRITER,
                                                   ObStorageIdMod::get_default_id_mod()));
  ASSERT_EQ(OB_SUCCESS, dev_handle->write(fd, test_content, strlen(test_content) , write_size));
  ASSERT_EQ(OB_SUCCESS, dev_handle->close(fd));

  ASSERT_EQ(OB_SUCCESS, util.open_with_access_type(dev_handle, fd_2, &storage_info, uri,  OB_STORAGE_ACCESS_OVERWRITER,
                                                   ObStorageIdMod::get_default_id_mod()));
  ASSERT_EQ(OB_SUCCESS, dev_handle->write(fd_2, test_content2, strlen(test_content2), write_size));
  ASSERT_EQ(OB_SUCCESS, dev_handle->close(fd_2));

  char read_buf[OB_MAX_URI_LENGTH];
  int64_t read_size = 0;
  ASSERT_EQ(OB_SUCCESS, util.read_single_file(uri, &storage_info,
      read_buf, sizeof(read_buf), read_size, ObStorageIdMod::get_default_id_mod()));
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

TEST_F(TestStorageFile, test_list_files_with_marker)
{
  class ObTestListFilesWithMarker : public ObBaseDirEntryOperator {
  public:
    ObTestListFilesWithMarker() : ObBaseDirEntryOperator(), global_objects_(0){}
    int func(const dirent *entry) final
    {
      global_objects_++;
      uris_.push_back(entry->d_name);
      return OB_SUCCESS;
    };
    int64_t global_objects_;
    std::vector<std::string> uris_;
  };
  ObBackupIoAdapter util;
  ObBackupStorageInfo storage_info;
  storage_info.device_type_ = ObStorageType::OB_STORAGE_FILE;
  int64_t write_size = -1;
  std::string test_dir_uri_root_str = std::string(test_dir_uri_);
  std::string test_dir_uri_str = test_dir_uri_root_str + "/" + "list_files_with_marker";
  ObString test_dir_uri(test_dir_uri_str.c_str());
  EXPECT_EQ(OB_SUCCESS, util.mkdir(test_dir_uri, &storage_info));
  bool mkdir_success = false;
  EXPECT_EQ(OB_SUCCESS, util.is_exist(test_dir_uri, &storage_info, mkdir_success));
  const int64_t scan_count = 10;
  ObTestListFilesWithMarker op;
  ASSERT_EQ(OB_SUCCESS, op.set_marker_flag("", scan_count));
  EXPECT_EQ(OB_SUCCESS, util.adaptively_list_files(test_dir_uri, &storage_info, op));
  EXPECT_EQ(0, op.global_objects_);
  EXPECT_EQ(0, op.uris_.size());
  char uri[OB_MAX_URI_LENGTH];
  char test_content[OB_MAX_URI_LENGTH];
  memset(test_content, 'c', sizeof(test_content));
  {
    const int64_t file_count = 20;
    for (int i = 0; i < file_count; i++) {
      EXPECT_EQ(OB_SUCCESS, databuff_printf(uri, OB_MAX_URI_LENGTH, "%s/%012d", test_dir_uri_str.c_str(), i));
      EXPECT_EQ(OB_SUCCESS, util.write_single_file(uri, &storage_info, test_content, strlen(test_content),
                                                   ObStorageIdMod::get_default_id_mod()));
    }
    op.global_objects_ = 0;
    op.uris_.clear();
    std::string test_with_marker_str = test_dir_uri_str+"/";
    ASSERT_EQ(OB_SUCCESS, op.set_marker_flag("0", scan_count));
    EXPECT_EQ(OB_SUCCESS, util.adaptively_list_files(test_with_marker_str.c_str(), &storage_info, op));
    EXPECT_EQ(scan_count, op.global_objects_);
    // 输出文件序与对象存储保持一致，字典序升序
    int j = 0;
    for (int i = 0; i < scan_count; i++) {
      char expected_uri_c[OB_MAX_URI_LENGTH] = {'\0'};
      EXPECT_EQ(OB_SUCCESS, databuff_printf(expected_uri_c, OB_MAX_URI_LENGTH, "%012d", i));
      std::string expected_uri(expected_uri_c);
      EXPECT_EQ(expected_uri, op.uris_[j++]);
    }
  }
}

int main(int argc, char **argv)
{
  system("rm -f test_storage_file.log");
  OB_LOGGER.set_file_name("test_storage_file.log");
  OB_LOGGER.set_log_level("TRACE");
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
