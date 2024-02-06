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
#include "lib/utility/ob_test_util.h"
#include "lib/restore/ob_storage.h"
#include "lib/allocator/page_arena.h"
#include "share/backup/ob_backup_io_adapter.h"
#include "common/storage/ob_fd_simulator.h"
#include "share/ob_device_manager.h"

using namespace oceanbase::common;

class TestOssShareCommon
{
public:
  TestOssShareCommon(){
    databuff_printf(storage_infos, sizeof(storage_infos), "host=%s&access_id=%s&access_key=%s",
          oss_host_, oss_id_, oss_key_);
    storage_info.assign_ptr(storage_infos, strlen(storage_infos));
  }
  ~TestOssShareCommon(){}

protected:
  char test_oss_bucket_[OB_MAX_URI_LENGTH] = "oss://antsys-oceanbasebackup";
  // oss config
  char oss_host_[OB_MAX_URI_LENGTH] = "";
  char oss_id_[MAX_OSS_ID_LENGTH] = "";  //test need add id
  char oss_key_[MAX_OSS_KEY_LENGTH] = ""; //test need add key

  const char *dir_name = "test_ut_object_device";
  const char *dir_name_pk = "test_ut_object_device/pkeys";

  ObBackupIoAdapter util;
  char uri[OB_MAX_URI_LENGTH];
  char storage_infos[OB_MAX_URI_LENGTH];
  char dir_uri[OB_MAX_URI_LENGTH];

  ObString storage_info;

};

//use to clean the env
class TestOSSShareCommonCleanOp : public ObBaseDirEntryOperator, public TestOssShareCommon
{
public:
  TestOSSShareCommonCleanOp(const char* dir_name) : dir_name_(dir_name)
  {
  }
  ~TestOSSShareCommonCleanOp() {}
  int func(const dirent *entry) override;
private :
  const char* dir_name_;
};

int TestOSSShareCommonCleanOp::func(const dirent *entry)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(databuff_printf(uri, sizeof(uri), "%s/%s/%s", test_oss_bucket_, dir_name_, entry->d_name))) {
  } else if (OB_FAIL(util.del_file(uri, storage_info))) {
  }
  return ret;
}

class TestStorageOss: public ::testing::Test, public TestOssShareCommon
{
public:
  TestStorageOss() {}
  virtual ~TestStorageOss(){}
  virtual void SetUp()
  {
    TestOSSShareCommonCleanOp clean_op(dir_name);
    TestOSSShareCommonCleanOp clean_op_pk(dir_name_pk);
    ASSERT_EQ(OB_SUCCESS, databuff_printf(dir_uri, sizeof(dir_uri), "%s/%s/", test_oss_bucket_, dir_name));
    ASSERT_EQ(OB_SUCCESS, util.list_files(dir_uri, storage_info, clean_op));
    ASSERT_EQ(OB_SUCCESS, databuff_printf(dir_uri, sizeof(dir_uri), "%s/%s/", test_oss_bucket_, dir_name_pk));
    ASSERT_EQ(OB_SUCCESS, util.list_files(dir_uri, storage_info, clean_op));
  }
  virtual void TearDown()
  {
  }

  static void SetUpTestCase()
  {
  }

  static void TearDownTestCase()
  {
  }
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(TestStorageOss);
protected:
  // function members
protected:
  // oss://ob-unit-test

};

class TestListFileOss1 : public ObBaseDirEntryOperator
{
public:
  TestListFileOss1(ObArray <ObString> &file_names) : file_names_(file_names), file_cnt_(0) {}
  ~TestListFileOss1() {}
  int func(const dirent *entry) override;
  int64_t get_file_cnt() {return file_cnt_;}
private:
  ObArenaAllocator allocator_;
  ObArray <ObString> &file_names_;
  int64_t file_cnt_;
};

int TestListFileOss1::func(const dirent *entry)
{
  ObString file_name;
  int ret = OB_SUCCESS;
  if (OB_FAIL(ob_write_string(allocator_, ObString(entry->d_name) , file_name, true))) { 
  } else if (OB_FAIL(file_names_.push_back(file_name))) {
  }
  
  file_cnt_++;
  return ret;
}

TEST_F(TestStorageOss, test_del)
{
  char uri_del[OB_MAX_URI_LENGTH];
  char test_content[OB_MAX_URI_LENGTH] = "just_for_test";
  int64_t read_size = strlen(test_content);

  ASSERT_EQ(OB_SUCCESS, databuff_printf(uri, sizeof(uri), "%s/%s/test_file", test_oss_bucket_, dir_name));
  ASSERT_EQ(OB_SUCCESS, databuff_printf(dir_uri, sizeof(dir_uri), "%s/%s/", test_oss_bucket_, dir_name));
  //del all related file
  ObArray <ObString> file_names_del;
  TestListFileOss1 op_del(file_names_del);
  ASSERT_EQ(OB_SUCCESS, util.list_files(dir_uri, storage_info, op_del));
  for (int64_t i = 0; i < file_names_del.count(); ++i) {
    ASSERT_EQ(OB_SUCCESS, databuff_printf(uri_del, sizeof(uri_del), "%s/%s/%s", test_oss_bucket_, dir_name, file_names_del.at(i).ptr()));
    util.del_file(uri_del, storage_info);
    STORAGE_LOG(INFO, "del file ", K(uri_del));
  }
  
  ASSERT_EQ(OB_SUCCESS, util.write_single_file(uri, storage_info, test_content, strlen(test_content)));
  ASSERT_EQ(OB_SUCCESS, util.get_file_length(uri, storage_info, read_size));
  
  ObArray <ObString> file_names;
  TestListFileOss1 op(file_names);
  ASSERT_EQ(OB_SUCCESS, util.list_files(dir_uri, storage_info, op));
  for (int64_t i = 0; i < file_names.count(); ++i) {
    STORAGE_LOG(INFO, "dump", K(read_size), K(i), K(file_names.at(i)));
  }
  bool is_exist = false;
  util.is_exist(uri, storage_info, is_exist);
  ASSERT_EQ(true, is_exist);
  ASSERT_EQ(1, op.get_file_cnt());
  ASSERT_EQ(1, file_names.count());
  //clean the env
  ASSERT_EQ(OB_SUCCESS, util.del_file(uri, storage_info));
  ObArray <ObString> file_names_2;
  TestListFileOss1 op2(file_names_2);
  ASSERT_EQ(OB_SUCCESS, util.list_files(dir_uri, storage_info, op2));
  ASSERT_EQ(0, file_names_2.count());
}

TEST_F(TestStorageOss, test_get_file_size)
{
  ObIODevice* device_handle = NULL;
  ObIOFd fd;
  ObIOFd fd_reader;
  ObBackupIoAdapter util;

  ASSERT_EQ(OB_SUCCESS, databuff_printf(uri, sizeof(uri), "%s/%s/test_file", test_oss_bucket_, dir_name));
  char test_content[OB_MAX_URI_LENGTH] = "just_for_test";
  char test_read_content[OB_MAX_URI_LENGTH];
  int64_t content_len = strlen(test_content);
  int64_t max_repeat_times = OB_MAX_URI_LENGTH/content_len;
  int64_t write_size = 0;
  int64_t read_size = 0;
  int64_t file_length = -1;
  ObIODFileStat stat_info;

  ASSERT_EQ(OB_SUCCESS, util.get_and_init_device(device_handle, storage_info, uri));
  //open with append mode
  ASSERT_EQ(OB_SUCCESS, util.open_with_access_type(device_handle, fd, storage_info, uri, OB_STORAGE_ACCESS_APPENDER));
  for (int i = 0; i < max_repeat_times; i++ ) {
    ASSERT_EQ(OB_SUCCESS, device_handle->write(fd, test_content, content_len, write_size));
  }
  ASSERT_EQ(OB_SUCCESS, device_handle->stat(uri, stat_info));
  ASSERT_EQ(stat_info.size_, max_repeat_times*content_len);
  ASSERT_EQ(OB_SUCCESS, device_handle->close(fd));
  ASSERT_EQ(OB_SUCCESS, util.open_with_access_type(device_handle, fd_reader, storage_info, uri, OB_STORAGE_ACCESS_READER));
  ASSERT_EQ(OB_SUCCESS, device_handle->pread(fd_reader, 0, OB_MAX_URI_LENGTH, test_read_content, read_size));
  ASSERT_EQ(read_size, max_repeat_times*content_len);
  ASSERT_EQ(OB_SUCCESS, device_handle->close(fd_reader));

  //open with overwrite moed
  ASSERT_EQ(OB_SUCCESS, util.open_with_access_type(device_handle, fd, storage_info, uri, OB_STORAGE_ACCESS_OVERWRITER));
  for (int i = 0; i < max_repeat_times; i++ ) {
    ASSERT_EQ(OB_SUCCESS, device_handle->write(fd, test_content, content_len, write_size));
  }
  ASSERT_EQ(OB_SUCCESS, device_handle->stat(uri, stat_info));
  ASSERT_EQ(stat_info.size_, content_len);
  //fd, 0, buf_size, buf, read_size
  ASSERT_EQ(OB_SUCCESS, device_handle->close(fd));
  ASSERT_EQ(OB_SUCCESS, util.open_with_access_type(device_handle, fd_reader, storage_info, uri, OB_STORAGE_ACCESS_READER));
  ASSERT_EQ(OB_SUCCESS, device_handle->pread(fd_reader, 0, OB_MAX_URI_LENGTH, test_read_content, read_size));
  ASSERT_EQ(read_size, content_len);
  ASSERT_EQ(OB_SUCCESS, device_handle->close(fd_reader));

  ObDeviceManager::get_instance().release_device(device_handle);
}

int main(int argc, char **argv)
{
  system("rm -f test_storage_oss_adapter.log");
  OB_LOGGER.set_file_name("test_storage_oss_adapter.log");
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
