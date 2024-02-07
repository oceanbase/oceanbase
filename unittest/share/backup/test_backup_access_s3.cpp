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
#include "lib/restore/ob_storage_s3_base.h"
#include "share/backup/ob_backup_io_adapter.h"

#include "lib/allocator/page_arena.h"
#include "test_backup_access_s3.h"

using namespace oceanbase::common;

class TestStorageS3Common {
public:
TestStorageS3Common() {}
~TestStorageS3Common() {}

void init()
{
  ASSERT_EQ(OB_SUCCESS,
            databuff_printf(account, sizeof(account),
            "s3_region=%s&host=%s&access_id=%s&access_key=%s",
            region, endpoint, secretid, secretkey));
  //build s3_base
  const ObString s3_storage_info(account);
  ASSERT_EQ(OB_SUCCESS, s3_base.set(ObStorageType::OB_STORAGE_S3, s3_storage_info.ptr()));
}
void destory()
{
}
protected:
  char account[OB_MAX_URI_LENGTH];
  const char *dir_name = "test_backup_io_adapter_access_s3_dir";
  char uri[OB_MAX_URI_LENGTH];
  char dir_uri[OB_MAX_URI_LENGTH];
  oceanbase::share::ObBackupStorageInfo s3_base;

  int object_prefix_len = 5;
};

class TestBackupIOAdapterAccessS3 : public ::testing::Test, public TestStorageS3Common
{
public:
  TestBackupIOAdapterAccessS3() : enable_test_(enable_test) {}
  virtual ~TestBackupIOAdapterAccessS3() {}
  virtual void SetUp()
  {
    init();
  }
  virtual void TearDown()
  {
    destory();
  }

  static void SetUpTestCase()
  {
  }

  static void TearDownTestCase()
  {
  }

private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(TestBackupIOAdapterAccessS3);
protected:
  bool enable_test_;
};

TEST_F(TestBackupIOAdapterAccessS3, test_basic_rw)
{
  int ret = OB_SUCCESS;
  if (enable_test_) {
    ObBackupIoAdapter adapter;
    const char *tmp_dir = "test_basic_rw";
    const int64_t ts = ObTimeUtility::current_time();
    ASSERT_EQ(OB_SUCCESS, databuff_printf(dir_uri, sizeof(dir_uri), "%s/%s/%s_%ld",
        bucket, dir_name, tmp_dir, ts));

    // write
    const char *write_content = "123456789ABCDEF";
    ASSERT_EQ(OB_SUCCESS, databuff_printf(uri, sizeof(uri), "%s/0", dir_uri));
    ASSERT_EQ(OB_SUCCESS,
        adapter.write_single_file(uri, &s3_base, write_content, strlen(write_content)));

    // read
    char read_buf[100] = {0};
    int64_t read_size = 0;
    ASSERT_EQ(OB_SUCCESS,
        adapter.read_single_file(uri, &s3_base, read_buf, sizeof(read_buf), read_size));
    ASSERT_STREQ(write_content, read_buf);
    ASSERT_EQ(strlen(write_content), read_size);

    ASSERT_EQ(OB_SUCCESS,
        adapter.read_part_file(uri, &s3_base, read_buf, sizeof(read_buf), 0, read_size));
    ASSERT_STREQ(write_content, read_buf);
    ASSERT_EQ(strlen(write_content), read_size);

    int64_t offset = 5;
    ASSERT_EQ(OB_SUCCESS,
        adapter.read_part_file(uri, &s3_base, read_buf, sizeof(read_buf), offset, read_size));
    ASSERT_EQ('6', read_buf[0]);
    ASSERT_EQ('F', read_buf[9]);
    ASSERT_EQ(strlen(write_content) - offset, read_size);

    offset = 6;
    ASSERT_EQ(OB_SUCCESS,
        adapter.read_part_file(uri, &s3_base, read_buf, 5, offset, read_size));
    ASSERT_EQ('7', read_buf[0]);
    ASSERT_EQ('B', read_buf[4]);
    ASSERT_EQ(5, read_size);

    // offset = strlen(write_content);
    // ASSERT_EQ(OB_COS_ERROR,
    //     adapter.read_part_file(uri, &s3_base, read_buf, sizeof(read_buf), offset, read_size));

    ASSERT_EQ(OB_SUCCESS, adapter.del_file(uri, &s3_base));
  }
}

TEST_F(TestBackupIOAdapterAccessS3, test_util)
{
  int ret = OB_SUCCESS;
  if (enable_test_) {
    ObBackupIoAdapter adapter;
    const char *tmp_dir = "test_util";
    const int64_t ts = ObTimeUtility::current_time();
    ASSERT_EQ(OB_SUCCESS, databuff_printf(dir_uri, sizeof(dir_uri), "%s/%s/%s_%ld",
        bucket, dir_name, tmp_dir, ts));
    ASSERT_EQ(OB_SUCCESS, databuff_printf(uri, sizeof(uri), "%s/0", dir_uri));

    bool is_obj_exist = true;
    ASSERT_EQ(OB_SUCCESS, adapter.is_exist(uri, &s3_base, is_obj_exist));
    ASSERT_FALSE(is_obj_exist);

    const char *write_content = "123456789ABCDEF";
    ASSERT_EQ(OB_SUCCESS,
        adapter.write_single_file(uri, &s3_base, write_content, strlen(write_content)));
    ASSERT_EQ(OB_SUCCESS, adapter.is_exist(uri, &s3_base, is_obj_exist));
    ASSERT_TRUE(is_obj_exist);

    int64_t file_length = -1;
    ASSERT_EQ(OB_SUCCESS, adapter.get_file_length(uri, &s3_base, file_length));
    ASSERT_EQ(strlen(write_content), file_length);

    ASSERT_EQ(OB_SUCCESS, adapter.del_file(uri, &s3_base));
    ASSERT_EQ(OB_SUCCESS, adapter.is_exist(uri, &s3_base, is_obj_exist));
    ASSERT_FALSE(is_obj_exist);
  }
}

TEST_F(TestBackupIOAdapterAccessS3, test_list_files)
{
  int ret = OB_SUCCESS;
  if (enable_test_) {
    ObBackupIoAdapter adapter;
    const char *tmp_dir = "test_list_files";
    const int64_t ts = ObTimeUtility::current_time();
    ASSERT_EQ(OB_SUCCESS, databuff_printf(dir_uri, sizeof(dir_uri), "%s/%s/%s_%ld",
        bucket, dir_name, tmp_dir, ts));

    int64_t file_num = 11;
    const char *write_content = "0123456789";
    const char *format = "%s/%0*ld_%ld";
    for (int64_t file_idx = 0; file_idx < file_num; file_idx++) {
      ASSERT_EQ(OB_SUCCESS,
          databuff_printf(uri, sizeof(uri), format, dir_uri, object_prefix_len, file_idx, file_idx));
      ASSERT_EQ(OB_SUCCESS,
          adapter.write_single_file(uri, &s3_base, write_content, strlen(write_content)));
    }

    ObArenaAllocator allocator;
    ObArray<ObString> name_array;
    ObFileListArrayOp op(name_array, allocator);
    ASSERT_EQ(OB_SUCCESS, adapter.list_files(dir_uri, &s3_base, op));
    ASSERT_EQ(file_num, name_array.size());
    for (int64_t file_idx = 0; file_idx < file_num; file_idx++) {
      // listed files do not contain prefix
      ASSERT_EQ(OB_SUCCESS,
          databuff_printf(uri, sizeof(uri), "%0*ld_%ld", object_prefix_len, file_idx, file_idx));
      ASSERT_STREQ(uri, name_array[file_idx].ptr());
      ASSERT_EQ(OB_SUCCESS,
          databuff_printf(uri, sizeof(uri), format, dir_uri, object_prefix_len, file_idx, file_idx));
      ASSERT_EQ(OB_SUCCESS, adapter.del_file(uri, &s3_base));
    }
  }
}

TEST_F(TestBackupIOAdapterAccessS3, test_list_directories)
{
  int ret = OB_SUCCESS;
  if (enable_test_) {
    ObBackupIoAdapter adapter;
    const char *tmp_dir = "test_list_directories";
    const int64_t ts = ObTimeUtility::current_time();
    ASSERT_EQ(OB_SUCCESS, databuff_printf(dir_uri, sizeof(dir_uri), "%s/%s/%s_%ld",
        bucket, dir_name, tmp_dir, ts));

    int64_t file_num = 11;
    const char *write_content = "0123456789";
    const char *format = "%s/%0*ld/%ld";
    for (int64_t file_idx = 0; file_idx < file_num; file_idx++) {
      ASSERT_EQ(OB_SUCCESS,
          databuff_printf(uri, sizeof(uri), format, dir_uri, object_prefix_len, file_idx, file_idx));
      ASSERT_EQ(OB_SUCCESS,
          adapter.write_single_file(uri, &s3_base, write_content, strlen(write_content)));
    }

    ObArenaAllocator allocator;
    ObArray<ObString> name_array;
    ObFileListArrayOp op(name_array, allocator);
    ASSERT_EQ(OB_SUCCESS, adapter.list_directories(dir_uri, &s3_base, op));
    ASSERT_EQ(file_num, name_array.size());
    for (int64_t file_idx = 0; file_idx < file_num; file_idx++) {
      // listed files do not contain prefix
      ASSERT_EQ(OB_SUCCESS, databuff_printf(uri, sizeof(uri), "%0*ld", object_prefix_len, file_idx));
      ASSERT_STREQ(uri, name_array[file_idx].ptr());
      ASSERT_EQ(OB_SUCCESS,
          databuff_printf(uri, sizeof(uri), format, dir_uri, object_prefix_len, file_idx, file_idx));
      ASSERT_EQ(OB_SUCCESS, adapter.del_file(uri, &s3_base));
    }
  }
}

TEST_F(TestBackupIOAdapterAccessS3, test_is_tagging)
{
  int ret = OB_SUCCESS;
  if (enable_test_) {
    ObBackupIoAdapter adapter;
    const char *tmp_util_dir = "test_util_is_tagging";
    const int64_t ts = ObTimeUtility::current_time();
    ASSERT_EQ(OB_SUCCESS, databuff_printf(dir_uri, sizeof(dir_uri), "%s/%s/%s_%ld",
      bucket, dir_name, tmp_util_dir, ts));

    bool is_tagging = true;
    char tmp_account[OB_MAX_URI_LENGTH];
    oceanbase::share::ObBackupStorageInfo tmp_s3_base;
    const char *write_content = "123456789ABCDEF";

    // wrong tag mode
    ASSERT_EQ(OB_SUCCESS,
              databuff_printf(tmp_account, sizeof(tmp_account),
              "s3_region=%s&host=%s&access_id=%s&access_key=%s&delete_mode=tag",
              region, endpoint, secretid, secretkey));
    ASSERT_EQ(OB_INVALID_ARGUMENT, tmp_s3_base.set(ObStorageType::OB_STORAGE_S3, tmp_account));
    tmp_s3_base.reset();

    ASSERT_EQ(OB_SUCCESS,
              databuff_printf(tmp_account, sizeof(tmp_account),
              "s3_region=%s&host=%s&access_id=%s&access_key=%s&delete_mode=delete_delete",
              region, endpoint, secretid, secretkey));
    ASSERT_EQ(OB_INVALID_ARGUMENT, tmp_s3_base.set(ObStorageType::OB_STORAGE_S3, tmp_account));
    tmp_s3_base.reset();

    // delete mode
    ASSERT_EQ(OB_SUCCESS,
              databuff_printf(tmp_account, sizeof(tmp_account),
              "s3_region=%s&host=%s&access_id=%s&access_key=%s&delete_mode=delete",
              region, endpoint, secretid, secretkey));
    ASSERT_EQ(OB_SUCCESS, tmp_s3_base.set(ObStorageType::OB_STORAGE_S3, tmp_account));

    ASSERT_EQ(OB_SUCCESS, databuff_printf(uri, sizeof(uri), "%s/delete_mode", dir_uri));
    ASSERT_EQ(OB_SUCCESS,
        adapter.write_single_file(uri, &tmp_s3_base, write_content, strlen(write_content)));
    ASSERT_EQ(OB_SUCCESS, adapter.is_tagging(uri, &tmp_s3_base, is_tagging));
    ASSERT_FALSE(is_tagging);

    ASSERT_EQ(OB_SUCCESS, adapter.del_file(uri, &tmp_s3_base));
    ASSERT_EQ(OB_BACKUP_FILE_NOT_EXIST, adapter.is_tagging(uri, &tmp_s3_base, is_tagging));
    tmp_s3_base.reset();

    // tagging mode
    ASSERT_EQ(OB_SUCCESS,
              databuff_printf(tmp_account, sizeof(tmp_account),
              "s3_region=%s&host=%s&access_id=%s&access_key=%s&delete_mode=tagging",
              region, endpoint, secretid, secretkey));
    ASSERT_EQ(OB_SUCCESS, tmp_s3_base.set(ObStorageType::OB_STORAGE_S3, tmp_account));

    ASSERT_EQ(OB_SUCCESS, databuff_printf(uri, sizeof(uri), "%s/tagging_mode", dir_uri));
    ASSERT_EQ(OB_SUCCESS,
        adapter.write_single_file(uri, &tmp_s3_base, write_content, strlen(write_content)));

    is_tagging = true;
    ASSERT_EQ(OB_SUCCESS, adapter.is_tagging(uri, &tmp_s3_base, is_tagging));
    ASSERT_FALSE(is_tagging);

    ASSERT_EQ(OB_SUCCESS, adapter.del_file(uri, &tmp_s3_base));
    ASSERT_EQ(OB_SUCCESS, adapter.is_tagging(uri, &tmp_s3_base, is_tagging));
    ASSERT_TRUE(is_tagging);
    tmp_s3_base.reset();

    // clean
    ASSERT_EQ(OB_SUCCESS,
              databuff_printf(tmp_account, sizeof(tmp_account),
              "s3_region=%s&host=%s&access_id=%s&access_key=%s",
              region, endpoint, secretid, secretkey));
    ASSERT_EQ(OB_SUCCESS, tmp_s3_base.set(ObStorageType::OB_STORAGE_S3, tmp_account));

    ASSERT_EQ(OB_SUCCESS, databuff_printf(uri, sizeof(uri), "%s/tagging_mode", dir_uri));
    ASSERT_EQ(OB_SUCCESS, adapter.del_file(uri, &tmp_s3_base));
    ASSERT_EQ(OB_BACKUP_FILE_NOT_EXIST, adapter.is_tagging(uri, &tmp_s3_base, is_tagging));
  }
}

int main(int argc, char **argv)
{
  system("rm -f test_backup_access_s3.log*");
  OB_LOGGER.set_file_name("test_backup_access_s3.log", true, true);
  OB_LOGGER.set_log_level("DEBUG");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}