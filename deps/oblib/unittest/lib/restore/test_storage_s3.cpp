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
#include "lib/allocator/page_arena.h"
#include "test_storage_s3.h"
#include "lib/container/ob_array_serialization.h"
#include <thread>

#include <aws/core/auth/AWSCredentials.h>

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
  const char *dir_name = "s3_unittest_dir";
  char uri[OB_MAX_URI_LENGTH];
  char dir_uri[OB_MAX_URI_LENGTH];
  ObObjectStorageInfo s3_base;

  int object_prefix_len = 5;
};

class TestStorageS3: public ::testing::Test, public TestStorageS3Common
{
public:
  TestStorageS3() : enable_test_(enable_test) {}
  virtual ~TestStorageS3(){}
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
    init_s3_env();
  }

  static void TearDownTestCase()
  {
    fin_s3_env();
  }

private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(TestStorageS3);
protected:
  bool enable_test_;
};

#define WRITE_SINGLE_FILE(file_suffix, file_content) \
  ASSERT_EQ(OB_SUCCESS, databuff_printf(uri, sizeof(uri), "%stest_write_file_%ld.back", dir_uri, file_suffix)); \
  ASSERT_EQ(OB_SUCCESS, writer.open(uri, &s3_base)); \
  const char write_content[] = file_content; \
  ASSERT_EQ(OB_SUCCESS, writer.write(write_content, strlen(write_content))); \
  ASSERT_EQ(OB_SUCCESS, writer.close()); \

TEST_F(TestStorageS3, test_basic_rw)
{
  std::vector<int> arr{1,2,3,4};
  auto it = std::upper_bound(arr.begin(), arr.end(), 10);
  std::cout << it - arr.begin() << std::endl;

  int ret = OB_SUCCESS;
  if (enable_test_) {
    ObStorageWriter writer;
    ObStorageReader reader;
    ObStorageUtil util;
    ASSERT_EQ(OB_SUCCESS, util.open(&s3_base));
    ASSERT_EQ(OB_SUCCESS, databuff_printf(dir_uri, sizeof(dir_uri), "%s/%s/", bucket, dir_name));

    // operate before open
    char tmp_buf[10] = {0};
    int64_t tmp_read_size = 0;
    ASSERT_EQ(OB_NOT_INIT, writer.write(tmp_buf, sizeof(tmp_buf)));
    ASSERT_EQ(OB_NOT_INIT, writer.close());
    ASSERT_EQ(OB_NOT_INIT, reader.pread(tmp_buf, sizeof(tmp_buf), 0, tmp_read_size));
    ASSERT_EQ(OB_NOT_INIT, reader.close());

    // wrong uri
    uri[0] = '\0';
    ASSERT_EQ(OB_INVALID_BACKUP_DEST, writer.open(uri, NULL));
    ASSERT_EQ(OB_NOT_INIT, writer.write(tmp_buf, sizeof(tmp_buf)));
    ASSERT_EQ(OB_NOT_INIT, writer.close());
    ASSERT_EQ(OB_INVALID_BACKUP_DEST, reader.open(uri, NULL));
    ASSERT_EQ(OB_NOT_INIT, reader.pread(tmp_buf, sizeof(tmp_buf), 0, tmp_read_size));
    ASSERT_EQ(OB_NOT_INIT, reader.close());

    ASSERT_EQ(OB_SUCCESS, databuff_printf(uri, sizeof(uri), "%stest_write_file_////", dir_uri));
    ASSERT_EQ(OB_INVALID_ARGUMENT, writer.open(uri, &s3_base));
    ASSERT_EQ(OB_NOT_INIT, writer.write(tmp_buf, sizeof(tmp_buf)));
    ASSERT_EQ(OB_NOT_INIT, writer.close());
    ASSERT_EQ(OB_INVALID_ARGUMENT, reader.open(uri, &s3_base));
    ASSERT_EQ(OB_NOT_INIT, reader.pread(tmp_buf, sizeof(tmp_buf), 0, tmp_read_size));
    ASSERT_EQ(OB_NOT_INIT, reader.close());

    const int64_t tmp_ts = ObTimeUtility::current_time();
    ASSERT_EQ(OB_SUCCESS, databuff_printf(uri, sizeof(uri), "%stest_write_file_%ld", dir_uri, tmp_ts));
    ASSERT_EQ(OB_SUCCESS, writer.open(uri, &s3_base));
    ASSERT_EQ(OB_SUCCESS, writer.close());

    // invalid storage info
    ASSERT_EQ(OB_INVALID_ARGUMENT, writer.open(uri, NULL));
    ASSERT_EQ(OB_NOT_INIT, writer.write(tmp_buf, sizeof(tmp_buf)));
    ASSERT_EQ(OB_NOT_INIT, writer.close());
    ASSERT_EQ(OB_INVALID_ARGUMENT, reader.open(uri, NULL));
    ASSERT_EQ(OB_NOT_INIT, reader.pread(tmp_buf, sizeof(tmp_buf), 0, tmp_read_size));
    ASSERT_EQ(OB_NOT_INIT, reader.close());

    {
      // rw empty object
      const int64_t ts = ObTimeUtility::current_time();
      WRITE_SINGLE_FILE(ts, "");

      ASSERT_EQ(OB_SUCCESS, reader.open(uri, &s3_base));
      ASSERT_EQ(strlen(write_content), reader.get_length());
      ASSERT_EQ(OB_SUCCESS, reader.close());
      ASSERT_EQ(OB_SUCCESS, util.del_file(uri));
    }

    {
      // ObStorageWriter writer;
      const int64_t ts = ObTimeUtility::current_time();
      WRITE_SINGLE_FILE(ts, "123456789");

      // ObStorageReader reader;
      ASSERT_EQ(OB_SUCCESS, reader.open(uri, &s3_base));
      ASSERT_EQ(strlen(write_content), reader.get_length());
      char read_buf[7] = {0};
      int64_t read_size = 0;
      ASSERT_EQ(OB_SUCCESS, reader.pread(read_buf, 7, 2, read_size));
      ASSERT_EQ('3', read_buf[0]);
      ASSERT_EQ('9', read_buf[6]);
      ASSERT_EQ(7, read_size);
      ASSERT_EQ(OB_SUCCESS, reader.close());

      ASSERT_EQ(OB_SUCCESS, util.del_file(uri));
    }

    {
      // ObStorageWriter writer;
      const int64_t ts = ObTimeUtility::current_time();
      WRITE_SINGLE_FILE(ts, "123456789ABCDEF");

      // ObStorageReader reader;
      ASSERT_EQ(OB_SUCCESS, reader.open(uri, &s3_base));
      ASSERT_EQ(strlen(write_content), reader.get_length());
      char read_buf[10] = {0};
      int64_t read_size = 0;
      ASSERT_EQ(OB_SUCCESS, reader.pread(read_buf, 10, 9, read_size));
      ASSERT_EQ('A', read_buf[0]);
      ASSERT_EQ('F', read_buf[5]);
      ASSERT_EQ(6, read_size);
      ASSERT_EQ(OB_SUCCESS, reader.close());

      ASSERT_EQ(OB_SUCCESS, util.del_file(uri));
    }

    {
      // read not exist object
      const int64_t ts = ObTimeUtility::current_time();
      ASSERT_EQ(OB_SUCCESS, databuff_printf(uri, sizeof(uri), "%snot_exist_%ld", dir_uri, ts));
      ASSERT_EQ(OB_BACKUP_FILE_NOT_EXIST, reader.open(uri, &s3_base));
      ASSERT_EQ(OB_NOT_INIT, reader.close());

      // open after fail
      WRITE_SINGLE_FILE(ts, "123456789ABCDEF");
      ASSERT_EQ(OB_SUCCESS, reader.open(uri, &s3_base));
      ASSERT_EQ(OB_SUCCESS, reader.close());
    }

    // open twice
    ASSERT_EQ(OB_SUCCESS, writer.open(uri, &s3_base));
    ASSERT_EQ(OB_INIT_TWICE, writer.open(uri, &s3_base));
    ASSERT_EQ(OB_NOT_INIT, writer.close());   // reader/writer will be closed if init twice
    ASSERT_EQ(OB_SUCCESS, reader.open(uri, &s3_base));
    ASSERT_EQ(OB_INIT_TWICE, reader.open(uri, &s3_base));
    ASSERT_EQ(OB_NOT_INIT, reader.close());
    ASSERT_EQ(OB_SUCCESS, util.del_file(uri));

    // operate after close
    ASSERT_EQ(OB_NOT_INIT, writer.write(tmp_buf, sizeof(tmp_buf)));
    ASSERT_EQ(OB_NOT_INIT, writer.close());
    ASSERT_EQ(OB_NOT_INIT, reader.pread(tmp_buf, sizeof(tmp_buf), 0, tmp_read_size));
    ASSERT_EQ(OB_NOT_INIT, reader.close());

    util.close();
  }
}

TEST_F(TestStorageS3, test_util)
{
  int ret = OB_SUCCESS;
  if (enable_test_) {
    ObStorageUtil util;
    const char *tmp_util_dir = "test_util";
    const int64_t ts = ObTimeUtility::current_time();
    ASSERT_EQ(OB_SUCCESS, databuff_printf(dir_uri, sizeof(dir_uri), "%s/%s/%s_%ld/",
      bucket, dir_name, tmp_util_dir, ts));
    ASSERT_EQ(OB_SUCCESS, util.open(&s3_base));

    ObStorageWriter writer;
    {
      WRITE_SINGLE_FILE(1L, "123456789ABC");

      bool is_obj_exist = false;
      ASSERT_EQ(OB_SUCCESS, util.is_exist(uri, is_obj_exist));
      ASSERT_TRUE(is_obj_exist);

      ASSERT_EQ(OB_SUCCESS, util.del_file(uri));
      ASSERT_EQ(OB_SUCCESS, util.is_exist(uri, is_obj_exist));
      ASSERT_FALSE(is_obj_exist);
    }
    {
      WRITE_SINGLE_FILE(2L, "123456789ABCDEF");

      int64_t file_length = 0;
      ASSERT_EQ(OB_SUCCESS, util.get_file_length(uri, file_length));
      ASSERT_EQ(strlen(write_content), file_length);
      ASSERT_EQ(OB_SUCCESS, util.del_file(uri));
    }

    // open twice
    ASSERT_EQ(OB_INIT_TWICE, util.open(&s3_base));
    util.close();

    // invalid storage info
    ASSERT_EQ(OB_INVALID_ARGUMENT, util.open(NULL));
  }
}

TEST_F(TestStorageS3, test_util_is_tagging)
{
  int ret = OB_SUCCESS;
  if (enable_test_) {
    ObStorageUtil util;
    const char *tmp_util_dir = "test_util_is_tagging";
    const int64_t ts = ObTimeUtility::current_time();
    ASSERT_EQ(OB_SUCCESS, databuff_printf(dir_uri, sizeof(dir_uri), "%s/%s/%s_%ld",
      bucket, dir_name, tmp_util_dir, ts));

    bool is_tagging = true;
    char tmp_account[OB_MAX_URI_LENGTH];
    ObObjectStorageInfo tmp_s3_base;
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
    ASSERT_EQ(OB_SUCCESS, util.open(&tmp_s3_base));
    ASSERT_EQ(OB_SUCCESS, util.write_single_file(uri, write_content, strlen(write_content)));

    ASSERT_EQ(OB_SUCCESS, util.is_tagging(uri, is_tagging));
    ASSERT_FALSE(is_tagging);

    ASSERT_EQ(OB_SUCCESS, util.del_file(uri));
    ASSERT_EQ(OB_BACKUP_FILE_NOT_EXIST, util.is_tagging(uri, is_tagging));
    tmp_s3_base.reset();
    util.close();

    // tagging mode
    ASSERT_EQ(OB_SUCCESS,
              databuff_printf(tmp_account, sizeof(tmp_account),
              "s3_region=%s&host=%s&access_id=%s&access_key=%s&delete_mode=tagging",
              region, endpoint, secretid, secretkey));
    ASSERT_EQ(OB_SUCCESS, tmp_s3_base.set(ObStorageType::OB_STORAGE_S3, tmp_account));

    ASSERT_EQ(OB_SUCCESS, databuff_printf(uri, sizeof(uri), "%s/tagging_mode", dir_uri));
    ASSERT_EQ(OB_SUCCESS, util.open(&tmp_s3_base));
    ASSERT_EQ(OB_SUCCESS, util.write_single_file(uri, write_content, strlen(write_content)));

    is_tagging = true;
    ASSERT_EQ(OB_SUCCESS, util.is_tagging(uri, is_tagging));
    ASSERT_FALSE(is_tagging);

    ASSERT_EQ(OB_SUCCESS, util.del_file(uri));
    ASSERT_EQ(OB_SUCCESS, util.is_tagging(uri, is_tagging));
    ASSERT_TRUE(is_tagging);
    tmp_s3_base.reset();
    util.close();

    // clean
    ASSERT_EQ(OB_SUCCESS,
              databuff_printf(tmp_account, sizeof(tmp_account),
              "s3_region=%s&host=%s&access_id=%s&access_key=%s",
              region, endpoint, secretid, secretkey));
    ASSERT_EQ(OB_SUCCESS, tmp_s3_base.set(ObStorageType::OB_STORAGE_S3, tmp_account));

    ASSERT_EQ(OB_SUCCESS, databuff_printf(uri, sizeof(uri), "%s/tagging_mode", dir_uri));
    ASSERT_EQ(OB_SUCCESS, util.open(&tmp_s3_base));
    ASSERT_EQ(OB_SUCCESS, util.del_file(uri));
    ASSERT_EQ(OB_BACKUP_FILE_NOT_EXIST, util.is_tagging(uri, is_tagging));
    util.close();
  }
}

class TestS3ListOp : public ObBaseDirEntryOperator
{
public:
  TestS3ListOp() {}
  virtual ~TestS3ListOp() {}
  int func(const dirent *entry) override;

  ObArray<char *> object_names_;
private :
  ObArenaAllocator allocator_;
};

int TestS3ListOp::func(const dirent *entry)
{
  int ret = OB_SUCCESS;
  OB_LOG(INFO, "=======");
  char *tmp_object_name = (char *)allocator_.alloc(strlen(entry->d_name) + 1);
  if (OB_ISNULL(tmp_object_name)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    OB_LOG(WARN,"fail to alloc buf for tmp object name", K(ret));
  } else {
    STRCPY(tmp_object_name, entry->d_name);
    object_names_.push_back(tmp_object_name);
    OB_LOG(DEBUG, "list entry", K(tmp_object_name));
  }
  return ret;
}

#define UTIL_WRITE_FILES(format, object_prefix_len, file_num, file_content) \
  for (int64_t file_idx = 0; file_idx < file_num; file_idx++) { \
    ASSERT_EQ(OB_SUCCESS, databuff_printf(uri, sizeof(uri), format, \
                                          dir_uri, object_prefix_len, file_idx, file_idx)); \
    ASSERT_EQ(OB_SUCCESS, util.write_single_file(uri, file_content, strlen(file_content))); \
  } \

#define UTIL_DELETE_FILES(format, object_prefix_len, file_num) \
  for (int64_t file_idx = 0; file_idx < file_num; file_idx++) { \
    ASSERT_EQ(OB_SUCCESS, databuff_printf(uri, sizeof(uri), format, \
                                          dir_uri, object_prefix_len, file_idx, file_idx)); \
    ASSERT_EQ(OB_SUCCESS, util.del_file(uri)); \
  } \

TEST_F(TestStorageS3, test_util_list_files)
{
  int ret = OB_SUCCESS;
  if (enable_test_) {
    ObStorageUtil util;
    const char *tmp_util_dir = "test_util_list_files";
    const int64_t ts = ObTimeUtility::current_time();
    ASSERT_EQ(OB_SUCCESS, databuff_printf(dir_uri, sizeof(dir_uri), "%s/%s/%s_%ld",
      bucket, dir_name, tmp_util_dir, ts));
    ASSERT_EQ(OB_SUCCESS, util.open(&s3_base));

    {
      // wrong uri
      uri[0] = '\0';
      TestS3ListOp op;
      ASSERT_EQ(OB_INVALID_BACKUP_DEST, util.list_files(uri, op));
    }

    int64_t file_num = 11;
    const char *write_content = "0123456789";

    // list objects
    {
      const char *format = "%s/%0*ld_%ld";
      UTIL_WRITE_FILES(format, object_prefix_len, file_num, write_content);

      // list and check and clean
      TestS3ListOp op;
      ASSERT_EQ(OB_SUCCESS, util.list_files(dir_uri, op));
      ASSERT_EQ(file_num, op.object_names_.size());
      for (int64_t i = 0; i < file_num; i++) {
        // listed files do not contain prefix
        ASSERT_EQ(OB_SUCCESS, databuff_printf(uri, sizeof(uri), "%0*ld_%ld", object_prefix_len, i, i));
        ASSERT_STREQ(uri, op.object_names_[i]);
      }
      UTIL_DELETE_FILES(format, object_prefix_len, file_num);
    }

    // list subfolders' objects
    {
      const char *format = "%s/%0*ld/%ld";
      UTIL_WRITE_FILES(format, object_prefix_len, file_num, write_content);

      // list and check and clean
      TestS3ListOp op;
      ASSERT_EQ(OB_SUCCESS, util.list_files(dir_uri, op));
      ASSERT_EQ(file_num, op.object_names_.size());
      for (int64_t i = 0; i < file_num; i++) {
        // listed files do not contain prefix
        ASSERT_EQ(OB_SUCCESS, databuff_printf(uri, sizeof(uri), "%0*ld/%ld", object_prefix_len, i, i));
        ASSERT_STREQ(uri, op.object_names_[i]);
      }
      UTIL_DELETE_FILES(format, object_prefix_len, file_num);
    }

    // list empty dir, now dir_uri should be empty after delete
    TestS3ListOp list_empty_op;
    ASSERT_EQ(OB_SUCCESS, util.list_files(dir_uri, list_empty_op));
    ASSERT_EQ(0, list_empty_op.object_names_.size());

    util.close();
  }
}

TEST_F(TestStorageS3, test_util_list_directories)
{
  int ret = OB_SUCCESS;
  if (enable_test_) {
    ObStorageUtil util;
    const char *tmp_util_dir = "test_util_list_directories";
    const int64_t ts = ObTimeUtility::current_time();
    ASSERT_EQ(OB_SUCCESS, databuff_printf(dir_uri, sizeof(dir_uri), "%s/%s/%s_%ld",
        bucket, dir_name, tmp_util_dir, ts));
    ASSERT_EQ(OB_SUCCESS, util.open(&s3_base));

    {
      // wrong uri
      uri[0] = '\0';
      TestS3ListOp op;
      ASSERT_EQ(OB_INVALID_ARGUMENT, util.list_directories(uri, op));
    }

    int64_t file_num = 11;
    const char *write_content = "0123456789";

    {
      const char *format = "%s/%0*ld_%ld";
      UTIL_WRITE_FILES(format, object_prefix_len, file_num, write_content);
      format = "%s/%0*ld/%ld";
      UTIL_WRITE_FILES(format, object_prefix_len, file_num, write_content);

      // list and check and clean
      TestS3ListOp op;
      ASSERT_EQ(OB_SUCCESS, util.list_directories(dir_uri, op));
      ASSERT_EQ(file_num, op.object_names_.size());
      for (int64_t i = 0; i < file_num; i++) {
        // listed directories do not contain prefix
        ASSERT_EQ(OB_SUCCESS, databuff_printf(uri, sizeof(uri), "%0*ld", object_prefix_len, i));
        ASSERT_STREQ(uri, op.object_names_[i]);
      }
      UTIL_DELETE_FILES(format, object_prefix_len, file_num);
      format = "%s/%0*ld_%ld";
      UTIL_DELETE_FILES(format, object_prefix_len, file_num);
    }

    // no sub directories
    {
      const char *format = "%s/%0*ld_%ld";
      UTIL_WRITE_FILES(format, object_prefix_len, file_num, write_content);

      // list and check and clean
      TestS3ListOp op;
      ASSERT_EQ(OB_SUCCESS, util.list_directories(dir_uri, op));
      ASSERT_EQ(0, op.object_names_.size());
      UTIL_DELETE_FILES(format, object_prefix_len, file_num);
    }

    util.close();
  }
}

// TEST_F(TestStorageS3, test_util_list_adaptive_files)
// {
//   int ret = OB_SUCCESS;
//   if (enable_test_) {
//     ObStorageUtil util;
//     const char *tmp_util_dir = "test_util_list_adaptive_files";
//     const int64_t ts = ObTimeUtility::current_time();
//     ASSERT_EQ(OB_SUCCESS, databuff_printf(dir_uri, sizeof(dir_uri), "%s/%s/%s_%ld",
//       bucket, dir_name, tmp_util_dir, ts));
//     ASSERT_EQ(OB_SUCCESS, util.open(&s3_base));

//     int64_t file_num = 11;
//     const char *write_content = "0123456789";

//     // list objects
//     {
//       const char *format = "%s/%0*ld_%ld";
//       UTIL_WRITE_FILES(format, object_prefix_len, file_num, write_content);
//       format = "%s/%0*ld/%ld";
//       UTIL_WRITE_FILES(format, object_prefix_len, file_num, write_content);

//       // list and check and clean
//       TestS3ListOp op;
//       ASSERT_EQ(OB_SUCCESS, util.list_adaptive_files(dir_uri, op));
//       ASSERT_EQ(file_num * 2, op.object_names_.size());
//       // for (int64_t i = 0; i < file_num; i++) {
//       //   // listed files do not contain prefix
//       //   ASSERT_EQ(OB_SUCCESS, databuff_printf(uri, sizeof(uri), "%0*ld_%ld", object_prefix_len, i, i));
//       //   ASSERT_STREQ(uri, op.object_names_[i]);
//       // }
//       format = "%s/%0*ld_%ld";
//       UTIL_DELETE_FILES(format, object_prefix_len, file_num);
//       format = "%s/%0*ld/%ld";
//       UTIL_DELETE_FILES(format, object_prefix_len, file_num);
//     }

//     // list empty dir, now dir_uri should be empty after delete
//     TestS3ListOp list_empty_op;
//     ASSERT_EQ(OB_SUCCESS, util.list_files(dir_uri, list_empty_op));
//     ASSERT_EQ(0, list_empty_op.object_names_.size());

//     util.close();
//   }
// }

void test_read_appendable_object(const char *content, const int64_t content_length,
    const int64_t n_run, const int64_t *read_start, const int64_t *read_end,
    ObStorageAdaptiveReader &reader)
{
  int64_t read_size = -1;
  int64_t expected_read_size = -2;
  char buf[content_length];
  for (int64_t i = 0; i < n_run; i++) {
    read_size = -1;
    expected_read_size = MIN(read_end[i], content_length) - MIN(read_start[i], content_length);
    ASSERT_TRUE(read_start[i] <= read_end[i]);
    ASSERT_EQ(OB_SUCCESS,
        reader.pread(buf, read_end[i] - read_start[i], read_start[i], read_size));
    ASSERT_EQ(expected_read_size, read_size);
    ASSERT_EQ(0, memcmp(buf, content + read_start[i], read_size));
  }
}

TEST_F(TestStorageS3, test_append_rw)
{
  const char *str = "012345678";
  char buf[5] = { 0 };
  int return_size = snprintf(buf, sizeof(buf), "%s", str);
  std::cout << "return size = " << return_size << std::endl;

  int ret = OB_SUCCESS;
  if (enable_test_) {
    ObStorageUtil util;
    ASSERT_EQ(OB_SUCCESS, util.open(&s3_base));
    const char *tmp_append_dir = "test_append";
    const int64_t ts = ObTimeUtility::current_time();
    ASSERT_EQ(OB_SUCCESS, databuff_printf(dir_uri, sizeof(dir_uri), "%s/%s/%s_%ld",
        bucket, dir_name, tmp_append_dir, ts));

    {
      ASSERT_EQ(OB_SUCCESS, databuff_printf(uri, sizeof(uri), "%s/test_append_file_%ld.back",
                                            dir_uri, ObTimeUtility::current_time()));

      ObStorageAppender appender;
      ASSERT_EQ(OB_SUCCESS, appender.open(uri, &s3_base));

      const int64_t content_length = 100;
      char content[content_length] = { 0 };
      for (int64_t i = 0; i < content_length; i++) {
        content[i] = '0' + (i % 10);
      }
      // "1-7", "2-5", "3-6", "4-7", "1-7", "1-7", "1-5",  // covered by "1-7"
      // "0-3",  "0-3", "0-1", "1-2", "2-3",               // covered "0-3"
      // "7-8", "8-9", "9-10", "10-11", "11-12", "12-20",  // no gap
      // "22-25" & "21-24" are covered by "15-25", and there is a gap from "15-25" to "26-30"
      // "15-25", "22-25", "21-24", "26-30", "30-100", "28-100"
      int64_t fragment_start[] = {1,2,3,4,1,1,1,0,0,0,1,2,7,8,9,10,11,12,15,22,21,26,30,28};
      int64_t fragment_end[] = {7,5,6,7,7,7,5,3,3,1,2,3,8,9,10,11,12,20,25,25,24,30,100,100};
      ASSERT_EQ(sizeof(fragment_start), sizeof(fragment_end));
      for (int64_t i = 0; i < sizeof(fragment_start) / sizeof(int64_t); i++) {
        ASSERT_TRUE(fragment_start[i] < fragment_end[i]);
        ASSERT_EQ(OB_SUCCESS, appender.pwrite(content + fragment_start[i],
            fragment_end[i] - fragment_start[i], fragment_start[i]));
      }
      ASSERT_EQ(content_length, appender.get_length());

      // read before close
      ObStorageAdaptiveReader reader;
      ASSERT_EQ(OB_SUCCESS, reader.open(uri, &s3_base));
      ASSERT_EQ(content_length, reader.get_length());
      int64_t read_start[] = {0, 1, 0, 4, 26, 30, 26, 0};
      int64_t read_end[] = {3, 4, 15, 25, 27, 100, 101, 0};
      ASSERT_EQ(sizeof(read_start), sizeof(read_end));
      test_read_appendable_object(content, content_length,
          sizeof(read_start) / sizeof(int64_t), read_start, read_end, reader);

      char buf[content_length];
      int64_t read_size = -1;
      ASSERT_EQ(OB_ERR_UNEXPECTED,
          reader.pread(buf, content_length, 0, read_size));
      ASSERT_EQ(content_length, reader.get_length());

      OB_LOG(INFO, "-=-=-=-=-===========-=-=====-=-=-=-=-=-=-=-=-=-=-=-=");
      ASSERT_EQ(OB_SUCCESS, appender.close());
      // open before close, read after close
      test_read_appendable_object(content, content_length,
          sizeof(read_start) / sizeof(int64_t), read_start, read_end, reader);
          ASSERT_EQ(content_length, reader.get_length());
      ASSERT_EQ(OB_SUCCESS, reader.close());
      OB_LOG(INFO, "-=-=-=-=-===========-=-=====-=-=-=-=-=-=-=-=-=-=-=-=");

      // open/read after close
      ASSERT_EQ(OB_SUCCESS, reader.open(uri, &s3_base));
      // ASSERT_EQ(content_length - 1, reader.get_appendable_object_size());
      test_read_appendable_object(content, content_length,
          sizeof(read_start) / sizeof(int64_t), read_start, read_end, reader);
      ASSERT_EQ(content_length, reader.get_length());

      // read after seal
      ASSERT_EQ(OB_SUCCESS, appender.open(uri, &s3_base));
      ASSERT_EQ(OB_SUCCESS, appender.seal_for_adaptive());
      test_read_appendable_object(content, content_length,
          sizeof(read_start) / sizeof(int64_t), read_start, read_end, reader);
          ASSERT_EQ(content_length, reader.get_length());
      ASSERT_EQ(OB_SUCCESS, reader.close());
      ASSERT_EQ(OB_SUCCESS, appender.close());

      // open/read after seal
      ASSERT_EQ(OB_SUCCESS, reader.open(uri, &s3_base));
      // ASSERT_EQ(content_length - 1, reader.get_appendable_object_size());
      test_read_appendable_object(content, content_length,
          sizeof(read_start) / sizeof(int64_t), read_start, read_end, reader);
      ASSERT_EQ(content_length, reader.get_length());
      ASSERT_EQ(OB_SUCCESS, reader.close());

      // ASSERT_EQ(OB_SUCCESS, util.del_appendable_file(uri));
    }

    {
      ASSERT_EQ(OB_SUCCESS, databuff_printf(uri, sizeof(uri), "%s/test_append_file_%ld.back",
                                            dir_uri, ObTimeUtility::current_time()));

      ObStorageAppender appender;
      ASSERT_EQ(OB_SUCCESS, appender.open(uri, &s3_base));

      const int64_t content_length = 100;
      char content[content_length] = { 0 };
      for (int64_t i = 0; i < content_length; i++) {
        content[i] = '0' + (i % 10);
      }
      // "1-7", "2-5", "3-6", "4-7", "1-7", "1-7", "1-5",  // covered by "1-7"
      // "0-3",  "0-3", "0-1", "1-2", "2-3",               // covered "0-3"
      // "7-8", "8-9", "9-10", "10-11", "11-12", "12-20",  // no gap
      // "22-25" & "21-24" are covered by "15-25", and there is a gap from "15-25" to "26-30"
      // "15-25", "22-25", "21-24", "26-30", "30-100", "28-100"
      int64_t fragment_start[] = {1,2,3,4,1,1,1,0,0,0,1,2,7,8,9,10,11,12,15,22,21,26,30,28};
      int64_t fragment_end[] = {7,5,6,7,7,7,5,3,3,1,2,3,8,9,10,11,12,20,25,25,24,30,100,100};
      ASSERT_EQ(sizeof(fragment_start), sizeof(fragment_end));
      for (int64_t i = 0; i < sizeof(fragment_start) / sizeof(int64_t); i++) {
        ASSERT_TRUE(fragment_start[i] < fragment_end[i]);
        ASSERT_EQ(OB_SUCCESS, appender.pwrite(content + fragment_start[i],
            fragment_end[i] - fragment_start[i], fragment_start[i]));
      }
      ASSERT_EQ(content_length, appender.get_length());

      // read before close
      ObStorageAdaptiveReader reader;
      ASSERT_EQ(OB_SUCCESS, reader.open(uri, &s3_base));
      ASSERT_EQ(content_length, reader.get_length());

      char buf[content_length];
      int64_t read_size = -1;
      ASSERT_EQ(OB_ERR_UNEXPECTED,
          reader.pread(buf, content_length, 0, read_size));
      ASSERT_EQ(OB_SUCCESS, reader.close());

      // appender not finished, use appendable put
      // ObStorageAdaptiveWriter writer;
      // ASSERT_EQ(OB_SUCCESS, writer.open(uri, &s3_base));
      // ASSERT_EQ(OB_SUCCESS, writer.write(content, content_length));
      // ASSERT_EQ(OB_SUCCESS, writer.close());

      // now gap is filled
      ASSERT_EQ(OB_SUCCESS, reader.open(uri, &s3_base));
      ASSERT_EQ(content_length, reader.get_length());
      ASSERT_EQ(OB_SUCCESS,
          reader.pread(buf, content_length, 0, read_size));
      ASSERT_EQ(OB_SUCCESS, reader.close());

      // test appendable put is valid
      ObStorageUtil s3_util;
      ObStorageObjectMeta appendable_obj_meta;
      ASSERT_EQ(OB_SUCCESS, s3_util.open(&s3_base));
      ASSERT_EQ(OB_SUCCESS, s3_util.list_appendable_file_fragments(uri, appendable_obj_meta));
      ASSERT_EQ(1, appendable_obj_meta.fragment_metas_.count());
      ASSERT_EQ(0, appendable_obj_meta.fragment_metas_[0].start_);
      ASSERT_EQ(9223372036854775807, appendable_obj_meta.fragment_metas_[0].end_);
      ASSERT_EQ(content_length, appendable_obj_meta.length_);

      ASSERT_EQ(OB_SUCCESS, appender.close());
      ASSERT_EQ(OB_SUCCESS, reader.open(uri, &s3_base));
      // open before close, read after close
      int64_t read_start[] = {0, 1, 0, 4, 26, 30, 26, 0};
      int64_t read_end[] = {3, 4, 15, 25, 27, 100, 101, 0};
      test_read_appendable_object(content, content_length,
          sizeof(read_start) / sizeof(int64_t), read_start, read_end, reader);
          ASSERT_EQ(content_length, reader.get_length());
      ASSERT_EQ(OB_SUCCESS, reader.close());

      // open/read after close
      ASSERT_EQ(OB_SUCCESS, reader.open(uri, &s3_base));
      // ASSERT_EQ(content_length - 1, reader.get_appendable_object_size());
      test_read_appendable_object(content, content_length,
          sizeof(read_start) / sizeof(int64_t), read_start, read_end, reader);
      ASSERT_EQ(content_length, reader.get_length());

      // read after seal
      ASSERT_EQ(OB_SUCCESS, appender.open(uri, &s3_base));
      ASSERT_EQ(OB_SUCCESS, appender.seal_for_adaptive());
      test_read_appendable_object(content, content_length,
          sizeof(read_start) / sizeof(int64_t), read_start, read_end, reader);
          ASSERT_EQ(content_length, reader.get_length());
      ASSERT_EQ(OB_SUCCESS, reader.close());
      ASSERT_EQ(OB_SUCCESS, appender.close());

      // open/read after seal
      ASSERT_EQ(OB_SUCCESS, reader.open(uri, &s3_base));
      // ASSERT_EQ(content_length - 1, reader.get_appendable_object_size());
      test_read_appendable_object(content, content_length,
          sizeof(read_start) / sizeof(int64_t), read_start, read_end, reader);
      ASSERT_EQ(content_length, reader.get_length());
      ASSERT_EQ(OB_SUCCESS, reader.close());

      // ASSERT_EQ(OB_SUCCESS, util.del_appendable_file(uri));
    }

    {
      ASSERT_EQ(OB_SUCCESS, databuff_printf(uri, sizeof(uri), "%s/test_append_file_%ld.back",
                                            dir_uri, ObTimeUtility::current_time()));

      ObStorageAppender appender_a;
      ObStorageAppender appender_b;
      ObStorageAppender appender_c;
      ASSERT_EQ(OB_SUCCESS, appender_a.open(uri, &s3_base));
      ASSERT_EQ(OB_SUCCESS, appender_b.open(uri, &s3_base));
      ASSERT_EQ(OB_SUCCESS, appender_c.open(uri, &s3_base));

      const int64_t content_length = 100;
      char content[content_length] = { 0 };
      for (int64_t i = 0; i < content_length; i++) {
        content[i] = '0' + (i % 10);
      }
      // "1-7", "2-5", "3-6", "4-7", "1-7", "1-7", "1-5",  // covered by "1-7"
      // "0-3",  "0-3", "0-1", "1-2", "2-3",               // covered "0-3"
      // "7-8", "8-9", "9-10", "10-11", "11-12", "12-20",  // no gap
      // "22-25" & "21-24" are covered by "15-25"
      // "15-25", "22-25", "21-24", "25-30", "30-100", "28-100"
      int64_t fragment_start[] = {1,2,3,4,1,1,1,0,0,0,1,2,7,8,9,10,11,12,15,22,21,25,30,28};
      int64_t fragment_end[] = {7,5,6,7,7,7,5,3,3,1,2,3,8,9,10,11,12,20,25,25,24,30,100,100};
      ASSERT_EQ(sizeof(fragment_start), sizeof(fragment_end));
      for (int64_t i = 0; i < sizeof(fragment_start) / sizeof(int64_t); i++) {
        ASSERT_TRUE(fragment_start[i] < fragment_end[i]);
        if (i % 3 == 0) {
          ASSERT_EQ(OB_SUCCESS, appender_a.pwrite(content + fragment_start[i],
            fragment_end[i] - fragment_start[i], fragment_start[i]));
        } else if (i % 3 == 1) {
          ASSERT_EQ(OB_SUCCESS, appender_b.pwrite(content + fragment_start[i],
            fragment_end[i] - fragment_start[i], fragment_start[i]));
        } else {
          ASSERT_EQ(OB_SUCCESS, appender_c.pwrite(content + fragment_start[i],
            fragment_end[i] - fragment_start[i], fragment_start[i]));
        }
      }

      // read before close
      ObStorageAdaptiveReader reader;
      ASSERT_EQ(OB_SUCCESS, reader.open(uri, &s3_base));
      int64_t read_start[] = {0, 1, 0, 4, 26, 30, 26, 0, 0};
      int64_t read_end[] = {3, 4, 15, 25, 27, 100, 101, 0, 100};
      ASSERT_EQ(sizeof(read_start), sizeof(read_end));
      int64_t n_run = sizeof(read_start) / sizeof(int64_t);
      std::thread read_thread_a(test_read_appendable_object, content, content_length,
                                n_run, read_start, read_end, std::ref(reader));
      std::thread read_thread_b(test_read_appendable_object, content, content_length,
                                n_run, read_start, read_end, std::ref(reader));
      read_thread_a.join();
      read_thread_b.join();
      ASSERT_EQ(content_length, reader.get_length());

      // open before close, read after close
      ASSERT_EQ(OB_SUCCESS, appender_a.close());
      ASSERT_EQ(OB_SUCCESS, appender_b.close());
      ASSERT_EQ(OB_SUCCESS, appender_c.close());
      std::thread read_thread_c(test_read_appendable_object, content, content_length,
                                n_run, read_start, read_end, std::ref(reader));
      std::thread read_thread_d(test_read_appendable_object, content, content_length,
                                n_run, read_start, read_end, std::ref(reader));
      read_thread_c.join();
      read_thread_d.join();
      ASSERT_EQ(content_length, reader.get_length());
      ASSERT_EQ(OB_SUCCESS, reader.close());

      // open/read after close
      ASSERT_EQ(OB_SUCCESS, reader.open(uri, &s3_base));
      // ASSERT_EQ(content_length, reader.get_appendable_object_size());
      std::thread read_thread_e(test_read_appendable_object, content, content_length,
                                n_run, read_start, read_end, std::ref(reader));
      std::thread read_thread_f(test_read_appendable_object, content, content_length,
                                n_run, read_start, read_end, std::ref(reader));
      read_thread_e.join();
      read_thread_f.join();
      ASSERT_EQ(content_length, reader.get_length());

      // read after seal
      ASSERT_EQ(OB_SUCCESS, appender_a.open(uri, &s3_base));
      ASSERT_EQ(OB_SUCCESS, appender_a.seal_for_adaptive());
      std::thread read_thread_g(test_read_appendable_object, content, content_length,
                                n_run, read_start, read_end, std::ref(reader));
      std::thread read_thread_h(test_read_appendable_object, content, content_length,
                                n_run, read_start, read_end, std::ref(reader));
      read_thread_g.join();
      read_thread_h.join();
      ASSERT_EQ(content_length, reader.get_length());
      ASSERT_EQ(OB_SUCCESS, reader.close());
      ASSERT_EQ(OB_SUCCESS, appender_a.close());

      // open/read after seal
      ASSERT_EQ(OB_SUCCESS, reader.open(uri, &s3_base));
      std::thread read_thread_i(test_read_appendable_object, content, content_length,
                                n_run, read_start, read_end, std::ref(reader));
      std::thread read_thread_j(test_read_appendable_object, content, content_length,
                                n_run, read_start, read_end, std::ref(reader));
      read_thread_i.join();
      read_thread_j.join();
      ASSERT_EQ(content_length, reader.get_length());
      ASSERT_EQ(OB_SUCCESS, reader.close());

      // ASSERT_EQ(OB_SUCCESS, util.del_appendable_file(uri));
    }
  }
}

void test_gen_object_meta(const char **fragments, const int64_t n_fragments,
    const int64_t n_remained_fragments, const int64_t *expected_start, const int64_t *expected_end,
    const int64_t expected_file_length, ObStorageObjectMeta &appendable_obj_meta)
{
  dirent entry;
  ListAppendableObjectFragmentOp op;
  appendable_obj_meta.reset();

  for (int64_t i = 0; i < n_fragments; i++) {
    ASSERT_TRUE(sizeof(entry.d_name) >= strlen(fragments[i]) + 1);
    STRCPY(entry.d_name, fragments[i]);
    // STRCPY(entry.d_name + strlen(entry.d_name), ".back");
    ASSERT_EQ(OB_SUCCESS, op.func(&entry));
  }

  ASSERT_EQ(OB_SUCCESS, op.gen_object_meta(appendable_obj_meta));
  OB_LOG(INFO, "--", K(appendable_obj_meta));
  ASSERT_EQ(expected_file_length, appendable_obj_meta.length_);
  ASSERT_EQ(n_remained_fragments, appendable_obj_meta.fragment_metas_.count());
  for (int64_t i = 0; i < n_remained_fragments; i++) {
    ASSERT_EQ(expected_start[i], appendable_obj_meta.fragment_metas_[i].start_);
    ASSERT_EQ(expected_end[i], appendable_obj_meta.fragment_metas_[i].end_);
  }
}

void test_get_needed_fragments(const int64_t start, const int64_t end,
    const int64_t n_expected_fragments, const int64_t *expected_start, const int64_t *expected_end,
    ObStorageObjectMeta &appendable_obj_meta)
{
  ObArray<ObAppendableFragmentMeta> fragments_need_to_read;
  ASSERT_EQ(OB_SUCCESS,
      appendable_obj_meta.get_needed_fragments(start, end, fragments_need_to_read));
  // OB_LOG(INFO, "*************************", K(start), K(end), K(n_expected_fragments), K(fragments_need_to_read), K(appendable_obj_meta));
  ASSERT_EQ(n_expected_fragments, fragments_need_to_read.count());
  for (int64_t i = 0; i < n_expected_fragments; i++) {
    ASSERT_EQ(expected_start[i], fragments_need_to_read[i].start_);
    ASSERT_EQ(expected_end[i], fragments_need_to_read[i].end_);
  }
}

TEST_F(TestStorageS3, test_appendable_object_util)
{
  int ret = OB_SUCCESS;
  if (enable_test_) {
    {
      ListAppendableObjectFragmentOp op;
      dirent entry;

      // meta file
      ASSERT_TRUE(sizeof(entry.d_name) >= sizeof(OB_S3_APPENDABLE_SEAL_META));
      STRCPY(entry.d_name, OB_S3_APPENDABLE_SEAL_META);
      ASSERT_EQ(OB_SUCCESS, op.func(&entry));

      // invalid fragment name
      const char *invalid_fragments[] = {"-2--1", "-1-1", "2--2", "3-2"};
      for (int64_t i = 0; i < sizeof(invalid_fragments) / sizeof(char *); i++) {
        ASSERT_TRUE(sizeof(entry.d_name) >= strlen(invalid_fragments[i]) + 1);
        STRCPY(entry.d_name, invalid_fragments[i]);
        ASSERT_EQ(OB_INVALID_ARGUMENT, op.func(&entry));
      }
    }

    {
      // empty appendable object
      const char *fragments[] = {};
      int64_t n_fragments = sizeof(fragments) / sizeof(char *);
      int64_t expected_start[] = {};
      int64_t expected_end[] = {};
      int64_t n_remained_fragments = sizeof(expected_start) / sizeof(int64_t);
      int64_t expected_file_length = 0;
      ObStorageObjectMeta appendable_obj_meta;
      test_gen_object_meta(
        fragments, n_fragments,
        n_remained_fragments, expected_start, expected_end,
        expected_file_length, appendable_obj_meta);

      ObArray<ObAppendableFragmentMeta> fragments_need_to_read;
      ASSERT_EQ(OB_INVALID_ARGUMENT,
          appendable_obj_meta.get_needed_fragments(-1, 1, fragments_need_to_read));
      ASSERT_EQ(OB_INVALID_ARGUMENT,
          appendable_obj_meta.get_needed_fragments(5, 4, fragments_need_to_read));
      ASSERT_EQ(OB_INVALID_ARGUMENT,
          appendable_obj_meta.get_needed_fragments(5, 5, fragments_need_to_read));
      ASSERT_EQ(OB_ERR_UNEXPECTED,
          appendable_obj_meta.get_needed_fragments(0, 1, fragments_need_to_read));
    }

    {
      // one fragment
      const char *fragments[] = {"10-20.back"};
      int64_t n_fragments = sizeof(fragments) / sizeof(char *);
      int64_t expected_start[] = {10};
      int64_t expected_end[] = {20};
      int64_t n_remained_fragments = sizeof(expected_start) / sizeof(int64_t);
      int64_t expected_file_length = 20;
      ObStorageObjectMeta appendable_obj_meta;
      test_gen_object_meta(
        fragments, n_fragments,
        n_remained_fragments, expected_start, expected_end,
        expected_file_length, appendable_obj_meta);

      ObArray<ObAppendableFragmentMeta> fragments_need_to_read;
      ASSERT_EQ(OB_ERR_UNEXPECTED,
          appendable_obj_meta.get_needed_fragments(5, 8, fragments_need_to_read));
      ASSERT_EQ(OB_ERR_UNEXPECTED,
          appendable_obj_meta.get_needed_fragments(5, 10, fragments_need_to_read));
      ASSERT_EQ(OB_ERR_UNEXPECTED,
          appendable_obj_meta.get_needed_fragments(5, 15, fragments_need_to_read));
      ASSERT_EQ(OB_INVALID_ARGUMENT,
          appendable_obj_meta.get_needed_fragments(11, 11, fragments_need_to_read));

      {
        int64_t start = 10;
        int64_t end = 18;
        int64_t expected_start[] = {10};
        int64_t expected_end[] = {20};
        int64_t n_expected_fragments = sizeof(expected_start) / sizeof(int64_t);
        test_get_needed_fragments(start, end,
            n_expected_fragments, expected_start, expected_end, appendable_obj_meta);
      }
      {
        int64_t start = 15;
        int64_t end = 18;
        int64_t expected_start[] = {10};
        int64_t expected_end[] = {20};
        int64_t n_expected_fragments = sizeof(expected_start) / sizeof(int64_t);
        test_get_needed_fragments(start, end,
            n_expected_fragments, expected_start, expected_end, appendable_obj_meta);
      }
      {
        int64_t start = 19;
        int64_t end = 20;
        int64_t expected_start[] = {10};
        int64_t expected_end[] = {20};
        int64_t n_expected_fragments = sizeof(expected_start) / sizeof(int64_t);
        test_get_needed_fragments(start, end,
            n_expected_fragments, expected_start, expected_end, appendable_obj_meta);
      }
      {
        int64_t start = 19;
        int64_t end = 21;
        int64_t expected_start[] = {10};
        int64_t expected_end[] = {20};
        int64_t n_expected_fragments = sizeof(expected_start) / sizeof(int64_t);
        test_get_needed_fragments(start, end,
            n_expected_fragments, expected_start, expected_end, appendable_obj_meta);
      }
      {
        int64_t start = 20;
        int64_t end = 21;
        int64_t expected_start[] = {};
        int64_t expected_end[] = {};
        int64_t n_expected_fragments = sizeof(expected_start) / sizeof(int64_t);
        test_get_needed_fragments(start, end,
            n_expected_fragments, expected_start, expected_end, appendable_obj_meta);
      }
    }

    {
      // valid fragment name
      const char *valid_fragments[] = {
          OB_S3_APPENDABLE_SEAL_META,
          "1-7", "2-5", "3-6", "4-7", "1-7", "1-7", "1-5",    // covered by "1-7"
          "0-3", "0-3", "0-1", "1-2", "2-3",                  // covered "0-3"
          "7-8", "8-9", "9-10", "10-11", "11-12", "12-20",    // no gap
          // "22-25" & "21-24" are covered by "15-25", and there is a gap from "15-25" to "26-30"
          "15-25", "22-25", "21-24", "26-30",
          "30-1234567", "10000-1234567", "28-1234566"
      };
      int64_t n_fragments = sizeof(valid_fragments) / sizeof(char *);
      int64_t expected_start[] = {0, 1, 7, 8, 9, 10, 11, 12, 15, 26, 30};
      int64_t expected_end[] = {3, 7, 8, 9, 10, 11, 12, 20, 25, 30, 1234567};
      int64_t n_remained_fragments = sizeof(expected_start) / sizeof(int64_t);
      int64_t expected_file_length = 1234567;
      ObStorageObjectMeta appendable_obj_meta;
      test_gen_object_meta(
          valid_fragments, n_fragments,
          n_remained_fragments, expected_start, expected_end,
          expected_file_length, appendable_obj_meta);

      {
        int64_t start = 0;
        int64_t end = 3;
        int64_t expected_start[] = {0};
        int64_t expected_end[] = {3};
        int64_t n_expected_fragments = sizeof(expected_start) / sizeof(int64_t);
        test_get_needed_fragments(start, end,
            n_expected_fragments, expected_start, expected_end, appendable_obj_meta);
      }
      {
        int64_t start = 1;
        int64_t end = 4;
        int64_t expected_start[] = {1};
        int64_t expected_end[] = {7};
        int64_t n_expected_fragments = sizeof(expected_start) / sizeof(int64_t);
        test_get_needed_fragments(start, end,
            n_expected_fragments, expected_start, expected_end, appendable_obj_meta);
      }
      {
        int64_t start = 0;
        int64_t end = 15;
        int64_t expected_start[] = {0, 1, 7, 8, 9, 10, 11, 12};
        int64_t expected_end[] = {3, 7, 8, 9, 10, 11, 12, 20};
        int64_t n_expected_fragments = sizeof(expected_start) / sizeof(int64_t);
        test_get_needed_fragments(start, end,
            n_expected_fragments, expected_start, expected_end, appendable_obj_meta);
      }
      {
        ObArray<ObAppendableFragmentMeta> fragments_need_to_read;
        ASSERT_EQ(OB_ERR_UNEXPECTED,
          appendable_obj_meta.get_needed_fragments(15, 30, fragments_need_to_read));
      }
      {
        int64_t start = 28;
        int64_t end = 2345678;
        int64_t expected_start[] = {26, 30};
        int64_t expected_end[] = {30, 1234567};
        int64_t n_expected_fragments = sizeof(expected_start) / sizeof(int64_t);
        test_get_needed_fragments(start, end,
            n_expected_fragments, expected_start, expected_end, appendable_obj_meta);
      }
    }
  }
}

int main(int argc, char **argv)
{
  system("rm -f test_storage_s3.log*");
  OB_LOGGER.set_file_name("test_storage_s3.log", true, true);
  OB_LOGGER.set_log_level("DEBUG");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}