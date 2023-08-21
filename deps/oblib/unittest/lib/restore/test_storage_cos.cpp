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
#include "lib/restore/ob_storage_cos_base.h"
#include "lib/allocator/page_arena.h"
#include "test_storage_cos.h"

using namespace oceanbase::common;

class TestStorageCosCommon {
public:
TestStorageCosCommon() {}
~TestStorageCosCommon() {}

void init()
{
  init_cos_env();
  ASSERT_EQ(OB_SUCCESS,
            databuff_printf(account, sizeof(account),
            "host=%s&access_id=%s&access_key=%s&appid=%s",
            endpoint, secretid, secretkey, appid));
  //build cos_base
  const ObString cos_storage_info(account);
  ASSERT_EQ(OB_SUCCESS, cos_base.set(ObStorageType::OB_STORAGE_COS, cos_storage_info.ptr()));
}
void destory()
{
  fin_cos_env();
}
protected:
  char account[OB_MAX_URI_LENGTH];
  const char *dir_name = "cos_unittest_dir";
  char uri[OB_MAX_URI_LENGTH];
  char dir_uri[OB_MAX_URI_LENGTH];
  ObObjectStorageInfo cos_base;

  int object_prefix_len = 5;
};

//use to clean the env
class TestCosCleanOp : public ObBaseDirEntryOperator, public TestStorageCosCommon
{
public:
  TestCosCleanOp()
  {
  }
  ~TestCosCleanOp() {}
  int func(const dirent *entry) override;
private :
};

int TestCosCleanOp::func(const dirent *entry)
{
  int ret = OB_SUCCESS;
  ObStorageCosUtil util;
  if (OB_FAIL(util.open(&cos_base))) {
  } else if (OB_FAIL(databuff_printf(uri, sizeof(uri), "%s/%s/%s", bucket, dir_name, entry->d_name))) {
  } else if (OB_FAIL(util.del_file(uri))) {
  }
  util.close();

  return ret;
}

class TestStorageCos: public ::testing::Test, public TestStorageCosCommon
{
public:
  TestStorageCos() : enable_test_(enable_test) {}
  virtual ~TestStorageCos(){}
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
  DISALLOW_COPY_AND_ASSIGN(TestStorageCos);
protected:
  bool enable_test_;
};

TEST_F(TestStorageCos, test_append)
{
  int ret = OB_SUCCESS;
  if (enable_test_) {
    ObStorageAppender appender;
    ObStorageReader reader;
    ObStorageUtil util;
    ASSERT_EQ(OB_SUCCESS, util.open(&cos_base));
    const char *tmp_append_dir = "test_append";
    const int64_t ts = ObTimeUtility::current_time();
    ASSERT_EQ(OB_SUCCESS, databuff_printf(dir_uri, sizeof(dir_uri), "%s/%s/%s_%ld",
        bucket, dir_name, tmp_append_dir, ts));

    ASSERT_EQ(OB_SUCCESS, databuff_printf(uri, sizeof(uri), "%s/test_append_file", dir_uri));

    // operate before open
    char tmp_buf[10] = {0};
    ASSERT_FALSE(appender.is_opened());
    ASSERT_EQ(OB_NOT_INIT, appender.write(tmp_buf, sizeof(tmp_buf)));
    ASSERT_EQ(OB_NOT_INIT, appender.pwrite(tmp_buf, sizeof(tmp_buf), 0));
    ASSERT_EQ(OB_NOT_INIT, appender.pwrite(tmp_buf, sizeof(tmp_buf), 0));
    ASSERT_EQ(OB_NOT_INIT, appender.close());

    // wrong uri
    uri[0] = '\0';
    ASSERT_EQ(OB_INVALID_ARGUMENT, appender.open(uri, &cos_base));
    ASSERT_EQ(OB_NOT_INIT, appender.write(tmp_buf, sizeof(tmp_buf)));
    ASSERT_EQ(OB_NOT_INIT, appender.pwrite(tmp_buf, sizeof(tmp_buf), 0));
    ASSERT_EQ(OB_NOT_INIT, appender.close());

    uri[0] = 'a';
    uri[1] = '\0';
    ASSERT_EQ(OB_INVALID_BACKUP_DEST, appender.open(uri, &cos_base));
    ASSERT_EQ(OB_NOT_INIT, appender.write(tmp_buf, sizeof(tmp_buf)));
    ASSERT_EQ(OB_NOT_INIT, appender.pwrite(tmp_buf, sizeof(tmp_buf), 0));
    ASSERT_EQ(OB_NOT_INIT, appender.close());

    ASSERT_EQ(OB_SUCCESS, databuff_printf(uri, sizeof(uri), "%s/test_append_file_////", dir_uri));
    ASSERT_EQ(OB_INVALID_ARGUMENT, appender.open(uri, &cos_base));
    ASSERT_EQ(OB_NOT_INIT, appender.write(tmp_buf, sizeof(tmp_buf)));
    ASSERT_EQ(OB_NOT_INIT, appender.pwrite(tmp_buf, sizeof(tmp_buf), 0));
    ASSERT_EQ(OB_NOT_INIT, appender.close());
    ASSERT_EQ(OB_SUCCESS, databuff_printf(uri, sizeof(uri), "%s/test_append_file", dir_uri));
    ASSERT_EQ(OB_SUCCESS, appender.open(uri, &cos_base));
    ASSERT_EQ(OB_SUCCESS, appender.close());

    // invalid storage info
    ASSERT_EQ(OB_INVALID_ARGUMENT, appender.open(uri, NULL));
    ASSERT_EQ(OB_NOT_INIT, appender.write(tmp_buf, sizeof(tmp_buf)));
    ASSERT_EQ(OB_NOT_INIT, appender.pwrite(tmp_buf, sizeof(tmp_buf), 0));
    ASSERT_EQ(OB_NOT_INIT, appender.close());

    ASSERT_EQ(OB_SUCCESS, databuff_printf(uri, sizeof(uri), "%s/test_append_file", dir_uri));
    ASSERT_EQ(OB_SUCCESS, appender.open(uri, &cos_base));
    ASSERT_TRUE(appender.is_opened());
    ASSERT_EQ(0LL, appender.get_length());

    // first append
    const char first_write[] = "123";
    ASSERT_EQ(OB_SUCCESS, appender.pwrite(first_write, strlen(first_write), 0));
    ASSERT_EQ(strlen(first_write), appender.get_length());

    // second append
    const char second_write[] = "4567";
    // repeatable_pwrite returned err code
    ASSERT_EQ(OB_BACKUP_PWRITE_CONTENT_NOT_MATCH,
        appender.pwrite(second_write, strlen(second_write), strlen(first_write) - 1));
    ASSERT_EQ(OB_BACKUP_PWRITE_OFFSET_NOT_MATCH,
        appender.pwrite(second_write, strlen(second_write) - 1, strlen(first_write) + 1));
    ASSERT_EQ(OB_SUCCESS, appender.pwrite(second_write, strlen(second_write), strlen(first_write)));
    ASSERT_EQ(strlen(first_write) + strlen(second_write), appender.get_length());

    // check size
    int64_t file_length = 0;
    ASSERT_EQ(OB_SUCCESS, util.get_file_length(uri, file_length));
    ASSERT_EQ(strlen(first_write) + strlen(second_write), file_length);

    // check data and clean
    ASSERT_EQ(OB_SUCCESS, reader.open(uri, &cos_base));
    ASSERT_EQ(strlen(first_write) + strlen(second_write), reader.get_length());
    char read_buf[5] = {0};
    int64_t read_size = 0;
    ASSERT_EQ(OB_SUCCESS, reader.pread(read_buf, 5, 2, read_size));
    ASSERT_EQ('3', read_buf[0]);
    ASSERT_EQ('7', read_buf[4]);
    ASSERT_EQ(5, read_size);
    ASSERT_EQ(OB_SUCCESS, reader.close());
    ASSERT_EQ(OB_SUCCESS, util.del_file(uri));
    ASSERT_EQ(OB_SUCCESS, appender.close());
    ASSERT_FALSE(appender.is_opened());

    // append to not appendable object
    ASSERT_EQ(OB_SUCCESS, databuff_printf(uri, sizeof(uri), "%s/test_append_to_not_appendable_file", dir_uri));
    ASSERT_EQ(OB_SUCCESS, util.write_single_file(uri, first_write, strlen(first_write)));
    ASSERT_EQ(OB_SUCCESS, appender.open(uri, &cos_base));
    ASSERT_EQ(OB_CLOUD_OBJECT_NOT_APPENDABLE, appender.pwrite(second_write, strlen(second_write), strlen(first_write)));
    ASSERT_EQ(OB_SUCCESS, appender.close());
    ASSERT_FALSE(appender.is_opened());
    ASSERT_EQ(OB_SUCCESS, util.del_file(uri));

    // operate after close
    ASSERT_EQ(OB_NOT_INIT, appender.write(tmp_buf, sizeof(tmp_buf)));
    ASSERT_EQ(OB_NOT_INIT, appender.pwrite(tmp_buf, sizeof(tmp_buf), 0));
    ASSERT_EQ(OB_NOT_INIT, appender.close());

    util.close();
  }
}

#define WRITE_SINGLE_FILE(file_suffix, file_content) \
  ASSERT_EQ(OB_SUCCESS, databuff_printf(uri, sizeof(uri), "%stest_write_file_%ld", dir_uri, file_suffix)); \
  ASSERT_EQ(OB_SUCCESS, writer.open(uri, &cos_base)); \
  const char write_content[] = file_content; \
  ASSERT_EQ(OB_SUCCESS, writer.write(write_content, strlen(write_content))); \
  ASSERT_EQ(OB_SUCCESS, writer.close()); \

TEST_F(TestStorageCos, test_basic_rw)
{
  int ret = OB_SUCCESS;
  if (enable_test_) {
    ObStorageWriter writer;
    ObStorageReader reader;
    ObStorageUtil util;
    ASSERT_EQ(OB_SUCCESS, util.open(&cos_base));
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
    ASSERT_EQ(OB_INVALID_ARGUMENT, writer.open(uri, &cos_base));
    ASSERT_EQ(OB_NOT_INIT, writer.write(tmp_buf, sizeof(tmp_buf)));
    ASSERT_EQ(OB_NOT_INIT, writer.close());
    ASSERT_EQ(OB_INVALID_ARGUMENT, reader.open(uri, &cos_base));
    ASSERT_EQ(OB_NOT_INIT, reader.pread(tmp_buf, sizeof(tmp_buf), 0, tmp_read_size));
    ASSERT_EQ(OB_NOT_INIT, reader.close());

    const int64_t tmp_ts = ObTimeUtility::current_time();
    ASSERT_EQ(OB_SUCCESS, databuff_printf(uri, sizeof(uri), "%stest_write_file_%ld", dir_uri, tmp_ts));
    ASSERT_EQ(OB_SUCCESS, writer.open(uri, &cos_base));
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

      ASSERT_EQ(OB_SUCCESS, reader.open(uri, &cos_base));
      ASSERT_EQ(strlen(write_content), reader.get_length());
      ASSERT_EQ(OB_SUCCESS, reader.close());
      ASSERT_EQ(OB_SUCCESS, util.del_file(uri));
    }

    {
      // ObStorageWriter writer;
      const int64_t ts = ObTimeUtility::current_time();
      WRITE_SINGLE_FILE(ts, "123456789");

      // ObStorageReader reader;
      ASSERT_EQ(OB_SUCCESS, reader.open(uri, &cos_base));
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
      ASSERT_EQ(OB_SUCCESS, reader.open(uri, &cos_base));
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
      ASSERT_EQ(OB_BACKUP_FILE_NOT_EXIST, reader.open(uri, &cos_base));
      ASSERT_EQ(OB_NOT_INIT, reader.close());

      // open after fail
      WRITE_SINGLE_FILE(ts, "123456789ABCDEF");
      ASSERT_EQ(OB_SUCCESS, reader.open(uri, &cos_base));
      ASSERT_EQ(OB_SUCCESS, reader.close());
    }

    // open twice
    ASSERT_EQ(OB_SUCCESS, writer.open(uri, &cos_base));
    ASSERT_EQ(OB_INIT_TWICE, writer.open(uri, &cos_base));
    ASSERT_EQ(OB_NOT_INIT, writer.close());   // reader/writer will be closed if init twice
    ASSERT_EQ(OB_SUCCESS, reader.open(uri, &cos_base));
    ASSERT_EQ(OB_INIT_TWICE, reader.open(uri, &cos_base));
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

TEST_F(TestStorageCos, test_util)
{
  int ret = OB_SUCCESS;
  if (enable_test_) {
    ObStorageUtil util;
    const char *tmp_util_dir = "test_util";
    const int64_t ts = ObTimeUtility::current_time();
    ASSERT_EQ(OB_SUCCESS, databuff_printf(dir_uri, sizeof(dir_uri), "%s/%s/%s_%ld/",
      bucket, dir_name, tmp_util_dir, ts));
    ASSERT_EQ(OB_SUCCESS, util.open(&cos_base));

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
    ASSERT_EQ(OB_INIT_TWICE, util.open(&cos_base));
    util.close();

    // invalid storage info
    ASSERT_EQ(OB_INVALID_ARGUMENT, util.open(NULL));
  }
}

TEST_F(TestStorageCos, test_util_write_single_file)
{
  int ret = OB_SUCCESS;
  if (enable_test_) {
    ObStorageReader reader;
    ObStorageUtil util;
    const char *tmp_util_dir = "test_util_write_single_file";
    const int64_t ts = ObTimeUtility::current_time();
    ASSERT_EQ(OB_SUCCESS, databuff_printf(dir_uri, sizeof(dir_uri), "%s/%s/%s_%ld",
      bucket, dir_name, tmp_util_dir, ts));
    ASSERT_EQ(OB_SUCCESS, util.open(&cos_base));

    // write data
    const char *write_content = "123456789ABCDEF";
    ASSERT_EQ(OB_SUCCESS, databuff_printf(uri, sizeof(uri), "%s/0", dir_uri));
    ASSERT_EQ(OB_SUCCESS, util.write_single_file(uri, write_content, strlen(write_content)));

    // check data
    ASSERT_EQ(OB_SUCCESS, reader.open(uri, &cos_base));
    ASSERT_EQ(strlen(write_content), reader.get_length());
    char read_buf[10] = {0};
    int64_t read_size = 0;
    ASSERT_EQ(OB_SUCCESS, reader.pread(read_buf, 10, 9, read_size));
    ASSERT_EQ('A', read_buf[0]);
    ASSERT_EQ('F', read_buf[5]);
    ASSERT_EQ(6, read_size);
    ASSERT_EQ(OB_SUCCESS, reader.close());

    ASSERT_EQ(OB_SUCCESS, util.del_file(uri));

    // wrong uri
    uri[0] = '\0';
    ASSERT_EQ(OB_INVALID_BACKUP_DEST, util.write_single_file(uri, write_content, strlen(write_content)));

    util.close();
  }
}

class TestCosListOp : public ObBaseDirEntryOperator
{
public:
  TestCosListOp() {}
  virtual ~TestCosListOp() {}
  int func(const dirent *entry) override;

  ObArray<char *> object_names_;
private :
  ObArenaAllocator allocator_;
};

int TestCosListOp::func(const dirent *entry)
{
  int ret = OB_SUCCESS;
  char *tmp_object_name = (char *)allocator_.alloc(strlen(entry->d_name) + 1);
  if (OB_ISNULL(tmp_object_name)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    OB_LOG(WARN,"fail to alloc buf for tmp object name", K(ret));
  } else {
    STRCPY(tmp_object_name, entry->d_name);
    object_names_.push_back(tmp_object_name);
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

TEST_F(TestStorageCos, test_util_list_files)
{
  int ret = OB_SUCCESS;
  if (enable_test_) {
    ObStorageReader reader;
    ObStorageUtil util;
    const char *tmp_util_dir = "test_util_list_files";
    const int64_t ts = ObTimeUtility::current_time();
    ASSERT_EQ(OB_SUCCESS, databuff_printf(dir_uri, sizeof(dir_uri), "%s/%s/%s_%ld",
      bucket, dir_name, tmp_util_dir, ts));
    ASSERT_EQ(OB_SUCCESS, util.open(&cos_base));

    {
      // wrong uri
      uri[0] = '\0';
      TestCosListOp op;
      ASSERT_EQ(OB_INVALID_BACKUP_DEST, util.list_files(uri, op));
    }

    int64_t file_num = 1001;
    const char *write_content = "0123456789";

    // list objects
    {
      const char *format = "%s/%0*ld_%ld";
      UTIL_WRITE_FILES(format, object_prefix_len, file_num, write_content);

      // list and check and clean
      TestCosListOp op;
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
      TestCosListOp op;
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
    TestCosListOp list_empty_op;
    ASSERT_EQ(OB_SUCCESS, util.list_files(dir_uri, list_empty_op));
    ASSERT_EQ(0, list_empty_op.object_names_.size());

    util.close();
  }
}

TEST_F(TestStorageCos, test_util_list_directories)
{
  int ret = OB_SUCCESS;
  if (enable_test_) {
    ObStorageReader reader;
    ObStorageUtil util;
    const char *tmp_util_dir = "test_util_list_directories";
    const int64_t ts = ObTimeUtility::current_time();
    ASSERT_EQ(OB_SUCCESS, databuff_printf(dir_uri, sizeof(dir_uri), "%s/%s/%s_%ld",
      bucket, dir_name, tmp_util_dir, ts));
    ASSERT_EQ(OB_SUCCESS, util.open(&cos_base));

    {
      // wrong uri
      uri[0] = '\0';
      TestCosListOp op;
      ASSERT_EQ(OB_INVALID_ARGUMENT, util.list_directories(uri, op));
    }

    int64_t file_num = 1001;
    const char *write_content = "0123456789";

    {
      const char *format = "%s/%0*ld/%ld";
      UTIL_WRITE_FILES(format, object_prefix_len, file_num, write_content);

      // list and check and clean
      TestCosListOp op;
      ASSERT_EQ(OB_SUCCESS, util.list_directories(dir_uri, op));
      ASSERT_EQ(file_num, op.object_names_.size());
      for (int64_t i = 0; i < file_num; i++) {
        // listed directories do not contain prefix
        ASSERT_EQ(OB_SUCCESS, databuff_printf(uri, sizeof(uri), "%0*ld", object_prefix_len, i));
        ASSERT_STREQ(uri, op.object_names_[i]);
      }
      UTIL_DELETE_FILES(format, object_prefix_len, file_num);
    }

    // no sub directories
    {
      const char *format = "%s/%0*ld_%ld";
      UTIL_WRITE_FILES(format, object_prefix_len, file_num, write_content);

      // list and check and clean
      TestCosListOp op;
      ASSERT_EQ(OB_SUCCESS, util.list_directories(dir_uri, op));
      ASSERT_EQ(0, op.object_names_.size());
      UTIL_DELETE_FILES(format, object_prefix_len, file_num);
    }

    util.close();
  }
}

TEST_F(TestStorageCos, test_util_is_tagging)
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
    ObObjectStorageInfo tmp_cos_base;
    const char *write_content = "123456789ABCDEF";

    // wrong tag mode
    ASSERT_EQ(OB_SUCCESS,
              databuff_printf(tmp_account, sizeof(tmp_account),
              "host=%s&access_id=%s&access_key=%s&appid=%s&delete_mode=tag",
              endpoint, secretid, secretkey, appid));
    ASSERT_EQ(OB_INVALID_ARGUMENT, tmp_cos_base.set(ObStorageType::OB_STORAGE_COS, tmp_account));
    tmp_cos_base.reset();

    ASSERT_EQ(OB_SUCCESS,
              databuff_printf(tmp_account, sizeof(tmp_account),
              "host=%s&access_id=%s&access_key=%s&appid=%s&delete_mode=delete_delete",
              endpoint, secretid, secretkey, appid));
    ASSERT_EQ(OB_INVALID_ARGUMENT, tmp_cos_base.set(ObStorageType::OB_STORAGE_COS, tmp_account));
    tmp_cos_base.reset();

    // delete mode
    ASSERT_EQ(OB_SUCCESS,
              databuff_printf(tmp_account, sizeof(tmp_account),
              "host=%s&access_id=%s&access_key=%s&appid=%s&delete_mode=delete",
              endpoint, secretid, secretkey, appid));
    ASSERT_EQ(OB_SUCCESS, tmp_cos_base.set(ObStorageType::OB_STORAGE_COS, tmp_account));

    ASSERT_EQ(OB_SUCCESS, databuff_printf(uri, sizeof(uri), "%s/delete_mode", dir_uri));
    ASSERT_EQ(OB_SUCCESS, util.open(&tmp_cos_base));
    ASSERT_EQ(OB_SUCCESS, util.write_single_file(uri, write_content, strlen(write_content)));

    ASSERT_EQ(OB_SUCCESS, util.is_tagging(uri, is_tagging));
    ASSERT_FALSE(is_tagging);

    ASSERT_EQ(OB_SUCCESS, util.del_file(uri));
    ASSERT_EQ(OB_BACKUP_FILE_NOT_EXIST, util.is_tagging(uri, is_tagging));
    tmp_cos_base.reset();
    util.close();

    // tagging mode
    ASSERT_EQ(OB_SUCCESS,
              databuff_printf(tmp_account, sizeof(tmp_account),
              "host=%s&access_id=%s&access_key=%s&appid=%s&delete_mode=tagging",
              endpoint, secretid, secretkey, appid));
    ASSERT_EQ(OB_SUCCESS, tmp_cos_base.set(ObStorageType::OB_STORAGE_COS, tmp_account));

    ASSERT_EQ(OB_SUCCESS, databuff_printf(uri, sizeof(uri), "%s/tagging_mode", dir_uri));
    ASSERT_EQ(OB_SUCCESS, util.open(&tmp_cos_base));
    ASSERT_EQ(OB_SUCCESS, util.write_single_file(uri, write_content, strlen(write_content)));

    is_tagging = true;
    ASSERT_EQ(OB_SUCCESS, util.is_tagging(uri, is_tagging));
    ASSERT_FALSE(is_tagging);

    ASSERT_EQ(OB_SUCCESS, util.del_file(uri));
    ASSERT_EQ(OB_SUCCESS, util.is_tagging(uri, is_tagging));
    ASSERT_TRUE(is_tagging);
    tmp_cos_base.reset();
    util.close();

    // clean
    ASSERT_EQ(OB_SUCCESS,
              databuff_printf(tmp_account, sizeof(tmp_account),
              "host=%s&access_id=%s&access_key=%s&appid=%s",
              endpoint, secretid, secretkey, appid));
    ASSERT_EQ(OB_SUCCESS, tmp_cos_base.set(ObStorageType::OB_STORAGE_COS, tmp_account));

    ASSERT_EQ(OB_SUCCESS, databuff_printf(uri, sizeof(uri), "%s/tagging_mode", dir_uri));
    ASSERT_EQ(OB_SUCCESS, util.open(&tmp_cos_base));
    ASSERT_EQ(OB_SUCCESS, util.del_file(uri));
    ASSERT_EQ(OB_BACKUP_FILE_NOT_EXIST, util.is_tagging(uri, is_tagging));
    util.close();
  }
}

int main(int argc, char **argv)
{
  system("rm -f test_storage_cos.log*");
  OB_LOGGER.set_file_name("test_storage_cos.log", true, true);
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}