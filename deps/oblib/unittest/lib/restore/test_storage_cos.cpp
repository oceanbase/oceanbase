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
#include "lib/restore/ob_storage_cos.h"
#include "lib/allocator/page_arena.h"

using namespace oceanbase::common;

class TestStorageCosCommon {
public:
TestStorageCosCommon() {}
~TestStorageCosCommon() {}
 void init()
  {
    init_cos_env();
    ASSERT_EQ(
      OB_SUCCESS,
      databuff_printf(account, sizeof(account), "host=%s&access_id=%s&access_key=%s&appid=%s",
      endpoint, secretid, secretkey, appid));
    //build cos_base
    const ObString storage_info(account);
    ASSERT_EQ(OB_SUCCESS, cos_base.build_account(storage_info));
  }
 void destory()
  {
    fin_cos_env();
  }
protected:
  char bucket[OB_MAX_URI_LENGTH] = "cos://ob-dbt3-test-1304889018";
  char endpoint[OB_MAX_URI_LENGTH] = "cos.ap-shanghai.myqcloud.com";
  char secretid[OB_MAX_URI_LENGTH] = "";
  char secretkey[OB_MAX_URI_LENGTH] = "";
  char appid[OB_MAX_URI_LENGTH] = "";
  char account[OB_MAX_URI_LENGTH];
  const char *dir_name = "cos_unittest_dir";
  char uri[OB_MAX_URI_LENGTH];
  char dir_uri[OB_MAX_URI_LENGTH];
  ObCosBase cos_base;
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
  ObCosUtil util;
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
  TestStorageCos() {}
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
};


TEST_F(TestStorageCos, test_appender)
{
  const int64_t ts = ObTimeUtility::current_time();
  // format dir path uri
  ASSERT_EQ(OB_SUCCESS, databuff_printf(dir_uri, sizeof(dir_uri), "%s/%s/", bucket, dir_name));
  // format object path uri
  ASSERT_EQ(OB_SUCCESS, databuff_printf(uri, sizeof(uri), "%stest_file%ld", dir_uri, ts));

  ObStorageReader reader;
  // File not exist before created
  ASSERT_EQ(OB_BACKUP_FILE_NOT_EXIST, reader.open(uri, &cos_base));

  // open a new object for write
  ObStorageAppender appender;
  ObStorageAppender::AppenderParam param;
  param.strategy_ = OB_APPEND_USE_OVERRITE;
  param.version_param_.open_object_version_ = false;
  ASSERT_EQ(OB_SUCCESS, appender.open(uri, &cos_base, param));
  ASSERT_TRUE(appender.is_opened());
  ASSERT_EQ(0LL, appender.get_length());

  // first write
  const char first_write[] = "123";
  ASSERT_EQ(OB_SUCCESS, appender.write(first_write, strlen(first_write)));
  ASSERT_EQ(strlen(first_write), appender.get_length());

  // second write
  const char second_write[] = "456";
  const char data[] = "123456";
  ASSERT_EQ(OB_SUCCESS, appender.write(second_write, strlen(second_write)));
  ASSERT_EQ(strlen(data), appender.get_length());

  // close object
  ASSERT_EQ(OB_SUCCESS, appender.close());
  ASSERT_FALSE(appender.is_opened());

  // open the object for read
  constexpr int64_t data_len = sizeof(data) - 1;
  ASSERT_EQ(OB_SUCCESS, reader.open(uri, &cos_base));
  ASSERT_EQ(data_len, reader.get_length());

  char buffer[data_len];
  // get object
  int64_t read_size = 0;
  ASSERT_EQ(OB_SUCCESS, reader.pread(buffer, data_len, 0LL, read_size));
  ASSERT_EQ(data_len, read_size);
  ASSERT_TRUE(0 == memcmp(buffer, data, data_len));

  // close object
  ASSERT_EQ(OB_SUCCESS, reader.close());
  ASSERT_EQ(OB_NOT_INIT, reader.pread(buffer, data_len, 0LL, read_size));

  // delete the created object
  ObCosUtil util;
  ASSERT_EQ(OB_SUCCESS, util.open(&cos_base));
  ASSERT_EQ(OB_SUCCESS, util.del_file(uri));

  // read the deleted object
  ASSERT_EQ(OB_BACKUP_FILE_NOT_EXIST, reader.open(uri, &cos_base));
  util.close();
}

TEST_F(TestStorageCos, test_create_empty_object1)
{
  const int64_t ts = ObTimeUtility::current_time();
  // format dir path uri
  ASSERT_EQ(OB_SUCCESS, databuff_printf(dir_uri, sizeof(dir_uri), "%s/%s/", bucket, dir_name));
  // format object path uri
  ASSERT_EQ(OB_SUCCESS, databuff_printf(uri, sizeof(uri), "%stest_file%ld", dir_uri, ts));

  // create empty object
  ObCosUtil util;
  ASSERT_EQ(OB_SUCCESS, util.open(&cos_base));
  ASSERT_EQ(OB_SUCCESS, util.write_single_file(uri, NULL, 0));

  // open the empty object
  ObStorageReader reader;
  ASSERT_EQ(OB_SUCCESS, reader.open(uri, &cos_base));
  ASSERT_EQ(reader.get_length(), 0);
  ASSERT_EQ(OB_SUCCESS, reader.close());

  // create the empty object with appender
  ObStorageAppender appender;
  ObStorageAppender::AppenderParam param;
  param.strategy_ = OB_APPEND_USE_OVERRITE;
  param.version_param_.open_object_version_ = false;
  ASSERT_EQ(OB_SUCCESS, appender.open(uri, &cos_base, param));
  ASSERT_EQ(OB_SUCCESS, appender.write(NULL, 0));
  ASSERT_EQ(OB_SUCCESS, appender.close());
  util.close();
}

TEST_F(TestStorageCos, test_create_empty_object2)
{
  const int64_t ts = ObTimeUtility::current_time();
  // format dir path uri
  ASSERT_EQ(OB_SUCCESS, databuff_printf(dir_uri, sizeof(dir_uri), "%s/%s/", bucket, dir_name));
  // format object path uri
  ASSERT_EQ(OB_SUCCESS, databuff_printf(uri, sizeof(uri), "%stest_file%ld", dir_uri, ts));

  // create the empty object with appender
  ObStorageAppender appender;
  ObStorageAppender::AppenderParam param;
  param.strategy_ = OB_APPEND_USE_OVERRITE;
  param.version_param_.open_object_version_ = false;
  ASSERT_EQ(OB_SUCCESS, appender.open(uri, &cos_base, param));
  ASSERT_EQ(OB_SUCCESS, appender.write(NULL, 0));
  ASSERT_EQ(OB_SUCCESS, appender.close());

  // open the empty object
  ObStorageReader reader;
  ASSERT_EQ(OB_SUCCESS, reader.open(uri, &cos_base));
  ASSERT_EQ(reader.get_length(), 0);
  ASSERT_EQ(OB_SUCCESS, reader.close());
}

TEST_F(TestStorageCos, test_create_empty_object3)
{
  const int64_t ts = ObTimeUtility::current_time();
  // format dir path uri
  ASSERT_EQ(OB_SUCCESS, databuff_printf(dir_uri, sizeof(dir_uri), "%s/%s/", bucket, dir_name));
  // format object path uri
  ASSERT_EQ(OB_SUCCESS, databuff_printf(uri, sizeof(uri), "%stest_file%ld", dir_uri, ts));

  // create empty object with appender
  ObStorageAppender appender;
  ObStorageAppender::AppenderParam param;
  param.strategy_ = OB_APPEND_USE_OVERRITE;
  param.version_param_.open_object_version_ = false;
  ASSERT_EQ(OB_SUCCESS, appender.open(uri, &cos_base, param));

  // first write is NULL
  ASSERT_EQ(OB_SUCCESS, appender.write(NULL, 0));

  // open the empty object
  ObStorageReader reader;
  ASSERT_EQ(OB_SUCCESS, reader.open(uri, &cos_base));
  ASSERT_EQ(reader.get_length(), 0);
  ASSERT_EQ(OB_SUCCESS, reader.close());

  // second write is not empty
  const char second_write[] = "123";
  ASSERT_EQ(OB_SUCCESS, appender.write(second_write, strlen(second_write)));
  ASSERT_EQ(strlen(second_write), appender.get_length());

  // third write is NULL
  ASSERT_EQ(OB_SUCCESS, appender.write(NULL, 0));

  // fourth write is not empty
  const char fourth_write[] = "456";
  const char data[] = "123456";
  ASSERT_EQ(OB_SUCCESS, appender.write(fourth_write, strlen(fourth_write)));
  ASSERT_EQ(strlen(data), appender.get_length());
  ASSERT_EQ(OB_SUCCESS, appender.close());

  // verify data
  constexpr int64_t data_len = sizeof(data) - 1;
  ASSERT_EQ(OB_SUCCESS, reader.open(uri, &cos_base));
  ASSERT_EQ(data_len, reader.get_length());

  char buffer[data_len];
  int64_t read_size = 0;
  ASSERT_EQ(OB_SUCCESS, reader.pread(buffer, data_len, 0LL, read_size));
  ASSERT_EQ(data_len, read_size);
  ASSERT_TRUE(0 == memcmp(buffer, data, data_len));
  ASSERT_EQ(OB_SUCCESS, reader.close());
}

TEST_F(TestStorageCos, test_append_2_exist_object)
{
  const int64_t ts = ObTimeUtility::current_time();
  // format dir path uri
  ASSERT_EQ(OB_SUCCESS, databuff_printf(dir_uri, sizeof(dir_uri), "%s/%s/", bucket, dir_name));
  // format object path uri
  ASSERT_EQ(OB_SUCCESS, databuff_printf(uri, sizeof(uri), "%stest_file%ld", dir_uri, ts));

  // open a new object for write
  ObStorageAppender appender;
  ObStorageAppender::AppenderParam param;
  param.strategy_ = OB_APPEND_USE_OVERRITE;
  param.version_param_.open_object_version_ = false;
  ASSERT_EQ(OB_SUCCESS, appender.open(uri, &cos_base, param));
  ASSERT_EQ(0LL, appender.get_length());

  // first write
  const char first_write[] = "123";
  ASSERT_EQ(OB_SUCCESS, appender.write(first_write, strlen(first_write)));
  ASSERT_EQ(strlen(first_write), appender.get_length());

  // close, then open again
  ASSERT_EQ(OB_SUCCESS, appender.close());
  ASSERT_EQ(OB_SUCCESS, appender.open(uri, &cos_base, param));
  ASSERT_EQ(0LL, appender.get_length());

  // second write
  const char second_write[] = "456";
  const char data[] = "123456";
  ASSERT_EQ(OB_SUCCESS, appender.write(second_write, strlen(second_write)));
  ASSERT_EQ(strlen(second_write), appender.get_length());

  // close object
  ASSERT_EQ(OB_SUCCESS, appender.close());

  // open the object for read
  ObStorageReader reader;
  constexpr int64_t data_len = sizeof(data) - 1;
  ASSERT_EQ(OB_SUCCESS, reader.open(uri, &cos_base));
  ASSERT_EQ(data_len, reader.get_length());

  char buffer[data_len];
  // get object
  int64_t read_size = 0;
  ASSERT_EQ(OB_SUCCESS, reader.pread(buffer, data_len, 0LL, read_size));
  ASSERT_EQ(data_len, read_size);
  ASSERT_TRUE(0 == memcmp(buffer, data, data_len));

  // close object
  ASSERT_EQ(OB_SUCCESS, reader.close());
}

TEST_F(TestStorageCos, test_read)
{
  const int64_t ts = ObTimeUtility::current_time();
  // format dir path uri
  ASSERT_EQ(OB_SUCCESS, databuff_printf(dir_uri, sizeof(dir_uri), "%s/%s/", bucket, dir_name));
  // format object path uri
  ASSERT_EQ(OB_SUCCESS, databuff_printf(uri, sizeof(uri), "%stest_file%ld", dir_uri, ts));

  const char data[] = "123456";
  ObCosUtil util;
  ASSERT_EQ(OB_SUCCESS, util.open(&cos_base));
  ASSERT_EQ(OB_SUCCESS, util.write_single_file(uri, data, strlen(data)));

  // open the object for read
  ObStorageReader reader;

  ASSERT_EQ(OB_SUCCESS, reader.open(uri, &cos_base));

  constexpr int64_t buffer_len = 1024;
  char buffer[buffer_len];
  // read object from offset 0
  int64_t read_size = 0;
  ASSERT_EQ(OB_SUCCESS, reader.pread(buffer, buffer_len, 0LL, read_size));
  ASSERT_EQ(read_size, reader.get_length());
  ASSERT_TRUE(0 == memcmp(buffer, data, read_size));

  // read object from offset which is beyond file length
  ASSERT_EQ(OB_FILE_LENGTH_INVALID, reader.pread(buffer, buffer_len, reader.get_length(), read_size));
  ASSERT_EQ(read_size, 0);

  // close object
  ASSERT_EQ(OB_SUCCESS, reader.close());
  util.close();
}


// no open version
TEST_F(TestStorageCos, test_container_simple)
{
  const int64_t ts = ObTimeUtility::current_time();
  // format dir path uri
  ASSERT_EQ(OB_SUCCESS, databuff_printf(dir_uri, sizeof(dir_uri), "%s/%s", bucket, dir_name));
  // format object path uri
  ASSERT_EQ(OB_SUCCESS, databuff_printf(uri, sizeof(uri), "%s/object_%ld", dir_uri, ts));

  // create container, let each put be a slice
  ObCosContainer::Option option;
  option.threshold = 0;
  option.open_version = false;

  char data1[1024] = "abcdefghijklmnopqrstuvwxyz";
  char data2[1024] = "bcdefghijklmnopqrstuvwxyza";
  char data3[1024] = "cdefghijklmnopqrstuvwxyzab";
  ObCosWritableContainer writable_container(option);

  // container not exist before created
  ObCosUtil util;
  bool is_file_exist;
  int64_t file_length;
  ASSERT_EQ(OB_SUCCESS, util.open(&cos_base));
  ASSERT_EQ(OB_SUCCESS, util.is_exist(uri, is_file_exist));
  ASSERT_FALSE(is_file_exist);
  ASSERT_EQ(OB_BACKUP_FILE_NOT_EXIST, util.get_file_length(uri, file_length));
  ASSERT_EQ(-1, file_length);

  // do write
  ASSERT_EQ(OB_SUCCESS, writable_container.open(uri, &cos_base));
  // first write
  ASSERT_EQ(OB_SUCCESS, writable_container.write(data1, sizeof(data1)));
  // second write
  ASSERT_EQ(OB_SUCCESS, writable_container.write(data2, sizeof(data2)));
  // third write
  ASSERT_EQ(OB_SUCCESS, writable_container.write(data3, sizeof(data3)));
  // close container
  ASSERT_EQ(OB_SUCCESS, writable_container.close());
  ASSERT_EQ(OB_COS_ERROR, writable_container.write(data3, sizeof(data3)));

  ASSERT_EQ(OB_SUCCESS, writable_container.open(uri, &cos_base));
  ASSERT_EQ(OB_SUCCESS, writable_container.close());


  // do read
  int64_t read_size;
  char buffer[10240];
  ObCosRandomAccessReader random_access_reader;
  ASSERT_EQ(OB_SUCCESS, random_access_reader.open(uri, &cos_base));
  // offset=0, size=100
  ASSERT_EQ(OB_SUCCESS, random_access_reader.pread(buffer, 100, 0, read_size));
  ASSERT_EQ(0, MEMCMP(data1, buffer, 100));
  ASSERT_EQ(100, read_size);

  // offset=0, size=1024
  ASSERT_EQ(OB_SUCCESS, random_access_reader.pread(buffer, 1024, 0, read_size));
  ASSERT_EQ(0, MEMCMP(data1, buffer, 1024));
  ASSERT_EQ(1024, read_size);

  // offset=0, size=1025
  ASSERT_EQ(OB_SUCCESS, random_access_reader.pread(buffer, 1025, 0, read_size));
  ASSERT_EQ(0, MEMCMP(data1, buffer, 1024));
  ASSERT_EQ(0, MEMCMP(data2, buffer + 1024, 1));
  ASSERT_EQ(1025, read_size);

  // offset=512, size=1024
  ASSERT_EQ(OB_SUCCESS, random_access_reader.pread(buffer, 1024, 512, read_size));
  ASSERT_EQ(0, MEMCMP(data1 + 512, buffer, 512));
  ASSERT_EQ(0, MEMCMP(data2, buffer + 512, 512));
  ASSERT_EQ(1024, read_size);

  // offset=2050, size=1024
  ASSERT_EQ(OB_SUCCESS, random_access_reader.pread(buffer, 1024, 2050, read_size));
  ASSERT_EQ(0, MEMCMP(data3 + 2, buffer, 1022));
  ASSERT_EQ(1022, read_size);

  // close container
  ASSERT_EQ(OB_SUCCESS, random_access_reader.close());
  ASSERT_EQ(OB_COS_ERROR, random_access_reader.pread(buffer, 1024, 2050, read_size));

  ASSERT_EQ(OB_SUCCESS, random_access_reader.open(uri, &cos_base));
  ASSERT_EQ(OB_SUCCESS, random_access_reader.close());

  ASSERT_EQ(OB_SUCCESS, util.is_exist(dir_uri, is_file_exist));
  ASSERT_FALSE(is_file_exist);
  ASSERT_EQ(OB_BACKUP_FILE_NOT_EXIST, util.get_file_length(dir_uri, file_length));
  ASSERT_EQ(-1, file_length);

  ASSERT_EQ(OB_SUCCESS, util.is_exist(uri, is_file_exist));
  ASSERT_TRUE(is_file_exist);
  ASSERT_EQ(OB_SUCCESS, util.get_file_length(uri, file_length));
  ASSERT_EQ(1024 * 3, file_length);

  // delete the first slice, make container corruption
  char to_delete_slice_uri[OB_MAX_URI_LENGTH];
  ASSERT_EQ(OB_SUCCESS, databuff_printf(to_delete_slice_uri, sizeof(to_delete_slice_uri), "%s/", uri));
  ObCosSlice::Option slice_option = {ObCosSlice::Mask::OB_COS_SLICE_ID_MASK, 2, 0};
  ObCosSlice slice(slice_option);
  ASSERT_EQ(OB_SUCCESS, slice.build_slice_name(to_delete_slice_uri + strlen(to_delete_slice_uri), OB_MAX_URI_LENGTH - strlen(to_delete_slice_uri)));

  // delete slice, id = 1
  ASSERT_EQ(OB_SUCCESS, util.del_file(to_delete_slice_uri));

  // open for read again, return corruption
  ASSERT_EQ(OB_COS_ERROR, util.is_exist(uri, is_file_exist));
  util.close();
}

// multi-version slice
TEST_F(TestStorageCos, test_container_multi_version)
{
  const int64_t ts = ObTimeUtility::current_time();
  // format dir path uri
  ASSERT_EQ(OB_SUCCESS, databuff_printf(dir_uri, sizeof(dir_uri), "%s/%s/", bucket, dir_name));
  // format object path uri
  ASSERT_EQ(OB_SUCCESS, databuff_printf(uri, sizeof(uri), "%sobject_%ld", dir_uri, ts));

  // create container
  ObCosContainer::Option option;
  option.threshold = 0;
  option.open_version = true;
  option.version = 10000;

  char data11[1024] = "abcdefghijklmnopqrstuvwxyz";
  char data12[512] = "bcdefghijklmnopqrstuvwxyza";
  char data13[1536] = "cdefghijklmnopqrstuvwxyzab";
  ObCosWritableContainer writable_container1(option);

  option.version = 20000;
  char data21[1536] = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
  char data22[1024] = "BCDEFGHIJKLMNOPQRSTUVWXYZA";
  char data23[1536] = "CDEFGHIJKLMNOPQRSTUVWXYZAB";
  char data24[4096] = "DEFGHIJKLMNOPQRSTUVWXYZABC";
  ObCosWritableContainer writable_container2(option);


  ASSERT_EQ(OB_SUCCESS, writable_container1.open(uri, &cos_base));
  ASSERT_EQ(OB_SUCCESS, writable_container2.open(uri, &cos_base));
  ASSERT_EQ(OB_SUCCESS, writable_container1.write(data11, sizeof(data11)));
  ASSERT_EQ(OB_SUCCESS, writable_container1.write(data12, sizeof(data12)));
  ASSERT_EQ(OB_SUCCESS, writable_container2.write(data21, sizeof(data21)));
  ASSERT_EQ(OB_SUCCESS, writable_container2.write(data22, sizeof(data22)));
  ASSERT_EQ(OB_SUCCESS, writable_container2.write(data23, sizeof(data23)));
  ASSERT_EQ(OB_SUCCESS, writable_container2.write(data24, sizeof(data24)));
  ASSERT_EQ(OB_SUCCESS, writable_container2.close());
  ASSERT_EQ(OB_SUCCESS, writable_container1.write(data13, sizeof(data13)));
  ASSERT_EQ(OB_SUCCESS, writable_container1.close());

  ObCosUtil util;
  ASSERT_EQ(OB_SUCCESS, util.open(&cos_base));
  int64_t file_length;
  ASSERT_EQ(OB_SUCCESS, util.get_file_length(uri, file_length));
  ASSERT_EQ(8192, file_length);

  // do read
  int64_t read_size;
  char buffer[10240];
  ObCosRandomAccessReader random_access_reader;
  ASSERT_EQ(OB_SUCCESS, random_access_reader.open(uri, &cos_base));
  ASSERT_EQ(OB_SUCCESS, random_access_reader.pread(buffer, sizeof(buffer), 0, read_size));
  ASSERT_EQ(0, MEMCMP(data21, buffer, 1536));
  ASSERT_EQ(0, MEMCMP(data22, buffer + 1536, 1024));
  ASSERT_EQ(0, MEMCMP(data23, buffer + 2560, 1536));
  ASSERT_EQ(0, MEMCMP(data24, buffer + 4096, 4096));
  ASSERT_EQ(8192, read_size);
  ASSERT_EQ(OB_SUCCESS, random_access_reader.close());
  util.close();
}

// many slices, let slice if of 11 returned before that of 2.
TEST_F(TestStorageCos, test_container_many_slices)
{
  const int64_t ts = ObTimeUtility::current_time();
  // format dir path uri
  ASSERT_EQ(OB_SUCCESS, databuff_printf(dir_uri, sizeof(dir_uri), "%s/%s/", bucket, dir_name));
  // format object path uri
  ASSERT_EQ(OB_SUCCESS, databuff_printf(uri, sizeof(uri), "%sobject_%ld", dir_uri, ts));

  char sample[] = "abcdefghijklmnopqrstuvwxyz";
  constexpr int sample_size = sizeof(sample) - 1;
  char data[sample_size][sample_size];
  for (int i = 0; i < sample_size; i++) {
    MEMCPY(data[i], sample + i, sample_size - i);
    MEMCPY(data[i] + sample_size - i, sample, i);
  }

  // create container
  ObCosContainer::Option option;
  option.threshold = 0;
  option.open_version = true;
  option.version = 10000;
  ObCosWritableContainer writable_container(option);

  ASSERT_EQ(OB_SUCCESS, writable_container.open(uri, &cos_base));
  for (int i = 0; i < sample_size; i++) {
    ASSERT_EQ(OB_SUCCESS, writable_container.write(data[i], sizeof(data[i])));
  }

  ASSERT_EQ(OB_SUCCESS, writable_container.close());

  ObCosUtil util;
  ASSERT_EQ(OB_SUCCESS, util.open(&cos_base));
  int64_t file_length;
  ASSERT_EQ(OB_SUCCESS, util.get_file_length(uri, file_length));
  ASSERT_EQ(sample_size * sample_size, file_length);

  // do read
  int64_t read_size;
  char buffer[10240];
  ObCosRandomAccessReader random_access_reader;
  ASSERT_EQ(OB_SUCCESS, random_access_reader.open(uri, &cos_base));
  ASSERT_EQ(OB_SUCCESS, random_access_reader.pread(buffer, sizeof(buffer), 0, read_size));
  for (int i = 0; i < sample_size; i++) {
    ASSERT_EQ(0, MEMCMP(data[i], buffer + sample_size * i, sample_size));
  }
  ASSERT_EQ(sample_size * sample_size, read_size);
  ASSERT_EQ(OB_SUCCESS, random_access_reader.close());
  util.close();
}

TEST_F(TestStorageCos, test_container_beyond_slice_number)
{
  const int64_t ts = ObTimeUtility::current_time();
  // format dir path uri
  ASSERT_EQ(OB_SUCCESS, databuff_printf(dir_uri, sizeof(dir_uri), "%s/%s/", bucket, dir_name));
  // format object path uri
  ASSERT_EQ(OB_SUCCESS, databuff_printf(uri, sizeof(uri), "%sobject_%ld", dir_uri, ts));

  char sample[] = "abcdefghijklmnopqrstuvwxyz";
  const int64_t sample_size = strlen(sample);


  // create container
  ObCosContainer::Option option;
  option.threshold = 0;
  option.open_version = true;
  option.version = 10000;
  ObCosWritableContainer writable_container(option);

  for (int i = 0; i < ObCosContainer::MAX_SLICE_ID; i++) {
    ASSERT_EQ(OB_SUCCESS, writable_container.open(uri, &cos_base));
    ASSERT_EQ(OB_SUCCESS, writable_container.write(sample, sample_size));
    ASSERT_EQ(OB_SUCCESS, writable_container.close());
  }

  ASSERT_EQ(OB_SUCCESS, writable_container.open(uri, &cos_base));
  ASSERT_EQ(OB_IO_ERROR, writable_container.write(sample, sample_size));
  ASSERT_EQ(OB_SUCCESS, writable_container.close());

  ObCosUtil util;
  ASSERT_EQ(OB_SUCCESS, util.open(&cos_base));
  int64_t file_length;
  ASSERT_EQ(OB_SUCCESS, util.get_file_length(uri, file_length));
  ASSERT_EQ(ObCosContainer::MAX_SLICE_ID * sample_size, file_length);

  // do verify
  int64_t read_size;
  ObArenaAllocator allocator;
  char *buffer = (char *)allocator.alloc(file_length);
  ObCosRandomAccessReader random_access_reader;
  ASSERT_EQ(OB_SUCCESS, random_access_reader.open(uri, &cos_base));
  ASSERT_EQ(OB_SUCCESS, random_access_reader.pread(buffer, file_length, 0, read_size));
  ASSERT_EQ(read_size, file_length);

  int64_t off = 0;
  for (int64_t i = 0; i < ObCosContainer::MAX_SLICE_ID; i++)
  {
    ASSERT_EQ(0, MEMCMP(buffer + off, sample, sample_size));
    off += sample_size;
  }
  util.close();
}

#if 0 //zeyong
TEST_F(TestStorageCos, test_container_too_many_slices)
{
  const int64_t ts = ObTimeUtility::current_time();
  // format dir path uri
  ASSERT_EQ(OB_SUCCESS, databuff_printf(dir_uri, sizeof(dir_uri), "%s/%s/", bucket, dir_name));
  // format object path uri
  ASSERT_EQ(OB_SUCCESS, databuff_printf(uri, sizeof(uri), "%sobject_%ld", dir_uri, ts));

  struct Entry
  {
    int64_t magic = 0x37FAB98037FAB980;
    int64_t no;
    char padding[2048];

    void serialize(char *buffer)
    {
      MEMCPY(buffer, this, sizeof(*this));
    }

    void deserialize(char *buffer)
    {
      MEMCPY(this, buffer, sizeof(*this));
    }
  }__attribute__ ((__packed__));

  // create container
  ObCosContainer::Option option;
  option.threshold = 0;
  option.open_version = true;
  option.version = 10000;
  ObCosWritableContainer writable_container(option);

  const int write_cnt = 1500;
  int64_t entry_id = 0;
  for (int i = 0; i < write_cnt; i++)
  {
    ASSERT_EQ(OB_SUCCESS, writable_container.open(uri, &cos_base));
    int64_t entry_cnt = ObRandom::rand(1, 100);
    ObArenaAllocator allocator;
    int64_t buffer_size = sizeof(Entry) * entry_cnt;
    char *buffer = (char *)allocator.alloc(buffer_size);
    int64_t off = 0;
    for (int j = 0; j < entry_cnt; j++)
    {
      Entry entry;
      entry.no = entry_id++;
      entry.serialize(buffer + off);

      off += sizeof(Entry);
    }

    ASSERT_EQ(OB_SUCCESS, writable_container.write(buffer, buffer_size));
    ASSERT_EQ(OB_SUCCESS, writable_container.close());
    allocator.clear();
  }

  ObCosUtil util;
  ASSERT_EQ(OB_SUCCESS, util.open(&cos_base));
  int64_t file_length;
  ASSERT_EQ(OB_SUCCESS, util.get_file_length(uri, file_length));
  ASSERT_EQ(entry_id * sizeof(Entry), file_length);

  // do verify
  int64_t read_size;
  ObArenaAllocator allocator;
  char *buffer = (char *)allocator.alloc(file_length);
  ObCosRandomAccessReader random_access_reader;
  ASSERT_EQ(OB_SUCCESS, random_access_reader.open(uri, &cos_base));
  ASSERT_EQ(OB_SUCCESS, random_access_reader.pread(buffer, file_length, 0, read_size));
  ASSERT_EQ(read_size, file_length);

  int64_t off = 0;
  for (int64_t id = 0; id < entry_id; id++)
  {
    Entry entry;
    entry.deserialize(buffer + off);
    ASSERT_EQ(id, entry.no);
    ASSERT_EQ(0x37FAB98037FAB980, entry.magic);
    off += sizeof(Entry);
  }

  ASSERT_EQ(OB_SUCCESS, random_access_reader.close());
  allocator.clear();
  util.close();
}
#endif //zeyong

TEST_F(TestStorageCos, test_delete_container)
{
  const int64_t ts = ObTimeUtility::current_time();
  // format dir path uri
  ASSERT_EQ(OB_SUCCESS, databuff_printf(dir_uri, sizeof(dir_uri), "%s/%s/", bucket, dir_name));
  // format object path uri
  ASSERT_EQ(OB_SUCCESS, databuff_printf(uri, sizeof(uri), "%sobject_%ld", dir_uri, ts));

  char sample[] = "abcdefghijklmnopqrstuvwxyz";
  constexpr int sample_size = sizeof(sample) - 1;
  char data[sample_size][sample_size];
  for (int i = 0; i < sample_size; i++) {
    MEMCPY(data[i], sample + i, sample_size - i);
    MEMCPY(data[i] + sample_size - i, sample, i);
  }

  // create container
  ObCosContainer::Option option;
  option.threshold = 0;
  option.open_version = true;
  option.version = 10000;
  ObCosWritableContainer writable_container(option);

  ASSERT_EQ(OB_SUCCESS, writable_container.open(uri, &cos_base));
  for (int i = 0; i < sample_size; i++) {
    ASSERT_EQ(OB_SUCCESS, writable_container.write(data[i], sizeof(data[i])));
  }

  ASSERT_EQ(OB_SUCCESS, writable_container.close());

  // delete container
  ObCosUtil util;
  ASSERT_EQ(OB_SUCCESS, util.open(&cos_base));
  ASSERT_EQ(OB_SUCCESS, util.del_file(uri));

  int64_t file_length;
  ASSERT_EQ(OB_BACKUP_FILE_NOT_EXIST, util.get_file_length(uri, file_length));
  ASSERT_EQ(-1, file_length);
  util.close();
}

#if 0 //zeyong
// delete container in which too many slices in.
TEST_F(TestStorageCos, test_delete_container_too_many_slices)
{
  const int64_t ts = ObTimeUtility::current_time();
  // format dir path uri
  ASSERT_EQ(OB_SUCCESS, databuff_printf(dir_uri, sizeof(dir_uri), "%s/%s/", bucket, dir_name));
  // format object path uri
  ASSERT_EQ(OB_SUCCESS, databuff_printf(uri, sizeof(uri), "%sobject_%ld", dir_uri, ts));

  char sample[] = "abcdefghijklmnopqrstuvwxyz";
  constexpr int sample_size = sizeof(sample) - 1;
  char data[sample_size][sample_size];
  for (int i = 0; i < sample_size; i++) {
    MEMCPY(data[i], sample + i, sample_size - i);
    MEMCPY(data[i] + sample_size - i, sample, i);
  }

  // make 2600 slices
  const int writer_cnt = 100;
  const int64_t start_version = 10000;
  for (int i = 0; i < writer_cnt; i++) {
    // create container
    ObCosContainer::Option option;
    option.threshold = 0;
    option.open_version = true;
    option.version = start_version + i;
    ObCosWritableContainer writable_container(option);

    ASSERT_EQ(OB_SUCCESS, writable_container.open(uri, &cos_base));
    for (int i = 0; i < sample_size; i++) {
      ASSERT_EQ(OB_SUCCESS, writable_container.write(data[i], sizeof(data[i])));
    }

    ASSERT_EQ(OB_SUCCESS, writable_container.close());
  }

  ObCosUtil util;
  ASSERT_EQ(OB_SUCCESS, util.open(&cos_base));
  int64_t file_length;
  ASSERT_EQ(OB_SUCCESS, util.get_file_length(uri, file_length));
  ASSERT_EQ(sample_size * sample_size, file_length);

  // delete container
  ASSERT_EQ(OB_SUCCESS, util.del_file(uri));

  ASSERT_EQ(OB_BACKUP_FILE_NOT_EXIST, util.get_file_length(uri, file_length));
  ASSERT_EQ(-1, file_length);
  util.close()
}
#endif

class TestListObjecContainerOp : public ObBaseDirEntryOperator
{
public:
  TestListObjecContainerOp(int64_t ts) : ts_(ts), obj_index_(0) {}
  ~TestListObjecContainerOp() {}
  int func(const dirent *entry) override;
  int64_t get_obj_count() {return obj_index_;}
private:
  int64_t ts_;
  int64_t obj_index_;
};

int TestListObjecContainerOp::func(const dirent *entry)
{
  int ret = OB_SUCCESS;
  char name_buff[1024];

  if (OB_ISNULL(entry)) {
    ret =  OB_INVALID_ARGUMENT;
  } else if (DT_REG != entry->d_type) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    ObString file_name(entry->d_name);
    if (OB_FAIL(databuff_printf(name_buff, sizeof(name_buff), "object_%ld", ts_ + obj_index_))) {
      //do nothing, just return error code
    } else if (strlen(name_buff) != file_name.length()) {
      ret = OB_INVALID_ARGUMENT;
    } else if (0 != MEMCMP(name_buff, file_name.ptr(), file_name.length())) {
      ret = OB_INVALID_ARGUMENT;
    }
    obj_index_++;
  }
  return ret;
}

// list objects and containers
TEST_F(TestStorageCos, test_list_objects_and_containers)
{
  int64_t ts = ObTimeUtility::current_time();

  // format dir path uri
  ASSERT_EQ(OB_SUCCESS, databuff_printf(dir_uri, sizeof(dir_uri), "%s/%s/%ld/", bucket, dir_name, ts));

  char buffer[1024] = "abcdefghijklmnopqrstuvwxyz";

  ObCosUtil util;
  ASSERT_EQ(OB_SUCCESS, util.open(&cos_base));
  // create 10 normal object
  const int objects_cnt = 10;
  for (int i = 0; i < objects_cnt; i++) {
    // format object path uri
    ASSERT_EQ(OB_SUCCESS, databuff_printf(uri, sizeof(uri), "%sobject_%ld", dir_uri, ts + i));

    ASSERT_EQ(OB_SUCCESS, util.write_single_file(uri, buffer, sizeof(buffer)));
  }

  // create 10 container, each container have 1~10 slices
  const int containers_cnt = 10;
  for (int i = 0; i < containers_cnt; i++) {
    // format object path uri
    ASSERT_EQ(OB_SUCCESS, databuff_printf(uri, sizeof(uri), "%sobject_%ld", dir_uri, ts + objects_cnt + i));

    // create container
    ObCosContainer::Option option;
    option.threshold = 0;
    option.open_version = true;
    option.version = 10000;
    ObCosWritableContainer writable_container(option);

    ASSERT_EQ(OB_SUCCESS, writable_container.open(uri, &cos_base));
    for (int j = 0; j <= i; j++) {
      ASSERT_EQ(OB_SUCCESS, writable_container.write(buffer, sizeof(buffer)));
    }

    ASSERT_EQ(OB_SUCCESS, writable_container.close());
  }

  // list dir
  TestListObjecContainerOp test_obj_op(ts);
  ASSERT_EQ(OB_SUCCESS, util.list_files(dir_uri, test_obj_op));
  ASSERT_EQ(20, test_obj_op.get_obj_count());
  util.close();
}


TEST_F(TestStorageCos, test_delete_empty_container)
{
  const int64_t ts = ObTimeUtility::current_time();
  // format dir path uri
  ASSERT_EQ(OB_SUCCESS, databuff_printf(dir_uri, sizeof(dir_uri), "%s/%s/", bucket, dir_name));
  // format object path uri
  ASSERT_EQ(OB_SUCCESS, databuff_printf(uri, sizeof(uri), "%sobject_%ld", dir_uri, ts));

  // delete empty container
  ObCosContainer container(&cos_base);
  int64_t deleted_cnt;
  ASSERT_EQ(OB_SUCCESS, container.del(uri, deleted_cnt));
  ASSERT_EQ(0, deleted_cnt);
}


TEST_F(TestStorageCos, test_open_many_times_1)
{
  const int64_t ts = ObTimeUtility::current_time();
  // format dir path uri
  ASSERT_EQ(OB_SUCCESS, databuff_printf(dir_uri, sizeof(dir_uri), "%s/%s/", bucket, dir_name));
  // format object path uri
  ASSERT_EQ(OB_SUCCESS, databuff_printf(uri, sizeof(uri), "%sobject_%ld", dir_uri, ts));

  char sample[] = "abcdefghijklmnopqrstuvwxyz";
  constexpr int sample_size = sizeof(sample) - 1;
  char data[sample_size][sample_size];
  for (int i = 0; i < sample_size; i++) {
    MEMCPY(data[i], sample + i, sample_size - i);
    MEMCPY(data[i] + sample_size - i, sample, i);
  }

  // make 26 slices
  const int writer_cnt = sample_size;
  const int64_t version = 10000;
  for (int i = 0; i < writer_cnt; i++) {
    // create container
    ObCosContainer::Option option;
    option.threshold = 0;
    option.open_version = true;
    option.version = version;
    ObCosWritableContainer writable_container(option);

    ASSERT_EQ(OB_SUCCESS, writable_container.open(uri, &cos_base));
    ASSERT_EQ(OB_SUCCESS, writable_container.write(data[i], sizeof(data[i])));
    ASSERT_EQ(OB_SUCCESS, writable_container.close());
  }

  ObCosUtil util;
  ASSERT_EQ(OB_SUCCESS, util.open(&cos_base));
  int64_t file_length;
  ASSERT_EQ(OB_SUCCESS, util.get_file_length(uri, file_length));
  ASSERT_EQ(sample_size * sample_size, file_length);
  util.close();
}

TEST_F(TestStorageCos, test_open_many_times_2)
{
  const int64_t ts = ObTimeUtility::current_time();
  // format dir path uri
  ASSERT_EQ(OB_SUCCESS, databuff_printf(dir_uri, sizeof(dir_uri), "%s/%s/", bucket, dir_name));
  // format object path uri
  ASSERT_EQ(OB_SUCCESS, databuff_printf(uri, sizeof(uri), "%sobject_%ld", dir_uri, ts));

  char sample[] = "abcdefghijklmnopqrstuvwxyz";
  constexpr int sample_size = sizeof(sample) - 1;
  char data[sample_size][sample_size];
  for (int i = 0; i < sample_size; i++) {
    MEMCPY(data[i], sample + i, sample_size - i);
    MEMCPY(data[i] + sample_size - i, sample, i);
  }

  // make 13 slices
  const int writer_cnt = sample_size;
  const int64_t version = 10000;
  for (int i = 0; i < writer_cnt; i++) {
    // create container
    ObCosContainer::Option option;
    option.threshold = 50;
    option.open_version = true;
    option.version = version;
    ObCosWritableContainer writable_container(option);

    ASSERT_EQ(OB_SUCCESS, writable_container.open(uri, &cos_base));
    ASSERT_EQ(OB_SUCCESS, writable_container.write(data[i], sizeof(data[i])));
    ASSERT_EQ(OB_SUCCESS, writable_container.close());
  }

  // make 20 slices of newer version, and failed
  const int64_t newer_writer_cnt = 20;
  for (int i = 0; i < newer_writer_cnt; i++) {
    // create container
    ObCosContainer::Option option;
    option.threshold = 0;
    option.open_version = true;
    option.version = version + 10000;
    ObCosWritableContainer writable_container(option);

    ASSERT_EQ(OB_IO_ERROR, writable_container.open(uri, &cos_base));
  }

  ObCosUtil util;
  ASSERT_EQ(OB_SUCCESS, util.open(&cos_base));
  int64_t file_length;
  ASSERT_EQ(OB_SUCCESS, util.get_file_length(uri, file_length));
  ASSERT_EQ(sample_size * sample_size, file_length);
  util.close();
}

int main(int argc, char **argv)
{
  OB_LOGGER.set_log_level("DEBUG");
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
