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
#include "lib/restore/ob_storage.h"
#define private public

using namespace oceanbase::common;

class TestStorageOss: public ::testing::Test
{
public:
  TestStorageOss() {}
  virtual ~TestStorageOss(){}
  virtual void SetUp()
  {
    ObOssEnvIniter::get_instance().global_init();
  }
  virtual void TearDown()
  {
    ObOssEnvIniter::get_instance().global_destroy();
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
  char test_oss_bucket_[OB_MAX_URI_LENGTH] = "oss://antsys-oceanbasebackup";

  // oss config
  char oss_host_[OB_MAX_URI_LENGTH] = "cn-hangzhou-alipay-b.oss-cdn.aliyun-inc.com";
  char oss_id_[MAX_OSS_ID_LENGTH] = "fill_test_id";
  char oss_key_[MAX_OSS_KEY_LENGTH] = "fill_test_key";
};

TEST_F(TestStorageOss, test_repeatable_pwrite)
{
  char uri[OB_MAX_URI_LENGTH];
  char storage_infos[OB_MAX_URI_LENGTH];

  ASSERT_EQ(OB_SUCCESS, databuff_printf(storage_infos, sizeof(storage_infos), "host=%s&access_id=%s&access_key=%s", oss_host_, oss_id_, oss_key_));
  const char *dir_name = "test";

  ASSERT_EQ(OB_SUCCESS, databuff_printf(uri, sizeof(uri), "%s/%s/pwrite_file", test_oss_bucket_, dir_name));

  const ObString storage_info(storage_infos);

  ObStorageAppender appender;
  ObStorageAppender::AppenderParam param;
  param.strategy_ = OB_APPEND_USE_APPEND;
  param.version_param_.open_object_version_ = false;

  ObOssAccount oss_account;
  ASSERT_EQ(OB_SUCCESS, oss_account.parse_oss_arg(storage_info));

  ASSERT_EQ(OB_SUCCESS, appender.open(uri, &oss_account, param));
  ASSERT_TRUE(appender.is_opened());
  ASSERT_EQ(0ll, appender.get_length());

  const char *buf1 = "0123";
  ASSERT_EQ(OB_SUCCESS, appender.pwrite(buf1, strlen(buf1), 0));
  ASSERT_EQ(4, appender.get_length());

  const char *buf2 = "56";
  ASSERT_EQ(OB_BACKUP_PWRITE_CONTENT_NOT_MATCH, appender.pwrite(buf2, strlen(buf2), 5));
  ASSERT_EQ(4, appender.get_length());

  const char *buf3 = "45";
  ASSERT_EQ(OB_SUCCESS, appender.pwrite(buf3, strlen(buf3), 4));
  ASSERT_EQ(6, appender.get_length());

  const char *buf4 = "345";
  ASSERT_EQ(OB_SUCCESS, appender.pwrite(buf4, strlen(buf4), 3));
  ASSERT_EQ(6, appender.get_length());

  const char *buf5 = "34";
  ASSERT_EQ(OB_SUCCESS, appender.pwrite(buf5, strlen(buf5), 3));
  ASSERT_EQ(6, appender.get_length());

  const char *buf6 = "3456";
  ASSERT_EQ(OB_SUCCESS, appender.pwrite(buf6, strlen(buf6), 3));
  ASSERT_EQ(7, appender.get_length());

  const char *buf7 = "427";
  ASSERT_EQ(OB_BACKUP_PWRITE_CONTENT_NOT_MATCH, appender.pwrite(buf7, strlen(buf7), 4));
  ASSERT_EQ(7, appender.get_length());

  ASSERT_EQ(OB_SUCCESS, appender.close());

}

int main(int argc, char **argv)
{
  OB_LOGGER.set_log_level("DEBUG");
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}