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

#define USING_LOG_PREFIX SHARE
#include <gtest/gtest.h>
#define private public
#include "share/ob_encrypt_kms.h"
#include "share/ob_encryption_util.h"
#include "lib/file/file_directory_utils.h"
#undef private

namespace oceanbase
{
namespace share
{
using namespace common;

class TestKmsClient : public ::testing::Test
{
public:
  TestKmsClient() {}
  ~TestKmsClient() {}
};

TEST_F(TestKmsClient, ceair)
{
  int status = 0;
  pid_t pid = fork();
  if (0 == pid) {
    int ret = setpgid(pid, pid);
    if (ret < 0) {
      LOG_ERROR("setpgid failed", K(errno));
    } else if (-1 == (ret = execl("/usr/bin/python", "python", "./mock_kms_server.py", NULL))) {
      LOG_ERROR("execl failed", K(errno));
    }
    exit(1);
  } else if (-1 == pid) {
    LOG_ERROR("fork failed", K(errno));
  } else {
    LOG_INFO("create child", K(pid));
    sleep(1);
    int64_t BUFFER_SIZE = 4000;
    char buf[BUFFER_SIZE];
    const char *path = "test_kms_client.test";
    FILE *fp = fopen(path, "r");
    int64_t len = fread(buf, 1, BUFFER_SIZE, fp);
    ObString kms_info(len, buf);
    bool result = false;
    ObCeairClient client;
    EXPECT_EQ(OB_SUCCESS, client.init(kms_info.ptr(), kms_info.length()));
    kill(-pid, SIGTERM);
    pid = wait(&status);
    LOG_INFO("child exit", K(pid));
  }
}

TEST_F(TestKmsClient, aliyun)
{
  int64_t version = 0;
  ObString encrypted_key;
  ObString data_key;
  ObAliyunClient client;
  const char *kms_info = "{\
    \"kms_host\": \"https://kms.cn-hangzhou.aliyuncs.com\",\
    \"region_id\": \"cn-hangzhou\",\
    \"access_key_id\": \"LTAI4GHD9rfFFmnTHbJZoz7X\",\
    \"access_key_secret\": \"2JVqS3M2m3phQnYPzqCYOqtUsHB1wW\",\
    \"cmk_id\": \"605467ea-d853-4dc5-8807-bf3fca0203cf\"\
}";
  EXPECT_EQ(OB_SUCCESS, client.init(kms_info, strlen(kms_info)));
  EXPECT_EQ(OB_SUCCESS, client.generate_key(ObPostKmsMethod::GENERATE_KEY, version, encrypted_key));
  EXPECT_EQ(OB_SUCCESS, client.get_key(0, encrypted_key, ObPostKmsMethod::GET_KEY, data_key));
  EXPECT_EQ(OB_MAX_MASTER_KEY_LENGTH, data_key.length());
}

TEST_F(TestKmsClient, aliyun_sts)
{
  int64_t version = 0;
  ObString encrypted_key;
  ObString data_key;
  ObAliyunClient client;
  const char *kms_info = "{\
    \"kms_host\": \"https://kms.cn-hangzhou.aliyuncs.com\",\
    \"region_id\": \"cn-hangzhou\",\
    \"access_key_id\": \"LTAI5tRvaqCQuU9RN9m58Wc6\",\
    \"access_key_secret\": \"cFwzjXeik2girV3jtbbFVBGuN94s3S\",\
    \"cmk_id\": \"4dcc6700-c010-430f-9fa3-c7dc02c1b63d\",\
    \"sts_host\": \"https://sts.cn-hangzhou.aliyuncs.com\",\
    \"role_arn\": \"acs:ram::1316987972461619:role/aliyunserviceroleforoceanbaseencryption\",\
    \"role_uid\": \"1316987972461619\"\
}";
  EXPECT_EQ(OB_SUCCESS, client.init(kms_info, strlen(kms_info)));
  EXPECT_EQ(OB_SUCCESS, client.generate_key(ObPostKmsMethod::GENERATE_KEY, version, encrypted_key));
  EXPECT_EQ(OB_SUCCESS, client.get_key(0, encrypted_key, ObPostKmsMethod::GET_KEY, data_key));
  EXPECT_EQ(OB_MAX_MASTER_KEY_LENGTH, data_key.length());
}


} // end namespace share
} // end namespace oceanbase

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_file_name("test_kms_client.log", true);
  oceanbase::common::ObLogger::get_logger().set_log_level("DEBUG");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
