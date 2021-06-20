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
#include "lib/utility/ob_test_util.h"
#include "share/restore/ob_restore_uri_parser.h"
#include "vsclient.h"
#include "aos_define.h"
#include "aos_http_io.h"

using namespace oceanbase::common;
using namespace oceanbase::share;
class TestRestoreURIParser : public ::testing::Test {
public:
  TestRestoreURIParser();
  virtual ~TestRestoreURIParser();
  virtual void SetUp();
  virtual void TearDown();

private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(TestRestoreURIParser);

protected:
  // function members
protected:
  // data members
};

TestRestoreURIParser::TestRestoreURIParser()
{
  int ret = OB_SUCCESS;
  int aos_ret = AOSE_OK;
  if (AOSE_OK != (aos_ret = aos_http_io_initialize(NULL, 0))) {
    OB_LOG(WARN, "fail to init aos", K(aos_ret));
  }
  VIPSrv_Result* result = NULL;
  VIPSrv_Config config;
  config.jmenv_dom = "jmenv.tbsite.net";
  config.cache_dir = getenv("HOME");
  if (NULL == (result = vipsrv_global_init(&config))) {
    ret = OB_OSS_ERROR;
    OB_LOG(WARN, "fail to init vipserver", K(ret));
  } else if (!result->is_success) {
    ret = OB_OSS_ERROR;
    OB_LOG(WARN, "fail to init vipserver", K(ret));
  }

  if (NULL != result) {
    vipsrv_result_deref(result);
  }
}

TestRestoreURIParser::~TestRestoreURIParser()
{
  vipsrv_global_cleanup();
  aos_http_io_deinitialize();
}

void TestRestoreURIParser::SetUp()
{}

void TestRestoreURIParser::TearDown()
{}

TEST_F(TestRestoreURIParser, test_basic)
{
  ObRestoreArgs arg;
  const char* uri_001 = "oss://071092/ob2.XX/"
                        "1001?timestamp=1495444477922616&host=oss-cn-hangzhou-zmf.aliyuncs.com&access_id="
                        "krtRx5e2pc0EvbwN&access_key=OjbK7VjBvOciHKcW5dPX8Rc4usUc0P&restore_user=a&restore_pass=b";
  ASSERT_EQ(OB_SUCCESS, ObRestoreURIParser::parse(ObString(uri_001), arg));
}

TEST_F(TestRestoreURIParser, test_long_bucket)
{
  ObRestoreArgs arg;
  const char* uri_001 = "oss://071092/test_ob1/1.323/ob2.XX/"
                        "1001?timestamp=1495444477922616&host=oss-cn-hangzhou-zmf.aliyuncs.com&access_id="
                        "krtRx5e2pc0EvbwN&access_key=OjbK7VjBvOciHKcW5dPX8Rc4usUc0P&restore_user=a&restore_pass=b";
  ASSERT_EQ(OB_SUCCESS, ObRestoreURIParser::parse(ObString(uri_001), arg));
  LOG_INFO("long bucket", K(arg));
}

TEST_F(TestRestoreURIParser, test_more)
{
  ObRestoreArgs arg;
  const char* uri_001 = "file:///071092/ob2.XX/"
                        "1001?timestamp=1495444477922616&host=oss-cn-hangzhou-zmf.aliyuncs.com&access_id="
                        "krtRx5e2pc0EvbwN&access_key=OjbK7VjBvOciHKcW5dPX8Rc4usUc0P&restore_user=a&restore_pass=b";
  ASSERT_EQ(OB_SUCCESS, ObRestoreURIParser::parse(ObString(uri_001), arg));
  LOG_INFO("tset more", K(arg));
  ASSERT_TRUE(0 == strcmp(arg.storage_info_,
                       "timestamp=1495444477922616&host=oss-cn-hangzhou-zmf.aliyuncs.com&access_id=krtRx5e2pc0EvbwN&"
                       "access_key=OjbK7VjBvOciHKcW5dPX8Rc4usUc0P&restore_user=a&restore_pass=b"));
  ASSERT_TRUE(1001 == arg.tenant_id_);
  ASSERT_TRUE(0 == strcmp(arg.uri_, "file:///071092/ob2.XX"));
  ASSERT_TRUE(1495444477922616 == arg.restore_timeu_);
}

TEST_F(TestRestoreURIParser, testBlankExtraParam)
{
  ObRestoreArgs arg;
  const char* uri_000 =
      "oss://bucket/appname "
      "/1001?timestamp=10&host=host_001&access_id=id_001&access_key=key_001&restore_user=a&restore_pass=b";
  const char* uri_001 =
      "oss://bucket/app "
      "name/1001?timestamp=10&host=host_001&access_id=id_001&access_key=key_001&restore_user=a&restore_pass=b";
  const char* uri_002 = "oss://bucket/appname/1001?timestamp "
                        "=10&host=host_001&access_id=id_001&access_key=key_001&restore_user=a&restore_pass=b";
  const char* uri_003 = "oss://bucket/appname/1001?timestamp=10 "
                        "&host=host_001&access_id=id_001&access_key=key_001&restore_user=a&restore_pass=b";
  const char* uri_004 = "oss://bucket/appname/1001? "
                        "timestamp=10&host=host_001&access_id=id_001&access_key=key_001&restore_user=a&restore_pass=b";
  const char* uri_005 = "oss://bucket/appname/1001? timestamp=10&host=host_001&access_id=id_001&access_key=key_001";
  ASSERT_EQ(OB_SUCCESS, ObRestoreURIParser::parse(ObString(uri_000), arg));
  ASSERT_EQ(OB_SUCCESS, ObRestoreURIParser::parse(ObString(uri_001), arg));
  ASSERT_EQ(OB_INVALID_ARGUMENT, ObRestoreURIParser::parse(ObString(uri_002), arg));
  ASSERT_EQ(OB_INVALID_ARGUMENT, ObRestoreURIParser::parse(ObString(uri_003), arg));
  ASSERT_EQ(OB_INVALID_ARGUMENT, ObRestoreURIParser::parse(ObString(uri_004), arg));
  ASSERT_EQ(OB_INVALID_ARGUMENT, ObRestoreURIParser::parse(ObString(uri_005), arg));
}

TEST_F(TestRestoreURIParser, idIsNotInt)
{
  ObRestoreArgs arg;
  const char* uri_001 =
      "oss://bucket/appname/"
      "1001?timestamp=10&host=host_001&access_id=id_001&access_key=key_001&restore_user=a&restore_pass=b";
  const char* uri_002 =
      "oss://bucket/appname/"
      "1001BadTenant?timestamp=10&host=host_001&access_id=id_001&access_key=key_001&restore_user=a&restore_pass=b";
  const char* uri_003 =
      "oss://bucket/appname/"
      "1001BadTenant?timestamp=10&host=host_001&access_id=id_001&access_key=key_001&restore_user=a&restore_pass=b";
  const char* uri_004 =
      "oss://bucket/appname/"
      "1001?timestamp=badTs0&host=host_001&access_id=id_001&access_key=key_001&restore_user=a&restore_pass=b";
  ASSERT_EQ(OB_SUCCESS, ObRestoreURIParser::parse(ObString(uri_001), arg));
  ASSERT_EQ(OB_INVALID_ARGUMENT, ObRestoreURIParser::parse(ObString(uri_002), arg));
  ASSERT_EQ(OB_INVALID_ARGUMENT, ObRestoreURIParser::parse(ObString(uri_003), arg));
  ASSERT_EQ(OB_INVALID_ARGUMENT, ObRestoreURIParser::parse(ObString(uri_004), arg));
}

TEST_F(TestRestoreURIParser, anyProtocol)
{
  ObRestoreArgs arg;
  const char* uri_001 =
      "os://bucket/appname/"
      "1001?timestamp=10&host=host_001&access_id=id_001&access_key=key_001&restore_user=a&restore_pass=b";
  const char* uri_002 =
      "oss?//bucket/appname/"
      "1001?timestamp=10&host=host_001&access_id=id_001&access_key=key_001&restore_user=a&restore_pass=b";
  const char* uri_003 =
      "oss///bucket/appname/"
      "1001?timestamp=10&host=host_001&access_id=id_001&access_key=key_001&restore_user=a&restore_pass=b";
  const char* uri_004 =
      "bucket/appname/"
      "1001?timestamp=10&host=host_001&access_id=id_001&access_key=key_001&restore_user=a&restore_pass=b";
  const char* uri_005 =
      "file:///appname/"
      "1001?timestamp=10&host=host_001&access_id=id_001&access_key=key_001&restore_user=a&restore_pass=b";
  ASSERT_EQ(OB_SUCCESS, ObRestoreURIParser::parse(ObString(uri_001), arg));
  ASSERT_EQ(OB_INVALID_ARGUMENT, ObRestoreURIParser::parse(ObString(uri_002), arg));  // oss? <= sep
  ASSERT_EQ(OB_SUCCESS, ObRestoreURIParser::parse(ObString(uri_003), arg));
  ASSERT_EQ(OB_SUCCESS, ObRestoreURIParser::parse(ObString(uri_004), arg));
  ASSERT_EQ(OB_SUCCESS, ObRestoreURIParser::parse(ObString(uri_005), arg));
}

TEST_F(TestRestoreURIParser, valueTooLong)
{
  ObRestoreArgs arg;
  char uri_001[4096];
  snprintf(uri_001,
      4096,
      "oss://%03000d/appname/11/"
      "1001?timestamp=10&host=host_001&access_id=id_001&access_key=key_001&restore_user=a&restore_pass=b",
      1);
  ASSERT_EQ(OB_SIZE_OVERFLOW, ObRestoreURIParser::parse(ObString(uri_001), arg));
  LOG_INFO("too long bucket", K(uri_001));
}

TEST_F(TestRestoreURIParser, LessOrMoreDir)
{
  ObRestoreArgs arg;
  const char* uri_002 =
      "oss://bucket/appname/11/111/"
      "123?timestamp=10&host=host_001&access_id=id_001&access_key=key_001&restore_user=a&restore_pass=b";
  const char* uri_003 =
      "oss://bucket/"
      "appname?timestamp=10&host=host_001&access_id=id_001&access_key=key_001&restore_user=a&restore_pass=b";
  const char* uri_004 =
      "oss://?timestamp=10&host=host_001&access_id=id_001&access_key=key_001&restore_user=a&restore_pass=b";
  EXPECT_EQ(OB_SUCCESS, ObRestoreURIParser::parse(ObString(uri_002), arg));
  EXPECT_EQ(OB_INVALID_ARGUMENT, ObRestoreURIParser::parse(ObString(uri_003), arg));
  EXPECT_EQ(OB_INVALID_ARGUMENT, ObRestoreURIParser::parse(ObString(uri_004), arg));
}

TEST_F(TestRestoreURIParser, ConflictVersionTimestamp)
{
  ObRestoreArgs arg;
  const char* uri_001 = "oss://bucket/appname/11/"
                        "1001?timestamp=123123123&access_id=id_001&access_key=key_001&restore_user=a&restore_pass=b";
  EXPECT_EQ(OB_SUCCESS, ObRestoreURIParser::parse(ObString(uri_001), arg));
  EXPECT_EQ(OB_INVALID_ARGUMENT, ObRestoreURIParserHelper::set_data_version(arg));
}

int main(int argc, char** argv)
{
  // oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  OB_LOGGER.set_log_level("INFO", "WARN");
  //  OB_LOGGER.set_mod_log_levels("ALL.*:INFO,LIB.MYSQLC:ERROR");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
