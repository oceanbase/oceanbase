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

#include "lib/utility/ob_print_utils.h"
#include "lib/time/Time.h"

#define private public
#include "share/ob_license_mgr.h"
#undef private
#include "share/ob_license_utils.h"

#include <gtest/gtest.h>
#include <thread>
#include <chrono>

namespace oceanbase
{
namespace share
{
using namespace oceanbase::common;
using namespace obutil;

const char *fake_tenant_name = "mysql";
const char *UNITTEST_LICENSE_PUB_KEY = "-----BEGIN PUBLIC KEY-----\n"
"MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAyuMQuAUAq1z3li7STl8I\n"
"2ki8FzCYOswX2P+yFxKN5gEbY4kZCNgcY+nSbhhEr1kxFZG9EtwDzLng2VRT/ceD\n"
"CiIHBALLE8KoMW1wq/MI/9BkHD6LEstAc3HTaVUK1z2yWx8UvGnn9CZG+tHNrq2s\n"
"sajiv3ntBwXtiKbKnTelz3so5DEXcahjG5earftKWXVq3ltkFYHGSfads1SQtmAp\n"
"Vxwp14xZx56LmwIxgUsQAr5nklr5a071WFOQkuFte6gY0ZP6Ukkb4CKlO8rMJl6g\n"
"XxL9CMslcL6mX8LqBRRQ1yblLI/e4d7clSPB6yP9NVikM4Qy2PqVyCMYjPetJIUQ\n"
"QQIDAQAB\n"
"-----END PUBLIC KEY-----\n";
const char *TEST_ENCRYPTED_LICENSE
    = "ktJByPfTlPBmT4xzvo5T5VtKF+9Gax0suO+U7P41Hwg1UNljqBlp89RnHwHIQY5NlB0ghlcKpyUM"
      "F+AnfDk5xa6FNqmhLMk9RaUYSAu2as1XGEsXh3EP3fDAfF4kuyO/Ewlx896vl2RaoAxWvu9J4JM+"
      "iVo8pZdx4IA2iwaPcV/wBTQXBGViS3I2K9SUvbVsyhonOKv2iq4lCVGmUTnYJ5vbrjp0S2vOMyQO"
      "M6jr8XfX1Ct+G9Dwdnu0AbXcdCtn2EA4ZU/hFTpoSkI+UaoD3sOh0WUUkQqqwY7C80qnRkvWXVIK"
      "52sY6GDVqF2G3gnmF3/aOx9ujksSlZLSjUOBRKm+A7P9wg/1tY6ENGtUCMpyx5lv/bmrH90uInLE"
      "LpIx0c00BUheLTP7qtGaF9RiJfGbpyvTP8WfagdcQRaVvBAfTV/PUOpfNWzMCWPxMOiKpYswqB4z"
      "mj4tXXtHGuFQ5+SD1P+TFHRzyj1C4IBa5zGafNQhEgvJaqeLpj9p+C9gz5iD3hedEw6zj0QvnECe"
      "JdYrI9UcCnAoELsNyAy1kjVbnALw/TBP0dM0NCH5z6zxiabpb4qgLOzir8SWcK7ywsVQEzDMZOyq"
      "BkUKV87DcBf57N1uRVxIINQEkSndBbpfidvyGNcOpLnMDcjXyhM6rtF/biWx45I2nQbwNPyNXq8p"
      "mtmPTJXm+QLsJ6+MGQCEVG3RHekE9EjvQ80CU43YecAIO4Rk2w+SQlpCJc2IWiUTPrEc2CF2gbSF"
      "JpDp7FN2MOpDubP/GN2VmjlBlJrkuZG7fqnAoxIUKfxiW6kugNb6yJsX9/Ql6mS3lM8E0LfpA8Av"
      "T5vEGDq507D6Glj2Z1PbsqZ7w3f0TQmRBc2Wkf5wetbAmasIoDhvKMHGu4NZjSpz9X75VTdfMv5A"
      "QRs5v7rzB8sSDDuRogBQsLWpct6wo4NdMaES0OYBZnb/2b8hON1LUXFK9S8x3krZhcr/3pQVUeBp"
      "/7g4nbNZUvloZrbTboY7Vo9Oe8bomdT+5XLg";
const char *TEST_DECRYPTED_LICENSE = R"({
    "license": {
        "EndUser": "shaoyibo.syb",
        "LicenseID": "7d52b498-ba0d-4f0b-a057-e492c6def206",
        "LicenseCode": "BD-123456-CT-12345",
        "LicenseType": "\u5546\u4e1a\u7248",
        "ProductType": "\u5355\u673a\u6807\u51c6\u7248",
        "ProductVersion": "v4.2.5",
        "IssuanceDate": "2025-02-19",
        "ValidityPeriod": 1828,
        "CoreNum": 2,
        "NodeNum": 1,
        "Options": [
            "a",
            "b",
            "c"
        ],
        "Version": 1
    }
})";

const char *INCORRECT_ENCODED_LICENSE
    = "afdsasdfasdgawefawfaewfwafe-+-=--=-231424awefaewfeawfawefweffawefwefeawfaewfewfwaefaewfewaf";
const char *INCORRECT_ENCRYPTED_LICENSE
    = "E4hvn9KoMiOoKBy9FPTKZdYyN/TW4AqGsDjcP2pDjP9UmCR6o2VBjKtDapRDImw4iF/AKbAjSicyfirFGOq36w==";

const char *LICENSE_INVALID_FORMAT_1 = "ImNotAJsonString";
const char *LICENSE_INVALID_FORMAT_2 = "{}";
const char *LICENSE_INVALID_FORMAT_3 = "{\"license\": 1}";
const char *LICENSE_INVALID_FORMAT_4 = "{\"license\": {}}";
const char *LICENSE_INVALID_FORMAT_5 = R"({
    "license": {
        "EndUser": 1
    }
})";
const char *LICENSE_INVALID_FORMAT_6 = R"({
    "license": {
        "EndUser": "shaoyibo.syb"
    }
})";
const char *LICENSE_INVALID_FORMAT_7 = R"({
    "license": {
        "EndUser": "shao",
        "LicenseID": "7d52b498-ba0d-4f0b-a057-e492c6def206",
        "LicenseCode": "BD-123456-CT-12345",
        "LicenseType": "\u5546\u4e1a\u7248",
        "ProductType": "\u5355\u673a\u6807\u51c6\u7248",
        "ProductVersion": "v4.2.5",
        "IssuanceDate": "bad-date",
        "ValidityPeriod": 1828,
        "CoreNum": 2,
        "NodeNum": 1,
        "Options": [
            "a",
            "b",
            "c"
        ],
        "Version": 1
    }
})";

const char *CORRECT_LICENSE = R"({
    "license": {
        "EndUser": "shaoyibo.syb",
        "LicenseID": "7d52b498-ba0d-4f0b-a057-e492c6def206",
        "LicenseCode": "BD-123456-CT-12345",
        "LicenseType": "trial",
        "ProductType": "Standalone Standard Edition",
        "ProductVersion": "v4.2.5",
        "IssuanceDate": "2025-02-19",
        "ValidityPeriod": "2099-02-19",
        "CoreNum": 2,
        "NodeNum": 1,
        "Options": [
            "a",
            "b",
            "c"
        ],
        "Version": 1
    }
})"; // 商业版 and 单机标准版

class TestLicense : public ::testing::Test
{
public:

  virtual void SetUp() override
  {
    LOG_INFO("set up");
    license_mgr_ = OB_NEW(ObLicenseMgr, "TestLicense");
    ASSERT_TRUE(OB_NOT_NULL(license_mgr_));
    license_mgr_->is_start_ = true;
    license_mgr_->is_unittest_ = true;
    license_mgr_->PUBLIC_KEY_PEM = UNITTEST_LICENSE_PUB_KEY;
    license_mgr_->timestamp_service_.UPDATE_TIME_DURATION = INT64_MAX;
    license_mgr_->timestamp_service_.is_unittest_ = true;
  }

  virtual void TearDown() override
  {
    LOG_INFO("tear down");
    if (OB_NOT_NULL(license_mgr_)) {
      OB_DELETE(ObLicenseMgr, "TestLicense", license_mgr_);
    }
  }

  ObLicenseMgr *license_mgr_;
};

TEST_F(TestLicense, DecryptLicense)
{
  ObArenaAllocator allocator;
  ObString license_str;
  ASSERT_EQ(license_mgr_->decrypt_license(allocator, TEST_ENCRYPTED_LICENSE, license_str),
            OB_SUCCESS);
  LOG_INFO("decrpted license_str is ", K(license_str));
  ASSERT_TRUE(license_str == TEST_DECRYPTED_LICENSE);
  ASSERT_EQ(license_mgr_->decrypt_license(allocator, INCORRECT_ENCRYPTED_LICENSE, license_str),
            OB_INVALID_LICENSE);
  ASSERT_EQ(license_mgr_->decrypt_license(allocator, INCORRECT_ENCODED_LICENSE, license_str),
            OB_INVALID_LICENSE);
  ASSERT_EQ(license_mgr_->decrypt_license(allocator, "", license_str), OB_INVALID_LICENSE);
}

TEST_F(TestLicense, ParseAndValidateLicense)
{
  ObLicense *license = nullptr;
  ASSERT_EQ(license_mgr_->timestamp_service_.start_with_time(1739894401000000), OB_SUCCESS);

  ASSERT_EQ(license_mgr_->parse_license(LICENSE_INVALID_FORMAT_1, license), OB_INVALID_LICENSE);
  ASSERT_EQ(license_mgr_->parse_license(LICENSE_INVALID_FORMAT_2, license), OB_INVALID_LICENSE);
  ASSERT_EQ(license_mgr_->parse_license(LICENSE_INVALID_FORMAT_3, license), OB_INVALID_LICENSE);
  ASSERT_EQ(license_mgr_->parse_license(LICENSE_INVALID_FORMAT_4, license), OB_INVALID_LICENSE);
  ASSERT_EQ(license_mgr_->parse_license(LICENSE_INVALID_FORMAT_5, license), OB_INVALID_LICENSE);
  ASSERT_EQ(license_mgr_->parse_license(LICENSE_INVALID_FORMAT_6, license), OB_INVALID_LICENSE);
  ASSERT_EQ(license_mgr_->parse_license(LICENSE_INVALID_FORMAT_7, license), OB_INVALID_LICENSE);
  ASSERT_EQ(license_mgr_->parse_license("", license), OB_INVALID_LICENSE);

  ASSERT_EQ(license_mgr_->parse_license(CORRECT_LICENSE, license), OB_SUCCESS);
  ASSERT_TRUE(OB_NOT_NULL(license));
  ASSERT_TRUE(license->end_user_ == "shaoyibo.syb");
  ASSERT_TRUE(license->license_id_ == "7d52b498-ba0d-4f0b-a057-e492c6def206");
  ASSERT_TRUE(license->license_code_ == "BD-123456-CT-12345");
  ASSERT_TRUE(license->license_type_ == "trial");
  ASSERT_TRUE(license->product_type_ == "Standalone Standard Edition");
  // ASSERT_TRUE(license->product_version_ == "v4.2.5");
  ASSERT_TRUE(license->issuance_date_ == 1739894400000000);
  ASSERT_TRUE(license->expiration_time_ == 4075113600000000);
  ASSERT_TRUE(license->activation_time_ == 1739894401000000);
  ASSERT_TRUE(license->core_num_ == 2);
  ASSERT_TRUE(license->node_num_ == 1);
  ASSERT_TRUE(license->options_ == "a,b,c");

  ASSERT_EQ(license_mgr_->validate_license(*license), OB_SUCCESS);

  license->activation_time_ = 1639894401000000;
  ASSERT_EQ(license_mgr_->validate_license(*license), OB_INVALID_LICENSE);
  license->activation_time_ = 1739894401000000;

  license->activation_time_ = 4175113600000000;
  ASSERT_EQ(license_mgr_->validate_license(*license), OB_INVALID_LICENSE);
  license->activation_time_ = 1739894401000000;

  license_mgr_->replace_license(license);
}

TEST_F(TestLicense, LicenseTimestampService) {
  license_mgr_->timestamp_service_.UPDATE_TIME_DURATION = 0;
  int64_t time = 0;
  int64_t last_time = 0;
  const int64_t single_us = 1;
  const int64_t single_ms = 1000;
  const int64_t single_second = 1000 * 1000;
  const int64_t single_minute = 60 * 1000 * 1000;
  const int64_t single_day = (int64_t) 24 * 60 * 60 * 1000 * 1000;
  int64_t current_sys_time = ObSysTime::now().toMicroSeconds();

  ASSERT_EQ(license_mgr_->timestamp_service_.start_with_time(current_sys_time), OB_SUCCESS);

  ASSERT_EQ(license_mgr_->timestamp_service_.get_time(last_time), OB_SUCCESS);
  for (int i = 0; i < 3; i ++) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));

    ASSERT_EQ(license_mgr_->timestamp_service_.get_time(time), OB_SUCCESS);
    ASSERT_LT(time, last_time + 20 * single_ms);
    ASSERT_GT(time, last_time + 5 * single_ms);
    last_time = time;
  }

  license_mgr_->timestamp_service_.modified_sys_time_ = current_sys_time - single_day;
  license_mgr_->timestamp_service_.get_time(time);
  ASSERT_GT(time, current_sys_time);
  ASSERT_LT(time, current_sys_time + single_minute);

  license_mgr_->timestamp_service_.modified_sys_time_ = current_sys_time + single_day;
  license_mgr_->timestamp_service_.get_time(time);
  ASSERT_GT(time, current_sys_time);
  ASSERT_LT(time, current_sys_time + single_minute);

  ASSERT_EQ(license_mgr_->timestamp_service_.get_time(last_time), OB_SUCCESS);
  for (int i = 0; i < 3; i ++) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));

    ASSERT_EQ(license_mgr_->timestamp_service_.get_time(time), OB_SUCCESS);
    ASSERT_LT(time, last_time + 20 * single_ms);
    ASSERT_GT(time, last_time + 5 * single_ms);
    last_time = time;
  }

  // test if work correctly in multithread environment
  std::vector<std::thread> threads;
  for (int i = 0; i < 10; i ++) {
    threads.emplace_back(std::thread([this]() {
      int64_t time = 0;
      int64_t last_time = 0;
      ASSERT_EQ(license_mgr_->timestamp_service_.get_time(last_time), OB_SUCCESS);

      for (int j = 0; j < 1000; j ++) {
        ASSERT_EQ(license_mgr_->timestamp_service_.get_time(time), OB_SUCCESS);
        ASSERT_LT(time, last_time + single_second);
        ASSERT_GE(time, last_time);
        last_time = time;
      }
    }));
  }

  for (auto &thread : threads) {
    thread.join();
  }
}

TEST_F(TestLicense, TestLicenseUtils) {
  ObLicenseMgr& g_license_mgr = ObLicenseMgr::instance;
  ObLicenseTimestampService& g_time_service = g_license_mgr.timestamp_service_;
  ObLicense *current_license = nullptr;
  char login_msg[50] = "Im a Invalid Login in message!      QwQ    =)   !";
  const int64_t tenant_id = 1002;

  g_license_mgr.is_start_ = true;
  g_license_mgr.is_unittest_ = true;
  g_time_service.is_unittest_ = true;
  g_time_service.UPDATE_TIME_DURATION = INT64_MAX;

  ASSERT_EQ(OB_SUCCESS, g_time_service.start_with_time(1739894401000000));

  ASSERT_EQ(OB_SUCCESS, g_license_mgr.gen_trail_license(current_license));
  ASSERT_TRUE(OB_NOT_NULL(current_license));
  ASSERT_EQ(OB_SUCCESS, g_license_mgr.replace_license(current_license));
  current_license->node_num_ = 1;

  ASSERT_EQ(OB_SUCCESS, ObLicenseUtils::check_dml_allowed());
  ASSERT_EQ(OB_SUCCESS, ObLicenseUtils::check_olap_allowed(tenant_id));
  ASSERT_EQ(OB_SUCCESS, ObLicenseUtils::check_add_tenant_allowed(0, fake_tenant_name));
  ASSERT_EQ(OB_SUCCESS, ObLicenseUtils::check_add_tenant_allowed(1, fake_tenant_name));
  ASSERT_EQ(OB_SUCCESS, ObLicenseUtils::check_standby_allowed());
  ASSERT_EQ(OB_SUCCESS, ObLicenseUtils::check_create_table_allowed(1000));

  current_license->license_trail_ = false;
  current_license->expiration_time_ = 1739894401000000 - 1000000;
  ASSERT_EQ(OB_SUCCESS, ObLicenseUtils::check_dml_allowed());
  ASSERT_EQ(OB_SUCCESS, ObLicenseUtils::check_olap_allowed(tenant_id));
  ASSERT_EQ(OB_SUCCESS, ObLicenseUtils::check_add_tenant_allowed(0, fake_tenant_name));
  ASSERT_EQ(OB_SUCCESS, ObLicenseUtils::check_add_tenant_allowed(1, fake_tenant_name));
  ASSERT_EQ(OB_SUCCESS, ObLicenseUtils::check_standby_allowed());
  ASSERT_EQ(OB_LICENSE_EXPIRED, ObLicenseUtils::check_create_table_allowed(1000));

  g_license_mgr.boot_with_expired_ = true;
  ASSERT_EQ(OB_LICENSE_EXPIRED, ObLicenseUtils::check_dml_allowed());
  ASSERT_EQ(OB_LICENSE_EXPIRED, ObLicenseUtils::check_olap_allowed(tenant_id));
  ASSERT_EQ(OB_SUCCESS, ObLicenseUtils::check_add_tenant_allowed(0, fake_tenant_name));
  ASSERT_EQ(OB_LICENSE_EXPIRED, ObLicenseUtils::check_add_tenant_allowed(1, fake_tenant_name));
  ASSERT_EQ(OB_LICENSE_EXPIRED, ObLicenseUtils::check_standby_allowed());
  ASSERT_EQ(OB_LICENSE_EXPIRED, ObLicenseUtils::check_create_table_allowed(1000));

  current_license->license_trail_ = true;
  g_license_mgr.boot_with_expired_ = false;
  ASSERT_EQ(OB_LICENSE_EXPIRED, ObLicenseUtils::check_dml_allowed());
  ASSERT_EQ(OB_LICENSE_EXPIRED, ObLicenseUtils::check_olap_allowed(tenant_id));
  ASSERT_EQ(OB_SUCCESS, ObLicenseUtils::check_add_tenant_allowed(0, fake_tenant_name));
  ASSERT_EQ(OB_LICENSE_EXPIRED, ObLicenseUtils::check_add_tenant_allowed(1, fake_tenant_name));
  ASSERT_EQ(OB_LICENSE_EXPIRED, ObLicenseUtils::check_standby_allowed());
  ASSERT_EQ(OB_LICENSE_EXPIRED, ObLicenseUtils::check_create_table_allowed(1000));

  ASSERT_EQ(OB_SUCCESS, ObLicenseUtils::get_login_message(login_msg, 50));
  ASSERT_EQ(0, strcmp(login_msg, "License has been expired"));


  current_license->license_trail_ = false;
  current_license->expiration_time_ = 1739894401000000 + 1000000;
  ASSERT_EQ(OB_SUCCESS, ObLicenseUtils::get_login_message(login_msg, 50));
  ASSERT_EQ(0, strcmp(login_msg, "License will be expired in 1 days"));


  const char *auxiliary_tenant_name = "mysql_$aux";
  current_license->allow_multi_tenant_ = false;
  current_license->allow_olap_ = false;
  current_license->allow_stand_by_ = false;
  ASSERT_EQ(OB_SUCCESS, ObLicenseUtils::check_dml_allowed());
  ASSERT_EQ(OB_LICENSE_SCOPE_EXCEEDED, ObLicenseUtils::check_olap_allowed(tenant_id));
  ASSERT_EQ(OB_SUCCESS, ObLicenseUtils::check_add_tenant_allowed(0, fake_tenant_name));
  ASSERT_EQ(OB_LICENSE_SCOPE_EXCEEDED, ObLicenseUtils::check_add_tenant_allowed(1, fake_tenant_name));
  ASSERT_EQ(OB_LICENSE_SCOPE_EXCEEDED, ObLicenseUtils::check_add_tenant_allowed(1, fake_tenant_name));
  ASSERT_EQ(OB_SUCCESS, ObLicenseUtils::check_add_tenant_allowed(1, auxiliary_tenant_name));
  ASSERT_EQ(OB_LICENSE_SCOPE_EXCEEDED, ObLicenseUtils::check_standby_allowed());
  ASSERT_EQ(OB_SUCCESS, ObLicenseUtils::check_create_table_allowed(1000));
}

} // end namespace share
} // end namespace oceanbase

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
