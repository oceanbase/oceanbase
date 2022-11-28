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
#include <gmock/gmock.h>
#define private public
#include "share/ob_encryption_util.h"
#undef private

namespace oceanbase
{
using namespace common;
namespace share
{
// TEST(TestEncryptionUtil, basic)
// {
//   const int64_t invalid_buf_len = 12;
//   char invalid_key[32] = "aabb";
//   char invalid_data[invalid_buf_len] = "123456789";
//   const int64_t buf_len = 128;
//   char key[32] = "abababab";
//   char origin_data[buf_len] = "123456789";
//   char origin_data2[buf_len] = "12345678";
//   char data[buf_len] = "123456789";
//   char data2[buf_len] = "12345678";
//   int64_t invalid_data_len = strlen(invalid_data);
//   int64_t data_len = strlen(data);
//   ASSERT_EQ(OB_INVALID_ARGUMENT, ObDesEncryption::des_encrypt(invalid_key, invalid_data, invalid_data_len, invalid_buf_len));
//   ASSERT_EQ(OB_INVALID_ARGUMENT, ObDesEncryption::des_encrypt(invalid_key, data, data_len, invalid_buf_len));
//   ASSERT_EQ(OB_SUCCESS, ObDesEncryption::des_encrypt(key, data, data_len, buf_len));
//   ASSERT_EQ(OB_SUCCESS, ObDesEncryption::des_decrypt(key, data, 16));
//   ASSERT_EQ(0, STRNCMP(data, origin_data, strlen(origin_data)));
//   ASSERT_EQ(OB_SUCCESS, ObDesEncryption::des_encrypt(key, data2, data_len, buf_len));
//   ASSERT_EQ(OB_SUCCESS, ObDesEncryption::des_decrypt(key, data2, 8));
//   ASSERT_EQ(0, STRNCMP(data2, origin_data2, strlen(origin_data2)));
// }

TEST(TestEncryptionUtil, aes_encrypt)
{
  const int64_t buf_len = 128;
  char key[OB_MAX_MASTER_KEY_LENGTH] = {0};
  const int64_t key_len = OB_MAX_MASTER_KEY_LENGTH;
  char iv[buf_len] = {0};
  const int64_t iv_len = OB_MAX_MASTER_KEY_LENGTH;
  char data[buf_len] = {0};
  int64_t data_len = 16;
  char encrypt_buf[buf_len] = {0};
  int64_t encrypt_len = 0;
  char out_buf[buf_len] = {0};
  int64_t out_len = 0;

  for (int i = ObAesOpMode::ob_invalid_mode + 1; i < ObAesOpMode::ob_max_mode; ++i) {
    ObAesOpMode mode = static_cast<ObAesOpMode>(i);
    EXPECT_EQ(OB_SUCCESS, ObKeyGenerator::generate_encrypt_key(key, key_len));
    EXPECT_EQ(OB_SUCCESS, ObKeyGenerator::generate_encrypt_key(iv, iv_len));
    EXPECT_EQ(OB_SUCCESS, ObKeyGenerator::generate_encrypt_key(data, data_len));

    EXPECT_EQ(OB_SUCCESS, ObAesEncryption::aes_encrypt(key, key_len, data, data_len, buf_len,
                                                      iv, iv_len, mode,
                                                      encrypt_buf, encrypt_len));
    encrypt_buf[encrypt_len] = '\0';
    EXPECT_STRNE(data, encrypt_buf);
    EXPECT_EQ(OB_SUCCESS, ObAesEncryption::aes_decrypt(key, key_len, encrypt_buf, encrypt_len, buf_len,
                                                       iv, iv_len, mode,
                                                       out_buf, out_len));
    EXPECT_EQ(data_len, out_len);
    out_buf[out_len] = '\0';
    EXPECT_STREQ(data, out_buf);
  }
}

TEST(TestEncryptionUtil, encrypted_length)
{
  const int64_t buf_len = 128;
  char key[OB_MAX_MASTER_KEY_LENGTH] = {0};
  const int64_t key_len = OB_MAX_MASTER_KEY_LENGTH;
  char iv[buf_len] = {0};
  const int64_t iv_len = OB_MAX_MASTER_KEY_LENGTH;
  char data[buf_len] = {0};
  int64_t data_len = 0;
  char encrypt_buf[buf_len] = {0};
  int64_t encrypt_len = 0;


  for (int i = ObAesOpMode::ob_invalid_mode + 1; i < ObAesOpMode::ob_max_mode; ++i) {
    ObAesOpMode mode = static_cast<ObAesOpMode>(i);
    for (data_len = 1; data_len <= 2 * ObAesEncryption::OB_AES_BLOCK_SIZE; ++data_len) {
      EXPECT_EQ(OB_SUCCESS, ObKeyGenerator::generate_encrypt_key(key, key_len));
      EXPECT_EQ(OB_SUCCESS, ObKeyGenerator::generate_encrypt_key(iv, iv_len));
      EXPECT_EQ(OB_SUCCESS, ObKeyGenerator::generate_encrypt_key(data, data_len));
      EXPECT_EQ(OB_SUCCESS, ObAesEncryption::aes_encrypt(key, key_len, data, data_len, buf_len,
                                                         iv, iv_len, mode,
                                                         encrypt_buf, encrypt_len));

      EXPECT_GE(ObEncryptionUtil::encrypted_length(data_len), encrypt_len);
    }
  }
}

TEST(TestEncryptionUtil, decrypted_length)
{
  const int64_t buf_len = 128;
  char key[OB_MAX_MASTER_KEY_LENGTH] = {0};
  const int64_t key_len = OB_MAX_MASTER_KEY_LENGTH;
  char iv[buf_len] = {0};
  const int64_t iv_len = OB_MAX_MASTER_KEY_LENGTH;
  char data[buf_len] = {0};
  int64_t data_len = 0;
  char encrypt_buf[buf_len] = {0};
  int64_t encrypt_len = 0;
  int64_t target_encrypt_len = 0;


  for (int i = ObAesOpMode::ob_invalid_mode + 1; i < ObAesOpMode::ob_max_mode; ++i) {
    ObAesOpMode mode = static_cast<ObAesOpMode>(i);
    for (target_encrypt_len = ObAesEncryption::OB_AES_BLOCK_SIZE;
         target_encrypt_len <= 3 * ObAesEncryption::OB_AES_BLOCK_SIZE; ++target_encrypt_len) {
      data_len = ObEncryptionUtil::decrypted_length(target_encrypt_len);
      EXPECT_EQ(OB_SUCCESS, ObKeyGenerator::generate_encrypt_key(key, key_len));
      EXPECT_EQ(OB_SUCCESS, ObKeyGenerator::generate_encrypt_key(iv, iv_len));
      EXPECT_EQ(OB_SUCCESS, ObKeyGenerator::generate_encrypt_key(data, data_len));
      EXPECT_EQ(OB_SUCCESS, ObAesEncryption::aes_encrypt(key, key_len, data, data_len, buf_len,
                                                         iv, iv_len, mode,
                                                         encrypt_buf, encrypt_len));

      EXPECT_LE(encrypt_len, target_encrypt_len);
    }
  }
}

TEST(TestEncryptionUtil, safe_buffer_length)
{
  const int64_t buf_len = 128;
  char key[OB_MAX_MASTER_KEY_LENGTH] = {0};
  const int64_t key_len = OB_MAX_MASTER_KEY_LENGTH;
  char iv[buf_len] = {0};
  const int64_t iv_len = OB_MAX_MASTER_KEY_LENGTH;
  char data[buf_len] = {0};
  int64_t data_len = 0;
  char encrypt_buf[buf_len] = {0};
  int64_t encrypt_len = 0;


  for (int i = ObAesOpMode::ob_invalid_mode + 1; i < ObAesOpMode::ob_max_mode; ++i) {
    ObAesOpMode mode = static_cast<ObAesOpMode>(i);
    for (data_len = 1; data_len <= 2 * ObAesEncryption::OB_AES_BLOCK_SIZE; ++data_len) {
      EXPECT_EQ(OB_SUCCESS, ObKeyGenerator::generate_encrypt_key(key, key_len));
      EXPECT_EQ(OB_SUCCESS, ObKeyGenerator::generate_encrypt_key(iv, iv_len));
      EXPECT_EQ(OB_SUCCESS, ObKeyGenerator::generate_encrypt_key(data, data_len));
      EXPECT_EQ(OB_SUCCESS, ObAesEncryption::aes_encrypt(key, key_len, data, data_len, buf_len,
                                                        iv, iv_len, mode,
                                                        encrypt_buf, encrypt_len));

      EXPECT_GE(ObEncryptionUtil::safe_buffer_length(encrypt_len), data_len);
    }
  }
}

TEST(TestEncryptionUtil, encrypt_master_key)
{
  const int64_t buf_len = 128;
  char data[buf_len] = {0};
  int64_t data_len = OB_MAX_MASTER_KEY_LENGTH;
  char encrypt_buf[buf_len] = {0};
  int64_t encrypt_len = 0;
  char out_buf[buf_len] = {0};
  int64_t out_len = 0;

  EXPECT_EQ(OB_SUCCESS, ObKeyGenerator::generate_encrypt_key(data, data_len));
  EXPECT_EQ(OB_SUCCESS, ObEncryptionUtil::encrypt_master_key(data, data_len,
                                                             encrypt_buf, buf_len, encrypt_len));
  EXPECT_LE(encrypt_len, OB_MAX_ENCRYPTED_KEY_LENGTH);
  encrypt_buf[encrypt_len] = '\0';
  EXPECT_STRNE(data, encrypt_buf);
  EXPECT_EQ(OB_SUCCESS, ObEncryptionUtil::decrypt_master_key(encrypt_buf, encrypt_len,
                                                             out_buf, buf_len, out_len));
  EXPECT_EQ(data_len, out_len);
  out_buf[out_len] = '\0';
  EXPECT_STREQ(data, out_buf);
}

//TEST(TestWebService, store)
//{
//  ObWebServiceRootAddr ws;
//  ObSystemConfig sys_config;
//  ASSERT_EQ(OB_SUCCESS, sys_config.init());
//  ObServerConfig &config = ObServerConfig::get_instance();
//  ASSERT_EQ(OB_SUCCESS, config.init(sys_config));
//  ws.init(config);
//  config.obconfig_url.set_value("");
//  config.cluster_id.set_value("1");
//  config.cluster.set_value("xr.admin");
//  ObArray<ObRootAddr> rs_list;
//  ObArray<ObRootAddr> readonly_rs_list;
//  for (int64_t i = 0; i < 10; i++) {
//    ObRootAddr rs;
//    rs.server_.set_ip_addr("127.0.0.1", 9988);
//    rs.sql_port_ = 1;
//    ASSERT_EQ(OB_SUCCESS, rs_list.push_back(rs));
//  }
//  for (int64_t i = 0; i < 5; i++) {
//    ObRootAddr rs;
//    rs.server_.set_ip_addr("127.0.0.1", 9988);
//    rs.sql_port_ = 1;
//    ASSERT_EQ(OB_SUCCESS, readonly_rs_list.push_back(rs));
//  }
//  ASSERT_EQ(OB_SUCCESS, ws.store(rs_list, readonly_rs_list, true));
//  for (int64_t i = 0; i < 800; i++) {
//    ObRootAddr rs;
//    rs.server_.set_ip_addr("127.0.0.1", 9988);
//    rs.sql_port_ = 1;
//    ASSERT_EQ(OB_SUCCESS, rs_list.push_back(rs));
//  }
//  for (int64_t i = 0; i < 300; i++) {
//    ObRootAddr rs;
//    rs.server_.set_ip_addr("127.0.0.1", 9988);
//    rs.sql_port_ = 1;
//    ASSERT_EQ(OB_SUCCESS, readonly_rs_list.push_back(rs));
//  }
//  ASSERT_EQ(OB_OBCONFIG_RETURN_ERROR, ws.store(rs_list, readonly_rs_list, true));
//
//}
} // end namespace share
} // end namespace oceanbase
int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
