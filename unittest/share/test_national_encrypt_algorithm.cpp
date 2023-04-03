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
#include "share/ob_encryption_util.h"
#define randmod(x) rand()%x
#define private public
using namespace oceanbase::common;
using namespace oceanbase::share;

class TestNationalEncrypt : public ::testing::Test
{
public:
 bool equal(char *A, char *B, int len);
};

bool TestNationalEncrypt::equal(char *A, char *B, int len)
{
  int ret = true;
  for (int i = 0; i < len; ++i) {
    if (A[i] != B[i]) {
      ret = false;
      break;
    }	    
  }
  return ret;
}
TEST_F(TestNationalEncrypt, basic_test)
{
  int ret = OB_SUCCESS;
  char input_buf[31];
  char encrypt_buf[100];
  char decrypt_buf[100];
  int64_t encrypt_len = 0;
  int64_t decrypt_len = 0;
  char key[16];
  MEMCPY(key, "oceanbaseforever", 16);
  MEMCPY(input_buf, "1234567890123456789012345678901", 31);
  ret = ObSm4Encryption::sm4_encrypt(key, 16, input_buf, 31, 100, encrypt_buf, encrypt_len);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_EQ(encrypt_len, 32);
  ret = ObSm4Encryption::sm4_decrypt(key, 16, encrypt_buf, 32, 100, decrypt_buf, decrypt_len);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_EQ(decrypt_len, 31);
  ASSERT_EQ(true, equal(decrypt_buf, input_buf, 31));
}
TEST_F(TestNationalEncrypt, random_test)
{
  //Random encryption and decryption test 20,000 times
  int ret = OB_SUCCESS;
  srand((unsigned)time(NULL));
  int64_t index = 20000;
  while (index--) {
    int64_t buf_len = randmod(1024) + 1;	  
    char *buf = new char[buf_len];
    char key[16];
    for (int i = 0; i < buf_len; ++i) {
      char tmp = 'a';
      if (rand() % 2 == 1) {
        tmp = 'A';
      }
      buf[i] = tmp + randmod(26);
    }
    for (auto &c : key) {
      c = 'a' + randmod(26);
    }
    char *encrypt_buf = new char[(buf_len/16 + 1) * 16];
    char *decrypt_buf = new char[(buf_len/16 + 1) * 16];
    int64_t encrypt_len = 0, decrypt_len = 0;
    ret = ObSm4Encryption::sm4_encrypt(key, 16, buf, buf_len, (buf_len/16 + 1) * 16, encrypt_buf, encrypt_len);
    ASSERT_EQ(ret, OB_SUCCESS);
    ASSERT_EQ(encrypt_len, (buf_len/16 + 1) * 16);
    ret = ObSm4Encryption::sm4_decrypt(key, 16, encrypt_buf, encrypt_len, (buf_len / 16 + 1) * 16, decrypt_buf, decrypt_len);   
    ASSERT_EQ(ret, OB_SUCCESS);
    ASSERT_EQ(decrypt_len, buf_len);
    ASSERT_EQ(true, equal(decrypt_buf, buf, buf_len));
    delete []buf;
    delete []encrypt_buf;
    delete []decrypt_buf;
  }
}




int main(int argc, char *argv[])
{
  OB_LOGGER.set_log_level("WARN");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
