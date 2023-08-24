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
#include "storage/blocksstable/ob_micro_block_encryption.h"
#include "storage/blocksstable/ob_block_sstable_struct.h"
#include "storage/blocksstable/ob_data_buffer.h"
#include "share/ob_master_key_getter.h"

#define randmod(x) rand()%x
#define private public
using namespace oceanbase::common;
using namespace oceanbase::blocksstable;
using namespace oceanbase::share;

namespace oceanbase
{
namespace unittest
{

class TestMicroBlockEncryption : public ::testing::Test
{
public:

  virtual void SetUp();
  virtual void TearDown();
  void random_string_encrypt();
  void test_header_serialize();
private:
  int64_t tenant_id_ = 1;
  int64_t master_key_id_ = 123;
  char master_key_[OB_MAX_MASTER_KEY_LENGTH] = "12345";
  char raw_key_[OB_MAX_ENCRYPTION_KEY_NAME_LENGTH] = "54321";
  char encrypt_key_[OB_MAX_ENCRYPTION_KEY_NAME_LENGTH];
  int64_t encrypt_key_len_;
};

void TestMicroBlockEncryption::SetUp()
{
  ASSERT_EQ(OB_SUCCESS, share::ObMasterKeyGetter::instance().init(nullptr));
  ASSERT_EQ(OB_SUCCESS, share::ObMasterKeyGetter::instance().set_master_key(
      tenant_id_, master_key_id_, master_key_, strlen(master_key_)));
  ObCipherOpMode mode = ObCipherOpMode::ob_invalid_mode;
  ASSERT_EQ(OB_SUCCESS, share::ObMasterKeyGetter::get_table_key_algorithm(tenant_id_, mode));
  ASSERT_EQ(OB_SUCCESS, share::ObBlockCipher::encrypt(master_key_, strlen(master_key_), raw_key_,
                        strlen(raw_key_), OB_MAX_ENCRYPTION_KEY_NAME_LENGTH, nullptr, 0, nullptr, 0,
                        0, mode, encrypt_key_, encrypt_key_len_, nullptr));
}

void TestMicroBlockEncryption::TearDown()
{
  share::ObMasterKeyGetter::instance().destroy();
}

void TestMicroBlockEncryption::random_string_encrypt()
{
  int ret = OB_SUCCESS;
  srand((unsigned)time(NULL));
  int64_t index = 0;
  while (index < 1000) {
    //1000次随机值加解密
    ObMicroBlockEncryption block_encrypt;
    int64_t encrypt_id = randmod(4);
    int64_t buf_len = randmod(1024) + 1;
    char *buf = new char[buf_len];
    for (int i = 0; i < buf_len; ++i) {
      char tmp = 'a';
      if (rand() % 2 == 1) {
        tmp = 'A';
      }
      buf[i] = tmp + randmod(26);
    }
    const char *encrypt_buf = NULL;
    int64_t encrypt_len = 0;

    ret = block_encrypt.init(encrypt_id, tenant_id_, master_key_id_, encrypt_key_, encrypt_key_len_);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = block_encrypt.encrypt(buf, buf_len,  encrypt_buf, encrypt_len);
    ASSERT_EQ(ret, OB_SUCCESS);
    const char *decrypt_buf = NULL;
    int64_t decrypt_len = 0;
    ret = block_encrypt.decrypt(encrypt_buf, encrypt_len, decrypt_buf, decrypt_len);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(buf_len, decrypt_len);
    ASSERT_EQ(0, strncmp(decrypt_buf, buf, buf_len));
    index++;
    delete []buf;
  }
}



TEST_F(TestMicroBlockEncryption , basic_test)
{
  int ret = OB_SUCCESS;
  int64_t encrypt_id = 0;
  char input_buf[15];
  const char *out_buf = NULL;
  int64_t out_size = 0;
  int64_t de_size = 0;
  const char *de_buf = NULL;
  MEMSET(input_buf, 0, sizeof(input_buf));
  MEMCPY(input_buf, "oceanbase", sizeof("oceanbase"));
  ObMicroBlockEncryption block_encrypt;
  ret = block_encrypt.init(encrypt_id, tenant_id_, master_key_id_, encrypt_key_, encrypt_key_len_);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = block_encrypt.encrypt(input_buf, sizeof(input_buf), out_buf, out_size);
  //encrrypt_id为0,此时不执行加密,直接赋值和地址给buf.
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ((void *)input_buf, (void *)out_buf);
  ASSERT_EQ(out_size, sizeof(input_buf));
  ret = block_encrypt.decrypt(out_buf, out_size, de_buf, de_size);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ((void *)de_buf, (void *)out_buf);
  ASSERT_EQ(out_size, de_size);

  (void)random_string_encrypt();
}

}//end namespace unittest
}//end namespace oceanbase

int main(int argc, char **argv)
{
  OB_LOGGER.set_log_level("WARN");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
