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
#include "share/ob_encryption_util.h"
#include "share/ob_master_key_getter.h"
#undef private

namespace oceanbase
{
namespace share
{
using namespace common;

class TestMasterKeyGetter : public ::testing::Test
{
public:
  virtual void SetUp();
  virtual void TearDown();
};

void TestMasterKeyGetter::SetUp()
{
  int ret = ObMasterKeyGetter::instance().init(NULL);
  ASSERT_EQ(OB_SUCCESS, ret);
}

void TestMasterKeyGetter::TearDown()
{
  ObMasterKeyGetter::instance().destroy();
}

TEST_F(TestMasterKeyGetter, master_key)
{
  const int key_num = 3;
  char orig_key_list[][key_num][OB_MAX_MASTER_KEY_LENGTH] = {
    {"12345", "abcde", "54321"},
    {"67890", "edcba", "09876"},
    {"aaaaa", "12345", "ccccc"}
  };
  char *cur_key;
  char data[OB_MAX_MASTER_KEY_LENGTH] = {0};
  int64_t data_len = 0;
  int tenant_num = sizeof(orig_key_list) / sizeof(orig_key_list[0]);
  for (int i = 0; i < tenant_num; ++i) {
    for (int j = 0; j < key_num; ++j) {
      cur_key = orig_key_list[i][j];
      EXPECT_EQ(OB_SUCCESS, ObMasterKeyGetter::instance().set_master_key(i, j + 1, cur_key, strlen(cur_key)));
    }
  }
  for (int i = 0; i < tenant_num; ++i) {
    for (int j = 0; j < key_num; ++j) {
      memset(data, 0, OB_MAX_MASTER_KEY_LENGTH);
      EXPECT_EQ(OB_SUCCESS, ObMasterKeyGetter::get_master_key(i, j + 1, data, OB_MAX_MASTER_KEY_LENGTH, data_len));
      EXPECT_STREQ(data, orig_key_list[i][j]);
    }
  }
}


TEST_F(TestMasterKeyGetter, key_serialize)
{
  char orig_key_list[][OB_MAX_MASTER_KEY_LENGTH] = {
    "12345",
    "abcde",
    "54321",
  };
  ObMasterKey cur_key;
  ObMasterKey new_key;
  int key_num = sizeof(orig_key_list) / sizeof(orig_key_list[0]);

  int64_t buf_len = 2048;
  char buf[buf_len];
  int64_t pos = 0;
  int64_t deserialize_pos = 0;

  for (int i = 0; i < key_num; ++i) {
    cur_key.reset();
    cur_key.len_ = strlen(orig_key_list[i]);
    MEMCPY(cur_key.key_, orig_key_list[i], cur_key.len_);

    MEMSET(buf, 0, buf_len);
    pos = 0;
    deserialize_pos = 0;
    new_key.reset();
    EXPECT_EQ(OB_SUCCESS, cur_key.serialize(buf, buf_len, pos));
    EXPECT_EQ(OB_SUCCESS, new_key.deserialize(buf, pos, deserialize_pos));
    EXPECT_EQ(pos, new_key.get_serialize_size());

    EXPECT_EQ(cur_key.len_, new_key.len_);
    EXPECT_STREQ(cur_key.key_, new_key.key_);
  }
}

TEST_F(TestMasterKeyGetter, key_algorithm)
{
  uint64_t tenant_id = 1001;
  ObAesOpMode key_algorithm = ObAesOpMode::ob_invalid_mode;
  EXPECT_EQ(OB_SUCCESS, ObMasterKeyGetter::get_table_key_algorithm(tenant_id, key_algorithm));
  EXPECT_EQ(ObAesOpMode::ob_aes_128_ecb, key_algorithm);
  EXPECT_EQ(0, ObMasterKeyGetter::instance().tenant_table_key_algorithm_map_.size());
  EXPECT_EQ(OB_SUCCESS, ObMasterKeyGetter::instance().tenant_table_key_algorithm_map_.set_refactored(tenant_id, ObAesOpMode::ob_sm4_mode));
  EXPECT_EQ(1, ObMasterKeyGetter::instance().tenant_table_key_algorithm_map_.size());
  EXPECT_EQ(OB_SUCCESS, ObMasterKeyGetter::get_table_key_algorithm(tenant_id, key_algorithm));
  EXPECT_EQ(ObAesOpMode::ob_sm4_mode, key_algorithm);
}

TEST_F(TestMasterKeyGetter, key_getter_serialize)
{
  const int key_num = 3;
  char orig_key_list[][key_num][OB_MAX_MASTER_KEY_LENGTH] = {
    {"12345", "abcde", "54321"},
    {"67890", "edcba", "09876"},
    {"aaaaa", "12345", "ccccc"}
  };
  char *cur_key;
  char data[OB_MAX_MASTER_KEY_LENGTH] = {0};
  int64_t data_len = 0;
  int tenant_num = sizeof(orig_key_list) / sizeof(orig_key_list[0]);
  for (int i = 0; i < tenant_num; ++i) {
    for (int j = 0; j < key_num; ++j) {
      cur_key = orig_key_list[i][j];
      EXPECT_EQ(OB_SUCCESS, ObMasterKeyGetter::instance().set_master_key(i, j + 1, cur_key, strlen(cur_key)));
    }
  }

  int64_t buf_len = 2048;
  char buf[buf_len];
  int64_t pos = 0;
  EXPECT_EQ(OB_SUCCESS, ObMasterKeyGetter::instance().serialize(buf, buf_len, pos));
  ObMasterKeyGetter::instance().id_value_map_.reuse();
  EXPECT_EQ(0, ObMasterKeyGetter::instance().id_value_map_.size());

  int64_t pos_result = 0;
  EXPECT_EQ(OB_SUCCESS, ObMasterKeyGetter::instance().deserialize(buf, pos, pos_result));
  EXPECT_EQ(pos, pos_result);
  EXPECT_EQ(tenant_num * key_num, ObMasterKeyGetter::instance().id_value_map_.size());
  for (int i = 0; i < tenant_num; ++i) {
    for (int j = 0; j < key_num; ++j) {
      memset(data, 0, OB_MAX_MASTER_KEY_LENGTH);
      EXPECT_EQ(OB_SUCCESS, ObMasterKeyGetter::get_master_key(i, j + 1, data, OB_MAX_MASTER_KEY_LENGTH, data_len));
      EXPECT_STREQ(data, orig_key_list[i][j]);
    }
  }
}

TEST_F(TestMasterKeyGetter, dump2file)
{
  const int key_num = 3;
  char orig_key_list[][key_num][OB_MAX_MASTER_KEY_LENGTH] = {
    {"12345", "abcde", "54321"},
    {"67890", "edcba", "09876"},
    {"aaaaa", "12345", "ccccc"}
  };
  char *cur_key;
  char data[OB_MAX_MASTER_KEY_LENGTH] = {0};
  int64_t data_len = 0;
  int tenant_num = sizeof(orig_key_list) / sizeof(orig_key_list[0]);
  uint64_t key_version = 0;
  ObAesOpMode key_algorithm = ObAesOpMode::ob_invalid_mode;
  for (int i = 0; i < tenant_num; ++i) {
    for (int j = 0; j < key_num; ++j) {
      cur_key = orig_key_list[i][j];
      EXPECT_EQ(OB_SUCCESS, ObMasterKeyGetter::instance().set_master_key(i, j + 1, cur_key, strlen(cur_key)));
    }
  }
  EXPECT_EQ(OB_SUCCESS, ObMasterKeyGetter::instance().set_expect_version(0, key_num + 2));
  EXPECT_EQ(OB_SUCCESS, ObMasterKeyGetter::instance().set_max_stored_version(0, key_num));
  EXPECT_EQ(OB_SUCCESS, ObMasterKeyGetter::instance().set_max_active_version(0, key_num - 2));

  EXPECT_EQ(OB_SUCCESS, ObMasterKeyGetter::instance().tenant_table_key_algorithm_map_.set_refactored(1, ObAesOpMode::ob_sm4_mode));
  EXPECT_EQ(OB_SUCCESS, ObMasterKeyGetter::instance().tenant_table_key_algorithm_map_.set_refactored(2, ObAesOpMode::ob_aes_128_ecb));

  const char *keystore_file = "wallet/wallet.bin";
  EXPECT_EQ(OB_SUCCESS, ObMasterKeyGetter::instance().dump2file(keystore_file));
  ObMasterKeyGetter::instance().id_value_map_.reuse();
  ObMasterKeyGetter::instance().tenant_key_version_map_.reuse();
  ObMasterKeyGetter::instance().tenant_table_key_algorithm_map_.reuse();
  EXPECT_EQ(0, ObMasterKeyGetter::instance().id_value_map_.size());
  EXPECT_EQ(OB_SUCCESS, ObMasterKeyGetter::instance().load_key(keystore_file));
  EXPECT_EQ(tenant_num * key_num, ObMasterKeyGetter::instance().id_value_map_.size());
  for (int i = 0; i < tenant_num; ++i) {
    for (int j = 0; j < key_num; ++j) {
      memset(data, 0, OB_MAX_MASTER_KEY_LENGTH);
      EXPECT_EQ(OB_SUCCESS, ObMasterKeyGetter::get_master_key(i, j + 1, data, OB_MAX_MASTER_KEY_LENGTH, data_len));
      EXPECT_STREQ(data, orig_key_list[i][j]);
    }
  }
  EXPECT_EQ(tenant_num, ObMasterKeyGetter::instance().tenant_key_version_map_.size());
  EXPECT_EQ(OB_SUCCESS, ObMasterKeyGetter::instance().get_max_active_version(0, key_version));
  EXPECT_EQ(key_num - 2, key_version);
  EXPECT_EQ(OB_SUCCESS, ObMasterKeyGetter::instance().get_max_stored_version(0, key_version));
  EXPECT_EQ(key_num, key_version);
  EXPECT_EQ(OB_SUCCESS, ObMasterKeyGetter::instance().get_expect_version(0, key_version));
  EXPECT_EQ(key_num + 2, key_version);

  EXPECT_EQ(2, ObMasterKeyGetter::instance().tenant_table_key_algorithm_map_.size());
  EXPECT_EQ(OB_SUCCESS, ObMasterKeyGetter::get_table_key_algorithm(1, key_algorithm));
  EXPECT_EQ(ObAesOpMode::ob_sm4_mode, key_algorithm);
  EXPECT_EQ(OB_SUCCESS, ObMasterKeyGetter::get_table_key_algorithm(2, key_algorithm));
  EXPECT_EQ(ObAesOpMode::ob_aes_128_ecb, key_algorithm);
}

// TEST_F(TestMasterKeyGetter, compat)
// {
//   const char *keystore_file = "old_wallet.test";
//   EXPECT_EQ(OB_SUCCESS, ObMasterKeyGetter::instance().load_key(keystore_file));
//   EXPECT_EQ(2, ObMasterKeyGetter::instance().id_value_map_.size());
//   EXPECT_EQ(1, ObMasterKeyGetter::instance().tenant_key_version_map_.size());
//   EXPECT_EQ(0, ObMasterKeyGetter::instance().tenant_table_key_algorithm_map_.size());
// }

} // end namespace share
} // end namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -rf test_master_key_getter.log* wallet");
  oceanbase::common::ObLogger::get_logger().set_file_name("test_master_key_getter.log", true);
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
