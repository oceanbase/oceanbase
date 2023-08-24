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
#include <gtest/gtest.h>
#include "share/config/ob_config.h"
#include "share/config/ob_common_config.h"
#include "share/ob_define.h"

using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase;
/* using namespace oceanbase::common::hash; */
/*  */
class TestLogArchiveConfig : public ::testing::Test
{

};

TEST_F(TestLogArchiveConfig, log_archive)
{
  ObConfigLogArchiveOptionsItem log_archive_item;
  bool ret = log_archive_item.set_value("MANDATORY COMPRESSION=lz4_1.0");
  ASSERT_EQ(true, log_archive_item.value_.valid_);
  ASSERT_EQ(true, log_archive_item.value_.is_mandatory_);
  ASSERT_EQ(true, log_archive_item.value_.is_compress_enabled_);
  ASSERT_EQ(LZ4_COMPRESSOR, log_archive_item.value_.compressor_type_);

  ret = log_archive_item.set_value("MANDATORY COMPRESSION=lz4_1.0 yyy");
  ASSERT_EQ(false, log_archive_item.value_.valid_);

  ret = log_archive_item.set_value("COMPRESSION=lz4_1.0");
  ASSERT_EQ(true, log_archive_item.value_.valid_);
  ASSERT_EQ(true, log_archive_item.value_.is_mandatory_);
  ASSERT_EQ(true, log_archive_item.value_.is_compress_enabled_);
  ASSERT_EQ(share::ObBackupEncryptionMode::NONE, log_archive_item.value_.encryption_mode_);
  ASSERT_EQ(LZ4_COMPRESSOR, log_archive_item.value_.compressor_type_);

  ret = log_archive_item.set_value("COMPRESSION=lz4_1.0 OPTIONAL ");
  ASSERT_EQ(true, log_archive_item.value_.valid_);
  ASSERT_EQ(false, log_archive_item.value_.is_mandatory_);
  ASSERT_EQ(true, log_archive_item.value_.is_compress_enabled_);
  ASSERT_EQ(LZ4_COMPRESSOR, log_archive_item.value_.compressor_type_);

  ret = log_archive_item.set_value("COMPRESSION=disable OPTIONAL ");
  ASSERT_EQ(true, log_archive_item.value_.valid_);
  ret = log_archive_item.set_value("COMPRESSION=enable OPTIONAL ");
  ASSERT_EQ(true, log_archive_item.value_.valid_);
  ret = log_archive_item.set_value("COMPRESSION=lz4_1.0 OPTIONAL ");
  ASSERT_EQ(true, log_archive_item.value_.valid_);
  ret = log_archive_item.set_value("COMPRESSION=zstd_1.3.8 OPTIONAL ");
  ASSERT_EQ(true, log_archive_item.value_.valid_);
  ret = log_archive_item.set_value("COMPRESSION=snappy_1.0 OPTIONAL ");
  ASSERT_EQ(false, log_archive_item.value_.valid_);

  ret = log_archive_item.set_value("");
  ASSERT_EQ(false, log_archive_item.value_.valid_);

  ret = log_archive_item.set_value(" optional COMPRESSION=disable");
  ASSERT_EQ(true, log_archive_item.value_.valid_);

  ret = log_archive_item.set_value("MANDATORY encryption_mode= transparent_encryption ENCRYPTION_ALGORITHM = aes-128");
  ASSERT_EQ(false, log_archive_item.value_.valid_);

//  ret = log_archive_item.set_value("MANDATORY encryption_mode= transparent_encryption ENCRYPTION_ALGORITHM = aes-128");
//  ASSERT_EQ(true, log_archive_item.value_.valid_);
//  ASSERT_EQ(true, log_archive_item.value_.is_mandatory_);
//  ASSERT_EQ(false, log_archive_item.value_.is_compress_enabled_);
//  ASSERT_EQ(ObBackupEncryptionMode::TRANSPARENT_ENCRYPTION, log_archive_item.value_.encryption_mode_);
//  ASSERT_EQ(share::ObCipherOpMode::ob_aes_128_ecb, log_archive_item.value_.encryption_algorithm_);
//
//  ret = log_archive_item.set_value("MANDATORY encryption_mode= none ENCRYPTION_ALGORITHM = aes-128");
//  ASSERT_EQ(true, log_archive_item.value_.valid_);
//  ASSERT_EQ(true, log_archive_item.value_.is_mandatory_);
//  ASSERT_EQ(false, log_archive_item.value_.is_compress_enabled_);
//  ASSERT_EQ(ObBackupEncryptionMode::NONE, log_archive_item.value_.encryption_mode_);
//  ASSERT_EQ(share::ObCipherOpMode::ob_aes_128_ecb, log_archive_item.value_.encryption_algorithm_);
//
//  ret = log_archive_item.set_value("MANDATORY encryption_mode= none ENCRYPTION_ALGORITHM = aes-192");
//  ASSERT_EQ(true, log_archive_item.value_.valid_);
//  ASSERT_EQ(ObBackupEncryptionMode::NONE, log_archive_item.value_.encryption_mode_);
//  ASSERT_EQ(share::ObCipherOpMode::ob_aes_192_ecb, log_archive_item.value_.encryption_algorithm_);
//
//  ret = log_archive_item.set_value("MANDATORY encryption_mode= none ENCRYPTION_ALGORITHM = aes-256");
//  ASSERT_EQ(true, log_archive_item.value_.valid_);
//  ASSERT_EQ(ObBackupEncryptionMode::NONE, log_archive_item.value_.encryption_mode_);
//  ASSERT_EQ(share::ObCipherOpMode::ob_aes_256_ecb, log_archive_item.value_.encryption_algorithm_);
//
//  ret = log_archive_item.set_value("MANDATORY encryption_mode= none ENCRYPTION_ALGORITHM = sm4");
//  ASSERT_EQ(true, log_archive_item.value_.valid_);
//  ASSERT_EQ(ObBackupEncryptionMode::NONE, log_archive_item.value_.encryption_mode_);
//  ASSERT_EQ(share::ObCipherOpMode::ob_sm4_mode, log_archive_item.value_.encryption_algorithm_);
//
//  ret = log_archive_item.set_value("encryption_mode= none ENCRYPTION_ALGORITHM = aes-120");
//  ASSERT_EQ(false, ret);
//  ASSERT_EQ(false, log_archive_item.value_.valid_);

}
int main(int argc, char* argv[])
{
  OB_LOGGER.set_file_name("test_config.log", true);
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
