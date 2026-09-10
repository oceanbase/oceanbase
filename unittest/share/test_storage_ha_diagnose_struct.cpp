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
#include "share/ob_storage_ha_diagnose_struct.h"

namespace oceanbase
{
namespace share
{

TEST(TestStorageHADiagnoseStruct, successful_result_msg)
{
  ObTransferPerfDiagInfo info;
  ASSERT_EQ(OB_SUCCESS, info.result_code_);
  ASSERT_EQ(ObStorageHACostItemName::MAX_NAME, info.result_msg_);
  EXPECT_STREQ("SUCCESS", info.get_transfer_error_diagnose_msg());

  // The result code takes precedence over a previously recorded error item.
  info.result_msg_ = ObStorageHACostItemName::TX_BACKFILL;
  EXPECT_STREQ("SUCCESS", info.get_transfer_error_diagnose_msg());
}

TEST(TestStorageHADiagnoseStruct, known_error_result_msg)
{
  ObTransferErrorDiagInfo info;
  info.result_code_ = OB_ERR_UNEXPECTED;
  info.result_msg_ = ObStorageHACostItemName::TX_BACKFILL;
  EXPECT_STREQ("TX_BACKFILL_ERROR", info.get_transfer_error_diagnose_msg());
}

TEST(TestStorageHADiagnoseStruct, unknown_error_result_msg)
{
  ObTransferErrorDiagInfo info;
  info.result_code_ = OB_ERR_UNEXPECTED;
  EXPECT_STREQ("Unstatistical errors", info.get_transfer_error_diagnose_msg());
  info.result_msg_ = static_cast<ObStorageHACostItemName>(-1);
  EXPECT_STREQ("Unstatistical errors", info.get_transfer_error_diagnose_msg());
  info.result_msg_ = static_cast<ObStorageHACostItemName>(static_cast<int>(ObStorageHACostItemName::MAX_NAME) + 1);
  EXPECT_STREQ("Unstatistical errors", info.get_transfer_error_diagnose_msg());
}

} // namespace share
} // namespace oceanbase

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
