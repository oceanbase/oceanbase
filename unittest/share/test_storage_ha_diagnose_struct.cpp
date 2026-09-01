/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <gtest/gtest.h>
#include "share/ob_storage_ha_diagnose_struct.h"

namespace oceanbase
{
namespace share
{

TEST(TestStorageHADiagnoseStruct, result_msg)
{
  EXPECT_STREQ("SUCCESS", ha_diag_result_msg(OB_SUCCESS, ObStorageHACostItemName::MAX_NAME));
  EXPECT_STREQ("SUCCESS", ha_diag_result_msg(OB_SUCCESS, ObStorageHACostItemName::TX_BACKFILL));
  EXPECT_STREQ("TX_BACKFILL_ERROR",
      ha_diag_result_msg(OB_ERR_UNEXPECTED, ObStorageHACostItemName::TX_BACKFILL));
  EXPECT_STREQ("Unstatistical errors",
      ha_diag_result_msg(OB_ERR_UNEXPECTED, ObStorageHACostItemName::MAX_NAME));
}

} // namespace share
} // namespace oceanbase

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
