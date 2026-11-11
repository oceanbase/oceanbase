/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "gtest/gtest.h"
#define private public
#include "rootserver/standby/ob_recovery_ls_service.h"
#undef private

namespace oceanbase
{
namespace rootserver
{

TEST(TestRecoveryLSService, semi_sync_disable_barrier_tracks_cfg_check_and_target_scn_by_epoch)
{
  ObRecoveryLSService::ObSemiSyncDisableBarrier barrier;
  const int64_t switchover_epoch = 100;
  share::SCN target_scn;

  ASSERT_EQ(OB_SUCCESS, target_scn.convert_for_logservice(1000));
  EXPECT_FALSE(barrier.is_cfg_checked_in_epoch(switchover_epoch));
  EXPECT_FALSE(barrier.has_target_scn_in_epoch(switchover_epoch));

  barrier.mark_cfg_checked(switchover_epoch);
  EXPECT_TRUE(barrier.is_cfg_checked_in_epoch(switchover_epoch));
  EXPECT_FALSE(barrier.is_cfg_checked_in_epoch(switchover_epoch + 1));
  EXPECT_FALSE(barrier.is_cfg_checked_in_epoch(OB_INVALID_VERSION));
  EXPECT_FALSE(barrier.has_target_scn_in_epoch(switchover_epoch));

  EXPECT_EQ(OB_STATE_NOT_MATCH, barrier.set_target_scn(switchover_epoch + 1, target_scn));
  EXPECT_FALSE(barrier.has_target_scn_in_epoch(switchover_epoch));

  share::SCN invalid_target_scn;
  EXPECT_EQ(OB_INVALID_ARGUMENT, barrier.set_target_scn(switchover_epoch, invalid_target_scn));
  EXPECT_FALSE(barrier.has_target_scn_in_epoch(switchover_epoch));

  ASSERT_EQ(OB_SUCCESS, barrier.set_target_scn(switchover_epoch, target_scn));
  EXPECT_TRUE(barrier.is_cfg_checked_in_epoch(switchover_epoch));
  EXPECT_TRUE(barrier.has_target_scn_in_epoch(switchover_epoch));
  EXPECT_FALSE(barrier.has_target_scn_in_epoch(switchover_epoch + 1));

  barrier.reset();
  EXPECT_FALSE(barrier.is_cfg_checked_in_epoch(switchover_epoch));
  EXPECT_FALSE(barrier.has_target_scn_in_epoch(switchover_epoch));
}

} // namespace rootserver
} // namespace oceanbase

int main(int argc, char **argv)
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
