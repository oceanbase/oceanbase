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
