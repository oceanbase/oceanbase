// Copyright (c) 2021 OceanBase
// SPDX-License-Identifier: Apache-2.0

#include <gtest/gtest.h>
#include "deps/oblib/src/lib/hash/ob_hashmap.h"
#define private public
#define protected public
#define USING_LOG_PREFIX TRANS
#include "storage/tx/ob_dup_table_lease.h"
#include "storage/tx/ob_dup_table_util.h"
#undef private
#undef protected

namespace oceanbase
{
using namespace transaction;

namespace unittest
{

class TestDupTableLease : public ::testing::Test
{
public:
  virtual void SetUp() {}
  virtual void TearDown() {}
public:
};

TEST_F(TestDupTableLease, test_dup_table_lease_log)
{

}

TEST_F(TestDupTableLease, block_clear_and_cleanup)
{
  ObDupTableLSLeaseMgr lease_mgr;
  bool is_lease_valid = true;

  EXPECT_FALSE(lease_mgr.is_lease_blocked_by_migration_);
  EXPECT_EQ(OB_SUCCESS, lease_mgr.block_dup_table_lease_for_migration(is_lease_valid));
  EXPECT_TRUE(lease_mgr.is_lease_blocked_by_migration_);
  EXPECT_FALSE(is_lease_valid);

  is_lease_valid = true;
  EXPECT_EQ(OB_SUCCESS, lease_mgr.block_dup_table_lease_for_migration(is_lease_valid));
  EXPECT_TRUE(lease_mgr.is_lease_blocked_by_migration_);
  EXPECT_FALSE(is_lease_valid);

  EXPECT_EQ(OB_SUCCESS, lease_mgr.clear_dup_table_lease_block_for_migration());
  EXPECT_FALSE(lease_mgr.is_lease_blocked_by_migration_);
  EXPECT_EQ(OB_SUCCESS, lease_mgr.clear_dup_table_lease_block_for_migration());
  EXPECT_FALSE(lease_mgr.is_lease_blocked_by_migration_);

  EXPECT_EQ(OB_SUCCESS, lease_mgr.block_dup_table_lease_for_migration(is_lease_valid));
  lease_mgr.offline();
  EXPECT_FALSE(lease_mgr.is_lease_blocked_by_migration_);

  EXPECT_EQ(OB_SUCCESS, lease_mgr.block_dup_table_lease_for_migration(is_lease_valid));
  lease_mgr.reset();
  EXPECT_FALSE(lease_mgr.is_lease_blocked_by_migration_);
}

TEST_F(TestDupTableLease, block_preserves_current_follower_lease)
{
  ObDupTableLSLeaseMgr lease_mgr;
  const int64_t now = ObTimeUtility::current_time();
  const int64_t request_ts = now - 1000;
  const int64_t lease_interval = ObDupTableLSLeaseMgr::DEFAULT_LEASE_INTERVAL;
  const int64_t lease_expired_ts = now + lease_interval;
  const share::SCN last_lease_scn = share::SCN::base_scn();
  const share::SCN lease_acquire_scn = share::SCN::base_scn();
  bool is_lease_valid = false;

  lease_mgr.follower_lease_info_.durable_lease_.request_ts_ = request_ts;
  lease_mgr.follower_lease_info_.durable_lease_.lease_interval_us_ = lease_interval;
  lease_mgr.follower_lease_info_.durable_lease_.flag_.flag_bit_.is_new_lease_ = true;
  lease_mgr.follower_lease_info_.lease_expired_ts_ = lease_expired_ts;
  lease_mgr.follower_lease_info_.last_lease_scn_ = last_lease_scn;
  lease_mgr.follower_lease_info_.lease_acquire_scn_ = lease_acquire_scn;

  EXPECT_EQ(OB_SUCCESS, lease_mgr.block_dup_table_lease_for_migration(is_lease_valid));
  EXPECT_TRUE(is_lease_valid);
  EXPECT_TRUE(lease_mgr.is_lease_blocked_by_migration_);
  EXPECT_EQ(request_ts, lease_mgr.follower_lease_info_.durable_lease_.request_ts_);
  EXPECT_EQ(lease_interval, lease_mgr.follower_lease_info_.durable_lease_.lease_interval_us_);
  EXPECT_TRUE(lease_mgr.follower_lease_info_.durable_lease_.flag_.flag_bit_.is_new_lease_);
  EXPECT_EQ(lease_expired_ts, lease_mgr.follower_lease_info_.lease_expired_ts_);
  EXPECT_EQ(last_lease_scn, lease_mgr.follower_lease_info_.last_lease_scn_);
  EXPECT_EQ(lease_acquire_scn, lease_mgr.follower_lease_info_.lease_acquire_scn_);
}

TEST_F(TestDupTableLease, block_reports_false_for_expired_or_leader_lease)
{
  ObDupTableLSLeaseMgr lease_mgr;
  bool is_lease_valid = true;

  lease_mgr.follower_lease_info_.lease_expired_ts_ = ObTimeUtility::current_time() - 1;
  EXPECT_EQ(OB_SUCCESS, lease_mgr.block_dup_table_lease_for_migration(is_lease_valid));
  EXPECT_FALSE(is_lease_valid);
  EXPECT_TRUE(lease_mgr.is_lease_blocked_by_migration_);

  EXPECT_EQ(OB_SUCCESS, lease_mgr.clear_dup_table_lease_block_for_migration());
  ATOMIC_STORE(&lease_mgr.is_master_, true);
  is_lease_valid = true;
  EXPECT_EQ(OB_SUCCESS, lease_mgr.block_dup_table_lease_for_migration(is_lease_valid));
  EXPECT_FALSE(is_lease_valid);
  EXPECT_TRUE(lease_mgr.is_lease_blocked_by_migration_);
}

TEST_F(TestDupTableLease, leader_takeover_clears_block)
{
  ObDupTableLSLeaseMgr lease_mgr;
  bool is_lease_valid = false;

  EXPECT_EQ(OB_SUCCESS, lease_mgr.block_dup_table_lease_for_migration(is_lease_valid));
  EXPECT_TRUE(lease_mgr.is_lease_blocked_by_migration_);

  EXPECT_EQ(OB_SUCCESS, lease_mgr.leader_takeover(true /*is_resume*/));
  EXPECT_FALSE(lease_mgr.is_lease_blocked_by_migration_);
}

TEST_F(TestDupTableLease, handler_uninitialized_block_and_clear_are_noop)
{
  ObDupTableLSHandler handler;
  bool is_lease_valid = true;

  EXPECT_EQ(OB_SUCCESS, handler.block_dup_table_lease_for_migration(is_lease_valid));
  EXPECT_FALSE(is_lease_valid);
  EXPECT_EQ(OB_SUCCESS, handler.clear_dup_table_lease_block_for_migration());
}

TEST_F(TestDupTableLease, handler_block_without_dup_tablet_keeps_future_block)
{
  ObDupTableLSHandler handler;
  ObDupTableLSLeaseMgr lease_mgr;
  bool is_lease_valid = true;

  handler.is_inited_ = true;
  handler.lease_mgr_ptr_ = &lease_mgr;
  handler.tablets_mgr_ptr_ = nullptr;

  EXPECT_EQ(OB_SUCCESS, handler.block_dup_table_lease_for_migration(is_lease_valid));
  EXPECT_FALSE(is_lease_valid);
  EXPECT_TRUE(lease_mgr.is_lease_blocked_by_migration_);
  EXPECT_EQ(OB_SUCCESS, handler.clear_dup_table_lease_block_for_migration());
  EXPECT_FALSE(lease_mgr.is_lease_blocked_by_migration_);

  handler.is_inited_ = false;
  handler.lease_mgr_ptr_ = nullptr;
}

}

} // namespace oceanbase

using namespace oceanbase;

int main(int argc, char **argv)
{
  int ret = 1;
  ObLogger &logger = ObLogger::get_logger();
  logger.set_file_name("test_dup_table_lease.log", true);
  logger.set_log_level(OB_LOG_LEVEL_INFO);
  testing::InitGoogleTest(&argc, argv);
  ret = RUN_ALL_TESTS();
  return ret;
}
