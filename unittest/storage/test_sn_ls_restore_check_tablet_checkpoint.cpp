/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

// Unit test for the restore-side tablet clog_checkpoint check policy.
// Verifies that the LS_TX_CTX_TABLET (49401) is exempted from the
// "clog_checkpoint_scn <= restore_scn" requirement, while other inner
// tablets and user tablets are not.
//
// Related code: src/storage/restore/ob_sn_ls_restore_state.cpp
//   ObLSQuickRestoreState::is_tablet_clog_checkpoint_acceptable_for_restore
//
// Background:
//   ObTxCtxMemtable::flush() sets the freeze sstable's end_scn to
//   MAX(max_consequent_callbacked_scn, max_end_scn_ + 1), introducing a
//   +1 bookkeeping offset that propagates to the tx ctx tablet's
//   clog_checkpoint_scn. Since the tx ctx sstable carries no real
//   log/data scn, this offset must not cause restore to fail with -4016.

#include <gtest/gtest.h>
// ObILSRestoreState's TO_STRING_KV(K_(*ls), ...) instantiates a printer for
// ObLS at parse time, so a full ObLS definition must precede the restore
// state header in any TU that includes it.
#include "storage/ls/ob_ls.h"
#include "storage/restore/ob_sn_ls_restore_state.h"
#include "common/ob_tablet_id.h"
#include "share/scn.h"

namespace oceanbase
{
namespace storage
{

using common::ObTabletID;
using share::SCN;

namespace
{

SCN make_scn(uint64_t v)
{
  SCN s;
  s.convert_for_logservice(v);
  return s;
}

const ObTabletID TX_CTX_TABLET(ObTabletID::LS_TX_CTX_TABLET_ID);     // 49401
const ObTabletID TX_DATA_TABLET(ObTabletID::LS_TX_DATA_TABLET_ID);   // 49402
const ObTabletID LOCK_TABLET(ObTabletID::LS_LOCK_TABLET_ID);         // 49403
const ObTabletID REORG_TABLET(ObTabletID::LS_REORG_INFO_TABLET_ID);  // 49404
const ObTabletID USER_TABLET(200001);                                // user data tablet

} // anonymous namespace

class TestSnLsRestoreCheckTabletCheckpoint : public ::testing::Test
{
};

// Baseline: clog_checkpoint_scn <= restore_scn is always acceptable for any tablet.
TEST_F(TestSnLsRestoreCheckTabletCheckpoint, checkpoint_le_restore_is_always_ok)
{
  const SCN restore_scn = make_scn(1000);

  const ObTabletID kinds[] = {TX_CTX_TABLET, TX_DATA_TABLET, LOCK_TABLET, REORG_TABLET, USER_TABLET};
  for (const ObTabletID &tid : kinds) {
    EXPECT_TRUE(ObLSQuickRestoreState::is_tablet_clog_checkpoint_acceptable_for_restore(
        tid, make_scn(500), restore_scn)) << "tablet_id=" << tid.id();
    EXPECT_TRUE(ObLSQuickRestoreState::is_tablet_clog_checkpoint_acceptable_for_restore(
        tid, make_scn(1000), restore_scn)) << "tablet_id=" << tid.id() << " (equal boundary)";
  }
}

// Core exemption: tx ctx tablet (49401) with checkpoint > restore_scn is acceptable.
TEST_F(TestSnLsRestoreCheckTabletCheckpoint, tx_ctx_tablet_overshoot_is_exempted)
{
  const SCN restore_scn = make_scn(1000);

  // Typical +1 case produced by ObTxCtxMemtable::flush.
  EXPECT_TRUE(ObLSQuickRestoreState::is_tablet_clog_checkpoint_acceptable_for_restore(
      TX_CTX_TABLET, make_scn(1001), restore_scn));

  // Accumulated overshoot across multiple freeze rounds.
  EXPECT_TRUE(ObLSQuickRestoreState::is_tablet_clog_checkpoint_acceptable_for_restore(
      TX_CTX_TABLET, make_scn(1100), restore_scn));

  // Extreme: still accepted regardless of how far above restore_scn it is.
  EXPECT_TRUE(ObLSQuickRestoreState::is_tablet_clog_checkpoint_acceptable_for_restore(
      TX_CTX_TABLET, SCN::max_scn(), restore_scn));
}

// Negative: non-tx-ctx tablets must still be rejected when checkpoint > restore_scn,
// guarding against accidental widening of the exemption to other inner tablets.
TEST_F(TestSnLsRestoreCheckTabletCheckpoint, non_tx_ctx_tablets_overshoot_is_rejected)
{
  const SCN restore_scn = make_scn(1000);

  const ObTabletID kinds[] = {TX_DATA_TABLET, LOCK_TABLET, REORG_TABLET, USER_TABLET};
  for (const ObTabletID &tid : kinds) {
    EXPECT_FALSE(ObLSQuickRestoreState::is_tablet_clog_checkpoint_acceptable_for_restore(
        tid, make_scn(1001), restore_scn)) << "tablet_id=" << tid.id();
    EXPECT_FALSE(ObLSQuickRestoreState::is_tablet_clog_checkpoint_acceptable_for_restore(
        tid, SCN::max_scn(), restore_scn)) << "tablet_id=" << tid.id();
  }
}

} // namespace storage
} // namespace oceanbase

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
