/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <gtest/gtest.h>
#include "storage/blocksstable/ob_datum_row.h"

#define protected public
#define private public
#include "storage/lob/ob_lob_access_param.h"
#include "storage/lob/ob_lob_persistent_adaptor.h"
#include "storage/lob/ob_lob_meta.h"
#include "storage/access/ob_dml_param.h"
#include "storage/memtable/ob_memtable_context.h"
#include "storage/tx/ob_tx_data_define.h"
#include "storage/tx/ob_tx_data_functor.h"
#undef private
#undef protected

namespace oceanbase
{
namespace unittest
{

using namespace common;
using namespace storage;
using namespace transaction;

class TestLobTransferReadLatest : public ::testing::Test
{
protected:
  void build_scan_param(ObTableScanParam &scan_param)
  {
    ObPersistentLobApator adaptor(OB_SYS_TENANT_ID);
    ASSERT_EQ(OB_SUCCESS, adaptor.build_common_scan_param(
        param_, false, ObLobMetaUtil::LOB_META_WITHOUT_TTL_COLUMN_CNT,
        scan_param, &allocator_, &allocator_));
  }

  void expect_version_visibility(const ObTableScanParam &scan_param,
                                 const int64_t data_seq,
                                 const bool expected,
                                 const int32_t state = ObTxData::RUNNING)
  {
    memtable::ObMvccAccessCtx ctx;
    ctx.snapshot_ = scan_param.snapshot_.core_;
    ctx.tx_id_ = scan_param.tx_id_;
    ObLockForReadArg arg(ctx, ctx.snapshot_.tx_id_, ObTxSEQ::cast_from_int(data_seq),
                        scan_param.scan_flag_.is_read_latest(), false, share::SCN::max_scn());
    ObTxData tx_data;
    tx_data.state_ = state;
    tx_data.tx_id_ = ctx.snapshot_.tx_id_;
    bool can_read = false;
    share::SCN trans_version;
    ObCleanoutNothingOperation cleanout;
    ObReCheckNothingOperation recheck;
    LockForReadFunctor functor(arg, can_read, trans_version, scan_param.ls_id_, cleanout, recheck);
    ASSERT_EQ(OB_SUCCESS, functor.inner_lock_for_read(tx_data, nullptr));
    EXPECT_EQ(expected, can_read);
  }

  void init_param(
      const bool has_read_snapshot,
      const share::ObLSID &snapshot_ls,
      const share::ObLSID &current_ls,
      const bool add_snapshot_part = true)
  {
    ObMemLobExternFlags flags;
    flags.set_empty();
    flags.has_location_info_ = true;
    flags.has_read_snapshot_ = has_read_snapshot;
    const uint32_t locator_size = ObLobLocatorV2::calc_locator_full_len(
        flags, 0, sizeof(ObLobCommon), 0, false);
    ObLobCommon lob_common;
    ASSERT_LE(locator_size, sizeof(locator_buf_));
    locator_.assign_buffer(locator_buf_, locator_size);
    ASSERT_EQ(OB_SUCCESS, locator_.fill(
        PERSISTENT_LOB, flags, ObString(), &lob_common, sizeof(lob_common), 0, 0, false));

    param_.lob_locator_ = &locator_;
    param_.ls_id_ = current_ls;
    ASSERT_EQ(OB_SUCCESS, param_.snapshot_.build_snapshot_for_lob(1, 1, 1, snapshot_ls));
    if (add_snapshot_part) {
      ASSERT_EQ(OB_SUCCESS, param_.snapshot_.parts_.push_back(ObTxLSEpochPair(snapshot_ls, 1)));
    }
  }

  ObArenaAllocator allocator_;
  ObLobAccessParam param_;
  ObLobLocatorV2 locator_;
  alignas(ObMemLobCommon) char locator_buf_[512];
};

TEST_F(TestLobTransferReadLatest, new_snapshot_current_ls_hit)
{
  const share::ObLSID current_ls(1001);
  init_param(true, current_ls, current_ls);
  EXPECT_FALSE(param_.need_transfer_own_write_read_latest());
}

TEST_F(TestLobTransferReadLatest, new_snapshot_current_ls_miss)
{
  init_param(true, share::ObLSID(1003), share::ObLSID(1001));
  EXPECT_TRUE(param_.need_transfer_own_write_read_latest());
}

TEST_F(TestLobTransferReadLatest, empty_snapshot_parts_does_not_prove_transfer)
{
  init_param(true, share::ObLSID(1003), share::ObLSID(1001), false);
  ASSERT_TRUE(param_.snapshot_.parts_.empty());
  EXPECT_FALSE(param_.need_transfer_own_write_read_latest());
}

TEST_F(TestLobTransferReadLatest, old_locator_current_ls_hit)
{
  const share::ObLSID current_ls(1001);
  init_param(false, current_ls, current_ls);
  EXPECT_FALSE(param_.need_transfer_own_write_read_latest());
}

TEST_F(TestLobTransferReadLatest, old_locator_current_ls_miss)
{
  init_param(false, share::ObLSID(1003), share::ObLSID(1001));
  EXPECT_FALSE(param_.need_transfer_own_write_read_latest());
}

TEST_F(TestLobTransferReadLatest, transfer_keeps_locator_sequence_boundary)
{
  init_param(true, share::ObLSID(1003), share::ObLSID(1001));
  ObTableScanParam scan_param;
  EXPECT_FALSE(scan_param.need_own_write_read_ctx_);
  build_scan_param(scan_param);
  EXPECT_TRUE(scan_param.need_own_write_read_ctx_);
  EXPECT_FALSE(scan_param.scan_flag_.is_read_latest());
  EXPECT_EQ(param_.snapshot_.core_.tx_id_, scan_param.tx_id_);
  EXPECT_EQ(param_.snapshot_.core_.scn_, scan_param.snapshot_.core_.scn_);
  expect_version_visibility(scan_param, 1, true);
  expect_version_visibility(scan_param, 2, false);
}

TEST_F(TestLobTransferReadLatest, new_locator_can_read_its_own_write_after_transfer)
{
  init_param(true, share::ObLSID(1003), share::ObLSID(1001));
  param_.snapshot_.core_.scn_ = ObTxSEQ::cast_from_int(2);
  ObTableScanParam scan_param;
  build_scan_param(scan_param);
  EXPECT_TRUE(scan_param.need_own_write_read_ctx_);
  EXPECT_FALSE(scan_param.scan_flag_.is_read_latest());
  expect_version_visibility(scan_param, 2, true);
  expect_version_visibility(scan_param, 3, false);
}

TEST_F(TestLobTransferReadLatest, explicit_latest_still_reads_later_own_write)
{
  init_param(true, share::ObLSID(1003), share::ObLSID(1001));
  param_.need_read_latest_ = true;
  ObTableScanParam scan_param;
  build_scan_param(scan_param);
  EXPECT_TRUE(scan_param.need_own_write_read_ctx_);
  EXPECT_TRUE(scan_param.scan_flag_.is_read_latest());
  expect_version_visibility(scan_param, 2, true);
  expect_version_visibility(scan_param, 1, false, ObTxData::ABORT);
}

TEST_F(TestLobTransferReadLatest, rebuilding_scan_clears_transfer_hint)
{
  init_param(true, share::ObLSID(1003), share::ObLSID(1001));
  ObTableScanParam scan_param;
  build_scan_param(scan_param);
  ASSERT_TRUE(scan_param.need_own_write_read_ctx_);
  param_.ls_id_ = share::ObLSID(1003);
  build_scan_param(scan_param);
  EXPECT_FALSE(scan_param.need_own_write_read_ctx_);
  EXPECT_FALSE(scan_param.scan_flag_.is_read_latest());
}

TEST_F(TestLobTransferReadLatest, old_locator_scan_semantics_unchanged)
{
  init_param(false, share::ObLSID(1003), share::ObLSID(1001));
  ObTableScanParam scan_param;
  build_scan_param(scan_param);
  EXPECT_FALSE(scan_param.need_own_write_read_ctx_);
  EXPECT_FALSE(scan_param.scan_flag_.is_read_latest());
  param_.need_read_latest_ = true;
  build_scan_param(scan_param);
  EXPECT_FALSE(scan_param.need_own_write_read_ctx_);
  EXPECT_TRUE(scan_param.scan_flag_.is_read_latest());
  EXPECT_EQ(param_.snapshot_.core_.tx_id_, scan_param.tx_id_);
}

TEST_F(TestLobTransferReadLatest, read_latest_preserves_scan_transaction_id)
{
  init_param(true, share::ObLSID(1001), share::ObLSID(1001));
  param_.read_latest_ = true;
  ObTableScanParam scan_param;
  build_scan_param(scan_param);
  EXPECT_FALSE(scan_param.need_own_write_read_ctx_);
  EXPECT_FALSE(scan_param.scan_flag_.is_read_latest());
  EXPECT_EQ(param_.snapshot_.core_.tx_id_, scan_param.tx_id_);
}

} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
