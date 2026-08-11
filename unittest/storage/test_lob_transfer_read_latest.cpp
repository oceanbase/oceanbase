/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <gtest/gtest.h>
#include "storage/blocksstable/ob_datum_row.h"

#define protected public
#define private public
#include "storage/lob/ob_lob_access_param.h"

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

} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
