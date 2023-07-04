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

#define USING_LOG_PREFIX CLOG

#define protected public
#define private public

#include "logservice/replayservice/ob_tablet_replay_executor.h"
#include "share/ob_tablet_autoincrement_param.h"

namespace oceanbase
{
namespace logservice
{
class TestTabletReplayexecutor : public ::testing::Test, public ObTabletReplayExecutor
{
public:
  TestTabletReplayexecutor() = default;
  virtual ~TestTabletReplayexecutor() = default;

  virtual void SetUp() override;
  virtual void TearDown() override;

  virtual bool is_replay_update_tablet_status_() const override;
  virtual int do_replay_(storage::ObTabletHandle &tablet_handle) override;
  virtual bool is_replay_update_mds_table_() const override;
};

void TestTabletReplayexecutor::SetUp()
{
}

void TestTabletReplayexecutor::TearDown()
{
}

bool TestTabletReplayexecutor::is_replay_update_tablet_status_() const
{
  return false;
}

int TestTabletReplayexecutor::do_replay_(storage::ObTabletHandle &tablet_handle)
{
  int ret = OB_SUCCESS;
  UNUSED(tablet_handle);
  return ret;
}

bool TestTabletReplayexecutor::is_replay_update_mds_table_() const
{
  return true;
}

int ObTabletReplayExecutor::replay_to_mds_table_(
    storage::ObTabletHandle &tablet_handle,
    const ObTabletCreateDeleteMdsUserData &mds,
    storage::mds::MdsCtx &ctx,
    const share::SCN &scn,
    const bool for_old_mds)
{
  int ret = 123;
  UNUSEDx(tablet_handle, mds, ctx, scn);
  return ret;
}

int ObTabletReplayExecutor::replay_to_mds_table_(
    storage::ObTabletHandle &tablet_handle,
    const ObTabletBindingMdsUserData &mds,
    storage::mds::MdsCtx &ctx,
    const share::SCN &scn,
    const bool for_old_mds)
{
  int ret = 456;
  UNUSEDx(tablet_handle, mds, ctx, scn);
  return ret;
}

TEST_F(TestTabletReplayexecutor, template_specialization)
{
  int ret = OB_SUCCESS;
  storage::ObTabletHandle tablet_handle;
  storage::mds::MdsCtx ctx;
  share::SCN scn;
  ObTabletCreateDeleteMdsUserData user_data1;
  ObTabletBindingMdsUserData user_data2;
  share::ObTabletAutoincSeq user_data3;

  ret = this->replay_to_mds_table_(tablet_handle, user_data1, ctx, scn);
  ASSERT_EQ(123, ret);
  ret = this->replay_to_mds_table_(tablet_handle, user_data2, ctx, scn);
  ASSERT_EQ(456, ret);
  ret = this->replay_to_mds_table_(tablet_handle, user_data3, ctx, scn);
  ASSERT_EQ(OB_ERR_UNEXPECTED, ret);
}
} // namespace logservice
} // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_tablet_replay_executor.log*");
  OB_LOGGER.set_file_name("test_tablet_replay_executor.log", true);
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
