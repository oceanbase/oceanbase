/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_OB_TABLET_REPLAY_EXECUTOR
#define OCEANBASE_STORAGE_OB_TABLET_REPLAY_EXECUTOR

#include "logservice/replayservice/ob_tablet_replay_executor.h"
#include "share/scn.h"

namespace oceanbase
{
namespace storage
{
namespace mds
{
class BufferCtx;
}

class ObTabletCreateReplayExecutor final : public logservice::ObTabletReplayExecutor
{
public:
  ObTabletCreateReplayExecutor();

  int init(
      mds::BufferCtx &user_ctx,
      const share::SCN &scn,
      const bool for_old_mds,
      const ObTabletCreateDeleteMdsUserData &user_data);

protected:
  bool is_replay_update_tablet_status_() const override
  {
    return true;
  }

  int do_replay_(ObTabletHandle &tablet_handle) override;

  virtual bool is_replay_update_mds_table_() const override
  {
    return true;
  }

private:
  mds::BufferCtx *user_ctx_;
  share::SCN scn_;
  bool for_old_mds_;
  const ObTabletCreateDeleteMdsUserData *user_data_;
};
} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_OB_TABLET_REPLAY_EXECUTOR
