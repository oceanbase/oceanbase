/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_OB_TABLET_SPLIT_SS_REPLAY_EXECUTOR
#define OCEANBASE_STORAGE_OB_TABLET_SPLIT_SS_REPLAY_EXECUTOR

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

class ObTabletSplitInfoReplayExecutor final : public logservice::ObTabletReplayExecutor
{
public:
  ObTabletSplitInfoReplayExecutor();

  int init(
      mds::BufferCtx &user_ctx,
      const share::SCN &scn,
      const ObTabletSplitInfoMdsUserData &user_data);

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
  const ObTabletSplitInfoMdsUserData *user_data_;
};
} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_OB_TABLET_SPLIT_SS_REPLAY_EXECUTOR