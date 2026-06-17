/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_OB_TABLET_RANDOM_REPLAY_EXECUTOR
#define OCEANBASE_STORAGE_OB_TABLET_RANDOM_REPLAY_EXECUTOR

#include "common/ob_tablet_id.h"
#include "logservice/replayservice/ob_tablet_replay_executor.h"
#include "storage/tablet/ob_tablet_random_mds_user_data.h"

namespace oceanbase
{
namespace storage
{

class ObTabletRandomReplayExecutor final : public logservice::ObTabletReplayExecutor
{
public:
  ObTabletRandomReplayExecutor()
    : logservice::ObTabletReplayExecutor(), user_ctx_(nullptr), scn_(), data_(nullptr) {}

  int init(mds::BufferCtx &user_ctx, const share::SCN &scn, const ObTabletRandomMdsUserData &data);

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
  const ObTabletRandomMdsUserData *data_;
};

}
}

#endif
