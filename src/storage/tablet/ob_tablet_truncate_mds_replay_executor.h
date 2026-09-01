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

#ifndef OCEANBASE_STORAGE_OB_TABLET_TRUNCATE_MDS_REPLAY_EXECUTOR_H
#define OCEANBASE_STORAGE_OB_TABLET_TRUNCATE_MDS_REPLAY_EXECUTOR_H

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

class ObTabletTruncateMdsUserData;

class ObTabletTruncateMdsReplayExecutor final : public logservice::ObTabletReplayExecutor
{
public:
  ObTabletTruncateMdsReplayExecutor();

  int init(
      mds::BufferCtx &user_ctx,
      const share::SCN &scn,
      const ObTabletTruncateMdsUserData &user_data);

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
  const ObTabletTruncateMdsUserData *user_data_;
};

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_OB_TABLET_TRUNCATE_MDS_REPLAY_EXECUTOR_H
