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

#ifndef OCEANBASE_STORAGE_OB_TABLET_DELETE_REPLAY_EXECUTOR
#define OCEANBASE_STORAGE_OB_TABLET_DELETE_REPLAY_EXECUTOR

#include "share/ob_ls_id.h"
#include "common/ob_tablet_id.h"
#include "logservice/replayservice/ob_tablet_replay_executor.h"

namespace oceanbase
{

namespace storage
{

struct ObRemoveTabletArg
{
  OB_UNIS_VERSION(1);
public:
  inline bool is_valid() const
  {
    return ls_id_.is_valid() && tablet_id_.is_valid();
  }

  TO_STRING_KV(K_(ls_id), K_(tablet_id));

public:
  share::ObLSID ls_id_;
  common::ObTabletID tablet_id_;
};

class ObTabletDeleteReplayExecutor final : public logservice::ObTabletReplayExecutor
{
public:
  ObTabletDeleteReplayExecutor();

  int init(mds::BufferCtx &ctx, const share::SCN &scn, const bool for_old_mds);

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
  const ObRemoveTabletArg *arg_;
  mds::BufferCtx *ctx_;
  share::SCN scn_;
  bool for_old_mds_;
};


}
}

#endif
