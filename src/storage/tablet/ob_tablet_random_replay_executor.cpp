/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "storage/tablet/ob_tablet_random_replay_executor.h"

#define USING_LOG_PREFIX STORAGE

namespace oceanbase
{
namespace storage
{

int ObTabletRandomReplayExecutor::init(mds::BufferCtx &user_ctx, const share::SCN &scn, const ObTabletRandomMdsUserData &data)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    TRANS_LOG(WARN, "tablet binding replay executor init twice", KR(ret), K_(is_inited));
  } else if (OB_UNLIKELY(!scn.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "get invalid argument", KR(ret), K(scn));
  } else {
    user_ctx_ = &user_ctx;
    scn_ = scn;
    data_ = &data;
    is_inited_ = true;
  }
  return ret;
}

int ObTabletRandomReplayExecutor::do_replay_(ObTabletHandle &tablet_handle)
{
  int ret = OB_SUCCESS;
  mds::MdsCtx &user_ctx = static_cast<mds::MdsCtx&>(*user_ctx_);
  if (OB_FAIL(replay_to_mds_table_(tablet_handle, *data_, user_ctx, scn_))) {
    TRANS_LOG(WARN, "failed to replay to tablet", K(ret));
  }
  return ret;
}

}
}
