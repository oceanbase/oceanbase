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

#include "storage/tablet/ob_tablet_truncate_mds_replay_executor.h"
#include "storage/tablet/ob_tablet_truncate_mds_user_data.h"
#include "storage/multi_data_source/mds_ctx.h"

#define USING_LOG_PREFIX STORAGE

namespace oceanbase
{
namespace storage
{

ObTabletTruncateMdsReplayExecutor::ObTabletTruncateMdsReplayExecutor()
  : logservice::ObTabletReplayExecutor(),
    user_ctx_(nullptr),
    scn_(),
    user_data_(nullptr)
{
}

int ObTabletTruncateMdsReplayExecutor::init(
    mds::BufferCtx &user_ctx,
    const share::SCN &scn,
    const ObTabletTruncateMdsUserData &user_data)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("tablet truncate replay executor init twice", KR(ret), K_(is_inited));
  } else if (OB_UNLIKELY(!scn.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid argument", KR(ret), K(scn));
  } else {
    user_ctx_ = &user_ctx;
    scn_ = scn;
    user_data_ = &user_data;
    is_inited_ = true;
  }
  return ret;
}

int ObTabletTruncateMdsReplayExecutor::do_replay_(ObTabletHandle &tablet_handle)
{
  int ret = OB_SUCCESS;
  mds::MdsCtx &user_ctx = static_cast<mds::MdsCtx&>(*user_ctx_);

  if (OB_FAIL(replay_to_mds_table_(tablet_handle, *user_data_, user_ctx, scn_))) {
    LOG_WARN("failed to replay truncate to mds table", K(ret), K_(scn));
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
