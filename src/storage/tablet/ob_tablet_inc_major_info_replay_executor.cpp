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

#include "storage/tablet/ob_tablet_inc_major_info_replay_executor.h"
#include "lib/ob_errno.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/utility/ob_print_utils.h"
#include "storage/multi_data_source/buffer_ctx.h"
#include "storage/tablet/ob_tablet_ddl_complete_mds_data.h"
#include "storage/tx_storage/ob_ls_service.h"
#define USING_LOG_PREFIX STORAGE

namespace oceanbase
{
namespace storage
{
ObTabletIncMajorInfoReplayExecutor::ObTabletIncMajorInfoReplayExecutor()
  : logservice::ObTabletReplayExecutor(),
    user_ctx_(nullptr), user_data_(nullptr)
{
}

int ObTabletIncMajorInfoReplayExecutor::init(
    mds::BufferCtx &user_ctx,
    const share::SCN &scn,
    const bool for_old_mds,
    const ObTabletDDLCompleteMdsUserData &user_data)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("tablet create replay executor init twice", KR(ret), K_(is_inited));
  } else if (OB_UNLIKELY(!scn.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid argument", KR(ret), K(scn));
  } else {
    user_ctx_ = &user_ctx;
    scn_ = scn;
    is_inited_ = true;
    for_old_mds_ = for_old_mds;
    user_data_ = &user_data;
  }
  return ret;
}

int ObTabletIncMajorInfoReplayExecutor::do_replay_(ObTabletHandle &tablet_handle)
{
  int ret     = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  mds::MdsCtx &user_ctx = static_cast<mds::MdsCtx&>(*user_ctx_);

  if (!tablet_handle.is_valid() || nullptr == user_data_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tablet handle", KR(ret), K(tablet_handle), KP(user_data_));
  } else if (OB_FAIL(replay_to_mds_table_(tablet_handle, *user_data_, user_ctx, scn_, for_old_mds_))) {
    LOG_WARN("failed to replay to tablet", KR(ret));
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
