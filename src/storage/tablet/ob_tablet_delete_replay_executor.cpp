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

#include "storage/tablet/ob_tablet_delete_replay_executor.h"
#include "lib/worker.h"
#include "storage/multi_data_source/mds_ctx.h"
#include "storage/meta_mem/ob_tenant_meta_mem_mgr.h"
#include "storage/meta_mem/ob_tablet_map_key.h"
#include "storage/tablet/ob_tablet_create_delete_helper.h"
#include "storage/tablet/ob_tablet_create_delete_mds_user_data.h"

#define USING_LOG_PREFIX MDS

namespace oceanbase
{
namespace storage
{

OB_SERIALIZE_MEMBER(ObRemoveTabletArg, ls_id_, tablet_id_);


// ObTabletDeleteReplayExecutor
ObTabletDeleteReplayExecutor::ObTabletDeleteReplayExecutor()
  :logservice::ObTabletReplayExecutor(), ctx_(nullptr)
{}

int ObTabletDeleteReplayExecutor::init(
    mds::BufferCtx &ctx,
    const share::SCN &scn,
    const bool for_old_mds)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("tablet delete replay executor init twice", KR(ret), K_(is_inited));
  } else if (OB_UNLIKELY(!scn.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid argument", KR(ret), K(scn));
  } else {
    ctx_ = &ctx;
    scn_ = scn;
    for_old_mds_ = for_old_mds;
    is_inited_ = true;
  }
  return ret;
}

int ObTabletDeleteReplayExecutor::do_replay_(ObTabletHandle &tablet_handle)
{
  MDS_TG(10_ms);
  int ret = OB_SUCCESS;
  ObTablet *tablet = tablet_handle.get_obj();
  mds::MdsCtx &user_ctx = static_cast<mds::MdsCtx&>(*ctx_);
  ObTabletCreateDeleteMdsUserData data;
  const int64_t timeout = THIS_WORKER.get_timeout_remain();

  if (OB_ISNULL(tablet)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet is null", K(ret), K(tablet_handle));
  } else if (CLICK_FAIL(tablet->ObITabletMdsInterface::get_tablet_status(share::SCN::max_scn(), data, timeout))) {
    LOG_WARN("failed to get tablet status", K(ret), K(timeout));
  } else {
    data.tablet_status_ = ObTabletStatus::DELETED;
    data.data_type_ = ObTabletMdsUserDataType::REMOVE_TABLET;
    if (CLICK_FAIL(replay_to_mds_table_(tablet_handle, data, user_ctx, scn_, for_old_mds_))) {
      LOG_WARN("failed to replay to tablet", K(ret));
    }
  }

  return ret;
}

}
}
