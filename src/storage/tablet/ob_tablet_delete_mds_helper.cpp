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

#include "storage/tablet/ob_tablet_delete_mds_helper.h"
#include "lib/worker.h"
#include "common/ob_tablet_id.h"
#include "share/scn.h"
#include "share/ob_rpc_struct.h"
#include "storage/multi_data_source/buffer_ctx.h"
#include "storage/multi_data_source/mds_ctx.h"
#include "storage/meta_mem/ob_tenant_meta_mem_mgr.h"
#include "storage/meta_mem/ob_tablet_map_key.h"
#include "storage/tablet/ob_tablet_create_delete_helper.h"
#include "storage/tablet/ob_tablet_create_delete_mds_user_data.h"
#include "storage/tablet/ob_tablet_delete_replay_executor.h"
#include "storage/tx_storage/ob_ls_service.h"

#define USING_LOG_PREFIX MDS

using namespace oceanbase::common;

namespace oceanbase
{
namespace storage
{
int ObTabletDeleteMdsHelper::register_process(
    obrpc::ObBatchRemoveTabletArg &arg,
    mds::BufferCtx &ctx)
{
  MDS_TG(1_s);
  int ret = OB_SUCCESS;

  if (CLICK_FAIL(delete_tablets(arg, ctx))) {
    LOG_WARN("failed to delete tablets", K(ret), K(arg));
  } else if (CLICK_FAIL(ObTabletCreateDeleteMdsUserData::set_tablet_empty_shell_trigger(arg.id_))) {
    LOG_WARN("failed to set_tablet_empty_shell_trigger", K(ret), K(arg));
  } else {
    LOG_INFO("delete tablet register", KR(ret), K(arg));
  }

  return ret;
}

int ObTabletDeleteMdsHelper::on_commit_for_old_mds(
    const char* buf,
    const int64_t len,
    const transaction::ObMulSourceDataNotifyArg &notify_arg)
{
  return ObTabletCreateDeleteHelper::process_for_old_mds<obrpc::ObBatchRemoveTabletArg, ObTabletDeleteMdsHelper>(buf, len, notify_arg);
}

int ObTabletDeleteMdsHelper::on_register(
    const char* buf,
    const int64_t len,
    mds::BufferCtx &ctx)
{
  MDS_TG(1_s);
  int ret = OB_SUCCESS;
  obrpc::ObBatchRemoveTabletArg arg;
  int64_t pos = 0;
  bool exist = true;

  if (OB_ISNULL(buf) || OB_UNLIKELY(len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), KP(buf), K(len));
  } else if (CLICK_FAIL(arg.deserialize(buf, len, pos))) {
    LOG_WARN("failed to deserialize", K(ret));
  } else if (arg.is_old_mds_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error, arg is old mds", K(ret), K(arg));
  } else if (CLICK_FAIL(register_process(arg, ctx))) {
    LOG_WARN("failed to register_process", K(ret), K(arg));
  }

  return ret;
}

int ObTabletDeleteMdsHelper::replay_process(
    obrpc::ObBatchRemoveTabletArg &arg,
    const share::SCN &scn,
    mds::BufferCtx &ctx)
{
  MDS_TG(1_s);
  int ret = OB_SUCCESS;

  if (CLICK_FAIL(replay_delete_tablets(arg, scn, ctx))) {
    LOG_WARN("failed to delete tablets", K(ret), K(arg), K(scn));
  } else if (CLICK_FAIL(ObTabletCreateDeleteMdsUserData::set_tablet_empty_shell_trigger(arg.id_))) {
    LOG_WARN("failed to set_tablet_empty_shell_trigger", K(ret), K(arg));
  } else {
    LOG_INFO("delete tablet replay", KR(ret), K(arg));
  }

  return ret;
}

int ObTabletDeleteMdsHelper::on_replay(
    const char* buf,
    const int64_t len,
    const share::SCN &scn,
    mds::BufferCtx &ctx)
{
  MDS_TG(1_s);
  int ret = OB_SUCCESS;
  obrpc::ObBatchRemoveTabletArg arg;
  int64_t pos = 0;
  bool exist = true;

  if (OB_ISNULL(buf) || OB_UNLIKELY(len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), KP(buf), K(len));
  } else if (CLICK_FAIL(arg.deserialize(buf, len, pos))) {
    LOG_WARN("failed to deserialize", K(ret));
  } else if (arg.is_old_mds_) {
    LOG_INFO("skip replay delete tablet for old mds", K(arg), K(scn));
  } else if (CLICK_FAIL(replay_process(arg, scn, ctx))) {
    LOG_WARN("failed to replay_process", K(ret), K(arg));
  }

  return ret;
}

int ObTabletDeleteMdsHelper::delete_tablets(
    const obrpc::ObBatchRemoveTabletArg &arg,
    mds::BufferCtx &ctx)
{
  MDS_TG(1_s);
  int ret = OB_SUCCESS;
  ObTenantMetaMemMgr *t3m = MTL(ObTenantMetaMemMgr*);
  bool exist = false;
  ObTabletHandle tablet_handle;
  ObTabletMapKey key;
  key.ls_id_ = arg.id_;

  ObLSService *ls_service = MTL(ObLSService*);
  ObLSHandle ls_handle;
  ObLS *ls = nullptr;
  if (CLICK_FAIL(ls_service->get_ls(key.ls_id_, ls_handle, ObLSGetMod::MDS_TABLE_MOD))) {
      LOG_WARN("failed to get ls", K(ret), K(key.ls_id_));
  } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
  } else {
    CLICK();
    for (int64_t i = 0; OB_SUCC(ret) && i < arg.tablet_ids_.count(); ++i) {
      MDS_TG(10_ms);
      exist = false;
      key.tablet_id_ = arg.tablet_ids_.at(i);

      if (CLICK_FAIL(ObTabletCreateDeleteHelper::get_tablet(key, tablet_handle))) {
        if (OB_TABLET_NOT_EXIST == ret) {
          exist = false;
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("failed to get tablet", K(ret), K(key));
        }
      } else {
        exist = true;
      }

      if (CLICK_FAIL(ret)) {
      } else if (!exist) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("tablet does not exist", K(ret), K(key));
      } else if (CLICK_FAIL(set_tablet_deleted_status(ls->get_tablet_svr(), tablet_handle, ctx))) {
        LOG_WARN("failed to set tablet deleted status", K(ret), K(key));
      }
    }
  }

  // remember to roll back if failed

  return ret;
}

int ObTabletDeleteMdsHelper::replay_delete_tablets(
    const obrpc::ObBatchRemoveTabletArg &arg,
    const share::SCN &scn,
    mds::BufferCtx &ctx)
{
  MDS_TG(1_s);
  int ret = OB_SUCCESS;
  bool exist = false;
  ObRemoveTabletArg remove_tablet_arg;
  remove_tablet_arg.ls_id_ = arg.id_;

  for (int64_t i = 0; OB_SUCC(ret) && i < arg.tablet_ids_.count(); ++i) {
    MDS_TG(10_ms);
    exist = true;
    remove_tablet_arg.tablet_id_ = arg.tablet_ids_.at(i);
    ObTabletDeleteReplayExecutor replayer;
    if (CLICK_FAIL(replayer.init(ctx, scn, arg.is_old_mds_))) {
      LOG_WARN("failed to init tablet delete replay executor", K(ret), K(remove_tablet_arg));
    } else if (CLICK_FAIL(replayer.execute(scn, remove_tablet_arg.ls_id_, remove_tablet_arg.tablet_id_))) {
      if (OB_TABLET_NOT_EXIST == ret) {
        exist = false;
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("failed to replay", K(ret), K(remove_tablet_arg), K(scn));
      }
    }

    if (OB_FAIL(ret)) {
    } else if (!exist) {
      // tablet does not exist, maybe already deleted, do nothing
    }
  }

  // remember to roll back if failed

  return ret;
}

int ObTabletDeleteMdsHelper::set_tablet_deleted_status(
    ObLSTabletService *ls_tablet_service,
    ObTabletHandle &tablet_handle,
    mds::BufferCtx &ctx)
{
  MDS_TG(10_ms);
  int ret = OB_SUCCESS;
  ObTablet *tablet = tablet_handle.get_obj();
  mds::MdsCtx &user_ctx = static_cast<mds::MdsCtx&>(ctx);
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
    if (CLICK_FAIL(ls_tablet_service->set_tablet_status(tablet->get_tablet_meta().tablet_id_, data, user_ctx))) {
      LOG_WARN("failed to set mds data", K(ret));
    }
  }

  return ret;
}

} // namespace storage
} // namespace oceanbase
