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

#include "storage/tablet/ob_tablet_finish_transfer_mds_helper.h"
#include "share/scn.h"
#include "share/ob_ls_id.h"
#include "share/transfer/ob_transfer_info.h"
#include "common/ob_tablet_id.h"
#include "storage/ls/ob_ls_get_mod.h"
#include "storage/multi_data_source/buffer_ctx.h"
#include "storage/multi_data_source/mds_ctx.h"
#include "storage/meta_mem/ob_tenant_meta_mem_mgr.h"
#include "storage/meta_mem/ob_tablet_map_key.h"
#include "storage/meta_mem/ob_tablet_handle.h"
#include "storage/tablet/ob_tablet_create_delete_mds_user_data.h"
#include "storage/tx_storage/ob_ls_handle.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "logservice/replayservice/ob_tablet_replay_executor.h"
#include "storage/tablet/ob_tablet_create_delete_helper.h"
#include "observer/ob_server_event_history_table_operator.h"
#include "storage/high_availability/ob_rebuild_service.h"
#include "storage/high_availability/ob_storage_ha_utils.h"
#include "storage/high_availability/ob_transfer_service.h"

#define USING_LOG_PREFIX MDS

namespace oceanbase
{
namespace storage
{

ERRSIM_POINT_DEF(EN_TRANSFER_NEED_REBUILD);

int ObTabletFinishTransferUtil::check_transfer_table_replaced(
    ObTabletHandle &tablet_handle,
    bool &all_replaced)
{
  int ret = OB_SUCCESS;
  all_replaced = true;
  ObTablet *tablet = NULL;
  if (!tablet_handle.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(tablet_handle));
  } else if (OB_ISNULL(tablet = tablet_handle.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet should not be NULL", K(ret), K(tablet_handle));
  } else if (tablet->get_tablet_meta().has_transfer_table()) {
    all_replaced = false;
  }
  return ret;
}

int ObTabletFinishTransferUtil::can_skip_check_transfer_tablets(
    const uint64_t tenant_id,
    const share::ObLSID &ls_id /* transfer dest ls */,
    const share::SCN &scn /* finish transfer in log scn */,
    bool &can_skip_check)
{
  // There are 2 cases that source tablets are no need to check ready while replaying transfer in. The following
  // order needs to be followed.
  // 1. Current ls is in restore, and scn is smaller than consistent_scn;
  // 2. GTS is over current log scn, but current ls is in rebuild.
  int ret = OB_SUCCESS;
  ObLSService *ls_svr = nullptr;
  ObLSHandle ls_handle;
  ObLS *ls = nullptr;
  ObLSRestoreHandler *restore_handler = nullptr;
  ObLSRestoreStatus restore_status;
  ObMigrationStatus migration_status = ObMigrationStatus::OB_MIGRATION_STATUS_MAX;
  SCN consistent_scn;
  SCN gts_scn;
  can_skip_check = false;

  if (OB_ISNULL(ls_svr = MTL(ObLSService *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls svr should not be NULL", K(ret), KP(ls_svr));
  } else if (OB_FAIL(ls_svr->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD))) {
    LOG_WARN("failed to get ls", K(ret), K(ls_id));
  } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls is NULL", K(ret), K(ls_id), K(ls_handle));
  } else if (OB_FAIL(ls->get_restore_status(restore_status))) {
    LOG_WARN("failed to get restore status", K(ret), KPC(ls));
  } else if (!restore_status.is_in_restore()) {
    // ls not in restore, cannot skip.
  } else if (OB_FALSE_IT(restore_handler = ls->get_ls_restore_handler())) {
  } else if (OB_FAIL(restore_handler->get_consistent_scn(consistent_scn))) {
    LOG_WARN("failed to get consistent_scn", K(ret), KPC(ls));
  } else if (scn <= consistent_scn) {
    can_skip_check = true;
    LOG_INFO("transfer finish in scn <= consistent_scn, skip check local finish transfer in tablet ready", K(scn), K(consistent_scn));
  } else {
    // transfer finish in log scn is bigger than consistent_scn, cannot skip.
  }

  if (OB_FAIL(ret)) {
  } else if (can_skip_check) {
  } else if (OB_FAIL(ObTransferUtils::get_gts(tenant_id, gts_scn))) {
    LOG_WARN("failed to get gts", K(ret), K(tenant_id), K(scn));
    //overwrite ret
    ret = OB_SUCCESS;
  } else if (gts_scn <= scn) {
    LOG_INFO("can not skip check transfer table replaced", K(gts_scn), K(scn));
  } else if (OB_FAIL(ls->get_migration_status(migration_status))) {
    LOG_WARN("failed to get migration status", K(ret), KPC(ls));
  } else if (ObMigrationStatus::OB_MIGRATION_STATUS_ADD == migration_status
      || ObMigrationStatus::OB_MIGRATION_STATUS_MIGRATE == migration_status
      || ObMigrationStatus::OB_MIGRATION_STATUS_REBUILD == migration_status) {
    can_skip_check = true;
    LOG_INFO("ls is in add or migrate or rebuild status, skip check local finish transfer in tablet ready",
        K(migration_status));
  }
  return ret;
}

/******************ObTabletFinishTransferOutReplayExecutor*********************/
class ObTabletFinishTransferOutReplayExecutor final : public logservice::ObTabletReplayExecutor
{
public:
  ObTabletFinishTransferOutReplayExecutor();
  virtual ~ObTabletFinishTransferOutReplayExecutor();

  int init(
      const share::SCN &scn,
      const share::ObLSID &src_ls_id,
      const share::ObLSID &dest_ls_id,
      const share::ObTransferTabletInfo &tablet_info,
      mds::BufferCtx &buffer_ctx);
protected:
  virtual bool is_replay_update_tablet_status_() const override
  {
    return true;
  }

  virtual int do_replay_(ObTabletHandle &tablet_handle) override;

  virtual bool is_replay_update_mds_table_() const override
  {
    return true;
  }

private:
  int check_src_transfer_tablet_(ObTabletHandle &tablet_handle);

private:
  share::SCN scn_;
  share::ObLSID src_ls_id_;
  share::ObLSID dest_ls_id_;
  share::ObTransferTabletInfo tablet_info_;
  mds::BufferCtx *buffer_ctx_;
  DISALLOW_COPY_AND_ASSIGN(ObTabletFinishTransferOutReplayExecutor);
};

ObTabletFinishTransferOutReplayExecutor::ObTabletFinishTransferOutReplayExecutor()
  :logservice::ObTabletReplayExecutor(),
   scn_(),
   src_ls_id_(),
   dest_ls_id_(),
   tablet_info_(),
   buffer_ctx_(nullptr)
{
}

ObTabletFinishTransferOutReplayExecutor::~ObTabletFinishTransferOutReplayExecutor()
{
}

int ObTabletFinishTransferOutReplayExecutor::init(
    const share::SCN &scn,
    const share::ObLSID &src_ls_id,
    const share::ObLSID &dest_ls_id,
    const share::ObTransferTabletInfo &tablet_info,
    mds::BufferCtx &buffer_ctx)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("tablet finish transfer out replay executor init twice", KR(ret), K_(is_inited));
  } else if (OB_UNLIKELY(!scn.is_valid())
      || OB_UNLIKELY(!src_ls_id.is_valid())
      || OB_UNLIKELY(!dest_ls_id.is_valid())
      || OB_UNLIKELY(!tablet_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid argument", KR(ret), K(scn), K(src_ls_id), K(dest_ls_id), K(tablet_info));
  } else {
    scn_ = scn;
    src_ls_id_ = src_ls_id;
    dest_ls_id_ = dest_ls_id;
    tablet_info_ = tablet_info;
    buffer_ctx_ = &buffer_ctx;
    scn_ = scn;
    is_inited_ = true;
  }
  return ret;
}

int ObTabletFinishTransferOutReplayExecutor::do_replay_(ObTabletHandle &tablet_handle)
{
  int ret = OB_SUCCESS;
  ObTabletCreateDeleteMdsUserData user_data;
  mds::MdsCtx &user_ctx = static_cast<mds::MdsCtx&>(*buffer_ctx_);
  ObTablet *tablet = nullptr;
  bool is_committed = false;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet finish transfer out replay executor do not init", K(ret));
  } else if (OB_FAIL(check_src_transfer_tablet_(tablet_handle))) {
    LOG_WARN("failed to check src transfer tablet", K(ret), K(tablet_handle));
  } else if (OB_ISNULL(tablet = tablet_handle.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet should not be NULL", K(ret), KP(tablet), K(tablet_handle));
  } else if (OB_FAIL(tablet->ObITabletMdsInterface::get_latest_tablet_status(user_data, is_committed))) {
    LOG_WARN("failed to get tablet status", K(ret), KPC(tablet), K(user_data));
  } else if (!is_committed) {
    ret = OB_EAGAIN;
    LOG_WARN("transfer out tablet still has uncommitted mds data", K(ret), K(user_data), K(is_committed), KPC(tablet));
  } else {
    user_data.tablet_status_ = ObTabletStatus::TRANSFER_OUT_DELETED;
    user_data.data_type_ = ObTabletMdsUserDataType::FINISH_TRANSFER_OUT;
    if (OB_FAIL(replay_to_mds_table_(tablet_handle, user_data, user_ctx, scn_))) {
      LOG_WARN("failed to replay to tablet", K(ret));
    } else {
      LOG_INFO("succeed replay finish transfer out to mds table", KP(tablet), K(user_data), K(scn_));
    }
#ifdef ERRSIM
    ObTransferEventRecorder::record_tablet_transfer_event("tx_finish_transfer_out",
        src_ls_id_, tablet->get_tablet_meta().tablet_id_,
        tablet->get_tablet_meta().transfer_info_.transfer_seq_, user_data.tablet_status_,
        ret);
#endif
  }
  return ret;
}

int ObTabletFinishTransferOutReplayExecutor::check_src_transfer_tablet_(
    ObTabletHandle &tablet_handle)
{
  int ret = OB_SUCCESS;
  ObTablet *tablet = nullptr;
  ObTabletCreateDeleteMdsUserData user_data;
  const int64_t transfer_seq = tablet_info_.transfer_seq();

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet start transfer out replay executor do not init", K(ret));
  } else if (OB_ISNULL(tablet = tablet_handle.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet should not be NULL", K(ret), KP(tablet), K(tablet_info_), K(src_ls_id_), K(dest_ls_id_));
  } else if (OB_FAIL(tablet->ObITabletMdsInterface::get_tablet_status(share::SCN::max_scn(), user_data, ObTabletCommon::DEFAULT_GET_TABLET_DURATION_US))) {
    LOG_WARN("failed to get tx data", K(ret), KPC(tablet), K(tablet_info_));
  } else if (ObTabletStatus::TRANSFER_OUT != user_data.tablet_status_
      || transfer_seq != tablet->get_tablet_meta().transfer_info_.transfer_seq_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet tx data is unexpected",
        K(ret),
        K(transfer_seq),
        K(user_data),
        KPC(tablet));
  }
  return ret;
}

/******************ObTabletFinishTransferOutHelper*********************/
int ObTabletFinishTransferOutHelper::on_register(
    const char *buf,
    const int64_t len,
    mds::BufferCtx &ctx)
{
  MDS_TG(1_s);
  int ret = OB_SUCCESS;
  ObTXFinishTransferOutInfo tx_finish_transfer_out_info;
  int64_t pos = 0;
  ObTransferUtils::set_transfer_module();

  if (OB_ISNULL(buf) || len < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("on register finish transfer out get invalid argument", K(ret), KP(buf), K(len));
  } else if (CLICK_FAIL(tx_finish_transfer_out_info.deserialize(buf, len, pos))) {
    LOG_WARN("failed to deserialize tx finish transfer out info", K(ret), K(len), K(pos));
  } else if (!tx_finish_transfer_out_info.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tx finish transfer out info is unexpected", K(ret), K(tx_finish_transfer_out_info));
  } else if (CLICK_FAIL(on_register_success_(tx_finish_transfer_out_info, ctx))) {
    LOG_WARN("failed to on register", K(ret), K(tx_finish_transfer_out_info));
  } else if (CLICK_FAIL(ObTabletCreateDeleteMdsUserData::set_tablet_empty_shell_trigger(tx_finish_transfer_out_info.src_ls_id_))) {
    LOG_WARN("failed to set_tablet_empty_shell_trigger", K(ret), K(tx_finish_transfer_out_info));
  }
  ObTransferUtils::clear_transfer_module();
  return ret;
}

int ObTabletFinishTransferOutHelper::on_register_success_(
    const ObTXFinishTransferOutInfo &tx_finish_transfer_out_info,
    mds::BufferCtx &ctx)
{
  MDS_TG(1_s);
  int ret = OB_SUCCESS;
  ObLSService *ls_svr = NULL;
  ObLSHandle ls_handle;
  ObLS *ls = NULL;
  const share::ObLSID &src_ls_id = tx_finish_transfer_out_info.src_ls_id_;
  const int64_t start_ts = ObTimeUtil::current_time();
  LOG_INFO("[TRANSFER] start tx finish transfer out on_register_success_", K(tx_finish_transfer_out_info));
#ifdef ERRSIM
  SERVER_EVENT_ADD("transfer", "tx_finish_transfer_out",
                   "stage", "on_register_success",
                   "tenant_id", MTL_ID(),
                   "src_ls_id", tx_finish_transfer_out_info.src_ls_id_.id(),
                   "dest_ls_id", tx_finish_transfer_out_info.dest_ls_id_.id(),
                   "finish_scn", tx_finish_transfer_out_info.finish_scn_,
                   "tablet_count", tx_finish_transfer_out_info.tablet_list_.count());
#endif

  if (OB_ISNULL(ls_svr = MTL(ObLSService *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls svr should not be NULL", K(ret), KP(ls_svr));
  } else if (CLICK_FAIL(ls_svr->get_ls(src_ls_id, ls_handle, ObLSGetMod::STORAGE_MOD))) {
    LOG_WARN("failed to get ls", K(ret), K(src_ls_id), K(tx_finish_transfer_out_info));
  } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls is NULL", K(ret), K(src_ls_id), K(ls_handle));
  } else if (CLICK_FAIL(check_transfer_out_tablets_validity_(tx_finish_transfer_out_info, ls))) {
    LOG_WARN("failed to check transfer out tablets validity", K(ret), K(src_ls_id), K(tx_finish_transfer_out_info));
  } else if (CLICK_FAIL(update_transfer_tablets_deleted_(tx_finish_transfer_out_info, ls, ctx))) {
    LOG_WARN("failed to update transfer tablets deleted", K(ret), K(tx_finish_transfer_out_info));
  }

  if (OB_FAIL(ret)) {
    LOG_WARN("tx finish transfer out on_register_success_ failed", K(ret), K(tx_finish_transfer_out_info));
  } else {
    ls->get_tablet_gc_handler()->set_tablet_persist_trigger();
    LOG_INFO("[TRANSFER] finish tx finish transfer out on_register_success_", K(tx_finish_transfer_out_info),
        "cost_ts", ObTimeUtil::current_time() - start_ts);
  }

  return ret;
}

int ObTabletFinishTransferOutHelper::check_transfer_out_tablets_validity_(
    const ObTXFinishTransferOutInfo &tx_finish_transfer_out_info,
    storage::ObLS *ls)
{
  MDS_TG(500_ms);
  int ret = OB_SUCCESS;
  const common::ObSArray<share::ObTransferTabletInfo> &tablet_list = tx_finish_transfer_out_info.tablet_list_;
  for (int64_t i = 0; OB_SUCC(ret) && i < tablet_list.count(); ++i) {
    const share::ObTransferTabletInfo &tablet_info = tablet_list.at(i);
    if (OB_FAIL(inner_check_transfer_out_tablet_validity_(tablet_info, ls))) {
      LOG_WARN("failed to inner check transfer out tablet validity", K(ret), K(tablet_info), KP(ls));
    }
  }
  return ret;
}

int ObTabletFinishTransferOutHelper::inner_check_transfer_out_tablet_validity_(
    const share::ObTransferTabletInfo &tablet_info,
    storage::ObLS *ls)
{
  MDS_TG(10_ms);
  int ret = OB_SUCCESS;
  ObTabletHandle tablet_handle;
  ObTablet *tablet = NULL;
  const common::ObTabletID &tablet_id = tablet_info.tablet_id();
  const int64_t transfer_seq = tablet_info.transfer_seq();
  ObTabletCreateDeleteMdsUserData user_data;

  if (CLICK_FAIL(ls->get_tablet(tablet_id, tablet_handle, 0, ObMDSGetTabletMode::READ_WITHOUT_CHECK))) {
    LOG_WARN("failed to get tablet", K(ret), K(tablet_info));
  } else if (OB_ISNULL(tablet = tablet_handle.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet should not be NULL", K(ret), K(tablet_info), KP(tablet));
  }  else if (CLICK_FAIL(tablet->ObITabletMdsInterface::get_tablet_status(share::SCN::max_scn(), user_data, ObTabletCommon::DEFAULT_GET_TABLET_DURATION_US))) {
    LOG_WARN("failed to get tablet data", K(ret), KPC(tablet));
  } else if (ObTabletStatus::TRANSFER_OUT != user_data.tablet_status_
      || transfer_seq != tablet->get_tablet_meta().transfer_info_.transfer_seq_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet tx data is unexpected",
        K(ret),
        K(transfer_seq),
        K(user_data),
        KPC(tablet));
  }
  return ret;
}

int ObTabletFinishTransferOutHelper::update_transfer_tablets_deleted_(
    const ObTXFinishTransferOutInfo &tx_finish_transfer_out_info,
    storage::ObLS *ls,
    mds::BufferCtx &ctx)
{
  MDS_TG(500_ms);
  int ret = OB_SUCCESS;
  const common::ObSArray<share::ObTransferTabletInfo> &tablet_list = tx_finish_transfer_out_info.tablet_list_;
  for (int64_t i = 0; OB_SUCC(ret) && i < tablet_list.count(); ++i) {
    const share::ObTransferTabletInfo &tablet_info = tablet_list.at(i);
    if (OB_FAIL(update_transfer_tablet_deleted_(tablet_info, ls, ctx))) {
      LOG_WARN("failed to update transfer tablet deleted", K(ret), K(tablet_info), KP(ls));
    }
  }
  return ret;
}

int ObTabletFinishTransferOutHelper::update_transfer_tablet_deleted_(
    const share::ObTransferTabletInfo &tablet_info,
    storage::ObLS *ls,
    mds::BufferCtx &ctx)
{
  MDS_TG(10_ms);
  int ret = OB_SUCCESS;
  ObTabletHandle tablet_handle;
  ObTablet *tablet = NULL;
  ObTabletCreateDeleteMdsUserData user_data;
  if (!tablet_info.is_valid() || OB_ISNULL(ls)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("check src transfer tablets get invalid argument", K(ret), K(tablet_info), KP(ls));
  } else if (CLICK_FAIL(ls->get_tablet(tablet_info.tablet_id(), tablet_handle, 0, ObMDSGetTabletMode::READ_WITHOUT_CHECK))) {
    LOG_WARN("failed to get tablet", K(ret), K(tablet_info));
  } else if (OB_ISNULL(tablet = tablet_handle.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet should not be NULL", K(ret), K(tablet_info));
  } else if (CLICK_FAIL(tablet->ObITabletMdsInterface::get_tablet_status(share::SCN::max_scn(), user_data, ObTabletCommon::DEFAULT_GET_TABLET_DURATION_US))) {
    LOG_WARN("failed to get tablet data", K(ret), KPC(tablet));
  } else if (ObTabletStatus::TRANSFER_OUT != user_data.tablet_status_
      || tablet_info.transfer_seq() != tablet->get_tablet_meta().transfer_info_.transfer_seq_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet tx data is unexpected", K(ret), KPC(tablet), K(tablet_info), K(user_data));
  } else {
    LOG_INFO("[TRANSFER] inner update transfer tablet deleted", K(tablet_info), K(user_data));
    mds::MdsCtx &user_ctx = static_cast<mds::MdsCtx&>(ctx);
    user_data.tablet_status_ = ObTabletStatus::TRANSFER_OUT_DELETED;
    user_data.data_type_ = ObTabletMdsUserDataType::FINISH_TRANSFER_OUT;
    if (OB_FAIL(ls->get_tablet_svr()->set_tablet_status(tablet->get_tablet_meta().tablet_id_, user_data, user_ctx))) {
      LOG_WARN("failed to set tx data", K(ret), K(user_data), K(tablet_info));
    }
#ifdef ERRSIM
    ObTransferEventRecorder::record_tablet_transfer_event("tx_finish_transfer_out",
        ls->get_ls_id(), tablet_info.tablet_id_, tablet->get_tablet_meta().transfer_info_.transfer_seq_,
        user_data.tablet_status_, ret);
#endif
  }
  return ret;
}

int ObTabletFinishTransferOutHelper::on_replay(
    const char* buf,
    const int64_t len,
    const share::SCN &scn,
    mds::BufferCtx &ctx)
{
  MDS_TG(1_s);
  int ret = OB_SUCCESS;
  ObTXFinishTransferOutInfo tx_finish_transfer_out_info;
  int64_t pos = 0;
  ObTransferUtils::set_transfer_module();

  if (OB_ISNULL(buf) || len < 0 || !scn.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("on replay finish transfer out get invalid argument", K(ret), KP(buf), K(len), K(scn));
  } else if (CLICK_FAIL(tx_finish_transfer_out_info.deserialize(buf, len, pos))) {
    LOG_WARN("failed to deserialize tx finish transfer out info", K(ret), K(len), K(pos));
  } else if (!tx_finish_transfer_out_info.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tx finish transfer out info is unexpected", K(ret), K(tx_finish_transfer_out_info));
  } else if (CLICK_FAIL(on_replay_success_(scn, tx_finish_transfer_out_info, ctx))) {
    LOG_WARN("failed to do on_replay_success_", K(ret), K(tx_finish_transfer_out_info));
  } else if (CLICK_FAIL(ObTabletCreateDeleteMdsUserData::set_tablet_empty_shell_trigger(tx_finish_transfer_out_info.src_ls_id_))) {
    LOG_WARN("failed to set_tablet_empty_shell_trigger", K(ret), K(tx_finish_transfer_out_info));
  }
  ObTransferUtils::clear_transfer_module();
  return ret;
}

int ObTabletFinishTransferOutHelper::on_replay_success_(
    const share::SCN &scn,
    const ObTXFinishTransferOutInfo &tx_finish_transfer_out_info,
    mds::BufferCtx &ctx)
{
  MDS_TG(1_s);
  int ret = OB_SUCCESS;
  const int64_t start_ts = ObTimeUtil::current_time();
  ObLSService *ls_svr = NULL;
  ObLSHandle ls_handle;
  ObLS *ls = NULL;

  FLOG_INFO("[TRANSFER] start tx finish transfer out on_replay_success_", K(scn), K(tx_finish_transfer_out_info));
#ifdef ERRSIM
  SERVER_EVENT_ADD("transfer", "tx_finish_transfer_out",
                   "stage", "on_replay_success",
                   "tenant_id", MTL_ID(),
                   "src_ls_id", tx_finish_transfer_out_info.src_ls_id_.id(),
                   "dest_ls_id", tx_finish_transfer_out_info.dest_ls_id_.id(),
                   "finish_scn", tx_finish_transfer_out_info.finish_scn_,
                   "tablet_count", tx_finish_transfer_out_info.tablet_list_.count());
#endif

  if (OB_ISNULL(ls_svr = MTL(ObLSService *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls svr should not be NULL", K(ret), KP(ls_svr));
  } else if (CLICK_FAIL(ls_svr->get_ls(tx_finish_transfer_out_info.src_ls_id_, ls_handle, ObLSGetMod::STORAGE_MOD))) {
    LOG_WARN("failed to get ls", K(ret), K(tx_finish_transfer_out_info), K(tx_finish_transfer_out_info));
  } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls is NULL", K(ret), K(tx_finish_transfer_out_info), K(ls_handle));
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < tx_finish_transfer_out_info.tablet_list_.count(); ++i) {
    const share::ObTransferTabletInfo &tablet_info = tx_finish_transfer_out_info.tablet_list_.at(i);
    ObTabletFinishTransferOutReplayExecutor executor;
    if (OB_FAIL(executor.init(scn, tx_finish_transfer_out_info.src_ls_id_, tx_finish_transfer_out_info.dest_ls_id_, tablet_info, ctx))) {
      LOG_WARN("failed to init tablet finish transfer out replay executor", K(ret), K(tx_finish_transfer_out_info), K(tablet_info));
    } else if (OB_FAIL(executor.execute(scn, tx_finish_transfer_out_info.src_ls_id_, tablet_info.tablet_id_))) {
      LOG_WARN("failed to do finish transfer out replay execute", K(ret), K(tx_finish_transfer_out_info), K(tablet_info));
      if (OB_TABLET_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("failed to replay tablet finish transfer out", K(ret), K(scn), K(tablet_info));
      }
    }
    if (i == tx_finish_transfer_out_info.tablet_list_.count() / 2) {
#ifdef ERRSIM
        SERVER_EVENT_SYNC_ADD("TRANSFER",
            "AFTER_PART_ON_REDO_FINISH_TRANSFER_OUT",
            "src_ls_id",
            tx_finish_transfer_out_info.src_ls_id_.id(),
            "dest_ls_id",
            tx_finish_transfer_out_info.dest_ls_id_.id());
#endif
        DEBUG_SYNC(AFTER_PART_ON_REDO_FINISH_TRANSFER_OUT);
    }
  }
#ifdef ERRSIM
  SERVER_EVENT_SYNC_ADD("TRANSFER",
      "AFTER_ON_REDO_FINISH_TRANSFER_OUT",
      "src_ls_id",
      tx_finish_transfer_out_info.src_ls_id_.id(),
      "dest_ls_id",
      tx_finish_transfer_out_info.dest_ls_id_.id());
#endif
  DEBUG_SYNC(AFTER_ON_REDO_FINISH_TRANSFER_OUT);
  CLICK();
  if (OB_FAIL(ret)) {
    LOG_WARN("tx finish transfer out on_replay_success_ failed", K(ret), K(scn), K(tx_finish_transfer_out_info));
    ret = OB_EAGAIN;
  } else {
    ls->get_tablet_gc_handler()->set_tablet_persist_trigger();
    LOG_INFO("[TRANSFER] finish tx finish transfer out on_replay_success_", K(scn), K(tx_finish_transfer_out_info),
        "cost_ts", ObTimeUtil::current_time() - start_ts);
  }

  return ret;
}

/******************ObTabletFinishTransferInReplayExecutor*********************/
class ObTabletFinishTransferInReplayExecutor final : public logservice::ObTabletReplayExecutor
{
public:
  ObTabletFinishTransferInReplayExecutor();
  virtual ~ObTabletFinishTransferInReplayExecutor();

  int init(
      const share::SCN &scn,
      const share::ObLSID &src_ls_id,
      const share::ObLSID &dest_ls_id,
      const share::ObTransferTabletInfo &tablet_info,
      mds::BufferCtx &buffer_ctx);
protected:
  virtual bool is_replay_update_tablet_status_() const override
  {
    return true;
  }

  virtual int do_replay_(ObTabletHandle &tablet_handle) override;

  virtual bool is_replay_update_mds_table_() const override
  {
    return true;
  }

private:
  int check_dest_transfer_tablet_(ObTabletHandle &tablet_handle);
  int check_transfer_table_replaced_(ObTabletHandle &tablet_handle);
  int try_make_dest_ls_rebuild_();
  int set_dest_ls_rebuild_();

private:
  share::SCN scn_;
  share::ObLSID src_ls_id_;
  share::ObLSID dest_ls_id_;
  share::ObTransferTabletInfo tablet_info_;
  mds::BufferCtx *buffer_ctx_;
  DISALLOW_COPY_AND_ASSIGN(ObTabletFinishTransferInReplayExecutor);
};

ObTabletFinishTransferInReplayExecutor::ObTabletFinishTransferInReplayExecutor()
  :logservice::ObTabletReplayExecutor(),
   scn_(),
   src_ls_id_(),
   dest_ls_id_(),
   tablet_info_(),
   buffer_ctx_(nullptr)
{
}

ObTabletFinishTransferInReplayExecutor::~ObTabletFinishTransferInReplayExecutor()
{
}

int ObTabletFinishTransferInReplayExecutor::init(
    const share::SCN &scn,
    const share::ObLSID &src_ls_id,
    const share::ObLSID &dest_ls_id,
    const share::ObTransferTabletInfo &tablet_info,
    mds::BufferCtx &buffer_ctx)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("tablet finish transfer out replay executor init twice", KR(ret), K_(is_inited));
  } else if (OB_UNLIKELY(!scn.is_valid())
      || OB_UNLIKELY(!src_ls_id.is_valid())
      || OB_UNLIKELY(!dest_ls_id.is_valid())
      || OB_UNLIKELY(!tablet_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid argument", KR(ret), K(scn), K(src_ls_id), K(dest_ls_id), K(tablet_info));
  } else {
    scn_ = scn;
    src_ls_id_ = src_ls_id;
    dest_ls_id_ = dest_ls_id;
    tablet_info_ = tablet_info;
    buffer_ctx_ = &buffer_ctx;
    scn_ = scn;
    is_inited_ = true;
  }
  return ret;
}

int ObTabletFinishTransferInReplayExecutor::do_replay_(ObTabletHandle &tablet_handle)
{
  int ret = OB_SUCCESS;
  ObLSService *ls_svr = NULL;
  ObLSHandle ls_handle;
  ObLS *ls = NULL;
  ObTabletCreateDeleteMdsUserData user_data;
  mds::MdsCtx &user_ctx = static_cast<mds::MdsCtx&>(*buffer_ctx_);
  ObTablet *tablet = nullptr;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet finish transfer out replay executor do not init", K(ret));
  } else if (OB_ISNULL(ls_svr = MTL(ObLSService *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls svr should not be NULL", K(ret), KP(ls_svr));
  } else if (OB_FAIL(ls_svr->get_ls(dest_ls_id_, ls_handle, ObLSGetMod::STORAGE_MOD))) {
    LOG_WARN("failed to get ls", K(ret), K(dest_ls_id_), K(tablet_info_));
  } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls is NULL", K(ret), K(dest_ls_id_), K(ls_handle));
  } else if (OB_FAIL(check_dest_transfer_tablet_(tablet_handle))) {
    LOG_WARN("failed to check src transfer tablet", K(ret), K(tablet_handle));
  } else if (OB_FAIL(check_transfer_table_replaced_(tablet_handle))) {
    LOG_WARN("failed to check transfer table replaced", K(ret), K(tablet_handle));
  } else if (OB_ISNULL(tablet = tablet_handle.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet should not be NULL", K(ret), KP(tablet), K(tablet_info_));
  } else if (OB_FAIL(tablet->ObITabletMdsInterface::get_tablet_status(share::SCN::max_scn(), user_data, ObTabletCommon::DEFAULT_GET_TABLET_DURATION_US))) {
    LOG_WARN("failed to get tablet data", K(ret), KPC(tablet));
  } else {
    user_data.tablet_status_ = ObTabletStatus::NORMAL;
    user_data.transfer_ls_id_.reset();
    user_data.data_type_ = ObTabletMdsUserDataType::FINISH_TRANSFER_IN;
    if (OB_FAIL(replay_to_mds_table_(tablet_handle, user_data, user_ctx, scn_))) {
      LOG_WARN("failed to replay to tablet", K(ret));
    } else {
      LOG_INFO("succeed replay finish transfer in to mds table", KP(tablet), K(user_data), K(scn_));
    }
#ifdef ERRSIM
    ObTransferEventRecorder::record_tablet_transfer_event("tx_finish_transfer_in",
        dest_ls_id_, tablet->get_tablet_meta().tablet_id_,
        tablet->get_tablet_meta().transfer_info_.transfer_seq_, user_data.tablet_status_,
        ret);
#endif
  }
  return ret;
}

int ObTabletFinishTransferInReplayExecutor::check_dest_transfer_tablet_(
    ObTabletHandle &tablet_handle)
{
  int ret = OB_SUCCESS;
  ObTablet *tablet = nullptr;
  ObTabletCreateDeleteMdsUserData user_data;
  const int64_t transfer_seq = tablet_info_.transfer_seq();

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet start transfer out replay executor do not init", K(ret));
  } else if (OB_ISNULL(tablet = tablet_handle.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet should not be NULL", K(ret), KP(tablet), K(tablet_info_), K(src_ls_id_), K(dest_ls_id_));
  } else if (OB_FAIL(tablet->ObITabletMdsInterface::get_tablet_status(share::SCN::max_scn(), user_data, ObTabletCommon::DEFAULT_GET_TABLET_DURATION_US))) {
    LOG_WARN("failed to get tablet data", K(ret), KPC(tablet));
  } else if (ObTabletStatus::TRANSFER_IN != user_data.tablet_status_
      || transfer_seq + 1 != tablet->get_tablet_meta().transfer_info_.transfer_seq_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("tablet tx data is unexpected",
        K(ret),
        K(scn_),
        K(transfer_seq),
        K(user_data),
        KPC(tablet));
  }
  return ret;
}

int ObTabletFinishTransferInReplayExecutor::check_transfer_table_replaced_(
    ObTabletHandle &tablet_handle)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  bool all_replaced = false;
  bool can_skip_check = false;
  const uint64_t tenant_id = MTL_ID();
  ObTablet *tablet = tablet_handle.get_obj();

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet start transfer out replay executor do not init", K(ret));
  } else if (OB_ISNULL(tablet)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet should not be NULL", K(ret), K(tablet_handle));
  } else if (OB_FAIL(ObTabletFinishTransferUtil::check_transfer_table_replaced(tablet_handle, all_replaced))) {
    LOG_WARN("failed to check transfer table replace", K(ret), KPC(tablet));
  } else if (all_replaced) {
    //do nothing
  } else if (OB_FAIL(ObTabletFinishTransferUtil::can_skip_check_transfer_tablets(tenant_id, dest_ls_id_, scn_, can_skip_check))) {
    LOG_WARN("failed to do can skip check transfer tablets", K(ret), K_(dest_ls_id), K_(scn));
  }

#ifdef ERRSIM
    if (OB_SUCC(ret)) {
      ret = EN_TRANSFER_NEED_REBUILD ? : OB_SUCCESS;
      if (OB_FAIL(ret)) {
        STORAGE_LOG(ERROR, "fake EN_TRANSFER_NEED_REBUILD", K(ret));
        can_skip_check = false;
        all_replaced = false;
        ret = OB_SUCCESS;
      }
    }
#endif

  if (OB_FAIL(ret)) {
  } else if (can_skip_check || all_replaced) {
    //do nothing
  } else {
    ret = OB_EAGAIN;
    LOG_WARN("transfer table still exist, need retry", K(ret), K(can_skip_check), K(all_replaced), KPC(tablet));
    ObTransferService *transfer_service = nullptr;
    if (OB_ISNULL(transfer_service = MTL(ObTransferService *))) {
      tmp_ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ls service should not be null", K(tmp_ret), KP(transfer_service));
    } else if (FALSE_IT(transfer_service->wakeup())) {
    } else if (OB_SUCCESS != (tmp_ret = (try_make_dest_ls_rebuild_()))) {
      LOG_WARN("failed to try make dest ls rebuild", K(tmp_ret), K(tablet_info_), K(src_ls_id_), K(dest_ls_id_));
    }
  }
  return ret;
}

int ObTabletFinishTransferInReplayExecutor::try_make_dest_ls_rebuild_()
{
  int ret = OB_SUCCESS;
  ObLSService *ls_svr = NULL;
  ObLSHandle ls_handle;
  ObLS *ls = NULL;
  share::SCN max_decided_scn;
  ObTabletHandle src_tablet_handle;
  bool need_rebuild = false;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet start transfer out replay executor do not init", K(ret));
  } else if (OB_ISNULL(ls_svr = MTL(ObLSService *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls svr should not be NULL", K(ret), KP(ls_svr));
  } else if (OB_FAIL(ls_svr->get_ls(src_ls_id_, ls_handle, ObLSGetMod::STORAGE_MOD))) {
    LOG_WARN("failed to get ls", K(ret), K(src_ls_id_), K(tablet_info_));
    if (OB_LS_NOT_EXIST == ret) {
      //overwrite ret
      if (OB_FAIL(ObStorageHAUtils::check_transfer_ls_can_rebuild(scn_, need_rebuild))) {
        LOG_WARN("failed to check transfer ls can rebuild", K(ret), K(scn_), K(src_ls_id_));
      }
    }
  } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls should not be NULL", K(ret), K(src_ls_id_), K(ls_handle));
  } else if (OB_FAIL(ls->get_max_decided_scn(max_decided_scn))) {
    LOG_WARN("failed to get max decided scn", K(ret), KPC(ls), K(src_ls_id_));
  } else if (max_decided_scn < scn_) {
    need_rebuild = false;
    //src still exist transfer out tablet, need wait
  } else if (OB_FAIL(ls->ha_get_tablet(tablet_info_.tablet_id_, src_tablet_handle))) {
    if (OB_TABLET_NOT_EXIST == ret) {
      need_rebuild = true;
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to do ha get tablet", K(ret), K(tablet_info_), K(scn_));
    }
  } else {
    need_rebuild = false;
  }

#ifdef ERRSIM
    if (OB_SUCC(ret)) {
      ret = EN_TRANSFER_NEED_REBUILD ? : OB_SUCCESS;
      if (OB_FAIL(ret)) {
        STORAGE_LOG(ERROR, "fake EN_TRANSFER_NEED_REBUILD", K(ret));
        need_rebuild = true;
        ret = OB_SUCCESS;
      }
    }
#endif

  if (OB_SUCC(ret) && need_rebuild) {
    if (OB_FAIL(set_dest_ls_rebuild_())) {
      LOG_WARN("failed to set dest ls rebuild", K(ret), K(dest_ls_id_));
    }
  }
  return ret;
}

int ObTabletFinishTransferInReplayExecutor::set_dest_ls_rebuild_()
{
  int ret = OB_SUCCESS;
  ObRebuildService *rebuild_service = nullptr;
  const ObLSRebuildType rebuild_type(ObLSRebuildType::TRANSFER);
  if (OB_ISNULL(rebuild_service = MTL(ObRebuildService *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("rebuild should not be null", K(ret), KP(rebuild_service));
  } else if (OB_FAIL(rebuild_service->add_rebuild_ls(dest_ls_id_, rebuild_type))) {
    LOG_WARN("failed to add rebuild ls", K(ret), K(dest_ls_id_), K(rebuild_type));
  }
  return ret;
}

/******************ObTabletStartTransferInHelper*********************/
int ObTabletFinishTransferInHelper::on_register(
    const char* buf,
    const int64_t len,
    mds::BufferCtx &ctx)
{
  MDS_TG(1_s);
  int ret = OB_SUCCESS;
  ObTXFinishTransferInInfo tx_finish_transfer_in_info;
  int64_t pos = 0;
  const bool for_replay = false;
  ObTransferUtils::set_transfer_module();

  if (OB_ISNULL(buf) || len < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("on register finish transfer in get invalid argument", K(ret), KP(buf), K(len));
  } else if (CLICK_FAIL(tx_finish_transfer_in_info.deserialize(buf, len, pos))) {
    LOG_WARN("failed to deserialize tx finish transfer in info", K(ret), K(len), K(pos));
  } else if (!tx_finish_transfer_in_info.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tx finish transfer in info is unexpected", K(ret), K(tx_finish_transfer_in_info));
  } else if (CLICK_FAIL(on_register_success_(tx_finish_transfer_in_info, ctx))) {
    LOG_WARN("failed to do on register success", K(ret), K(tx_finish_transfer_in_info));
  }

  ObTransferUtils::clear_transfer_module();
  return ret;
}

int ObTabletFinishTransferInHelper::on_register_success_(
    const ObTXFinishTransferInInfo &tx_finish_transfer_in_info,
    mds::BufferCtx &ctx)
{
  MDS_TG(1_s);
  int ret = OB_SUCCESS;
  ObLSService *ls_svr = NULL;
  ObLSHandle ls_handle;
  ObLS *ls = NULL;
  const share::ObLSID &dest_ls_id = tx_finish_transfer_in_info.dest_ls_id_;
  const int64_t start_ts = ObTimeUtil::current_time();
  LOG_INFO("[TRANSFER] start tx finish transfer in on_register_success_", K(tx_finish_transfer_in_info));
#ifdef ERRSIM
  SERVER_EVENT_ADD("transfer", "tx_finish_transfer_in",
                   "stage", "on_register_success",
                   "tenant_id", MTL_ID(),
                   "src_ls_id", tx_finish_transfer_in_info.src_ls_id_.id(),
                   "dest_ls_id", tx_finish_transfer_in_info.dest_ls_id_.id(),
                   "start_scn", tx_finish_transfer_in_info.start_scn_,
                   "tablet_count", tx_finish_transfer_in_info.tablet_list_.count());
#endif

  if (OB_ISNULL(ls_svr = MTL(ObLSService *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls svr should not be NULL", K(ret), KP(ls_svr));
  } else if (CLICK_FAIL(ls_svr->get_ls(dest_ls_id, ls_handle, ObLSGetMod::STORAGE_MOD))) {
    LOG_WARN("failed to get ls", K(ret), K(dest_ls_id), K(tx_finish_transfer_in_info));
  } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls is NULL", K(ret), K(dest_ls_id), K(ls_handle));
  } else if (CLICK_FAIL(check_ls_replay_scn_(tx_finish_transfer_in_info, ls))) {
    LOG_WARN("check ls replay scn", K(ret), K(tx_finish_transfer_in_info), KPC(ls));
  } else if (CLICK_FAIL(check_transfer_in_tablets_validity_(tx_finish_transfer_in_info, ls))) {
    LOG_WARN("failed to check transfer in tablets validity", K(ret), K(tx_finish_transfer_in_info)
        , "ls_meta", ls->get_ls_meta());
  } else if (CLICK_FAIL(update_transfer_tablets_normal_(tx_finish_transfer_in_info, ls, ctx))) {
    LOG_WARN("failed to update transfer tablets normal", K(ret), K(tx_finish_transfer_in_info), KPC(ls));
  }

  if (OB_FAIL(ret)) {
    LOG_WARN("tx finish transfer in on_register_success_ failed", K(ret), K(tx_finish_transfer_in_info));
  } else {
    LOG_INFO("[TRANSFER] finish tx finish transfer in on_register_success_", K(tx_finish_transfer_in_info),
        "cost_ts", ObTimeUtil::current_time() - start_ts);
  }

  return ret;
}

int ObTabletFinishTransferInHelper::check_ls_replay_scn_(
    const ObTXFinishTransferInInfo &tx_finish_transfer_in_info,
    storage::ObLS *dest_ls)
{
  int ret = OB_SUCCESS;
  SCN max_decided_scn;
  if (OB_ISNULL(dest_ls)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dest ls should not be null", K(ret));
  } else if (OB_FAIL(dest_ls->get_max_decided_scn(max_decided_scn))) {
    LOG_WARN("failed to get max decided scn", K(ret), KPC(dest_ls));
  } else if (max_decided_scn < tx_finish_transfer_in_info.start_scn_) {
    ret = OB_EAGAIN;
    LOG_INFO("ls replay scn do not greater than max decided scn", K(ret), K(tx_finish_transfer_in_info), KPC(dest_ls), K(max_decided_scn));
  }
  return ret;
}

int ObTabletFinishTransferInHelper::check_transfer_in_tablets_validity_(
    const ObTXFinishTransferInInfo &tx_finish_transfer_in_info,
    storage::ObLS *ls)
{
  MDS_TG(500_ms);
  int ret = OB_SUCCESS;
  const common::ObSArray<share::ObTransferTabletInfo> &tablet_list = tx_finish_transfer_in_info.tablet_list_;
  for (int64_t i = 0; OB_SUCC(ret) && i < tablet_list.count(); ++i) {
    MDS_TG(10_ms);
    const share::ObTransferTabletInfo &tablet_info = tablet_list.at(i);
    bool all_replaced = false;
    ObTabletHandle tablet_handle;
    if (CLICK_FAIL(inner_check_transfer_in_tablet_validity_(tablet_info, ls))) {
      LOG_WARN("failed to inner check transfer in tablet validity", K(ret), K(tablet_info), KPC(ls));
    } else if (CLICK_FAIL(ls->get_tablet(tablet_info.tablet_id_, tablet_handle, 0, ObMDSGetTabletMode::READ_WITHOUT_CHECK))) {
      LOG_WARN("failed to get transfer in tablet", K(ret), K(tablet_info));
    } else if (CLICK_FAIL(ObTabletFinishTransferUtil::check_transfer_table_replaced(tablet_handle, all_replaced))) {
      LOG_WARN("failed to check transfer table replace", K(ret), K(tablet_info), KPC(ls));
    } else if (all_replaced) {
      //do nothing
    } else {
      ret = OB_EAGAIN;
      LOG_WARN("transfer table still exit, need retry", K(ret), K(tablet_info));
    }
  }
  return ret;
}

int ObTabletFinishTransferInHelper::inner_check_transfer_in_tablet_validity_(
    const share::ObTransferTabletInfo &tablet_info,
    storage::ObLS *ls)
{
  MDS_TG(10_ms);
  int ret = OB_SUCCESS;
  ObTabletHandle tablet_handle;
  ObTablet *tablet = NULL;
  ObTabletCreateDeleteMdsUserData data;
  const common::ObTabletID &tablet_id = tablet_info.tablet_id();
  const int64_t transfer_seq = tablet_info.transfer_seq();

  if (CLICK_FAIL(ls->get_tablet(tablet_id, tablet_handle, 0, ObMDSGetTabletMode::READ_WITHOUT_CHECK))) {
    LOG_WARN("failed to get tablet", K(ret), K(tablet_id));
  } else if (OB_ISNULL(tablet = tablet_handle.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet should not be NULL", K(ret), K(tablet_info));
  } else if (CLICK_FAIL(tablet->ObITabletMdsInterface::get_tablet_status(share::SCN::max_scn(), data, ObTabletCommon::DEFAULT_GET_TABLET_DURATION_US))) {
    LOG_WARN("failed to get tablet data", K(ret), KPC(tablet));
  } else if (ObTabletStatus::TRANSFER_IN != data.tablet_status_
      || transfer_seq + 1 != tablet->get_tablet_meta().transfer_info_.transfer_seq_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("tablet tx data is unexpected",
        K(ret),
        K(transfer_seq),
        K(data),
        KPC(tablet));
  }
  return ret;
}

int ObTabletFinishTransferInHelper::update_transfer_tablets_normal_(
    const ObTXFinishTransferInInfo &tx_finish_transfer_in_info,
    storage::ObLS *src_ls,
    mds::BufferCtx &ctx)
{
  MDS_TG(500_ms);
  int ret = OB_SUCCESS;
  const common::ObSArray<share::ObTransferTabletInfo> &tablet_list = tx_finish_transfer_in_info.tablet_list_;
  for (int64_t i = 0; OB_SUCC(ret) && i < tablet_list.count(); ++i) {
    const share::ObTransferTabletInfo &tablet_info = tablet_list.at(i);
    if (OB_FAIL(update_transfer_tablet_normal_(tablet_info, src_ls, ctx))) {
      LOG_WARN("failed to update transfer tablet normal", K(ret), K(tablet_info), KPC(src_ls));
    }
  }
  return ret;
}

int ObTabletFinishTransferInHelper::update_transfer_tablet_normal_(
    const share::ObTransferTabletInfo &tablet_info,
    storage::ObLS *ls,
    mds::BufferCtx &ctx)
{
  MDS_TG(10_ms);
  int ret = OB_SUCCESS;
  ObTabletHandle tablet_handle;
  ObTablet *tablet = NULL;
  ObTabletCreateDeleteMdsUserData data;
  mds::MdsCtx &user_ctx = static_cast<mds::MdsCtx&>(ctx);

  if (!tablet_info.is_valid() || OB_ISNULL(ls)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("check src transfer tablets get invalid argument", K(ret), K(tablet_info), KP(ls));
  } else if (CLICK_FAIL(ls->get_tablet(tablet_info.tablet_id(), tablet_handle, 0, ObMDSGetTabletMode::READ_WITHOUT_CHECK))) {
    LOG_WARN("failed to get tablet", K(ret), K(tablet_info));
  } else if (OB_ISNULL(tablet = tablet_handle.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet should not be NULL", K(ret), K(tablet_info));
  } else if (CLICK_FAIL(tablet->ObITabletMdsInterface::get_tablet_status(share::SCN::max_scn(), data, ObTabletCommon::DEFAULT_GET_TABLET_DURATION_US))) {
    LOG_WARN("failed to get tx data", K(ret), KPC(tablet), K(tablet_info));
  } else if (ObTabletStatus::TRANSFER_IN != data.tablet_status_
      || tablet_info.transfer_seq() + 1 != tablet->get_tablet_meta().transfer_info_.transfer_seq_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet tx data is unexpected", K(ret), KPC(tablet), K(tablet_info));
  } else {
    LOG_INFO("[TRANSFER] update transfer tablet normal", K(ret), K(tablet_info), K(data));
    data.tablet_status_ = ObTabletStatus::NORMAL;
    data.transfer_ls_id_.reset();
    data.data_type_ = ObTabletMdsUserDataType::FINISH_TRANSFER_IN;
    if (CLICK_FAIL(ls->get_tablet_svr()->set_tablet_status(tablet->get_tablet_meta().tablet_id_, data, user_ctx))) {
      LOG_WARN("failed to set mds data", K(ret), K(data));
    }
#ifdef ERRSIM
    ObTransferEventRecorder::record_tablet_transfer_event("tx_finish_transfer_in",
        ls->get_ls_id(), tablet_info.tablet_id(),
        tablet->get_tablet_meta().transfer_info_.transfer_seq_, data.tablet_status_, ret);
#endif
  }
  return ret;
}

int ObTabletFinishTransferInHelper::on_replay(
    const char* buf,
    const int64_t len,
    const share::SCN &scn,
    mds::BufferCtx &ctx)
{
  MDS_TG(1_s);
  int ret = OB_SUCCESS;
  ObTXFinishTransferInInfo tx_finish_transfer_in_info;
  int64_t pos = 0;
  bool skip_replay = false;
  const bool for_replay = true;
  ObTransferUtils::set_transfer_module();

  if (OB_ISNULL(buf) || len < 0 || !scn.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("on_replay_success_ finish transfer in get invalid argument", K(ret), KP(buf), K(len), K(scn));
  } else if (CLICK_FAIL(tx_finish_transfer_in_info.deserialize(buf, len, pos))) {
    LOG_WARN("failed to deserialize tx finish transfer in info", K(ret), K(len), K(pos));
  } else if (!tx_finish_transfer_in_info.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tx finish transfer in info is unexpected", K(ret), K(tx_finish_transfer_in_info));
  } else if (CLICK_FAIL(on_replay_success_(scn, tx_finish_transfer_in_info, ctx))) {
    LOG_WARN("failed to do on_replay_success_", K(ret), K(tx_finish_transfer_in_info));
  }
  ObTransferUtils::clear_transfer_module();
  return ret;
}

int ObTabletFinishTransferInHelper::on_replay_success_(
    const share::SCN &scn,
    const ObTXFinishTransferInInfo &tx_finish_transfer_in_info,
    mds::BufferCtx &ctx)
{
  MDS_TG(1_s);
  int ret = OB_SUCCESS;
  const int64_t start_ts = ObTimeUtil::current_time();
  LOG_INFO("[TRANSFER] start tx finish transfer in on_replay_success_", K(scn), K(tx_finish_transfer_in_info));
#ifdef ERRSIM
  SERVER_EVENT_ADD("transfer", "tx_finish_transfer_in",
                   "stage", "on_replay_success",
                   "tenant_id", MTL_ID(),
                   "src_ls_id", tx_finish_transfer_in_info.src_ls_id_.id(),
                   "dest_ls_id", tx_finish_transfer_in_info.dest_ls_id_.id(),
                   "start_scn", tx_finish_transfer_in_info.start_scn_,
                   "tablet_count", tx_finish_transfer_in_info.tablet_list_.count());
#endif
  ObLSService *ls_svr = NULL;
  ObLSHandle ls_handle;
  ObLS *ls = NULL;

  if (OB_ISNULL(ls_svr = MTL(ObLSService *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls svr should not be NULL", K(ret), KP(ls_svr));
  } else if (CLICK_FAIL(ls_svr->get_ls(tx_finish_transfer_in_info.dest_ls_id_, ls_handle, ObLSGetMod::STORAGE_MOD))) {
    LOG_WARN("failed to get ls", K(ret), K(tx_finish_transfer_in_info), K(tx_finish_transfer_in_info));
  } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls is NULL", K(ret), K(tx_finish_transfer_in_info), K(ls_handle));
  } else if (CLICK_FAIL(check_ls_replay_scn_(tx_finish_transfer_in_info, ls))) {
    LOG_WARN("failed to check ls replay scn", K(ret), K(tx_finish_transfer_in_info));
  } else {
    CLICK();
    for (int64_t i = 0; OB_SUCC(ret) && i < tx_finish_transfer_in_info.tablet_list_.count(); ++i) {
      const share::ObTransferTabletInfo &tablet_info = tx_finish_transfer_in_info.tablet_list_.at(i);
      ObTabletFinishTransferInReplayExecutor executor;
      if (OB_FAIL(executor.init(scn, tx_finish_transfer_in_info.src_ls_id_,
          tx_finish_transfer_in_info.dest_ls_id_, tablet_info, ctx))) {
        LOG_WARN("failed to init tablet finish transfer in replay executor", K(ret), K(tx_finish_transfer_in_info), K(tablet_info));
      } else if (OB_FAIL(executor.execute(scn, tx_finish_transfer_in_info.dest_ls_id_, tablet_info.tablet_id_))) {
        LOG_WARN("failed to do transfer finish in executor", K(ret), K(tx_finish_transfer_in_info), K(tablet_info));
      }
    }
  }

  if (OB_FAIL(ret)) {
    LOG_WARN("tx finish transfer in on_replay_success_ failed", K(ret), K(scn), K(tx_finish_transfer_in_info));
    ret = OB_EAGAIN;
  } else {
    LOG_INFO("[TRANSFER] finish tx finish transfer in on_replay_success_", K(scn), K(tx_finish_transfer_in_info),
        "cost_ts", ObTimeUtil::current_time() - start_ts);
  }

  return ret;
}

}
}
