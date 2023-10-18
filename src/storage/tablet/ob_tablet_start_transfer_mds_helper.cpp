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

#include "storage/tablet/ob_tablet_start_transfer_mds_helper.h"
#include "share/scn.h"
#include "share/ob_ls_id.h"
#include "share/transfer/ob_transfer_info.h"
#include "storage/ls/ob_ls_get_mod.h"
#include "storage/multi_data_source/buffer_ctx.h"
#include "storage/multi_data_source/mds_ctx.h"
#include "storage/meta_mem/ob_tenant_meta_mem_mgr.h"
#include "storage/meta_mem/ob_tablet_map_key.h"
#include "storage/meta_mem/ob_tablet_handle.h"
#include "storage/tablet/ob_tablet.h"
#include "storage/tablet/ob_tablet_create_delete_mds_user_data.h"
#include "storage/tx_storage/ob_ls_handle.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "logservice/replayservice/ob_tablet_replay_executor.h"
#include "storage/tablet/ob_tablet_create_delete_helper.h"
#include "observer/ob_server_event_history_table_operator.h"
#include "storage/high_availability/ob_transfer_service.h"
#include "storage/high_availability/ob_rebuild_service.h"
#include "storage/high_availability/ob_storage_ha_utils.h"
#define USING_LOG_PREFIX MDS

namespace oceanbase
{
namespace storage
{

/******************ObTabletStartTransferOutReplayExecutor*********************/
class ObTabletStartTransferOutReplayExecutor final : public logservice::ObTabletReplayExecutor
{
public:
  ObTabletStartTransferOutReplayExecutor();
  virtual ~ObTabletStartTransferOutReplayExecutor();

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
  DISALLOW_COPY_AND_ASSIGN(ObTabletStartTransferOutReplayExecutor);
};

ObTabletStartTransferOutReplayExecutor::ObTabletStartTransferOutReplayExecutor()
  :logservice::ObTabletReplayExecutor(),
   scn_(),
   src_ls_id_(),
   dest_ls_id_(),
   tablet_info_(),
   buffer_ctx_(nullptr)
{
}

ObTabletStartTransferOutReplayExecutor::~ObTabletStartTransferOutReplayExecutor()
{
}

int ObTabletStartTransferOutReplayExecutor::init(
    const share::SCN &scn,
    const share::ObLSID &src_ls_id,
    const share::ObLSID &dest_ls_id,
    const share::ObTransferTabletInfo &tablet_info,
    mds::BufferCtx &buffer_ctx)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("tablet start transfer out replay executor init twice", KR(ret), K_(is_inited));
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
    buffer_ctx_ = &buffer_ctx;
    tablet_info_ = tablet_info;
    scn_ = scn;
    is_inited_ = true;
  }
  return ret;
}


int ObTabletStartTransferOutReplayExecutor::do_replay_(ObTabletHandle &tablet_handle)
{
  int ret = OB_SUCCESS;
  ObTabletCreateDeleteMdsUserData user_data;
  mds::MdsCtx &user_ctx = static_cast<mds::MdsCtx&>(*buffer_ctx_);
  ObTablet *tablet = nullptr;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet start transfer out replay executor do not init", K(ret));
  } else if (OB_FAIL(check_src_transfer_tablet_(tablet_handle))) {
    LOG_WARN("failed to check src transfer tablet", K(ret), K(tablet_handle));
  } else if (OB_ISNULL(tablet = tablet_handle.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet should not be NULL", K(ret), KP(tablet), K(tablet_handle));
  } else if (OB_FAIL(tablet->ObITabletMdsInterface::get_tablet_status(share::SCN::max_scn(), user_data, ObTabletCommon::DEFAULT_GET_TABLET_DURATION_US))) {
    LOG_WARN("failed to get tx data", K(ret), KPC(tablet), K(tablet_info_));
  } else {
    user_data.transfer_ls_id_ = dest_ls_id_;
    user_data.data_type_ = ObTabletMdsUserDataType::START_TRANSFER_OUT;
    user_data.tablet_status_ = ObTabletStatus::TRANSFER_OUT;
    user_data.transfer_scn_.set_min();
    //user_data.transfer_scn_ will be update in user data on_redo
    if (OB_FAIL(replay_to_mds_table_(tablet_handle, user_data, user_ctx, scn_))) {
      LOG_WARN("failed to replay to tablet", K(ret));
    }
#ifdef ERRSIM
    ObTransferEventRecorder::record_tablet_transfer_event("tx_start_transfer_out",
        src_ls_id_, tablet->get_tablet_meta().tablet_id_,
        tablet->get_tablet_meta().transfer_info_.transfer_seq_, user_data.tablet_status_,
        ret);
#endif
  }
  return ret;
}

int ObTabletStartTransferOutReplayExecutor::check_src_transfer_tablet_(
    ObTabletHandle &tablet_handle)
{
  int ret = OB_SUCCESS;
  ObTablet *tablet = nullptr;
  ObTabletCreateDeleteMdsUserData user_data;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet start transfer out replay executor do not init", K(ret));
  } else if (OB_ISNULL(tablet = tablet_handle.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet should not be NULL", K(ret), KP(tablet), K(tablet_info_), K(src_ls_id_), K(dest_ls_id_));
  } else if (OB_FAIL(tablet->ObITabletMdsInterface::get_tablet_status(share::SCN::max_scn(), user_data, ObTabletCommon::DEFAULT_GET_TABLET_DURATION_US))) {
    LOG_WARN("failed to get tx data", K(ret), KPC(tablet), K(tablet_info_));
  } else if (scn_ <= tablet->get_tablet_meta().mds_checkpoint_scn_) {
    LOG_INFO("skip replay", K(ret), K_(scn), K(tablet->get_tablet_meta()));
  } else if (ObTabletStatus::NORMAL != user_data.tablet_status_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet status is unexpected", K(ret), KPC(tablet), K(tablet_info_), K(user_data));
  } else if (tablet_info_.transfer_seq_ != tablet->get_tablet_meta().transfer_info_.transfer_seq_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet transfer seq is unexpected", K(ret), KPC(tablet), K(tablet_info_), K(user_data));
  }
  return ret;
}

/******************ObTabletStartTransferOutHelper*********************/
int ObTabletStartTransferOutHelper::on_register(
    const char *buf,
    const int64_t len,
    mds::BufferCtx &ctx)
{
  MDS_TG(1_s);
  int ret = OB_SUCCESS;
  ObTXStartTransferOutInfo tx_start_transfer_out_info;
  int64_t pos = 0;
  const bool for_replay = false;
  ObTransferUtils::set_transfer_module();

  if (OB_ISNULL(buf) || len < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("on register start transfer out get invalid argument", K(ret), KP(buf), K(len));
  } else if (CLICK_FAIL(tx_start_transfer_out_info.deserialize(buf, len, pos))) {
    LOG_WARN("failed to deserialize tx start transfer out info", K(ret), K(len), K(pos));
  } else if (!tx_start_transfer_out_info.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tx start transfer out info is unexpected", K(ret), K(tx_start_transfer_out_info));
  } else if (CLICK_FAIL(on_register_success_(tx_start_transfer_out_info, ctx))) {
    LOG_WARN("failed to on register", K(ret), K(tx_start_transfer_out_info));
  }
  ObTransferUtils::clear_transfer_module();
  return ret;
}

int ObTabletStartTransferOutHelper::on_register_success_(
    const ObTXStartTransferOutInfo &tx_start_transfer_out_info,
    mds::BufferCtx &ctx)
{
  MDS_TG(1_s);
  int ret = OB_SUCCESS;
  ObLSHandle ls_handle;
  ObLSService *ls_service = nullptr;
  ObLS *ls = nullptr;
  const int64_t start_ts = ObTimeUtil::current_time();
  LOG_INFO("[TRANSFER] start tx start transfer out on_register_success_", K(tx_start_transfer_out_info));
#ifdef ERRSIM
  SERVER_EVENT_ADD("transfer", "tx_start_transfer_out",
                   "stage", "on_register_success",
                   "tenant_id", MTL_ID(),
                   "src_ls_id", tx_start_transfer_out_info.src_ls_id_.id(),
                   "dest_ls_id", tx_start_transfer_out_info.dest_ls_id_.id(),
                   "tablet_count", tx_start_transfer_out_info.tablet_list_.count());
#endif

  if (!tx_start_transfer_out_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("on_register_ get invalid argument", K(ret), K(tx_start_transfer_out_info));
  } else if (OB_ISNULL(ls_service = MTL(ObLSService *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls service should not be null", K(ret), KP(ls_service));
  } else if (CLICK_FAIL(ls_service->get_ls(tx_start_transfer_out_info.src_ls_id_, ls_handle, ObLSGetMod::STORAGE_MOD))) {
    LOG_WARN("fail to get ls", KR(ret), K(tx_start_transfer_out_info));
  } else if (OB_UNLIKELY(nullptr == (ls = ls_handle.get_ls()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls should not be NULL", KR(ret), K(tx_start_transfer_out_info), KP(ls));
  } else if (CLICK_FAIL(prepare_src_transfer_tablets_(tx_start_transfer_out_info , ls))) {
    LOG_WARN("failed to prepare src transfer tablets", K(ret), K(tx_start_transfer_out_info), KPC(ls));
  } else if (CLICK_FAIL(update_tablets_transfer_out_(tx_start_transfer_out_info, ls, ctx))) {
    LOG_WARN("failed to update tables transfer out", K(ret), K(tx_start_transfer_out_info), KPC(ls));
  }

  if (OB_FAIL(ret)) {
    LOG_WARN("tx start transfer out on_register_ failed", K(ret), K(tx_start_transfer_out_info));
  } else {
    LOG_INFO("[TRANSFER] finish tx start transfer out on_register_success_", K(tx_start_transfer_out_info),
        "cost_ts", ObTimeUtil::current_time() - start_ts);
  }
  return ret;
}

int ObTabletStartTransferOutHelper::prepare_src_transfer_tablets_(
    const ObTXStartTransferOutInfo &tx_start_transfer_out_info,
    ObLS *ls)
{
  MDS_TG(1_s);
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (!tx_start_transfer_out_info.is_valid() || OB_ISNULL(ls)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("prepare src transfer tablets get invalid argument", K(ret),
        K(tx_start_transfer_out_info), KP(ls));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < tx_start_transfer_out_info.tablet_list_.count(); ++i) {
      MDS_TG(50_ms);
      const share::ObTransferTabletInfo &tablet_info = tx_start_transfer_out_info.tablet_list_.at(i);
      if (OB_FAIL(prepare_src_transfer_tablet_(tablet_info, ls))) {
        LOG_WARN("failed to prepare src transfer tablet", K(ret), K(tablet_info));
      }
    }
  }
  return ret;
}

int ObTabletStartTransferOutHelper::prepare_src_transfer_tablet_(
    const share::ObTransferTabletInfo &tablet_info,
    ObLS *ls)
{
  MDS_TG(10_ms);
  int ret = OB_SUCCESS;
  ObTabletHandle tablet_handle;
  ObTablet *tablet = nullptr;
  if (!tablet_info.is_valid() || OB_ISNULL(ls)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("check src transfer tablets get invalid argument", K(ret), K(tablet_info), KP(ls));
  } else if (CLICK_FAIL(ls->get_tablet(tablet_info.tablet_id_, tablet_handle, 0, ObMDSGetTabletMode::READ_WITHOUT_CHECK))) {
    LOG_WARN("failed to get tablet", K(ret), K(tablet_info));
  } else if (OB_ISNULL(tablet = tablet_handle.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet should not be NULL", K(ret), K(tablet_info));
  } else if (CLICK_FAIL(check_src_transfer_tablet_(ls->get_ls_id(), tablet_info, tablet))) {
    LOG_WARN("failed to check src transfer tablet", K(ret), K(tablet_info), KPC(tablet));
  } else if (CLICK_FAIL(ObTXTransferUtils::set_tablet_freeze_flag(*ls, tablet))) {
    LOG_WARN("failed to freeze memtable", K(ret), K(tablet_info), KPC(tablet));
  }
  return ret;
}

int ObTabletStartTransferOutHelper::check_src_transfer_tablet_(
    const share::ObLSID &ls_id,
    const share::ObTransferTabletInfo &tablet_info,
    ObTablet *tablet)
{
  MDS_TG(10_ms);
  int ret = OB_SUCCESS;
  ObTabletCreateDeleteMdsUserData user_data;

  if (!tablet_info.is_valid() || OB_ISNULL(tablet)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("check src transfer tablets get invalid argument", K(ret), K(tablet_info), KP(tablet));
  } else if (CLICK_FAIL(tablet->ObITabletMdsInterface::get_tablet_status(share::SCN::max_scn(), user_data, ObTabletCommon::DEFAULT_GET_TABLET_DURATION_US))) {
    LOG_WARN("failed to get tx data", K(ret), KPC(tablet), K(tablet_info));
  } else if (ObTabletStatus::NORMAL != user_data.tablet_status_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet status is unexpected", K(ret), KPC(tablet), K(tablet_info), K(user_data));
  } else if (tablet_info.transfer_seq_ != tablet->get_tablet_meta().transfer_info_.transfer_seq_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet transfer seq is unexpected", K(ret), KPC(tablet), K(tablet_info), K(user_data));
  }
  return ret;
}

int ObTabletStartTransferOutHelper::update_tablets_transfer_out_(
    const ObTXStartTransferOutInfo &tx_start_transfer_out_info,
    ObLS *ls,
    mds::BufferCtx &ctx)
{
  MDS_TG(1_s);
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (!tx_start_transfer_out_info.is_valid() || OB_ISNULL(ls)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("update tablets transfer out get invalid argument", K(ret),
        K(tx_start_transfer_out_info), KP(ls));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < tx_start_transfer_out_info.tablet_list_.count(); ++i) {
      MDS_TG(50_ms);
      const share::ObTransferTabletInfo &tablet_info = tx_start_transfer_out_info.tablet_list_.at(i);
      const share::ObLSID &dest_ls_id = tx_start_transfer_out_info.dest_ls_id_;
      if (CLICK_FAIL(update_tablet_transfer_out_(dest_ls_id, tablet_info, ls, ctx))) {
        LOG_WARN("failed to update tablet transfer out", K(ret), K(dest_ls_id), K(tablet_info));
      }
    }
  }
  return ret;
}

int ObTabletStartTransferOutHelper::update_tablet_transfer_out_(
    const share::ObLSID &dest_ls_id,
    const share::ObTransferTabletInfo &tablet_info,
    ObLS *ls,
    mds::BufferCtx &ctx)
{
  MDS_TG(50_ms);
  int ret = OB_SUCCESS;
  ObTabletHandle tablet_handle;
  ObTablet *tablet = nullptr;
  ObTabletCreateDeleteMdsUserData user_data;

  if (!tablet_info.is_valid() || OB_ISNULL(ls)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("check src transfer tablets get invalid argument", K(ret), K(tablet_info), KP(ls));
  } else if (CLICK_FAIL(ls->get_tablet(tablet_info.tablet_id_, tablet_handle, 0,
      ObMDSGetTabletMode::READ_WITHOUT_CHECK))) {
    LOG_WARN("failed to get tablet", K(ret), K(tablet_info));
  } else if (OB_ISNULL(tablet = tablet_handle.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet should not be NULL", K(ret), K(tablet_info));
  } else if (CLICK_FAIL(tablet->ObITabletMdsInterface::get_tablet_status(share::SCN::max_scn(), user_data, ObTabletCommon::DEFAULT_GET_TABLET_DURATION_US))) {
    LOG_WARN("failed to get tx data", K(ret), KPC(tablet), K(tablet_info));
  } else if (ObTabletStatus::NORMAL != user_data.tablet_status_
      || tablet->get_tablet_meta().transfer_info_.transfer_seq_ != tablet_info.transfer_seq_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet user data is unexpected", K(ret), KPC(tablet), K(tablet_info), K(user_data));
  } else {
    mds::MdsCtx &user_ctx = static_cast<mds::MdsCtx&>(ctx);
    user_data.transfer_ls_id_ = dest_ls_id;
    user_data.data_type_ = ObTabletMdsUserDataType::START_TRANSFER_OUT;
    user_data.tablet_status_ = ObTabletStatus::TRANSFER_OUT;
    user_data.transfer_scn_.set_min();
    //user_data.transfer_scn_ will be update in user data on_redo
    //now here setting min value is inorder to check transfer scn which is setted in redo stage.
    if (CLICK_FAIL(ls->get_tablet_svr()->set_tablet_status(tablet->get_tablet_meta().tablet_id_, user_data, user_ctx))) {
      LOG_WARN("failed to set user data", K(ret), K(user_data), K(tablet_info));
    } else {
      LOG_INFO("succeed to update tablet status transfer out", KPC(tablet), K(user_data), K(tablet_info));
    }
#ifdef ERRSIM
    ObTabletStatus tablet_status(ObTabletStatus::TRANSFER_OUT);
    ObTransferEventRecorder::record_tablet_transfer_event("tx_start_transfer_out",
        ls->get_ls_id(), tablet_info.tablet_id_,
        tablet->get_tablet_meta().transfer_info_.transfer_seq_, tablet_status,
        ret);
#endif
  }
  return ret;
}

int ObTabletStartTransferOutHelper::on_replay(
    const char* buf,
    const int64_t len,
    const share::SCN &scn,
    mds::BufferCtx &ctx)
{
  MDS_TG(1_s);
  int ret = OB_SUCCESS;
  ObTXStartTransferOutInfo tx_start_transfer_out_info;
  int64_t pos = 0;
  const bool for_replay = true;
  ObTransferUtils::set_transfer_module();

  if (OB_ISNULL(buf) || len < 0 || !scn.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("on replay start transfer out get invalid argument", K(ret), KP(buf), K(len), K(scn));
  } else if (CLICK_FAIL(tx_start_transfer_out_info.deserialize(buf, len, pos))) {
    LOG_WARN("failed to deserialize tx start transfer out info", K(ret), K(len), K(pos));
  } else if (!tx_start_transfer_out_info.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tx start transfer out info is unexpected", K(ret), K(tx_start_transfer_out_info));
  } else {
#ifdef ERRSIM
    omt::ObTenantConfigGuard tenant_config(TENANT_CONF(MTL_ID()));
    if (tenant_config.is_valid()) {
      const bool block_transfer_out_replay = tenant_config->block_transfer_out_replay;
      if (block_transfer_out_replay) {
        ret = OB_EAGAIN;
        LOG_WARN("errsim block transfer out replay", K(ret));
      }
    }
#endif
  }
#ifdef ERRSIM
  SERVER_EVENT_SYNC_ADD("TRANSFER", "BEFORE_ON_REDO_START_TRANSFER_OUT",
                        "tenant_id", MTL_ID(),
                        "src_ls_id", tx_start_transfer_out_info.src_ls_id_.id(),
                        "dest_ls_id", tx_start_transfer_out_info.dest_ls_id_.id(),
                        "for_replay", for_replay,
                        "scn", scn);
#endif
  DEBUG_SYNC(BEFORE_ON_REDO_START_TRANSFER_OUT);
  if (CLICK() && FAILEDx(on_replay_success_(scn, tx_start_transfer_out_info, ctx))) {
    LOG_WARN("failed to on register_success_", K(ret), K(scn), K(tx_start_transfer_out_info));
  }
#ifdef ERRSIM
  SERVER_EVENT_SYNC_ADD("TRANSFER", "AFTER_ON_REDO_START_TRANSFER_OUT",
                        "tenant_id", MTL_ID(),
                        "src_ls_id", tx_start_transfer_out_info.src_ls_id_.id(),
                        "dest_ls_id", tx_start_transfer_out_info.dest_ls_id_.id(),
                        "for_replay", for_replay,
                        "scn", scn);
#endif
  DEBUG_SYNC(AFTER_ON_REDO_START_TRANSFER_OUT);
  ObTransferUtils::clear_transfer_module();
  return ret;
}

int ObTabletStartTransferOutHelper::try_enable_dest_ls_clog_replay(
    const share::SCN &scn,
    const share::ObLSID &dest_ls_id)
{
  MDS_TG(100_ms);
  int ret = OB_SUCCESS;
  ObLSService* ls_srv = nullptr;
  ObLSHandle dest_ls_handle;
  ObLS *dest_ls = NULL;
  SCN max_decided_scn;
  bool can_replay = true;
  ObLSTransferInfo transfer_info;
  if (!scn.is_valid() || !dest_ls_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("replay scn or dest ls id is invalid", K(ret), K(scn), K(dest_ls_id));
  } else if (OB_ISNULL(ls_srv = MTL(ObLSService*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("ls srv should not be NULL", K(ret), KP(ls_srv));
  } else if (CLICK_FAIL(ls_srv->get_ls(dest_ls_id, dest_ls_handle, ObLSGetMod::STORAGE_MOD))) {
    if (OB_LS_NOT_EXIST == ret) {
      LOG_INFO("ls not exist", KR(ret), K(dest_ls_id));
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to get ls", KR(ret), K(dest_ls_id));
    }
  } else if (OB_ISNULL(dest_ls = dest_ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls is NULL", KR(ret), K(dest_ls));
  } else if (dest_ls->get_ls_startup_transfer_info().already_enable_replay()) {
    // do nothing
  } else if (scn < dest_ls->get_ls_startup_transfer_info().transfer_start_scn_) {
    LOG_INFO("replay scn is smaller than transfer start scn, no need enable clog replay", K(dest_ls_id), K(scn),
        "ls_startup_transfer_info", dest_ls->get_ls_startup_transfer_info());
  } else {
    transfer_info = dest_ls->get_ls_startup_transfer_info();
    dest_ls->get_ls_startup_transfer_info().reset();
    if (OB_FAIL(dest_ls->check_can_replay_clog(can_replay))) {
      LOG_WARN("failed to check can replay clog", KR(ret), K(dest_ls));
    } else if (!can_replay) {
      // do nothing
    } else if (CLICK_FAIL(dest_ls->enable_replay())) {
      dest_ls->get_ls_startup_transfer_info() = transfer_info;
      LOG_ERROR("fail to enable replay", K(ret), K(scn), K(dest_ls_id), "ls_startup_transfer_info", dest_ls->get_ls_startup_transfer_info());
    } else {
      LOG_INFO("succ enable ls clog replay", K(dest_ls_id), K(scn), "ls_startup_transfer_info", dest_ls->get_ls_startup_transfer_info());
    }
  }

  return ret;
}

int ObTabletStartTransferOutHelper::set_transfer_tablets_freeze_flag_(const ObTXStartTransferOutInfo &tx_start_transfer_out_info)
{
  int ret = OB_SUCCESS;
  ObLSService *ls_service = nullptr;
  ObLS *ls = nullptr;
  ObLSHandle ls_handle;
  if (!tx_start_transfer_out_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tx_start_transfer_out_info));
  } else if (OB_ISNULL(ls_service = MTL(ObLSService*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get ObLSService from MTL", K(ret), KP(ls_service));
  } else if (OB_FAIL(ls_service->get_ls(tx_start_transfer_out_info.src_ls_id_, ls_handle, ObLSGetMod::HA_MOD))) {
    LOG_WARN("failed to get ls", K(ret));
  } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls should not be NULL", K(ret), KP(ls));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < tx_start_transfer_out_info.tablet_list_.count(); ++i) {
      const share::ObTransferTabletInfo &tablet_info = tx_start_transfer_out_info.tablet_list_.at(i);
      const ObTabletMapKey key(tx_start_transfer_out_info.src_ls_id_, tablet_info.tablet_id_);
      ObTabletHandle tablet_handle;
      ObTablet *tablet = nullptr;
      if (OB_FAIL(ObTabletCreateDeleteHelper::get_tablet(key, tablet_handle))) {
        if (OB_TABLET_NOT_EXIST == ret) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("failed to get tablet", K(ret), K(key));
        }
      } else if (OB_ISNULL(tablet = tablet_handle.get_obj())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("tablet should not be NULL", K(ret), KP(tablet), K(key));
      } else if (OB_FAIL(ObTXTransferUtils::set_tablet_freeze_flag(*ls, tablet))) {
        LOG_WARN("failed to freeze tablet memtable", K(ret), K(key));
      }
    }
  }
  return ret;
}

int ObTabletStartTransferOutHelper::on_replay_success_(
    const share::SCN &scn,
    const ObTXStartTransferOutInfo &tx_start_transfer_out_info,
    mds::BufferCtx &ctx)
{
  MDS_TG(1_s);
  int ret = OB_SUCCESS;
  ObLSHandle ls_handle;
  ObLSService *ls_service = nullptr;
  ObLS *ls = nullptr;
  const int64_t start_ts = ObTimeUtil::current_time();
  LOG_INFO("[TRANSFER] start tx start transfer out on_replay_success_", K(scn), K(tx_start_transfer_out_info));
#ifdef ERRSIM
  SERVER_EVENT_ADD("transfer", "tx_start_transfer_out",
                   "stage", "on_replay_success",
                   "tenant_id", MTL_ID(),
                   "src_ls_id", tx_start_transfer_out_info.src_ls_id_.id(),
                   "dest_ls_id", tx_start_transfer_out_info.dest_ls_id_.id(),
                   "tablet_count", tx_start_transfer_out_info.tablet_list_.count());
#endif

  if (!scn.is_valid() || !tx_start_transfer_out_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("on_replay_success_ get invalid argument", K(ret), K(scn), K(tx_start_transfer_out_info));
  } else if (CLICK_FAIL(try_enable_dest_ls_clog_replay(scn, tx_start_transfer_out_info.dest_ls_id_))) {
    LOG_WARN("failed to try enable dest ls clog replay", K(ret), K(scn), K(tx_start_transfer_out_info));
  } else if (CLICK_FAIL(set_transfer_tablets_freeze_flag_(tx_start_transfer_out_info))) {
    LOG_WARN("failed to set transfer src tablets freeze flag", K(ret), K(scn), K(tx_start_transfer_out_info));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < tx_start_transfer_out_info.tablet_list_.count(); ++i) {
      MDS_TG(10_ms);
      const share::ObTransferTabletInfo &tablet_info = tx_start_transfer_out_info.tablet_list_.at(i);
      ObTabletStartTransferOutReplayExecutor executor;
      if (CLICK_FAIL(executor.init(scn, tx_start_transfer_out_info.src_ls_id_, tx_start_transfer_out_info.dest_ls_id_, tablet_info, ctx))) {
        LOG_WARN("failed to init tablet start transfer out replay executor", K(ret), K(scn), K(tx_start_transfer_out_info), K(tablet_info));
      } else if (CLICK_FAIL(executor.execute(scn, tx_start_transfer_out_info.src_ls_id_, tablet_info.tablet_id_))) {
        LOG_WARN("failed to execute start transfer out replay", K(ret), K(scn), K(tx_start_transfer_out_info), K(tablet_info));
      }
    }
  }
  if (OB_FAIL(ret)) {
    LOG_WARN("tx start transfer out on_replay_success_ failed", K(ret), K(scn), K(tx_start_transfer_out_info));
    ret = OB_EAGAIN;
  } else {
    LOG_INFO("[TRANSFER] finish tx start transfer out on_replay_success_", K(scn), K(tx_start_transfer_out_info),
        "cost_ts", ObTimeUtil::current_time() - start_ts);
  }
  return ret;
}

/******************ObTabletStartTransferInReplayExecutor*********************/
class ObTabletStartTransferInReplayExecutor final : public logservice::ObTabletReplayExecutor
{
public:
  ObTabletStartTransferInReplayExecutor();
  virtual ~ObTabletStartTransferInReplayExecutor();

  int init(
      const share::SCN &scn,
      const ObTabletCreateDeleteMdsUserData &user_data,
      mds::BufferCtx &user_ctx);

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

  virtual int replay_check_restore_status_(storage::ObTabletHandle &tablet_handle) override
  {
    UNUSED(tablet_handle);
    return OB_SUCCESS;
  }

private:
  share::SCN scn_;
  ObTabletCreateDeleteMdsUserData user_data_;
  mds::BufferCtx *user_ctx_;
};


ObTabletStartTransferInReplayExecutor::ObTabletStartTransferInReplayExecutor()
  :logservice::ObTabletReplayExecutor(),
   user_ctx_(nullptr)
{
}

ObTabletStartTransferInReplayExecutor::~ObTabletStartTransferInReplayExecutor()
{
}

int ObTabletStartTransferInReplayExecutor::init(
    const share::SCN &scn,
    const ObTabletCreateDeleteMdsUserData &user_data,
    mds::BufferCtx &user_ctx)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("tablet create replay executor init twice", KR(ret), K_(is_inited));
  } else if (OB_UNLIKELY(!scn.is_valid()) || OB_UNLIKELY(!user_data.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid argument", KR(ret), K(user_data));
  } else if (OB_FAIL(user_data_.assign(user_data))) {
    LOG_WARN("fail to assign user data", KR(ret), K(user_data));
  } else {
    scn_ = scn;
    user_ctx_ = &user_ctx;
    is_inited_ = true;
  }
  return ret;
}

int ObTabletStartTransferInReplayExecutor::do_replay_(ObTabletHandle &tablet_handle)
{
  int ret = OB_SUCCESS;
  mds::MdsCtx &user_ctx = static_cast<mds::MdsCtx&>(*user_ctx_);

  if (OB_FAIL(replay_to_mds_table_(tablet_handle, user_data_, user_ctx, scn_))) {
    LOG_WARN("failed to replay to tablet", K(ret), K(tablet_handle), K(user_data_), K(scn_));
  } else {
    LOG_INFO("succeed to replay transfer in to mds table", K(ret), K(user_data_), K(scn_), K(tablet_handle));
  }

  return ret;
}

/******************ObTabletStartTransferInHelper*********************/
int ObTabletStartTransferInHelper::on_register(
    const char* buf,
    const int64_t len,
    mds::BufferCtx &ctx)
{
  MDS_TG(1_s);
  int ret = OB_SUCCESS;
  ObTXStartTransferInInfo tx_start_transfer_in_info;
  int64_t pos = 0;

  ObTransferUtils::set_transfer_module();
  if (OB_ISNULL(buf) || len < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("on register start transfer in get invalid argument", K(ret), KP(buf), K(len));
  } else if (CLICK_FAIL(tx_start_transfer_in_info.deserialize(buf, len, pos))) {
    LOG_WARN("failed to deserialize tx start transfer in info", K(ret), K(len), K(pos));
  } else if (!tx_start_transfer_in_info.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tx start transfer in info is unexpected", K(ret), K(tx_start_transfer_in_info));
  } else if (CLICK_FAIL(on_register_success_(tx_start_transfer_in_info, ctx))) {
    LOG_WARN("failed to do on register success", K(ret), K(tx_start_transfer_in_info));
  } else if (CLICK_FAIL(ObTabletCreateDeleteMdsUserData::set_tablet_gc_trigger(tx_start_transfer_in_info.dest_ls_id_))) {
    LOG_WARN("failed to set_tablet_gc_trigger", K(ret), K(tx_start_transfer_in_info));
  }
  ObTransferUtils::clear_transfer_module();
  return ret;
}

int ObTabletStartTransferInHelper::on_register_success_(
    const ObTXStartTransferInInfo &tx_start_transfer_in_info,
    mds::BufferCtx &ctx)
{
  MDS_TG(1_s);
  int ret = OB_SUCCESS;
  ObLSHandle ls_handle;
  ObLSService *ls_service = nullptr;
  ObLS *ls = nullptr;
  const share::SCN scn;
  const bool for_replay = false;
  const int64_t start_ts = ObTimeUtil::current_time();
  LOG_INFO("[TRANSFER] start tx start transfer in on_register_success_",
      K(tx_start_transfer_in_info));
  if (!tx_start_transfer_in_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("on register success get invalid argument", K(ret), K(tx_start_transfer_in_info));
  } else if (CLICK_FAIL(check_transfer_dest_tablets_(tx_start_transfer_in_info, for_replay))) {
    LOG_WARN("failed to check transfer dest tablets", K(ret), K(tx_start_transfer_in_info));
  } else if (CLICK_FAIL(check_transfer_src_tablets_(scn, for_replay, tx_start_transfer_in_info))) {
    LOG_WARN("failed to check transfer src tablets", K(ret), K(tx_start_transfer_in_info));
  } else if (CLICK_FAIL(create_transfer_in_tablets_(scn, for_replay, tx_start_transfer_in_info, ctx))) {
    LOG_WARN("failed to create transfer in tablets", K(ret), K(tx_start_transfer_in_info));
  }

  if (OB_FAIL(ret)) {
    LOG_WARN("tx start transfer in on register success failed", K(ret), K(tx_start_transfer_in_info));
  } else {
    LOG_INFO("[TRANSFER] finish tx start transfer in on_register_success_", K(tx_start_transfer_in_info),
        "cost_ts", ObTimeUtil::current_time() - start_ts);
#ifdef ERRSIM
  SERVER_EVENT_ADD("transfer", "tx_start_transfer_in",
                   "stage", "on_register_success",
                   "tenant_id", MTL_ID(),
                   "src_ls_id", tx_start_transfer_in_info.src_ls_id_.id(),
                   "dest_ls_id", tx_start_transfer_in_info.dest_ls_id_.id(),
                   "start_scn", tx_start_transfer_in_info.start_scn_);
#endif
  }
  return ret;
}

int ObTabletStartTransferInHelper::check_transfer_dest_tablets_(
    const ObTXStartTransferInInfo &tx_start_transfer_in_info,
    const bool for_replay)
{
  MDS_TG(10_ms);
  int ret = OB_SUCCESS;
  ObLSHandle ls_handle;
  ObLSService *ls_service = nullptr;
  ObLS *ls = nullptr;
  if (!tx_start_transfer_in_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("start transfer in check transfer dest tablets exist get invalid argument", K(ret), K(tx_start_transfer_in_info));
  } else if (OB_ISNULL(ls_service = MTL(ObLSService *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls service should not be null", K(ret), KP(ls_service));
  } else if (CLICK_FAIL(ls_service->get_ls(tx_start_transfer_in_info.dest_ls_id_, ls_handle, ObLSGetMod::STORAGE_MOD))) {
    LOG_WARN("fail to get ls", KR(ret), K(tx_start_transfer_in_info));
  } else if (OB_UNLIKELY(nullptr == (ls = ls_handle.get_ls()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls should not be NULL", KR(ret), K(tx_start_transfer_in_info), KP(ls));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < tx_start_transfer_in_info.tablet_meta_list_.count(); ++i) {
      MDS_TG(10_ms);
      const ObMigrationTabletParam &tablet_meta = tx_start_transfer_in_info.tablet_meta_list_.at(i);
      if (CLICK_FAIL(check_transfer_dest_tablet_(tablet_meta, for_replay, ls))) {
        LOG_WARN("failed to check transfer dest tablet", K(ret), K(tablet_meta));
      }
    }
  }
  return ret;
}

int ObTabletStartTransferInHelper::check_transfer_dest_tablet_(
    const ObMigrationTabletParam &tablet_meta,
    const bool for_replay,
    ObLS *ls)
{
  int ret = OB_SUCCESS;
  ObTabletHandle tablet_handle;
  ObTablet *tablet = nullptr;
  ObTabletCreateDeleteMdsUserData user_data;
  bool unused_committed_flag = false;
  SCN local_tablet_start_scn;
  SCN transfer_info_scn;
  int64_t local_transfer_seq = 0;
  int64_t transfer_info_seq = 0;

  if (!tablet_meta.is_valid() || OB_ISNULL(ls)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("check transfer dest tablet get invalid argument", K(ret), K(tablet_meta), KP(ls));
  } else {
    const ObTabletMapKey key(ls->get_ls_id(), tablet_meta.tablet_id_);
    if (OB_FAIL(ObTabletCreateDeleteHelper::get_tablet(key, tablet_handle))) {
      if (OB_TABLET_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("failed to get tablet", K(ret), K(tablet_meta));
      }
    } else if (OB_ISNULL(tablet = tablet_handle.get_obj())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tablet should not be NULL", K(ret), K(tablet_handle), KP(tablet));
    } else if (FALSE_IT(local_tablet_start_scn = tablet->get_tablet_meta().transfer_info_.transfer_start_scn_)) {
    } else if (FALSE_IT(transfer_info_scn = tablet_meta.transfer_info_.transfer_start_scn_)) {
    } else if (FALSE_IT(local_transfer_seq = tablet->get_tablet_meta().transfer_info_.transfer_seq_)) {
    } else if (FALSE_IT(transfer_info_seq = tablet_meta.transfer_info_.transfer_seq_)) {
    } else if (!for_replay) {
      if (!tablet->is_empty_shell()) {
        ret = OB_EAGAIN;
        LOG_WARN("on register start transfer in tablet should not exist", K(ret), K(tablet_handle), K(tablet_meta));
      } else if (local_tablet_start_scn > transfer_info_scn) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("on register start transfer in tablet empty shell start transfer scn should not bigger than create tablet transfer scn",
            K(ret), KPC(tablet), K(tablet_meta));
      }
    } else if (tablet->is_empty_shell()) {
      if ((local_tablet_start_scn > transfer_info_scn && local_transfer_seq < transfer_info_seq)
          || (local_tablet_start_scn == transfer_info_scn && local_transfer_seq != transfer_info_seq)
          || (local_tablet_start_scn < transfer_info_scn && local_transfer_seq > transfer_info_seq)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("transfer in tablet in empty shell status is unexpected", K(ret), KPC(tablet), K(tablet_meta));
      }
    } else if (local_tablet_start_scn < transfer_info_scn) {
      ret = OB_EAGAIN;
      LOG_WARN("tablet should be delete, need wait gc", K(ret), KPC(tablet), K(tablet_meta));
    } else if (local_tablet_start_scn > transfer_info_scn) {
      FLOG_INFO("tablet is already exist", KPC(tablet), K(tablet_meta));
    } else if (tablet->get_tablet_meta().transfer_info_.transfer_seq_ != tablet_meta.transfer_info_.transfer_seq_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tablet seq is not match", K(ret), KPC(tablet), K(tablet_meta));
    } else if (OB_FAIL(tablet->ObITabletMdsInterface::get_latest_tablet_status(user_data, unused_committed_flag))) {
      LOG_WARN("failed to get lastest tablet status", K(ret), KPC(tablet), K(tablet_meta));
      if (OB_SNAPSHOT_DISCARDED == ret || OB_ENTRY_NOT_EXIST == ret || OB_EMPTY_RESULT == ret) {
        //local tablet exist but mds table do not persistence
        ret = OB_SUCCESS;
        FLOG_INFO("tablet is already exist", KPC(tablet), K(tablet_meta));
      }
    } else if (ObTabletStatus::TRANSFER_IN != user_data.tablet_status_
        && ObTabletStatus::NORMAL != user_data.tablet_status_
        && ObTabletStatus::TRANSFER_OUT != user_data.tablet_status_
        && ObTabletStatus::TRANSFER_OUT_DELETED != user_data.tablet_status_
        && ObTabletStatus::DELETED != user_data.tablet_status_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tablet status is unexpected", K(ret), K(user_data), KPC(tablet), K(tablet_meta));
    } else {
      FLOG_INFO("tablet is already exist", K(tablet_handle), K(tablet_meta), K(user_data));
    }
  }
  return ret;
}

int ObTabletStartTransferInHelper::check_can_skip_replay_(
    const share::SCN &scn,
    const ObTXStartTransferInInfo &tx_start_transfer_in_info,
    bool &skip_replay)
{
  int ret = OB_SUCCESS;
  skip_replay = true;
  ObLSHandle ls_handle;
  ObLSService *ls_service = nullptr;
  ObLS *ls = nullptr;

  if (!scn.is_valid() || !tx_start_transfer_in_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("check can skip replay start transfer in get invalid argument", K(ret), K(scn), K(tx_start_transfer_in_info));
  } else if (OB_ISNULL(ls_service = MTL(ObLSService *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls service should not be null", K(ret), KP(ls_service));
  } else if (OB_FAIL(ls_service->get_ls(tx_start_transfer_in_info.dest_ls_id_, ls_handle, ObLSGetMod::STORAGE_MOD))) {
    LOG_WARN("fail to get ls", KR(ret), K(tx_start_transfer_in_info));
  } else if (OB_UNLIKELY(nullptr == (ls = ls_handle.get_ls()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls should not be NULL", KR(ret), K(tx_start_transfer_in_info), KP(ls));
  } else if (scn <= ls->get_tablet_change_checkpoint_scn()) {
    skip_replay = true;
    LOG_INFO("replay skip for register tx start transfer in", KR(ret), K(scn),
        K(tx_start_transfer_in_info), K(ls->get_ls_meta()));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < tx_start_transfer_in_info.tablet_meta_list_.count(); ++i) {
      const ObMigrationTabletParam &tablet_meta = tx_start_transfer_in_info.tablet_meta_list_.at(i);
      ObTabletHandle tablet_handle;
      ObTablet *tablet = nullptr;
      const ObTabletMapKey key(ls->get_ls_id(), tablet_meta.tablet_id_);
      if (OB_FAIL(ObTabletCreateDeleteHelper::get_tablet(key, tablet_handle))) {
        if (OB_TABLET_NOT_EXIST == ret) {
          skip_replay = false;
          ret = OB_SUCCESS;
          break;
        } else {
          LOG_WARN("failed to get tablet", K(ret), K(tablet_meta));
        }
      } else if (OB_ISNULL(tablet = tablet_handle.get_obj())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("tablet should not be NULL", K(ret), K(tablet_meta), KP(tablet));
      } else if (tablet->get_tablet_meta().mds_checkpoint_scn_ < scn) {
        skip_replay = false;
        break;
      }
    }
  }
  return ret;
}

int ObTabletStartTransferInHelper::check_transfer_src_tablets_(
    const share::SCN &scn,
    const bool for_replay,
    const ObTXStartTransferInInfo &tx_start_transfer_in_info)
{
  MDS_TG(10_ms);
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObLSHandle src_ls_handle;
  ObLS *src_ls = NULL;
  ObLSService* ls_srv = nullptr;
  SCN max_decided_scn;
  ObMigrationStatus migration_status = ObMigrationStatus::OB_MIGRATION_STATUS_NONE;
  if ((!scn.is_valid() && for_replay) || !tx_start_transfer_in_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(scn), K(for_replay), K(tx_start_transfer_in_info));
  } else if (OB_ISNULL(ls_srv = MTL(ObLSService*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("ls srv should not be NULL", K(ret), KP(ls_srv));
  } else if (CLICK_FAIL(ls_srv->get_ls(tx_start_transfer_in_info.src_ls_id_, src_ls_handle, ObLSGetMod::STORAGE_MOD))) {
    LOG_WARN("ls_srv->get_ls() fail", KR(ret), "src ls id", tx_start_transfer_in_info.src_ls_id_);
    if (OB_LS_NOT_EXIST == ret) {
      bool need_rebuild = false;
      if (!for_replay) {
        // do nothing
      } else if (OB_SUCCESS != (tmp_ret = ObStorageHAUtils::check_transfer_ls_can_rebuild(scn, need_rebuild))) {
        LOG_WARN("failed to check transfer ls can rebuild", K(tmp_ret), K(scn), K(tx_start_transfer_in_info));
      } else if (need_rebuild && OB_SUCCESS != (tmp_ret = set_dest_ls_rebuild_(tx_start_transfer_in_info.dest_ls_id_))) {
        LOG_WARN("failed to set dest ls rebuild", K(tmp_ret), K(tx_start_transfer_in_info));
      }
    }
  } else if (OB_ISNULL(src_ls = src_ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls is NULL", KR(ret), K(src_ls));
  } else if (CLICK_FAIL(src_ls->get_migration_status(migration_status))) {
    LOG_WARN("failed to get rebuild info", K(ret), KPC(src_ls));
  } else if (ObMigrationStatus::OB_MIGRATION_STATUS_NONE != migration_status) {
    ret = OB_EAGAIN;
    LOG_WARN("src ls migration status not none", K(ret), K(scn), K(migration_status), KPC(src_ls));
  } else if (CLICK_FAIL(src_ls->get_max_decided_scn(max_decided_scn))) {
    LOG_WARN("failed to log stream get decided scn", K(ret), K(src_ls), K(tx_start_transfer_in_info));
  } else if (max_decided_scn < tx_start_transfer_in_info.start_scn_) {
    ret = OB_EAGAIN;
    LOG_WARN("src ls max decided scn is smaller than transfer start scn, need wait", K(ret), K(max_decided_scn), K(tx_start_transfer_in_info));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < tx_start_transfer_in_info.tablet_meta_list_.count(); ++i) {
      MDS_TG(10_ms);
      const ObMigrationTabletParam &tablet_meta = tx_start_transfer_in_info.tablet_meta_list_.at(i);
      if (CLICK_FAIL(check_transfer_src_tablet_(for_replay, tablet_meta, src_ls))) {
        LOG_WARN("failed to check src tablet", K(ret), K(for_replay), K(tablet_meta));
      }
    }
  }
  return ret;
}

int ObTabletStartTransferInHelper::check_transfer_src_tablet_(
    const bool for_replay,
    const ObMigrationTabletParam &tablet_meta,
    ObLS *src_ls)
{
  MDS_TG(10_ms);
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObTabletHandle tablet_handle;
  ObTablet *tablet = nullptr;
  ObTabletCreateDeleteMdsUserData user_data;
  bool unused_committed_flag = false;
  const ObLSID &dest_ls_id = tablet_meta.ls_id_;
  ObMigrationStatus migration_status = ObMigrationStatus::OB_MIGRATION_STATUS_NONE;

  //replay scn need check
  if (!tablet_meta.is_valid() || OB_ISNULL(src_ls)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("check src tablete get invalid argument", K(ret), K(tablet_meta), KP(src_ls));
  } else if (CLICK_FAIL(src_ls->get_migration_status(migration_status))) {
    LOG_WARN("failed to get rebuild info", K(ret), KPC(src_ls));
  } else if (ObMigrationStatus::OB_MIGRATION_STATUS_NONE != migration_status) {
    ret = OB_EAGAIN;
    LOG_WARN("src ls migration status not none", K(ret), K(migration_status), KPC(src_ls));
  } else if (CLICK_FAIL(src_ls->get_tablet(tablet_meta.tablet_id_, tablet_handle, 0,
      ObMDSGetTabletMode::READ_WITHOUT_CHECK))) {
    if (ret == OB_TABLET_NOT_EXIST) {
      ret = OB_EAGAIN;
      if (!for_replay) {
      } else if (OB_SUCCESS != (tmp_ret = set_dest_ls_rebuild_(dest_ls_id))) {
        LOG_WARN("failed to set dest ls rebuild", K(tmp_ret), K(dest_ls_id));
      }
    } else {
      LOG_WARN("failed to get tablet", K(ret), KPC(src_ls), K(tablet_meta));
    }
  } else if (OB_ISNULL(tablet = tablet_handle.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet should not be NULL", K(ret), KPC(src_ls), KP(tablet), K(tablet_meta));
  } else if (CLICK_FAIL(tablet->ObITabletMdsInterface::get_latest_tablet_status(user_data, unused_committed_flag))) {
    LOG_WARN("failed to get lastest tablet status", K(ret), KPC(tablet), K(tablet_meta));
  } else if (tablet->get_tablet_meta().transfer_info_.transfer_seq_ != tablet_meta.transfer_info_.transfer_seq_ - 1
      || (user_data.tablet_status_ != ObTabletStatus::TRANSFER_OUT
          && user_data.tablet_status_ != ObTabletStatus::TRANSFER_OUT_DELETED)) {
    ret = OB_EAGAIN;
    LOG_WARN("src ls tablet not ready, need retry", K(ret), K(user_data), K(tablet_meta));
    if (!for_replay) {
    } else if (OB_SUCCESS != (tmp_ret = set_dest_ls_rebuild_(dest_ls_id))) {
      LOG_WARN("failed to set dest ls rebuild", K(tmp_ret), K(dest_ls_id));
    }
  } else if (tablet->get_tablet_meta().ha_status_.is_restore_status_empty()) {
    // Minor is not exist, wait to be restored.
    ret = OB_EAGAIN;
    LOG_WARN("src ls tablet is EMPTY, need retry", K(ret), K(tablet_meta));
  }
  return ret;
}

int ObTabletStartTransferInHelper::create_transfer_in_tablets_(
    const share::SCN &scn,
    const bool for_replay,
    const ObTXStartTransferInInfo &tx_start_transfer_in_info,
    mds::BufferCtx &ctx)
{
  MDS_TG(10_ms);
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObLSHandle dest_ls_handle;
  ObLS *dest_ls = NULL;
  ObLSService* ls_srv = nullptr;
  ObArray<ObTabletID> tablet_id_array;
  ObMigrationStatus migration_status = ObMigrationStatus::OB_MIGRATION_STATUS_MAX;
  if ((!scn.is_valid() && for_replay) || !tx_start_transfer_in_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("create transfer in tablet get invalid argument", K(ret), K(scn), K(tx_start_transfer_in_info));
  } else if (OB_ISNULL(ls_srv = MTL(ObLSService*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("ls srv should not be NULL", K(ret), KP(ls_srv));
  } else if (CLICK_FAIL(ls_srv->get_ls(tx_start_transfer_in_info.dest_ls_id_, dest_ls_handle, ObLSGetMod::STORAGE_MOD))) {
    LOG_ERROR("ls_srv->get_ls() fail", KR(ret));
  } else if (OB_ISNULL(dest_ls = dest_ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls is NULL", KR(ret), KP(dest_ls));
  } else if (CLICK_FAIL(dest_ls->get_migration_status(migration_status))) {
    LOG_WARN("failed to get migration status", KR(ret), KP(dest_ls), "dest_ls_id", dest_ls->get_ls_id());
  } else if (ObMigrationStatus::OB_MIGRATION_STATUS_GC == migration_status) {
    ret = OB_LS_WAITING_SAFE_DESTROY;
    LOG_WARN("the migration status of transfer dest_ls is OB_MIGRATION_STATUS_GC", KR(ret), "dest_ls_id", dest_ls->get_ls_id());
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < tx_start_transfer_in_info.tablet_meta_list_.count(); ++i) {
      MDS_TG(10_ms);
      const ObMigrationTabletParam &tablet_meta = tx_start_transfer_in_info.tablet_meta_list_.at(i);
      if (CLICK_FAIL(create_transfer_in_tablet_(scn, for_replay, tablet_meta, dest_ls, ctx, tablet_id_array))) {
        LOG_WARN("failed to create transfer in tablet", K(ret), K(tablet_meta));
      }
    }

    // roll back operation
    if (OB_FAIL(ret)) {
      int tmp_ret = OB_SUCCESS;
      if (CLICK() && OB_TMP_FAIL(rollback_transfer_in_tablets_(tablet_id_array, dest_ls))) {
        LOG_WARN("failed to roll back remove tablets", K(tmp_ret),
            K(tx_start_transfer_in_info), K(lbt()));
        ob_usleep(1000 * 1000);
        ob_abort(); // roll back operation should NOT fail
      }
    }
  }
  return ret;
}


int ObTabletStartTransferInHelper::create_transfer_in_tablet_(
    const share::SCN &scn,
    const bool for_replay,
    const ObMigrationTabletParam &tablet_meta,
    ObLS *dest_ls,
    mds::BufferCtx &ctx,
    common::ObIArray<common::ObTabletID> &tablet_id_array)
{
  int ret = OB_SUCCESS;
  ObTabletHandle tablet_handle;
  ObTablet *tablet = nullptr;
  mds::MdsCtx &user_ctx = static_cast<mds::MdsCtx&>(ctx);
  const ObTabletID &tablet_id = tablet_meta.tablet_id_;
  const ObTabletMapKey key(dest_ls->get_ls_id(), tablet_id);
  bool need_create_tablet = false;

  if (OB_FAIL(ObTabletCreateDeleteHelper::get_tablet(key, tablet_handle))) {
    //overwrite ret
    if (OB_TABLET_NOT_EXIST == ret) {
      need_create_tablet = true;
      ret = OB_SUCCESS;
      LOG_INFO("need create transfer in tablet", K(key));
    } else {
      LOG_WARN("failed to get tablet", K(ret), KPC(dest_ls), K(tablet_meta));
    }
  } else if (OB_ISNULL(tablet = tablet_handle.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet should not be NULL", K(ret), KP(tablet));
  } else if (tablet->is_empty_shell() && tablet->get_tablet_meta().transfer_info_.transfer_start_scn_ < tablet_meta.transfer_info_.transfer_start_scn_) {
    need_create_tablet = true;
    LOG_INFO("need create transfer in tablet", K(key), K(tablet->is_empty_shell()));
  }

  if (OB_FAIL(ret)) {
  } else if (!need_create_tablet) {
    //do nothing
  } else if (OB_FAIL(tablet_id_array.push_back(tablet_meta.tablet_id_))) {
    LOG_WARN("failed to push tablet id into array", K(ret), K(tablet_meta));
  } else if (OB_FAIL(inner_create_transfer_in_tablet_(scn, for_replay, tablet_meta, dest_ls, tablet_handle))) {
    LOG_WARN("failed to create transfer in tablet", K(ret), K(scn), K(for_replay), K(tablet_meta), KPC(dest_ls));
  }


  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(tablet = tablet_handle.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet should not be NULL", K(ret), KP(tablet));
  } else {
    const mds::MdsDumpKV &kv = tablet_meta.mds_data_.tablet_status_uncommitted_kv_;
    const common::ObString &str = kv.v_.user_data_;
    ObTabletCreateDeleteMdsUserData user_data;
    int64_t pos = 0;
    if (OB_FAIL(user_data.deserialize(str.ptr(), str.length(), pos))) {
      LOG_WARN("failed to deserialize user data", K(ret));
    } else if (OB_UNLIKELY(ObTabletStatus::TRANSFER_IN != user_data.tablet_status_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tablet status is not TRANSFER_IN", K(ret), K(key), K(user_data));
    } else {
      user_data.transfer_scn_ = tablet_meta.transfer_info_.transfer_start_scn_;
    }

    if (OB_FAIL(ret)) {
    } else if (for_replay) {
      if (OB_FAIL(do_for_replay_(scn, user_data, dest_ls->get_ls_id(), tablet_meta.transfer_info_.transfer_start_scn_,
          tablet_handle, ctx))) {
        LOG_WARN("failed to do for replay", K(ret), K(scn), K(user_data));
      }
    } else {
      if (OB_FAIL(dest_ls->get_tablet_svr()->set_tablet_status(tablet_id, user_data, user_ctx))) {
        LOG_WARN("failed to set mds data", K(ret), K(user_data));
      }
    }

    if (OB_SUCC(ret)) {
      LOG_INFO("succeeded to create transfer in tablet", K(key), "ls_id", dest_ls->get_ls_id(), K(user_data));
    }
#ifdef ERRSIM
    ObTransferEventRecorder::record_tablet_transfer_event("tx_start_transfer_in",
        dest_ls->get_ls_id(), tablet->get_tablet_meta().tablet_id_,
        tablet->get_tablet_meta().transfer_info_.transfer_seq_, user_data.tablet_status_,
        ret);
#endif
  }
  return ret;
}

int ObTabletStartTransferInHelper::inner_create_transfer_in_tablet_(
    const share::SCN &scn,
    const bool for_replay,
    const ObMigrationTabletParam &tablet_meta,
    ObLS *dest_ls,
    ObTabletHandle &tablet_handle)
{
  UNUSED(scn);
  UNUSED(for_replay);
  int ret = OB_SUCCESS;
  if (OB_FAIL(dest_ls->get_tablet_svr()->create_transfer_in_tablet(dest_ls->get_ls_id(), tablet_meta, tablet_handle))) {
    LOG_WARN("failed to create transfer in tablet", K(ret), K(tablet_meta), KPC(dest_ls));
  }
  return ret;
}

int ObTabletStartTransferInHelper::rollback_transfer_in_tablets_(
    const common::ObIArray<common::ObTabletID> &tablet_id_array,
    ObLS *dest_ls)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < tablet_id_array.count(); ++i) {
    const ObTabletID &tablet_id = tablet_id_array.at(i);
    if (OB_FAIL(rollback_transfer_in_tablet_(tablet_id, dest_ls))) {
      LOG_WARN("failed to rollback transfer in tablet", K(ret), K(tablet_id_array));
    }
  }
  return ret;
}

int ObTabletStartTransferInHelper::rollback_transfer_in_tablet_(
    const common::ObTabletID &tablet_id,
    ObLS *dest_ls)
{
  int ret = OB_SUCCESS;
  ObTenantMetaMemMgr *t3m = MTL(ObTenantMetaMemMgr*);
  const ObTabletMapKey key(dest_ls->get_ls_id(), tablet_id);
  ObTabletHandle tablet_handle;

  // Try figure out if tablet needs to dec memtable ref or not
  if (OB_FAIL(dest_ls->get_tablet(tablet_id, tablet_handle, 0, ObMDSGetTabletMode::READ_WITHOUT_CHECK))) {
    if (OB_TABLET_NOT_EXIST == ret || OB_ITEM_NOT_SETTED == ret) {
      // tablet does not exist or tablet creation failed on half way, do nothing
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to get tablet", K(ret), KPC(dest_ls), K(tablet_id));
    }
  } else if (OB_FAIL(dest_ls->get_tablet_svr()->rollback_tablet(dest_ls->get_ls_id(), tablet_id))) {
    LOG_WARN("failed to rollback tablet", K(ret), K(dest_ls), K(tablet_id));
  }
  return ret;
}

int ObTabletStartTransferInHelper::on_replay(
    const char* buf,
    const int64_t len,
    const share::SCN &scn,
    mds::BufferCtx &ctx)
{
  MDS_TG(1_s);
  int ret = OB_SUCCESS;
  ObTXStartTransferInInfo tx_start_transfer_in_info;
  int64_t pos = 0;
  bool skip_replay = false;
  ObTransferService *transfer_service = nullptr;
  ObTransferUtils::set_transfer_module();

  if (OB_ISNULL(buf) || len < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("on replay start transfer in get invalid argument", K(ret), KP(buf), K(len));
  } else if (OB_ISNULL(transfer_service = MTL(ObTransferService *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls service should not be null", K(ret), KP(transfer_service));
  } else if (CLICK_FAIL(tx_start_transfer_in_info.deserialize(buf, len, pos))) {
    LOG_WARN("failed to deserialize tx start transfer in info", K(ret), K(len), K(pos));
  } else if (!tx_start_transfer_in_info.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tx start transfer in info is unexpected", K(ret), K(tx_start_transfer_in_info));
  } else if (CLICK_FAIL(check_can_skip_replay_(scn, tx_start_transfer_in_info, skip_replay))) {
    LOG_WARN("failed to check can skip replay", K(ret), K(tx_start_transfer_in_info));
  } else if (skip_replay) {
    FLOG_INFO("skip replay start transfer in", K(scn), K(tx_start_transfer_in_info));
  } else if (CLICK_FAIL(on_replay_success_(scn, tx_start_transfer_in_info, ctx))) {
    LOG_WARN("failed to do on_replay_success_", K(ret), K(tx_start_transfer_in_info));
  } else if (CLICK_FAIL(ObTabletCreateDeleteMdsUserData::set_tablet_gc_trigger(tx_start_transfer_in_info.dest_ls_id_))) {
    LOG_WARN("failed to set_tablet_gc_trigger", K(ret), K(tx_start_transfer_in_info));
  } else {
    transfer_service->wakeup();
  }
#ifdef ERRSIM
  SERVER_EVENT_SYNC_ADD("TRANSFER", "AFTER_ON_REDO_START_TRANSFER_IN");
#endif
  DEBUG_SYNC(AFTER_ON_REDO_START_TRANSFER_IN);
  ObTransferUtils::clear_transfer_module();
  return ret;
}

int ObTabletStartTransferInHelper::on_replay_success_(
    const share::SCN &scn,
    const ObTXStartTransferInInfo &tx_start_transfer_in_info,
    mds::BufferCtx &ctx)
{
  MDS_TG(1_s);
  int ret = OB_SUCCESS;
  ObLSHandle ls_handle;
  ObLSService *ls_service = nullptr;
  ObLS *ls = nullptr;
  const bool for_replay = true;
  bool can_skip_replay = false;
  bool can_skip_check_src = false;
  const int64_t start_ts = ObTimeUtil::current_time();
  LOG_INFO("[TRANSFER] start tx start transfer in on_replay_success_",
      K(tx_start_transfer_in_info));
#ifdef ERRSIM
  SERVER_EVENT_ADD("transfer", "tx_start_transfer_in",
                   "stage", "on_replay_success",
                   "tenant_id", MTL_ID(),
                   "src_ls_id", tx_start_transfer_in_info.src_ls_id_.id(),
                   "dest_ls_id", tx_start_transfer_in_info.dest_ls_id_.id(),
                   "start_scn", tx_start_transfer_in_info.start_scn_);
#endif

  if (!scn.is_valid() || !tx_start_transfer_in_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("on replay success get invalid argument", K(ret), K(scn), K(tx_start_transfer_in_info));
  } else if (CLICK_FAIL(check_transfer_dest_tablets_(tx_start_transfer_in_info, for_replay))) {
    LOG_WARN("failed to check transfer dest tablets", K(ret), K(tx_start_transfer_in_info));
  } else if (CLICK_FAIL(create_transfer_in_tablets_(scn, for_replay, tx_start_transfer_in_info, ctx))) {
    LOG_WARN("failed to create transfer in tablets", K(ret), K(tx_start_transfer_in_info));
  }

  if (OB_FAIL(ret)) {
    LOG_WARN("tx start transfer in on_replay_success_ failed", K(ret), K(tx_start_transfer_in_info));
    ret = OB_EAGAIN;
  } else {
    LOG_INFO("[TRANSFER] finish tx start transfer in on_replay_success_", K(tx_start_transfer_in_info),
        K(for_replay), "cost_ts", ObTimeUtil::current_time() - start_ts);
  }
  return ret;
}

int ObTabletStartTransferInHelper::do_for_replay_(
    const share::SCN &scn,
    const ObTabletCreateDeleteMdsUserData &user_data,
    const share::ObLSID &ls_id,
    const share::SCN &transfer_start_scn,
    ObTabletHandle &tablet_handle,
    mds::BufferCtx &ctx)
{
  int ret = OB_SUCCESS;
  ObTabletStartTransferInReplayExecutor executor;
  storage::ObTablet *tablet = tablet_handle.get_obj();
  SCN local_tablet_start_scn;

  if (OB_ISNULL(tablet)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet is NULL", K(ret), K(scn), K(user_data), K(ls_id));
  } else if (FALSE_IT(local_tablet_start_scn = tablet->get_tablet_meta().transfer_info_.transfer_start_scn_)) {
  } else if (tablet->is_empty_shell()) {
    if (local_tablet_start_scn >= transfer_start_scn) {
      LOG_INFO("tablet is empty shell and transfer start scn is bigger than transfer in tablet start scn", K(ret),
          KPC(tablet), K(transfer_start_scn));
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("local tablet is empty shell but transfer start scn is smaller than transfer in", K(ret), KPC(tablet), K(transfer_start_scn));
    }
  } else if (local_tablet_start_scn < transfer_start_scn) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("local tablet transfer start scn is smaller than transfer in", K(ret), KPC(tablet), K(transfer_start_scn));
  } else if (local_tablet_start_scn > transfer_start_scn) {
    //do nothing
  } else if (OB_FAIL(executor.init(scn, user_data, ctx))) {
    LOG_WARN("failed to init tablet start transfer in replay executor", K(ret), K(scn), K(user_data), K(tablet_handle));
  } else if (OB_FAIL(executor.execute(scn, ls_id, tablet->get_tablet_meta().tablet_id_))) {
    LOG_WARN("failed to do start transfer in replay execute", K(ret), K(scn), K(user_data), K(tablet_handle));
  }
  return ret;
}

int ObTabletStartTransferInHelper::check_can_skip_check_transfer_src_tablet_(
    const share::SCN &scn,
    const ObTXStartTransferInInfo &tx_start_transfer_in_info,
    bool &can_skip)
{
  int ret = OB_SUCCESS;
  can_skip = false;
  bool is_gts_push = false;
  ObLSRestoreStatus restore_status;
  // There are 2 cases that source tablets are no need to check ready while replaying transfer in. The following
  // order needs to be followed.
  // 1. Current ls is in restore, and scn is smaller than consistent_scn;
  // 2. GTS is over current log scn, but current ls is in rebuild.

  if (!scn.is_valid() || !tx_start_transfer_in_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("check can skip check transfer src tablet get invalid argument", K(ret));
  } else if (OB_FAIL(check_transfer_dest_tablets_ready_(scn, tx_start_transfer_in_info, can_skip))) {
    LOG_WARN("failed to check transfer dest tablets ready", K(ret), K(scn), K(tx_start_transfer_in_info));
  } else if (can_skip) {
    //do nothing
  } else if (OB_FAIL(check_transfer_dest_ls_restore_status_(scn, tx_start_transfer_in_info.dest_ls_id_, can_skip))) {
    LOG_WARN("failed to check transfer dest ls restore status", K(ret), K(scn), K(tx_start_transfer_in_info));
  } else if (can_skip) {
    //do nothing
  } else if (OB_FAIL(check_gts_(scn, is_gts_push))) {
    LOG_WARN("failed to check gts", K(ret), K(scn));
  } else if (!is_gts_push) {
    can_skip = false;
  } else if (OB_FAIL(check_transfer_dest_ls_status_(scn, tx_start_transfer_in_info.dest_ls_id_, can_skip))) {
    LOG_WARN("failed to check transfer dest ls status", K(ret), K(tx_start_transfer_in_info));
  }
  return ret;
}

int ObTabletStartTransferInHelper::check_gts_(
    const share::SCN &scn,
    bool &is_gts_push)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = MTL_ID();
  share::SCN gts_scn;
  is_gts_push = false;

  if (!scn.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("check gts get invalid argument", K(ret), K(scn));
  } else if (OB_FAIL(ObTransferUtils::get_gts(tenant_id, gts_scn))) {
    LOG_WARN("failed to get gts", K(ret), K(tenant_id), K(scn));
    is_gts_push = false;
    //overwrite ret
    ret = OB_SUCCESS;
  } else if (gts_scn <= scn) {
    is_gts_push = false;
    LOG_INFO("can not skip check transfer src ls and tablet", K(gts_scn), K(scn));
  } else {
    is_gts_push = true;
  }
  return ret;
}

int ObTabletStartTransferInHelper::check_transfer_dest_ls_status_(
    const share::SCN &scn,
    const share::ObLSID &ls_id,
    bool &can_skip)
{
  int ret = OB_SUCCESS;
  ObLSService *ls_service = nullptr;
  ObLSHandle ls_handle;
  ObLS *ls = nullptr;
  ObMigrationStatus migration_status = ObMigrationStatus::OB_MIGRATION_STATUS_MAX;
  can_skip = false;

  if (!ls_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("check transfer dest ls status get invalid argument", K(ret), K(ls_id));
  } else if (OB_ISNULL(ls_service = MTL(ObLSService *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls service should not be null", K(ret), KP(ls_service));
  } else if (OB_FAIL(ls_service->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD))) {
    LOG_WARN("fail to get ls", KR(ret), K(ls_id));
  } else if (OB_UNLIKELY(nullptr == (ls = ls_handle.get_ls()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls should not be NULL", KR(ret), K(ls_id), KP(ls));
  } else if (OB_FAIL(ls->get_migration_status(migration_status))) {
    LOG_WARN("failed to get migration status", K(ret), K(ls_id));
  } else if (ObMigrationStatus::OB_MIGRATION_STATUS_ADD == migration_status
      || ObMigrationStatus::OB_MIGRATION_STATUS_MIGRATE == migration_status
      || ObMigrationStatus::OB_MIGRATION_STATUS_REBUILD == migration_status) {
    can_skip = true;
    FLOG_INFO("ls migration status is in add or migrate or rebuild, skip check transfer src ls",
        K(ret), K(migration_status), KPC(ls));
  }
  return ret;
}

int ObTabletStartTransferInHelper::check_transfer_dest_ls_restore_status_(
    const share::SCN &scn,
    const share::ObLSID &ls_id,
    bool &can_skip)
{
  int ret = OB_SUCCESS;
  ObLSService *ls_service = nullptr;
  ObLSHandle ls_handle;
  ObLS *ls = nullptr;
  ObLSRestoreStatus restore_status;
  share::SCN consistent_scn;
  can_skip = false;
  if (!ls_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("check transfer dest ls restore status get invalid argument", K(ret), K(ls_id));
  } else if (OB_ISNULL(ls_service = MTL(ObLSService *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls service should not be null", K(ret), KP(ls_service));
  } else if (OB_FAIL(ls_service->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD))) {
    LOG_WARN("fail to get ls", KR(ret), K(ls_id));
  } else if (OB_UNLIKELY(nullptr == (ls = ls_handle.get_ls()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls should not be NULL", KR(ret), K(ls_id), KP(ls));
  } else if (OB_FAIL(ls->get_restore_status(restore_status))) {
    LOG_WARN("failed to get restore status", K(ret), KPC(ls));
  } else if (restore_status.is_in_restore()) {
    if(OB_FAIL(ls->get_ls_restore_handler()->get_consistent_scn(consistent_scn))) {
      LOG_WARN("failed to get consistent scn", K(ret));
    } else if (!consistent_scn.is_valid_and_not_min()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid consistent_scn", K(ret), K(consistent_scn));
    } else if (scn <= consistent_scn) {
      can_skip = true;
      LOG_INFO("ls is in restore, and cur scn is older than restore consistent scn, skip check transfer src ls",
          K(ret), K(restore_status), K(consistent_scn), K(scn), KPC(ls));
    }
  }
  return ret;
}

int ObTabletStartTransferInHelper::check_transfer_dest_tablets_ready_(
    const share::SCN &scn,
    const ObTXStartTransferInInfo &tx_start_transfer_in_info,
    bool &can_skip)
{
  int ret = OB_SUCCESS;
  can_skip = false;

  if (!scn.is_valid() || !tx_start_transfer_in_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("check transfer dest tablets ready invalid argument", K(ret), K(scn));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < tx_start_transfer_in_info.tablet_meta_list_.count(); ++i) {
      const ObMigrationTabletParam &tablet_meta = tx_start_transfer_in_info.tablet_meta_list_.at(i);
      if (OB_FAIL(check_transfer_dest_tablet_ready_(tablet_meta, can_skip))) {
        LOG_WARN("failed to create transfer in tablet", K(ret), K(tablet_meta));
      } else if (!can_skip) {
        break;
      }
    }
  }
  return ret;
}

int ObTabletStartTransferInHelper::check_transfer_dest_tablet_ready_(
    const ObMigrationTabletParam &tablet_meta,
    bool &can_skip)
{
  int ret = OB_SUCCESS;
  ObTabletHandle tablet_handle;
  ObTablet *tablet = nullptr;
  can_skip = false;
  SCN local_tablet_start_scn;
  SCN transfer_info_scn;

  if (!tablet_meta.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("check transfer dest tablet get invalid argument", K(ret), K(tablet_meta));
  } else {
    const ObTabletMapKey key(tablet_meta.ls_id_, tablet_meta.tablet_id_);
    if (OB_FAIL(ObTabletCreateDeleteHelper::get_tablet(key, tablet_handle))) {
      if (OB_TABLET_NOT_EXIST == ret) {
        can_skip = false;
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("failed to get tablet", K(ret), K(tablet_meta));
      }
    } else if (OB_ISNULL(tablet = tablet_handle.get_obj())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tablet should not be NULL", K(ret), K(tablet_handle), KP(tablet));
    } else if (FALSE_IT(local_tablet_start_scn = tablet->get_tablet_meta().transfer_info_.transfer_start_scn_))  {
    } else if (FALSE_IT(transfer_info_scn = tablet_meta.transfer_info_.transfer_start_scn_)) {
    } else if (tablet->is_empty_shell()) {
      if (local_tablet_start_scn >= transfer_info_scn) {
        can_skip = true;
      } else {
        can_skip = false;
      }
      LOG_INFO("tablet is empty shell", KPC(tablet), K(tablet_meta), K(can_skip));
    } else if (local_tablet_start_scn > transfer_info_scn) {
      can_skip = true;
    } else if (local_tablet_start_scn < transfer_info_scn) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tablet transfer start scn is not match", K(ret), KPC(tablet), K(tablet_meta));
    } else if (!tablet->get_tablet_meta().has_transfer_table()) {
      can_skip = true;
    } else {
      LOG_INFO("tablet still has transfer table, cannot skip check src tablet", KPC(tablet));
    }
  }
  return ret;
}

int ObTabletStartTransferInHelper::set_dest_ls_rebuild_(
    const share::ObLSID &dest_ls_id)
{
  int ret = OB_SUCCESS;
  ObRebuildService *rebuild_service = nullptr;
  const ObLSRebuildType rebuild_type(ObLSRebuildType::TRANSFER);

  if (!dest_ls_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("set dest ls rebuild get invalid argument", K(ret), K(dest_ls_id));
  } else if (OB_ISNULL(rebuild_service = MTL(ObRebuildService *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("rebuild should not be null", K(ret), KP(rebuild_service));
  } else if (OB_FAIL(rebuild_service->add_rebuild_ls(dest_ls_id, rebuild_type))) {
    LOG_WARN("failed to add rebuild ls", K(ret), K(dest_ls_id), K(rebuild_type));
  }
  return ret;
}

bool ObTabletStartTransferInHelper::check_can_replay_commit(
    const char* buf,
    const int64_t len,
    const share::SCN &scn,
    mds::BufferCtx &ctx)
{
  bool b_ret = false;
  int ret = OB_SUCCESS;
  ObTXStartTransferInInfo tx_start_transfer_in_info;
  int64_t pos = 0;
  bool skip_replay = false;
  ObTransferService *transfer_service = nullptr;
  bool can_skip_check_src = false;
  ObTransferUtils::set_transfer_module();

  LOG_INFO("check can replay start transfer in commit", K(scn));
  if (OB_ISNULL(buf) || len < 0 || !scn.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("on register start transfer in get invalid argument", K(ret), KP(buf), K(len), K(scn));
  } else if (OB_ISNULL(transfer_service = MTL(ObTransferService *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls service should not be null", K(ret), KP(transfer_service));
  } else if (OB_FAIL(tx_start_transfer_in_info.deserialize(buf, len, pos))) {
    LOG_WARN("failed to deserialize tx start transfer in info", K(ret), K(len), K(pos));
  } else if (!tx_start_transfer_in_info.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tx start transfer in info is unexpected", K(ret), K(tx_start_transfer_in_info));
  } else if (OB_FAIL(check_can_skip_replay_(scn, tx_start_transfer_in_info, skip_replay))) {
    LOG_WARN("failed to check can skip replay commit", K(ret), K(scn), K(tx_start_transfer_in_info));
  } else if (skip_replay) {
    b_ret = true;
    LOG_INFO("skip replay start transfer in commit", K(scn), K(tx_start_transfer_in_info));
  } else {
    if (OB_FAIL(check_can_skip_check_transfer_src_tablet_(scn, tx_start_transfer_in_info, can_skip_check_src))) {
      LOG_WARN("failed to check can skip check transfer src tablet", K(ret), K(tx_start_transfer_in_info));
    } else if (!can_skip_check_src && OB_FAIL(check_transfer_src_tablets_(scn, true /* for replay */, tx_start_transfer_in_info))) {
      LOG_WARN("failed to check transfer src tablets", K(ret), K(tx_start_transfer_in_info));
    }
    if (OB_FAIL(ret)) {
      b_ret = false;
      LOG_WARN("start transfer in src tablet not ready, need retry", K(ret), K(scn), K(tx_start_transfer_in_info));
    } else {
      b_ret = true;
      transfer_service->wakeup();
    }
  }
  ObTransferUtils::clear_transfer_module();
  return b_ret;
}

}
}
