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

#include "share/scn.h"
#include "share/ob_ls_id.h"
#include "share/transfer/ob_transfer_info.h"
#include "common/ob_version_def.h"
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
#include "ob_tablet_abort_transfer_mds_helper.h"
#include "storage/tablet/ob_tablet_create_delete_helper.h"
#include "observer/ob_server_event_history_table_operator.h"
#include "storage/high_availability/ob_transfer_service.h"
#include "storage/high_availability/ob_storage_ha_utils.h"
#define USING_LOG_PREFIX MDS

using namespace oceanbase::common;

namespace oceanbase
{
namespace storage
{

int ObTabletAbortTransferHelper::on_register(
    const char *buf,
    const int64_t len,
    mds::BufferCtx &ctx)
{
  MDS_TG(1_s);
  int ret = OB_SUCCESS;
  ObTXTransferInAbortedInfo transfer_in_aborted_info;
  int64_t pos = 0;
  const bool for_replay = false;
  ObTransferUtils::set_transfer_module();

  if (OB_ISNULL(buf) || len < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("on register transfer in aborted get invalid argument", K(ret), KP(buf), K(len));
  } else if (CLICK_FAIL(transfer_in_aborted_info.deserialize(buf, len, pos))) {
    LOG_WARN("failed to deserialize tx transfer in aborted info", K(ret), K(len), K(pos));
  } else if (!transfer_in_aborted_info.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("transfer in aborted info is unexpected", K(ret), K(transfer_in_aborted_info));
  } else if (CLICK_FAIL(on_register_success_(transfer_in_aborted_info, ctx))) {
    LOG_WARN("failed to on register", K(ret), K(transfer_in_aborted_info));
  }
  ObTransferUtils::clear_transfer_module();
  return ret;
}

int ObTabletAbortTransferHelper::on_register_success_(
    const ObTXTransferInAbortedInfo &transfer_in_aborted_info,
    mds::BufferCtx &ctx)
{
  MDS_TG(1_s);
  int ret = OB_SUCCESS;
  ObLSHandle ls_handle;
  ObLSService *ls_service = nullptr;
  ObLS *ls = nullptr;
  const int64_t start_ts = ObTimeUtil::current_time();
  share::SCN unsed_scn;
  const bool for_replay = false;
  LOG_INFO("[TRANSFER] transfer in tablet aborted on_register_success_", K(transfer_in_aborted_info));

  if (!transfer_in_aborted_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("on_register_ get invalid argument", K(ret), K(transfer_in_aborted_info));
  } else if (OB_ISNULL(ls_service = MTL(ObLSService *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls service should not be null", K(ret), KP(ls_service));
  } else if (CLICK_FAIL(ls_service->get_ls(transfer_in_aborted_info.dest_ls_id_, ls_handle, ObLSGetMod::STORAGE_MOD))) {
    LOG_WARN("fail to get ls", KR(ret), K(transfer_in_aborted_info));
  } else if (OB_UNLIKELY(nullptr == (ls = ls_handle.get_ls()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls should not be NULL", KR(ret), K(transfer_in_aborted_info), KP(ls));
  } else if (CLICK_FAIL(check_transfer_in_tablet_aborted_(unsed_scn, for_replay, transfer_in_aborted_info , ls))) {
    LOG_WARN("failed to check transfer in tablet aborted", K(ret), K(transfer_in_aborted_info), KPC(ls));
  }

  if (OB_FAIL(ret)) {
    LOG_WARN("transfer in tablet aborted on_register_ failed", K(ret), K(transfer_in_aborted_info));
  } else {
    LOG_INFO("[TRANSFER] finish transfer in tablet aborted on_register_success_", K(transfer_in_aborted_info),
        "cost_ts", ObTimeUtil::current_time() - start_ts);
  }
  return ret;
}

int ObTabletAbortTransferHelper::check_transfer_in_tablet_aborted_(
    const share::SCN &scn,
    const bool for_replay,
    const ObTXTransferInAbortedInfo &transfer_in_aborted_info,
    ObLS *ls)
{
  MDS_TG(1_s);
  int ret = OB_SUCCESS;
  if ((for_replay && !scn.is_valid()) || !transfer_in_aborted_info.is_valid() || OB_ISNULL(ls)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("check transfer in tablet aborted get invalid argument", K(ret),
        K(transfer_in_aborted_info), KP(ls));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < transfer_in_aborted_info.tablet_list_.count(); ++i) {
      const share::ObTransferTabletInfo &tablet_info = transfer_in_aborted_info.tablet_list_.at(i);
      ObTabletHandle tablet_handle;
      ObTablet *tablet = nullptr;
      if (CLICK_FAIL(ls->get_tablet(tablet_info.tablet_id_, tablet_handle, 0, ObMDSGetTabletMode::READ_WITHOUT_CHECK))) {
        if (OB_TABLET_NOT_EXIST == ret) {
          //tablet has been already deleted, skip it
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("failed to get tablet", K(ret), K(tablet_info));
        }
      } else if (OB_ISNULL(tablet = tablet_handle.get_obj())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("tablet should not be NULL", K(ret), K(tablet_info));
      } else if (tablet->is_empty_shell()) {
        //do nothing
      } else if (for_replay && tablet->get_tablet_meta().transfer_info_.transfer_start_scn_ > scn) {
        LOG_INFO("tablet is new, skip wait transfer in abort tablet", KPC(tablet), K(scn));
      } else {
        ret = OB_EAGAIN;
        LOG_WARN("tablet still exist, need retry", K(ret), K(tablet_info), KPC(tablet));
      }
    }
  }
  return ret;
}

int ObTabletAbortTransferHelper::on_replay(
    const char* buf,
    const int64_t len,
    const share::SCN &scn,
    mds::BufferCtx &ctx)
{
  MDS_TG(1_s);
  int ret = OB_SUCCESS;
  ObTXTransferInAbortedInfo transfer_in_aborted_info;
  int64_t pos = 0;
  const bool for_replay = true;
  ObTransferUtils::set_transfer_module();
  ObLSHandle ls_handle;
  ObLSService *ls_service = nullptr;
  ObLS *ls = nullptr;
  bool skip_replay = false;

  if (OB_ISNULL(buf) || len < 0 || !scn.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("on replay transfer in tablet aborted get invalid argument", K(ret), KP(buf), K(len), K(scn));
  } else if (CLICK_FAIL(transfer_in_aborted_info.deserialize(buf, len, pos))) {
    LOG_WARN("failed to deserialize transfer in aborted info", K(ret), K(len), K(pos));
  } else if (!transfer_in_aborted_info.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("transfer in aborted info is unexpected", K(ret), K(transfer_in_aborted_info));
  } else if (OB_FAIL(check_can_skip_replay_(scn, transfer_in_aborted_info, skip_replay))) {
    LOG_WARN("failed to check can skip replay", K(ret), K(scn), K(transfer_in_aborted_info));
  } else if (skip_replay) {
    LOG_INFO("skip replay transfer in abort", K(ret), K(scn));
  } else if (CLICK_FAIL(on_replay_success_(scn, transfer_in_aborted_info, ctx))) {
    LOG_WARN("failed to on register_success_", K(ret), K(scn), K(transfer_in_aborted_info));
  }
  ObTransferUtils::clear_transfer_module();
  return ret;
}

int ObTabletAbortTransferHelper::on_replay_success_(
    const share::SCN &scn,
    const ObTXTransferInAbortedInfo &transfer_in_aborted_info,
    mds::BufferCtx &ctx)
{
  MDS_TG(1_s);
  int ret = OB_SUCCESS;
  ObLSHandle ls_handle;
  ObLSService *ls_service = nullptr;
  ObLS *ls = nullptr;
  const int64_t start_ts = ObTimeUtil::current_time();
  const bool for_replay = true;
  LOG_INFO("[TRANSFER] transfer in tablet aborted on_replay_success_", K(scn), K(transfer_in_aborted_info));

  if (!scn.is_valid() || !transfer_in_aborted_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("on_replay_success_ get invalid argument", K(ret), K(scn), K(transfer_in_aborted_info));
  } else if (OB_ISNULL(ls_service = MTL(ObLSService *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls service should not be null", K(ret), KP(ls_service));
  } else if (CLICK_FAIL(ls_service->get_ls(transfer_in_aborted_info.dest_ls_id_, ls_handle, ObLSGetMod::STORAGE_MOD))) {
    LOG_WARN("fail to get ls", KR(ret), K(transfer_in_aborted_info));
  } else if (OB_UNLIKELY(nullptr == (ls = ls_handle.get_ls()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls should not be NULL", KR(ret), K(transfer_in_aborted_info), KP(ls));
  } else if (CLICK_FAIL(check_transfer_in_tablet_aborted_(scn, for_replay, transfer_in_aborted_info , ls))) {
    LOG_WARN("failed to check transfer in tablet aborted", K(ret), K(transfer_in_aborted_info), KPC(ls));
  }

  if (OB_FAIL(ret)) {
    LOG_WARN("transfer in tablet aborted on_replay_success_ failed", K(ret), K(scn), K(transfer_in_aborted_info));
    ret = OB_EAGAIN;
  } else {
    LOG_INFO("[TRANSFER] finish transfer in tablet aborted on_replay_success_", K(scn), K(transfer_in_aborted_info),
        "cost_ts", ObTimeUtil::current_time() - start_ts);
  }
  return ret;
}

bool ObTabletAbortTransferHelper::check_can_do_tx_end(
     const bool is_willing_to_commit,
     const bool for_replay,
     const share::SCN &log_scn,
     const char *buf,
     const int64_t buf_len,
     mds::BufferCtx &ctx,
     const char *&can_not_do_reason)
{
  bool b_ret = false;
  int ret = OB_SUCCESS;
  ObTXTransferInAbortedInfo transfer_in_aborted_info;
  int64_t pos = 0;
  ObTransferUtils::set_transfer_module();

  LOG_INFO("check can do finish transfer in tx end", K(is_willing_to_commit), K(for_replay), K(log_scn));
  if (OB_ISNULL(buf) || buf_len < 0 || (for_replay && !log_scn.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("check can replay finish transfer in commit get invalid argument", K(ret), KP(buf), K(buf_len), K(for_replay), K(log_scn));
  } else if (OB_FAIL(transfer_in_aborted_info.deserialize(buf, buf_len, pos))) {
    LOG_WARN("failed to deserialize tx finish transfer in info", K(ret), K(buf_len), K(pos));
  } else if (!transfer_in_aborted_info.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tx finish transfer in info is unexpected", K(ret), K(transfer_in_aborted_info));
  } else if (!ObTransferUtils::enable_transfer_dml_ctrl(transfer_in_aborted_info.data_version_)) {
    //do nothing
  } else if (!is_willing_to_commit) {
    //do nothing
  } else {
    const int64_t type_id = mds::TupleTypeIdx<mds::BufferCtxTupleHelper, mds::ObAbortTransferInMdsCtx>::value;
    if (type_id != ctx.get_binding_type_id()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tablet abort transfer mds ctx type is unexpected", K(ret), K(ctx), K(type_id));
    } else {
      mds::ObAbortTransferInMdsCtx &abort_transfer_in_ctx = static_cast<mds::ObAbortTransferInMdsCtx&>(ctx);
      const share::SCN &redo_scn = abort_transfer_in_ctx.get_redo_scn();
      if (!redo_scn.is_valid() || redo_scn.is_base_scn()) {
        ret = OB_EAGAIN;
        LOG_WARN("tablet abort transfer redo scn is invalid or base scn, need retry", K(ret), K(redo_scn),
            K(abort_transfer_in_ctx), K(transfer_in_aborted_info));
      } else if (OB_FAIL(do_tx_end_before_commit_(transfer_in_aborted_info, redo_scn, can_not_do_reason))) {
        LOG_WARN("failed to do tx end before commit", K(ret), K(transfer_in_aborted_info));
      }
    }
  }

  if (OB_FAIL(ret)) {
    can_not_do_reason = "abort transfer in check can do tx end failed";
    b_ret = false;
    LOG_WARN("abort transfer check can do tx end failed, need retry", K(ret), K(is_willing_to_commit),
        K(for_replay), K(transfer_in_aborted_info));
  } else {
    b_ret = true;
  }
  ObTransferUtils::clear_transfer_module();
  return b_ret;
}

int ObTabletAbortTransferHelper::do_tx_end_before_commit_(
    const ObTXTransferInAbortedInfo &transfer_in_aborted_info,
    const share::SCN &abort_redo_scn,
    const char *&can_not_do_reason)
{
  int ret = OB_SUCCESS;
  ObLSService *ls_svr = nullptr;
  ObLSHandle ls_handle;
  ObLS *ls = nullptr;

  if (!transfer_in_aborted_info.is_valid() || !abort_redo_scn.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("abort transfer do tx end before commit get invalid argument", K(ret), K(transfer_in_aborted_info), K(abort_redo_scn));
  } else if (OB_ISNULL(ls_svr = MTL(ObLSService *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls svr should not be NULL", K(ret), KP(ls_svr));
  } else if (OB_FAIL(ls_svr->get_ls(transfer_in_aborted_info.dest_ls_id_, ls_handle, ObLSGetMod::STORAGE_MOD))) {
    LOG_WARN("failed to get ls", K(ret), K(transfer_in_aborted_info));
  } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls is NULL", K(ret), K(transfer_in_aborted_info), K(ls_handle));
  } else if (OB_FAIL(ls->cleanup_transfer_meta_info(abort_redo_scn))) {
    LOG_WARN("failed to cleanup transfer meta info", K(ret), K(transfer_in_aborted_info), K(abort_redo_scn));
  }
  return ret;
}

int ObTabletAbortTransferHelper::check_can_skip_replay_(
    const share::SCN &scn,
    const ObTXTransferInAbortedInfo &transfer_in_aborted_info,
    bool &skip_replay)
{
  int ret = OB_SUCCESS;
  skip_replay = true;
  ObLSHandle ls_handle;
  ObLSService *ls_service = nullptr;
  ObLS *ls = nullptr;

  if (!scn.is_valid() || !transfer_in_aborted_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("check can skip replay transfer abort get invalid argument", K(ret), K(scn), K(transfer_in_aborted_info));
  } else if (OB_ISNULL(ls_service = MTL(ObLSService *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls service should not be null", K(ret), KP(ls_service));
  } else if (OB_FAIL(ls_service->get_ls(transfer_in_aborted_info.dest_ls_id_, ls_handle, ObLSGetMod::STORAGE_MOD))) {
    LOG_WARN("fail to get ls", KR(ret), K(transfer_in_aborted_info));
  } else if (OB_UNLIKELY(nullptr == (ls = ls_handle.get_ls()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls should not be NULL", KR(ret), K(transfer_in_aborted_info), KP(ls));
  } else if (scn <= ls->get_tablet_change_checkpoint_scn()) {
    skip_replay = true;
    LOG_INFO("replay skip for transfer in abort", KR(ret), K(scn),
        K(transfer_in_aborted_info), K(ls->get_ls_meta()));
  } else {
  }
  return ret;
}


}
}

