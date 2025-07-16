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

#define USING_LOG_PREFIX STORAGE
#include "ob_tablet_reorg_info_service.h"
#include "storage/tx_storage/ob_ls_handle.h"
#include "share/ls/ob_ls_table_operator.h"
#include "lib/utility/ob_tracepoint.h"
#include "rootserver/ob_rs_event_history_table_operator.h"
#include "observer/ob_server.h"
#include "logservice/ob_log_service.h"
#include "observer/ob_server_event_history_table_operator.h"
#include "storage/meta_store/ob_server_storage_meta_service.h"
#include "ob_tablet_reorg_info_table_operation.h"

using namespace oceanbase;
using namespace share;
using namespace storage;

ObTabletReorgInfoTableService::ObTabletReorgInfoTableService()
  : is_inited_(false),
    thread_cond_(),
    wakeup_cnt_(0),
    ls_service_(nullptr)
{
}

ObTabletReorgInfoTableService::~ObTabletReorgInfoTableService()
{
}

int ObTabletReorgInfoTableService::mtl_init(ObTabletReorgInfoTableService *&reorg_info_service)
{
  int ret = OB_SUCCESS;
  ObLSService *ls_service = nullptr;

  if (OB_ISNULL(ls_service =  (MTL(ObLSService *)))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls service should not be NULL", K(ret), KP(ls_service));
  } else if (OB_FAIL(reorg_info_service->init(ls_service))) {
    LOG_WARN("failed to init tablet reorg info service", K(ret), KP(ls_service));
  }
  return ret;
}

int ObTabletReorgInfoTableService::init(
    ObLSService *ls_service)
{
  int ret = OB_SUCCESS;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("tablet reorg info table service is already init", K(ret));
  } else if (OB_ISNULL(ls_service)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("init tablet reorg info table service get invalid argument", K(ret), KP(ls_service));
  } else if (OB_FAIL(thread_cond_.init(ObWaitEventIds::HA_SERVICE_COND_WAIT))) {
    LOG_WARN("failed to init ha service thread cond", K(ret));
  } else {
    lib::ThreadPool::set_run_wrapper(MTL_CTX());
    ls_service_ = ls_service;
    is_inited_ = true;
  }
  return ret;
}

void ObTabletReorgInfoTableService::wakeup()
{
  ObThreadCondGuard cond_guard(thread_cond_);
  wakeup_cnt_++;
  thread_cond_.signal();
}

void ObTabletReorgInfoTableService::destroy()
{
  if (is_inited_) {
    COMMON_LOG(INFO, "ObTabletReorgInfoTableService starts to destroy");
    thread_cond_.destroy();
    wakeup_cnt_ = 0;
    is_inited_ = false;
    COMMON_LOG(INFO, "ObTabletReorgInfoTableService destroyed");
  }
}

void ObTabletReorgInfoTableService::stop()
{
  if (is_inited_) {
    COMMON_LOG(INFO, "ObTabletReorgInfoTableService starts to stop");
    ThreadPool::stop();
    wakeup();
    COMMON_LOG(INFO, "ObTabletReorgInfoTableService stopped");
  }
}

void ObTabletReorgInfoTableService::wait()
{
  if (is_inited_) {
    COMMON_LOG(INFO, "ObTabletReorgInfoTableService starts to wait");
    ThreadPool::wait();
    COMMON_LOG(INFO, "ObTabletReorgInfoTableService finish to wait");
  }
}

int ObTabletReorgInfoTableService::start()
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet reorg info table service do not init", K(ret));
  } else {
    if (OB_FAIL(lib::ThreadPool::start())) {
      COMMON_LOG(WARN, "ObTabletReorgInfoTableService start thread failed", K(ret));
    } else {
      COMMON_LOG(INFO, "ObTabletReorgInfoTableService start");
    }
  }
  return ret;
}

void ObTabletReorgInfoTableService::run1()
{
  int tmp_ret = OB_SUCCESS;
  lib::set_thread_name("ReorgSrv");

  while (!has_set_stop()) {
    if (SS_SERVING != GCTX.status_) {
      tmp_ret = OB_SERVER_IS_INIT;
      LOG_WARN_RET(tmp_ret, "server is not serving", K(GCTX.status_));
    } else if (OB_SUCCESS != (tmp_ret = calc_max_recycle_scn_())) {
      LOG_WARN_RET(tmp_ret, "failed to calc max recycle scn");
    }

    ObThreadCondGuard guard(thread_cond_);
    if (has_set_stop() || wakeup_cnt_ > 0) {
      wakeup_cnt_ = 0;
    } else {
      int64_t wait_time_ms = SCHEDULER_WAIT_TIME_MS;
      if (OB_SERVER_IS_INIT == tmp_ret) {
        wait_time_ms = WAIT_SERVER_IN_SERVICE_TIME_MS;
      }
      ObBKGDSessInActiveGuard inactive_guard;
      thread_cond_.wait(wait_time_ms);
    }
  }
}

int ObTabletReorgInfoTableService::calc_max_recycle_scn_()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObArray<share::ObLSID> ls_id_array;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet reorg info table service do not init", K(ret));
  } else if (OB_FAIL(ls_service_->get_ls_ids(ls_id_array))) {
    LOG_WARN("failed to get ls ids", K(ret));
  } else {
    for (int64_t i = 0; i < ls_id_array.count(); ++i) {
      const ObLSID &ls_id = ls_id_array.at(i);
      if (OB_SUCCESS != (tmp_ret = calc_ls_max_recycle_scn_(ls_id))) {
        LOG_WARN("failed to calc ls max recycle scn", K(tmp_ret), K(ls_id));
      }
    }
  }
  return ret;
}

int ObTabletReorgInfoTableService::calc_ls_max_recycle_scn_(const share::ObLSID &ls_id)
{
  int ret = OB_SUCCESS;
  ObTabletReorgInfoTableReadOperator read_op;
  const uint64_t tenant_id = MTL_ID();
  const int64_t timeout_us = 600 * 1000 * 1000; //10min
  share::SCN max_recycle_scn(share::SCN::min_scn());
  share::SCN min_unrecycle_scn(share::SCN::max_scn());
  share::SCN recycle_scn;
  ObLSHandle ls_handle;
  ObLS *ls = nullptr;
  bool can_recycle = false;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet reorg info table service do not init", K(ret));
  } else if (!ls_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("calc ls max recycle scn get invalid argument", K(ret), K(ls_id));
  } else if (OB_FAIL(ls_service_->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD))) {
    LOG_WARN("failed to get ls", K(ret), K(ls_id));
  } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls should not be NULL", K(ret), K(ls_id), K(ls_handle));
  } else if (OB_FAIL(read_op.init(tenant_id, ls_id, timeout_us))) {
    LOG_WARN("failed to init read operator", K(ret), K(tenant_id), K(ls_id));
  } else {
    ObTabletReorgInfoData data;
    share::SCN commit_scn;
    while (OB_SUCC(ret)) {
      data.reset();
      commit_scn.reset();
      if (OB_FAIL(read_op.get_next(data, commit_scn))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          break;
        } else {
          LOG_WARN("failed to get next data", K(ret), K(tenant_id), K(ls_id));
        }
      } else if (OB_FAIL(check_can_recycle_(*ls, data, can_recycle))) {
        LOG_WARN("failed to check can recycle", K(ret), K(data));
      } else if (can_recycle) {
        max_recycle_scn = share::SCN::max(max_recycle_scn, commit_scn);
      } else {
        min_unrecycle_scn = share::SCN::min(min_unrecycle_scn, commit_scn);
      }
    }

    if (OB_SUCC(ret)) {
      if (max_recycle_scn < min_unrecycle_scn) {
        recycle_scn = max_recycle_scn;
      } else {
        recycle_scn = share::SCN::scn_dec(min_unrecycle_scn);
      }

      if (OB_FAIL(ls->get_reorg_info_table()->update_can_recycle_scn(recycle_scn))) {
        LOG_WARN("failed to update can recycle scn", K(ret), K(recycle_scn));
      }
    }
  }
  return ret;
}

int ObTabletReorgInfoTableService::check_can_recycle_(
    ObLS &ls,
    const ObTabletReorgInfoData &data,
    bool &can_recycle)
{
  int ret = OB_SUCCESS;
  can_recycle = false;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet reorg info table service do not init", K(ret));
  } else if (ObTabletReorgInfoDataType::is_transfer(data.key_.type_)) {
    if (OB_FAIL(check_transfer_tablet_(ls, data, can_recycle))) {
      LOG_WARN("failed to check transfer tablet", K(ret), K(data));
    }
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("tablet reorg info table tablet status do not supported", K(ret), K(data));
  }
  return ret;
}

int ObTabletReorgInfoTableService::check_transfer_tablet_(
    ObLS &ls,
    const ObTabletReorgInfoData &data,
    bool &can_recycle)
{
  int ret = OB_SUCCESS;
  can_recycle = false;
  bool is_finised = false;
  ObTransferDataValue data_value;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet reorg info table service do not init", K(ret));
  } else if (OB_FAIL(data.get_transfer_data_value(data_value))) {
    LOG_WARN("failed to get transfer data value", K(ret), K(data));
  } else if (ObTabletStatus::TRANSFER_IN == data_value.tablet_status_) {
    if (OB_FAIL(check_transfer_in_tablet_finish_(ls, data, data_value, is_finised))) {
      LOG_WARN("failed to check transfer in tablet finish", K(ret), K(ls), K(data));
    } else {
      can_recycle = is_finised;
    }
  } else if (ObTabletStatus::TRANSFER_OUT == data_value.tablet_status_) {
    if (OB_FAIL(check_transfer_out_tablet_finish_(ls, data, data_value, is_finised))) {
      LOG_WARN("failed to check transfer in tablet finish", K(ret), K(ls), K(data));
    } else {
      can_recycle = is_finised;
    }
  }
  return ret;
}

int ObTabletReorgInfoTableService::check_transfer_in_tablet_finish_(
    ObLS &ls,
    const ObTabletReorgInfoData &data,
    const ObTransferDataValue &data_value,
    bool &is_finish)
{
  int ret = OB_SUCCESS;
  ObTabletHandle tablet_handle;
  ObTablet *tablet = nullptr;
  is_finish = false;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet reorg info table service do not init", K(ret));
  } else if (OB_FAIL(ls.ha_get_tablet(data.key_.tablet_id_, tablet_handle))) {
    if (OB_TABLET_NOT_EXIST == ret) {
      is_finish = true;
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to get tablet", K(ret), K(data));
    }
  } else if (OB_ISNULL(tablet = tablet_handle.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet should not be NULL", K(ret), K(data), K(tablet_handle));
  } else {
    const ObTabletMeta &meta = tablet->get_tablet_meta();
    if (meta.transfer_info_.transfer_seq_ > data_value.transfer_seq_
        || meta.transfer_info_.transfer_start_scn_ > data.key_.reorganization_scn_) {
      is_finish = true;
    } else if (meta.transfer_info_.transfer_seq_ == data_value.transfer_seq_
        && meta.transfer_info_.transfer_start_scn_ == data.key_.reorganization_scn_
        && !meta.transfer_info_.has_transfer_table()) {
      is_finish = true;
    } else {
      is_finish = false;
    }
  }
  return ret;
}

int ObTabletReorgInfoTableService::check_transfer_out_tablet_finish_(
    ObLS &ls,
    const ObTabletReorgInfoData &data,
    const ObTransferDataValue &data_value,
    bool &is_finish)
{
  int ret = OB_SUCCESS;
  ObTabletHandle tablet_handle;
  ObTablet *tablet = nullptr;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet reorg info table service do not init", K(ret));
  } else if (OB_FAIL(ls.ha_get_tablet(data.key_.tablet_id_, tablet_handle))) {
    if (OB_TABLET_NOT_EXIST == ret) {
      is_finish = true;
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to get tablet", K(ret), K(data));
    }
  } else if (OB_ISNULL(tablet = tablet_handle.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet should not be NULL", K(ret), K(data), K(tablet_handle));
  } else {
    const ObTabletMeta &meta = tablet->get_tablet_meta();
    if (meta.transfer_info_.transfer_seq_ > data_value.transfer_seq_) {
      is_finish = true;
    } else if (meta.transfer_info_.transfer_seq_ < data_value.transfer_seq_) {
      is_finish = false;
    }
  }
  return ret;
}
