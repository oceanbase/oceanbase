/**
 * Copyright (c) 2022 OceanBase
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
#include "ob_transfer_backfill_tx.h"
#include "storage/tablet/ob_tablet_iterator.h"
#include "storage/tablet/ob_tablet.h"
#include "logservice/ob_log_service.h"
#include "share/rc/ob_tenant_base.h"
#include "ob_tablet_backfill_tx.h"
#include "share/scheduler/ob_dag_scheduler.h"
#include "ob_transfer_service.h"
#include "observer/ob_server_event_history_table_operator.h"
#include "share/scheduler/ob_dag_warning_history_mgr.h"
#include "share/ob_debug_sync_point.h"
#include "lib/utility/ob_tracepoint.h"
#include "storage/tablet/ob_tablet.h"

namespace oceanbase
{
using namespace share;
namespace storage
{

ObTransferWorkerMgr::ObTransferWorkerMgr()
  : is_inited_(false),
    tenant_id_(OB_INVALID_TENANT_ID),
    task_id_(),
    dest_ls_(NULL)
{
}

ObTransferWorkerMgr::~ObTransferWorkerMgr()
{
}

int ObTransferWorkerMgr::init(ObLS *dest_ls)
{
  int ret = OB_SUCCESS;
  bool is_exist = false;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_ISNULL(dest_ls)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("ls is nullptr", K(ret));
  } else {
    tenant_id_ = MTL_ID();
    dest_ls_ = dest_ls;
    is_inited_ = true;
  }
  return ret;
}

void ObTransferWorkerMgr::reset_task_id()
{
  task_id_.reset();
}

void ObTransferWorkerMgr::update_task_id_()
{
  reset_task_id();
  task_id_.init(GCONF.self_addr_);
}
int ObTransferWorkerMgr::get_need_backfill_tx_tablets_(ObTransferBackfillTXParam &param)
{
  int ret = OB_SUCCESS;
  int64_t start_time = common::ObTimeUtility::current_time();
  param.reset();
  ObLSTabletIterator tablet_iter(ObMDSGetTabletMode::READ_WITHOUT_CHECK);
  share::ObLSID src_ls_id;
  SCN transfer_scn;
  SCN src_max_backfill_scn;
  src_max_backfill_scn.set_min();
  ObMigrationStatus migration_status = ObMigrationStatus::OB_MIGRATION_STATUS_MAX;
  bool in_migration = false;
  ObLSRestoreStatus restore_status;

  DEBUG_SYNC(TRANSFER_GET_BACKFILL_TABLETS_BEFORE);

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("transfer work not init", K(ret));
  } else if (OB_FAIL(dest_ls_->build_tablet_iter(tablet_iter))) {
    LOG_WARN("failed to build ls tablet iter", K(ret));
  } else if (OB_FAIL(dest_ls_->get_migration_status(migration_status))) {
    LOG_WARN("failed to get migration status", K(ret), KPC(dest_ls_));
  } else if (FALSE_IT(in_migration = ObMigrationStatus::OB_MIGRATION_STATUS_NONE != migration_status)) {
  } else if (OB_FAIL(dest_ls_->get_restore_status(restore_status))) {
    LOG_WARN("failed to get restore status", K(ret), KPC_(dest_ls));
  } else if (restore_status.is_restore_to_consistent_scn()) {
    LOG_INFO("[TRANSFER_BACKFILL]ls is in RESTORE_TO_CONSISTENT_SCN, skip backfill", KPC_(dest_ls));
  } else {
    ObTabletHandle tablet_handle;
    ObTablet *tablet = nullptr;
    ObTabletCreateDeleteMdsUserData user_data;
    ObTabletHAStatus src_tablet_ha_status;
    bool last_is_committed = false;
    while (OB_SUCC(ret)) {
      tablet_handle.reset();
      user_data.reset();
      tablet = nullptr;
      bool is_ready = false;
      bool is_committed = false;
      ObTabletBackfillInfo tablet_info;
      if (OB_FAIL(tablet_iter.get_next_tablet(tablet_handle))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          break;
        } else {
          LOG_WARN("failed to get tablet", K(ret), KPC(dest_ls_));
        }
      } else if (OB_ISNULL(tablet = tablet_handle.get_obj())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("tablet should not be NULL", K(ret), KP(tablet));
      } else if (tablet->get_tablet_meta().tablet_id_.is_ls_inner_tablet()) {
        //do nothing
      } else if (OB_FAIL(tablet->ObITabletMdsInterface::get_latest_tablet_status(user_data, is_committed))) {
        if (OB_EMPTY_RESULT == ret) {
          LOG_INFO("tablet_status does not exist", K(ret), "tablet_id", tablet->get_tablet_meta().tablet_id_);
          ret = OB_SUCCESS;
        } else {
         LOG_WARN("failed to get latest tablet status", K(ret), KPC(tablet), K(user_data));
        }
      } else if (ObTabletStatus::TRANSFER_IN != user_data.tablet_status_ && !in_migration) {
        // do nothing
      } else if (!tablet->get_tablet_meta().has_transfer_table()) {
        // do nothing
      } else if (!tablet->get_tablet_meta().ha_status_.is_restore_status_full()) {
        // Restore status is FULL when the tablet is created by transfer in. It can
        // turn into one of the following status.
        // 1. FULL if not in restore;
        // 2. EMPTY if the transfer table cannot be replaced with source tablet, may be source tablet is UNDEFINED;
        // 3. MINOR_AND_MAJOR_META with no transfer table if in restore and source tablet only has minor tables.
        // Here, the restore status must be EMPTY. The transfer table should be replaced by physical restore.
        LOG_INFO("[TRANSFER_BACKFILL]skip tablet which restore status is not full.",
                "tablet_id", tablet->get_tablet_meta().tablet_id_,
                "ha_status", tablet->get_tablet_meta().ha_status_);
      } else if (!tablet->get_tablet_meta().transfer_info_.ls_id_.is_valid()
          || !tablet->get_tablet_meta().transfer_info_.transfer_start_scn_.is_valid()) {
        ret = OB_TRANSFER_SYS_ERROR;
        LOG_ERROR("transfer_ls_id_ or transfer_scn_ are invalid", K(ret), "transfer_info", tablet->get_tablet_meta().transfer_info_,
            K(in_migration), K(migration_status));
      } else if (OB_FAIL(check_source_tablet_ready_(tablet->get_tablet_meta().transfer_info_.ls_id_,
                                                    tablet->get_tablet_meta().tablet_id_,
                                                    tablet->get_tablet_meta().transfer_info_,
                                                    is_ready,
                                                    src_tablet_ha_status))) {
        LOG_WARN("fail to check source tablet ready", K(ret), "transfer_info", tablet->get_tablet_meta().transfer_info_,
            "tablet_id", tablet->get_tablet_meta().tablet_id_);
      } else if (!is_ready) {
        LOG_INFO("[TRANSFER_BACKFILL]skip tablet which is not ready.",
          "tablet_id", tablet->get_tablet_meta().tablet_id_);
      } else if (src_tablet_ha_status.is_restore_status_undefined()) {
        // If source tablet is UNDEFINED, directly set dest tablet EMPTY, but keep
        // transfer table. Then the restore handler will schedule it to restore minor
        // without creating remote logical table.
        if (OB_FAIL(dest_ls_->update_tablet_restore_status(tablet->get_tablet_meta().tablet_id_,
                                                           ObTabletRestoreStatus::EMPTY))) {
          LOG_WARN("fail to set empty", K(ret), KPC(tablet));
        } else {
          dest_ls_->get_ls_restore_handler()->try_record_one_tablet_to_restore(tablet->get_tablet_meta().tablet_id_);
          LOG_INFO("[TRANSFER_BACKFILL]direct set tablet EMPTY if source tablet is UNDEFINED.",
                   "tablet_meta", tablet->get_tablet_meta());
        }
      } else {
#ifdef ERRSIM
        SERVER_EVENT_SYNC_ADD("TRANSFER", "get_need_backfil_tx_tablets",
                              "src_ls_id", tablet->get_tablet_meta().transfer_info_.ls_id_.id(),
                              "dest_ls_id", dest_ls_->get_ls_id().id(),
                              "tablet_id", tablet->get_tablet_meta().tablet_id_,
                              "has_transfer_table", tablet->get_tablet_meta().has_transfer_table());
#endif
        if (OB_FAIL(tablet_info.init(tablet->get_tablet_meta().tablet_id_, is_committed))) {
          LOG_WARN("failed to init ObTabletBackfillInfo", K(ret), "backfilled tablet id", tablet->get_tablet_meta().tablet_id_, K(is_committed));
        } else if (OB_FAIL(param.tablet_infos_.push_back(tablet_info))) {
          LOG_WARN("failed to push tablet id into array", K(ret), KPC(tablet));
        } else if (src_ls_id.is_valid() && transfer_scn.is_valid()) {
          // Only one transfer task is allowed to execute at the same time, verify that the transferred tablets parameter are the same.
          if (in_migration) {
            //migration will has multi transfer task tablets.
            if (src_ls_id != tablet->get_tablet_meta().transfer_info_.ls_id_
                || transfer_scn != tablet->get_tablet_meta().transfer_info_.transfer_start_scn_) {
              param.tablet_infos_.pop_back();
            }
          } else if (src_ls_id == tablet->get_tablet_meta().transfer_info_.ls_id_
              && transfer_scn == tablet->get_tablet_meta().transfer_info_.transfer_start_scn_) {
            last_is_committed = is_committed;
          } else if (transfer_scn != tablet->get_tablet_meta().transfer_info_.transfer_start_scn_ && last_is_committed && is_committed) {
            ret = OB_TRANSFER_SYS_ERROR;
            LOG_ERROR("transfer task is not unique", K(ret), K(src_ls_id), K(transfer_scn), K(last_is_committed), K(is_committed), KPC(tablet));
          } else {
            ret = OB_EAGAIN;
            LOG_WARN("transfer start trans is likely to rollback", K(ret), K(src_ls_id), K(transfer_scn),
                "tablet_id", tablet->get_tablet_meta().tablet_id_,
                "tablet_transfer_info", tablet->get_tablet_meta().transfer_info_);
          }
        } else {
          src_ls_id = tablet->get_tablet_meta().transfer_info_.ls_id_;
          transfer_scn = tablet->get_tablet_meta().transfer_info_.transfer_start_scn_;
          last_is_committed = is_committed;
        }
      }
    }

    if (OB_SUCC(ret)) {
      param.src_ls_id_ = src_ls_id;
      param.backfill_scn_ = std::max(src_max_backfill_scn, transfer_scn);
      param.tenant_id_ = tenant_id_;
      param.task_id_ = task_id_;
      param.dest_ls_id_ = dest_ls_->get_ls_id();
      const int64_t cost_time = common::ObTimeUtility::current_time() - start_time;
      LOG_INFO("statistics the time that get needed to backfill tablets", "ls id", param.dest_ls_id_, K(cost_time));
    }
  }
  return ret;
}

int ObTransferWorkerMgr::check_source_tablet_ready_(
    const share::ObLSID &ls_id,
    const ObTabletID &tablet_id,
    const ObTabletTransferInfo &transfer_info,
    bool &is_ready,
    ObTabletHAStatus &ha_status/* source tablet ha status */) const
{
  int ret = OB_SUCCESS;
  ObLSService *ls_service = nullptr;
  ObLSHandle ls_handle;
  ObLS *ls = nullptr;
  ObTabletHandle tablet_handle;
  ObTablet *tablet = nullptr;
  SCN max_decided_scn;
  ObTabletCreateDeleteMdsUserData user_data;
  ObMigrationStatus migration_status = ObMigrationStatus::OB_MIGRATION_STATUS_MAX;
  bool need_check_tablet = false;
  is_ready = false;
  if (!ls_id.is_valid() || !tablet_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(ls_id), K(tablet_id));
  } else if (OB_ISNULL(ls_service = MTL(ObLSService*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get ObLSService from MTL", K(ret), KP(ls_service));
  } else if (OB_FAIL(ls_service->get_ls(ls_id, ls_handle, ObLSGetMod::HA_MOD))) {
    if (OB_LS_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      LOG_INFO("[TRANSFER_BACKFILL]source ls not exist", K(ls_id), K(tablet_id));
    } else {
      LOG_WARN("failed to get ls", K(ret), K(ls_id), K(tablet_id));
    }
  } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls should not be NULL", K(ret), KP(ls), K(ls_id));
  } else if (OB_FAIL(ls->get_migration_status(migration_status))) {
    LOG_WARN("failed to get migration status", K(ret), KPC(ls));
  } else if (ObMigrationStatus::OB_MIGRATION_STATUS_NONE != migration_status
      && ObMigrationStatus::OB_MIGRATION_STATUS_GC != migration_status) {
    LOG_INFO("[TRANSFER_BACKFILL]source ls is not in migration none", K(ls_id), K(migration_status));
  } else if (OB_FAIL(ls->get_max_decided_scn(max_decided_scn))) {
    if (OB_STATE_NOT_MATCH == ret && ObMigrationStatus::OB_MIGRATION_STATUS_GC == migration_status) {
      LOG_INFO("the migration status of the log stream is OB_MIGRATION_STATUS_GC", K(ret), KPC(ls));
      need_check_tablet = true;
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to get source ls max decided scn", K(ret), KPC(ls));
    }
  } else if (max_decided_scn < transfer_info.transfer_start_scn_) {
    LOG_INFO("[TRANSFER_BACKFILL]src ls max decided scn is smaller than transfer start scn, need wait",
      K(ls_id), K(tablet_id), K(max_decided_scn), K(transfer_info));
  } else {
    need_check_tablet = true;
  }

  if (OB_FAIL(ret) || !need_check_tablet) {
  } else if (OB_FAIL(ls->ha_get_tablet(tablet_id, tablet_handle))) {
    if (OB_TABLET_NOT_EXIST == ret) {
      LOG_INFO("[TRANSFER_BACKFILL]source tablet not exist", K(ls_id), K(tablet_id));
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to get tablet", K(ret), K(ls_id), K(tablet_id));
    }
  } else if (OB_ISNULL(tablet = tablet_handle.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet should not be NULL", K(ret), K(ls_id), K(tablet_id));
  } else if (tablet->is_empty_shell()) {
    LOG_INFO("[TRANSFER_BACKFILL]source tablet is empty shell", K(ls_id), K(tablet_id));
  } else if (OB_FAIL(ObTXTransferUtils::get_tablet_status(false/*get_commit*/, tablet, user_data))) {
    LOG_WARN("failed to get tablet status", K(ret), K(ls_id), KPC(tablet));
  } else if (ObTabletStatus::TRANSFER_OUT != user_data.tablet_status_
             && ObTabletStatus::TRANSFER_OUT_DELETED != user_data.tablet_status_ ) {
    // wait source tablet transfer start commit
    LOG_INFO("[TRANSFER_BACKFILL]source tablet is not ready", K(tablet_id), "tablet_status", user_data.tablet_status_, K(ls_id));
  } else if (tablet->get_tablet_meta().transfer_info_.transfer_seq_ != transfer_info.transfer_seq_ - 1) {
    LOG_INFO("[TRANSFER_BACKFILL]source tablet transfer seq is unexpected, need rebuild", K(ls_id), K(tablet_id),
             "src transfer info", tablet->get_tablet_meta().transfer_info_,
             "dest transfer info", transfer_info);
  } else {
    ha_status = tablet->get_tablet_meta().ha_status_;
    if (ha_status.is_restore_status_minor_and_major_meta()
        || ha_status.is_restore_status_full()
        || ha_status.is_restore_status_undefined()) {
      is_ready = true;
      LOG_INFO("[TRANSFER_BACKFILL]source tablet is ready", K(ls_id), K(tablet_id), K(ha_status));
    } else {
      LOG_INFO("[TRANSFER_BACKFILL]source tablet is not ready", K(ls_id), K(tablet_id), K(ha_status));
#ifdef ERRSIM
      SERVER_EVENT_ADD("transfer", "backfill_tx_with_restoring_tablet", "tablet_id", tablet->get_tablet_meta().tablet_id_);
#endif
    }
  }
  return ret;
}

int ObTransferWorkerMgr::process()
{
  int ret = OB_SUCCESS;
  bool is_exist = false;
  ObTransferBackfillTXParam param;

#ifdef ERRSIM
    if (OB_SUCC(ret)) {
      ret = OB_E(EventTable::EN_CHECK_TRANSFER_TASK_EXSIT) OB_SUCCESS;
      if (OB_FAIL(ret)) {
        STORAGE_LOG(ERROR, "fake EN_CHECK_TRANSFER_TASK_EXSIT", K(ret));
        is_exist = true;
        ret = OB_SUCCESS;
      }
    }
#endif

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("transfer work not init", K(ret));
  } else if (task_id_.is_valid() && OB_FAIL(check_task_exist_(task_id_, is_exist))) {
    LOG_WARN("failed to check task exist", K(ret), "ls_id", dest_ls_->get_ls_id(), K(*this));
  } else if (is_exist) {
    // only one transfer backfill tx task is allowed to execute at a time
    LOG_INFO("[TRANSFER_BACKFILL]transfer backfill tx task exist", "ls_id", dest_ls_->get_ls_id(), K(*this));
  } else {
    update_task_id_();
    if (OB_FAIL(get_need_backfill_tx_tablets_(param))) {
      LOG_WARN("failed to get need backfill tx tablets", K(ret), "ls_id", dest_ls_->get_ls_id(), K(*this));
    } else if (param.tablet_infos_.empty()) {
      // There are no tablets that require backfill transactions
    } else if (OB_FAIL(do_transfer_backfill_tx_(param))) {
      LOG_WARN("failed to do transfer backfill tx", K(ret), K(param));
    }
  }

  return ret;
}

int ObTransferWorkerMgr::check_task_exist_(
    const share::ObTaskId &task_id,
    bool &is_exist)
{
  int ret = OB_SUCCESS;
  is_exist = false;
  share::ObTenantDagScheduler *scheduler = nullptr;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("transfer worker do not init", K(ret));
  } else if (OB_ISNULL(scheduler = MTL(ObTenantDagScheduler*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get ObTenantDagScheduler from MTL", K(ret));
  } else if (OB_FAIL(scheduler->check_dag_net_exist(task_id, is_exist))) {
    LOG_WARN("failed to check dag net exist", K(ret), K(task_id));
  }
  return ret;
}

int ObTransferWorkerMgr::cancel_dag_net()
{
  int ret = OB_SUCCESS;
  ObTenantDagScheduler *scheduler = nullptr;
  bool is_exist = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("transfer worker do not init", K(ret));
  } else if (task_id_.is_invalid()) {
    // do nothing
  } else if (OB_FAIL(check_task_exist_(task_id_, is_exist))) {
    LOG_WARN("fail to check task exist", K(ret), K_(task_id));
  } else if (is_exist) {
    if (OB_ISNULL(scheduler = MTL(ObTenantDagScheduler*))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("failed to get ObTenantDagScheduler from MTL", K(ret), KPC(this));
    } else if (OB_FAIL(scheduler->cancel_dag_net(task_id_))) {
      LOG_WARN("failed to cancel dag net", K(ret), K(this));
    }
    if (OB_FAIL(ret)) {
    } else {
      int64_t start_ts = ObTimeUtil::current_time();
      do {
        if (OB_FAIL(check_task_exist_(task_id_, is_exist))) {
          LOG_WARN("fail to check task exist", K(ret), K_(task_id));
        } else if (is_exist && REACH_TIME_INTERVAL(60 * 1000 * 1000)) {
          ret = OB_EAGAIN;
          LOG_WARN("cancel dag task cost too much time", K(ret), K_(task_id),
              "cost_time", ObTimeUtil::current_time() - start_ts);
        }
      } while (is_exist && OB_SUCC(ret));
    }
  }
  return ret;
}

ERRSIM_POINT_DEF(EN_ERRSIM_ALLOW_TRANSFER_BACKFILL_TX);

int ObTransferWorkerMgr::do_transfer_backfill_tx_(const ObTransferBackfillTXParam &param)
{
  int ret = OB_SUCCESS;
  set_errsim_backfill_point_();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("transfer worker do not init", K(ret));
  } else if (!param.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("transfer backfill tx parameter is invalid", K(ret), K(param));
  } else {
    DEBUG_SYNC(TRANSFER_BACKFILL_TX_BEFORE);
#ifdef ERRSIM
    common::ObAddr addr;
    ret = EN_ERRSIM_ALLOW_TRANSFER_BACKFILL_TX ? : OB_SUCCESS;
    char errsim_server_addr[OB_MAX_SERVER_ADDR_SIZE] = "";
    if (OB_FAIL(GCONF.errsim_transfer_backfill_server_addr.copy(errsim_server_addr, sizeof(errsim_server_addr)))) {
      LOG_WARN("failed to copy errrsim transfer backfill server addr", K(ret));
    } else if (0 == strlen(errsim_server_addr)) {
      // do nothing
    } else if (OB_FAIL(addr.parse_from_string(errsim_server_addr))) {
      LOG_WARN("failed to parse from string", K(ret), K(errsim_server_addr));
    } else if (GCTX.self_addr() == addr) {
      ret = OB_EAGAIN;
      LOG_WARN("errsim forbid execute transfer backfill", K(ret), K(addr));
    }

    ObErrsimBackfillPointType point_type(ObErrsimBackfillPointType::TYPE::ERRSIM_START_BACKFILL_BEFORE);
    if (OB_SUCC(ret) && errsim_point_info_.is_errsim_point(point_type)) {
      ret = OB_EAGAIN;
      LOG_WARN("[ERRSIM TRANSFER] errsim start transfer backfill error", K(ret), K(param));
    }
#endif
    share::ObTenantDagScheduler *scheduler = nullptr;
    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(scheduler = MTL(ObTenantDagScheduler*))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to get ObTenantDagScheduler from MTL", K(ret), KP(scheduler));
    } else if (OB_FAIL(scheduler->create_and_add_dag_net<ObTransferBackfillTXDagNet>(&param))) {
      LOG_WARN("failed to create and add transfer backfill tx dag net", K(ret), K(param));
    } else {
      LOG_INFO("[TRANSFER_BACKFILL]success to create transfer backfill tx dag net", K(ret), K(param));
    }
  }
  return ret;
}

void ObTransferWorkerMgr::set_errsim_backfill_point_()
{
#ifdef ERRSIM
  int ret = OB_SUCCESS;
  int64_t point_time = 0;
  int64_t current_time = common::ObTimeUtility::current_time();
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(MTL_ID()));
  if (tenant_config.is_valid()) {
    point_time = tenant_config->errsim_transfer_backfill_error_time;
  }
  if (0 == point_time) {
    errsim_point_info_.reset();
  } else if (errsim_point_info_.is_valid()
      && (current_time - errsim_point_info_.get_point_start_time()) < point_time) {
    FLOG_INFO("wait clear errsim point", K(ret), K(errsim_point_info_), K(point_time), K(current_time));
    // do nothing
  } else {
    errsim_point_info_.reset();
    const ObErrsimBackfillPointType::TYPE point_type =
        (ObErrsimBackfillPointType::TYPE)ObRandom::rand(ObErrsimBackfillPointType::TYPE::ERRSIM_POINT_NONE, ObErrsimBackfillPointType::TYPE::ERRSIM_MODULE_MAX);
    ObErrsimBackfillPointType type(point_type);
    if (OB_FAIL(errsim_point_info_.set_point_type(type))) {
      LOG_WARN("failed to set point type", K(ret), K(type));
    } else if (OB_FAIL(errsim_point_info_.set_point_start_time(current_time))) {
      LOG_WARN("failed to set point start time", K(ret), K(current_time));
    } else {
      FLOG_INFO("succ to set point type", K(ret), K(errsim_point_info_));
    }
  }
#endif
}

/******************ObTransferBackfillTXCtx*********************/
ObTransferBackfillTXCtx::ObTransferBackfillTXCtx()
  : ObIHADagNetCtx(),
    tenant_id_(OB_INVALID_TENANT_ID),
#ifdef ERRSIM
    errsim_point_info_(),
#endif
    task_id_(),
    src_ls_id_(),
    dest_ls_id_(),
    backfill_scn_(),
    tablet_infos_()
{
}

ObTransferBackfillTXCtx::~ObTransferBackfillTXCtx()
{
}

bool ObTransferBackfillTXCtx::is_valid() const
{
  return OB_INVALID_TENANT_ID != tenant_id_
      && !task_id_.is_invalid()
      && src_ls_id_.is_valid()
      && dest_ls_id_.is_valid()
      && backfill_scn_.is_valid()
      && !tablet_infos_.empty();
}

void ObTransferBackfillTXCtx::reset()
{
  tenant_id_ = OB_INVALID_TENANT_ID;
  task_id_.reset();
  src_ls_id_.reset();
  dest_ls_id_.reset();
  backfill_scn_.reset();
  tablet_infos_.reset();
  ObIHADagNetCtx::reset();
}

int ObTransferBackfillTXCtx::fill_comment(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;

  if (!is_valid()) {
    ret = OB_NOT_INIT;
    LOG_WARN("transfer backfill TX ctx do not init", K(ret));
  } else if (NULL == buf || buf_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), KP(buf), K(buf_len));
  } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, "transfer backfill TX :tenant_id = %s, task_id = %s, "
      "src_ls_id = %s, dest_ls_id = %s, transfer_scn = %s", to_cstring(tenant_id_),
      to_cstring(task_id_), to_cstring(src_ls_id_), to_cstring(dest_ls_id_), to_cstring(backfill_scn_)))) {
    LOG_WARN("failed to set comment", K(ret), K(buf), K(pos), K(buf_len));
  }
  return ret;
}

void ObTransferBackfillTXCtx::reuse()
{
  ObIHADagNetCtx::reuse();
  backfill_scn_.reset();
  tablet_infos_.reset();
}

/******************ObTransferBackfillTXParam*********************/
ObTransferBackfillTXParam::ObTransferBackfillTXParam()
  : tenant_id_(OB_INVALID_TENANT_ID),
#ifdef ERRSIM
    errsim_point_info_(),
#endif
    task_id_(),
    src_ls_id_(),
    dest_ls_id_(),
    backfill_scn_(),
    tablet_infos_()
{
}

bool ObTransferBackfillTXParam::is_valid() const
{
  return OB_INVALID_TENANT_ID != tenant_id_
      && src_ls_id_.is_valid()
      && dest_ls_id_.is_valid()
      && !task_id_.is_invalid()
      && backfill_scn_.is_valid()
      && !tablet_infos_.empty();
}

void ObTransferBackfillTXParam::reset()
{
  tenant_id_ = OB_INVALID_TENANT_ID;
  task_id_.reset();
  src_ls_id_.reset();
  dest_ls_id_.reset();
  backfill_scn_.reset();
  tablet_infos_.reset();
#ifdef ERRSIM
  errsim_point_info_.reset();
#endif
}

/******************ObTransferBackfillTXDagNet*********************/
ObTransferBackfillTXDagNet::ObTransferBackfillTXDagNet()
    : ObIDagNet(ObDagNetType::DAG_NET_TRANSFER_BACKFILL_TX),
      is_inited_(false),
      ctx_()
{
}

ObTransferBackfillTXDagNet::~ObTransferBackfillTXDagNet()
{
}

int ObTransferBackfillTXDagNet::init_by_param(const ObIDagInitParam *param)
{
  int ret = OB_SUCCESS;
  const ObTransferBackfillTXParam* init_param = static_cast<const ObTransferBackfillTXParam*>(param);
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("transfer backfill tx dag net is init twice", K(ret));
  } else if (OB_ISNULL(param) || !param->is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("param is null or invalid", K(ret), KPC(init_param));
  } else if (OB_FAIL(this->set_dag_id(init_param->task_id_))) {
    LOG_WARN("failed to set dag id", K(ret), KPC(init_param));
  } else if (OB_FAIL(ctx_.tablet_infos_.assign(init_param->tablet_infos_))) {
    LOG_WARN("failed to set transfer tablet list", K(ret), KPC(init_param));
  } else {
    ctx_.tenant_id_ = init_param->tenant_id_;
    ctx_.task_id_ = init_param->task_id_;
    ctx_.src_ls_id_ = init_param->src_ls_id_;
    ctx_.dest_ls_id_ = init_param->dest_ls_id_;
    ctx_.backfill_scn_ = init_param->backfill_scn_;
#ifdef ERRSIM
    ctx_.errsim_point_info_ = init_param->errsim_point_info_;
#endif
    is_inited_ = true;
  }
  return ret;
}

bool ObTransferBackfillTXDagNet::is_valid() const
{
  return ctx_.is_valid();
}

int ObTransferBackfillTXDagNet::start_running()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("transfer backfill tx dag net do not init", K(ret));
  } else if (OB_FAIL(start_running_for_backfill_())) {
    LOG_WARN("failed to start running for transfer backfill tx", K(ret));
  }

  return ret;
}

int ObTransferBackfillTXDagNet::start_running_for_backfill_()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObStartTransferBackfillTXDag *backfill_tx_dag = nullptr;
  ObTransferReplaceTableDag *replace_logical_dag = nullptr;
  share::ObTenantDagScheduler *scheduler = nullptr;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("transfer backfill tx dag net do not init", K(ret));
  } else if (OB_ISNULL(scheduler = MTL(ObTenantDagScheduler*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get ObTenantDagScheduler from MTL", K(ret));
  } else if (OB_FAIL(scheduler->alloc_dag(backfill_tx_dag))) {
    LOG_WARN("failed to alloc transfer backfill tx dag ", K(ret));
  } else if (OB_FAIL(scheduler->alloc_dag(replace_logical_dag))) {
    LOG_WARN("failed to alloc replace logical dag ", K(ret));
  } else if (OB_FAIL(backfill_tx_dag->init(this))) {
    LOG_WARN("failed to init transfer backfill tx dag", K(ret));
  } else if (OB_FAIL(add_dag_into_dag_net(*backfill_tx_dag))) {
    LOG_WARN("failed to add transfer backfill tx dag into dag net", K(ret));
  } else if (OB_FAIL(backfill_tx_dag->create_first_task())) {
    LOG_WARN("failed to create transfer backfill tx first task", K(ret));
  } else if (OB_FAIL(replace_logical_dag->init(this))) {
    LOG_WARN("failed to init replace logical dag", K(ret));
  } else if (OB_FAIL(backfill_tx_dag->add_child(*replace_logical_dag))) {
    LOG_WARN("failed to add child into transfer backfill tx", K(ret));
  } else if (OB_FAIL(replace_logical_dag->create_first_task())) {
    LOG_WARN("failed to create replace logical first task", K(ret));
  } else if (OB_FAIL(scheduler->add_dag(replace_logical_dag))) {
    LOG_WARN("failed to add transfer backfill tx dag into scheduer", K(ret), K(*replace_logical_dag));
    if (OB_SIZE_OVERFLOW != ret && OB_EAGAIN != ret) {
      LOG_WARN("Fail to add task", K(ret));
      ret = OB_EAGAIN;
    }
  } else if (OB_FAIL(scheduler->add_dag(backfill_tx_dag))) {
    LOG_WARN("failed to add backfill dag", K(ret), K(*backfill_tx_dag));
    if (OB_SIZE_OVERFLOW != ret && OB_EAGAIN != ret) {
      LOG_WARN("Fail to add task", K(ret));
      ret = OB_EAGAIN;
    }
    if (OB_NOT_NULL(replace_logical_dag)) {
      if (OB_SUCCESS != (tmp_ret = scheduler->cancel_dag(replace_logical_dag, backfill_tx_dag))) {
        LOG_WARN("failed to cancel replace logical dag", K(tmp_ret), KPC(backfill_tx_dag));
      } else {
        replace_logical_dag = nullptr;
      }
    }
  } else {
    FLOG_INFO("[TRANSFER_BACKFILL]succeed to schedule transfer backfill tx dag", K(*backfill_tx_dag), K(*replace_logical_dag));
    backfill_tx_dag = nullptr;
    replace_logical_dag = nullptr;
  }

  if (OB_NOT_NULL(replace_logical_dag) && OB_NOT_NULL(scheduler)) {
    scheduler->free_dag(*replace_logical_dag, backfill_tx_dag);
    replace_logical_dag = nullptr;
  }

  if (OB_NOT_NULL(backfill_tx_dag) && OB_NOT_NULL(scheduler)) {
    if (OB_SUCCESS != (tmp_ret = erase_dag_from_dag_net(*backfill_tx_dag))) {
      LOG_WARN("failed to erase dag from dag net", K(tmp_ret), KPC(backfill_tx_dag));
    }
    scheduler->free_dag(*backfill_tx_dag);
    backfill_tx_dag = nullptr;
  }
  return ret;
}

bool ObTransferBackfillTXDagNet::operator == (const ObIDagNet &other) const
{
  bool is_same = true;
  if (this == &other) {
    // same
  } else if (this->get_type() != other.get_type()) {
    is_same = false;
  } else {
    const ObTransferBackfillTXDagNet &other_dag_net = static_cast<const ObTransferBackfillTXDagNet &>(other);
    if (!is_valid() || !other_dag_net.is_valid()) {
      is_same = false;
      LOG_ERROR_RET(OB_ERR_SYS, "transfer backfill tx dag net is invalid", K(*this), K(other));
    } else if (ctx_.tenant_id_ != other_dag_net.ctx_.tenant_id_
        || ctx_.dest_ls_id_ != other_dag_net.ctx_.dest_ls_id_) {
      is_same = false;
    }
  }
  return is_same;
}

int64_t ObTransferBackfillTXDagNet::hash() const
{
  int64_t hash_value = 0;

  const int64_t type = ObDagNetType::DAG_NET_TRANSFER_BACKFILL_TX;
  hash_value = common::murmurhash(&type, sizeof(type), hash_value);
  hash_value = common::murmurhash(&ctx_.tenant_id_, sizeof(ctx_.tenant_id_), hash_value);
  hash_value += ctx_.dest_ls_id_.hash();

  return hash_value;
}

int ObTransferBackfillTXDagNet::fill_comment(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  const int64_t MAX_TRACE_ID_LENGTH = 64;
  char task_id_str[MAX_TRACE_ID_LENGTH] = { 0 };
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("transfer backfill tx dag net do not init ", K(ret));
  } else if (OB_FAIL(ctx_.task_id_.to_string(task_id_str, MAX_TRACE_ID_LENGTH))) {
    LOG_WARN("failed to trace task id to string", K(ret), K(ctx_));
  } else if (OB_FAIL(databuff_printf(buf, buf_len,
      "ObTransferBackfillTXDagNet: tenant_id=%s, src_ls_id=%s, dest_ls_id=%s, trace_id=%s, start_scn=%s",
      to_cstring(ctx_.tenant_id_), to_cstring(ctx_.src_ls_id_), to_cstring(ctx_.dest_ls_id_),
      task_id_str, to_cstring(ctx_.backfill_scn_)))) {
    LOG_WARN("failed to fill comment", K(ret), K(ctx_));
  }
  return ret;
}

int ObTransferBackfillTXDagNet::fill_dag_net_key(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("transfer backfill tx dag net do not init", K(ret));
  } else if (OB_FAIL(databuff_printf(buf, buf_len,
      "ObTransferBackfillTXDagNet: tenant_id=%s, src_ls_id = %s, dest_ls_id = %s, task_id=%s, start_scn=%s",
      to_cstring(ctx_.tenant_id_), to_cstring(ctx_.src_ls_id_), to_cstring(ctx_.dest_ls_id_),
      to_cstring(ctx_.task_id_),to_cstring(ctx_.backfill_scn_)))) {
    LOG_WARN("failed to fill comment", K(ret), K(ctx_));
  }
  return ret;
}

int ObTransferBackfillTXDagNet::clear_dag_net_ctx()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObTransferService *transfer_service = nullptr;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("transfer backfill tx dag net do not init", K(ret));
  } else if (OB_ISNULL(transfer_service = (MTL(ObTransferService *)))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("transfer service should not be NULL", K(ret), KP(transfer_service));
  } else {
    transfer_service->wakeup();
  }
  return ret;
}

int ObTransferBackfillTXDagNet::deal_with_cancel()
{
  int ret = OB_SUCCESS;
  const int32_t result = OB_CANCELED;
  const bool need_retry = false;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("transfer backfill tx dag net do not init", K(ret));
  } else if (OB_FAIL(ctx_.set_result(result, need_retry))) {
    LOG_WARN("failed to set result", K(ret), KPC(this));
  }
  return ret;
}

/******************ObBaseTransferBackfillTXDag*********************/
ObBaseTransferBackfillTXDag::ObBaseTransferBackfillTXDag(const share::ObDagType::ObDagTypeEnum &dag_type)
  : ObStorageHADag(dag_type)
{
}

ObBaseTransferBackfillTXDag::~ObBaseTransferBackfillTXDag()
{
}

int ObBaseTransferBackfillTXDag::prepare_ctx(share::ObIDagNet *dag_net)
{
  int ret = OB_SUCCESS;
  ObTransferBackfillTXDagNet *backfill_dag_net = nullptr;
  ObTransferBackfillTXCtx *self_ctx = nullptr;

  if (OB_ISNULL(dag_net)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag net should not be NULL", K(ret), KP(dag_net));
  } else if (ObDagNetType::DAG_NET_TRANSFER_BACKFILL_TX != dag_net->get_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag net type is unexpected", K(ret), KPC(dag_net));
  } else if (FALSE_IT(backfill_dag_net = static_cast<ObTransferBackfillTXDagNet*>(dag_net))) {
  } else if (FALSE_IT(self_ctx = backfill_dag_net->get_ctx())) {
  } else if (OB_ISNULL(self_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("transfer backfill tx dag net ctx should not be NULL", K(ret), KP(self_ctx));
  } else {
    ha_dag_net_ctx_ = self_ctx;
  }
  return ret;
}

bool ObBaseTransferBackfillTXDag::operator == (const ObIDag &other) const
{
  bool is_same = true;
  if (this == &other) {
    // same
  } else if (get_type() != other.get_type()) {
    is_same = false;
  } else {
    const ObStorageHADag &ha_dag = static_cast<const ObStorageHADag&>(other);
    if (OB_ISNULL(ha_dag_net_ctx_) || OB_ISNULL(ha_dag.get_ha_dag_net_ctx())) {
      is_same = false;
      LOG_ERROR_RET(OB_INVALID_ARGUMENT, "transfer backfill tx ctx should not be NULL", KP(ha_dag_net_ctx_), KP(ha_dag.get_ha_dag_net_ctx()));
    } else if (ha_dag_net_ctx_->get_dag_net_ctx_type() != ha_dag.get_ha_dag_net_ctx()->get_dag_net_ctx_type()) {
      is_same = false;
    } else {
      ObTransferBackfillTXCtx *self_ctx = static_cast<ObTransferBackfillTXCtx *>(ha_dag_net_ctx_);
      ObTransferBackfillTXCtx *other_ctx = static_cast<ObTransferBackfillTXCtx *>(ha_dag.get_ha_dag_net_ctx());
      if (self_ctx->tenant_id_ != other_ctx->tenant_id_
          || self_ctx->dest_ls_id_ != other_ctx->dest_ls_id_) {
        is_same = false;
      }
    }
  }
  return is_same;
}

int64_t ObBaseTransferBackfillTXDag::hash() const
{
  int ret = OB_SUCCESS;
  int64_t hash_value = 0;
  if (OB_ISNULL(ha_dag_net_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("transfer backfill tx ctx should not be NULL", KP(ha_dag_net_ctx_));
  } else if (ObIHADagNetCtx::TRANSFER_BACKFILL_TX != ha_dag_net_ctx_->get_dag_net_ctx_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("ha dag net ctx type is unexpected", K(ret), KPC(ha_dag_net_ctx_));
  } else {
    ObTransferBackfillTXCtx *self_ctx = static_cast<ObTransferBackfillTXCtx *>(ha_dag_net_ctx_);
    ObDagType::ObDagTypeEnum dag_type = get_type();
    hash_value = common::murmurhash(&dag_type, sizeof(dag_type), hash_value);
    hash_value = common::murmurhash(&self_ctx->tenant_id_, sizeof(self_ctx->tenant_id_), hash_value);
    hash_value += self_ctx->dest_ls_id_.hash();
  }
  return hash_value;
}

/*****************************************************************/
ObStartTransferBackfillTXDag::ObStartTransferBackfillTXDag()
  : ObBaseTransferBackfillTXDag(ObDagType::DAG_TYPE_TRANSFER_BACKFILL_TX),
    is_inited_(false)
{
}

ObStartTransferBackfillTXDag::~ObStartTransferBackfillTXDag()
{
}

int ObStartTransferBackfillTXDag::fill_dag_key(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  ObTransferBackfillTXCtx *self_ctx = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("start backfill tx dag do not init", K(ret));
  } else if (ObIHADagNetCtx::TRANSFER_BACKFILL_TX != ha_dag_net_ctx_->get_dag_net_ctx_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ha dag net ctx type is unexpected", K(ret), KPC(ha_dag_net_ctx_));
  } else if (FALSE_IT(self_ctx = static_cast<ObTransferBackfillTXCtx *>(ha_dag_net_ctx_))) {
  } else if (OB_FAIL(databuff_printf(buf, buf_len,
         "ObStartTransferBackfillTXDag: tenant_id=%s, ls_id=%s, task_id=%s, start_scn=%s",
         to_cstring(self_ctx->tenant_id_), to_cstring(self_ctx->src_ls_id_),
         to_cstring(self_ctx->task_id_),to_cstring(self_ctx->backfill_scn_)))) {
    LOG_WARN("failed to fill comment", K(ret), K(*self_ctx));
  }
  return ret;
}

int ObStartTransferBackfillTXDag::init(ObIDagNet *dag_net)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("start transfer backfill tx dag init twice", K(ret));
  } else if (OB_ISNULL(dag_net)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("dag_net is NULL", K(ret));
  } else if (OB_FAIL(prepare_ctx(dag_net))) {
    LOG_WARN("failed to prepare ctx", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObStartTransferBackfillTXDag::create_first_task()
{
  int ret = OB_SUCCESS;
  ObStartTransferBackfillTXTask *task = NULL;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("start transfer backfill tx dag do not init", K(ret));
  } else if (OB_FAIL(alloc_task(task))) {
    LOG_WARN("Fail to alloc task", K(ret));
  } else if (OB_FAIL(task->init())) {
    LOG_WARN("failed to init start backfill tx task", K(ret), KPC(ha_dag_net_ctx_));
  } else if (OB_FAIL(add_task(*task))) {
    LOG_WARN("fail to add ObStartTransferBackfillTXTask", K(ret));
  } else {
    LOG_INFO("[TRANSFER_BACKFILL]success to create first ObStartTransferBackfillTXTask", K(ret), KPC(this));
  }
  return ret;
}

int ObStartTransferBackfillTXDag::fill_info_param(compaction::ObIBasicInfoParam *&out_param, ObIAllocator &allocator) const
{
  int ret = OB_SUCCESS;
  ObTransferBackfillTXCtx *ctx = nullptr;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("start transfer backfill tx dag do not init", K(ret));
  } else if (FALSE_IT(ctx = static_cast<ObTransferBackfillTXCtx *>(ha_dag_net_ctx_))) {
  } else if (OB_FAIL(ADD_DAG_WARN_INFO_PARAM(out_param, allocator, get_type(),
                                static_cast<int64_t>(ctx->tenant_id_),
                                ctx->src_ls_id_.id(),
                                static_cast<int64_t>(ctx->backfill_scn_.get_val_for_inner_table_field()),
                                "dag_net_task_id", to_cstring(ctx->task_id_)))){
    LOG_WARN("failed to fill info param", K(ret));
  }
  return ret;
}

/******************ObStartTransferBackfillTXTask*********************/
ObStartTransferBackfillTXTask::ObStartTransferBackfillTXTask()
  : ObITask(TASK_TYPE_TRANSFER_BACKFILL_TX),
    is_inited_(false),
    ctx_(nullptr),
    dag_net_(nullptr)
{
}

ObStartTransferBackfillTXTask::~ObStartTransferBackfillTXTask()
{
}

int ObStartTransferBackfillTXTask::init()
{
  int ret = OB_SUCCESS;
  ObIDagNet *dag_net = nullptr;
  ObTransferBackfillTXDagNet *backfill_dag_net = nullptr;

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("start transfer backfill tx task init twice", K(ret));
  } else if (FALSE_IT(dag_net = this->get_dag()->get_dag_net())) {
  } else if (OB_ISNULL(dag_net)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag net should not be NULL", K(ret), KP(dag_net));
  } else if (ObDagNetType::DAG_NET_TRANSFER_BACKFILL_TX != dag_net->get_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag net type is unexpected", K(ret), KPC(dag_net));
  } else if (FALSE_IT(backfill_dag_net = static_cast<ObTransferBackfillTXDagNet*>(dag_net))) {
  } else {
    ctx_ = backfill_dag_net->get_ctx();
    is_inited_ = true;
    LOG_INFO("[TRANSFER_BACKFILL]succeed init transfer backfill tx task", "ls id", ctx_->src_ls_id_,
        "dag_id", *ObCurTraceId::get_trace_id(), "dag_net_id", ctx_->task_id_);
  }
  return ret;
}


int ObStartTransferBackfillTXTask::process()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("start transfer backfill tx task do not init", K(ret));
  } else if (ctx_->is_failed()) {
    //do nothing
  } else if (OB_FAIL(generate_transfer_backfill_tx_dags_())) {
    LOG_WARN("failed to generate transfer backfill tx dags", K(ret), KPC(ctx_));
  }

  if (OB_FAIL(ret)) {
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = ObStorageHADagUtils::deal_with_fo(ret, this->get_dag()))) {
      LOG_WARN("failed to deal with fo", K(ret), K(tmp_ret), KPC(ctx_));
    }
  }
  return ret;
}

int ObStartTransferBackfillTXTask::generate_transfer_backfill_tx_dags_()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObTabletBackfillTXDag *tablet_backfill_tx_dag = nullptr;
  ObFinishBackfillTXDag *finish_backfill_tx_dag = nullptr;
  ObTenantDagScheduler *scheduler = nullptr;
  ObIDagNet *dag_net = nullptr;
  ObBackfillTXCtx *backfill_tx_ctx = nullptr;
  storage::ObTabletBackfillInfo tablet_info;
  ObStartTransferBackfillTXDag *backfill_tx_dag = nullptr;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("start transfer backfill tx task do not init", K(ret));
  } else if (!(ObDagType::DAG_TYPE_TRANSFER_BACKFILL_TX <= this->get_dag()->get_type()
      && ObDagType::DAG_TYPE_TRANSFER_REPLACE_TABLE >= this->get_dag()->get_type())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag type is not match", K(ret), KP(this->get_dag()));
  } else if (OB_ISNULL(backfill_tx_dag = static_cast<ObStartTransferBackfillTXDag *>(this->get_dag()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("start transfer backfill tx dag should not be NULL", K(ret), KP(backfill_tx_dag));
  } else if (OB_ISNULL(dag_net = backfill_tx_dag->get_dag_net())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag net should not be NULL", K(ret), KP(dag_net));
  } else if (OB_ISNULL(scheduler = MTL(ObTenantDagScheduler*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get ObTenantDagScheduler from MTL", K(ret));
  } else {
    if (OB_FAIL(scheduler->alloc_dag(finish_backfill_tx_dag))) {
      LOG_WARN("failed to alloc finish backfill tx transfer dag ", K(ret));
    } else if (OB_FAIL(finish_backfill_tx_dag->init(ctx_->task_id_, ctx_->src_ls_id_, ctx_->backfill_scn_, ctx_->tablet_infos_, ctx_))) {
      LOG_WARN("failed to init data tablets transfer dag", K(ret), K(*ctx_));
    } else if (OB_ISNULL(backfill_tx_ctx = finish_backfill_tx_dag->get_backfill_tx_ctx())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("backfill tx ctx should not be NULL", K(ret), KP(backfill_tx_ctx));
    } else if (backfill_tx_ctx->is_empty()) {
      if (OB_FAIL(this->get_dag()->add_child(*finish_backfill_tx_dag))) {
        LOG_WARN("failed to add finish backfill tx dag as chilid", K(ret), K(*ctx_));
      }
    } else {
      if (OB_FAIL(backfill_tx_ctx->get_tablet_info(tablet_info))) {
        LOG_WARN("failed to get tablet id", K(ret), KPC(ctx_));
      } else if (OB_FAIL(scheduler->alloc_dag(tablet_backfill_tx_dag))) {
        LOG_WARN("failed to alloc tablet backfill tx  dag ", K(ret));
      } else if (OB_FAIL(tablet_backfill_tx_dag->init(ctx_->task_id_, ctx_->src_ls_id_, tablet_info, ctx_, backfill_tx_ctx))) {
        LOG_WARN("failed to init tablet backfill tx dag", K(ret), K(*ctx_));
      } else if (OB_FAIL(this->get_dag()->add_child(*tablet_backfill_tx_dag))) {
        LOG_WARN("failed to add tablet backfill tx dag as chilid", K(ret), K(*ctx_));
      } else if (OB_FAIL(tablet_backfill_tx_dag->create_first_task())) {
        LOG_WARN("failed to create first task", K(ret));
      } else if (OB_FAIL(tablet_backfill_tx_dag->add_child(*finish_backfill_tx_dag))) {
        LOG_WARN("failed to add child dag", K(ret), K(*ctx_));
      } else if (OB_FAIL(finish_backfill_tx_dag->create_first_task())) {
        LOG_WARN("failed to create first task", K(ret));
      } else if (OB_FAIL(scheduler->add_dag(tablet_backfill_tx_dag))) {
        LOG_WARN("failed to add tablet backfill tx dag", K(ret), K(*tablet_backfill_tx_dag));
        if (OB_SIZE_OVERFLOW != ret && OB_EAGAIN != ret) {
          LOG_WARN("Fail to add task", K(ret));
          ret = OB_EAGAIN;
        }
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(scheduler->add_dag(finish_backfill_tx_dag))) {
      LOG_WARN("failed to add finish backfill tx dag", K(ret), K(*finish_backfill_tx_dag));
      if (OB_SIZE_OVERFLOW != ret && OB_EAGAIN != ret) {
        LOG_WARN("Fail to add task", K(ret));
        ret = OB_EAGAIN;
      }
      if (OB_NOT_NULL(tablet_backfill_tx_dag)) {
        if (OB_SUCCESS != (tmp_ret = scheduler->cancel_dag(tablet_backfill_tx_dag, backfill_tx_dag))) {
          LOG_WARN("failed to cancel tablet backfill tx dag", K(tmp_ret), KPC(backfill_tx_dag));
        } else {
          tablet_backfill_tx_dag = nullptr;
        }
      }
    } else {
      LOG_INFO("[TRANSFER_BACKFILL]succeed to schedule tablet backfill tx dag and finish backfill tx dag",
          KPC(tablet_backfill_tx_dag), KPC(finish_backfill_tx_dag));
      tablet_backfill_tx_dag = nullptr;
      finish_backfill_tx_dag = nullptr;
    }

    if (OB_FAIL(ret)) {
      if (OB_NOT_NULL(finish_backfill_tx_dag)) {
        scheduler->free_dag(*finish_backfill_tx_dag, tablet_backfill_tx_dag);
        finish_backfill_tx_dag = nullptr;
      }

      if (OB_NOT_NULL(tablet_backfill_tx_dag)) {
        scheduler->free_dag(*tablet_backfill_tx_dag, backfill_tx_dag);
        tablet_backfill_tx_dag = nullptr;
      }
    }
  }
  return ret;
}

/*****************ObTransferReplaceTableDag***********************/
ObTransferReplaceTableDag::ObTransferReplaceTableDag()
  : ObBaseTransferBackfillTXDag(ObDagType::DAG_TYPE_TRANSFER_REPLACE_TABLE),
    is_inited_(false)
{
}

ObTransferReplaceTableDag::~ObTransferReplaceTableDag()
{
}

int ObTransferReplaceTableDag::fill_dag_key(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  ObTransferBackfillTXCtx *self_ctx = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("transfer replace tables dag do not init", K(ret));
  } else if (ObIHADagNetCtx::TRANSFER_BACKFILL_TX != ha_dag_net_ctx_->get_dag_net_ctx_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ha dag net ctx type is unexpected", K(ret), KPC(ha_dag_net_ctx_));
  } else if (FALSE_IT(self_ctx = static_cast<ObTransferBackfillTXCtx *>(ha_dag_net_ctx_))) {
  } else if (OB_FAIL(databuff_printf(buf, buf_len,
         "ObTransferReplaceTableDag: ls_id = %s",
         to_cstring(self_ctx->dest_ls_id_)))) {
    LOG_WARN("failed to fill comment", K(ret), K(*self_ctx));
  }
  return ret;
}

int ObTransferReplaceTableDag::init(ObIDagNet *dag_net)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("transfer replace tables dag init twice", K(ret));
  } else if (OB_ISNULL(dag_net)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("dag net is NULL", K(ret));
  } else if (OB_FAIL(prepare_ctx(dag_net))) {
    LOG_WARN("failed to prepare ctx", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObTransferReplaceTableDag::create_first_task()
{
  int ret = OB_SUCCESS;
  ObTransferReplaceTableTask *task = NULL;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("transfer replace tables dag do not init", K(ret));
  } else if (OB_FAIL(alloc_task(task))) {
    LOG_WARN("Fail to alloc task", K(ret));
  } else if (OB_FAIL(task->init())) {
    LOG_WARN("failed to init transfer replace tables task", K(ret), KPC(ha_dag_net_ctx_));
  } else if (OB_FAIL(add_task(*task))) {
    LOG_WARN("fail to add ObTransferReplaceTableTask", K(ret));
  } else {
    LOG_INFO("[TRANSFER_BACKFILL]success to create first ObTransferReplaceTableTask", K(ret), KPC(this));
  }
  return ret;
}

int ObTransferReplaceTableDag::fill_info_param(compaction::ObIBasicInfoParam *&out_param, ObIAllocator &allocator) const
{
  int ret = OB_SUCCESS;
  ObTransferBackfillTXCtx *ctx = nullptr;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("start transfer backfill tx dag do not init", K(ret));
  } else if (FALSE_IT(ctx = static_cast<ObTransferBackfillTXCtx *>(ha_dag_net_ctx_))) {
  } else if (OB_FAIL(ADD_DAG_WARN_INFO_PARAM(out_param, allocator, get_type(),
                                static_cast<int64_t>(ctx->tenant_id_),
                                ctx->dest_ls_id_.id(),
                                "dag_net_task_id", to_cstring(ctx->task_id_)))){
    LOG_WARN("failed to fill info param", K(ret));
  }
  return ret;
}

bool ObTransferReplaceTableDag::check_can_retry()
{
  return false;
}

/******************ObTransferReplaceTableTask*********************/
ObTransferReplaceTableTask::ObTransferReplaceTableTask()
  : ObITask(TASK_TYPE_TRANSFER_REPLACE_TABLE),
    is_inited_(false),
    ctx_(nullptr)
{
}

ObTransferReplaceTableTask::~ObTransferReplaceTableTask()
{
}

int ObTransferReplaceTableTask::init()
{
  int ret = OB_SUCCESS;
  ObIDagNet *dag_net = nullptr;
  ObTransferBackfillTXDagNet *backfill_dag_net = nullptr;

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("transfer replace tables task init twice", K(ret));
  } else if (FALSE_IT(dag_net = this->get_dag()->get_dag_net())) {
  } else if (OB_ISNULL(dag_net)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag net should not be NULL", K(ret), KP(dag_net));
  } else if (ObDagNetType::DAG_NET_TRANSFER_BACKFILL_TX != dag_net->get_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag net type is unexpected", K(ret), KPC(dag_net));
  } else if (FALSE_IT(backfill_dag_net = static_cast<ObTransferBackfillTXDagNet*>(dag_net))) {
  } else {
    ctx_ = backfill_dag_net->get_ctx();
    is_inited_ = true;
    LOG_INFO("[TRANSFER_BACKFILL]succeed init transfer replace tables task", "ls id", ctx_->dest_ls_id_,
        "dag_id", *ObCurTraceId::get_trace_id(), "dag_net_id", ctx_->task_id_);
  }
  return ret;
}
int ObTransferReplaceTableTask::check_src_memtable_is_empty_(
    ObTablet *tablet,
    const share::SCN &transfer_scn)
{
  int ret = OB_SUCCESS;
  ObArray<ObTableHandleV2> memtables;
  ObIMemtableMgr *memtable_mgr = nullptr;
  if (OB_ISNULL(tablet) || !transfer_scn.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tablet should not be nullptr.", KR(ret), K(transfer_scn), KPC(this));
  } else if (OB_ISNULL(memtable_mgr = tablet->get_memtable_mgr())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("memtable mgr should not be NULL", K(ret), KP(memtable_mgr));
  } else if (OB_FAIL(memtable_mgr->get_all_memtables(memtables))) {
    LOG_WARN("failed to get all memtables", K(ret), KPC(tablet));
  } else if (!memtables.empty()) {
    for (int64_t i = 0; OB_SUCC(ret) && i < memtables.count(); ++i) {
      ObITable *table = memtables.at(i).get_table();
      memtable::ObMemtable *memtable = static_cast<memtable::ObMemtable *>(table);
      if (OB_ISNULL(table) || !table->is_memtable()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("table should not be NULL or table type is unexpected", K(ret), KP(table));
      } else if (memtable->is_active_memtable()) {
        if (memtable->not_empty()) {
          ret = OB_TRANSFER_SYS_ERROR;
          LOG_ERROR("memtable should not be active", K(ret), KPC_(ctx),
              KPC(memtable), "transfer meta", tablet->get_tablet_meta());
        }
      } else if (!memtable->get_key().scn_range_.is_empty()) {
        ret = OB_TRANSFER_SYS_ERROR;
        LOG_ERROR("The range of the memtable is not empty", K(ret), KPC(memtable));
      } else if (memtable->not_empty()
          && memtable->get_start_scn() >= transfer_scn) {
        LOG_ERROR("There have been transactions in memtable but no data", K(OB_TRANSFER_SYS_ERROR), KPC_(ctx), KPC(memtable));
      }
    }
  }

  return ret;
}

int ObTransferReplaceTableTask::check_source_minor_end_scn_(
    const ObTabletMemberWrapper<ObTabletTableStore> &wrapper,
    const ObTablet *dest_tablet,
    bool &need_fill_minor)
{
  int ret = OB_SUCCESS;

  need_fill_minor = false;

  if (!wrapper.is_valid() || OB_ISNULL(dest_tablet)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("wrapper is invalid or tablet be nullptr.", KR(ret), K(wrapper), KPC(this));
  } else {
    const ObTabletTableStore &table_store = *(wrapper.get_member());
    ObITable *last_minor_mini_sstable = table_store.get_minor_sstables().get_boundary_table(true /*is_last*/);

    if (OB_ISNULL(last_minor_mini_sstable)) {
      LOG_INFO("[TRANSFER_BACKFILL]minor sstable no exists", K(ret), KPC(this), K(table_store));
    } else if (!last_minor_mini_sstable->is_minor_sstable()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("last sstable type is incorrect", K(ret), KPC(last_minor_mini_sstable));
    } else if (last_minor_mini_sstable->get_end_scn() < dest_tablet->get_tablet_meta().transfer_info_.transfer_start_scn_) {
      need_fill_minor = true;
      LOG_INFO("[TRANSFER_BACKFILL]need fill empty minor sstable", "end scn", last_minor_mini_sstable->get_end_scn(),
          "transfer info", dest_tablet->get_tablet_meta().transfer_info_);
    } else if (last_minor_mini_sstable->get_start_scn() >= dest_tablet->get_tablet_meta().transfer_info_.transfer_start_scn_) {
      ObSSTable *sstable = static_cast<ObSSTable *>(last_minor_mini_sstable);
      if (!sstable->is_empty()) {
        ret = OB_TRANSFER_SYS_ERROR;
        LOG_ERROR("last sstable start scn is bigger than transfer start scn", K(ret), KPC(last_minor_mini_sstable),
            "transfer info", dest_tablet->get_tablet_meta().transfer_info_);
      }
    } else if (last_minor_mini_sstable->get_end_scn() > dest_tablet->get_tablet_meta().transfer_info_.transfer_start_scn_) {
      // After adding transfer_freeze_flag_, it can ensure that start is less than transfer_start_scn's sstable,
      // and end_scn will not be greater than transfer_start_scn
      ret = OB_TRANSFER_SYS_ERROR;
      LOG_ERROR("last sstable end scn is bigger than transfer start scn", K(ret), KPC(last_minor_mini_sstable),
            "transfer info", dest_tablet->get_tablet_meta().transfer_info_);
    }
  }
  return ret;
}

int ObTransferReplaceTableTask::check_major_sstable_(
    const ObTablet *tablet,
    const ObTabletMemberWrapper<ObTabletTableStore> &table_store_wrapper)
{
  int ret = OB_SUCCESS;
  ObTableStoreIterator ddl_iter;
  if (OB_ISNULL(tablet) || !table_store_wrapper.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("argumemt is invalid", K(ret), KP(tablet), K(table_store_wrapper));
  } else if (!table_store_wrapper.get_member()->get_major_sstables().empty()) {
    // do nothing
  } else if (OB_FAIL(tablet->get_ddl_sstables(ddl_iter))) {
    LOG_WARN("failed to get ddl sstable", K(ret));
  } else if (ddl_iter.is_valid()) {
    ret = OB_EAGAIN;
    LOG_WARN("wait for ddl sstable to merge to generate major sstable", K(ret), K(ddl_iter));
  } else if (tablet->get_tablet_meta().ha_status_.is_restore_status_full()) {
    ret = OB_INVALID_TABLE_STORE;
    LOG_ERROR("neither major sstable nor ddl sstable exists", K(ret), K(ddl_iter));
  }

  return ret;
}

int ObTransferReplaceTableTask::get_all_sstable_handles_(
    const ObTablet *tablet,
    const ObTabletMemberWrapper<ObTabletTableStore> &wrapper,
    ObTableStoreIterator &sstable_iter,
    ObTablesHandleArray &sstable_handles)
{
  int ret = OB_SUCCESS;
  ObTableHandleV2 table_handle;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("transfer replace tables task do not init", K(ret));
  } else if (OB_ISNULL(tablet)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tablet should not be nullptr", K(ret));
  } else if (tablet->get_tablet_meta().ha_status_.is_restore_status_undefined()
      || tablet->get_tablet_meta().ha_status_.is_restore_status_pending()
      || tablet->get_tablet_meta().ha_status_.is_restore_status_empty()) {
    ret = OB_ERR_UNEXPECTED;;
    LOG_WARN("tablet data is incomplete, replacement should not be performed", K(ret), KP(tablet));
  } else if (OB_FAIL(check_major_sstable_(tablet, wrapper))) {
    LOG_WARN("failed check major sstable", K(ret), KP(tablet));
  } else if (OB_FAIL(wrapper.get_member()->get_all_sstable(sstable_iter))) {
    LOG_WARN("get all sstable fail", K(ret));
  } else {
    while (OB_SUCC(ret) && OB_SUCC(sstable_iter.get_next(table_handle))) {
      if (OB_UNLIKELY(!table_handle.is_valid() || !table_handle.get_table()->is_sstable())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected error, table is nullptr", K(ret), K(table_handle));
      } else if (OB_FAIL(sstable_handles.add_table(table_handle))) {
        LOG_WARN("fail to fill sstable write info", K(ret), K(table_handle));
      }
    }
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

// when the end_scn of the last minor sstable is less than transfer_start_scn,
// it need to fill in an empty minor to ensure the continuity of the sstable
int ObTransferReplaceTableTask::fill_empty_minor_sstable(
    ObTablet *tablet,
    bool need_fill_minor,
    const share::SCN &end_scn,
    const ObTabletMemberWrapper<ObTabletTableStore> &wrapper,
    common::ObArenaAllocator &table_allocator,
    ObTablesHandleArray &tables_handle)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator;
  const ObStorageSchema *tablet_storage_schema = nullptr;
  ObTableHandleV2 empty_minor_table_handle;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
     LOG_WARN("transfer replace tables task do not init", K(ret));
  } else if (!need_fill_minor) {
    // do nothing
  } else if (OB_ISNULL(tablet)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tablet is null", K(ret));
  } else {
    const ObTabletTableStore &table_store = *(wrapper.get_member());
    ObITable *last_minor_mini_sstable = table_store.get_minor_sstables().get_boundary_table(true /*is_last*/);
    share::SCN start_scn;
    if (OB_ISNULL(last_minor_mini_sstable)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("minor sstable is not exit, no need backfill", K(ret));
    } else {
      start_scn = last_minor_mini_sstable->get_end_scn();
      if (start_scn >= end_scn) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("start scn is bigger or equal than end scn", K(ret), K(start_scn), K(end_scn));
      } else if (OB_FAIL(tablet->load_storage_schema(allocator, tablet_storage_schema))) {
        LOG_WARN("fail to load storage schema failed", K(ret));
      } else if (OB_FAIL(ObTXTransferUtils::create_empty_minor_sstable(tablet->get_tablet_meta().tablet_id_, start_scn, end_scn,
          *tablet_storage_schema, table_allocator, empty_minor_table_handle))) {
        LOG_WARN("failed to create empty minor sstable", K(ret), K(start_scn), K(end_scn));
      } else if (OB_FAIL(tables_handle.add_table(empty_minor_table_handle))) {
        LOG_WARN("failed to add table", K(ret), K(empty_minor_table_handle));
      } else {
        LOG_INFO("[TRANSFER_BACKFILL]succ fill empty minor sstable", K(ret), "tablet_id", tablet->get_tablet_meta().tablet_id_,
            K(empty_minor_table_handle), K(start_scn), K(end_scn));
      }
      ObTablet::free_storage_schema(allocator, tablet_storage_schema);
    }
  }
  return ret;
}

int ObTransferReplaceTableTask::get_source_tablet_tables_(
    const ObTablet *dest_tablet,
    const ObTabletBackfillInfo &tablet_info,
    ObTableStoreIterator &sstable_iter,
    ObTabletHandle &tablet_handle,
    ObTabletRestoreStatus::STATUS &restore_status,
    common::ObArenaAllocator &allocator,
    ObTablesHandleArray &tables_handle)
{
  int ret = OB_SUCCESS;
  ObLS *ls = nullptr;
  ObLSService *ls_service = nullptr;
  ObLSHandle ls_handle;
  ObTablet *tablet = nullptr;
  tables_handle.reset();
  ObTabletCreateDeleteMdsUserData src_user_data;
  ObTabletCreateDeleteMdsUserData dest_user_data;
 int64_t src_transfer_seq = 0;
  int64_t dest_transfer_seq = 0;
  bool need_backill = false;
  share::SCN transfer_scn;
  ObTabletMemberWrapper<ObTabletTableStore> wrapper;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("transfer replace tables task do not init", K(ret));
  } else if (OB_ISNULL(dest_tablet) || !tablet_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tablet info is invalid", K(ret), K(tablet_info));
  } else if (FALSE_IT(transfer_scn = dest_tablet->get_tablet_meta().transfer_info_.transfer_start_scn_)) {
  } else if (OB_ISNULL(ls_service = MTL(ObLSService*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get ObLSService from MTL", K(ret), KP(ls_service));
  } else if (OB_FAIL(ls_service->get_ls(ctx_->src_ls_id_, ls_handle, ObLSGetMod::HA_MOD))) {
    LOG_WARN("failed to get ls", K(ret), "ls_id", ctx_->src_ls_id_);
  } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls should not be NULL", K(ret), KP(ls), "ls_id", ctx_->src_ls_id_);
  } else if (OB_FAIL(ls->get_tablet(tablet_info.tablet_id_, tablet_handle, 0, ObMDSGetTabletMode::READ_WITHOUT_CHECK))) {
    LOG_WARN("failed to get tablet", K(ret), K(tablet_info));
  } else if (OB_ISNULL(tablet = tablet_handle.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet should not be NULL", K(ret), KPC(tablet));
  } else if (tablet->is_empty_shell()) {
    ret = OB_EAGAIN;
    LOG_WARN("transfer src tablet should not be empty shell, task need to retry", K(ret), KPC(tablet), "ls_id", ctx_->src_ls_id_);
  } else if (OB_FAIL(ObTXTransferUtils::get_tablet_status(false/*get_commit*/, tablet_handle, src_user_data))) {
    LOG_WARN("failed to get src user data", K(ret), K(tablet_handle), KPC(tablet));
  } else if (OB_FAIL(ObTXTransferUtils::get_tablet_status(false/*get_commit*/, dest_tablet, dest_user_data))) {
    LOG_WARN("failed to get src user data", K(ret), K(tablet_handle), KPC(tablet));
  } else if (FALSE_IT(src_transfer_seq = tablet->get_tablet_meta().transfer_info_.transfer_seq_)) {
  } else if (FALSE_IT(dest_transfer_seq = dest_tablet->get_tablet_meta().transfer_info_.transfer_seq_)) {
  } else if (src_transfer_seq > dest_transfer_seq) {
    ret = OB_TRANSFER_SYS_ERROR;
    LOG_ERROR("src tablet transfer_seq is not match dest tablet transfer_seq", K(ret),
        KPC(tablet), KPC(dest_tablet), K(src_user_data), K(dest_user_data), K(src_transfer_seq), K(dest_transfer_seq));
  } else if (src_transfer_seq + 1 != dest_transfer_seq) {
    ret = OB_EAGAIN;
    LOG_WARN("need to wait for source LS replay", K(ret), KPC(tablet),
        K(src_user_data), K(dest_user_data), K(src_transfer_seq), K(dest_transfer_seq));
  } else if (ObTabletStatus::TRANSFER_OUT != src_user_data.tablet_status_
      && ObTabletStatus::TRANSFER_OUT_DELETED != src_user_data.tablet_status_) {
    if (tablet_info.is_committed_) {
      ret = OB_UNEXPECTED_TABLET_STATUS;
      LOG_WARN("tablet status should be TRANSFER_OUT or TRANSFER_OUT_DELETED", K(ret), KPC(tablet), K(src_user_data));
    } else {
      ret = OB_EAGAIN;
      LOG_WARN("the transfer start transaction was rolledback and the task needs to be retried", K(ret), K(tablet_info), K(src_user_data));
    }
  } else if (OB_FAIL(tablet->get_tablet_meta().ha_status_.get_restore_status(restore_status))) {
    LOG_WARN("failed to get tablet restore status", K(ret));
  } else if (OB_FAIL(tablet->fetch_table_store(wrapper))) {
    LOG_WARN("fetch table store fail", K(ret), KP(tablet));
  } else if (OB_FAIL(check_src_memtable_is_empty_(tablet, transfer_scn))) {
    LOG_WARN("failed to check src memtable", K(ret), KPC(tablet));
  } else if (OB_FAIL(check_source_minor_end_scn_(wrapper, dest_tablet, need_backill))) {
    LOG_WARN("fail to check source max end scn from tablet", K(ret), KPC(tablet));
  } else if (OB_FAIL(get_all_sstable_handles_(tablet, wrapper, sstable_iter, tables_handle))) {
    LOG_WARN("failed to get all sstable handles", K(ret), KPC(tablet));
  } else if (OB_FAIL(fill_empty_minor_sstable(tablet, need_backill,
      dest_tablet->get_tablet_meta().transfer_info_.transfer_start_scn_, wrapper, allocator, tables_handle))) {
    LOG_WARN("failed to check src minor sstables", K(ret), KPC(tablet));
  } else if (OB_FAIL(check_src_tablet_sstables_(tablet, tables_handle))) {
    LOG_WARN("failed to check src minor sstables", K(ret), KPC(tablet));
  }
  return ret;
}

int ObTransferReplaceTableTask::check_src_tablet_sstables_(
    const ObTablet *tablet,
    ObTablesHandleArray &tables_handle)
{
  int ret = OB_SUCCESS;
  bool has_major_sstable = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("replace logical table task do not init", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < tables_handle.get_count(); ++i) {
      ObITable *table = tables_handle.get_table(i);
      ObSSTable *sstable = nullptr;
      if (OB_ISNULL(table) || !table->is_sstable()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table should not be NULL or table type is unexpected", K(ret), KP(table));
      } else if (table->is_major_sstable()) {
        has_major_sstable = true;
      } else if (!table->is_minor_sstable()) {
        //do nothing
      } else {
        sstable = static_cast<ObSSTable *>(table);
        if (sstable->contain_uncommitted_row()) {
          if (table->get_end_scn() >= ctx_->backfill_scn_) {
            ret = OB_TRANSFER_SYS_ERROR;
            LOG_ERROR("src minor still has uncommitted row, unexpected", K(ret), KPC(sstable), KPC(ctx_));
          } else {
            ret = OB_EAGAIN;
            LOG_WARN("sstable has not yet backfilled transactions", K(ret), KPC(sstable), KPC(ctx_));
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (tablet->get_tablet_meta().ha_status_.is_restore_status_full() && !has_major_sstable) {
        ret = OB_TRANSFER_SYS_ERROR;
        LOG_ERROR("major sstable is not exist ", K(ret), K(tables_handle));
      }
    }
  }
  return ret;
}

int ObTransferReplaceTableTask::transfer_replace_tables_(
    ObLS *ls,
    const ObTabletBackfillInfo &tablet_info,
    const ObTablet *tablet)
{
  int ret = OB_SUCCESS;
  common::ObArenaAllocator allocator("TransferTmpTab");
  ObMigrationStatus migration_status;
  ObTabletMemberWrapper<ObTabletTableStore> dest_wrapper;
  ObTabletCreateDeleteMdsUserData user_data;
  ObTabletHandle src_tablet_handle;
  ObTableStoreIterator src_sstable_iter;
  ObMigrationTabletParam mig_param;
  ObBatchUpdateTableStoreParam param;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("transfer replace tables task do not init", K(ret));
  } else if (OB_ISNULL(ls)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("ls should not be nullptr", K(ret));
  } else if (OB_FAIL(ls->get_migration_status(migration_status))) {
    LOG_WARN("failed to get migration status", K(ret), KPC(ls));
  } else if (OB_FAIL(ObTXTransferUtils::get_tablet_status(false/*get_commit*/, tablet, user_data))) {
    LOG_WARN("failed to get tablet status", K(ret), KPC(ls));
  } else if (OB_FAIL(tablet->fetch_table_store(dest_wrapper))) {
    LOG_WARN("failed to fetch table store", K(ret), KPC(tablet));
  } else if (!tablet->get_tablet_meta().has_transfer_table()) {
    if (dest_wrapper.get_member()->get_major_sstables().empty()) {
      ret = OB_TRANSFER_SYS_ERROR;
      LOG_ERROR("There is no transfer table, but the tablet does not exist major sstable",
          K(ret), K(user_data), "tablet_meta", tablet->get_tablet_meta());
    } else {
      LOG_INFO("tablet already complete replace", K(ret), K(user_data), "tablet_meta", tablet->get_tablet_meta());
    }
  } else if (!dest_wrapper.get_member()->get_major_sstables().empty()) {
    ret = OB_INVALID_TABLE_STORE;
    LOG_WARN("tablet should not exist major sstable", K(ret), KPC(tablet));
  } else if (OB_FAIL(get_source_tablet_tables_(tablet, tablet_info, src_sstable_iter, src_tablet_handle, param.restore_status_, allocator, param.tables_handle_))) {
    LOG_WARN("failed to get source tablet tables", K(ret), K(tablet_info));
  } else if (OB_FAIL(build_migration_param_(tablet, src_tablet_handle, mig_param))) {
    LOG_WARN("failed to build migration param", K(ret), KPC(tablet));
  } else {
    param.rebuild_seq_ = ls->get_rebuild_seq();
    param.is_transfer_replace_ = true;
    param.tablet_meta_ = &mig_param;
#ifdef ERRSIM
    param.errsim_point_info_ = ctx_->errsim_point_info_;
    SERVER_EVENT_SYNC_ADD("TRANSFER", "TRANSFER_REPLACE_TABLE_WITH_LOG_REPLAY_SKIP_CHECK",
                          "dest_ls_id", ls->get_ls_id(),
                          "migration_status", migration_status,
                          "tablet_id", tablet_info.tablet_id_.id(),
                          "tablet_status", ObTabletStatus::get_str(user_data.tablet_status_),
                          "has_transfer_table", tablet->get_tablet_meta().has_transfer_table());
#endif

    if (FAILEDx(ls->build_ha_tablet_new_table_store(tablet_info.tablet_id_, param))) {
      LOG_WARN("failed to build ha tablet new table store", K(ret), K(param), K(tablet_info));
    } else {
      LOG_INFO("[TRANSFER_BACKFILL]succ transfer replace tables", K(ret), K(param), K(tablet_info), KPC_(ctx));
    }
#ifdef ERRSIM
    SERVER_EVENT_SYNC_ADD("TRANSFER", "AFTER_TRANSFER_DUMP_MDS_TABLE");
#endif
    DEBUG_SYNC(AFTER_TRANSFER_DUMP_MDS_TABLE);
  }

  return ret;
}

int ObTransferReplaceTableTask::do_replace_logical_tables_(ObLS *ls)
{
  int ret = OB_SUCCESS;
  ObTabletHandle tablet_handle;
  ObTablet *tablet = nullptr;
  ObTabletCreateDeleteMdsUserData user_data;
  DEBUG_SYNC(TRANSFER_REPLACE_TABLE_BEFORE);
  if (OB_ISNULL(ls)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("ls is nullptr", K(ret));
  } else {
    for (int i = 0; OB_SUCC(ret) && i < ctx_->tablet_infos_.count(); i++) {
      user_data.reset();
      const ObTabletBackfillInfo tablet_info = ctx_->tablet_infos_.at(i);
      bool in_migration = false;
      ObMigrationStatus migration_status = ObMigrationStatus::OB_MIGRATION_STATUS_MAX;
      if (ctx_->is_failed()) {
        int tmp_ret = OB_SUCCESS;
        if (OB_SUCCESS != (tmp_ret = ctx_->get_result(ret))) {
          ret = tmp_ret;
        }
        LOG_WARN("ctx already failed", K(ret), KPC(ctx_), K(tablet_info));
      } else if (OB_FAIL(ls->get_migration_status(migration_status))) {
        LOG_WARN("failed to get migration status", K(ret), KPC(ls));
      } else if (FALSE_IT(in_migration = ObMigrationStatus::OB_MIGRATION_STATUS_NONE != migration_status)) {
      } else if (OB_FAIL(ls->get_tablet(tablet_info.tablet_id_, tablet_handle, 0, ObMDSGetTabletMode::READ_WITHOUT_CHECK))) {
        LOG_WARN("failed to get tablet", K(ret), K(tablet_info));
      } else if (OB_ISNULL(tablet = tablet_handle.get_obj())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("tablet should not be NULL", K(ret), KPC(tablet));
      } else if (tablet_info.tablet_id_.is_ls_inner_tablet()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("inner tablet cannot transfer", KR(ret), K(tablet_info), KPC(this));
      } else if (OB_FAIL(ObTXTransferUtils::get_tablet_status(false/*get_commit*/, tablet, user_data))) {
        LOG_WARN("failed to get tablet status", K(ret), K(tablet_handle));
      } else if (ObTabletStatus::TRANSFER_IN != user_data.tablet_status_ && !in_migration) {
        ret = OB_UNEXPECTED_TABLET_STATUS;
        LOG_WARN("tablet status should be TRANSFER_IN", K(ret), K(user_data), K(in_migration), KPC(tablet), KPC(ls));
      } else if (OB_FAIL(transfer_replace_tables_(ls, tablet_info, tablet))) {
        LOG_WARN("failed to transfer replace tables", K(ret), K(tablet_info), KPC(ls), KPC(tablet), KPC(ctx_));
      } else {
#ifdef ERRSIM
        ObErrsimBackfillPointType point_type(ObErrsimBackfillPointType::TYPE::ERRSIM_REPLACE_AFTER);
        if (ctx_->errsim_point_info_.is_errsim_point(point_type)) {
          ret = OB_EAGAIN;
          LOG_WARN("[ERRSIM TRANSFER] errsim transfer replace after", K(ret), K(point_type));
        }
        SERVER_EVENT_ADD("TRANSFER", "REPLACE_LOGICAL_TABLE",
                         "task_id", ctx_->task_id_,
                         "tenant_id", ctx_->tenant_id_,
                         "src_ls_id", ctx_->src_ls_id_.id(),
                         "dest_ls_id", ctx_->dest_ls_id_.id(),
                         "tablet_id", tablet_info.tablet_id_.id());
#endif
      }
    }
  }
  return ret;
}

int ObTransferReplaceTableTask::process()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObLS *ls = nullptr;
  ObLSService *ls_service = nullptr;
  ObLSHandle ls_handle;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("transfer replace tables task do not init", K(ret));
  } else if (ctx_->is_failed()) {
    LOG_INFO("[TRANSFER_BACKFILL]ctx already failed", KPC(ctx_), "tablet_list", ctx_->tablet_infos_);
  } else if (OB_ISNULL(ls_service = MTL(ObLSService*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get ObLSService from MTL", K(ret), KP(ls_service));
  } else if (OB_FAIL(ls_service->get_ls(ctx_->dest_ls_id_, ls_handle, ObLSGetMod::HA_MOD))) {
    LOG_WARN("failed to get ls", K(ret), KPC(ctx_));
  } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls should not be NULL", K(ret), KP(ls), KPC(ctx_));
  } else if (OB_FAIL(do_replace_logical_tables_(ls))) {
    LOG_WARN("failed to repalce logical tables", K(ret), KP(ls), KPC(ctx_), "tablet_list", ctx_->tablet_infos_);
  } else {
    LOG_INFO("[TRANSFER_BACKFILL]complete transfer replace task", K(ret), KPC(ctx_), "tablet_list", ctx_->tablet_infos_);
  }

  if (OB_FAIL(ret)) {
    if (OB_SUCCESS != (tmp_ret = ObStorageHADagUtils::deal_with_fo(ret, this->get_dag()))) {
      LOG_WARN("failed to deal with fo", K(ret), K(tmp_ret), KPC(ctx_));
    }
  }
  return ret;
}

int ObTransferReplaceTableTask::build_migration_param_(
    const ObTablet *tablet,
    ObTabletHandle &src_tablet_handle,
    ObMigrationTabletParam &param)
{
  int ret = OB_SUCCESS;
  param.reset();
  ObTablet *src_tablet = nullptr;
  ObArenaAllocator allocator;
  const ObStorageSchema *src_storage_schema = nullptr;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("transfer replace tables task do not init", K(ret));
  } else if (OB_ISNULL(tablet) || !src_tablet_handle.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("build migration param get invalid argument", K(ret), KP(tablet), K(src_tablet_handle));
  } else if (OB_FAIL(tablet->build_migration_tablet_param(param))) {
    LOG_WARN("failed to build migration tablet param", K(ret), KPC(tablet));
  } else if (OB_ISNULL(src_tablet = src_tablet_handle.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("src tablet should not be NULL", K(ret), KP(src_tablet));
  } else {
    param.mds_data_.reset();
    param.storage_schema_.reset();
    param.snapshot_version_ = src_tablet->get_tablet_meta().snapshot_version_;
    param.multi_version_start_ = src_tablet->get_tablet_meta().multi_version_start_;

    if (OB_FAIL(src_tablet->get_fused_medium_info_list(param.allocator_, param.mds_data_))) {
      LOG_WARN("failed to init mds data", K(ret), K(param));
    } else if (OB_FAIL(src_tablet->load_storage_schema(allocator, src_storage_schema))) {
      LOG_WARN("failed to load storage schema", K(ret), KPC(tablet));
    } else if (OB_FAIL(param.storage_schema_.assign(param.allocator_, *src_storage_schema))) {
      LOG_WARN("failed to assign src storage schema", K(ret), KPC(src_storage_schema));
    } else {
      LOG_INFO("[TRANSFER_BACKFILL]succeed build transfer replace task migration param", K(param));
    }
  }
  return ret;
}

}
}
