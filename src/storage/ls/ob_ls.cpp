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

#include "lib/utility/utility.h"
#include "share/ob_tenant_info_proxy.h"
#include "storage/ls/ob_ls.h"
#include "share/leak_checker/obj_leak_checker.h"
#include "share/ob_ls_id.h"
#include "share/ob_global_autoinc_service.h"
#include "logservice/ob_garbage_collector.h"
#include "logservice/ob_log_base_type.h"
#include "logservice/ob_log_service.h"
#include "logservice/archiveservice/ob_archive_service.h"
#include "logservice/data_dictionary/ob_data_dict_service.h"
#include "storage/tx/ob_trans_service.h"
#include "observer/report/ob_i_meta_report.h"
#include "storage/compaction/ob_tenant_tablet_scheduler.h"
#include "storage/tx/ob_tx_log_adapter.h"
#include "storage/tx_table/ob_tx_table.h"
#include "storage/slog/ob_storage_log.h"
#include "storage/slog/ob_storage_logger.h"
#include "storage/slog/ob_storage_log_struct.h"
#include "storage/tablet/ob_tablet_create_delete_helper.h"
#include "storage/tablet/ob_tablet_iterator.h"
#include "storage/tx/ob_timestamp_service.h"
#include "storage/tx/ob_trans_id_service.h"
#include "storage/tx/ob_standby_timestamp_service.h"
#include "rootserver/freeze/ob_major_freeze_service.h"
#include "rootserver/ob_primary_ls_service.h"
#include "rootserver/ob_recovery_ls_service.h"
#include "rootserver/restore/ob_restore_scheduler.h"
#include "sql/das/ob_das_id_service.h"
#include "storage/tablet/ob_tablet.h"

namespace oceanbase
{
using namespace share;
using namespace logservice;
using namespace transaction;

namespace storage
{
using namespace checkpoint;

const uint64_t ObLS::INNER_TABLET_ID_LIST[TOTAL_INNER_TABLET_NUM] = {
    common::ObTabletID::LS_TX_CTX_TABLET_ID,
    common::ObTabletID::LS_TX_DATA_TABLET_ID,
    common::ObTabletID::LS_LOCK_TABLET_ID,
};

ObLS::ObLS()
  : ls_tx_svr_(this),
    replay_handler_(this),
    ls_freezer_(this),
    ls_sync_tablet_seq_handler_(),
    ls_ddl_log_handler_(),
    ls_recovery_stat_handler_(),
    is_inited_(false),
    tenant_id_(OB_INVALID_TENANT_ID),
    is_stopped_(false),
    is_offlined_(false),
    ls_meta_(),
    rs_reporter_(nullptr)
{}

ObLS::~ObLS()
{
  destroy();
}

int ObLS::init(const share::ObLSID &ls_id,
               const uint64_t tenant_id,
               const ObReplicaType replica_type,
               const ObMigrationStatus &migration_status,
               const ObLSRestoreStatus &restore_status,
               const SCN &create_scn,
               observer::ObIMetaReport *reporter)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  logservice::ObLogService *logservice = MTL(logservice::ObLogService *);
  transaction::ObTransService *txs_svr = MTL(transaction::ObTransService *);
  ObLSService *ls_service = MTL(ObLSService *);

  if (!ls_id.is_valid() ||
      !is_valid_tenant_id(tenant_id) ||
      !common::ObReplicaTypeCheck::is_replica_type_valid(replica_type) ||
      !ObMigrationStatusHelper::is_valid(migration_status) ||
      OB_ISNULL(reporter)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(ls_id), K(tenant_id), K(replica_type), K(migration_status),
             KP(reporter));
  } else if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ls is already initialized", K(ret), K_(ls_meta));
  } else if (tenant_id != MTL_ID()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("tenant is not match", K(tenant_id), K(MTL_ID()), K(ret));
  } else if (FALSE_IT(tenant_id_ = tenant_id)) {
  } else if (OB_FAIL(ls_meta_.init(tenant_id,
                                   ls_id,
                                   replica_type,
                                   migration_status,
                                   restore_status,
                                   create_scn))) {
    LOG_WARN("failed to init ls meta", K(ret), K(tenant_id), K(ls_id), K(replica_type));
  } else {
    rs_reporter_ = reporter;
    ls_freezer_.init(this);
    transaction::ObTxPalfParam tx_palf_param(get_log_handler());

    // tx_table_.init() should after ls_table_svr.init()
    if (OB_FAIL(txs_svr->create_ls(ls_id, *this, &tx_palf_param, nullptr))) {
      LOG_WARN("create trans service failed.", K(ret), K(ls_id));
    } else if (OB_FAIL(ls_tablet_svr_.init(this, reporter))) {
      LOG_WARN("ls tablet service init failed.", K(ret), K(ls_id), K(reporter));
    } else if (OB_FAIL(tx_table_.init(this))) {
      LOG_WARN("init tx table failed",K(ret));
    } else if (OB_FAIL(checkpoint_executor_.init(this, get_log_handler()))) {
      LOG_WARN("checkpoint executor init failed", K(ret));
    } else if (OB_FAIL(data_checkpoint_.init(this))) {
      LOG_WARN("init data checkpoint failed",K(ret));
    } else if (OB_FAIL(ls_tx_svr_.register_common_checkpoint(checkpoint::DATA_CHECKPOINT_TYPE, &data_checkpoint_))) {
      LOG_WARN("data_checkpoint_ register_common_checkpoint failed", K(ret));
    } else if (OB_FAIL(lock_table_.init(this))) {
      LOG_WARN("init lock table failed",K(ret));
    } else if (OB_FAIL(ls_sync_tablet_seq_handler_.init(this))) {
      LOG_WARN("init ls sync tablet seq handler failed", K(ret));
    } else if (OB_FAIL(ls_ddl_log_handler_.init(this))) {
      LOG_WARN("init ls ddl log handler failed", K(ret));
    } else if (OB_FAIL(keep_alive_ls_handler_.init(ls_meta_.ls_id_, get_log_handler()))) {
      LOG_WARN("init keep_alive_ls_handler failed", K(ret));
    } else if (OB_FAIL(gc_handler_.init(this))) {
      LOG_WARN("init gc handler failed", K(ret));
    } else if (OB_FAIL(ls_wrs_handler_.init())) {
      LOG_WARN("ls loop worker init failed", K(ret));
    } else if (OB_FAIL(ls_restore_handler_.init(this))) {
      LOG_WARN("init ls restore handler", K(ret));
    } else if (OB_FAIL(ls_migration_handler_.init(this, GCTX.bandwidth_throttle_,
        ls_service->get_storage_rpc_proxy(), ls_service->get_storage_rpc(), GCTX.sql_proxy_))) {
      LOG_WARN("failed to init ls migration handler", K(ret));
    } else if (OB_FAIL(ls_remove_member_handler_.init(this, ls_service->get_storage_rpc()))) {
      LOG_WARN("failed to init ls remove member handler", K(ret));
    } else if (OB_FAIL(ls_rebuild_cb_impl_.init(this, GCTX.bandwidth_throttle_,
        ls_service->get_storage_rpc_proxy(), ls_service->get_storage_rpc()))) {
      LOG_WARN("failed to init ls rebuild cb impl", K(ret));
    } else if (OB_FAIL(tablet_gc_handler_.init(this))) {
      LOG_WARN("init tablet gc handler", K(ret));
    } else if (OB_FAIL(reserved_snapshot_mgr_.init(this, &log_handler_))) {
      LOG_WARN("failed to init reserved snapshot mgr", K(ret), K(ls_id));
    } else if (OB_FAIL(reserved_snapshot_clog_handler_.init(this))) {
      LOG_WARN("failed to init reserved snapshot clog handler", K(ret), K(ls_id));
    } else if (OB_FAIL(medium_compaction_clog_handler_.init(this))) {
      LOG_WARN("failed to init medium compaction clog handler", K(ret), K(ls_id));
    } else if (OB_FAIL(ls_recovery_stat_handler_.init(tenant_id, this))) {
      LOG_WARN("ls_recovery_stat_handler_ init failed", KR(ret));
    } else {
      REGISTER_TO_LOGSERVICE(logservice::TRANS_SERVICE_LOG_BASE_TYPE, &ls_tx_svr_);
      REGISTER_TO_LOGSERVICE(logservice::STORAGE_SCHEMA_LOG_BASE_TYPE, &ls_tablet_svr_);
      REGISTER_TO_LOGSERVICE(logservice::TABLET_SEQ_SYNC_LOG_BASE_TYPE, &ls_sync_tablet_seq_handler_);
      REGISTER_TO_LOGSERVICE(logservice::DDL_LOG_BASE_TYPE, &ls_ddl_log_handler_);
      REGISTER_TO_LOGSERVICE(logservice::KEEP_ALIVE_LOG_BASE_TYPE, &keep_alive_ls_handler_);
      REGISTER_TO_LOGSERVICE(logservice::GC_LS_LOG_BASE_TYPE, &gc_handler_);
      REGISTER_TO_LOGSERVICE(logservice::RESERVED_SNAPSHOT_LOG_BASE_TYPE, &reserved_snapshot_clog_handler_);
      REGISTER_TO_LOGSERVICE(logservice::MEDIUM_COMPACTION_LOG_BASE_TYPE, &medium_compaction_clog_handler_);

      if (ls_id == IDS_LS) {
        REGISTER_TO_LOGSERVICE(logservice::TIMESTAMP_LOG_BASE_TYPE, MTL(transaction::ObTimestampService *));
        REGISTER_TO_LOGSERVICE(logservice::TRANS_ID_LOG_BASE_TYPE, MTL(transaction::ObTransIDService *));
        if (is_user_tenant(tenant_id)) {
          REGISTER_TO_RESTORESERVICE(logservice::STANDBY_TIMESTAMP_LOG_BASE_TYPE, MTL(transaction::ObStandbyTimestampService *));
        } else {
          REGISTER_TO_LOGSERVICE(logservice::DAS_ID_LOG_BASE_TYPE, MTL(sql::ObDASIDService *));
        }
        LOG_INFO("register id service success");
      }

      if (OB_SUCC(ret) && (ls_id == MAJOR_FREEZE_LS)) {
        REGISTER_TO_LOGSERVICE(logservice::MAJOR_FREEZE_LOG_BASE_TYPE, MTL(rootserver::ObPrimaryMajorFreezeService *));
        LOG_INFO("register primary major freeze service complete", KR(ret));
        REGISTER_TO_RESTORESERVICE(logservice::MAJOR_FREEZE_LOG_BASE_TYPE, MTL(rootserver::ObRestoreMajorFreezeService *));
        LOG_INFO("register restore major freeze service complete", KR(ret));
      }

      if (ls_id == GAIS_LS && OB_SUCC(ret)) {
        REGISTER_TO_LOGSERVICE(logservice::GAIS_LOG_BASE_TYPE, MTL(share::ObGlobalAutoIncService *));
        MTL(share::ObGlobalAutoIncService *)->set_cache_ls(this);
      }
      if (OB_SUCC(ret) && ls_id.is_sys_ls()) {
        //meta tenant need create thread for ls manager
        REGISTER_TO_LOGSERVICE(logservice::PRIMARY_LS_SERVICE_LOG_BASE_TYPE, MTL(rootserver::ObPrimaryLSService *));
        LOG_INFO("primary ls manager registre to logservice success");
      }

      if (OB_SUCC(ret) && is_user_tenant(tenant_id) && ls_id.is_sys_ls()) {
        // only user tenant need dump datadict
        REGISTER_TO_LOGSERVICE(logservice::DATA_DICT_LOG_BASE_TYPE, MTL(datadict::ObDataDictService *));
        //only user table need recovery
        REGISTER_TO_RESTORESERVICE(logservice::RECOVERY_LS_SERVICE_LOG_BASE_TYPE, MTL(rootserver::ObRecoveryLSService *));
        LOG_INFO("recovery ls manager registre to restoreservice success");
      }

      if (OB_SUCC(ret) && !is_user_tenant(tenant_id) && ls_id.is_sys_ls()) {
        //sys and meta tenant
        REGISTER_TO_LOGSERVICE(logservice::RESTORE_SERVICE_LOG_BASE_TYPE, MTL(rootserver::ObRestoreService *));
      }


      if (OB_SUCC(ret)) {             // don't delete it
        election_priority_.set_ls_id(ls_id);
        is_inited_ = true;
        LOG_INFO("ls init success", K(ls_id));
      }
    }
    // do some rollback work
    if (OB_FAIL(ret)) {
      destroy();
    }
  }
  return ret;
}

int ObLS::create_ls_inner_tablet(const lib::Worker::CompatMode compat_mode,
                                 const SCN &create_scn)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(tx_table_.create_tablet(compat_mode, create_scn))) {
    LOG_WARN("tx table create tablet failed", K(ret), K_(ls_meta), K(compat_mode), K(create_scn));
  } else if (OB_FAIL(lock_table_.create_tablet(compat_mode, create_scn))) {
    LOG_WARN("lock table create tablet failed", K(ret), K_(ls_meta), K(compat_mode), K(create_scn));
  }
  return ret;
}

int ObLS::load_ls_inner_tablet()
{
  int ret = OB_SUCCESS;
  // lock_table should recover before tx_table_
  // because the lock info at tx ctx need recover to
  // lock memtable.
  if (OB_FAIL(lock_table_.load_lock())) {
    LOG_WARN("lock table load lock failed",K(ret), K_(ls_meta));
  } else if (OB_FAIL(tx_table_.load_tx_table())) {
    LOG_WARN("tx table load tablet failed",K(ret), K_(ls_meta));
  }
  return ret;
}

int ObLS::create_ls(const share::ObTenantRole tenant_role,
                    const palf::PalfBaseInfo &palf_base_info,
                    const bool allow_log_sync)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  bool is_palf_exist = false;
  bool need_retry = false;
  static const int64_t SLEEP_TS = 100_ms;
  int64_t retry_cnt = 0;
  logservice::ObLogService *logservice = MTL(logservice::ObLogService *);
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls do not init", K(ret));
  } else if (OB_FAIL(logservice->check_palf_exist(ls_meta_.ls_id_, is_palf_exist))) {
    LOG_WARN("check_palf_exist failed", K(ret), K_(ls_meta));
  } else if (is_palf_exist) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("palf should not exist now", K(ret), K_(ls_meta));
  } else if (OB_FAIL(logservice->create_ls(ls_meta_.ls_id_,
                                           ls_meta_.replica_type_,
                                           tenant_role,
                                           palf_base_info,
                                           allow_log_sync,
                                           log_handler_,
                                           restore_handler_))) {
    LOG_WARN("create palf failed", K(ret), K_(ls_meta));
  } else {
    if (OB_FAIL(log_handler_.set_election_priority(&election_priority_))) {
      LOG_WARN("set election failed", K(ret), K_(ls_meta));
    } else if (OB_FAIL(log_handler_.register_rebuild_cb(&ls_rebuild_cb_impl_))) {
      LOG_WARN("failed to register rebuild cb", K(ret), KPC(this));
    }
    if (OB_FAIL(ret)) {
      do {
        // TODO: yanyuan.cxf every remove disable or stop function need be re-entrant
        need_retry = false;
        if (OB_TMP_FAIL(remove_ls())) {
          need_retry = true;
          LOG_WARN("remove_ls from disk failed", K(tmp_ret), K_(ls_meta));
        }
        if (need_retry) {
          retry_cnt++;
          ob_usleep(SLEEP_TS);
          if (retry_cnt % 100 == 0) {
            LOG_ERROR("remove_ls from disk cost too much time", K(tmp_ret), K(need_retry), K_(ls_meta));
          }
        }
      } while (need_retry);
    }
  }
  return ret;
}

int ObLS::load_ls(const share::ObTenantRole &tenant_role,
                  const palf::PalfBaseInfo &palf_base_info,
                  const bool allow_log_sync)
{
  int ret = OB_SUCCESS;
  logservice::ObLogService *logservice = MTL(logservice::ObLogService *);
  bool is_palf_exist = false;

  if (OB_FAIL(logservice->check_palf_exist(ls_meta_.ls_id_, is_palf_exist))) {
    LOG_WARN("check_palf_exist failed", K(ret), K_(ls_meta));
  } else if (!is_palf_exist) {
    LOG_WARN("there is no ls at disk, skip load", K_(ls_meta));
  } else if (OB_FAIL(logservice->add_ls(ls_meta_.ls_id_,
                                        ls_meta_.replica_type_,
                                        log_handler_,
                                        restore_handler_))) {
    LOG_WARN("add ls failed", K(ret), K_(ls_meta));
  } else {
    if (OB_FAIL(log_handler_.set_election_priority(&election_priority_))) {
      LOG_WARN("set election failed", K(ret), K_(ls_meta));
    } else if (OB_FAIL(log_handler_.register_rebuild_cb(&ls_rebuild_cb_impl_))) {
      LOG_WARN("failed to register rebuild cb", K(ret), KPC(this));
    }
    // TODO: add_ls has no interface to rollback now, something can not rollback.
    if (OB_FAIL(ret)) {
      LOG_ERROR("load ls failed", K(ret), K(ls_meta_), K(tenant_role), K(palf_base_info));
    }
  }
  return ret;
}

int ObLS::remove_ls()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  logservice::ObLogService *logservice = MTL(logservice::ObLogService *);
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls do not init", K(ret));
  } else {
    log_handler_.reset_election_priority();
    if (OB_TMP_FAIL(log_handler_.unregister_rebuild_cb())) {
      LOG_WARN("unregister rebuild cb failed", K(ret), K(ls_meta_));
    }
    if (OB_FAIL(logservice->remove_ls(ls_meta_.ls_id_, log_handler_, restore_handler_))) {
      LOG_ERROR("remove log stream from logservice failed", K(ret), K(ls_meta_.ls_id_));
    }
  }
  LOG_INFO("remove ls from disk", K(ret), K(ls_meta_));
  return ret;
}

void ObLS::set_create_state(const ObInnerLSStatus new_status)
{
  ls_meta_.set_ls_create_status(new_status);
}

ObInnerLSStatus ObLS::get_create_state() const
{
  return ls_meta_.get_ls_create_status();
}

bool ObLS::is_need_gc() const
{
  int ret = OB_SUCCESS;
  bool bool_ret = false;
  ObMigrationStatus migration_status;
  ObInnerLSStatus create_status = ls_meta_.get_ls_create_status();

  if (OB_FAIL(ls_meta_.get_migration_status(migration_status))) {
    LOG_WARN("get migration status failed", K(ret), K(ls_meta_.ls_id_));
  }

  bool_ret = (OB_FAIL(ret) ||
              (ObInnerLSStatus::CREATING == create_status) ||
              (ObInnerLSStatus::REMOVED == create_status) ||
              (OB_MIGRATION_STATUS_NONE != migration_status &&
               ObMigrationStatusHelper::check_allow_gc(migration_status)));
  if (bool_ret) {
    FLOG_INFO("ls need gc", K(ret), K(ls_meta_), K(migration_status));
  }
  return bool_ret;
}

bool ObLS::is_enable_for_restore() const
{
  int ret = OB_SUCCESS;
  bool bool_ret = false;
  ObLSRestoreStatus restore_status;
  if (OB_FAIL(ls_meta_.get_restore_status(restore_status))) {
    LOG_WARN("fail to get restore status", K(ret), K(ls_meta_.ls_id_));
  } else {
    bool_ret = restore_status.is_enable_for_restore();
  }
  return bool_ret;
}

void ObLS::finish_create(const bool is_commit)
{
  ObInnerLSStatus status = ObInnerLSStatus::CREATING;
  if (is_commit) {
    status = ObInnerLSStatus::COMMITTED;
  } else {
    status = ObInnerLSStatus::ABORTED;
  }
  ls_meta_.set_ls_create_status(status);
}

int ObLS::start()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls is not inited", K(ret));
  } else if (OB_FAIL(tx_table_.start())) {
    LOG_WARN("tx table start failed", K(ret), KPC(this));
  } else {
    checkpoint_executor_.start();
    LOG_INFO("start_ls finish", KR(ret), KPC(this));
    // do nothing
  }
  return ret;
}

int ObLS::stop()
{
  int64_t read_lock = 0;
  int64_t write_lock = LSLOCKALL;

  ObLSLockGuard lock_myself(lock_, read_lock, write_lock);
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls is not inited", K(ret));
  } else if (OB_FAIL(stop_())) {
    LOG_WARN("stop ls failed", K(ret), K(ls_meta_));
  } else {
  }
  return ret;
}

int ObLS::stop_()
{
  int ret = OB_SUCCESS;
  tx_table_.stop();
  ls_restore_handler_.stop();
  keep_alive_ls_handler_.stop();
  log_handler_.reset_election_priority();
  restore_handler_.stop();
  if (OB_FAIL(log_handler_.stop())) {
    LOG_WARN("stop log handler failed", K(ret), KPC(this));
  }
  ls_migration_handler_.stop();
  ls_tablet_svr_.stop();
  is_stopped_ = true;

  if (OB_SUCC(ret)) {
    if (OB_FAIL(prepare_for_safe_destroy_())) {
      LOG_WARN("fail to prepare_for_safe_destroy", K(ret));
    } else {
      LOG_INFO("stop_ls finish", KR(ret), KPC(this));
    }
  }

  return ret;
}

void ObLS::wait()
{
  ObTimeGuard time_guard("ObLS::wait", 10 * 1000 * 1000);
  int64_t read_lock = LSLOCKALL;
  int64_t write_lock = 0;
  bool wait_finished = true;
  int64_t start_ts = ObTimeUtility::current_time();
  int64_t retry_times = 0;

  do {
    retry_times++;
    {
      ObLSLockGuard lock_myself(lock_, read_lock, write_lock);
      ls_migration_handler_.wait(wait_finished);
    }
    if (!wait_finished) {
      ob_usleep(100 * 1000); // 100 ms
      if (retry_times % 100 == 0) { // every 10 s
        LOG_WARN_RET(OB_ERR_UNEXPECTED, "ls wait not finished.", K(ls_meta_), K(start_ts));
      }
    }
  } while (!wait_finished);
}

void ObLS::wait_()
{
  ObTimeGuard time_guard("ObLS::wait", 10 * 1000 * 1000);
  bool wait_finished = true;
  int64_t start_ts = ObTimeUtility::current_time();
  int64_t retry_times = 0;
  do {
    retry_times++;
    ls_migration_handler_.wait(wait_finished);
    if (!wait_finished) {
      ob_usleep(100 * 1000); // 100 ms
      if (retry_times % 100 == 0) { // every 10 s
        LOG_WARN_RET(OB_ERR_UNEXPECTED, "ls wait not finished.", K(ls_meta_), K(start_ts));
      }
    }
  } while (!wait_finished);
}

// a class should implement prepare_for_safe_destroy() if it has
// resource which depend on ls. the resource here is refer to all kinds of
// memtables which are delayed GC in t3m due to performance problem.
int ObLS::prepare_for_safe_destroy_()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(lock_table_.prepare_for_safe_destroy())) {
    LOG_WARN("fail to prepare_for_safe_destroy", K(ret));
  } else if (OB_FAIL(ls_tablet_svr_.prepare_for_safe_destroy())) {
    LOG_WARN("fail to prepare_for_safe_destroy", K(ret));
  } else if (OB_FAIL(tx_table_.prepare_for_safe_destroy())) {
    LOG_WARN("fail to prepare_for_safe_destroy", K(ret));
  }
  return ret;
}

bool ObLS::safe_to_destroy()
{
  int ret = OB_SUCCESS;
  bool is_safe = false;
  bool is_ls_restore_handler_safe = false;
  bool is_tablet_service_safe = false;
  bool is_data_check_point_safe = false;
  bool is_log_handler_safe = false;

  if (OB_FAIL(ls_tablet_svr_.safe_to_destroy(is_tablet_service_safe))) {
    LOG_WARN("ls tablet service check safe to destroy failed", K(ret), KPC(this));
  } else if (!is_tablet_service_safe) {
  } else if (OB_FAIL(data_checkpoint_.safe_to_destroy(is_data_check_point_safe))) {
    LOG_WARN("data_checkpoint check safe to destroy failed", K(ret), KPC(this));
  } else if (!is_data_check_point_safe) {
  } else if (OB_FAIL(ls_restore_handler_.safe_to_destroy(is_ls_restore_handler_safe))) {
    LOG_WARN("ls restore handler safe to destroy failed", K(ret), KPC(this));
  } else if (!is_ls_restore_handler_safe) {
  } else if (OB_FAIL(log_handler_.safe_to_destroy(is_log_handler_safe))) {
    LOG_WARN("log_handler_ check safe to destroy failed", K(ret), KPC(this));
  } else if (!is_log_handler_safe) {
  } else {
    if (1 == ref_mgr_.get_total_ref_cnt()) { // only has one ref at the safe destroy task
      is_safe = true;
    }
  }

  // check safe to destroy of all the sub module.
  if (OB_SUCC(ret)) {
    if (!is_safe) {
      if (REACH_TIME_INTERVAL(60 * 1000 * 1000)) {
        LOG_WARN("this ls is not safe to destroy", K(is_safe),
                 K(is_tablet_service_safe), K(is_data_check_point_safe),
                 K(is_ls_restore_handler_safe), K(is_log_handler_safe),
                 "ls_ref", ref_mgr_.get_total_ref_cnt(),
                 K(ret), KP(this), KPC(this));
        ref_mgr_.print();
        PRINT_OBJ_LEAK(MTL_ID(), share::LEAK_CHECK_OBJ_LS_HANDLE);
      }
    } else {
      LOG_INFO("this ls is safe to destroy", KP(this), KPC(this));
    }
  }
  return is_safe;
}

void ObLS::destroy()
{
  // TODO: (yanyuan.cxf) destroy all the sub module.
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (tenant_id_ != OB_INVALID_TENANT_ID) {
    if (tenant_id_ != MTL_ID()) {
      LOG_ERROR("ls destroy happen in wrong tenant ctx", K(tenant_id_), K(MTL_ID()));
      abort();
    }
  }
  transaction::ObTransService *txs_svr = MTL(transaction::ObTransService *);
  FLOG_INFO("ObLS destroy", K(this), K(*this), K(lbt()));
  if (OB_TMP_FAIL(offline_())) {
    LOG_WARN("ls offline failed.", K(tmp_ret), K(ls_meta_.ls_id_));
  } else if (OB_TMP_FAIL(stop_())) {
    LOG_WARN("ls stop failed.", K(tmp_ret), K(ls_meta_.ls_id_));
  } else {
    wait_();
  }
  UNREGISTER_FROM_LOGSERVICE(logservice::TRANS_SERVICE_LOG_BASE_TYPE, &ls_tx_svr_);
  UNREGISTER_FROM_LOGSERVICE(logservice::STORAGE_SCHEMA_LOG_BASE_TYPE, &ls_tablet_svr_);
  UNREGISTER_FROM_LOGSERVICE(logservice::TABLET_SEQ_SYNC_LOG_BASE_TYPE, &ls_sync_tablet_seq_handler_);
  UNREGISTER_FROM_LOGSERVICE(logservice::DDL_LOG_BASE_TYPE, &ls_ddl_log_handler_);
  UNREGISTER_FROM_LOGSERVICE(logservice::KEEP_ALIVE_LOG_BASE_TYPE, &keep_alive_ls_handler_);
  UNREGISTER_FROM_LOGSERVICE(logservice::GC_LS_LOG_BASE_TYPE, &gc_handler_);
  UNREGISTER_FROM_LOGSERVICE(logservice::RESERVED_SNAPSHOT_LOG_BASE_TYPE, &reserved_snapshot_clog_handler_);
  UNREGISTER_FROM_LOGSERVICE(logservice::MEDIUM_COMPACTION_LOG_BASE_TYPE, &medium_compaction_clog_handler_);
  if (ls_meta_.ls_id_ == IDS_LS) {
    MTL(transaction::ObTransIDService *)->reset_ls();
    MTL(transaction::ObTimestampService *)->reset_ls();
    MTL(sql::ObDASIDService *)->reset_ls();
    UNREGISTER_FROM_LOGSERVICE(logservice::TIMESTAMP_LOG_BASE_TYPE, MTL(transaction::ObTimestampService *));
    UNREGISTER_FROM_LOGSERVICE(logservice::TRANS_ID_LOG_BASE_TYPE, MTL(transaction::ObTransIDService *));
    if (is_user_tenant(MTL_ID())) {
      UNREGISTER_FROM_RESTORESERVICE(logservice::STANDBY_TIMESTAMP_LOG_BASE_TYPE, MTL(transaction::ObStandbyTimestampService *));
    } else {
      UNREGISTER_FROM_LOGSERVICE(logservice::DAS_ID_LOG_BASE_TYPE, MTL(sql::ObDASIDService *));
    }
  }
  if (ls_meta_.ls_id_ == MAJOR_FREEZE_LS) {
    rootserver::ObRestoreMajorFreezeService *restore_major_freeze_service = MTL(rootserver::ObRestoreMajorFreezeService *);
    UNREGISTER_FROM_RESTORESERVICE(logservice::MAJOR_FREEZE_LOG_BASE_TYPE, restore_major_freeze_service);
    rootserver::ObPrimaryMajorFreezeService *primary_major_freeze_service = MTL(rootserver::ObPrimaryMajorFreezeService *);
    UNREGISTER_FROM_LOGSERVICE(logservice::MAJOR_FREEZE_LOG_BASE_TYPE, primary_major_freeze_service);
  }
  if (ls_meta_.ls_id_ == GAIS_LS && OB_SUCC(ret)) {
    UNREGISTER_FROM_LOGSERVICE(logservice::GAIS_LOG_BASE_TYPE, MTL(share::ObGlobalAutoIncService *));
    MTL(share::ObGlobalAutoIncService *)->set_cache_ls(nullptr);
  }
  if (OB_SUCC(ret) && ls_meta_.ls_id_.is_sys_ls()) {
    rootserver::ObPrimaryLSService* ls_service = MTL(rootserver::ObPrimaryLSService*);
    UNREGISTER_FROM_LOGSERVICE(logservice::PRIMARY_LS_SERVICE_LOG_BASE_TYPE, ls_service);
  }

  if (OB_SUCC(ret) && is_user_tenant(MTL_ID()) && ls_meta_.ls_id_.is_sys_ls()) {
    UNREGISTER_FROM_LOGSERVICE(logservice::DATA_DICT_LOG_BASE_TYPE, MTL(datadict::ObDataDictService *));
    rootserver::ObRecoveryLSService* ls_service = MTL(rootserver::ObRecoveryLSService*);
    UNREGISTER_FROM_RESTORESERVICE(logservice::RECOVERY_LS_SERVICE_LOG_BASE_TYPE, ls_service);
  }
  if (OB_SUCC(ret) && !is_user_tenant(MTL_ID()) && ls_meta_.ls_id_.is_sys_ls()) {
    rootserver::ObRestoreService * restore_service = MTL(rootserver::ObRestoreService*);
    UNREGISTER_FROM_LOGSERVICE(logservice::RESTORE_SERVICE_LOG_BASE_TYPE, restore_service);
  }
  tx_table_.destroy();
  lock_table_.destroy();
  ls_tablet_svr_.destroy();
  keep_alive_ls_handler_.destroy();
  // may be not ininted, need bypass remove at txs_svr
  // test case may not init ls and ObTransService may have been destroyed before ls destroy.
  if (OB_ISNULL(txs_svr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tx service is null, may be memory leak", KP(txs_svr));
  //少数派follower在GC时强杀事务需要走非gracefully流程,多数派GC写offline日志时已经判断了事务结束
  } else if (OB_FAIL(txs_svr->remove_ls(ls_meta_.ls_id_, false))) {
    // we may has remove it before.
    LOG_WARN("remove log stream from txs service failed", K(ret), K(ls_meta_.ls_id_));
  }
  gc_handler_.reset();
  ls_restore_handler_.destroy();
  checkpoint_executor_.reset();
  log_handler_.reset_election_priority();
  log_handler_.destroy();
  restore_handler_.destroy();
  ls_meta_.reset();
  ls_sync_tablet_seq_handler_.reset();
  ls_ddl_log_handler_.reset();
  ls_migration_handler_.destroy();
  ls_remove_member_handler_.destroy();
  tablet_gc_handler_.reset();
  reserved_snapshot_mgr_.destroy();
  reserved_snapshot_clog_handler_.reset();
  medium_compaction_clog_handler_.reset();
  ls_recovery_stat_handler_.reset();
  rs_reporter_ = nullptr;
  is_inited_ = false;
  tenant_id_ = OB_INVALID_TENANT_ID;
}

int ObLS::offline_tx_()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(tx_table_.prepare_offline())) {
    LOG_WARN("tx table prepare offline failed", K(ret), K(ls_meta_));
  } else if (OB_FAIL(ls_tx_svr_.offline())) {
    LOG_WARN("offline ls tx service failed", K(ret), K(ls_meta_));
  } else if (OB_FAIL(tx_table_.offline())) {
    LOG_WARN("tx table offline failed", K(ret), K(ls_meta_));
  }
  return ret;
}

int ObLS::offline_compaction_()
{
  int ret = OB_SUCCESS;
  if (FALSE_IT(ls_freezer_.offline())) {
  } else if (OB_FAIL(MTL(ObTenantTabletScheduler *)->
                     check_ls_compaction_finish(ls_meta_.ls_id_))) {
    LOG_WARN("check compaction finish failed", K(ret), K(ls_meta_));
  }
  return ret;
}

int ObLS::offline_()
{
  int ret = OB_SUCCESS;
  // only follower can do this.
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls is not inited", K(ret));
  } else if (FALSE_IT(is_offlined_ = true)) {
  } else if (FALSE_IT(checkpoint_executor_.offline())) {
    LOG_WARN("checkpoint executor offline failed", K(ret), K(ls_meta_));
  } else if (OB_FAIL(ls_restore_handler_.offline())) {
    LOG_WARN("failed to offline ls restore handler", K(ret));
  } else if (OB_FAIL(offline_compaction_())) {
    LOG_WARN("compaction offline failed", K(ret), K(ls_meta_));
  } else if (OB_FAIL(ls_wrs_handler_.offline())) {
    LOG_WARN("weak read handler offline failed", K(ret), K(ls_meta_));
  } else if (OB_FAIL(log_handler_.offline())) {
    LOG_WARN("failed to offline log", K(ret));
  } else if (OB_FAIL(ls_ddl_log_handler_.offline())) {
    LOG_WARN("ddl log handler offline failed", K(ret), K(ls_meta_));
  } else if (OB_FAIL(offline_tx_())) {
    LOG_WARN("offline tx service failed", K(ret), K(ls_meta_));
  } else if (OB_FAIL(lock_table_.offline())) {
    LOG_WARN("lock table offline failed", K(ret), K(ls_meta_));
  } else if (OB_FAIL(ls_tablet_svr_.offline())) {
    LOG_WARN("tablet service offline failed", K(ret), K(ls_meta_));
  } else if (OB_FAIL(tablet_gc_handler_.offline())) {
    LOG_WARN("tablet gc handler offline failed", K(ret), K(ls_meta_));
  } else {
    // do nothing
  }

  return ret;
}

int ObLS::offline()
{
  int ret = OB_SUCCESS;
  int64_t read_lock = 0;
  int64_t write_lock = LSLOCKALL;
  int64_t start_ts = ObTimeUtility::current_time();
  int64_t retry_times = 0;

  do {
    retry_times++;
    {
      ObLSLockGuard lock_myself(lock_, read_lock, write_lock);
      // only follower can do this.
      if (OB_FAIL(offline_())) {
        LOG_WARN("ls offline failed", K(ret), K(ls_meta_));
      }
    }
    if (OB_EAGAIN == ret) {
      ob_usleep(100 * 1000); // 100 ms
      if (retry_times % 100 == 0) { // every 10 s
        LOG_WARN_RET(OB_ERR_TOO_MUCH_TIME, "ls offline use too much time.", K(ls_meta_), K(start_ts));
      }
    }
  } while (OB_EAGAIN == ret);
  FLOG_INFO("ls offline end", KR(ret), "ls_id", get_ls_id());
  return ret;
}

int ObLS::offline_without_lock()
{
  int ret = OB_SUCCESS;
  int64_t start_ts = ObTimeUtility::current_time();
  int64_t retry_times = 0;

  do {
    retry_times++;
    {
      if (OB_FAIL(offline_())) {
        LOG_WARN("ls offline failed", K(ret), K(ls_meta_));
      }
    }
    if (OB_EAGAIN == ret) {
      ob_usleep(100 * 1000); // 100 ms
      if (retry_times % 100 == 0) { // every 10 s
        LOG_WARN_RET(OB_ERR_TOO_MUCH_TIME, "ls offline use too much time.", K(ls_meta_), K(start_ts));
      }
    }
  } while (OB_EAGAIN == ret);
  FLOG_INFO("ls offline end", KR(ret), "ls_id", get_ls_id());
  return ret;
}

int ObLS::online_tx_()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ls_tx_svr_.online())) {
    LOG_WARN("ls tx service online failed", K(ret), K(ls_meta_));
  } else if (OB_FAIL(tx_table_.online())) {
    LOG_WARN("tx table online failed", K(ret), K(ls_meta_));
  }
  return ret;
}

int ObLS::online_compaction_()
{
  int ret = OB_SUCCESS;
  ls_freezer_.online();
  return ret;
}

int ObLS::online()
{
  int ret = OB_SUCCESS;
  int64_t read_lock = 0;
  int64_t write_lock = LSLOCKALL;
  ObLSLockGuard lock_myself(lock_, read_lock, write_lock);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls is not inited", K(ret));
  } else if (OB_FAIL(ls_tablet_svr_.online())) {
    LOG_WARN("tablet service online failed", K(ret), K(ls_meta_));
  } else if (OB_FAIL(lock_table_.online())) {
    LOG_WARN("lock table online failed", K(ret), K(ls_meta_));
  } else if (OB_FAIL(online_tx_())) {
    LOG_WARN("ls tx online failed", K(ret), K(ls_meta_));
  } else if (OB_FAIL(ls_ddl_log_handler_.online())) {
    LOG_WARN("ddl log handler online failed", K(ret), K(ls_meta_));
  } else if (OB_FAIL(log_handler_.online(ls_meta_.get_clog_base_lsn(),
                                         ls_meta_.get_clog_checkpoint_scn()))) {
    LOG_WARN("failed to online log", K(ret));
  } else if (OB_FAIL(ls_wrs_handler_.online())) {
    LOG_WARN("weak read handler online failed", K(ret), K(ls_meta_));
  } else if (OB_FAIL(online_compaction_())) {
    LOG_WARN("compaction online failed", K(ret), K(ls_meta_));
  } else if (OB_FAIL(ls_restore_handler_.online())) {
    LOG_WARN("ls restore handler online failed", K(ret));
  } else if (FALSE_IT(checkpoint_executor_.online())) {
  } else if (FALSE_IT(tablet_gc_handler_.online())) {
  } else {
    is_offlined_ = false;
    // do nothing
  }

  FLOG_INFO("ls online end", KR(ret), "ls_id", get_ls_id());
  return ret;
}

int ObLS::enable_for_restore()
{
  int ret = OB_SUCCESS;
  int64_t read_lock = 0;
  int64_t write_lock = LSLOCKALL;
  ObLSLockGuard lock_myself(lock_, read_lock, write_lock);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls is not inited", K(ret));
  } else if (OB_FAIL(log_handler_.enable_sync())) {
    LOG_WARN("failed to enable sync", K(ret));
  } else if (OB_FAIL(ls_restore_handler_.online())) {
    LOG_WARN("failed to online restore", K(ret));
  }
  return ret;
}

int ObLS::get_ls_meta_package(const bool check_archive, ObLSMetaPackage &meta_package)
{
  int ret = OB_SUCCESS;
  palf::LSN begin_lsn;
  palf::LSN archive_lsn;
  SCN unused_archive_scn;
  bool archive_force = false;
  bool archive_ignore = false;
  const ObLSID &id = get_ls_id();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls is not inited", K(ret));
  } else {
    meta_package.ls_meta_ = ls_meta_;
    palf::LSN curr_lsn = meta_package.ls_meta_.get_clog_base_lsn();
    if (! check_archive) {
      LOG_TRACE("no need check archive", K(id), K(check_archive));
    } else if (OB_FAIL(MTL(archive::ObArchiveService*)->get_ls_archive_progress(
            id, archive_lsn, unused_archive_scn, archive_force, archive_ignore))) {
      LOG_WARN("get ls archive progress failed", K(ret), K(id));
    } else if (archive_ignore) {
      LOG_TRACE("just ignore archive", K(id), K(archive_ignore));
    } else if (archive_lsn >= curr_lsn) {
      LOG_TRACE("archive_lsn not smaller than curr_lsn", K(id), K(archive_lsn), K(curr_lsn));
    } else if (OB_FAIL(log_handler_.get_begin_lsn(begin_lsn))) {
      LOG_WARN("get begin lsn failed", K(ret), K(id));
    } else if (begin_lsn > archive_lsn) {
      if (archive_force) {
        ret = OB_FILE_RECYCLED;
        LOG_WARN("archive in mandatory mode and log recycled", K(ret));
      } else {
        curr_lsn = std::min(archive_lsn, curr_lsn);
      }
    }

    if (OB_SUCC(ret) && OB_FAIL(log_handler_.get_palf_base_info(curr_lsn,
                                            meta_package.palf_meta_))) {
      LOG_WARN("get palf base info failed", K(ret), K(id), K(curr_lsn),
          K(archive_force), K(archive_ignore), K(archive_lsn), K_(ls_meta));
    }
  }
  return ret;
}

int ObLS::set_ls_meta(const ObLSMeta &ls_meta)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls is not inited", K(ret));
  } else {
    ls_meta_ = ls_meta;
    if (IDS_LS == ls_meta_.ls_id_) {
      ObAllIDMeta all_id_meta;
      if (OB_FAIL(ls_meta_.get_all_id_meta(all_id_meta))) {
        LOG_WARN("get all id meta failed", K(ret), K(ls_meta_));
      } else if (OB_FAIL(ObIDService::update_id_service(all_id_meta))) {
        LOG_WARN("update id service fail", K(ret), K(all_id_meta), K(*this));
      }
    }
  }
  return ret;
}

int ObLS::get_ls_meta(ObLSMeta &ls_meta) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls is not inited", K(ret));
  } else {
    ls_meta = ls_meta_;
  }
  return ret;
}

int ObLS::save_base_schema_version()
{
  // TODO: yanyuan.cxf
  int ret = OB_SUCCESS;
  return ret;
}

int ObLS::get_replica_status(ObReplicaStatus &replica_status)
{
  int ret = OB_SUCCESS;
  // Todo: need to get migration status
  replica_status = REPLICA_STATUS_NORMAL;
  return ret;
}

int ObLS::get_ls_role(ObRole &role)
{
  int ret = OB_SUCCESS;
  role = INVALID_ROLE;
  int64_t proposal_id = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls is not inited", K(ret));
  } else {
    int64_t read_lock = LSLOCKLOG;
    int64_t write_lock = 0;
    ObLSLockGuard lock_myself(lock_, read_lock, write_lock);
    if (OB_FAIL(log_handler_.get_role(role, proposal_id))) {
      LOG_WARN("get ls role failed", K(ret), KPC(this));
    }
  }
  return ret;
}

int ObLS::try_sync_reserved_snapshot(
    const int64_t new_reserved_snapshot,
    const bool update_flag)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls is not inited", K(ret));
  } else {
    ObRole role = INVALID_ROLE;
    int64_t proposal_id = 0;
    if (is_stopped_) {
      // do nothing
    } else if (OB_FAIL(log_handler_.get_role(role, proposal_id))) {
      LOG_WARN("get ls role failed", K(ret), KPC(this));
    } else if (LEADER != role) {
      // do nothing
    } else {
      ret = reserved_snapshot_mgr_.try_sync_reserved_snapshot(new_reserved_snapshot, update_flag);
    }
  }
  return ret;
}

int ObLS::get_ls_info(ObLSVTInfo &ls_info)
{
  int ret = OB_SUCCESS;
  ObRole role;
  int64_t proposal_id = 0;
  bool is_log_sync = false;
  bool is_need_rebuild = false;
  ObMigrationStatus migrate_status;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls is not inited", K(ret));
  } else if (OB_FAIL(log_handler_.get_role(role, proposal_id))) {
    LOG_WARN("get ls role failed", K(ret), KPC(this));
  } else if (OB_FAIL(log_handler_.is_in_sync(is_log_sync,
                                             is_need_rebuild))) {
    LOG_WARN("get ls need rebuild info failed", K(ret), KPC(this));
  } else if (OB_FAIL(ls_meta_.get_migration_status(migrate_status))) {
    LOG_WARN("get ls migrate status failed", K(ret), KPC(this));
  } else {
    ls_info.ls_id_ = ls_meta_.ls_id_;
    ls_info.replica_type_ = ls_meta_.replica_type_;
    ls_info.ls_state_ = role;
    ls_info.migrate_status_ = migrate_status;
    ls_info.tablet_count_ = ls_tablet_svr_.get_tablet_count();
    ls_info.weak_read_scn_ = ls_wrs_handler_.get_ls_weak_read_ts();
    ls_info.need_rebuild_ = is_need_rebuild;
    ls_info.checkpoint_scn_ = ls_meta_.get_clog_checkpoint_scn();
    ls_info.checkpoint_lsn_ = ls_meta_.get_clog_base_lsn().val_;
    ls_info.rebuild_seq_ = ls_meta_.get_rebuild_seq();
  }
  return ret;
}

int ObLS::report_replica_info()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls is not inited", K(ret));
  } else if (OB_FAIL(rs_reporter_->submit_ls_update_task(ls_meta_.tenant_id_,
                                                         ls_meta_.ls_id_))) {
    LOG_WARN("report ls info failed", K(ret));
  } else {
    LOG_INFO("submit log stream update task", K(ls_meta_.tenant_id_), K(ls_meta_.ls_id_));
    // do nothing.
  }
  return ret;
}

int ObLS::ObLSInnerTabletIDIter::get_next(common::ObTabletID  &tablet_id)
{
  int ret = OB_SUCCESS;
  if (pos_ >= TOTAL_INNER_TABLET_NUM) {
    ret = OB_ITER_END;
  } else {
    tablet_id = INNER_TABLET_ID_LIST[pos_++];
  }
  return ret;
}

int ObLS::block_tx_start()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls is not inited", K(ret));
  } else {
    int64_t read_lock = 0;
    int64_t write_lock = LSLOCKSTORAGE | LSLOCKTX;
    ObLSLockGuard lock_myself(lock_, read_lock, write_lock);
    // protect with lock_ to make sure there is no tablet transfer in process doing.
    // transfer in must use this lock too.
    if (OB_FAIL(block_tx())) {
      LOG_WARN("block_tx failed", K(ret), K(get_ls_id()));
    }
  }
  return ret;
}

int ObLS::tablet_transfer_in(const ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls is not inited", K(ret));
  } else {
    int64_t read_lock = 0;
    int64_t write_lock = LSLOCKSTORAGE;
    ObLSLockGuard lock_myself(lock_, read_lock, write_lock);
    // make sure there is no block on going.
    logservice::LSGCState gc_state = logservice::LSGCState::INVALID_LS_GC_STATE;
    if (OB_FAIL(get_gc_state(gc_state))) {
      LOG_WARN("get_gc_state failed", K(ret), K(get_ls_id()));
    } else if (OB_UNLIKELY(logservice::LSGCState::LS_BLOCKED == gc_state)) {
      ret = OB_OP_NOT_ALLOW;
      LOG_WARN("ls is blocked, cannot transfer in", K(ret), K(tablet_id));
    } else {
      // TODO: yanyuan.cxf transfer in a tablet.
    }
  }
  return ret;
}

int ObLS::update_tablet_table_store(
    const ObTabletID &tablet_id,
    const ObUpdateTableStoreParam &param,
    ObTabletHandle &handle)
{
  int ret = OB_SUCCESS;
  const int64_t read_lock = LSLOCKLOGMETA;
  const int64_t write_lock = 0;
  ObLSLockGuard lock_myself(lock_, read_lock, write_lock);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls is not inited", K(ret));
  } else if (OB_UNLIKELY(!tablet_id.is_valid() || !param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("update tablet table store get invalid argument", K(ret), K(tablet_id), K(param));
  } else {
    const int64_t rebuild_seq = ls_meta_.get_rebuild_seq();
    if (param.rebuild_seq_ != rebuild_seq) {
      ret = OB_EAGAIN;
      LOG_WARN("update tablet table store rebuild seq not same, need retry",
          K(ret), K(tablet_id), K(rebuild_seq), K(param));
    } else if (OB_FAIL(ls_tablet_svr_.update_tablet_table_store(tablet_id, param, handle))) {
      LOG_WARN("failed to update tablet table store", K(ret), K(tablet_id), K(param));
    }
  }
  return ret;
}

int ObLS::update_tablet_table_store(
    const int64_t rebuild_seq,
    const ObTabletHandle &old_tablet_handle,
    const ObIArray<ObTableHandleV2> &table_handles)
{
  int ret = OB_SUCCESS;
  const int64_t read_lock = LSLOCKLOGMETA;
  const int64_t write_lock = 0;
  ObLSLockGuard lock_myself(lock_, read_lock, write_lock);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls hasn't been inited", K(ret));
  } else if (OB_UNLIKELY(!old_tablet_handle.is_valid() || 0 == table_handles.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(old_tablet_handle), K(table_handles));
  } else {
    const int64_t seq = ls_meta_.get_rebuild_seq();
    if (rebuild_seq != seq) {
      ret = OB_EAGAIN;
      LOG_WARN("rebuild seq has changed, retry", K(ret), K(seq), K(rebuild_seq));
    } else if (OB_FAIL(ls_tablet_svr_.update_tablet_table_store(old_tablet_handle, table_handles))) {
      LOG_WARN("fail to replace small sstables in the tablet", K(ret), K(old_tablet_handle), K(table_handles));
    }
  }
  return ret;
}

int ObLS::build_ha_tablet_new_table_store(
    const ObTabletID &tablet_id,
    const ObBatchUpdateTableStoreParam &param)
{
  int ret = OB_SUCCESS;
  int64_t read_lock = LSLOCKLOGMETA;
  int64_t write_lock = 0;
  ObLSLockGuard lock_myself(lock_, read_lock, write_lock);
  const int64_t rebuild_seq = ls_meta_.get_rebuild_seq();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls is not inited", K(ret));
  } else if (!tablet_id.is_valid() || !param.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("build ha tablet new table store get invalid argument", K(ret), K(tablet_id), K(param));
  } else {
    if (param.rebuild_seq_ != rebuild_seq) {
      ret = OB_EAGAIN;
      LOG_WARN("build ha tablet new table store rebuild seq not same, need retry",
          K(ret), K(tablet_id), K(rebuild_seq), K(param));
    } else if (OB_FAIL(ls_tablet_svr_.build_ha_tablet_new_table_store(tablet_id, param))) {
      LOG_WARN("failed to update tablet table store", K(ret), K(tablet_id), K(param));
    }
  }
  return ret;
}

int ObLS::finish_slog_replay()
{
  int ret = OB_SUCCESS;
  ObMigrationStatus current_migration_status;
  ObMigrationStatus new_migration_status;
  int64_t read_lock = 0;
  int64_t write_lock = LSLOCKALL - LSLOCKLOGMETA;
  ObLSLockGuard lock_myself(lock_, read_lock, write_lock);

  if (OB_FAIL(get_migration_status(current_migration_status))) {
    LOG_WARN("failed to get migration status", K(ret), KPC(this));
  } else if (OB_FAIL(ObMigrationStatusHelper::trans_fail_status(current_migration_status,
                                                                new_migration_status))) {
    LOG_WARN("failed to trans fail status", K(ret), K(current_migration_status),
             K(new_migration_status));
  } else if (can_update_ls_meta(ls_meta_.get_ls_create_status()) &&
             OB_FAIL(ls_meta_.set_migration_status(new_migration_status, false /*no need write slog*/))) {
    LOG_WARN("failed to set migration status", K(ret), K(new_migration_status));
  } else if (is_need_gc()) {
    LOG_INFO("this ls should be gc later", KPC(this));
    // ls will be gc later and tablets in the ls are not complete,
    // so skip the following steps, otherwise load_ls_inner_tablet maybe encounter error.
  } else if (OB_FAIL(start())) {
    LOG_WARN("ls can not start to work", K(ret));
  } else if (is_enable_for_restore()) {
    if (OB_FAIL(offline_())) {
      LOG_WARN("failed to offline", K(ret), KPC(this));
    } else if (OB_FAIL(log_handler_.enable_sync())) {
      LOG_WARN("failed to enable sync", K(ret), KPC(this));
    } else if (OB_FAIL(ls_restore_handler_.online())) {
      LOG_WARN("failed to online ls restore handler", K(ret), KPC(this));
    }
  } else if (OB_FAIL(load_ls_inner_tablet())) {
    LOG_WARN("ls load inner tablet failed", K(ret), KPC(this));
  } else {
    // do nothing
  }
  return ret;
}

int ObLS::replay_get_tablet(const common::ObTabletID &tablet_id,
                            const SCN &scn,
                            ObTabletHandle &handle) const
{
  int ret = OB_SUCCESS;
  const ObTabletMapKey key(ls_meta_.ls_id_, tablet_id);
  const SCN tablet_change_checkpoint_scn = ls_meta_.get_tablet_change_checkpoint_scn();
  ObTabletHandle tablet_handle;
  SCN max_scn;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls is not inited", KR(ret));
  } else if (OB_UNLIKELY(!tablet_id.is_valid() || !scn.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(tablet_id), K(scn));
  } else if (OB_FAIL(ObTabletCreateDeleteHelper::get_tablet(key, tablet_handle))) {
    if (OB_TABLET_NOT_EXIST != ret) {
      LOG_WARN("failed to get tablet", K(ret), K(key));
    } else if (scn <= tablet_change_checkpoint_scn) {
      LOG_WARN("tablet already gc", K(ret), K(key), K(scn), K(tablet_change_checkpoint_scn));
    } else if (OB_FAIL(MTL(ObLogService*)->get_log_replay_service()->get_max_replayed_scn(ls_meta_.ls_id_, max_scn))) {
      LOG_WARN("failed to get_max_replayed_scn", KR(ret), K_(ls_meta), K(scn), K(tablet_id));
    }
    // double check for this scenario:
    // 1. get_tablet return OB_TABLET_NOT_EXIST
    // 2. create tablet
    // 3. get_max_replayed_scn > scn
    else if (OB_FAIL(ObTabletCreateDeleteHelper::get_tablet(key, tablet_handle))) {
      if (OB_TABLET_NOT_EXIST != ret) {
        LOG_WARN("failed to get tablet", K(ret), K(key));
      } else if (!max_scn.is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("max_scn is invalid", KR(ret), K(key), K(scn), K(tablet_change_checkpoint_scn));
      } else if (scn > SCN::scn_inc(max_scn)) {
        ret = OB_EAGAIN;
        LOG_INFO("tablet does not exist, but need retry", KR(ret), K(key), K(scn), K(tablet_change_checkpoint_scn), K(max_scn));
      } else {
        LOG_INFO("tablet already gc, but scn is more than tablet_change_checkpoint_scn", KR(ret), K(key), K(scn), K(tablet_change_checkpoint_scn), K(max_scn));
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else {
    ObTabletTxMultiSourceDataUnit tx_data;
    if (OB_FAIL(tablet_handle.get_obj()->get_tx_data(tx_data, false/*check_valid*/))) {
      LOG_WARN("failed to get tablet tx data", KR(ret), K(tablet_handle));
    } else if (ObTabletStatus::CREATING == tx_data.tablet_status_) {
      ret = OB_EAGAIN;
      LOG_INFO("tablet is CREATING, need retry", KR(ret), K(key), K(tx_data), K(scn));
    } else if (ObTabletStatus::NORMAL == tx_data.tablet_status_) {
      // do nothing
    } else if (ObTabletStatus::DELETING == tx_data.tablet_status_) {
      LOG_INFO("tablet is DELETING, just continue", KR(ret), K(key), K(tx_data), K(scn));
    } else if (ObTabletStatus::DELETED == tx_data.tablet_status_) {
      ret = OB_TABLET_NOT_EXIST;
      LOG_INFO("tablet is already deleted", KR(ret), K(key), K(tx_data), K(scn));
    } else {
      ret = OB_EAGAIN;
      LOG_INFO("tablet may be in creating procedure", KR(ret), K(key), K(tx_data), K(scn));
    }
  }

  if (OB_SUCC(ret)) {
    handle = tablet_handle;
  }

  return ret;
}

int ObLS::logstream_freeze(bool is_sync)
{
  int ret = OB_SUCCESS;
  ObFuture<int> result;

  {
    int64_t read_lock = LSLOCKALL - LSLOCKLOGMETA;
    int64_t write_lock = 0;
    ObLSLockGuard lock_myself(lock_, read_lock, write_lock);
    if (IS_NOT_INIT) {
      ret = OB_NOT_INIT;
      LOG_WARN("ls is not inited", K(ret));
    } else if (OB_UNLIKELY(is_stopped_)) {
      ret = OB_NOT_RUNNING;
      LOG_WARN("ls stopped", K(ret), K_(ls_meta));
    } else if (OB_UNLIKELY(!log_handler_.is_replay_enabled())) {
      ret = OB_NOT_RUNNING;
      LOG_WARN("log handler not enable replay, should not freeze", K(ret), K_(ls_meta));
    } else if (OB_FAIL(ls_freezer_.logstream_freeze(&result))) {
      LOG_WARN("logstream freeze failed", K(ret), K_(ls_meta));
    } else {
      // do nothing
    }
  }

  if (is_sync) {
    ret = ls_freezer_.wait_freeze_finished(result);
  }

  return ret;
}

int ObLS::tablet_freeze(const ObTabletID &tablet_id, bool is_sync)
{
  int ret = OB_SUCCESS;
  ObFuture<int> result;

  {
    int64_t read_lock = LSLOCKALL - LSLOCKLOGMETA;
    int64_t write_lock = 0;
    ObLSLockGuard lock_myself(lock_, read_lock, write_lock);
    if (IS_NOT_INIT) {
      ret = OB_NOT_INIT;
      LOG_WARN("ls is not inited", K(ret));
    } else if (OB_UNLIKELY(is_stopped_)) {
      ret = OB_NOT_RUNNING;
      LOG_WARN("ls stopped", K(ret), K_(ls_meta));
    } else if (OB_UNLIKELY(!log_handler_.is_replay_enabled())) {
      ret = OB_NOT_RUNNING;
      LOG_WARN("log handler not enable replay, should not freeze", K(ret), K(tablet_id), K_(ls_meta));
    } else if (OB_FAIL(ls_freezer_.tablet_freeze(tablet_id, &result))) {
      LOG_WARN("tablet freeze failed", K(ret), K(tablet_id));
    } else {
      // do nothing
    }
  }

  if (is_sync) {
    ret = ls_freezer_.wait_freeze_finished(result);
  }

  return ret;
}

int ObLS::force_tablet_freeze(const ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  int64_t read_lock = LSLOCKALL - LSLOCKLOGMETA;
  int64_t write_lock = 0;
  ObLSLockGuard lock_myself(lock_, read_lock, write_lock);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls is not inited", K(ret));
  } else if (OB_UNLIKELY(is_stopped_)) {
    ret = OB_NOT_RUNNING;
    LOG_WARN("ls stopped", K(ret), K_(ls_meta));
  } else if (OB_UNLIKELY(!log_handler_.is_replay_enabled())) {
    ret = OB_NOT_RUNNING;
    LOG_WARN("log handler not enable replay, should not freeze", K(ret), K(tablet_id), K_(ls_meta));
  } else if (OB_FAIL(ls_freezer_.force_tablet_freeze(tablet_id))) {
    LOG_WARN("tablet force freeze failed", K(ret), K(tablet_id));
  } else {
    // do nothing
  }
  return ret;
}

int ObLS::advance_checkpoint_by_flush(SCN recycle_scn)
{
  int64_t read_lock = LSLOCKALL;
  int64_t write_lock = 0;
  ObLSLockGuard lock_myself(lock_, read_lock, write_lock);
  return checkpoint_executor_.advance_checkpoint_by_flush(recycle_scn);
}

int ObLS::get_ls_meta_package_and_tablet_ids(const bool check_archive,
    ObLSMetaPackage &meta_package,
    common::ObIArray<common::ObTabletID> &tablet_ids)
{
  int ret = OB_SUCCESS;
  int64_t read_lock = LSLOCKLOGMETA;
  int64_t write_lock = 0;
  const bool need_initial_state = false;
  ObHALSTabletIDIterator iter(ls_meta_.ls_id_, need_initial_state);
  ObLSLockGuard lock_myself(lock_, read_lock, write_lock);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls is not inited", K(ret));
  } else if (OB_UNLIKELY(is_stopped_)) {
    ret = OB_NOT_RUNNING;
    LOG_WARN("ls stopped", K(ret), K_(ls_meta));
  } else if (OB_FAIL(get_ls_meta_package(check_archive, meta_package))) {
    LOG_WARN("failed to get ls meta package", K(ret), K_(ls_meta));
  } else if (OB_FAIL(ls_tablet_svr_.build_tablet_iter(iter))) {
    LOG_WARN("failed to build tablet iter", K(ret), K_(ls_meta));
  } else {
    tablet_ids.reset();
    common::ObTabletID tablet_id;
    while (OB_SUCC(ret)) {
      if (OB_FAIL(iter.get_next_tablet_id(tablet_id))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          break;
        } else {
          LOG_WARN("failed to get tablet id", K(ret));
        }
      } else if (OB_FAIL(tablet_ids.push_back(tablet_id))) {
        LOG_WARN("failed to push tablet id into array", K(ret), K(tablet_id));
      }
    }
  }
  return ret;
}

int ObLS::disable_sync()
{
  int ret = OB_SUCCESS;
  int64_t read_lock = LSLOCKLS;
  int64_t write_lock = LSLOCKLOG;
  ObLSLockGuard lock_myself(lock_, read_lock, write_lock);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls is not inited", K(ret));
  } else if (OB_FAIL(log_handler_.disable_sync())) {
    LOG_WARN("disable sync failed", K(ret), K_(ls_meta));
  } else {
    LOG_INFO("disable sync successfully", K(ret), K_(ls_meta));
  }
  return ret;
}

int ObLS::enable_replay()
{
  int ret = OB_SUCCESS;
  int64_t read_lock = LSLOCKLS;
  int64_t write_lock = LSLOCKLOG;
  ObLSLockGuard lock_myself(lock_, read_lock, write_lock);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls is not inited", K(ret));
  } else if (OB_FAIL(log_handler_.enable_replay(ls_meta_.get_clog_base_lsn(),
                                                ls_meta_.get_clog_checkpoint_scn()))) {
    LOG_WARN("failed to enable replay", K(ret));
  }
  return ret;
}

int ObLS::enable_replay_without_lock()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls is not inited", K(ret));
  } else if (OB_FAIL(log_handler_.enable_replay(ls_meta_.get_clog_base_lsn(),
                                                ls_meta_.get_clog_checkpoint_scn()))) {
    LOG_WARN("enable replay without lock failed", K(ret), K_(ls_meta));
  } else {
    LOG_INFO("enable replay without lock successfully", K(ret), K_(ls_meta));
  }
  return ret;
}

int ObLS::disable_replay()
{
  int ret = OB_SUCCESS;
  int64_t read_lock = LSLOCKLS;
  int64_t write_lock = LSLOCKLOG;
  ObLSLockGuard lock_myself(lock_, read_lock, write_lock);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls is not inited", K(ret));
  } else if (OB_FAIL(log_handler_.disable_replay())) {
    LOG_WARN("disable replay failed", K(ret), K_(ls_meta));
  } else {
    LOG_INFO("disable replay successfully", K(ret), K_(ls_meta));
  }
  return ret;
}

int ObLS::disable_replay_without_lock()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls is not inited", K(ret));
  } else if (OB_FAIL(log_handler_.disable_replay())) {
    LOG_WARN("disable replay without lock failed", K(ret), K_(ls_meta));
  } else {
    LOG_INFO("disable replay without lock successfully", K(ret), K_(ls_meta));
  }
  return ret;
}

int ObLS::flush_if_need(const bool need_flush)
{
  int ret = OB_SUCCESS;

  int64_t read_lock = LSLOCKALL - LSLOCKLOGMETA;
  int64_t write_lock = 0;
  ObLSLockGuard lock_myself(lock_, read_lock, write_lock);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls is not inited", K(ret));
  } else if (OB_UNLIKELY(is_stopped_)) {
    ret = OB_NOT_RUNNING;
    LOG_WARN("ls stopped", K(ret), K_(ls_meta));
  } else if (OB_UNLIKELY(!log_handler_.is_replay_enabled())) {
    ret = OB_NOT_RUNNING;
    LOG_WARN("log handler not enable replay, should not freeze", K(ret), K_(ls_meta));
  } else if (OB_FAIL(flush_if_need_(need_flush))) {
    LOG_WARN("flush if need failed", K(ret), K_(ls_meta));
  } else {
    // do nothing
  }
  return ret;
}

int ObLS::flush_if_need_(const bool need_flush)
{
  int ret = OB_SUCCESS;
  SCN clog_checkpoint_scn = get_clog_checkpoint_scn();
  if (!need_flush) {
    STORAGE_LOG(INFO, "the ls no need flush to advance_checkpoint",
                K(get_ls_id()),
                K(need_flush));
  } else if (OB_FAIL(checkpoint_executor_.advance_checkpoint_by_flush())) {
    STORAGE_LOG(WARN, "advance_checkpoint_by_flush failed", KR(ret), K(get_ls_id()));
  }
  return ret;
}

int ObLS::try_update_uppder_trans_version()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  int64_t read_lock = LSLOCKLOGMETA;
  int64_t write_lock = 0;
  ObLSLockGuard lock_myself(lock_, read_lock, write_lock);

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls is not inited", K(ret));
  } else if (OB_UNLIKELY(is_stopped_)) {
    ret = OB_NOT_RUNNING;
    LOG_WARN("ls stopped", K(ret), K_(ls_meta));
  } else {
    ObLSTabletIterator tablet_iter(ObTabletCommon::DIRECT_GET_COMMITTED_TABLET_TIMEOUT_US);
    ObTabletHandle tablet_handle;
    bool is_updated = false;
    ObMigrationStatus migration_status;

    if (OB_FAIL(get_migration_status(migration_status))) {
      LOG_WARN("failed to get migration status", K(ret), KPC(this));
    } else if (ObMigrationStatus::OB_MIGRATION_STATUS_NONE != migration_status) {
      //no need update upper trans version
    } else if (OB_FAIL(build_tablet_iter(tablet_iter))) {
      LOG_WARN("failed to build ls tablet iter", K(ret), KPC(this));
    } else {
      while (OB_SUCC(ret)) {
        if (OB_FAIL(tablet_iter.get_next_tablet(tablet_handle))) {
          if (OB_ITER_END != ret) {
            LOG_WARN("failed to get tablet", K(ret), K(ls_id), K(tablet_handle));
          } else {
            ret = OB_SUCCESS;
            break;
          }
        } else if (OB_UNLIKELY(!tablet_handle.is_valid())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid tablet handle", K(ret), K(ls_id), K(tablet_handle));
        } else if (!tablet_handle.get_obj()->get_tablet_meta().ha_status_.is_data_status_complete()) {
          //no need update upper trans version
        } else if (OB_TMP_FAIL(tablet_handle.get_obj()->update_upper_trans_version(*this, is_updated))) {
          LOG_WARN("failed to update upper trans version", K(tmp_ret), "tablet meta", tablet_handle.get_obj()->get_tablet_meta());
        }
      }
    }
  }
  return ret;
}

int ObLS::set_tablet_change_checkpoint_scn(const SCN &scn)
{
  int ret = OB_SUCCESS;
  int64_t read_lock = 0;
  int64_t write_lock = LSLOCKLOGMETA;
  const bool try_lock = true; // the upper layer should deal with try lock fail.
  ObLSLockGuard lock_myself(lock_, read_lock, write_lock, try_lock);
  if (!lock_myself.locked()) {
    ret = OB_EAGAIN;
    LOG_WARN("try lock failed, please retry later", K(ret), K(ls_meta_));
  } else if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls is not inited", K(ret), K(ls_meta_));
  } else if (OB_UNLIKELY(is_stopped_)) {
    ret = OB_NOT_RUNNING;
    LOG_WARN("ls stopped", K(ret), K_(ls_meta));
  } else if (OB_FAIL(ls_meta_.set_tablet_change_checkpoint_scn(scn))) {
    LOG_WARN("fail to set tablet_change_checkpoint_ts", K(ret), K(scn), K_(ls_meta));
  } else {
    // do nothing
  }
  return ret;
}

int ObLS::update_ls_meta(const bool update_restore_status,
                         const ObLSMeta &src_ls_meta)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls is not inited", K(ret), K(ls_meta_));
  } else if (OB_UNLIKELY(is_stopped_)) {
    ret = OB_NOT_RUNNING;
    LOG_WARN("ls stopped", K(ret), K_(ls_meta));
  } else if (OB_FAIL(ls_meta_.update_ls_meta(update_restore_status, src_ls_meta))) {
    LOG_WARN("update ls meta fail", K(ret), K_(ls_meta), K(update_restore_status), K(src_ls_meta));
  } else if (IDS_LS == ls_meta_.ls_id_) {
    ObAllIDMeta all_id_meta;
    if (OB_FAIL(ls_meta_.get_all_id_meta(all_id_meta))) {
      LOG_WARN("get all id meta failed", K(ret), K(ls_meta_));
    } else if (OB_FAIL(ObIDService::update_id_service(all_id_meta))) {
      LOG_WARN("update id service fail", K(ret), K(all_id_meta), K(*this));
    }
  } else {
    // do nothing
  }

  return ret;
}

int ObLS::diagnose(DiagnoseInfo &info) const
{
  int ret = OB_SUCCESS;
  logservice::ObLogService *log_service = MTL(logservice::ObLogService *);
  share::ObLSID ls_id = get_ls_id();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ls is not inited", K(ret));
  } else if (FALSE_IT(info.ls_id_ = ls_id.id()) ||
             FALSE_IT(info.rc_diagnose_info_.id_ = ls_id.id())) {
  } else if (OB_FAIL(gc_handler_.diagnose(info.gc_diagnose_info_))) {
    STORAGE_LOG(WARN, "diagnose gc failed", K(ret), K(ls_id));
  } else if (OB_FAIL(checkpoint_executor_.diagnose(info.checkpoint_diagnose_info_))) {
    STORAGE_LOG(WARN, "diagnose checkpoint failed", K(ret), K(ls_id));
  } else if (OB_FAIL(log_service->diagnose_apply(ls_id, info.apply_diagnose_info_))) {
    STORAGE_LOG(WARN, "diagnose apply failed", K(ret), K(ls_id));
  } else if (OB_FAIL(log_service->diagnose_replay(ls_id, info.replay_diagnose_info_))) {
    STORAGE_LOG(WARN, "diagnose replay failed", K(ret), K(ls_id));
  } else if (OB_FAIL(log_handler_.diagnose(info.log_handler_diagnose_info_))) {
    STORAGE_LOG(WARN, "diagnose log handler failed", K(ret), K(ls_id));
  } else if (OB_FAIL(log_handler_.diagnose_palf(info.palf_diagnose_info_))) {
    STORAGE_LOG(WARN, "diagnose palf failed", K(ret), K(ls_id));
  } else if (info.is_role_sync()) {
    // 角色同步时不需要诊断role change service
    info.rc_diagnose_info_.state_ = logservice::TakeOverState::TAKE_OVER_FINISH;
    info.rc_diagnose_info_.log_type_ = logservice::ObLogBaseType::INVALID_LOG_BASE_TYPE;
  } else if (OB_FAIL(log_service->diagnose_role_change(info.rc_diagnose_info_))) {
    // election, palf, log handler角色不统一时可能出现无主
    STORAGE_LOG(WARN, "diagnose rc service failed", K(ret), K(ls_id));
  }
  STORAGE_LOG(INFO, "diagnose finish", K(ret), K(info), K(ls_id));
  return ret;
}

int ObLS::set_migration_status(
    const ObMigrationStatus &migration_status,
    const int64_t rebuild_seq,
    const bool write_slog)
{
  int ret = OB_SUCCESS;
  share::ObLSRestoreStatus restore_status;
  int64_t read_lock = LSLOCKLS;
  int64_t write_lock = LSLOCKLOGMETA;
  ObLSLockGuard lock_myself(lock_, read_lock, write_lock);

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ls is not inited", K(ret), K(ls_meta_));
  } else if (!ObMigrationStatusHelper::is_valid(migration_status)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("set migration status get invalid argument", K(ret), K(migration_status));
  // migration status should be update after ls stopped, to make sure migrate task
  // will be finished later.
  } else if (!can_update_ls_meta(ls_meta_.get_ls_create_status())) {
    ret = OB_STATE_NOT_MATCH;
    STORAGE_LOG(WARN, "state not match, cannot update ls meta", K(ret), K(ls_meta_));
  } else if (ls_meta_.get_rebuild_seq() != rebuild_seq) {
    ret = OB_STATE_NOT_MATCH;
    STORAGE_LOG(WARN, "rebuild seq not match, cannot update migration status", K(ret),
        K(ls_meta_), K(rebuild_seq));
  } else if (OB_FAIL(ls_meta_.get_restore_status(restore_status))) {
    LOG_WARN("failed to get restore status", K(ret), K(ls_meta_));
  } else if (OB_FAIL(ls_meta_.set_migration_status(migration_status, write_slog))) {
    LOG_WARN("failed to set migration status", K(ret), K(migration_status));
  } else if (ObMigrationStatus::OB_MIGRATION_STATUS_NONE == migration_status
      && restore_status.is_restore_none()) {
    ls_tablet_svr_.enable_to_read();
  } else {
    ls_tablet_svr_.disable_to_read();
  }
  return ret;
}

int ObLS::set_restore_status(
    const share::ObLSRestoreStatus &restore_status,
    const int64_t rebuild_seq)
{
  int ret = OB_SUCCESS;
  ObMigrationStatus migration_status = ObMigrationStatus::OB_MIGRATION_STATUS_MAX;
  int64_t read_lock = LSLOCKLS;
  int64_t write_lock = LSLOCKLOGMETA;
  ObLSLockGuard lock_myself(lock_, read_lock, write_lock);

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ls is not inited", K(ret), K(ls_meta_));
  } else if (!restore_status.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("set restore status get invalid argument", K(ret), K(restore_status));
  // restore status should be update after ls stopped, to make sure restore task
  // will be finished later.
  } else if (!can_update_ls_meta(ls_meta_.get_ls_create_status())) {
    ret = OB_STATE_NOT_MATCH;
    STORAGE_LOG(WARN, "state not match, cannot update ls meta", K(ret), K(ls_meta_));
  } else if (ls_meta_.get_rebuild_seq() != rebuild_seq) {
    ret = OB_STATE_NOT_MATCH;
    STORAGE_LOG(WARN, "rebuild seq not match, cannot update restore status", K(ret),
        K(ls_meta_), K(rebuild_seq));
  } else if (OB_FAIL(ls_meta_.get_migration_status(migration_status))) {
    LOG_WARN("failed to get migration status", K(ret), K(ls_meta_));
  } else if (OB_FAIL(ls_meta_.set_restore_status(restore_status))) {
    LOG_WARN("failed to set restore status", K(ret), K(restore_status));
  } else if (ObMigrationStatus::OB_MIGRATION_STATUS_NONE == migration_status
      && restore_status.is_restore_none()) {
    ls_tablet_svr_.enable_to_read();
  } else {
    ls_tablet_svr_.disable_to_read();
  }
  return ret;
}

int ObLS::set_ls_rebuild()
{
  int ret = OB_SUCCESS;
  int64_t read_lock = LSLOCKLS;
  int64_t write_lock = LSLOCKLOGMETA;
  const bool try_lock = true; // the upper layer should deal with try lock fail.
  ObLSLockGuard lock_myself(lock_, read_lock, write_lock, try_lock);

  if (!lock_myself.locked()) {
    ret = OB_EAGAIN;
    LOG_WARN("try lock failed, please retry later", K(ret), K(ls_meta_));
  } else if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ls is not inited", K(ret), K(ls_meta_));
  } else if (OB_UNLIKELY(is_stopped_)) {
    ret = OB_NOT_RUNNING;
    STORAGE_LOG(WARN, "ls stopped", K(ret), K_(ls_meta));
  } else if (!can_update_ls_meta(ls_meta_.get_ls_create_status())) {
    ret = OB_STATE_NOT_MATCH;
    STORAGE_LOG(WARN, "state not match, cannot update ls meta", K(ret), K(ls_meta_));
  } else if (OB_FAIL(ls_meta_.set_ls_rebuild())) {
    LOG_WARN("failed to set ls rebuild", K(ret), K(ls_meta_));
  } else {
    ls_tablet_svr_.disable_to_read();
  }
  return ret;
}


}
}
