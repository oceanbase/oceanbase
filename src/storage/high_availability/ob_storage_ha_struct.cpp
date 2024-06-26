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
#include "ob_storage_ha_struct.h"
#include "storage/ls/ob_ls_meta_package.h"
#include "storage/tx_storage/ob_ls_handle.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/tablet/ob_tablet.h"
#include "storage/tablet/ob_tablet_common.h"
#include "storage/tablet/ob_tablet_iterator.h"
#include "storage/ls/ob_ls_tablet_service.h"
#include "logservice/ob_log_service.h"
#include "share/transfer/ob_transfer_task_operator.h"

namespace oceanbase
{
namespace storage
{
ERRSIM_POINT_DEF(EN_REBUILD_FAILED_STATUS);
ERRSIM_POINT_DEF(ALLOW_MIGRATION_STATUS_CHANGED);

/******************ObMigrationOpType*********************/
static const char *migration_op_type_strs[] = {
    "ADD_LS_OP",
    "MIGRATE_LS_OP",
    "REBUILD_LS_OP",
    "CHANGE_LS_OP",
    "REMOVE_LS_OP",
    "RESTORE_STANDBY_LS_OP",
};

const char *ObMigrationOpType::get_str(const TYPE &type)
{
  const char *str = nullptr;

  if (type < 0 || type >= MAX_LS_OP) {
    str = "UNKNOWN_OP";
  } else {
    str = migration_op_type_strs[type];
  }
  return str;
}

ObMigrationOpType::TYPE ObMigrationOpType::get_type(const char *type_str)
{
  ObMigrationOpType::TYPE type = ObMigrationOpType::MAX_LS_OP;

  const int64_t count = ARRAYSIZEOF(migration_op_type_strs);
  STATIC_ASSERT(static_cast<int64_t>(ObMigrationOpType::MAX_LS_OP) == count, "type count mismatch");
  for (int64_t i = 0; i < count; ++i) {
    if (0 == strcmp(type_str, migration_op_type_strs[i])) {
      type = static_cast<ObMigrationOpType::TYPE>(i);
      break;
    }
  }
  return type;
}

bool ObMigrationOpType::need_keep_old_tablet(const TYPE &type)
{
  int ret = OB_SUCCESS;
  bool bool_ret = false;

  if (!is_valid(type)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("check need keep old tablet get invaid argument", K(ret), K(type));
  } else if (ObMigrationOpType::REBUILD_LS_OP == type || ObMigrationOpType::CHANGE_LS_OP == type) {
    // TODO(yangyi.yyy): fix in 5.0: open this restriction if support tablet link
    bool_ret = false;
  } else {
    bool_ret = false;
  }
  return bool_ret;
}

int ObMigrationOpType::get_ls_wait_status(const TYPE &type, ObMigrationStatus &wait_status)
{
  int ret = OB_SUCCESS;
  wait_status = ObMigrationStatus::OB_MIGRATION_STATUS_MAX;
  if (!is_valid(type)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invaid argument", K(ret), K(type));
  } else if (ObMigrationOpType::MIGRATE_LS_OP == type) {
    wait_status = ObMigrationStatus::OB_MIGRATION_STATUS_MIGRATE_WAIT;
  } else if (ObMigrationOpType::ADD_LS_OP == type) {
    wait_status = ObMigrationStatus::OB_MIGRATION_STATUS_ADD_WAIT;
  } else if (ObMigrationOpType::REBUILD_LS_OP == type) {
    wait_status = ObMigrationStatus::OB_MIGRATION_STATUS_REBUILD_WAIT;
  } else {
    ret = OB_ERR_UNDEFINED;
    LOG_WARN("type is not valid", K(type));
  }
  return ret;
}

/******************ObMigrationStatusHelper*********************/
int ObMigrationStatusHelper::trans_migration_op(
    const ObMigrationOpType::TYPE &op_type, ObMigrationStatus &migration_status)
{
  int ret = OB_SUCCESS;
  migration_status = OB_MIGRATION_STATUS_MAX;

  if (!ObMigrationOpType::is_valid(op_type)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(op_type));
  } else {
    switch (op_type) {
    case ObMigrationOpType::ADD_LS_OP: {
      migration_status = OB_MIGRATION_STATUS_ADD;
      break;
    }
    case ObMigrationOpType::MIGRATE_LS_OP: {
      migration_status = OB_MIGRATION_STATUS_MIGRATE;
      break;
    }
    case ObMigrationOpType::REBUILD_LS_OP: {
      migration_status = OB_MIGRATION_STATUS_REBUILD;
      break;
    }
    case ObMigrationOpType::CHANGE_LS_OP: {
      migration_status = OB_MIGRATION_STATUS_CHANGE;
      break;
    }
    case ObMigrationOpType::RESTORE_STANDBY_LS_OP: {
      migration_status = OB_MIGRATION_STATUS_RESTORE_STANDBY;
      break;
    }
    default: {
      ret = OB_INVALID_ARGUMENT;
      LOG_ERROR("unknown op type", K(ret), K(op_type));
    }
    }
  }

  return ret;
}

int ObMigrationStatusHelper::trans_fail_status(const ObMigrationStatus &cur_status, ObMigrationStatus &fail_status)
{
  int ret = OB_SUCCESS;
  fail_status = OB_MIGRATION_STATUS_MAX;

  if (cur_status < OB_MIGRATION_STATUS_NONE || cur_status >= OB_MIGRATION_STATUS_MAX) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(cur_status));
  } else {
    switch (cur_status) {
    case OB_MIGRATION_STATUS_NONE: {
      // do nothing
      fail_status = OB_MIGRATION_STATUS_NONE;
      break;
    }
    case OB_MIGRATION_STATUS_ADD: {
      fail_status = OB_MIGRATION_STATUS_ADD_FAIL;
      break;
    }
    case OB_MIGRATION_STATUS_ADD_FAIL: {
      fail_status = OB_MIGRATION_STATUS_ADD_FAIL;
      break;
    }
    case OB_MIGRATION_STATUS_MIGRATE: {
      fail_status = OB_MIGRATION_STATUS_MIGRATE_FAIL;
      break;
    }
    case OB_MIGRATION_STATUS_MIGRATE_FAIL: {
      fail_status = OB_MIGRATION_STATUS_MIGRATE_FAIL;
      break;
    }
    case OB_MIGRATION_STATUS_CHANGE: {
      fail_status = OB_MIGRATION_STATUS_NONE;
      break;
    }
    case OB_MIGRATION_STATUS_RESTORE_STANDBY : {
      //allow observer self reentry
      fail_status = OB_MIGRATION_STATUS_NONE;
      break;
    }
    case OB_MIGRATION_STATUS_HOLD: {
      fail_status = OB_MIGRATION_STATUS_NONE;
      break;
    }
    case OB_MIGRATION_STATUS_MIGRATE_WAIT : {
      fail_status = OB_MIGRATION_STATUS_MIGRATE_FAIL;
      break;
    }
    case OB_MIGRATION_STATUS_ADD_WAIT : {
      fail_status = OB_MIGRATION_STATUS_ADD_FAIL;
      break;
    }
    //rebuild and rebuild_wait need use trans_rebuild_fail_status interface
    default: {
      ret = OB_INVALID_ARGUMENT;
      LOG_ERROR("invalid cur status for fail", K(ret), K(cur_status));
    }
    }
  }
  return ret;
}

int ObMigrationStatusHelper::trans_reboot_status(const ObMigrationStatus &cur_status, ObMigrationStatus &reboot_status)
{
  int ret = OB_SUCCESS;
  reboot_status = OB_MIGRATION_STATUS_MAX;

  if (cur_status < OB_MIGRATION_STATUS_NONE || cur_status >= OB_MIGRATION_STATUS_MAX) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(cur_status));
  } else {
    switch (cur_status) {
    case OB_MIGRATION_STATUS_NONE: {
      reboot_status = OB_MIGRATION_STATUS_NONE;
      break;
    }
    case OB_MIGRATION_STATUS_ADD:
    case OB_MIGRATION_STATUS_ADD_FAIL: {
      reboot_status = OB_MIGRATION_STATUS_ADD_FAIL;
      break;
    }
    case OB_MIGRATION_STATUS_MIGRATE:
    case OB_MIGRATION_STATUS_MIGRATE_FAIL: {
      reboot_status = OB_MIGRATION_STATUS_MIGRATE_FAIL;
      break;
    }
    case OB_MIGRATION_STATUS_REBUILD: {
      reboot_status = OB_MIGRATION_STATUS_REBUILD;
      break;
    }
    case OB_MIGRATION_STATUS_CHANGE: {
      reboot_status = OB_MIGRATION_STATUS_NONE;
      break;
    }
    case OB_MIGRATION_STATUS_RESTORE_STANDBY: {
      reboot_status = OB_MIGRATION_STATUS_NONE;
      break;
    }
    case OB_MIGRATION_STATUS_HOLD: {
      reboot_status = OB_MIGRATION_STATUS_NONE;
      break;
    }
    case OB_MIGRATION_STATUS_MIGRATE_WAIT : {
      reboot_status = OB_MIGRATION_STATUS_MIGRATE_FAIL;
      break;
    }
    case OB_MIGRATION_STATUS_ADD_WAIT : {
      reboot_status = OB_MIGRATION_STATUS_ADD_FAIL;
      break;
    }
    case OB_MIGRATION_STATUS_REBUILD_WAIT: {
      reboot_status = OB_MIGRATION_STATUS_REBUILD;
      break;
    }
    case OB_MIGRATION_STATUS_REBUILD_FAIL : {
      reboot_status = OB_MIGRATION_STATUS_REBUILD_FAIL;
      break;
    }
    case OB_MIGRATION_STATUS_GC: {
      reboot_status = OB_MIGRATION_STATUS_GC;
      break;
    }
    default: {
      ret = OB_INVALID_ARGUMENT;
      LOG_ERROR("invalid cur status for fail", K(ret), K(cur_status));
    }
    }
  }
  return ret;
}


bool ObMigrationStatusHelper::check_can_election(const ObMigrationStatus &cur_status)
{
  bool can_election = true;

  if (OB_MIGRATION_STATUS_ADD == cur_status
      || OB_MIGRATION_STATUS_ADD_FAIL == cur_status
      || OB_MIGRATION_STATUS_MIGRATE == cur_status
      || OB_MIGRATION_STATUS_MIGRATE_FAIL == cur_status) {
    can_election = false;
  }

  return can_election;
}

bool ObMigrationStatusHelper::check_can_restore(const ObMigrationStatus &cur_status)
{
  return OB_MIGRATION_STATUS_NONE == cur_status;
}

// If dest_tablet does not exist, the log stream allows GC.
// If dest_tablet exists, has_transfer_table=false, the log stream allows GC.
// src_ls GC process: offline log_handler ---> set OB_MIGRATION_STATUS_GC ---> get dest_tablet
// dest_ls replay clog process: create transfer in tablet(on_redo) ----> check the migration_status of src_ls in dest_ls replay clog(on_prepare)
// if the replay of the next start transfer in log depends on this log stream, the replay of the on_prepare log will be stuck, and the newly created transfer in tablet will be unreadable
// If dest_tablet exists, has_transfer_table=true, the log stream does not allow GC, because the data of the log stream also needs to be relied on
int ObMigrationStatusHelper::check_transfer_dest_tablet_for_ls_gc(
    ObLS *ls,
    const ObTabletID &tablet_id,
    const share::SCN &transfer_scn,
    const bool need_wait_dest_ls_replay,
    bool &allow_gc)
{
  int ret = OB_SUCCESS;
  ObTabletHandle tablet_handle;
  ObTablet *tablet = nullptr;
  SCN dest_transfer_scn;
  if (OB_ISNULL(ls) || !tablet_id.is_valid() || !transfer_scn.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(ls), K(tablet_id), K(transfer_scn));
  } else if (OB_FAIL(ls->ha_get_tablet(tablet_id, tablet_handle))) {
    if (OB_TABLET_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      if (need_wait_dest_ls_replay) {
        allow_gc = false;
      } else {
        allow_gc = true;
      }
      LOG_WARN("dest tablet not exist", K(ret), "ls_id", ls->get_ls_id(), K(tablet_id), K(allow_gc));
    } else {
      LOG_WARN("failed to get tablet", K(ret), "ls_id", ls->get_ls_id(), K(tablet_id));
    }
  } else if (OB_ISNULL(tablet = tablet_handle.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet should not be NULL", K(ret), "ls_id", ls->get_ls_id(), K(tablet_id));
  } else if (FALSE_IT(dest_transfer_scn = tablet->get_tablet_meta().transfer_info_.transfer_start_scn_)) {
  } else if (transfer_scn < dest_transfer_scn) {
    allow_gc = true;
    LOG_INFO("src tablet transfer is smaller than dest tablet, allow gc", "ls_id", ls->get_ls_id(), K(tablet_id),
        "src_transfer_scn", transfer_scn, "dest_transfer_scn", dest_transfer_scn, KPC(tablet));
  } else if (transfer_scn > dest_transfer_scn) {
    allow_gc = false;
    LOG_INFO("src tablet transfer is bigger than dest tablet, do not allow gc", "ls_id", ls->get_ls_id(),
        K(tablet_id), "src_transfer_scn", transfer_scn, "dest_transfer_scn", dest_transfer_scn, KPC(tablet));
  } else if (tablet->get_tablet_meta().has_transfer_table()) {
    allow_gc = false;
    LOG_INFO("dest tablet has transfer table", "ls_id", ls->get_ls_id(), K(tablet_id));
  } else {
    allow_gc = true;
    LOG_INFO("dest tablet has no transfer table", "ls_id", ls->get_ls_id(), K(tablet_id));
  }
  return ret;
}

int ObMigrationStatusHelper::check_transfer_dest_ls_status_for_ls_gc(
    const share::ObLSID &transfer_ls_id,
    const ObTabletID &tablet_id,
    const share::SCN &transfer_scn,
    const bool need_wait_dest_ls_replay,
    bool &allow_gc)
{
  int ret = OB_SUCCESS;
  ObLSService *ls_service = nullptr;
  ObLS *dest_ls = nullptr;
  ObLSHandle ls_handle;
  allow_gc = false;
  ObMigrationStatus dest_ls_status = ObMigrationStatus::OB_MIGRATION_STATUS_MAX;
  if (!transfer_ls_id.is_valid() || !tablet_id.is_valid() || !transfer_scn.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("ls id is invalid", K(ret), K(transfer_ls_id), K(tablet_id), K(transfer_scn));
  } else if (OB_ISNULL(ls_service = MTL(ObLSService*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get ObLSService from MTL", K(ret), KP(ls_service));
  } else if (OB_FAIL(ls_service->get_ls(transfer_ls_id, ls_handle, ObLSGetMod::HA_MOD))) {
    if (OB_LS_NOT_EXIST == ret) {
      LOG_INFO("transfer dest ls not exist", K(ret), K(transfer_ls_id));
      allow_gc = true;
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to get ls", K(ret), K(transfer_ls_id));
    }
  } else if (OB_ISNULL(dest_ls = ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls should not be NULL", K(ret), KP(dest_ls), K(transfer_ls_id));
  } else if (OB_FAIL(dest_ls->get_migration_status(dest_ls_status))) {
    LOG_WARN("failed to get migration status", K(ret), KPC(dest_ls));
  } else if (ObMigrationStatus::OB_MIGRATION_STATUS_NONE != dest_ls_status
      && ObMigrationStatus::OB_MIGRATION_STATUS_MIGRATE_WAIT != dest_ls_status
      && ObMigrationStatus::OB_MIGRATION_STATUS_ADD_WAIT != dest_ls_status
      && ObMigrationStatus::OB_MIGRATION_STATUS_REBUILD_WAIT != dest_ls_status
      && ObMigrationStatus::OB_MIGRATION_STATUS_HOLD != dest_ls_status) {
    allow_gc = true;
    LOG_INFO("transfer dest ls check transfer status passed", K(ret), K(transfer_ls_id), K(dest_ls_status));
  } else if (OB_FAIL(check_transfer_dest_tablet_for_ls_gc(dest_ls, tablet_id, transfer_scn, need_wait_dest_ls_replay, allow_gc))) {
    LOG_WARN("failed to check transfer dest tablet", K(ret), KPC(dest_ls), K(tablet_id));
  }

  return ret;
}

// The status of the log stream is OB_MIGRATION_STATUS_GC, which will block the replay of the start transfer in log corresponding to transfer dest_ls
// Log stream that is not in the member_list will not be added to the member_list.
// If the log stream status modification fails, there is no need to online log_handler.
// After setting the flag of ls gc and stopping log synchronization, it will only affect the destination of the transfer minority,
// and the destination can be restored through rebuilding.
int ObMigrationStatusHelper::set_ls_migrate_gc_status_(
  ObLS &ls,
  bool &allow_gc)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ls.set_ls_migration_gc(allow_gc))) {
    LOG_WARN("failed to set migration status", K(ret));
  } else if (!allow_gc) {
    LOG_INFO("ls is not allow gc", K(ret), K(ls));
  } else if (OB_FAIL(ls.get_log_handler()->disable_sync())) {
    LOG_WARN("failed to disable replay", K(ret));
  }
  return ret;
}

int ObMigrationStatusHelper::check_ls_transfer_tablet_(
    const share::ObLSID &ls_id,
    const ObMigrationStatus &migration_status,
    bool &allow_gc)
{
  int ret = OB_SUCCESS;
  allow_gc = false;
  ObLSService *ls_service = nullptr;
  ObLS *ls = nullptr;
  ObLSHandle ls_handle;
  ObLSTabletIterator tablet_iter(ObMDSGetTabletMode::READ_WITHOUT_CHECK);
  ObLSPersistentState ls_status;
  bool need_check_allow_gc = true;
  bool need_wait_dest_ls_replay = false;
  ObLSRestoreStatus restore_status;
  if (!ls_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("ls id is invalid", K(ret), K(ls_id));
  } else if (OB_ISNULL(ls_service = MTL(ObLSService*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get ObLSService from MTL", K(ret), KP(ls_service));
  } else if (OB_FAIL(ls_service->get_ls(ls_id, ls_handle, ObLSGetMod::OBSERVER_MOD))) {
    LOG_WARN("get ls failed", K(ret), K(ls_id));
  } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls not exist", K(ret), K(ls_id));
  } else if (FALSE_IT(ls_status = ls->get_persistent_state())) {
  } else if (ls_status.is_need_gc()) {
    allow_gc = true;
  } else if (OB_FAIL(set_ls_migrate_gc_status_(*ls, allow_gc))) {
    LOG_WARN("failed to set ls gc status", KR(ret));
  } else if (!allow_gc) {
    //do nothing
  } else if (OB_FAIL(ls->get_restore_status(restore_status))) {
    LOG_WARN("failed to get restore status", K(ret), KPC(ls));
  } else if (restore_status.is_in_restore()) {
    allow_gc = true;
    LOG_INFO("ls ls in restore status, allow gc", K(ret), K(restore_status), K(ls_id));
  } else if (OB_FAIL(check_ls_with_transfer_task_(*ls, need_check_allow_gc, need_wait_dest_ls_replay))) {
    LOG_WARN("failed to check ls with transfer task", K(ret), KPC(ls));
  } else if (!need_check_allow_gc) {
    allow_gc = false;
  } else if (OB_FAIL(ls->get_tablet_svr()->build_tablet_iter(tablet_iter))) {
    LOG_WARN("failed to build ls tablet iter", KR(ret));
  } else {
    ObTabletHandle tablet_handle;
    ObTablet *tablet = NULL;
    ObTabletCreateDeleteMdsUserData user_data;
    bool committed_flag = false;
    while (OB_SUCC(ret)) {
      committed_flag = false;
      if (OB_FAIL(tablet_iter.get_next_tablet(tablet_handle))) {
        if (OB_ITER_END == ret) {
          allow_gc = true;
          ret = OB_SUCCESS;
          break;
        } else {
          LOG_WARN("failed to get tablet", KR(ret), K(tablet_handle), K(ls_id));
        }
      } else if (OB_UNLIKELY(!tablet_handle.is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid tablet handle", KR(ret), K(tablet_handle), K(ls_id));
      } else if (OB_ISNULL(tablet = tablet_handle.get_obj())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("tablet is NULL", KR(ret), K(ls_id));
      } else if (tablet->is_ls_inner_tablet() || tablet->is_empty_shell()) {
        // do nothing
      } else if (OB_FAIL(tablet->ObITabletMdsInterface::get_latest_tablet_status(user_data, committed_flag))) {
        if (OB_EMPTY_RESULT == ret) {
          LOG_INFO("tablet_status is null, ls is allowed to be GC", KR(ret), "tablet_id", tablet->get_tablet_meta().tablet_id_, K(ls_id));
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("failed to get latest tablet status", K(ret), KP(tablet), K(ls_id));
        }
      } else if (ObTabletStatus::TRANSFER_OUT != user_data.tablet_status_
          && ObTabletStatus::TRANSFER_OUT_DELETED != user_data.tablet_status_) {
        // do nothing
      } else if (user_data.transfer_scn_.is_min()) {
        if (committed_flag) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("transfer out is committed but transfer scn is invalid, unexpected", K(ret), K(user_data), K(committed_flag));
        } else {
          allow_gc = true;
          LOG_INFO("transfer out is not committed, allow gc", K(ret), K(user_data));
        }
      } else if (OB_FAIL(check_transfer_dest_ls_status_for_ls_gc(
          user_data.transfer_ls_id_, tablet->get_tablet_meta().tablet_id_,
          user_data.transfer_scn_, need_wait_dest_ls_replay, allow_gc))) {
        LOG_WARN("failed to check ls transfer tablet", K(ret), K(ls), K(user_data));
      } else if (!allow_gc) {
        LOG_INFO("The ls is not allowed to be GC because it is also dependent on other ls", K(user_data),
            K(ls_id), "tablet_id", tablet->get_tablet_meta().tablet_id_, K(migration_status));
        break;
      }
    }
  }
  return ret;
}

int ObMigrationStatusHelper::check_ls_allow_gc(
    const share::ObLSID &ls_id,
    const ObMigrationStatus &cur_status,
    bool &allow_gc)
{
  int ret = OB_SUCCESS;
  allow_gc = false;
  if (!ls_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("ls_id is invalid", K(ret), K(ls_id));
  } else if (check_migration_status_is_fail_(cur_status)) {
    allow_gc = true;
  } else if (OB_FAIL(check_ls_transfer_tablet_(ls_id, cur_status, allow_gc))) {
    LOG_WARN("failed to check ls transfer tablet", K(ret), K(ls_id));
  }
  return ret;
}

int ObMigrationStatusHelper::check_ls_with_transfer_task_(
    ObLS &ls,
    bool &need_check_allow_gc,
    bool &need_wait_dest_ls_replay)
{
  int ret = OB_SUCCESS;
  need_check_allow_gc = false;
  need_wait_dest_ls_replay = false;
  common::ObMySQLProxy *sql_proxy = GCTX.sql_proxy_;
  ObTransferTask task;
  const uint64_t tenant_id = ls.get_tenant_id();
  const ObLSID &src_ls_id = ls.get_ls_id();
  share::ObTransferTaskInfo task_info;
  SCN max_decided_scn(SCN::base_scn());
  ObLSService *ls_service = NULL;
  ObLSHandle dest_ls_handle;
  bool is_tenant_deleted = false;

  if (OB_ISNULL(sql_proxy)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("mysql proxy should not be NULL", K(ret), KP(sql_proxy));
  } else if (OB_FAIL(ObStorageHAUtils::check_tenant_will_be_deleted(is_tenant_deleted))) {
    LOG_WARN("failed to check tenant deleted", K(ret), K(ls));
  } else if (is_tenant_deleted) {
    need_check_allow_gc = true;
    need_wait_dest_ls_replay = false;
    FLOG_INFO("unit wait gc in observer, allow gc", K(tenant_id), K(src_ls_id));
  } else if (OB_FAIL(ObTransferTaskOperator::get_by_src_ls(
      *sql_proxy, tenant_id, src_ls_id, task, share::OBCG_STORAGE))) {
    LOG_WARN("failed to get transfer task", K(ret), K(tenant_id), K(src_ls_id));
    if (OB_ENTRY_NOT_EXIST == ret || OB_TABLE_NOT_EXIST == ret) {
      need_check_allow_gc = true;
      need_wait_dest_ls_replay = false;
      ret = OB_SUCCESS;
    }
  } else if (OB_FAIL(task_info.convert_from(tenant_id, task))) {
    LOG_WARN("failed to convert from transfer task", K(ret), K(task));
  } else if (!task_info.status_.is_doing_status()) {
    need_check_allow_gc = true;
    need_wait_dest_ls_replay = false;
  } else if (OB_ISNULL(ls_service = MTL(ObLSService*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get ObLSService from MTL", K(ret), KP(ls_service));
  } else if (OB_FAIL(ls_service->get_ls(task_info.dest_ls_id_, dest_ls_handle, ObLSGetMod::HA_MOD))) {
    if (OB_LS_NOT_EXIST == ret) {
      LOG_INFO("transfer dest ls not exist", K(ret), K(task_info));
      need_check_allow_gc = true;
      need_wait_dest_ls_replay = false;
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to get ls", K(ret), K(task_info));
    }
  } else {
    if (OB_FAIL(ls.get_max_decided_scn(max_decided_scn))) {
      LOG_WARN("failed to get max decided scn", K(ret), K(ls));
    } else if (max_decided_scn < task_info.start_scn_) {
      need_check_allow_gc = false;
      need_wait_dest_ls_replay = false;
      LOG_INFO("transfer src ls is not replay to transfer scn, do not allow gc", K(max_decided_scn), K(task_info));
    } else {
      need_check_allow_gc = true;
      need_wait_dest_ls_replay = true;
      LOG_INFO("transfer src ls is in doing status, need wait dest ls replay", K(max_decided_scn), K(task_info));
    }
  }
  return ret;
}

bool ObMigrationStatusHelper::check_migration_status_is_fail_(const ObMigrationStatus &cur_status)
{
  bool is_fail = false;
  if (OB_MIGRATION_STATUS_ADD_FAIL == cur_status
      || OB_MIGRATION_STATUS_MIGRATE_FAIL == cur_status
      || OB_MIGRATION_STATUS_REBUILD_FAIL == cur_status) {
    is_fail = true;
  }
  return is_fail;
}

bool ObMigrationStatusHelper::need_online(const ObMigrationStatus &cur_status)
{
  return (OB_MIGRATION_STATUS_NONE == cur_status
         || OB_MIGRATION_STATUS_GC == cur_status);
}

bool ObMigrationStatusHelper::check_allow_gc_abandoned_ls(const ObMigrationStatus &cur_status)
{
  bool allow_gc = false;
  if (check_migration_status_is_fail_(cur_status)) {
    allow_gc = true;
  } else if (OB_MIGRATION_STATUS_GC == cur_status) {
    allow_gc = true;
  }
  return allow_gc;
}

bool ObMigrationStatusHelper::check_is_running_migration(const ObMigrationStatus &cur_status)
{
  bool is_running = true;
  if (check_allow_gc_abandoned_ls(cur_status)) {
    is_running = false;
  } else if (OB_MIGRATION_STATUS_NONE == cur_status) {
    is_running = false;
  } else {
    is_running = true;
  }
  return is_running;
}

bool ObMigrationStatusHelper::check_can_migrate_out(const ObMigrationStatus &cur_status)
{
  bool can_migrate_out = true;
  if (OB_MIGRATION_STATUS_NONE != cur_status) {
    can_migrate_out = false;
  }
  return can_migrate_out;
}

int ObMigrationStatusHelper::check_can_change_status(
    const ObMigrationStatus &cur_status,
    const ObMigrationStatus &change_status,
    bool &can_change)
{
  int ret = OB_SUCCESS;
  can_change = false;

  if (!is_valid(cur_status) || !is_valid(change_status)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("check can change status get invalid argument", K(ret), K(cur_status), K(change_status));
  } else {
    switch (cur_status) {
    case OB_MIGRATION_STATUS_NONE: {
      if (OB_MIGRATION_STATUS_ADD == change_status
          || OB_MIGRATION_STATUS_MIGRATE == change_status
          || OB_MIGRATION_STATUS_CHANGE == change_status
          || OB_MIGRATION_STATUS_REBUILD == change_status
          || OB_MIGRATION_STATUS_RESTORE_STANDBY == change_status
          || OB_MIGRATION_STATUS_GC == change_status) {
        can_change = true;
      }
      break;
    }
    case OB_MIGRATION_STATUS_ADD: {
      if (OB_MIGRATION_STATUS_ADD == change_status
          || OB_MIGRATION_STATUS_ADD_FAIL == change_status
          || OB_MIGRATION_STATUS_ADD_WAIT == change_status) {
        can_change = true;
      }
      break;
    }
    case OB_MIGRATION_STATUS_ADD_FAIL: {
      if (OB_MIGRATION_STATUS_ADD_FAIL == change_status) {
        can_change = true;
      }
      break;
    }
    case OB_MIGRATION_STATUS_MIGRATE: {
      if (OB_MIGRATION_STATUS_MIGRATE == change_status
          || OB_MIGRATION_STATUS_MIGRATE_FAIL == change_status
          || OB_MIGRATION_STATUS_MIGRATE_WAIT == change_status) {
        can_change = true;
      }
      break;
    }
    case OB_MIGRATION_STATUS_MIGRATE_FAIL: {
      if (OB_MIGRATION_STATUS_MIGRATE_FAIL == change_status) {
        can_change = true;
      }
      break;
    }
    case OB_MIGRATION_STATUS_REBUILD: {
      if (OB_MIGRATION_STATUS_NONE == change_status
          || OB_MIGRATION_STATUS_REBUILD == change_status
          || OB_MIGRATION_STATUS_REBUILD_WAIT == change_status
          || OB_MIGRATION_STATUS_REBUILD_FAIL == change_status) {
        can_change = true;
      }
      break;
    }
    case OB_MIGRATION_STATUS_CHANGE: {
      if (OB_MIGRATION_STATUS_NONE == change_status
          || OB_MIGRATION_STATUS_CHANGE == change_status) {
        can_change = true;
      }
      break;
    }
    case OB_MIGRATION_STATUS_RESTORE_STANDBY: {
      if (OB_MIGRATION_STATUS_NONE == change_status
          || OB_MIGRATION_STATUS_RESTORE_STANDBY == change_status) {
        can_change = true;
      }
      break;
    }
    case OB_MIGRATION_STATUS_HOLD: {
      if (OB_MIGRATION_STATUS_HOLD == change_status
          || OB_MIGRATION_STATUS_ADD_FAIL == change_status
          || OB_MIGRATION_STATUS_MIGRATE_FAIL == change_status
          || OB_MIGRATION_STATUS_NONE == change_status) {
        can_change = true;
      }
      break;
    }
    case OB_MIGRATION_STATUS_MIGRATE_WAIT: {
      if (OB_MIGRATION_STATUS_HOLD == change_status
          || OB_MIGRATION_STATUS_MIGRATE_FAIL == change_status) {
        can_change = true;
      }
      break;
    }
    case OB_MIGRATION_STATUS_ADD_WAIT: {
      if (OB_MIGRATION_STATUS_HOLD == change_status
          || OB_MIGRATION_STATUS_ADD_FAIL == change_status) {
        can_change = true;
      }
      break;
    }
    case OB_MIGRATION_STATUS_REBUILD_WAIT: {
      if (OB_MIGRATION_STATUS_NONE == change_status
          || OB_MIGRATION_STATUS_REBUILD_WAIT == change_status
          || OB_MIGRATION_STATUS_REBUILD == change_status
          || OB_MIGRATION_STATUS_REBUILD_FAIL == change_status) {
        can_change = true;
      }
      break;
    }
    case OB_MIGRATION_STATUS_REBUILD_FAIL: {
      if (OB_MIGRATION_STATUS_REBUILD_FAIL == change_status) {
        can_change = true;
      }
      break;
    }
    case OB_MIGRATION_STATUS_GC: {
      if (OB_MIGRATION_STATUS_GC == change_status) {
        can_change = true;
      }
      break;
    }
    default: {
      ret = OB_INVALID_ARGUMENT;
      LOG_ERROR("invalid cur status for fail", K(ret), K(cur_status));
    }
    }
  }

#ifdef ERRSIM
    if (OB_SUCC(ret)) {
      ret = ALLOW_MIGRATION_STATUS_CHANGED ? : OB_SUCCESS;
      if (OB_FAIL(ret)) {
        can_change = true;
        ret = OB_SUCCESS;
      }
    }
#endif

  return ret;
}

bool ObMigrationStatusHelper::is_valid(const ObMigrationStatus &status)
{
  return status >= ObMigrationStatus::OB_MIGRATION_STATUS_NONE
      && status < ObMigrationStatus::OB_MIGRATION_STATUS_MAX;
}

int ObMigrationStatusHelper::trans_rebuild_fail_status(
    const ObMigrationStatus &cur_status,
    const bool is_in_member_list,
    const bool is_ls_deleted,
    const bool is_tenant_dropped,
    ObMigrationStatus &fail_status)
{
  int ret = OB_SUCCESS;
  fail_status = OB_MIGRATION_STATUS_MAX;

  if (OB_MIGRATION_STATUS_REBUILD != cur_status && OB_MIGRATION_STATUS_REBUILD_WAIT != cur_status) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(cur_status));
  } else if (is_tenant_dropped || !is_in_member_list || is_ls_deleted) {
    fail_status = OB_MIGRATION_STATUS_REBUILD_FAIL;
  } else {
    fail_status = OB_MIGRATION_STATUS_REBUILD;
  }
#ifdef ERRSIM
    if (OB_SUCC(ret)) {
      ret = EN_REBUILD_FAILED_STATUS ? : OB_SUCCESS;
      if (OB_FAIL(ret)) {
        fail_status = OB_MIGRATION_STATUS_REBUILD_FAIL;
        ret = OB_SUCCESS;
      }
    }
#endif

  return ret;
}

int ObMigrationStatusHelper::check_migration_in_final_state(
    const ObMigrationStatus &status,
    bool &in_final_state)
{
  int ret = OB_SUCCESS;
  in_final_state = false;

  if (!is_valid(status)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("check migration in final state get invalid argument", K(ret), K(status));
  } else if (ObMigrationStatus::OB_MIGRATION_STATUS_NONE == status
      || ObMigrationStatus::OB_MIGRATION_STATUS_ADD_FAIL == status
      || ObMigrationStatus::OB_MIGRATION_STATUS_MIGRATE_FAIL == status
      || ObMigrationStatus::OB_MIGRATION_STATUS_REBUILD_FAIL == status
      || ObMigrationStatus::OB_MIGRATION_STATUS_GC == status) {
    in_final_state = true;
  } else {
    in_final_state = false;
  }
  return ret;
}

/******************ObMigrationOpArg*********************/
ObMigrationOpArg::ObMigrationOpArg()
  : ls_id_(),
    type_(ObMigrationOpType::MAX_LS_OP),
    cluster_id_(0),
    priority_(ObMigrationOpPriority::PRIO_INVALID),
    src_(),
    dst_(),
    data_src_(),
    paxos_replica_number_(0)
{
}

bool ObMigrationOpArg::is_valid() const
{
  return ls_id_.is_valid()
      && type_>= 0 && type_ < ObMigrationOpType::MAX_LS_OP
      && cluster_id_ > 0
      && src_.is_valid()
      && dst_.is_valid()
      && (paxos_replica_number_ > 0 || ObMigrationOpType::REBUILD_LS_OP == type_)
      && ObMigrationOpType::MIGRATE_LS_OP == type_ ?
         (src_.get_server() != dst_.get_server()) : true;
}

void ObMigrationOpArg::reset()
{
  ls_id_.reset();
  type_ = ObMigrationOpType::MAX_LS_OP;
  cluster_id_ = 0;
  priority_ = ObMigrationOpPriority::PRIO_INVALID;
  src_.reset();
  dst_.reset();
  data_src_.reset();
  paxos_replica_number_ = 0;
}

/******************ObTabletsTransferArg*********************/
ObTabletsTransferArg::ObTabletsTransferArg()
  : tenant_id_(OB_INVALID_ID), 
    ls_id_(),
    src_(),
    tablet_id_array_(),
    snapshot_log_ts_(0)
{
}

bool ObTabletsTransferArg::is_valid() const
{
  return tenant_id_ != OB_INVALID_ID
      && ls_id_.is_valid()
      && src_.is_valid()
      && tablet_id_array_.count() > 0
      && snapshot_log_ts_ > 0;
}

void ObTabletsTransferArg::reset()
{
  tenant_id_ = OB_INVALID_ID;
  ls_id_.reset();
  src_.reset();
  tablet_id_array_.reset();
  snapshot_log_ts_ = 0;
}

/******************ObStorageHASrcInfo*********************/

ObStorageHASrcInfo::ObStorageHASrcInfo()
    : src_addr_(),
      cluster_id_(-1)
{
}

bool ObStorageHASrcInfo::is_valid() const
{
  return src_addr_.is_valid() && -1 != cluster_id_;
}

void ObStorageHASrcInfo::reset()
{
  src_addr_.reset();
  cluster_id_ = -1;
}

uint64_t ObStorageHASrcInfo::hash() const
{
  uint64_t hash_value = 0;
  hash_value = common::murmurhash(&cluster_id_, sizeof(cluster_id_), hash_value);
  hash_value += src_addr_.hash();
  return hash_value;
}

bool ObStorageHASrcInfo::operator ==(const ObStorageHASrcInfo &src_info) const
{
  return src_addr_ == src_info.src_addr_
      && cluster_id_ == src_info.cluster_id_;
}

/******************ObMacroBlockCopyInfo*********************/
ObMacroBlockCopyInfo::ObMacroBlockCopyInfo()
  : logic_macro_block_id_(),
    need_copy_(true)
{
}

ObMacroBlockCopyInfo::~ObMacroBlockCopyInfo()
{
}

bool ObMacroBlockCopyInfo::is_valid() const
{
  return logic_macro_block_id_.is_valid();
}

void ObMacroBlockCopyInfo::reset()
{
  //logic_macro_block_id_.reset();
  need_copy_ = true;
}

/******************ObMacroBlockCopyArgInfo*********************/

ObMacroBlockCopyArgInfo::ObMacroBlockCopyArgInfo()
  : logic_macro_block_id_()
{
}

ObMacroBlockCopyArgInfo::~ObMacroBlockCopyArgInfo()
{
}

bool ObMacroBlockCopyArgInfo::is_valid() const
{
  return logic_macro_block_id_.is_valid();
}

void ObMacroBlockCopyArgInfo::reset()
{
  //logic_macro_block_id_.reset();
}

/******************ObCopyTabletSimpleInfo*********************/
ObCopyTabletSimpleInfo::ObCopyTabletSimpleInfo()
  : tablet_id_(),
    status_(ObCopyTabletStatus::MAX_STATUS),
    data_size_(0)
{
}

void ObCopyTabletSimpleInfo::reset()
{
  tablet_id_.reset();
  status_ = ObCopyTabletStatus::MAX_STATUS;
  data_size_ = 0;
}

bool ObCopyTabletSimpleInfo::is_valid() const
{
  return tablet_id_.is_valid() && ObCopyTabletStatus::is_valid(status_) && data_size_ >= 0;
}

/******************ObMigrationFakeBlockID*********************/
ObMigrationFakeBlockID::ObMigrationFakeBlockID()
{
  migration_fake_block_id_.reset();
  migration_fake_block_id_.set_block_index(FAKE_BLOCK_INDEX);
}

/******************ObCopySSTableHelper*********************/
bool ObCopySSTableHelper::check_can_reuse(
    const ObSSTableStatus &status)
{
  int bool_ret = false;

  if (ObSSTableStatus::SSTABLE_READY_FOR_READ == status
      || ObSSTableStatus::SSTABLE_READY_FOR_REMOTE_LOGICAL_READ == status
      || ObSSTableStatus::SSTABLE_READY_FOR_REMOTE_PHYTSICAL_READ == status) {
    bool_ret = true;
  }
  return bool_ret;
}

/******************ObMigrationUtils*********************/
bool ObMigrationUtils::is_need_retry_error(const int err)
{
  bool bret = true;
  switch (err) {
    case OB_NOT_INIT :
    case OB_INVALID_ARGUMENT :
    case OB_ERR_UNEXPECTED :
    case OB_ERR_SYS :
    case OB_INIT_TWICE :
    case OB_SRC_DO_NOT_ALLOWED_MIGRATE :
    case OB_CANCELED :
    case OB_NOT_SUPPORTED :
    case OB_SERVER_OUTOF_DISK_SPACE :
    case OB_LOG_NOT_SYNC :
    case OB_INVALID_DATA :
    case OB_CHECKSUM_ERROR :
    case OB_DDL_SSTABLE_RANGE_CROSS :
    case OB_TENANT_NOT_EXIST :
    case OB_TRANSFER_SYS_ERROR :
    case OB_INVALID_TABLE_STORE :
    case OB_UNEXPECTED_TABLET_STATUS :
    case OB_TABLET_TRANSFER_SEQ_NOT_MATCH:
      bret = false;
      break;
    default:
      break;
  }
  return bret;
}

int ObMigrationUtils::check_tablets_has_inner_table(
    const common::ObIArray<ObTabletID> &tablet_ids,
    bool &has_inner_table)
{
  int ret = OB_SUCCESS;
  has_inner_table = false;
  for (int64_t i = 0; OB_SUCC(ret) && i < tablet_ids.count(); ++i) {
    const ObTabletID &tablet_id = tablet_ids.at(i);
    if (tablet_id.is_inner_tablet() || tablet_id.is_ls_inner_tablet()) {
      has_inner_table = true;
      break;
    }
  }
  return ret;
}

int ObMigrationUtils::get_ls_rebuild_seq(const uint64_t tenant_id,
    const share::ObLSID &ls_id, int64_t &rebuild_seq)
{
  int ret = OB_SUCCESS;
  rebuild_seq = 0;
  storage::ObLS *ls = NULL;
  ObLSService *ls_service = NULL;
  ObLSHandle handle;
  ObMigrationStatus status = ObMigrationStatus::OB_MIGRATION_STATUS_MAX;
  if (OB_INVALID_ID == tenant_id || !ls_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(tenant_id), K(ls_id));
  } else if (OB_ISNULL(ls_service = MTL_WITH_CHECK_TENANT(ObLSService *, tenant_id))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("log stream service is NULL", K(ret));
  } else if (OB_FAIL(ls_service->get_ls(ls_id, handle, ObLSGetMod::HA_MOD))) {
    LOG_WARN("failed to get log stream", K(ret), K(ls_id));
  } else if (OB_ISNULL(ls = handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("log stream not exist", K(ret), K(ls_id));
  } else if (OB_FAIL(ls->get_migration_status(status))) {
    LOG_WARN("failed to get migration status", K(ret), KPC(ls));
  } else if (!ObMigrationStatusHelper::check_can_migrate_out(status) || ls->is_stopped() || ls->is_offline()) {
    ret = OB_SRC_DO_NOT_ALLOWED_MIGRATE;
    LOG_WARN("migration src ls migration status is not none or ls in stop status",
        K(ret), KPC(ls), K(status));
  } else {
    rebuild_seq = ls->get_rebuild_seq();
  }
  return ret;
}

int ObMigrationUtils::get_dag_priority(
    const ObMigrationOpType::TYPE &type,
    ObDagPrio::ObDagPrioEnum &prio)
{
  int ret = OB_SUCCESS;
  if (!ObMigrationOpType::is_valid(type)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid argument", K(ret), K(type));
  } else {
    switch (type) {
      case ObMigrationOpType::TYPE::ADD_LS_OP: {
        prio = ObDagPrio::DAG_PRIO_HA_HIGH;
        break;
      }
      case ObMigrationOpType::TYPE::MIGRATE_LS_OP: {
        prio = ObDagPrio::DAG_PRIO_HA_MID;
        break;
      }
      case ObMigrationOpType::TYPE::REBUILD_LS_OP: {
        prio = ObDagPrio::DAG_PRIO_HA_HIGH;
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("migration op type not expected", K(ret), K(type));
        break;
      }
    }
  }
  return ret;
}

/******************ObCopyTableKeyInfo*********************/
ObCopyTableKeyInfo::ObCopyTableKeyInfo()
  : src_table_key_(),
    dest_table_key_()
{
}

void ObCopyTableKeyInfo::reset()
{
  src_table_key_.reset();
  dest_table_key_.reset();
}

bool ObCopyTableKeyInfo::is_valid() const
{
  return src_table_key_.is_valid() && dest_table_key_.is_valid()
      && src_table_key_.table_type_ == dest_table_key_.table_type_;
}

uint64_t ObCopyTableKeyInfo::hash() const
{
  return src_table_key_.hash() + dest_table_key_.hash();
}

bool ObCopyTableKeyInfo::operator ==(const ObCopyTableKeyInfo &other) const
{
  return src_table_key_ == other.src_table_key_
      && dest_table_key_ == other.dest_table_key_;
}

OB_SERIALIZE_MEMBER(ObCopyTableKeyInfo, src_table_key_, dest_table_key_);

/******************ObCopyMacroRangeInfo*********************/
ObCopyMacroRangeInfo::ObCopyMacroRangeInfo()
  : start_macro_block_id_(),
    end_macro_block_id_(),
    macro_block_count_(0),
    is_leader_restore_(false),
    start_macro_block_end_key_(datums_, OB_INNER_MAX_ROWKEY_COLUMN_NUMBER),
    allocator_("CopyMacroRange")
{
}

ObCopyMacroRangeInfo::~ObCopyMacroRangeInfo()
{
}

void ObCopyMacroRangeInfo::reset()
{
  start_macro_block_id_.reset();
  end_macro_block_id_.reset();
  macro_block_count_ = 0;
  start_macro_block_end_key_.reset();
  is_leader_restore_ = false;
  allocator_.reset();
}

void ObCopyMacroRangeInfo::reuse()
{
  start_macro_block_id_.reset();
  end_macro_block_id_.reset();
  macro_block_count_ = 0;
  is_leader_restore_ = false;
  start_macro_block_end_key_.datums_ = datums_;
  start_macro_block_end_key_.datum_cnt_ = OB_INNER_MAX_ROWKEY_COLUMN_NUMBER;
  start_macro_block_end_key_.reuse();
  allocator_.reuse();
}

bool ObCopyMacroRangeInfo::is_valid() const
{
  bool bool_ret = false;
  bool_ret = start_macro_block_id_.is_valid()
      && end_macro_block_id_.is_valid()
      && macro_block_count_ > 0;

  if (bool_ret) {
    if (is_leader_restore_) {
    } else {
      bool_ret = start_macro_block_end_key_.is_valid();
    }
  }
  return bool_ret;
}

int ObCopyMacroRangeInfo::deep_copy_start_end_key(
    const blocksstable::ObDatumRowkey &start_macro_block_end_key)
{
  int ret = OB_SUCCESS;
  if (!start_macro_block_end_key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("deep copy start end key get invalid argument", K(ret), K(start_macro_block_end_key));
  } else if (OB_FAIL(start_macro_block_end_key.deep_copy(start_macro_block_end_key_, allocator_))) {
    LOG_WARN("failed to copy start macro block end key", K(ret), K(start_macro_block_end_key));
  }
  return ret;
}

int ObCopyMacroRangeInfo::assign(const ObCopyMacroRangeInfo &macro_range_info)
{
  int ret = OB_SUCCESS;
  if (!macro_range_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("copy macro range info is invalid", K(ret), K(macro_range_info));
  } else if (OB_FAIL(deep_copy_start_end_key(macro_range_info.start_macro_block_end_key_))) {
    LOG_WARN("failed to deep copy start end key", K(ret), K(macro_range_info));
  } else {
    start_macro_block_id_ = macro_range_info.start_macro_block_id_;
    end_macro_block_id_ = macro_range_info.end_macro_block_id_;
    macro_block_count_ = macro_range_info.macro_block_count_;
    is_leader_restore_ = macro_range_info.is_leader_restore_;
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObCopyMacroRangeInfo,
    start_macro_block_id_, end_macro_block_id_, macro_block_count_, is_leader_restore_, start_macro_block_end_key_);

/******************ObCopyMacroRangeInfo*********************/
ObCopySSTableMacroRangeInfo::ObCopySSTableMacroRangeInfo()
  : copy_table_key_(),
    copy_macro_range_array_()
{
  lib::ObMemAttr attr(MTL_ID(), "MacroRangeInfo");
  copy_macro_range_array_.set_attr(attr);
}

ObCopySSTableMacroRangeInfo::~ObCopySSTableMacroRangeInfo()
{
}

void ObCopySSTableMacroRangeInfo::reset()
{
  copy_table_key_.reset();
  copy_macro_range_array_.reset();
}

bool ObCopySSTableMacroRangeInfo::is_valid() const
{
  return copy_table_key_.is_valid()
      && copy_macro_range_array_.count() >= 0;
}

int ObCopySSTableMacroRangeInfo::assign(const ObCopySSTableMacroRangeInfo &sstable_macro_range_info)
{
  int ret = OB_SUCCESS;
  if (!sstable_macro_range_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("copy sstable macro range info is invalid", K(ret), K(sstable_macro_range_info));
  } else if (OB_FAIL(copy_macro_range_array_.assign(sstable_macro_range_info.copy_macro_range_array_))) {
    LOG_WARN("failed to assign sstable macro range info", K(ret), K(sstable_macro_range_info));
  } else {
    copy_table_key_ = sstable_macro_range_info.copy_table_key_;
  }
  return ret;
}

/******************ObLSRebuildStatus*********************/
ObLSRebuildStatus::ObLSRebuildStatus()
  : status_(NONE)
{
}

ObLSRebuildStatus::ObLSRebuildStatus(const STATUS &status)
 : status_(status)
{
}

ObLSRebuildStatus &ObLSRebuildStatus::operator=(const ObLSRebuildStatus &status)
{
  if (this != &status) {
    status_ = status.status_;
  }
  return *this;
}

ObLSRebuildStatus &ObLSRebuildStatus::operator=(const STATUS &status)
{
  status_ = status;
  return *this;
}

void ObLSRebuildStatus::reset()
{
  status_ = MAX;
}

bool ObLSRebuildStatus::is_valid() const
{
  return status_ >= NONE && status_ < MAX;
}

int ObLSRebuildStatus::set_status(int32_t status)
{
  int ret = OB_SUCCESS;
  if (status < NONE|| status >= MAX) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid status", K(ret), K(status));
  } else {
    status_ = static_cast<STATUS>(status);
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObLSRebuildStatus, status_);

/******************ObLSRebuildType*********************/
ObLSRebuildType::ObLSRebuildType()
  : type_(NONE)
{
}

ObLSRebuildType::ObLSRebuildType(const TYPE &type)
  : type_(type)
{
}

ObLSRebuildType &ObLSRebuildType::operator=(const ObLSRebuildType &type)
{
  if (this != &type) {
    type_ = type.type_;
  }
  return *this;
}

ObLSRebuildType &ObLSRebuildType::operator=(const TYPE &type)
{
  type_ = type;
  return *this;
}

void ObLSRebuildType::reset()
{
  type_ = MAX;
}

bool ObLSRebuildType::is_valid() const
{
  return type_ >= NONE && type_ < MAX;
}

int ObLSRebuildType::set_type(int32_t type)
{
  int ret = OB_SUCCESS;
  if (type < NONE|| type >= MAX) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid type", K(ret), K(type));
  } else {
    type_ = static_cast<TYPE>(type);
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObLSRebuildType, type_);

/******************ObLSRebuildInfo*********************/
ObLSRebuildInfo::ObLSRebuildInfo()
  : status_(),
    type_()
{
}

void ObLSRebuildInfo::reset()
{
  status_.reset();
  type_.reset();
}

bool ObLSRebuildInfo::is_valid() const
{
  return status_.is_valid()
      && type_.is_valid()
      && ((ObLSRebuildStatus::NONE == status_ && ObLSRebuildType::NONE == type_)
          || (ObLSRebuildStatus::NONE != status_ && ObLSRebuildType::NONE != type_));
}

bool ObLSRebuildInfo::is_in_rebuild() const
{
  return ObLSRebuildStatus::NONE != status_;
}

bool ObLSRebuildInfo::operator ==(const ObLSRebuildInfo &other) const
{
  return status_ == other.status_
      && type_ == other.type_;
}

OB_SERIALIZE_MEMBER(ObLSRebuildInfo, status_, type_);

ObTabletBackfillInfo::ObTabletBackfillInfo()
  : tablet_id_(),
    is_committed_(false)
{}

int ObTabletBackfillInfo::init(const common::ObTabletID &tablet_id, bool is_committed)
{
  int ret = OB_SUCCESS;
  if (!tablet_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tablet it", K(ret), K(tablet_id));
  } else {
    tablet_id_ = tablet_id;
    is_committed_ = is_committed;
  }
  return ret;
}

void ObTabletBackfillInfo::reset()
{
  tablet_id_.reset();
  is_committed_ = false;
}

bool ObTabletBackfillInfo::is_valid() const
{
  return tablet_id_.is_valid();
}

bool ObTabletBackfillInfo::operator == (const ObTabletBackfillInfo &other) const
{
  bool is_same = true;
  if (this == &other) {
    // same
  } else if (tablet_id_ != other.tablet_id_
      || is_committed_ != other.is_committed_) {
    is_same = false;
  } else {
    is_same = true;
  }
  return is_same;
}

/******************ObBackfillTabletsTableMgr*********************/
ObBackfillTabletsTableMgr::ObTabletTableMgr::ObTabletTableMgr()
  : is_inited_(false),
    tablet_id_(),
    transfer_seq_(0),
    max_major_end_scn_(SCN::min_scn()),
    allocator_("Backfill"),
    table_handle_array_()
{
}

ObBackfillTabletsTableMgr::ObTabletTableMgr::~ObTabletTableMgr()
{
}

int ObBackfillTabletsTableMgr::ObTabletTableMgr::init(
    const common::ObTabletID &tablet_id,
    const int64_t transfer_seq)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("tablet table mgr init twice", K(ret));
  } else if (!tablet_id.is_valid() || transfer_seq < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("init tablet table mgr get invalid argument", K(ret), K(tablet_id), K(transfer_seq));
  } else {
    tablet_id_ = tablet_id;
    transfer_seq_ = transfer_seq;
    is_inited_ = true;
  }
  return ret;
}

int ObBackfillTabletsTableMgr::ObTabletTableMgr::add_sstable(
    const int64_t transfer_seq,
    const share::SCN &transfer_start_scn,
    ObTableHandleV2 &table_handle)
{
  int ret = OB_SUCCESS;
  ObITable *table = nullptr;
  ObTableHandleV2 new_sstable_handle;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet table mgr do not init", K(ret));
  } else if (!table_handle.is_valid() || transfer_seq < 0 || !transfer_start_scn.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("table handle should not be invalid", K(ret), K(tablet_id_), K(table_handle), K(transfer_seq), K(transfer_start_scn));
  } else if (transfer_seq != transfer_seq_) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("tablet transfer seq not match, cannot add sstable", K(ret), K(transfer_seq_), K(transfer_seq));
  } else if (OB_ISNULL(table = table_handle.get_table())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table should not be NULL", K(ret), KP(table), K(table_handle), K(tablet_id_), K(transfer_seq_));
  } else if (!table->is_sstable()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table is not sstable, unexpected", K(ret), KPC(table));
  } else {
    ObSSTable *sstable = static_cast<ObSSTable *>(table);
    ObSSTable *orig_sstable = nullptr;
    ObSSTable *copied_sstable = nullptr;
    ObMetaDiskAddr addr;
    addr.set_mem_addr(0, sizeof(ObSSTable));
    ObStorageMetaHandle sstable_handle;
    if (sstable->is_loaded()) {
      orig_sstable = sstable;
    } else if (OB_FAIL(ObTabletTableStore::load_sstable(
        sstable->get_addr(), sstable->is_co_sstable(), sstable_handle))) {
      LOG_WARN("failed to load sstable", K(ret), KPC(sstable));
    } else if (OB_FAIL(sstable_handle.get_sstable(orig_sstable))) {
      LOG_WARN("failed to get sstable from sstable handle", K(ret), K(sstable_handle));
    }

    if (FAILEDx(orig_sstable->copy_from_old_sstable(*orig_sstable, allocator_, copied_sstable))) {
      LOG_WARN("failed to copy from old sstable", K(ret), KPC(orig_sstable), KP(copied_sstable));
    } else if (OB_FAIL(copied_sstable->set_addr(addr))) {
      LOG_WARN("failed to set sstable addr", K(ret), K(addr), KPC(copied_sstable));
    } else if (OB_FAIL(new_sstable_handle.set_sstable(copied_sstable, &allocator_))) {
      LOG_WARN("failed to set sstable", K(ret), K(addr), KPC(copied_sstable));
    } else if (OB_FAIL(table_handle_array_.add_table(new_sstable_handle))) {
      LOG_WARN("failed to push table into array", K(ret), K(tablet_id_));
    }
  }
  return ret;
}

int ObBackfillTabletsTableMgr::ObTabletTableMgr::get_all_sstables(
    ObTablesHandleArray &table_handle_array)
{
  int ret = OB_SUCCESS;
  table_handle_array.reset();

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet table mgr do not init", K(ret));
  } else if (OB_FAIL(table_handle_array.assign(table_handle_array_))) {
    LOG_WARN("failed to get table handle array", K(ret), K(tablet_id_));
  }
  return ret;
}

int ObBackfillTabletsTableMgr::ObTabletTableMgr::set_max_major_end_scn(
    const share::SCN &max_major_end_scn)
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet table mgr do not init", K(ret));
  } else if (!max_major_end_scn.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("set max major end scn get invalid argument", K(ret), K(max_major_end_scn));
  } else {
    max_major_end_scn_ = max_major_end_scn;
  }
  return ret;
}

int ObBackfillTabletsTableMgr::ObTabletTableMgr::get_max_major_end_scn(
    share::SCN &max_major_end_scn)
{
  int ret = OB_SUCCESS;
  max_major_end_scn.reset();

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet table mgr do not init", K(ret));
  } else {
    max_major_end_scn = max_major_end_scn_;
  }
  return ret;
}

ObBackfillTabletsTableMgr::ObBackfillTabletsTableMgr()
  : is_inited_(false),
    lock_(),
    map_(),
    local_rebuild_seq_(0),
    transfer_start_scn_()
{
}

ObBackfillTabletsTableMgr::~ObBackfillTabletsTableMgr()
{
  reuse();
}

void ObBackfillTabletsTableMgr::reuse()
{
  common::SpinWLockGuard guard(lock_);
  if (!map_.created()) {
  } else {
    for (TransferTableMap::iterator iter = map_.begin(); iter != map_.end(); ++iter) {
      ObTabletTableMgr *tablet_table_mgr = iter->second;
      tablet_table_mgr->~ObTabletTableMgr();
      mtl_free(tablet_table_mgr);
      tablet_table_mgr = nullptr;
    }
    map_.reuse();
  }
}

int ObBackfillTabletsTableMgr::init(const int64_t rebuild_seq, const share::SCN &transfer_start_scn)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("backfill tablets table mgr init twice", K(ret));
  } else if (rebuild_seq < 0 || !transfer_start_scn.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("backfill tablets table mgr init get invalid argument", K(ret), K(rebuild_seq), K(transfer_start_scn));
  } else if (OB_FAIL(map_.create(MAX_BUCKET_NUM, "BackfillMap"))) {
    LOG_WARN("failed to create backfill table mgr map", K(ret));
  } else {
    local_rebuild_seq_ = rebuild_seq;
    transfer_start_scn_ = transfer_start_scn;
    is_inited_ = true;
  }
  return ret;
}

int ObBackfillTabletsTableMgr::add_sstable(
    const common::ObTabletID &tablet_id,
    const int64_t rebuild_seq,
    const share::SCN &transfer_start_scn,
    const int64_t transfer_seq,
    ObTableHandleV2 &table_handle)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backfill tablets table mgr do not init", K(ret));
  } else if (!tablet_id.is_valid() || rebuild_seq < 0 || !transfer_start_scn.is_valid() || transfer_seq < 0 || !table_handle.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("add sstable get invalid argument", K(ret), K(tablet_id), K(rebuild_seq), K(transfer_start_scn), K(transfer_seq), K(table_handle));
  } else {
    common::SpinWLockGuard guard(lock_);
    if (rebuild_seq != local_rebuild_seq_ || transfer_start_scn != transfer_start_scn_) {
      ret = OB_STATE_NOT_MATCH;
      LOG_WARN("ls rebuild seq not match or transfer start scn is not match, cannot add sstable",
          K(ret), K(tablet_id), K(rebuild_seq), K(local_rebuild_seq_), K(transfer_start_scn), K(transfer_start_scn_));
    } else {
      ObTabletTableMgr *tablet_table_mgr = nullptr;
      int hash_ret = map_.get_refactored(tablet_id, tablet_table_mgr);
      if (OB_SUCCESS == hash_ret) {
        if (OB_FAIL(tablet_table_mgr->add_sstable(transfer_seq, transfer_start_scn, table_handle))) {
          LOG_WARN("failed to add sstable into tablet table mgr", K(ret), K(tablet_id), K(transfer_seq), K(table_handle));
        }
      } else {
        ret = hash_ret;
        LOG_WARN("failed to add sstable", K(ret), K(tablet_id), K(rebuild_seq), K(transfer_start_scn), K(transfer_seq), K(table_handle));
      }
    }
  }
  return ret;
}

int ObBackfillTabletsTableMgr::get_tablet_all_sstables(
    const common::ObTabletID &tablet_id, ObTablesHandleArray &table_handle_array)
{
  int ret = OB_SUCCESS;
  table_handle_array.reset();

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backfill tablets table mgr do not init", K(ret));
  } else if (!tablet_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get tablet all sstables get invalid argument", K(ret), K(tablet_id));
  } else {
    common::SpinRLockGuard guard(lock_);
    ObTabletTableMgr *tablet_table_mgr = nullptr;
    int hash_ret = map_.get_refactored(tablet_id, tablet_table_mgr);
    if (OB_SUCCESS == hash_ret) {
      if (OB_FAIL(tablet_table_mgr->get_all_sstables(table_handle_array))) {
        LOG_WARN("failed to get all sstables", K(ret), K(tablet_id));
      }
    } else {
      ret = hash_ret;
      LOG_WARN("tablet sstable do not exist", K(ret), K(tablet_id));
    }
  }
  return ret;
}

int ObBackfillTabletsTableMgr::init_tablet_table_mgr(const common::ObTabletID &tablet_id, const int64_t transfer_seq)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backfill tablets table mgr do not init", K(ret));
  } else if (!tablet_id.is_valid() || transfer_seq < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("init tablet table mgr get invalid argument", K(ret), K(tablet_id), K(transfer_seq));
  } else {
    common::SpinWLockGuard guard(lock_);
    ObTabletTableMgr *tablet_table_mgr = nullptr;
    int hash_ret = map_.get_refactored(tablet_id, tablet_table_mgr);
    if (OB_HASH_NOT_EXIST == hash_ret) {
      void *buf = NULL;
      tablet_table_mgr = nullptr;
      if (OB_ISNULL(buf = mtl_malloc(sizeof(ObTabletTableMgr), "TabletTableMgr"))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to alloc memory", K(ret), KP(buf));
      } else if (FALSE_IT(tablet_table_mgr = new (buf) ObTabletTableMgr())) {
      } else if (OB_FAIL(tablet_table_mgr->init(tablet_id, transfer_seq))) {
        LOG_WARN("failed to init tablet table mgr", K(ret), K(tablet_id));
      } else if (OB_FAIL(map_.set_refactored(tablet_id, tablet_table_mgr))) {
        LOG_WARN("failed to set tablet table mgr into map", K(ret), K(tablet_id));
      } else {
        tablet_table_mgr = nullptr;
      }
      if (OB_NOT_NULL(tablet_table_mgr)) {
        tablet_table_mgr->~ObTabletTableMgr();
        mtl_free(tablet_table_mgr);
        tablet_table_mgr = nullptr;
      }
    } else if (OB_SUCCESS == hash_ret) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tablet table mgr is already exist, unexpected", K(ret), K(tablet_id), KP(tablet_table_mgr));
    } else {
      ret = hash_ret;
      LOG_WARN("failed to get tablet table mgr", K(ret), K(tablet_id));
    }
  }
  return ret;
}

int ObBackfillTabletsTableMgr::remove_tablet_table_mgr(
    const common::ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  ObTabletTableMgr *tablet_table_mgr = nullptr;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backfill tablets table mgr do not init", K(ret));
  } else if (!tablet_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("remove tablet table key mgr get invalid argument", K(ret), K(tablet_id));
  } else {
    common::SpinWLockGuard guard(lock_);
    if (OB_FAIL(map_.erase_refactored(tablet_id, &tablet_table_mgr))) {
      LOG_WARN("failed to erase tablet table mgr", K(ret), K(tablet_id));
    } else if (nullptr == tablet_table_mgr) {
      //do nothing
    } else {
      tablet_table_mgr->~ObTabletTableMgr();
      mtl_free(tablet_table_mgr);
      tablet_table_mgr = nullptr;
    }
  }
  return ret;
}

int ObBackfillTabletsTableMgr::set_max_major_end_scn(
    const common::ObTabletID &tablet_id,
    const share::SCN &max_major_end_scn)
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backfill tablets table mgr do not init", K(ret));
  } else if (!tablet_id.is_valid() || !max_major_end_scn.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("set max major end scn get invalid argument", K(ret), K(tablet_id));
  } else {
    common::SpinWLockGuard guard(lock_);
    ObTabletTableMgr *tablet_table_mgr = nullptr;
    int hash_ret = map_.get_refactored(tablet_id, tablet_table_mgr);
    if (OB_SUCCESS == hash_ret) {
      if (OB_FAIL(tablet_table_mgr->set_max_major_end_scn(max_major_end_scn))) {
        LOG_WARN("failed to set max major end scn", K(ret), K(tablet_id), K(max_major_end_scn));
      }
    } else {
      ret = hash_ret;
      LOG_WARN("tablet table mgr do not exist", K(ret), K(tablet_id), K(max_major_end_scn));
    }
  }
  return ret;
}

int ObBackfillTabletsTableMgr::get_max_major_end_scn(
    const common::ObTabletID &tablet_id,
    share::SCN &max_major_end_scn)
{
  int ret = OB_SUCCESS;
  max_major_end_scn.reset();

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backfill tablets table mgr do not init", K(ret));
  } else if (!tablet_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get max major end scn get invalid argument", K(ret), K(tablet_id));
  } else {
    common::SpinRLockGuard guard(lock_);
    ObTabletTableMgr *tablet_table_mgr = nullptr;
    int hash_ret = map_.get_refactored(tablet_id, tablet_table_mgr);
    if (OB_SUCCESS == hash_ret) {
      if (OB_FAIL(tablet_table_mgr->get_max_major_end_scn(max_major_end_scn))) {
        LOG_WARN("failed to set max major end scn", K(ret), K(tablet_id), K(max_major_end_scn));
      }
    } else {
      ret = hash_ret;
      LOG_WARN("tablet table mgr do not exist", K(ret), K(tablet_id), K(max_major_end_scn));
    }
  }
  return ret;
}

int ObBackfillTabletsTableMgr::get_local_rebuild_seq(int64_t &local_rebuild_seq)
{
  int ret = OB_SUCCESS;
  local_rebuild_seq = 0;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backfill tablets table mgr do not init", K(ret));
  } else {
    common::SpinRLockGuard guard(lock_);
    local_rebuild_seq = local_rebuild_seq_;
  }
  return ret;
}


}
}

