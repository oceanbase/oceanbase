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
#include "ob_restore_compatibility_util.h"
#include "storage/high_availability/ob_storage_ha_utils.h"

using namespace oceanbase;
using namespace share;
using namespace storage;

ObRestoreCompatibilityUtil::ObRestoreCompatibilityUtil(const ObBackupSetFileDesc::Compatible compatible)
  : backup_compatible_(compatible)
{

}


int ObRestoreCompatibilityUtil::is_tablet_restore_phase_done(
    const ObLSID &ls_id,
    const ObLSRestoreStatus &ls_restore_status,
    const ObTabletHandle &tablet_handle,
    bool &is_finish) const
{
  int ret = OB_SUCCESS;

  if (ObBackupSetFileDesc::is_allow_quick_restore(backup_compatible_)) {
    if (OB_FAIL(is_tablet_restore_phase_done_(ls_id, ls_restore_status, tablet_handle, is_finish))) {
      LOG_WARN("failed to check tablet restore finished", K(ret));
    }
  } else {
    if (OB_FAIL(is_tablet_restore_phase_done_prev_v4_(ls_id, ls_restore_status, tablet_handle, is_finish))) {
      LOG_WARN("failed to check tablet restore finished prev version 432", K(ret));
    }
  }

  return ret;
}

ObTabletRestoreAction::ACTION ObRestoreCompatibilityUtil::get_restore_action(
    const ObLSID &ls_id,
    const ObLSRestoreStatus &ls_restore_status) const
{
  ObTabletRestoreAction::ACTION action = ObTabletRestoreAction::RESTORE_NONE;
  if (ObBackupSetFileDesc::is_allow_quick_restore(backup_compatible_)) {
    action = get_restore_action_(ls_id, ls_restore_status);
  } else {
    action = get_restore_action_prev_v4_(ls_id, ls_restore_status);
  }

  return action;
}

int ObRestoreCompatibilityUtil::is_tablet_restore_phase_done_(
    const ObLSID &ls_id,
    const ObLSRestoreStatus &ls_restore_status,
    const ObTabletHandle &tablet_handle,
    bool &is_finish) const
{
  int ret = OB_SUCCESS;
  const ObTabletMeta &tablet_meta = tablet_handle.get_obj()->get_tablet_meta();
  const ObTabletHAStatus &ha_status = tablet_meta.ha_status_;

  switch (ls_restore_status.get_status()) {
    case ObLSRestoreStatus::RESTORE_TABLETS_META :
    case ObLSRestoreStatus::WAIT_RESTORE_TABLETS_META : {
      is_finish = !ha_status.is_restore_status_pending();
      break;
    }

    case ObLSRestoreStatus::RESTORE_TO_CONSISTENT_SCN :
    case ObLSRestoreStatus::WAIT_RESTORE_TO_CONSISTENT_SCN : {
      // FULL tablets whose has_transfer_table flag is true must not be exist after log has been
      // recovered to consistent scn. As the data of table store cannot be at the transfer source tablets,
      // but rather in backup sets.
      is_finish = !(ha_status.is_restore_status_full() && tablet_meta.has_transfer_table());
      break;
    }

    case ObLSRestoreStatus::QUICK_RESTORE: {
      if (ls_id.is_sys_ls()) {
        is_finish = ha_status.is_restore_status_full();
      } else {
        is_finish = !ha_status.is_restore_status_empty();
      }
      break;
    }

    case ObLSRestoreStatus::WAIT_QUICK_RESTORE:
    case ObLSRestoreStatus::QUICK_RESTORE_FINISH: {
      if (ls_id.is_sys_ls()) {
        is_finish = ha_status.is_restore_status_full();
      } else {
        if (ha_status.is_restore_status_remote()
            || ha_status.is_restore_status_full()) {
          is_finish = true;
        } else if (ha_status.is_restore_status_undefined()) {
          // UNDEFINED should be deleted after log has been recovered.
          bool is_deleted = true;
          if (OB_FAIL(ObStorageHAUtils::check_tablet_is_deleted(tablet_handle, is_deleted))) {
            LOG_WARN("failed to check tablet is deleted", K(ret), K(tablet_meta));
          } else {
            is_finish = is_deleted;
          }
        } else {
          is_finish = false;
        }
      }
      break;
    }

    case ObLSRestoreStatus::RESTORE_MAJOR_DATA : {
      is_finish = !ha_status.is_restore_status_remote();
      break;
    }

    case ObLSRestoreStatus::WAIT_RESTORE_MAJOR_DATA : {
      if (ha_status.is_restore_status_full()) {
        is_finish = true;
      } else if (ha_status.is_restore_status_undefined()) {
        // UNDEFINED should be deleted after log has been recovered.
        bool is_deleted = true;
        if (OB_FAIL(ObStorageHAUtils::check_tablet_is_deleted(tablet_handle, is_deleted))) {
          LOG_WARN("failed to check tablet is deleted", K(ret), K(tablet_meta));
        } else {
          is_finish = is_deleted;
        }
      } else {
        is_finish = false;
      }
      break;
    }

    default: {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to check tablet is deleted", K(ret), K(ls_id), K(ls_restore_status), K(tablet_meta));
      break;
    }
  }

  return ret;
}

int ObRestoreCompatibilityUtil::is_tablet_restore_phase_done_prev_v4_(
    const ObLSID &ls_id,
    const ObLSRestoreStatus &ls_restore_status,
    const ObTabletHandle &tablet_handle,
    bool &is_finish) const
{
  int ret = OB_SUCCESS;
  const ObTabletMeta &tablet_meta = tablet_handle.get_obj()->get_tablet_meta();
  const ObTabletHAStatus &ha_status = tablet_meta.ha_status_;

  switch (ls_restore_status.get_status()) {
    case ObLSRestoreStatus::RESTORE_TABLETS_META :
    case ObLSRestoreStatus::WAIT_RESTORE_TABLETS_META : {
      is_finish = !ha_status.is_restore_status_pending();
      break;
    }

    case ObLSRestoreStatus::RESTORE_TO_CONSISTENT_SCN :
    case ObLSRestoreStatus::WAIT_RESTORE_TO_CONSISTENT_SCN : {
      // FULL tablets whose has_transfer_table flag is true must not be exist after log has been
      // recovered to consistent scn. As the data of table store cannot be at the transfer source tablets,
      // but rather in backup sets.
      is_finish = !(ha_status.is_restore_status_full() && tablet_meta.has_transfer_table());
      break;
    }

    case ObLSRestoreStatus::QUICK_RESTORE: {
      is_finish = !ha_status.is_restore_status_empty();
      break;
    }

    case ObLSRestoreStatus::WAIT_QUICK_RESTORE:
    case ObLSRestoreStatus::QUICK_RESTORE_FINISH: {
      if (ha_status.is_restore_status_minor_and_major_meta()
          || ha_status.is_restore_status_full()) {
        is_finish = true;
      } else if (ha_status.is_restore_status_undefined()) {
        // UNDEFINED should be deleted after log has been recovered.
        bool is_deleted = true;
        if (OB_FAIL(ObStorageHAUtils::check_tablet_is_deleted(tablet_handle, is_deleted))) {
          LOG_WARN("failed to check tablet is deleted", K(ret), K(tablet_meta));
        } else {
          is_finish = is_deleted;
        }
      } else {
        is_finish = false;
      }
      break;
    }

    case ObLSRestoreStatus::RESTORE_MAJOR_DATA : {
      is_finish = !ha_status.is_restore_status_minor_and_major_meta();
      break;
    }

    case ObLSRestoreStatus::WAIT_RESTORE_MAJOR_DATA : {
      if (ha_status.is_restore_status_full()) {
        is_finish = true;
      } else if (ha_status.is_restore_status_undefined()) {
        // UNDEFINED should be deleted after log has been recovered.
        bool is_deleted = true;
        if (OB_FAIL(ObStorageHAUtils::check_tablet_is_deleted(tablet_handle, is_deleted))) {
          LOG_WARN("failed to check tablet is deleted", K(ret), K(tablet_meta));
        } else {
          is_finish = is_deleted;
        }
      } else {
        is_finish = false;
      }
      break;
    }

    default: {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to check tablet is deleted", K(ret), K(ls_id), K(ls_restore_status), K(tablet_meta));
      break;
    }
  }

  return ret;
}

ObTabletRestoreAction::ACTION ObRestoreCompatibilityUtil::get_restore_action_(
    const ObLSID &ls_id,
    const ObLSRestoreStatus &ls_restore_status) const
{
  ObTabletRestoreAction::ACTION action = ObTabletRestoreAction::RESTORE_NONE;
  switch (ls_restore_status.get_status()) {
    case ObLSRestoreStatus::RESTORE_TABLETS_META : {
      action = ObTabletRestoreAction::RESTORE_TABLET_META;
      break;
    }

    case ObLSRestoreStatus::QUICK_RESTORE: {
      if (ls_id.is_sys_ls()) {
        action = ObTabletRestoreAction::RESTORE_ALL;
      } else {
        action = ObTabletRestoreAction::RESTORE_REMOTE_SSTABLE;
      }
      break;
    }

    case ObLSRestoreStatus::RESTORE_MAJOR_DATA : {
      if (ls_id.is_user_ls()) {
        action = ObTabletRestoreAction::RESTORE_REPLACE_REMOTE_SSTABLE;
      }
      break;
    }

    default: {
      action = ObTabletRestoreAction::RESTORE_NONE;
      break;
    }
  }

  return action;
}

ObTabletRestoreAction::ACTION ObRestoreCompatibilityUtil::get_restore_action_prev_v4_(
    const ObLSID &ls_id,
    const ObLSRestoreStatus &ls_restore_status) const
{
  ObTabletRestoreAction::ACTION action = ObTabletRestoreAction::RESTORE_NONE;
  switch (ls_restore_status.get_status()) {
    case ObLSRestoreStatus::RESTORE_TABLETS_META : {
      action = ObTabletRestoreAction::RESTORE_TABLET_META;
      break;
    }

    case ObLSRestoreStatus::QUICK_RESTORE: {
      action = ObTabletRestoreAction::RESTORE_MINOR;
      break;
    }

    case ObLSRestoreStatus::RESTORE_MAJOR_DATA : {
      action = ObTabletRestoreAction::RESTORE_MAJOR;
      break;
    }

    default: {
      action = ObTabletRestoreAction::RESTORE_NONE;
      break;
    }
  }

  UNUSED(ls_id);

  return action;
}