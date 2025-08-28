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

#ifndef OCEANBASE_STORAGE_TABLET_RESTORE_COMPATIBILITY_UTIL_H
#define OCEANBASE_STORAGE_TABLET_RESTORE_COMPATIBILITY_UTIL_H

#include "share/ob_ls_id.h"
#include "share/restore/ob_ls_restore_status.h"
#include "share/backup/ob_backup_struct.h"
#include "storage/meta_mem/ob_tablet_handle.h"
#include "storage/tablet/ob_tablet.h"
#include "storage/high_availability/ob_storage_restore_struct.h"

namespace oceanbase
{
namespace storage
{
class ObRestoreCompatibilityUtil final
{
public:
  explicit ObRestoreCompatibilityUtil(const share::ObBackupSetFileDesc::Compatible compatible);
  
  int is_tablet_restore_phase_done(
      const share::ObLSID &ls_id,
      const share::ObLSRestoreStatus &ls_restore_status,
      const ObTabletHandle &tablet_handle, 
      bool &is_finish) const;
  ObTabletRestoreAction::ACTION get_restore_action(const share::ObLSID &ls_id, const share::ObLSRestoreStatus &ls_restore_status) const;
  TO_STRING_KV(K_(backup_compatible));

private:
  int is_tablet_restore_phase_done_(
      const share::ObLSID &ls_id,
      const share::ObLSRestoreStatus &ls_restore_status,
      const ObTabletHandle &tablet_handle, 
      bool &is_finish) const;

  int is_tablet_restore_phase_done_prev_v4_(
      const share::ObLSID &ls_id,
      const share::ObLSRestoreStatus &ls_restore_status,
      const ObTabletHandle &tablet_handle, 
      bool &is_finish) const;
  
  ObTabletRestoreAction::ACTION get_restore_action_(const share::ObLSID &ls_id, const share::ObLSRestoreStatus &ls_restore_status) const;
  ObTabletRestoreAction::ACTION get_restore_action_prev_v4_(const share::ObLSID &ls_id, const share::ObLSRestoreStatus &ls_restore_status) const;

  share::ObBackupSetFileDesc::Compatible backup_compatible_;

  DISALLOW_COPY_AND_ASSIGN(ObRestoreCompatibilityUtil);
};


}
}

#endif