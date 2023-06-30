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

#ifndef OCEABASE_STORAGE_LS_RESTORE_ARGS_H
#define OCEABASE_STORAGE_LS_RESTORE_ARGS_H

#include "lib/string/ob_string.h"
#include "lib/container/ob_array.h"
#include "share/ob_ls_id.h"
#include "share/restore/ob_restore_type.h"
#include "share/backup/ob_backup_struct.h"
#include "storage/backup/ob_backup_data_struct.h"
#include "share/backup/ob_backup_path.h"

namespace oceanbase
{
namespace storage
{

// log stream restore context
struct ObTenantRestoreCtx 
{
  ObTenantRestoreCtx();
  ~ObTenantRestoreCtx();

  bool is_valid() const;
  int assign(const ObTenantRestoreCtx &args);
  int64_t get_job_id() const { return job_id_; }
  const share::ObRestoreType &get_restore_type() const { return restore_type_; }
  const share::SCN &get_restore_scn() const { return restore_scn_; }
  const share::SCN &get_consistent_scn() const { return consistent_scn_; }
  uint64_t get_tenant_id() const { return tenant_id_; }
  uint64_t get_backup_cluster_version() const { return backup_cluster_version_; }
  uint64_t get_backup_data_version() const { return backup_data_version_; }
  const common::ObArray<share::ObRestoreBackupSetBriefInfo> &get_backup_set_list() const { return backup_set_list_; }
  const common::ObArray<share::ObBackupPiecePath> &get_backup_piece_list() const { return backup_piece_list_; }
  TO_STRING_KV(
      K_(job_id), 
      K_(restore_type), 
      K_(restore_scn), 
      K_(consistent_scn),
      K_(tenant_id), 
      K_(backup_cluster_version),
      K_(backup_data_version),
      K_(backup_set_list), 
      K_(backup_piece_list));
  
  int64_t job_id_;
  share::ObRestoreType restore_type_; // quick restore or normal restore
  share::SCN restore_scn_; // restore end scn
  share::SCN consistent_scn_;
  uint64_t tenant_id_;
  uint64_t backup_cluster_version_;
  uint64_t backup_data_version_;
  // every set path is integral.
  common::ObArray<share::ObRestoreBackupSetBriefInfo> backup_set_list_;
  // every piece path is integral.
  common::ObArray<share::ObBackupPiecePath> backup_piece_list_;
};

}
}

#endif



