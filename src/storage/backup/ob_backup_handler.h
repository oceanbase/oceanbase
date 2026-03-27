/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef STORAGE_LOG_STREAM_BACKUP_HANDLER_H_
#define STORAGE_LOG_STREAM_BACKUP_HANDLER_H_

#include "common/ob_tablet_id.h"
#include "share/ob_ls_id.h"
#include "storage/backup/ob_backup_data_struct.h"

namespace oceanbase {
namespace backup {

class ObBackupHandler {
public:
  static int schedule_backup_meta_dag(const ObBackupJobDesc &job_desc, const share::ObBackupDest &backup_dest,
      const uint64_t tenant_id, const share::ObBackupSetDesc &backup_set_desc, const share::ObLSID &ls_id,
      const int64_t turn_id, const int64_t retry_id, const share::SCN &start_scn);
  static int schedule_backup_data_dag(const ObBackupJobDesc &job_desc, const share::ObBackupDest &backup_dest,
      const uint64_t tenant_id, const share::ObBackupSetDesc &backup_set_desc, const share::ObLSID &ls_id,
      const int64_t turn_id, const int64_t retry_id, const share::ObBackupDataType &backup_data_type);
  static int schedule_build_tenant_level_index_dag(const ObBackupJobDesc &job_desc,
      const share::ObBackupDest &backup_dest, const uint64_t tenant_id, const share::ObBackupSetDesc &backup_set_desc,
      const int64_t turn_id, const int64_t retry_id, const share::ObBackupDataType &backup_data_type);
  static int schedule_backup_complement_log_dag(const ObBackupJobDesc &job_desc, const share::ObBackupDest &backup_dest,
      const uint64_t tenant_id, const share::ObBackupSetDesc &backup_set_desc, const share::ObLSID &ls_id,
      const share::SCN &start_scn, const share::SCN &end_scn, const bool is_only_calc_stat);
  static int schedule_backup_fuse_tablet_meta_dag(const ObBackupJobDesc &job_desc, const share::ObBackupDest &backup_dest,
      const uint64_t tenant_id, const share::ObBackupSetDesc &backup_set_desc, const share::ObLSID &ls_id, const int64_t turn_id, const int64_t retry_id);

#ifdef OB_BUILD_SHARED_STORAGE
public:
  static int schedule_ss_backup_dag(const ObBackupJobDesc &job_desc, const share::ObBackupDest &backup_dest,
      const uint64_t tenant_id, const share::ObBackupSetDesc &backup_set_desc,
      const share::ObLSID &ls_id, const int64_t retry_id, const share::SCN &start_scn, const bool is_backup_data);
#endif
};

}  // namespace backup
}  // namespace oceanbase

#endif
