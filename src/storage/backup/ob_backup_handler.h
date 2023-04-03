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
      const share::SCN &start_scn, const share::SCN &end_scn);
};

}  // namespace backup
}  // namespace oceanbase

#endif
