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

#ifndef SRC_SHARE_BACKUP_OB_LOG_ARCHIVE_BACKUP_INFO_MGR_H_
#define SRC_SHARE_BACKUP_OB_LOG_ARCHIVE_BACKUP_INFO_MGR_H_

#include "lib/mysqlclient/ob_isql_client.h"
#include "ob_backup_info_mgr.h"
#include "ob_backup_struct.h"
#include "ob_backup_path.h"

namespace oceanbase
{
namespace share
{
class ObIBackupLeaseService;

class ObExternLogArchiveBackupInfo final
{
  static const uint8_t VERSION = 1;
  static const uint8_t LOG_ARCHIVE_BACKUP_INFO_FILE_VERSION = 1;
  static const uint8_t LOG_ARCHIVE_BACKUP_INFO_CONTENT_VERSION = 1;
  OB_UNIS_VERSION(VERSION);
public:
  ObExternLogArchiveBackupInfo();
  ~ObExternLogArchiveBackupInfo();

  void reset();
  bool is_valid() const;
  int64_t get_write_buf_size() const;
  int write_buf(char *buf, const int64_t buf_len, int64_t &pos) const;
  int read_buf(const char *buf, const int64_t buf_len);

  int update(const ObTenantLogArchiveStatus &status);
  int get_last(ObTenantLogArchiveStatus &status);
  int get_log_archive_status(const int64_t restore_timestamp, ObTenantLogArchiveStatus &status);
  int get_log_archive_status(common::ObIArray<ObTenantLogArchiveStatus> &status_array);
  int mark_log_archive_deleted(const common::ObIArray<int64_t> &round_ids);
  int delete_marked_log_archive_info(const common::ObIArray<int64_t> &round_ids);
  bool is_empty() const { return status_array_.empty(); }

  TO_STRING_KV(K_(status_array));
private:
  common::ObSArray<ObTenantLogArchiveStatus> status_array_;
  DISALLOW_COPY_AND_ASSIGN(ObExternLogArchiveBackupInfo);
};

}//share
}//oceanbase

#endif /* SRC_SHARE_BACKUP_OB_LOG_ARCHIVE_BACKUP_INFO_MGR_H_ */
