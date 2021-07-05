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

#ifndef _SHARE_OB_ALL_VIRTUAL_SERVER_BACKUP_LOG_ARCHIVE_STATUS_H_
#define _SHARE_OB_ALL_VIRTUAL_SERVER_BACKUP_LOG_ARCHIVE_STATUS_H_
#include "share/ob_virtual_table_iterator.h"  // ObVirtualTableIterator
#include "share/backup/ob_backup_struct.h"
namespace oceanbase {
namespace storage {
class ObPartitionService;
class ObIPartitionGroupIterator;
}  // namespace storage

namespace share {
struct ObGetTenantLogArchiveStatusArg;
struct ObTenantLogArchiveStatusWrapper;
}  // namespace share
namespace observer {
class ObAllVirtualServerBackupLogArchiveStatus : public common::ObVirtualTableIterator {
  enum SERVER_BACKUP_LOG_ARCHIVE_STATUS_COLUMN {
    SVR_IP = oceanbase::common::OB_APP_MIN_COLUMN_ID,
    SVR_PORT,
    INCARNATION,
    LOG_ARCHIVE_ROUND,
    START_TS,
    CUR_LOG_ARCHIVE_PROGRESS,
    PG_COUNT
  };

public:
  ObAllVirtualServerBackupLogArchiveStatus();
  virtual ~ObAllVirtualServerBackupLogArchiveStatus();

public:
  virtual int inner_get_next_row(common::ObNewRow*& row);
  int init(storage::ObPartitionService* partition_service, common::ObAddr& addr);

private:
  int fill_rows_();
  int prepare_to_read_();
  void finish_read_();

private:
  bool is_inited_;
  bool has_reported_;
  storage::ObPartitionService* ps_;
  storage::ObIPartitionGroupIterator* iter_;
  common::ObAddr addr_;
  char ip_buf_[common::OB_IP_STR_BUFF];

  int64_t incarnation_;
  int64_t round_;
  int64_t pg_count_;

  int64_t start_archive_time_;
  int64_t max_archive_progress_;
};
}  // namespace observer
}  // namespace oceanbase
#endif  //_SHARE_OB_ALL_VIRTUAL_SERVER_BACKUP_LOG_ARCHIVE_STATUS_H_
