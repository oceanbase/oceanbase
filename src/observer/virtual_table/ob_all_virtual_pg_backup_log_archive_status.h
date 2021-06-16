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

#ifndef OCEANBASE_VIRTUAL_TABLE_OB_ALL_VIRTUAL_PG_BACKUP_LOG_ARCHIVER_STATUS_H
#define OCEANBASE_VIRTUAL_TABLE_OB_ALL_VIRTUAL_PG_BACKUP_LOG_ARCHIVER_STATUS_H

#include "share/ob_virtual_table_iterator.h"  // ObVirtualTableIterator
#include "share/backup/ob_backup_struct.h"
namespace oceanbase {
namespace storage {
class ObPartitionService;
class ObIPartitionGroupIterator;
}  // namespace storage
namespace observer {
class ObAllVirtualPGBackupLogArchiveStatus : public common::ObVirtualTableIterator {
  enum PG_BACKUP_LOG_ARCHIVE_STATUS_COLUMN {
    SVR_IP = oceanbase::common::OB_APP_MIN_COLUMN_ID,
    SVR_PORT,
    TENANT_ID,
    TABLE_ID,
    PARTITION_ID,
    INCARNATION,
    LOG_ARCHIVE_ROUND,
    START_TS,
    STATUS,
    LAST_ARCHIVED_LOG_ID,
    LAST_ARCHIVED_LOG_TS,
    MAX_LOG_ID,
    MAX_LOG_TS,
  };

public:
  ObAllVirtualPGBackupLogArchiveStatus();
  virtual ~ObAllVirtualPGBackupLogArchiveStatus();

public:
  virtual int inner_get_next_row(common::ObNewRow*& row);
  void reset();
  int init(storage::ObPartitionService* partition_service, common::ObAddr& addr);

private:
  int inner_get_next_row_(common::ObNewRow*& row);
  const char* get_log_archive_status_str_(share::ObLogArchiveStatus::STATUS status);

private:
  bool is_inited_;
  storage::ObPartitionService* ps_;
  common::ObAddr addr_;
  storage::ObIPartitionGroupIterator* ptt_iter_;
  char ip_buf_[common::OB_IP_STR_BUFF];
};
}  // namespace observer
}  // namespace oceanbase
#endif  // OCEANBASE_VIRTUAL_TABLE_OB_ALL_VIRTUAL_PG_BACKUP_LOG_ARCHIVER_STATUS_H
