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

#ifndef SRC_OBSERVER_VIRTUAL_TABLE_OB_ALL_VIRTUAL_BACKUPSET_HISTORY_MGR_H_
#define SRC_OBSERVER_VIRTUAL_TABLE_OB_ALL_VIRTUAL_BACKUPSET_HISTORY_MGR_H_

#include "share/ob_virtual_table_scanner_iterator.h"
#include "share/ob_scanner.h"
#include "common/row/ob_row.h"
#include "share/backup/ob_backup_struct.h"

namespace oceanbase {
namespace storage {
class ObIPartitionGroupIterator;
}
namespace observer {
class ObAllVirtualBackupSetHistoryMgr : public common::ObVirtualTableScannerIterator {
  enum COLUMN_ID_LIST {
    TENANT_ID = common::OB_APP_MIN_COLUMN_ID,
    INCARNATION,
    BACKUP_SET_ID,
    BACKUP_TYPE,
    DEVICE_TYPE,
    SNAPSHOT_VERSION,
    PREV_FULL_BACKUP_SET_ID,
    PREV_INC_BACKUP_SET_ID,
    PREV_BACKUP_DATA_VERSION,
    PG_COUNT,
    MACRO_BLOCK_COUNT,
    FINISH_PG_COUNT,
    FINISH_MACRO_BLOCK_COUNT,
    INPUT_BYTES,
    OUTPUT_BYTES,
    START_TIME,
    END_TIME,
    COMPATIBLE,
    CLUSTER_VERSION,
    STATUS,
    RESULT,
    CLUSTER_ID,
    BACKUP_DEST,
    BACKUP_DATA_VERSION,
    BACKUP_SCHMEA_VERSION,
    CLUSTER_VERSION_DISPLAY,
    PARTITION_COUNT,
    FINISH_PARTITION_COUNT,
    IS_MARK_DELETED,
    ENCRYPTION_MODE,
    PASSWD,
    BACKUP_RECOVERY_WINDOW,
  };

public:
  ObAllVirtualBackupSetHistoryMgr();
  virtual ~ObAllVirtualBackupSetHistoryMgr();
  int init(common::ObMySQLProxy& sql_proxy);
  virtual void reset() override;
  virtual int inner_get_next_row(common::ObNewRow*& row) override;

private:
  bool is_inited_;
  ObMySQLProxy::MySQLResult res_;
  sqlclient::ObMySQLResult* result_;
  int64_t backup_recovery_window_;
  share::ObTenantBackupTaskInfo tenant_backup_task_info_;
  char backup_dest_str_[share::OB_MAX_BACKUP_DEST_LENGTH];
  char cluster_version_display_[OB_INNER_TABLE_BACKUP_TASK_CLUSTER_FORMAT_LENGTH];

private:
  DISALLOW_COPY_AND_ASSIGN(ObAllVirtualBackupSetHistoryMgr);
};

}  // namespace observer
}  // namespace oceanbase
#endif /* SRC_OBSERVER_VIRTUAL_TABLE_OB_ALL_VIRTUAL_RESERVED_TABLE_MGR_H_ */
