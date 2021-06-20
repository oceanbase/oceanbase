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

#ifndef SRC_OBSERVER_VIRTUAL_TABLE_OB_ALL_VIRTUAL_MIGRATION_STATUS_H_
#define SRC_OBSERVER_VIRTUAL_TABLE_OB_ALL_VIRTUAL_MIGRATION_STATUS_H_

#include "share/ob_virtual_table_scanner_iterator.h"
#include "share/ob_scanner.h"
#include "common/row/ob_row.h"
#include "lib/container/ob_se_array.h"
#include "storage/ob_partition_migrator.h"
#include "storage/ob_partition_migration_status.h"

namespace oceanbase {
namespace observer {
class ObAllVirtualPartitionMigrationStatus : public common::ObVirtualTableScannerIterator {
public:
  ObAllVirtualPartitionMigrationStatus();
  virtual ~ObAllVirtualPartitionMigrationStatus();

  int init(storage::ObPartitionMigrationStatusMgr& status_mgr);
  virtual int inner_get_next_row(common::ObNewRow*& row);
  virtual void reset();

private:
  enum TABLE_COLUMN {
    TASK_ID = common::OB_APP_MIN_COLUMN_ID,
    TENANT_ID,
    TABLE_ID,
    PARTITION_IDX,
    SVR_IP,
    SVR_PORT,
    MIGRATE_TYPE,
    PARENT_IP,
    PARENT_PORT,
    SRC_IP,
    SRC_PORT,
    DEST_IP,
    DEST_PORT,
    RESULT,
    START_TIME,
    FINISH_TIME,
    ACTION,
    REPLICA_STATE,
    REBUILD_COUNT,
    TOTAL_MACRO_BLOCK,
    DONE_MACRO_BLOCK,
    MAJOR_COUNT,
    MINI_MINOR_COUNT,
    NORMAL_MINOR_COUNT,
    BUF_MINOR_COUNT,
    REUSE_COUNT,
    COMMENT,
  };
  storage::ObPartitionMigrationStatusMgrIter iter_;
  char task_id_[OB_TRACE_STAT_BUFFER_SIZE];
  char svr_ip_[common::OB_IP_STR_BUFF];
  char parent_svr_ip_[common::OB_IP_STR_BUFF];
  char src_svr_ip_[common::OB_IP_STR_BUFF];
  char dest_svr_ip_[common::OB_IP_STR_BUFF];
  char replica_state_[OB_MIGRATE_REPLICA_STATE_LENGTH];
  char local_versions_[MAX_VALUE_LENGTH];
  char target_[MAX_VALUE_LENGTH];
  char comment_[common::OB_MAX_TASK_COMMENT_LENGTH];

  int convert_version_source_string(const common::ObIArray<ObVersion>& version_list,
      const common::ObIArray<ObAddr>& src_list, char* buf, int64_t buf_len) const;

  DISALLOW_COPY_AND_ASSIGN(ObAllVirtualPartitionMigrationStatus);
};

}  // namespace observer
}  // namespace oceanbase
#endif /* SRC_OBSERVER_VIRTUAL_TABLE_OB_ALL_VIRTUAL_MIGRATION_STATUS_H_ */
