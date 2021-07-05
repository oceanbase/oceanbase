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

#ifndef OB_I_PARTITION_REPORT_H_
#define OB_I_PARTITION_REPORT_H_
#include "share/schema/ob_table_schema.h"
#include "observer/ob_sstable_checksum_updater.h"
#include "share/backup/ob_backup_struct.h"

namespace oceanbase {
namespace common {
class ObPartitionKey;
class ObAddr;
}  // namespace common
namespace storage {
enum ObRebuildSwitch {
  OB_REBUILD_INVALID,
  OB_REBUILD_ON,
  OB_REBUILD_OFF,
  OB_REBUILD_MAX,
};

class ObIPartitionReport {
public:
  ObIPartitionReport()
  {}
  virtual ~ObIPartitionReport()
  {}
  virtual int submit_pt_update_task(
      const common::ObPartitionKey& part_key, const bool need_report_checksum = true, const bool with_role = false) = 0;
  virtual int submit_pt_update_role_task(const common::ObPartitionKey& pkey) = 0;
  virtual void submit_pg_pt_update_task(const common::ObPartitionArray& pg_partitions) = 0;
  virtual int submit_checksum_update_task(const common::ObPartitionKey& pkey, const uint64_t sstable_id,
      const int sstable_type,
      const observer::ObSSTableChecksumUpdateType update_type = observer::ObSSTableChecksumUpdateType::UPDATE_ALL,
      const bool need_batch = true) = 0;
  virtual int submit_pt_remove_task(const common::ObPartitionKey& part_key) = 0;
  virtual int pt_sync_update(const common::ObPartitionKey& part_key) = 0;
  virtual int report_merge_error(const common::ObPartitionKey& part_key, const int error_code) = 0;
  virtual int report_merge_finished(const int64_t frozen_version) = 0;
  virtual int report_local_index_build_complete(const common::ObPartitionKey& part_key, const uint64_t index_id,
      const share::schema::ObIndexStatus index_status, const int32_t ret_code) = 0;
  virtual int report_rebuild_replica(
      const common::ObPartitionKey& part_key, const common::ObAddr& server, const ObRebuildSwitch& rebuild_switch) = 0;
  virtual int report_rebuild_replica_async(
      const common::ObPartitionKey& part_key, const common::ObAddr& server, const ObRebuildSwitch& rebuild_switch) = 0;
  virtual int update_pg_backup_task_info(const common::ObIArray<share::ObPGBackupTaskInfo>& pg_task_info_array) = 0;
};

}  // namespace storage
}  // namespace oceanbase

#endif /* OB_I_PARTITION_REPORT_H_ */
