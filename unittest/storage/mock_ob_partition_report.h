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

#ifndef MOCK_OB_PARTITION_REPORT_H_
#define MOCK_OB_PARTITION_REPORT_H_

#undef private
#undef protected
#include <gmock/gmock.h>
#define private public
#define protected public
#include "storage/ob_i_partition_report.h"

namespace oceanbase {
namespace storage {

class MockObIPartitionReport : public ObIPartitionReport {
public:
  MockObIPartitionReport()
  {}
  virtual ~MockObIPartitionReport()
  {}
  MOCK_METHOD3(submit_pt_update_task,
      int(const common::ObPartitionKey& part_key, const bool need_report_checksum, const bool with_role));
  MOCK_METHOD1(submit_pt_update_role_task, int(const common::ObPartitionKey& part_key));
  MOCK_METHOD1(submit_pg_pt_update_task, void(const common::ObPartitionArray& pg_partitions));
  MOCK_METHOD1(pt_sync_update, int(const common::ObPartitionKey& part_key));
  MOCK_METHOD1(report_merge_finished, int(const int64_t frozen_version));
  MOCK_METHOD1(submit_pt_remove_task, int(const common::ObPartitionKey& part_key));
  MOCK_METHOD4(
      report_local_index_build_complete, int(const common::ObPartitionKey& part_key, const uint64_t index_id,
                                             const share::schema::ObIndexStatus index_status, const int32_t ret_code));
  MOCK_METHOD2(report_merge_error, int(const common::ObPartitionKey&, const int));
  MOCK_METHOD3(report_rebuild_replica,
      int(const common::ObPartitionKey&, const common::ObAddr&, const storage::ObRebuildSwitch&));
  MOCK_METHOD3(report_rebuild_replica_async,
      int(const common::ObPartitionKey&, const common::ObAddr&, const storage::ObRebuildSwitch&));
  MOCK_METHOD5(submit_checksum_update_task,
      int(const common::ObPartitionKey& pkey, const uint64_t sstable_id, const int sstable_type,
          const observer::ObSSTableChecksumUpdateType update_type, const bool task_need_batch));
  MOCK_METHOD1(update_pg_backup_task_info, int(const common::ObIArray<share::ObPGBackupTaskInfo>& pg_task_array));
};

}  // namespace storage
}  // namespace oceanbase

#endif /* MOCK_OB_PARTITION_REPORT_H_ */
