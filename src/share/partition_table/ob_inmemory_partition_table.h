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

#ifndef OCEANBASE_PARTITION_TABLE_OB_INMEMORY_PARTITION_TABLE_H_
#define OCEANBASE_PARTITION_TABLE_OB_INMEMORY_PARTITION_TABLE_H_

#include "lib/lock/ob_mutex.h"
#include "share/partition_table/ob_ipartition_table.h"
#include "share/partition_table/ob_partition_info.h"
#include "common/ob_role.h"

namespace oceanbase {
namespace common {
class ObPartitionKey;
}
namespace share {

class ObIRsListChangeCb {
public:
  virtual int submit_update_rslist_task(const bool force_update = false) = 0;
  virtual int submit_report_replica() = 0;
  virtual int submit_report_replica(const common::ObPartitionKey& key) = 0;
};

class ObInMemoryPartitionTable : public ObIPartitionTable {
public:
  friend class TestInMemoryPartitionTable_common_Test;
  friend class TestInMemoryPartitionTable_to_leader_time_Test;
  friend class TestInMemoryPartitionTable_leader_update_Test;
  friend class TestInMemoryPartitionTable_log_unit_id_Test;
  explicit ObInMemoryPartitionTable(ObIPartPropertyGetter& prop_getter);
  virtual ~ObInMemoryPartitionTable();

  int init(ObIRsListChangeCb& rs_list_change_cb);
  inline bool is_inited() const
  {
    return inited_;
  }
  virtual int get(const uint64_t table_id, const int64_t partition_id, ObPartitionInfo& partition_info,
      const bool need_fetch_faillist = false, const int64_t cluster_id = common::OB_INVALID_ID) override;

  virtual int prefetch_by_table_id(const uint64_t tenant_id, const uint64_t start_table_id,
      const int64_t start_partition_id, common::ObIArray<ObPartitionInfo>& partition_infos,
      const bool need_fetch_faillist = false) override;

  virtual int prefetch(const uint64_t tenant_id, const uint64_t start_table_id, const int64_t start_partition_id,
      common::ObIArray<ObPartitionInfo>& partition_infos, bool ignore_row_checksum,
      const bool need_fetch_faillist = false) override;

  virtual int prefetch(const uint64_t pt_table_id, const int64_t pt_partition_id, const uint64_t start_table_id,
      const int64_t start_partition_id, common::ObIArray<ObPartitionInfo>& partition_infos,
      const bool need_fetch_faillist = false) override;

  virtual int batch_fetch_partition_infos(const common::ObIArray<common::ObPartitionKey>& keys,
      common::ObIAllocator& allocator, common::ObArray<ObPartitionInfo*>& partitions,
      const int64_t cluster_id = common::OB_INVALID_ID) override;

  virtual int batch_execute(const common::ObIArray<ObPartitionReplica>& replicas) override;
  virtual int batch_report_with_optimization(
      const common::ObIArray<ObPartitionReplica>& replicas, const bool with_role) override;

  virtual int batch_report_partition_role(
      const common::ObIArray<share::ObPartitionReplica>& pkey_array, const common::ObRole new_role) override;
  virtual int set_unit_id(const uint64_t table_id, const int64_t partition_id, const common::ObAddr& server,
      const uint64_t unit_id) override;

  virtual int set_original_leader(
      const uint64_t table_id, const int64_t partition_id, const bool is_original_leader) override;

  virtual int update_rebuild_flag(
      const uint64_t table_id, const int64_t partition_id, const common::ObAddr& server, const bool rebuild) override;

  virtual int update_fail_list(const uint64_t table_id, const int64_t partition_id, const common::ObAddr& server,
      const ObPartitionReplica::FailList& fail_list) override;

  virtual int handover_partition(
      const common::ObPGKey& pg_key, const common::ObAddr& src_addr, const common::ObAddr& dest_addr) override;

  virtual int replace_partition(
      const ObPartitionReplica& replica, const common::ObAddr& src_addr, const common::ObAddr& dest_addr) override;

  void reuse();

private:
  // holds mutex_
  int inner_get(const uint64_t table_id, const int64_t partition_id, const bool filter_flag_replica,
      ObPartitionInfo& partition_info);
  // holds mutex_
  int inner_prefetch(const uint64_t tenant_id, const uint64_t start_table_id, const int64_t start_partition_id,
      const bool filter_flag_replica, common::ObIArray<ObPartitionInfo>& partition_infos,
      const bool need_fetch_faillist = false);
  virtual int update(const ObPartitionReplica& replica) override;
  virtual int remove(const uint64_t table_id, const int64_t partition_id, const common::ObAddr& server) override;

  // holds mutex_
  int update_leader_replica(const ObPartitionReplica& replica);
  // holds mutex_
  int update_follower_replica(const ObPartitionReplica& replica);

  int check_leader();

private:
  bool inited_;
  ObPartitionInfo partition_info_;
  lib::ObMutex mutex_;
  ObIRsListChangeCb* rs_list_change_cb_;

  DISALLOW_COPY_AND_ASSIGN(ObInMemoryPartitionTable);
};

}  // end namespace share
}  // end namespace oceanbase

#endif  // OCEANBASE_PARTITION_TABLE_OB_INMEMORY_PARTITION_TABLE_H_
