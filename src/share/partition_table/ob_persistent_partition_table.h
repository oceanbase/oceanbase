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

#ifndef OCEANBASE_PARTITION_TABLE_OB_PERSISTENT_PARTITION_TABLE_H_
#define OCEANBASE_PARTITION_TABLE_OB_PERSISTENT_PARTITION_TABLE_H_

#include "share/partition_table/ob_ipartition_table.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "common/ob_role.h"

namespace oceanbase {
namespace common {
class ObMySQLProxy;
class ObServerConfig;
}  // namespace common
namespace share {

class ObPartitionTableProxy;

// for partition table store in __all_core_table __all_root_table and __all_tenant_meta_table
class ObPersistentPartitionTable : public ObIPartitionTable {
public:
  explicit ObPersistentPartitionTable(ObIPartPropertyGetter& prop_getter);
  virtual ~ObPersistentPartitionTable();

  int init(common::ObISQLClient& sql_proxy, common::ObServerConfig* config);
  bool is_inited() const
  {
    return inited_;
  }

  virtual int get(const uint64_t table_id, const int64_t partition_id, ObPartitionInfo& partition_info,
      const bool need_fetch_faillist = false, const int64_t cluster_id = common::OB_INVALID_ID) override;
  virtual int batch_fetch_partition_infos(const common::ObIArray<common::ObPartitionKey>& keys,
      common::ObIAllocator& allocator, common::ObArray<ObPartitionInfo*>& partitions,
      const int64_t cluster_id = common::OB_INVALID_ID) override;

  virtual int prefetch_by_table_id(const uint64_t tenant_id, const uint64_t start_table_id,
      const int64_t start_partition_id, common::ObIArray<ObPartitionInfo>& partition_infos,
      const bool need_fetch_faillist = false) override;

  virtual int prefetch(const uint64_t tenant_id, const uint64_t start_table_id, const int64_t start_partition_id,
      common::ObIArray<ObPartitionInfo>& partition_infos, bool ignore_row_checksum,
      const bool need_fetch_faillist = false) override;

  virtual int prefetch(const uint64_t pt_table_id, const int64_t pt_partition_id, const uint64_t start_table_id,
      const int64_t start_partition_id, common::ObIArray<ObPartitionInfo>& partition_infos,
      const bool need_fetch_faillist = false) override;

  virtual int update(const ObPartitionReplica& replica) override;
  virtual int batch_report_partition_role(
      const common::ObIArray<share::ObPartitionReplica>& pkey_array, const common::ObRole new_role) override;

  virtual int batch_execute(const common::ObIArray<ObPartitionReplica>& replicas) override;
  virtual int batch_report_with_optimization(
      const common::ObIArray<ObPartitionReplica>& replicas, const bool with_role) override;
  virtual int remove(const uint64_t table_id, const int64_t partition_id, const common::ObAddr& server) override;

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

  common::ObISQLClient* get_sql_proxy() const
  {
    return sql_proxy_;
  }

private:
  int get_partition_info(const uint64_t table_id, const int64_t partition_id, const bool filter_flag_replica,
      ObPartitionInfo& partition_info, const bool need_fetch_faillist = false,
      const int64_t cluster_id = common::OB_INVALID_ID);
  int update_leader_replica(
      ObPartitionTableProxy& pt_proxy, ObPartitionInfo& partition, const ObPartitionReplica& replica);
  int update_follower_replica(
      ObPartitionTableProxy& pt_proxy, ObPartitionInfo& partition, const ObPartitionReplica& replica);
  int update_replica(ObPartitionTableProxy& pt_proxy, ObPartitionInfo& partition, const ObPartitionReplica& replica);
  int execute(ObPartitionTableProxy* proxy, const ObPartitionReplica& replica);

private:
  bool inited_;
  common::ObISQLClient* sql_proxy_;
  common::ObServerConfig* config_;

  DISALLOW_COPY_AND_ASSIGN(ObPersistentPartitionTable);
};

}  // end namespace share
}  // end namespace oceanbase

#endif  // OCEANBASE_PARTITION_TABLE_OB_PERSISTENT_PARTITION_TABLE_H_
