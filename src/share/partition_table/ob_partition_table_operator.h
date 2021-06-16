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

#ifndef OCEANBASE_PARTITION_TABLE_OB_PARTITION_TABLE_OPERATOR_H_
#define OCEANBASE_PARTITION_TABLE_OB_PARTITION_TABLE_OPERATOR_H_

#include "ob_ipartition_table.h"
#include "ob_inmemory_partition_table.h"
#include "ob_persistent_partition_table.h"
#include "ob_rpc_partition_table.h"
#include "share/ob_cluster_version.h"
#include "share/config/ob_server_config.h"

namespace oceanbase {
// forward declaration
namespace obrpc {
class ObCommonRpcProxy;
}  // end of namespace obrpc
namespace common {
class ObServerConfig;
}  // end namespace common
namespace share {
class ObIMergeErrorCb;

class ObPartitionTableOperator : public ObIPartitionTable {
public:
  explicit ObPartitionTableOperator(ObIPartPropertyGetter& prop_getter);
  virtual ~ObPartitionTableOperator();

  int init(common::ObISQLClient& sql_proxy, common::ObServerConfig* config = NULL);
  bool is_inited() const
  {
    return inited_;
  }
  int set_callback_for_rs(ObIRsListChangeCb& rs_list_change_cb, ObIMergeErrorCb& merge_error_cb);
  int set_callback_for_obs(obrpc::ObCommonRpcProxy& rpc_proxy, ObRsMgr& rs_mgr, common::ObServerConfig& config);

  virtual int get(const uint64_t table_id, const int64_t partition_id, ObPartitionInfo& partition_info,
      const bool need_fetch_faillist = false, const int64_t cluster_id = common::OB_INVALID_ID) override;

  virtual int prefetch_by_table_id(const uint64_t tenant_id, const uint64_t start_table_id,
      const int64_t set_partition_id, common::ObIArray<ObPartitionInfo>& partition_infos,
      const bool need_fetch_faillist = false) override;

  virtual int prefetch(const uint64_t tenant_id, const uint64_t start_table_id, const int64_t set_partition_id,
      common::ObIArray<ObPartitionInfo>& partition_infos, bool ignore_row_checksum,
      const bool need_fetch_faillist = false);

  virtual int prefetch(const uint64_t pt_table_id, const int64_t pt_partition_id, const uint64_t start_table_id,
      const int64_t start_partition_id, common::ObIArray<ObPartitionInfo>& partition_infos,
      const bool need_fetch_faillist = false) override;

  virtual int batch_fetch_partition_infos(const common::ObIArray<common::ObPartitionKey>& keys,
      common::ObIAllocator& allocator, common::ObArray<ObPartitionInfo*>& partitions,
      const int64_t cluster_id = common::OB_INVALID_ID) override;
  virtual int update(const ObPartitionReplica& replica) override;

  virtual int batch_execute(const common::ObIArray<ObPartitionReplica>& replicas) override;
  virtual int batch_report_with_optimization(
      const common::ObIArray<ObPartitionReplica>& replicas, const bool with_role) override;
  virtual int batch_report_partition_role(
      const common::ObIArray<share::ObPartitionReplica>& pkey_array, const ObRole new_role) override;
  virtual int remove(const uint64_t table_id, const int64_t partition_id, const common::ObAddr& server) override;

  virtual int set_unit_id(const uint64_t table_id, const int64_t partition_id, const common::ObAddr& server,
      const uint64_t unit_id) override;

  virtual int set_original_leader(
      const uint64_t table_id, int64_t partition_id, const bool is_original_leader) override;

  virtual int update_rebuild_flag(
      const uint64_t table_id, const int64_t partition_id, const common::ObAddr& server, const bool rebuild) override;

  virtual int update_fail_list(const uint64_t table_id, const int64_t partition_id, const common::ObAddr& server,
      const ObPartitionReplica::FailList& fail_list) override;

  virtual int handover_partition(
      const common::ObPGKey& pg_key, const common::ObAddr& src_addr, const common::ObAddr& dest_addr) override;

  virtual int replace_partition(
      const ObPartitionReplica& replica, const common::ObAddr& src_addr, const common::ObAddr& dest_addr) override;

  ObInMemoryPartitionTable& get_inmemory_table()
  {
    return inmemory_table_;
  }

  ObPersistentPartitionTable& get_persistent_table()
  {
    return persistent_table_;
  }
  static uint64_t get_partition_table_id(const uint64_t tid);

private:
  int get_partition_table(const uint64_t table_id, ObIPartitionTable*& partition_table);
  // with_rootserver means that the observer with rootserver module
  int set_use_memory_table(ObIRsListChangeCb& rs_list_change_cb);

  int set_use_rpc_table(obrpc::ObCommonRpcProxy& rpc_proxy, ObRsMgr& rs_mgr, common::ObServerConfig& config);

private:
  bool inited_;
  ObIPartitionTable* root_meta_table_;
  ObInMemoryPartitionTable inmemory_table_;
  ObRpcPartitionTable rpc_table_;
  ObPersistentPartitionTable persistent_table_;  // use __all_tenant_meta_table, __all_root_table

  DISALLOW_COPY_AND_ASSIGN(ObPartitionTableOperator);
};

}  // end namespace share
}  // end namespace oceanbase
#endif  // OCEANBASE_PARTITION_TABLE_OB_PARTITION_TABLE_OPERATOR_H_
