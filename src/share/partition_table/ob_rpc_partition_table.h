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

#ifndef OCEANBASE_PARTITION_TABLE_OB_RPC_PARTITION_TABLE_H_
#define OCEANBASE_PARTITION_TABLE_OB_RPC_PARTITION_TABLE_H_

#include "share/partition_table/ob_ipartition_table.h"
#include "share/partition_table/ob_partition_info.h"
#include "common/ob_role.h"

namespace oceanbase {
namespace common {
class ObServerConfig;
}
namespace obrpc {
class ObCommonRpcProxy;
}

namespace share {
class ObRsMgr;

class ObRpcPartitionTable : public ObIPartitionTable {
public:
  explicit ObRpcPartitionTable(ObIPartPropertyGetter& prop_getter);
  virtual ~ObRpcPartitionTable();

  int init(obrpc::ObCommonRpcProxy& rpc_proxy, share::ObRsMgr& rs_mgr, common::ObServerConfig& config);

  inline bool is_inited() const
  {
    return is_inited_;
  }

  virtual int get(const uint64_t table_id, const int64_t partition_id, ObPartitionInfo& partition_info,
      const bool need_fetch_faillist = false, const int64_t cluster_id = common::OB_INVALID_ID) override;

  virtual int prefetch_by_table_id(const uint64_t tenant_id, const uint64_t start_table_id,
      const int64_t start_partition_id, common::ObIArray<ObPartitionInfo>& partition_infos,
      const bool need_fetch_faillist = false) override;

  virtual int prefetch(const uint64_t tenant_id, const uint64_t start_table_id, const int64_t start_partition_id,
      common::ObIArray<ObPartitionInfo>& partition_info, bool ignore_row_checksum,
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
  virtual int update(const ObPartitionReplica& replica) override;

  virtual int remove(const uint64_t table_id, const int64_t partition_id, const common::ObAddr& server) override;

  virtual int set_unit_id(const uint64_t table_id, const int64_t partition_id, const common::ObAddr& server,
      const uint64_t unit_id) override;

  virtual int update_rebuild_flag(
      const uint64_t table_id, const int64_t partition_id, const common::ObAddr& server, const bool rebuild) override;
  virtual int update_fail_list(const uint64_t table_id, const int64_t partition_id, const common::ObAddr& server,
      const ObPartitionReplica::FailList& fail_list) override;

  virtual int handover_partition(
      const common::ObPGKey& pg_key, const common::ObAddr& src_addr, const common::ObAddr& dest_addr) override;

  virtual int replace_partition(
      const ObPartitionReplica& replica, const common::ObAddr& src_addr, const common::ObAddr& dest_addr) override;

  virtual int set_original_leader(
      const uint64_t table_id, const int64_t partition_id, const bool is_original_leader) override;

private:
  int get_timeout(int64_t& timeout);
  int fetch_root_partition_v1(ObPartitionInfo& partition_info);
  int fetch_root_partition_v2(ObPartitionInfo& partition_info);
  int fetch_root_partition_from_rs_list_v1(ObPartitionInfo& partition_info);
  int fetch_root_partition_from_all_server_v1(ObPartitionInfo& partition);

  int fetch_root_partition_from_rs_list_v2(common::ObIArray<ObAddr>& obs_list, ObPartitionInfo& partition_info);
  int fetch_root_partition_from_all_server_v2(const common::ObIArray<ObAddr>& obs_list, ObPartitionInfo& partition);
  int fetch_root_partition_from_ps_v2(common::ObIArray<ObAddr>& obs_list, ObPartitionInfo& partition);
  int fetch_root_partition_from_obs_v1(const common::ObIArray<ObAddr>& obs_list, ObPartitionInfo& partition_info);

  const int64_t CACHE_VALID_INTERVAL = 300 * 1000 * 1000;  // 5min

private:
  bool is_inited_;
  obrpc::ObCommonRpcProxy* rpc_proxy_;
  share::ObRsMgr* rs_mgr_;
  common::ObServerConfig* config_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObRpcPartitionTable);
};

}  // end namespace share
}  // end namespace oceanbase

#endif
