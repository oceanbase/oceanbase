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

#ifndef OCEANBASE_SHARE_OB_MULTI_CLUSTER_UTIL_H
#define OCEANBASE_SHARE_OB_MULTI_CLUSTER_UTIL_H

#include "lib/ob_define.h"
#include "share/ob_web_service_root_addr.h"

namespace oceanbase {
namespace common {
class ObISQLClient;
}
namespace obrpc {
class ObCommonRpcProxy;
struct ObGetClusterInfoArg;
}  // namespace obrpc
namespace share {
class ObClusterInfo;

class ObMultiClusterUtil {
public:
  ObMultiClusterUtil()
  {}
  virtual ~ObMultiClusterUtil()
  {}

  // get primary cluster status
  // return code : 0 success; OB_ENTRY_NOT_EXIST : primary cluster not exist; other error code: fail
  //[output] cluster_info: primary cluster status
  static int get_primary_cluster_status(const obrpc::ObCommonRpcProxy* rpc_proxy, common::ObISQLClient& sql_proxy,
      ObClusterInfo& cluster_info, ObClusterAddr& cluster_addr);

  // get standby cluster status
  // return code : 0 success; OB_ENTRY_NOT_EXIST : standby cluster not exist; other error code: fail
  //[output] cluster_list: standby cluster status
  static int get_standby_cluster_status(const obrpc::ObCommonRpcProxy* rpc_proxy, const obrpc::ObGetClusterInfoArg& arg,
      common::ObISQLClient& sql_proxy, common::ObIArray<ObClusterInfo>& cluster_list);
  static int get_standby_cluster_list_for_protect_mode(
      common::ObISQLClient& sql_proxy, ObClusterAddrList& all_cluster_addr);
  static int get_standby_cluster_rs_addrs(
      common::ObISQLClient& sql_proxy, common::ObIArray<ObClusterRsAddr>& standby_cluster_rs_addrs);
  static int get_all_cluster_status(const obrpc::ObCommonRpcProxy* rpc_proxy, const bool ignore_fail,
      const obrpc::ObGetClusterInfoArg& arg, common::ObISQLClient& sql_proxy,
      common::ObIArray<ObClusterInfo>& cluster_list, ObClusterAddrList& cluster_addr_list,
      common::ObIArray<int64_t>* can_not_access_cluster = NULL, const bool ignore_disabled_cluster = false);
  static bool need_create_partition(
      const uint64_t tenant_id, const uint64_t table_id, const bool has_partition, const bool is_standby_cluster);

  static int get_parent_cluster(const obrpc::ObCommonRpcProxy& rpc_proxy, common::ObISQLClient& sql_proxy,
      ObClusterInfo& parent_cluster, ObClusterAddr& parent_addr);
  static int get_parent_cluster_from_addrlist(const obrpc::ObCommonRpcProxy& rpc_proxy,
      const ObClusterIAddrList& cluster_addr_list, ObClusterInfo& parent_cluster_info, ObClusterAddr& cluster_addr);
  // cluster private table, each cluster has strong leader, support writing and strongly consistent reading
  static bool is_cluster_private_table(const uint64_t table_id);
  static bool is_valid_paxos_replica(const uint64_t tenant_id, const uint64_t table_id, const bool is_standby_cluster,
      const common::ObReplicaType replica_type);
  static int get_leader(const ObIAddrList& rs_list, common::ObAddr& leader);
  static bool is_cluster_allow_submit_log(const uint64_t table_id);
  static bool is_cluster_allow_elect(const uint64_t table_id);
  static bool is_cluster_allow_strong_consistency_read_write(const uint64_t table_id);
  static bool can_be_parent_cluster(const ObClusterInfo& cluster_info);
  static bool can_be_clog_parent_cluster(const ObClusterInfo& cluster_info);
  static bool need_sync_to_standby(const ObClusterInfo& cluster_info);
  static int get_gts(const uint64_t tenant_id, int64_t& gts_value, const int64_t timeout);
};

struct ObTenantFlashbackSCN {

  OB_UNIS_VERSION(1);

public:
  int64_t tenant_id_;
  int64_t inner_scn_;
  int64_t user_scn_;
  int64_t schema_version_;
  ObTenantFlashbackSCN()
      : tenant_id_(common::OB_INVALID_TENANT_ID),
        inner_scn_(0),
        user_scn_(0),
        schema_version_(common::OB_INVALID_VERSION)
  {}
  ObTenantFlashbackSCN(
      const int64_t tenant_id, const int64_t inner_scn, const int64_t user_scn, const int64_t schema_version)
      : tenant_id_(tenant_id), inner_scn_(inner_scn), user_scn_(user_scn), schema_version_(schema_version)
  {}
  bool operator==(const ObTenantFlashbackSCN& other) const
  {
    return ((this == &other) || (tenant_id_ == other.tenant_id_ && inner_scn_ == other.inner_scn_ &&
                                    user_scn_ == other.user_scn_ && schema_version_ == other.schema_version_));
  }
  int64_t get_min_scn() const
  {
    return std::min(inner_scn_, user_scn_);
  }
  TO_STRING_KV(K_(tenant_id), K_(inner_scn), K_(user_scn), K_(schema_version));
};
}  // namespace share
}  // namespace oceanbase
#endif
