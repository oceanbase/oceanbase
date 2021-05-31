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

#define USING_LOG_PREFIX SHARE

#include "ob_multi_cluster_util.h"
#include "ob_srv_rpc_proxy.h"
#include "ob_common_rpc_proxy.h"
#include "share/ob_web_service_root_addr.h"
#include "ob_rpc_struct.h"
#include "observer/ob_server_struct.h"
#include "lib/ob_define.h"
#include "observer/ob_server_struct.h"
#include "storage/transaction/ob_ts_mgr.h"  // ObTsMgr
namespace oceanbase {
using namespace common;
using namespace share::schema;
namespace share {

int ObMultiClusterUtil::get_parent_cluster(const obrpc::ObCommonRpcProxy& rpc_proxy, common::ObISQLClient& sql_proxy,
    ObClusterInfo& parent_cluster, ObClusterAddr& parent_addr)
{
  int ret = OB_SUCCESS;
  UNUSEDx(rpc_proxy, sql_proxy, parent_cluster, parent_addr);
  return ret;
}

int ObMultiClusterUtil::get_primary_cluster_status(const obrpc::ObCommonRpcProxy* rpc_proxy,
    common::ObISQLClient& sql_proxy, ObClusterInfo& cluster_info, ObClusterAddr& cluster_addr)
{
  int ret = OB_SUCCESS;
  UNUSEDx(rpc_proxy, sql_proxy, cluster_info, cluster_addr);
  return ret;
}

int ObMultiClusterUtil::get_parent_cluster_from_addrlist(const obrpc::ObCommonRpcProxy& rpc_proxy,
    const ObClusterIAddrList& cluster_addr_list, ObClusterInfo& parent_cluster_info, ObClusterAddr& cluster_addr)
{
  int ret = OB_SUCCESS;
  UNUSEDx(rpc_proxy, cluster_addr_list, parent_cluster_info, cluster_addr);
  return ret;
}

int ObMultiClusterUtil::get_standby_cluster_status(const obrpc::ObCommonRpcProxy* rpc_proxy,
    const obrpc::ObGetClusterInfoArg& arg, common::ObISQLClient& sql_proxy,
    common::ObIArray<ObClusterInfo>& cluster_list)
{
  int ret = OB_SUCCESS;
  UNUSEDx(rpc_proxy, arg, sql_proxy, cluster_list);
  return ret;
}

int ObMultiClusterUtil::get_leader(const ObIAddrList& rs_list, ObAddr& leader)
{
  int ret = OB_ENTRY_NOT_EXIST;
  for (int64_t i = 0; i < rs_list.count() && OB_ENTRY_NOT_EXIST == ret; i++) {
    const ObRootAddr& addr = rs_list.at(i);
    if (is_strong_leader(addr.role_)) {
      leader = addr.server_;
      ret = OB_SUCCESS;
    }
  }
  if (OB_ENTRY_NOT_EXIST == ret) {
    // without leader, return first obs.
    if (0 < rs_list.count()) {
      leader = rs_list.at(0).server_;
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

int ObMultiClusterUtil::get_standby_cluster_list_for_protect_mode(
    common::ObISQLClient& sql_proxy, ObClusterAddrList& all_cluster_addr)
{
  int ret = OB_SUCCESS;
  UNUSEDx(sql_proxy, all_cluster_addr);
  return ret;
}

int ObMultiClusterUtil::get_standby_cluster_rs_addrs(
    common::ObISQLClient& sql_proxy, common::ObIArray<ObClusterRsAddr>& standby_cluster_rs_addrs)
{
  int ret = OB_SUCCESS;
  UNUSEDx(sql_proxy, standby_cluster_rs_addrs);
  return ret;
}

int ObMultiClusterUtil::get_all_cluster_status(const obrpc::ObCommonRpcProxy* rpc_proxy, const bool ignore_fail,
    const obrpc::ObGetClusterInfoArg& arg, common::ObISQLClient& sql_proxy,
    common::ObIArray<ObClusterInfo>& all_cluster_list, ObClusterAddrList& all_cluster_addr,
    common::ObIArray<int64_t>* can_not_access_cluster, const bool ignore_disabled_cluster)
{
  int ret = OB_SUCCESS;
  UNUSEDx(rpc_proxy,
      ignore_fail,
      arg,
      sql_proxy,
      all_cluster_list,
      all_cluster_addr,
      can_not_access_cluster,
      ignore_disabled_cluster);
  return ret;
}

bool ObMultiClusterUtil::need_create_partition(
    const uint64_t tenant_id, const uint64_t table_id, const bool has_partition, const bool is_standby_cluster)
{
  bool bret = false;
  int64_t new_table_id = combine_id(tenant_id, extract_pure_id(table_id));
  if (!has_partition) {
    bret = false;
  } else if (OB_SYS_TENANT_ID == tenant_id || !is_standby_cluster) {
    bret = true;
  } else if (is_cluster_private_table(new_table_id)) {
    bret = true;
  } else {
    bret = false;
  }
  LOG_DEBUG("has_partition", K(has_partition), K(new_table_id), K(bret), K(tenant_id));
  return bret;
}

bool ObMultiClusterUtil::is_cluster_private_table(const uint64_t table_id)
{
  bool bret = false;
  uint64_t tenant_id = extract_tenant_id(table_id);
  if (OB_SYS_TENANT_ID == tenant_id) {
    // all tables in sys tenant are private
    bret = true;
  } else {
    bret = ObSysTableChecker::is_cluster_private_tenant_table(table_id);
  }
  return bret;
}

bool ObMultiClusterUtil::is_cluster_allow_submit_log(const uint64_t table_id)
{
  bool bool_ret = (GCTX.is_primary_cluster() || is_cluster_private_table(table_id) || GCTX.is_in_flashback_state() ||
                   GCTX.is_in_cleanup_state());
  return bool_ret;
}

bool ObMultiClusterUtil::is_cluster_allow_elect(const uint64_t table_id)
{
  bool bool_ret =
      (GCTX.is_primary_cluster() || is_cluster_private_table(table_id) ||
          (!is_cluster_private_table(table_id) &&
              !GCTX.is_in_invalid_state()));  // non privete table can not elect leader while cluster in invalid status
  return bool_ret;
}

bool ObMultiClusterUtil::is_cluster_allow_strong_consistency_read_write(const uint64_t table_id)
{
  // cluster can not enable write and strong read
  bool bool_ret = (GCTX.is_primary_cluster() || is_cluster_private_table(table_id) ||
                   (GCTX.is_in_flashback_state() && is_inner_table(table_id)) ||
                   (GCTX.is_in_cleanup_state() && is_inner_table(table_id)));
  return bool_ret;
}

bool ObMultiClusterUtil::is_valid_paxos_replica(
    const uint64_t tenant_id, const uint64_t table_id, const bool is_standby_cluster, const ObReplicaType replica_type)
{
  bool bret = true;
  bool has_partition = true;
  if (!ObReplicaTypeCheck::is_paxos_replica_V2(replica_type)) {
    bret = false;
  } else if (!need_create_partition(tenant_id, table_id, has_partition, is_standby_cluster)) {
    bret = false;
  }
  return bret;
}

bool ObMultiClusterUtil::can_be_parent_cluster(const ObClusterInfo& cluster_info)
{
  bool bret = true;
  UNUSED(cluster_info);
  return bret;
}

bool ObMultiClusterUtil::can_be_clog_parent_cluster(const ObClusterInfo& cluster_info)
{
  bool bret = true;
  UNUSED(cluster_info);
  return bret;
}

bool ObMultiClusterUtil::need_sync_to_standby(const ObClusterInfo& cluster_info)
{
  bool bret = false;
  UNUSED(cluster_info);
  return bret;
}

int ObMultiClusterUtil::get_gts(const uint64_t tenant_id, int64_t& gts, const int64_t timeout)
{
  int ret = OB_EAGAIN;
  const int64_t WAIT_US = 100;
  transaction::MonotonicTs stc = transaction::MonotonicTs::current_time();
  int64_t cur_ts = ObTimeUtility::current_time();
  int64_t end_ts = cur_ts + timeout;
  transaction::ObTsMgr& ts_mgr = transaction::ObTsMgr::get_instance();

  gts = OB_INVALID_TIMESTAMP;
  while (OB_EAGAIN == ret) {
    transaction::MonotonicTs unused_ts;
    if (OB_FAIL(ts_mgr.get_gts(tenant_id, stc, NULL, gts, unused_ts))) {
      if (OB_EAGAIN == ret) {
        // retry
      } else {
        LOG_WARN("ts_mgr get_gts fail", KR(ret), K(tenant_id), K(stc));
      }
    }

    int64_t left_time = end_ts - ObTimeUtility::current_time();
    if (left_time <= 0) {
      ret = OB_TIMEOUT;
      LOG_WARN("already timeout", K(ret), K(tenant_id));
    } else {
      usleep(WAIT_US);
    }
  }
  return ret;
}
OB_SERIALIZE_MEMBER(ObTenantFlashbackSCN, tenant_id_, inner_scn_, user_scn_, schema_version_);

}  // namespace share
}  // namespace oceanbase
