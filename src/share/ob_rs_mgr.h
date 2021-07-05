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

#ifndef OCEANBASE_SHARE_HEARTBEAT_OB_RS_MGR_H_
#define OCEANBASE_SHARE_HEARTBEAT_OB_RS_MGR_H_

#include "lib/container/ob_se_array.h"
#include "lib/lock/ob_spin_lock.h"
#include "lib/net/ob_addr.h"
#include "lib/utility/ob_print_utils.h"
#include "share/ob_lease_struct.h"
#include "share/ob_inner_config_root_addr.h"
#include "share/ob_web_service_root_addr.h"
#include "share/ob_cluster_type.h"  // ObClusterType

namespace oceanbase {
namespace obrpc {
class ObCommonRpcProxy;
class ObGetRootserverRoleResult;
}  // namespace obrpc
namespace common {
class ObCommonConfig;
class ObMySQLProxy;
}  // namespace common

namespace share {
class IHeartBeatProcess;

class ObUnifiedAddrAgent : public ObRootAddrAgent {
public:
  ObUnifiedAddrAgent();
  int init(common::ObMySQLProxy& sql_proxy, common::ObServerConfig& config);
  virtual int store(const ObIAddrList& addr_list, const ObIAddrList& readonly_addr_list, const bool force,
      const common::ObClusterType cluster_type, const int64_t timestamp) override;
  virtual int fetch(
      ObIAddrList& addr_list, ObIAddrList& readonly_addr_list, common::ObClusterType& cluster_typ) override;
  virtual int fetch_remote_rslist(const int64_t cluster_id, ObIAddrList& addr_list, ObIAddrList& readonly_addr_list,
      common::ObClusterType& cluster_type) override;
  int fetch_rslist_by_agent_idx(const int64_t index, const int64_t cluster_id, ObIAddrList& addr_list,
      ObIAddrList& readonly_addr_list, common::ObClusterType& cluster_type);
  int64_t get_agent_num() const
  {
    return AGENT_NUM;
  }
  virtual int delete_cluster(const int64_t cluster_id) override;
  int reload();
  bool is_valid() override;

private:
  const static int64_t AGENT_NUM = 3;
  bool is_inited_;
  // ObWebServiceRootAddr and ObInnerConfigRootAddr
  // double write, at least one success is considered to be a success.
  // read alone, at least one success, imediately return to success.
  share::ObRootAddrAgent* agents_[AGENT_NUM];
  share::ObWebServiceRootAddr web_service_root_addr_;
  share::ObInnerConfigRootAddr inner_config_root_addr_;

  DISALLOW_COPY_AND_ASSIGN(ObUnifiedAddrAgent);
};

/* @feature
 *    1   single instance one server
 *    2   maintenance in lease state manager
 * @do_list
 *    1   maintain rs pool
 *    2   detect master rs if needed
 */
class ObRsMgr {
public:
  friend class ObLeaseStateMgr;
  ObRsMgr();
  virtual ~ObRsMgr();

  int init(obrpc::ObCommonRpcProxy* rpc_proxy, common::ObServerConfig* config, common::ObMySQLProxy* sql_proxy);
  bool is_inited() const
  {
    return inited_;
  }

  virtual int get_master_root_server(common::ObAddr& addr) const;
  int get_all_rs_list(common::ObIArray<common::ObAddr>& list);
  int renew_master_rootserver();
  int get_primary_cluster_master_rs(common::ObAddr& addr);
  int get_remote_cluster_master_rs(const int64_t cluster_id, common::ObAddr& addr);
  int force_set_master_rs(const common::ObAddr master_rs);
  int do_detect_master_rs_v3(const common::ObIArray<common::ObAddr>& server_list, ObPartitionInfo& partition_info);
  int fetch_rs_list(ObIAddrList& addr_list, ObIAddrList& readonly_addr_list);

private:
  int renew_master_rootserver_v2();
  int do_detect_master_rs_v2(common::ObIArray<common::ObAddr>& rs_list);
  int renew_master_rootserver_v3();
  int do_detect_master_rs_v3(
      const common::ObAddr& dst_server, const int64_t cluster_id, obrpc::ObGetRootserverRoleResult& result);

private:
  static const int64_t DETECT_MASTER_TIMEOUT = 1 * 1000 * 1000;  // 1s
  typedef common::ObSEArray<common::ObAddr, common::MAX_ZONE_NUM> RsList;

  bool inited_;
  obrpc::ObCommonRpcProxy* rpc_proxy_;
  common::ObCommonConfig* config_;
  ObUnifiedAddrAgent addr_agent_;
  ObRootAddrAgent& root_addr_agent_;

  mutable common::ObSpinLock lock_;  // protect master_rs_
  common::ObAddr master_rs_;

  DISALLOW_COPY_AND_ASSIGN(ObRsMgr);
};

}  // namespace share
}  // namespace oceanbase

#endif  //__OB_COMMON_OB_RS_INFO_H__
