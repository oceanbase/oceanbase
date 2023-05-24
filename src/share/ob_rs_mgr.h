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
#include "lib/hash/ob_hashmap.h"
#include "lib/lock/ob_spin_lock.h"
#include "lib/net/ob_addr.h"
#include "lib/utility/ob_print_utils.h"
#include "share/ob_lease_struct.h"
#include "share/ob_inner_config_root_addr.h"
#include "share/ob_web_service_root_addr.h"
#include "share/ob_cluster_role.h"              // ObClusterRole

namespace oceanbase
{
namespace obrpc
{
class ObCommonRpcProxy;
class ObSrvRpcProxy;
struct ObDetectMasterRsLSResult;
}
namespace common
{
class ObCommonConfig;
class ObMySQLProxy;
}

namespace rootserver
{
class ObRootService;
}

namespace share
{
class IHeartBeatProcess;
class ObLSInfo;

class ObUnifiedAddrAgent : public ObRootAddrAgent
{
public:
  enum AgentType {
    INNER_CONFIG_AGENT = 0,
    WEB_SERVICE_AGENT = 1,
    MAX_AGENT_NUM
  };
public:
  ObUnifiedAddrAgent();
  int init(common::ObMySQLProxy &sql_proxy, common::ObServerConfig &config);
  virtual int store(const ObIAddrList &addr_list, const ObIAddrList &readonly_addr_list,
                    const bool force, const common::ObClusterRole cluster_role,
                    const int64_t timestamp);
  virtual int fetch(ObIAddrList &addr_list,
                    ObIAddrList &readonly_addr_list);
  int64_t get_agent_num() const
  {
    return MAX_AGENT_NUM;
  }
  int reload();
  bool is_valid();
private:
  virtual int check_inner_stat() const;
private:
  bool is_inited_;
  // ObWebServiceRootAddr and ObInnerConfigRootAddr
  share::ObRootAddrAgent *agents_[MAX_AGENT_NUM];
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
class ObRsMgr
{
public:
  friend class ObLeaseStateMgr;
  ObRsMgr();
  virtual ~ObRsMgr();

  int init(obrpc::ObSrvRpcProxy *srv_rpc_proxy, common::ObServerConfig *config,
      common::ObMySQLProxy *sql_proxy);
  bool is_inited() const { return inited_; }

  ObUnifiedAddrAgent &get_addr_agent() { return addr_agent_; }

  //get master rootserver by cluster id
  //@param [in] cluster_id
  //@param [out] master rs of the cluster
  int get_master_root_server(const int64_t cluster_id, common::ObAddr &addr) const;

  //get master rootserver of local cluster
  //@param [out] master rs of local cluster
  int get_master_root_server(common::ObAddr &addr) const;

  //renew master rootserver by cluster_id
  //@param [in] cluster_id
  int renew_master_rootserver(const int64_t cluster_id);

  //renew master rootserver of local cluster
  int renew_master_rootserver();

  //only local cluster
  int force_set_master_rs(const common::ObAddr &master_rs);

  //for remote cluster renew master rs and remove unused cluster
  int renew_remote_master_rootserver();


  // build a server_list to ask informations
  // @param [in] check_ls_service: if is true, try get rs_list from ls service.
  // @param [out] server_list, build from:
  //              (1) get master_rs from ObRsMgr
  //              (2) get rs_list from local configure
  //              (3) get member_list from ObLSService
  int construct_initial_server_list(
      const bool check_ls_service,
      common::ObIArray<common::ObAddr> &server_list);

private:
  class ObRemoteClusterIdGetter
  {
  public:
    ObRemoteClusterIdGetter() : cluster_id_list_() {}
    ~ObRemoteClusterIdGetter() {}
    int operator() (common::hash::HashMapPair<int64_t, common::ObAddr> &entry);
    const ObIArray<int64_t> &get_cluster_id_list() const { return cluster_id_list_; }
  private:
    common::ObArray<int64_t> cluster_id_list_;
    DISALLOW_COPY_AND_ASSIGN(ObRemoteClusterIdGetter);
  };
private:
  int check_inner_stat() const;
  int remove_unused_remote_master_rs_(const common::ObIArray<int64_t> &remote_cluster_id_list);
  int convert_addr_array(
          const ObIAddrList &root_addr_list,
          common::ObIArray<common::ObAddr> &addr_list);
  int get_all_rs_list_from_configure_(common::ObIArray<common::ObAddr> &addr_list);
private:
  bool inited_;
  obrpc::ObSrvRpcProxy *srv_rpc_proxy_;
  common::ObCommonConfig *config_;
  ObUnifiedAddrAgent addr_agent_;

  mutable common::ObSpinLock lock_; //protect master_rs_
  common::ObAddr master_rs_;
  common::hash::ObHashMap<int64_t, common::ObAddr, common::hash::ReadWriteDefendMode> remote_master_rs_map_;

  DISALLOW_COPY_AND_ASSIGN(ObRsMgr);
};

}//namespace share
}//namespace oceanbase

#endif //__OB_COMMON_OB_RS_INFO_H__
