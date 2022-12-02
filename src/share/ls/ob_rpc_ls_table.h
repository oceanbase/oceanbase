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

#ifndef OCEANBASE_LS_OB_RPC_LS_TABLE_H_
#define OCEANBASE_LS_OB_RPC_LS_TABLE_H_

#include "share/ls/ob_ls_table.h"    // for ObLSTable
#include "share/ls/ob_ls_info.h"     // for ObLSInfo

namespace oceanbase
{
namespace common
{
class ObServerConfig;
class ObMySQLProxy;
}
namespace obrpc
{
class ObCommonRpcProxy;
class ObSrvRpcProxy;
}

namespace share
{
class ObRsMgr;

// [class_full_name] ObRpcLSTable
// [class_functions] use rpc path to get and update informations
// [class_attention] None
class ObRpcLSTable : public ObLSTable
{
public:
  // initial related
  explicit ObRpcLSTable();
  virtual ~ObRpcLSTable();
  int init(
      obrpc::ObCommonRpcProxy &rpc_proxy,
      obrpc::ObSrvRpcProxy &srv_rpc_proxy,
      share::ObRsMgr &rs_mgr,
      common::ObMySQLProxy &sql_proxy);
  inline bool is_inited() const { return is_inited_; }

  // get informations about a certain ls through rpc
  // @param [in] cluster_id, belong to which cluster
  // @parma [in] tenant_id, get whose ls info
  // @param [in] ls_id, get which ls info
  // @param [in] mode, should not be ObLSTable::INNER_TABLE_ONLY_MODE
  // @param [out] ls_info, informations about a certain ls
  // TODO: enable cluster_id
  virtual int get(
      const int64_t cluster_id,
      const uint64_t tenant_id,
      const ObLSID &ls_id,
      const ObLSTable::Mode mode,
      ObLSInfo &ls_info) override;

  // update new log stream replica to meta table
  // @param [in] replica, new informations to update
  // @param [in] inner_table_only, shoud be false
  virtual int update(const ObLSReplica &replica, const bool inner_table_only) override;

  // @param [in] inner_table_only, shoud be false
  virtual int remove(
      const uint64_t tenant_id,
      const ObLSID &ls_id,
      const ObAddr &server,
      const bool inner_table_only) override;

private:
  int check_inner_stat_() const;
  int get_timeout_(int64_t &timeout);

  int get_ls_info_(ObLSInfo &ls_info);
  int get_ls_info_across_cluster_(const int64_t cluster_id, ObLSInfo &ls_info);

  // to fetch ls_info from the given server_list
  // @param [in] cluster_id, from which cluster to fetch infos
  // @param [in] initial_server_list, servers to send rpc
  // @param [out] ls_info, structure to store informations of a certain ls
  int do_detect_master_rs_ls_(const int64_t cluster_id,
                             const ObIArray<ObAddr> &initial_server_list,
                             share::ObLSInfo &ls_info);
  // find the leader replica of a certain ls after get its ls_info from server_list
  // @param [in] cluster_id, from which cluster to fetch infos
  // @param [in] initial_server_list, servers to send rpc
  // @params[out] leader, the leader replica of a certain ls
  int do_detect_master_rs_ls_(const int64_t cluster_id,
                             const common::ObIArray<common::ObAddr> &initial_server_list,
                             common::ObAddr &leader);

  // to fetch infos about a ls among servers in server_list which has index between start_idx and end_idx
  // @param [in] cluster_id, from which cluster to fetch infos
  // @param [in] start_idx, start index in server_list
  // @param [in] end_idx, end index in server_list
  // @param [out] server_list, servers to fetch infos, this list could change(expand)
  // @param [out] log_strema_info, the structure to store infos about a ls
  int do_detect_master_rs_ls_(
      const int64_t cluster_id,
      const int64_t start_idx,
      const int64_t end_idx,
      common::ObIArray<ObAddr> &server_list,
      share::ObLSInfo &ls_info);
  //
  // @param [in] result, result about a ls infos fetched through rpc
  // @param [out] leader_exist, whether a leader replica has been found
  // @param [out] server_list, expanded server_list to fetch infos
  // @param [out] ls_info, the structure to store infos about a ls
  int deal_with_result_ls_(
      const obrpc::ObDetectMasterRsLSResult &result,
      bool &leader_exist,
      common::ObIArray<ObAddr> &server_list,
      share::ObLSInfo &ls_info);
  
private:
  bool is_inited_;                      //whether this class is inited
  obrpc::ObCommonRpcProxy *rpc_proxy_;  //the rpc proxy to use
  share::ObRsMgr *rs_mgr_;              //the rs_mgr to use
  obrpc::ObSrvRpcProxy *srv_rpc_proxy_; //for async rpc
  common::ObMySQLProxy *sql_proxy_;     //for get rs_list from __all_cluster_config
private:
  DISALLOW_COPY_AND_ASSIGN(ObRpcLSTable);
};

}//end namespace share
}//end namespace oceanbase

#endif
