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

#ifndef OCEANBASE_LS_OB_LS_TABLE_OPERATOR_H_
#define OCEANBASE_LS_OB_LS_TABLE_OPERATOR_H_

#include "ob_ls_table.h"                  // for ObLSTable
#include "ob_inmemory_ls_table.h"         // for ObInMemoryLSTable
#include "ob_rpc_ls_table.h"              // for ObRpcLSTable
#include "ob_persistent_ls_table.h"       // for ObPersistentLSTable

namespace oceanbase
{
// forward declaration
namespace obrpc
{
class ObCommonRpcProxy;
}// end namespace obrpc
namespace common
{
class ObServerConfig;
class ObMySQLProxy;
}// end namespace common
namespace share
{

// [class_full_name] ObLSTableOperator
// [class_functions] To do related operations towards __all_ls_meta_table
// [class_attention] None
class ObLSTableOperator : public ObLSTable
{
public:
  explicit ObLSTableOperator();
  virtual ~ObLSTableOperator();

  int init(common::ObISQLClient &sql_proxy, common::ObServerConfig *config = NULL);
  bool is_inited() const { return inited_; }

  // use for rootservice to set a certain log-stream to use
  ObInMemoryLSTable* get_inmemory_ls() { return &inmemory_ls_; }

  // when server is rs to use inmemory path to get and update infod
  int set_callback_for_rs(ObIRsListChangeCb &rs_list_change_cb);

  // when server is observer to use rpc to get and update informations
  // @param [in] rpc_proxy, to use which proxy
  // @param [in] srv_rpc_proxy, to use which proxy
  // @param [in] rs_mgr, rs_mgr to use
  // @param [in] sql_proxy, sql proxy to use
  int set_callback_for_obs(
      obrpc::ObCommonRpcProxy &rpc_proxy,
      obrpc::ObSrvRpcProxy &srv_rpc_proxy,
      ObRsMgr &rs_mgr,
      common::ObMySQLProxy &sql_proxy);

  // operator for location_cache - get informations about a certain ls
  // @param [in] cluster_id, belong to which cluster
  // @parma [in] tenant_id, get whose ls info
  // @param [in] ls_id, get which ls info
  // @param [in] mode, determine data source of sys tenant's ls info
  // @param [out] ls_info, informations about a certain ls
  // TODO: make sure how to enable cluster_id
  virtual int get(
      const int64_t cluster_id,
      const uint64_t tenant_id,
      const ObLSID &ls_id,
      const ObLSTable::Mode mode,
      ObLSInfo &ls_info) override;

  // operator for report - update a certain log stream's info
  // @param [in] replica, new replica informations to update
  // @param [in] inner_table_only, determine whether the sys tenant's ls info is recorded in inner table or in memory.
  virtual int update(const ObLSReplica &replica, const bool inner_table_only) override;

  // remove ls replica from __all_ls_meta_table
  //
  // @param [in] tenant_id, the tenant which the ls belongs to
  // @param [in] ls_id, the ls which you want to remove
  // @param [in] server, address of the ls replica
  // @param [in] inner_table_only, determine whether the sys tenant's ls info is recorded in inner table or in memory.
  virtual int remove(
      const uint64_t tenant_id,
      const ObLSID &ls_id,
      const ObAddr &server,
      const bool inner_table_only) override;

  // get all ls info in __all_ls_meta_table according to tenant_id
  // used for ObLSTableIterator in ObTenantMetaChecker
  // @param [in] tenant_id, target tenant
  // @param [out] ls_infos, all ls infos in __all_ls_meta_table
  // @param [in] inner_table_only, determine whether the sys tenant's ls info is recorded in inner table or in memory.
  int get_by_tenant(const uint64_t tenant_id, const bool inner_table_only, ObIArray<ObLSInfo> &ls_infos);

  // load all ls info in __all_ls_meta_table of the tenant_id
  //
  // used by ObAllLSTableIterator/ObTenantLSTableIterator in ObEmptyServerChecker/ObLostReplicaChecker
  // will use share::ObLSTable::COMPOSITE_MODE to fetch sys tenant's ls
  //
  // @param [in] tenant_id, target tenant, only sys and meta tenant
  // @param [out] ls_infos, all ls infos in __all_ls_meta_table or __all_virtual_core_meta_table
  int load_all_ls_in_tenant(const uint64_t exec_tenant_id, ObIArray<ObLSInfo> &ls_infos);

  // remove residual ls in __all_ls_meta_table for ObServerMetaTableChecker
  // won't deal with sys tenant
  // @param [in] tenant_id, tenant for query
  // @param [in] server, target ObAddr
  // @param [out] residual_count, count of residual ls in table
  int remove_residual_ls(
      const uint64_t tenant_id,
      const ObAddr &server,
      int64_t &residual_count);
  // batch get ls info
  //
  // @param [in] cluster_id, target cluster_id
  // @parma [in] tenant_id,  target tenant_id
  // @param [in] ls_ids,  target ls_id array
  // @param [in] mode, determine data source of sys tenant's ls info
  // @param [out] ls_infos, information of ls
  // @return OB_ERR_DUP_ARGUMENT if ls_ids have duplicate values
  int batch_get(
      const int64_t cluster_id,
      const uint64_t tenant_id,
      const common::ObIArray<ObLSID> &ls_ids,
      const ObLSTable::Mode mode,
      common::ObIArray<ObLSInfo> &ls_infos);
private:
  // to use inmemory path to get and update informations
  int set_use_memory_ls_(ObIRsListChangeCb &rs_list_change_cb);

  // to use rpc path to get and update informations
  // @param [in] rpc_proxy, to use which proxy
  // @param [in] srv_rpc_proxy, to use which proxy
  // @param [in] rs_mgr, rs_mgr to use
  // @param [in] sql_proxy, sql proxy to use
  int set_use_rpc_ls_(
      obrpc::ObCommonRpcProxy &rpc_proxy,
      obrpc::ObSrvRpcProxy &srv_rpc_proxy,
      ObRsMgr &rs_mgr,
      common::ObMySQLProxy &sql_proxy);

  // decide which ls_table to use according to ls_id: inmemory/rpc/persistent
  // @param [in] tenant_id, get whose ls info
  // @param [in] ls_id, get which ls info
  // @param [in] inner_table_only, determine whether the sys tenant's ls info is recorded in inner table or in memory.
  // @param [out] ls_table, decide inmemory/rpc/persistent ls_table to use
  int get_ls_table_(
      const int64_t cluster_id,
      const uint64_t tenant_id,
      const ObLSID &ls_id,
      const bool inner_table_only,
      ObLSTable *&ls_table);

private:
  bool inited_;                        //whether this class is inited
  ObLSTable *root_ls_;                 //basic class for different LsTable
  ObInMemoryLSTable inmemory_ls_;      //abstract of sys_tenant for rs
  ObRpcLSTable rpc_ls_;                //abstract of sys_tenant for no-rs
  ObPersistentLSTable persistent_ls_;  //abstract for user_tenant and meta_tenant

  DISALLOW_COPY_AND_ASSIGN(ObLSTableOperator);
};

} // end namespace share
} // end namespace oceanbase
#endif // OCEANBASE_LS_OB_LS_TABLE_OPERATOR_H_
