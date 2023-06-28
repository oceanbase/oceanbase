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

#ifndef OCEANBASE_LS_OB_PERSISTENT_LS_TABLE_H_
#define OCEANBASE_LS_OB_PERSISTENT_LS_TABLE_H_

#include "share/ls/ob_ls_table.h" // for ObLSTable

namespace oceanbase
{
namespace common
{
class ObMySQLProxy;
class ObServerConfig;
class ObISQLClient;
namespace sqlclient
{
class ObMySQLResult;
}
} // end namespace common

namespace share
{
// [class_full_name] ObPersistentLSTable
// [class_functions] for log stream table store in meta_tanant or user_tenant
// [class_attention] None
class ObPersistentLSTable : public ObLSTable
{
public:
  // Use this class to decide whether to use trans automatically
  // You can use start_trans to start a trans
  // and get_sql_client will return trans_ if trans is started
  class AutoTransProxy
  {
  public:
    AutoTransProxy(common::ObISQLClient &sql_proxy)
        : sql_proxy_(sql_proxy),
          trans_()
    {}
    virtual ~AutoTransProxy() {}
    // start_trans - to start a trans by using sql_proxy
    // @param [in] sql_tenant_id, to run under which tenant
    // @param [in] with_snapshot, whether use snapshot
    int start_trans(uint64_t sql_tenant_id, bool with_snapshot);
    // end_trans - to end a trans when needed
    int end_trans(const bool is_succ);
    // is_trans_started - whether a trans is runing
    bool is_trans_started() const { return trans_.is_started(); }
    // get_sql_client - return  ObISQLClient or ObMySQLTransaction
    common::ObISQLClient& get_sql_client();
  private:
    common::ObISQLClient &sql_proxy_;  //use common proxy to execute sql
    common::ObMySQLTransaction trans_; //use trans proxy to execute sql
  };

  // initial related functions
  explicit ObPersistentLSTable();
  virtual ~ObPersistentLSTable();
  int init(common::ObISQLClient &sql_proxy, common::ObServerConfig *config);
  bool is_inited() const { return inited_; }

  // get informations about a ls belongs to meta/ueser tenant
  // @param [in] cluster_id, belong to which cluster
  // @parma [in] tenant_id, get whose ls info
  // @param [in] ls_id, get which ls info
  // @param [in] mode, useless
  // @param [out] ls_info, informations about this ls
  // TODO: enable cluster_id
  virtual int get(
      const int64_t cluster_id,
      const uint64_t tenant_id,
      const ObLSID &ls_id,
      const ObLSTable::Mode mode,
      ObLSInfo &ls_info) override;

  // update new replica infos to meta table
  // @param [in] replica, new replica infos to update
  // @param [in] inner_table_only, useless
  virtual int update(const ObLSReplica &replica, const bool inner_table_only) override;

  // get all ls info from __all_ls_meta_table according to tenant_id
  //
  // @param [in] tenant_id, target tenant
  // @param [out] ls_infos, all ObLSInfos in __all_ls_meta_table
  int get_by_tenant(const uint64_t tenant_id, ObIArray<ObLSInfo> &ls_infos);

  // load all ls info in __all_ls_meta_table of the tenant_id
  // @param [in] tenant_id, target tenant, only sys and meta tenant
  // @param [out] ls_infos, all ls infos in __all_ls_meta_table
  int load_all_ls_in_tenant(const uint64_t exec_tenant_id, ObIArray<ObLSInfo> &ls_infos);

  // @param [in] inner_table_only, useless
  virtual int remove(
      const uint64_t tenant_id,
      const ObLSID &ls_id,
      const ObAddr &server,
      const bool inner_table_only) override;

  // remove residual ls in __all_ls_meta_table for ObServerMetaTableChecker
  //
  // @param [in] tenant_id, tenant for query
  // @param [in] server, target ObAddr
  // @param [out] residual_count, count of residual ls in table
  int remove_residual_ls(
      const uint64_t tenant_id,
      const ObAddr &server,
      int64_t &residual_count);

  // batch get ls info from __all_ls_meta_table
  //
  // @param [in] cluster_id, target cluster_id
  // @parma [in] tenant_id,  target tenant_id
  // @param [in] ls_ids,  target ls_id array
  // @param [out] ls_infos, information of ls
  // @return OB_SUCCESS if success
  //         OB_ERR_DUP_ARGUMENT if ls_ids have duplicate values
  int batch_get(
      const int64_t cluster_id,
      const uint64_t tenant_id,
      const common::ObIArray<ObLSID> &ls_ids,
      common::ObIArray<ObLSInfo> &ls_infos);

private:
  // ls table sql operation
  // use select for update for reading if lock_replica is true.
  // @parma [in] lock_replica, whether to execute 'for update'
  // @param [in] ls_id, which ls's info to fetch
  // @param [in] filter_flag_replica, to deal with flag_replica
  // @param [in] cluster_id, from which cluster to fetch infos
  // @param [in] sql_client, use sql_client or trans to update
  // @param [out] ls_info, meta/user tenant's ls's info
  // TODO: 1. make sure what is a flag replica  2. enable cluster_id
  int fetch_ls_info_(
      const bool lock_replica,
      const bool filter_flag_replica,
      const int64_t cluster_id,
      const uint64_t tenant_id,
      const ObLSID &ls_id,
      ObISQLClient &sql_client,
      ObLSInfo &ls_info);

  // construct a replica from what is read out of table
  // @param [in] res, result read from table belongs to a certain tenant
  // @param [out] replica, the structure to store informations about a replica
  int construct_ls_replica(
      common::sqlclient::ObMySQLResult &res,
      ObLSReplica &replica);

  // contruct ls_info by constructing each replica
  // @param [in] res, result read from table belongs to a certain tenant
  // @param [in] filter_flag_replica, to deal with flag_replica
  // @param [out] ls_info, meta/user tenant's ls's info

  int construct_ls_info(
      common::sqlclient::ObMySQLResult &res,
      const bool filter_flag_replica,
      ObLSInfo &ls_info);
  int construct_ls_infos_(
      common::sqlclient::ObMySQLResult &res,
      ObIArray<ObLSInfo> &ls_infos);

  // if update a leader replica, lock lines in meta table to avoid parallel updates
  // @param [in] cluster_id, from which cluster to fetch infos
  // @param [in] sql_client, use sql_client or trans to update
  // @param [in] tenant_id, whose ls to set
  // @param [in] ls_id, which ls to set
  // @param [out] max_proposal_id, max proposal id of the ls from ls meta table
  int lock_lines_in_meta_table_for_leader_update_(
      const int64_t cluster_id,
      ObISQLClient &sql_client,
      const uint64_t tenant_id,
      const ObLSID &ls_id,
      int64_t &max_proposal_id);

  // if update a leader replica, set others to follower
  // @param [in] sql_client, use sql_client or trans to update
  // @param [in] tenant_id, whose ls to set
  // @param [in] ls_id, which ls to set
  // @param [in] leader_server, which server/replica is leader
  int set_role_(
      ObISQLClient &sql_client,
      const uint64_t tenant_id,
      const ObLSID &ls_id,
      const common::ObAddr &leader_server);

  // the real action to change the content of the table
  // @param [in] sql_client, use sql_client or trans to update
  // @param [in] replica, informations to update
  int update_replica_(
      ObISQLClient &sql_client,
      const ObLSReplica &replica);

  // construct the infos to update into table
  int fill_dml_splicer_(
      const ObLSReplica &replica,
      ObDMLSqlSplicer &dml_splicer);

private:
  bool inited_;                     //whether this class is inited
  common::ObISQLClient *sql_proxy_; //sql_proxy to use
  common::ObServerConfig *config_;  //server config to use

  DISALLOW_COPY_AND_ASSIGN(ObPersistentLSTable);
};

} // end namespace share
} // end namespace oceanbase

#endif // OCEANBASE_LS_OB_PERSISTENT_LS_TABLE_H_
