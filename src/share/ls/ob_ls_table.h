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

#ifndef OCEANBASE_LS_OB_LS_TABLE_H_
#define OCEANBASE_LS_OB_LS_TABLE_H_

#include "share/ls/ob_ls_info.h"       // for ObLSInfo
#include "share/ob_rpc_struct.h"                       // for common::is_strong_leader()related

namespace oceanbase
{
namespace common
{
class ObAddr;
}

namespace share
{
class ObLSReplica;
class ObLSInfo;

// [class_full_name] ObLSTable
// [class_functions] the basic class to handle different operators
// [class_attention] None
class ObLSTable
{
public:
  // Mode is used to control data source of sys tenant's ls info.
  // Other tenant's ls infos always come from inner table.
  enum Mode {
    DEFAULT_MODE          = 0,  // sys tenant's ls info come from rs
    INNER_TABLE_ONLY_MODE = 1,  // sys tenant's ls info come from inner table
    COMPOSITE_MODE        = 2   // sys tenant's ls info come from both rs and inner table
  };
public:
  explicit ObLSTable();
  virtual ~ObLSTable();

  // get a certain ls's paxos_member_list
  // @param [in] tenant_id, which tenant's ls
  // @param [in] ls_id, which ls's member_list
  // @param [out] member_list, the paxos_mamber_list of a certain ls
  int get_member_list(
    const uint64_t tenant_id,
    const ObLSID &ls_id,
    ObMemberList &member_list);

  // get the role of this server's replica of a certain ls
  // param [in] tenant_id, which tenant's ls
  // param [in] ls_id, which ls's member_list
  // param [out] role, role of replica in this server
  int get_role(
    const uint64_t tenant_id,
    const ObLSID &ls_id,
    common::ObRole &role);

  // check whether tenant_id and ls_id is valid
  // @param [in] tenant_id, identifier of a tenant
  // @param [in] ls_id, identifier of a ls
  // @param [out] bool, valid or not
  static bool is_valid_key(const uint64_t tenant_id, const ObLSID &ls_id);

  // base function to get informations about a certain ls
  // @param [in] cluster_id, belong to which cluster
  // @parma [in] tenant_id, get whose ls info
  // @param [in] ls_id, get which ls info
  // @param [in] mode, determine data source of sys tenant's ls info
  // @param [out] ls_info, informations about a certain ls
  // TODO: enable cluster_id
  virtual int get(
      const int64_t cluster_id,
      const uint64_t tenant_id,
      const ObLSID &ls_id,
      const ObLSTable::Mode mode,
      ObLSInfo &ls_info) = 0;

  // base function to report a new replica
  // @param [in] replica, new replica informations to update
  // @param [in] inner_table_only, determine whether the sys tenant's ls info is recorded in inner table or in memory.
  virtual int update(const ObLSReplica &replica, const bool inner_table_only) = 0;
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
      const bool inner_table_only) = 0;

  // check whether have to update a replica
  // @param [in] lhs, the old log stream replica
  // @param [in] rhs, the new log stream replica
  // @param [in] update, whether have to do update
  static int check_need_update(const ObLSReplica &lhs, const ObLSReplica &rhs, bool &update);
protected:
  DISALLOW_COPY_AND_ASSIGN(ObLSTable);
};
} // end namespace share
} // end namespace oceanbase

#endif // OCEANBASE_LS_OB_LS_TABLE_H_
