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

#ifndef OCEANBASE_ROOTSERVICE_OBADMIN_SWITCH_REPLICA_ROLE_H_
#define OCEANBASE_ROOTSERVICE_OBADMIN_SWITCH_REPLICA_ROLE_H_

#include "share/ob_rpc_struct.h"
#include "share/ob_ls_id.h"
#include "lib/string/ob_string.h"
#include "lib/string/ob_sql_string.h"

namespace oceanbase
{
using namespace obrpc;

namespace rootserver
{

class ObAdminSwitchReplicaRole
{
  const static int64_t OB_ADMIN_SWITCH_REPLICA_ROLE_ARG_COUNT = 4;
public:
  static int handle_switch_replica_role_sql_command(
      const obrpc::ObAdminSwitchReplicaRoleArg &arg);
  static int handle_switch_replica_role_obadmin_command(
      const ObAdminSwitchReplicaRoleStr &command_arg_str);
  static int parse_params_from_obadmin_command_arg(
      const ObAdminSwitchReplicaRoleStr &command_arg_str,
      ObAdminSwitchReplicaRoleArg &command_arg,
      uint64_t &tenant_id);
private:
  static int execute_switch_replica_role_(
      const obrpc::ObAdminSwitchReplicaRoleArg &arg,
      const uint64_t &tenant_id);
  static int check_server_valid_(
      const obrpc::ObAdminSwitchReplicaRoleArg &arg);
  static int get_tenant_id_by_name_(
      const obrpc::ObAdminSwitchReplicaRoleArg &arg,
      uint64_t &tenant_id);
  static int update_ls_election_reference_info_table_(
      const obrpc::ObAdminSwitchReplicaRoleArg &arg,
      const int64_t tenant_id);
  static int trans_role_str_to_value_(
      const ObSqlString &role_str,
      common::ObRole &role);
private:
  DISALLOW_COPY_AND_ASSIGN(ObAdminSwitchReplicaRole);
};

} // end namespace rootserver
} // end namespace oceanbase
#endif // OCEANBASE_ROOTSERVICE_OBADMIN_SWITCH_REPLICA_ROLE_H_