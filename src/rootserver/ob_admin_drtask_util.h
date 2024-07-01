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

#ifndef OCEANBASE_ROOTSERVICE_OBADMIN_DRTASK_UTIL_H_
#define OCEANBASE_ROOTSERVICE_OBADMIN_DRTASK_UTIL_H_

#include "logservice/palf/palf_handle_impl.h" // for PalfStat
namespace oceanbase
{
namespace rootserver
{

enum ObAdminDRTaskRetComment
{
  SUCCEED_TO_SEND_COMMAND = 0,
  TENANT_ID_OR_LS_ID_NOT_VALID = 1,
  SERVER_TO_EXECUTE_COMMAND_NOT_LEADER = 2,
  FAILED_TO_SEND_RPC = 3,
  FAIL_TO_EXECUTE_COMMAND = 4,
  MAX_COMMENT
};
const char* ob_admin_drtask_ret_comment_strs(const rootserver::ObAdminDRTaskRetComment ret_comment);

class ObAdminDRTaskUtil
{
public:
  // handle ob_admin request
  // params[in]  command_arg, arg which contains admin_command
  static int handle_obadmin_command(const ObAdminCommandArg &command_arg);

  // parse parameters from ob_admin command
  // params[in]  command_arg, arg which contains admin_command
  // params[out] tenant_id, specified tenant_id
  // params[out] ls_id, specified ls_id
  // params[out] replica_type, specified replica_type
  // params[out] data_source_server, specified data_source
  // params[out] target_member, specified target_member
  // params[out] leader_addr, specified leader_addr
  // params[out] orig_paxos_replica_number, specified original quorum
  // params[out] new_paxos_replica_number, specified new quorum
  static int parse_params_from_obadmin_command_arg(
         const ObAdminCommandArg &command_arg,
         uint64_t &tenant_id,
         share::ObLSID &ls_id,
         ObReplicaType &replica_type,
         common::ObAddr &data_source_server,
         common::ObAddr &target_server,
         int64_t &orig_paxos_replica_number,
         int64_t &new_paxos_replica_number);
private:
  // handle ob_admin remove replica task request
  // params[in]  command_arg, arg which contains admin_command
  // params[in]  tenant_id, specified tenant_id
  // params[in]  ls_id, specified ls_id
  // params[out] ret_comment, failed reason
  static int handle_remove_command_(
         const ObAdminCommandArg &command_arg,
         uint64_t &tenant_id,
         share::ObLSID &ls_id,
         ObAdminDRTaskRetComment &ret_comment);

  // construct remove paxos replica task arg
  // params[in]  tenant_id, specified tenant_id
  // params[in]  ls_id, specified ls_id
  // params[in]  target_server, the replica to remove on which server
  // params[out] orig_paxos_replica_number, orig paxos_replica_number
  // params[out] new_paxos_replica_number, new paxos_replica_number
  // params[out] ret_comment, failed reason
  // params[out] remove_paxos_arg, arg for remove-F task
  static int construct_remove_paxos_task_arg_(
         const uint64_t &tenant_id,
         const share::ObLSID &ls_id,
         const common::ObAddr &target_server,
         int64_t &orig_paxos_replica_number,
         int64_t &new_paxos_replica_number,
         ObAdminDRTaskRetComment &ret_comment,
         ObLSDropPaxosReplicaArg &remove_paxos_arg);

  // construct remove non-paxos replica task arg
  // params[in]  tenant_id, specified tenant_id
  // params[in]  ls_id, specified ls_id
  // params[in]  target_server, the replica to remove on which server
  // params[out] ret_comment, failed reason
  // params[out] remove_non_paxos_arg, arg for remove-R task
  static int construct_remove_nonpaxos_task_arg_(
         const uint64_t &tenant_id,
         const share::ObLSID &ls_id,
         const common::ObAddr &target_server,
         ObAdminDRTaskRetComment &ret_comment,
         ObLSDropNonPaxosReplicaArg &remove_nonpaxos_arg);

  // get palf stat locally
  // params[in]  tenant_id, specified tenant_id
  // params[in]  ls_id, specified ls_id
  // params[out] palf_stat, palf informations
  // params[out] ret_comment, failed reason
  static int get_local_palf_stat_(
         const uint64_t &tenant_id,
         const share::ObLSID &ls_id,
         palf::PalfStat &palf_stat,
         ObAdminDRTaskRetComment &ret_comment);

  // execute remove paxos replica task
  // params[in]  command_arg, arg which contains admin_command
  // params[in]  remove_paxos_arg, arg for remove-F task
  static int execute_remove_paxos_task_(
         const ObAdminCommandArg &command_arg,
         const ObLSDropPaxosReplicaArg &remove_paxos_arg);

  // execute remove non-paxos replica task
  // params[in]  command_arg, arg which contains admin_command
  // params[in]  remove_non_paxos_arg, arg for remove-R task
  static int execute_remove_nonpaxos_task_(
         const ObAdminCommandArg &command_arg,
         const ObLSDropNonPaxosReplicaArg &remove_non_paxos_arg);

  // handle ob_admin add replica task request
  // params[in] command_arg, arg which contains admin_command
  // params[in] tenant_id, specified tenant_id
  // params[in] ls_id, specified ls_id
  // params[out] ret_comment, failed reason
  static int handle_add_command_(
         const ObAdminCommandArg &command_arg,
         uint64_t &tenant_id,
         share::ObLSID &ls_id,
         ObAdminDRTaskRetComment &ret_comment);

  // construct arg for add task
  // params[in]  command_arg, arg which contains admin_command
  // params[in]  arg, arg for add replica task
  // params[out] ret_comment, failed reason
  static int construct_arg_for_add_command_(
         const ObAdminCommandArg &command_arg,
         ObLSAddReplicaArg &arg,
         ObAdminDRTaskRetComment &ret_comment);

  // construct default value for some params
  // params[in]  tenant_id, specified tenant_id
  // params[in]  ls_id, specified ls_id
  // params[out] orig_paxos_replica_number, orig paxos_replica_number
  // params[out] leader_server, data source
  static int construct_default_params_for_add_command_(
         const uint64_t &tenant_id,
         const share::ObLSID &ls_id,
         int64_t &orig_paxos_replica_number,
         common::ObAddr &leader_server);

  // execute remove task
  // params[in]  command_arg, arg which contains admin_command
  // params[in]  arg, arg for add replica task
  // params[out] ret_comment, failed reason
  static int execute_task_for_add_command_(
         const ObAdminCommandArg &command_arg,
         const ObLSAddReplicaArg &arg,
         ObAdminDRTaskRetComment &ret_comment);

  // try construct ret comment to show
  // params[in]  ret_code, retured ret_code
  // params[in]  ret_comment, failed reason
  // params[out] result_comment, the output message
  static int try_construct_result_comment_(
         const int &ret_code,
         const ObAdminDRTaskRetComment &ret_comment,
         ObSqlString &result_comment);
};
} // end namespace rootserver
} // end namespace oceanbase
#endif /* OCEANBASE_ROOTSERVICE_OBADMIN_DRTASK_UTIL_H_ */
