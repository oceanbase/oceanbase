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

#include "ob_admin_routine.h"

#include <cstring>
#include <cstdlib>
#include <iostream>
#include <iomanip>
#include <unistd.h>
#include "ob_admin_utils.h"
#include "share/ob_rpc_struct.h"
#include "storage/tablelock/ob_table_lock_rpc_struct.h"

using namespace std;

#include "lib/time/ob_time_utility.h"           // ObTimeUtility
#define ADMIN_WARN(format, ...) fprintf(stderr, format "\n", ##__VA_ARGS__)
#define DEF_COMMAND(t, cmd, v...)                       \
  namespace oceanbase {                                 \
    namespace tools {                                   \
      class ObAdmin_##cmd                               \
        : public ObAdminRoutine                         \
        {                                               \
          public:                                       \
          ObAdmin_##cmd()                               \
            : ObAdminRoutine(#cmd, ##v)                 \
          { t; g_routines.push_back(this); }            \
            virtual int process();  \
        } command_##cmd; }}                             \
  int ObAdmin_##cmd::process()

#define TRANS target_="TRX"
#define SERVER target_="SVR"

using namespace oceanbase::tools;
using namespace oceanbase::common;
using namespace oceanbase::transaction;
using namespace oceanbase::transaction::tablelock;

namespace oceanbase {
  namespace tools {
    std::vector<ObAdminRoutine*> g_routines;
  } /* end of namespace tools */
} /* end of namespace oceanbase */

ObAdminRoutine::ObAdminRoutine(const string &action_name, int version, const string &args)
  : action_name_(action_name), version_(version), timeout_(3000000)
{
  args_ = args;
}

ObAdminRoutine::~ObAdminRoutine()
{}

bool ObAdminRoutine::match(const string &cmd) const
{
  string action;
  string::size_type pos;
  pos = cmd.find(" ");
  action = cmd.substr(0, pos);
  return action == action_name_;
}

DEF_COMMAND(TRANS, dump_memtable, 1, "tenant_id:ls_id:tablet_id # dump memtable to /tmp/memtable.*")
{
  int ret = OB_SUCCESS;
  string arg_str;
  uint64_t tablet_id;
  obrpc::ObDumpMemtableArg arg;
  if (cmd_ == action_name_) {
    ret = OB_INVALID_ARGUMENT;
    ADMIN_WARN("should provide partition_key");
  } else {
    arg_str = cmd_.substr(action_name_.length() + 1);
  }

  if (OB_FAIL(ret)) {
  } else if (3 != sscanf(arg_str.c_str(), "%ld:%ld:%ld", &arg.tenant_id_, &arg.ls_id_, &tablet_id)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid arg", K(ret));
  } else if (FALSE_IT(arg.tablet_id_ = tablet_id)) {
  } else if (OB_SUCCESS != (ret = client_->dump_memtable(arg))) {
    COMMON_LOG(ERROR, "send req fail", K(ret));
  }
  COMMON_LOG(INFO, "dump_memtable", K(arg));
  return ret;
}

DEF_COMMAND(TRANS, dump_tx_data_memtable, 1, "tenant_id:ls_id # dump tx data memtable to /tmp/tx_data_memtable.*")
{
  int ret = OB_SUCCESS;
  string arg_str;
  obrpc::ObDumpTxDataMemtableArg arg;
  if (cmd_.length() <= action_name_.length()) {
    ret = OB_INVALID_ARGUMENT;
    ADMIN_WARN("cmd is not completed.");
  } else {
    arg_str = cmd_.substr(action_name_.length() + 1);
  }

  if (OB_FAIL(ret)) {
  } else if (2 != sscanf(arg_str.c_str(), "%ld:%ld", &arg.tenant_id_, &arg.ls_id_)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid arg", K(ret));
  } else if (OB_SUCCESS != (ret = client_->dump_tx_data_memtable(arg))) {
    COMMON_LOG(ERROR, "send req fail", K(ret));
  }
  COMMON_LOG(INFO, "dump_tx_data_memtable", K(arg));
  return ret;
}

DEF_COMMAND(TRANS, dump_single_tx_data, 1, "tenant_id:ls_id:tx_id # dump a single tx data to /tmp/single_tx_data.*")
{
  int ret = OB_SUCCESS;
  string arg_str;
  obrpc::ObDumpSingleTxDataArg arg;
  if (cmd_.length() <= action_name_.length()) {
    ret = OB_INVALID_ARGUMENT;
    ADMIN_WARN("cmd is not completed");
  } else {
    arg_str = cmd_.substr(action_name_.length() + 1);
  }

  if (OB_FAIL(ret)) {
  } else if (3 != sscanf(arg_str.c_str(), "%ld:%ld:%ld", &arg.tenant_id_, &arg.ls_id_, &arg.tx_id_)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid arg", KR(ret));
  } else if (OB_SUCCESS != (ret = client_->dump_single_tx_data(arg))) {
    COMMON_LOG(ERROR, "send req fail", K(ret));
  }
  COMMON_LOG(INFO, "dump_single_tx_data", K(arg));
  return ret;
}

DEF_COMMAND(TRANS, force_set_replica_num, 1, "table_id:partition_idx:partition_cnt replica_num # force set replica_num")
{
  return OB_NOT_SUPPORTED;
}

DEF_COMMAND(TRANS, force_set_parent, 1, "table_id:partition_idx:partition_cnt ip:port # force set parent")
{
  return OB_NOT_SUPPORTED;
}

DEF_COMMAND(TRANS, force_set_server_list, 1, "replica_num ip:port ip:port ... #force set server list")
{
  int ret = OB_SUCCESS;

  string replica_num_str;
  ObAddr server;
  string server_str;
  obrpc::ObForceSetServerListArg arg;

  if (cmd_ == action_name_) {
    ret = OB_INVALID_ARGUMENT;
    ADMIN_WARN("should provide replica_num and ip:port");
  } else {
    string cmd_tail = cmd_.substr(action_name_.length() + 1);
    string::size_type pos;
    pos = cmd_tail.find(" ");
    if (pos == string::npos) {
      ret = OB_INVALID_ARGUMENT;
      ADMIN_WARN("should provide replica_num and ip:port");
    } else {
      replica_num_str = cmd_tail.substr(0, pos);
      arg.replica_num_ = atoi(replica_num_str.c_str());

      cmd_tail = cmd_tail.substr(pos + 1);

      while ((pos = cmd_tail.find(" ")) != string::npos && OB_SUCC(ret)) {
        server_str = cmd_tail.substr(0, pos);
        if (OB_FAIL(server.parse_from_cstring(server_str.c_str()))) {
          ADMIN_WARN("server parse_from_cstring failed");
        } else if (OB_FAIL(arg.server_list_.push_back(server))) {
          ADMIN_WARN("server_list_ push_back failed");
        } else {
          cmd_tail = cmd_tail.substr(pos+1);
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(server.parse_from_cstring(cmd_tail.c_str()))) {
          ADMIN_WARN("server parse_from_cstring failed");
        } else if (OB_FAIL(arg.server_list_.push_back(server))) {
          ADMIN_WARN("server_list_ push_back failed");
        } else {
          // do nothing
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(client_->force_set_server_list(arg))) {
      COMMON_LOG(ERROR, "force_set_server_list failed", K(ret));
    }
  }

  COMMON_LOG(INFO, "force_set_server_list", K(ret), "replica_num", arg.replica_num_, "server_list", arg.server_list_);

  return ret;
}

DEF_COMMAND(TRANS, halt_all_prewarming, 1, "# halt all warming up, release memstores")
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS != (ret = client_->halt_all_prewarming())) {
    COMMON_LOG(ERROR, "halt_all_warming_up", K(ret));
  }
  return ret;
}

DEF_COMMAND(TRANS, set_server_config, 1, "#set config")
{
  int ret = OB_SUCCESS;
  string sstr;
  ObString config_str;
  if (cmd_ == action_name_) {
    ret = OB_INVALID_ARGUMENT;
    ADMIN_WARN("should provide partition_key");
  } else {
    sstr = cmd_.substr(action_name_.length() + 1);
    config_str.assign_ptr(sstr.c_str(), static_cast<int32_t>(sstr.length()));
  }
  if (OB_FAIL(ret)) {
  } else if (OB_SUCCESS != client_->set_config(config_str)) {
    COMMON_LOG(ERROR, "add config failed.", K(config_str), K(ret));
  }
  return ret;
}

DEF_COMMAND(TRANS, set_tenant_config, 1, "tenant_id config_item1=config_value1,config_item2=config_value2 # set tenant config")
{
  int ret = OB_SUCCESS;
  string arg_str;
  ObTenantConfigArg arg;
  string::size_type pos;
  string tenant_id_str;
  string config_str;
  if (cmd_ == action_name_) {
    ret = OB_INVALID_ARGUMENT;
    ADMIN_WARN("should provide partition_key");
  } else {
    arg_str = cmd_.substr(action_name_.length() + 1);
    // resolve tenant_id
    pos = arg_str.find(" ");
    if (pos == string::npos) {
      ret = OB_INVALID_ARGUMENT;
      COMMON_LOG(WARN, "invalid format", K(ret), "str", arg_str.c_str());
    } else {
      tenant_id_str = arg_str.substr(0, pos);
      char *end_ptr = NULL;
      arg.tenant_id_ = strtoll(tenant_id_str.c_str(), &end_ptr, 10);
      if (*end_ptr != '\0') {
        ret = OB_INVALID_ARGUMENT;
        COMMON_LOG(WARN, "invalid format", K(ret), "str", arg_str.c_str());
      }
    }
    // resolve config_str
    if (OB_SUCC(ret)) {
      pos = arg_str.find_first_not_of(' ', pos);
      if (pos == string::npos) {
        ret = OB_INVALID_ARGUMENT;
        COMMON_LOG(WARN, "invalid format", K(ret), "str", arg_str.c_str());
      } else {
        config_str = arg_str.substr(pos);
        if (config_str.length() > common::OB_MAX_EXTRA_CONFIG_LENGTH) {
          ret = OB_SIZE_OVERFLOW;
          COMMON_LOG(WARN, "invalid config str length", K(ret), "str", arg_str.c_str());
        } else {
          arg.config_str_.assign_ptr(config_str.c_str(), config_str.length());
        }
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_SUCCESS != client_->set_tenant_config(arg)) {
    COMMON_LOG(ERROR, "add tenant config failed.", K(arg), K(ret));
  }
  COMMON_LOG(INFO, "set_tenant_config", K(ret), K(arg));
  return ret;
}

DEF_COMMAND(SERVER, diag, 1, "#login OceanBase")
{
  int ret = OB_SUCCESS;
  char buf[1024];
  ObString argstr(1024, 0, buf);
  if (OB_FAIL(client_->get_diagnose_args(argstr))) {
    ADMIN_WARN("get diagnose args from observer fail, ret: %d", ret);
  } else {
    vector<string> args;
    tools::split(string(argstr.ptr(), argstr.length()), ' ', args);
    for (vector<string>::const_iterator it = args.begin();
         it != args.end();
         it++) {
      cout << *it << endl;
    }
    tools::execv("/usr/bin/mysql", args);
  }
  return ret;
}

DEF_COMMAND(SERVER, force_switch_ilog_file, 1, "#force switch ilog file")
{
  int ret = OB_SUCCESS;
  obrpc::ObForceSwitchILogFileArg arg;
  arg.force_ = true;
  if (OB_SUCCESS != (ret = client_->force_switch_ilog_file(arg))) {
    COMMON_LOG(ERROR, "send req failed", K(ret));
  }
  COMMON_LOG(INFO, "force_switch_ilog_file", K(ret));
  return ret;
}

DEF_COMMAND(SERVER, force_set_all_as_single_replica, 1, "#force set all as single replica")
{
  int ret = OB_SUCCESS;
  obrpc::ObForceSetAllAsSingleReplicaArg arg;
  arg.force_ = true;
  if (OB_SUCCESS != (ret = client_->force_set_all_as_single_replica(arg))) {
    COMMON_LOG(ERROR, "send req failed", K(ret));
  }
  COMMON_LOG(INFO, "force_set_all_as_single_replica", K(ret));
  return ret;
}

DEF_COMMAND(SERVER, force_set_ls_as_single_replica, 1, "tenant_id:ls_id  # force set as single replica")
{
  int ret = OB_SUCCESS;
  obrpc::ObForceSetLSAsSingleReplicaArg arg;
  string arg_str;
  if (cmd_ == action_name_) {
    ret = OB_INVALID_ARGUMENT;
    ADMIN_WARN("## Error: should provide tenant_id:ls_id");
  } else {
    arg_str = cmd_.substr(action_name_.length() + 1);
  }
  uint64_t tenant_id = 0;
  int64_t ls_id_val = -1;
  if (OB_FAIL(ret)) {
  } else if (2 != sscanf(arg_str.c_str(), "%ld:%ld", &tenant_id, &ls_id_val)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid arg", K(ret));
  } else {
    share::ObLSID ls_id(ls_id_val);
    if (OB_SUCCESS != (ret = arg.init(tenant_id, ls_id))) {
      COMMON_LOG(ERROR, "init arg failed", K(ret), K(arg), K(tenant_id), K(ls_id));
    } else if (OB_SUCCESS != (ret = client_->force_set_ls_as_single_replica(arg))) {
      COMMON_LOG(ERROR, "send req failed", K(ret), K(arg));
    }
  }
  COMMON_LOG(INFO, "force_set_ls_as_single_replica", K(ret), K(arg));
  return ret;
}

DEF_COMMAND(SERVER, force_create_sys_table, 1, "tenant_id:table_id:last_replay_log_id # force create sys table")
{
  int ret = OB_SUCCESS;
  string arg_str;
  obrpc::ObForceCreateSysTableArg arg;
  if (cmd_ == action_name_) {
    ret = OB_INVALID_ARGUMENT;
    ADMIN_WARN("should provide tenant_id:table_id:last_replay_log_id");
  } else {
    arg_str = cmd_.substr(action_name_.length() + 1);
  }
  if (OB_FAIL(ret)) {
  } else if (3 != sscanf(arg_str.c_str(), "%ld:%ld:%ld", &arg.tenant_id_, &arg.table_id_, &arg.last_replay_log_id_)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid arg", K(ret));
  } else if (OB_SUCCESS != (ret = client_->force_create_sys_table(arg))) {
    COMMON_LOG(ERROR, "send req failed", K(ret));
  }
  COMMON_LOG(INFO, "force create sys table", K(ret), K(arg));
  return ret;
}

DEF_COMMAND(SERVER, force_set_locality, 1, "tenant_id locality # force set locality")
{
  int ret = OB_SUCCESS;
  string arg_str;
  obrpc::ObForceSetLocalityArg arg;
  if (cmd_ == action_name_) {
    ret = OB_INVALID_ARGUMENT;
    ADMIN_WARN("should provide tenant_id/locality");
  } else {
    arg_str = cmd_.substr(action_name_.length() + 1);
    string::size_type pos;
    string tenant_id_str;
    string locality_str;
    // resolve tenant_id
    pos = arg_str.find(" ");
    if (pos == string::npos) {
      ret = OB_INVALID_ARGUMENT;
      COMMON_LOG(WARN, "invalid format", K(ret), "str", arg_str.c_str());
    } else {
      tenant_id_str = arg_str.substr(0, pos);
      char *end_ptr = NULL;
      arg.exec_tenant_id_ = strtoll(tenant_id_str.c_str(), &end_ptr, 10);
      if (*end_ptr != '\0') {
        ret = OB_INVALID_ARGUMENT;
        COMMON_LOG(WARN, "invalid format", K(ret), "str", arg_str.c_str());
      }
    }
    // resolve locality
    if (OB_SUCC(ret)) {
      pos = arg_str.find_first_not_of(' ', pos);
      if (pos == string::npos) {
        ret = OB_INVALID_ARGUMENT;
        COMMON_LOG(WARN, "invalid format", K(ret), "str", arg_str.c_str());
      } else {
        locality_str = arg_str.substr(pos);
        if (locality_str.length() > common::MAX_LOCALITY_LENGTH) {
          ret = OB_SIZE_OVERFLOW;
          COMMON_LOG(WARN, "invalid locality str length", K(ret), "str", arg_str.c_str());
        } else {
          arg.locality_.assign_ptr(locality_str.c_str(), locality_str.length());
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(client_->force_set_locality(arg))) {
        COMMON_LOG(ERROR, "send req failed", K(ret));
      }
    }
  }
  COMMON_LOG(INFO, "force set locality", K(ret), K(arg));
  return ret;
}

DEF_COMMAND(SERVER, force_disable_blacklist, 1, "# force disable blacklist")
{
  int ret = OB_SUCCESS;

  if (OB_SUCCESS != (ret = client_->force_disable_blacklist())) {
    COMMON_LOG(ERROR, "send req fail", K(ret));
  }
  return ret;
}

DEF_COMMAND(SERVER, force_enable_blacklist, 1, "# force enable blacklist")
{
  int ret = OB_SUCCESS;

  if (OB_SUCCESS != (ret = client_->force_enable_blacklist())) {
    COMMON_LOG(ERROR, "send req fail", K(ret));
  }
  return ret;
}

DEF_COMMAND(SERVER, force_clear_blacklist, 1, "# force clear blacklist")
{
  int ret = OB_SUCCESS;

  if (OB_SUCCESS != (ret = client_->force_clear_srv_blacklist())) {
    COMMON_LOG(ERROR, "send req fail", K(ret));
  }
  return ret;
}

DEF_COMMAND(SERVER, force_switch_leader, 1, "table_id:partition_idx:partition_cnt ip:port # force switch leader")
{
  return OB_NOT_SUPPORTED;
}

DEF_COMMAND(SERVER, batch_switch_rs_leader, 1, "ip:port # batch switch rs leader")
{
  int ret = OB_SUCCESS;
  string pkey_str;
  string new_parent_str;  // ip:port
  ObAddr new_leader;
  obrpc::ObSwitchLeaderArg arg;
  ObString cmd;
  ObString action_name;
  cmd.assign_ptr(cmd_.c_str(), static_cast<int32_t>(cmd_.length()));
  action_name.assign_ptr(action_name_.c_str(), static_cast<int32_t>(action_name_.length()));
  if (OB_UNLIKELY(cmd_ == action_name_)) {
    new_leader.reset();
    COMMON_LOG(INFO, "addr is default, auto batch switch rs leader", 
        K(cmd), K(action_name));
  } else {
    new_parent_str = cmd_.substr(action_name_.length() + 1);
    if (OB_SUCCESS != (ret = new_leader.parse_from_cstring(new_parent_str.c_str()))) {
      COMMON_LOG(ERROR, "new_leader.parse_from_cstring fail", K(new_parent_str.c_str()), 
          KR(ret), K(cmd), K(action_name));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(client_)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "client_ is NULL", KR(ret));
  } else if (OB_SUCCESS != (ret = client_->batch_switch_rs_leader(new_leader))) {
    COMMON_LOG(ERROR, "send batch switch rs leader req fail",
        KR(ret), K(new_leader), K(cmd), K(action_name));
  }

  return ret;
}

DEF_COMMAND(SERVER, batch_switch_leader, 1, "[table_id:partition_idx;] ip:port # batch switch leader")
{
  return OB_NOT_SUPPORTED;
}

DEF_COMMAND(SERVER, modify_tenant_memory, 1,
    "tenant_id:memory_size:refresh_interval(s) # modify_tenant_memory")
{
  int ret = OB_SUCCESS;
  int64_t tenant_id = 0;
  int64_t memory_size = 0;
  int64_t refresh_interval = 0;
  obrpc::ObTenantMemoryArg tenant_memory;
  if (cmd_ == action_name_) {
    ret = OB_INVALID_ARGUMENT;
    ADMIN_WARN("should provide tenant_id and memory_size");
  } else {
    string cmd_tail = cmd_.substr(action_name_.length() + 1);
    if (3 != sscanf(cmd_tail.c_str(), "%ld:%ld:%ld", &tenant_id, &memory_size, &refresh_interval)) {
      ret = OB_INVALID_ARGUMENT;
      COMMON_LOG(WARN, "invalid argument", K(ret), K(tenant_id), K(memory_size), K(refresh_interval));
    }
  }

  if (OB_FAIL(ret)) {
  } else {
    tenant_memory.tenant_id_ = tenant_id;
    tenant_memory.memory_size_ = memory_size;
    tenant_memory.refresh_interval_ = refresh_interval;
    COMMON_LOG(INFO, "tenant_id, memory_size, refresh_interval", K(tenant_id),
      K(memory_size), K(refresh_interval));
    if (OB_SUCCESS != (ret = client_->update_tenant_memory(tenant_memory))) {
      COMMON_LOG(ERROR, "send req fail", K(ret));
    }
  }
  return ret;
}

DEF_COMMAND(TRANS, kill_part_trans_ctx, 1,
    "partition_id:partition_cnt:table_id trans_inc:timestamp svr_ip svr_port # kill_part_trans_ctx")
{
  return OB_NOT_SUPPORTED;
}

// ls_remove_member
// @params [in]  tenant_id, which tenant to modify
// @params [in]  ls_id, which log stream to modify
// @params [in]  svr_ip, the server ip want to delete
// @params [in]  svr_port, the server port want to delete
// @params [in]  orig_paxos_number, paxos replica number before this deletion
// @params [in]  new_paxos_number, paxos replica number after this deletion
// ATTENTION:
//    Please make sure let log stream's leader to execute this command
//    For permanant offline, orig_paxos_number should equals to new_paxos_number
DEF_COMMAND(TRANS, ls_remove_member, 1, "tenant_id ls_id svr_ip svr_port orig_paxos_number new_paxos_number # ls_remove_member")
{
  int ret = OB_SUCCESS;
  string arg_str;
  ObLSDropPaxosReplicaArg arg;
  int64_t tenant_id_to_set = OB_INVALID_TENANT_ID;
  int64_t ls_id_to_set = 0;
  int64_t orig_paxos_replica_number = 0;
  int64_t new_paxos_replica_number = 0;
  int32_t port = 0;
  char ip[30];

  if (cmd_ == action_name_) {
    ret = OB_INVALID_ARGUMENT;
    ADMIN_WARN("should provide tenant_id, ls_id ,member to remove, previous and new paxos replica number");
  } else {
    arg_str = cmd_.substr(action_name_.length() + 1);
  }

  if (OB_FAIL(ret)) {
  } else if (6 != sscanf(arg_str.c_str(), "%ld %ld %s %d %ld %ld", &tenant_id_to_set, &ls_id_to_set,
                         ip, &port, &orig_paxos_replica_number, &new_paxos_replica_number)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid arg", K(ret), K(arg_str.c_str()), K(cmd_.c_str()),
               K(tenant_id_to_set), K(ls_id_to_set),
               K(port), K(orig_paxos_replica_number), K(new_paxos_replica_number));
  } else {
    common::ObAddr server_to_remove(common::ObAddr::VER::IPV4, ip, port);
    common::ObReplicaMember remove_member(server_to_remove, 1);
    share::ObTaskId task_id;
    share::ObLSID ls_id(ls_id_to_set);
    task_id.init(server_to_remove);
    if (OB_ISNULL(client_)
        || OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id_to_set
                      || !ls_id.is_valid_with_tenant(tenant_id_to_set)
                      || !server_to_remove.is_valid()
                      || 1 < orig_paxos_replica_number - new_paxos_replica_number
                      || 0 > orig_paxos_replica_number - new_paxos_replica_number)) {
      ret = OB_INVALID_ARGUMENT;
      COMMON_LOG(WARN, "invalid argument", K(ret), K(tenant_id_to_set), K(ls_id),
                 K(remove_member), K(task_id), K(orig_paxos_replica_number),
                 K(new_paxos_replica_number), K(port), K(ip), KP(client_));
    } else if (OB_FAIL(arg.init(
                 task_id,
                 tenant_id_to_set,
                 ls_id,
                 remove_member,
                 orig_paxos_replica_number,
                 new_paxos_replica_number))) {
      COMMON_LOG(WARN, "init arg failed", K(ret), K(task_id), K(tenant_id_to_set), K(ls_id),
                 K(remove_member), K(orig_paxos_replica_number), K(new_paxos_replica_number));
    } else if (OB_FAIL(client_->ls_remove_paxos_replica(arg))) {
      COMMON_LOG(ERROR, "send req fail", K(ret));
    }
  }
  COMMON_LOG(INFO, "ls_remove_member", K(arg));
  return ret;
}

// remove_lock
// @params [in]  tenant_id, which tenant to modify
// @params [in]  ls_id, which log stream to modify
// @params [in]  obj_type, lock object type (1:OBJ_TYPE_TABLE, 2:OBJ_TYPE_TABLET)
// @params [in]  obj_id, lock object id
// @params [in]  lock_mode, lock mode (1:EXCLUSIVE, 2:SHARE, 4:ROW_EXCLUSIVE, 6:SHARE_ROW_EXCLUSIVE, 8:ROW_SHARE)
// @params [in]  owner_id, for OUT_TRANS lock and unlock
// @params [in]  create_tx_id, which transaction create this lock
// @params [in]  op_type, (1:IN_TRANS_DML_LOCK; 2:OUT_TRANS_LOCK; 3:OUT_TRANS_UNLOCK; 4:IN_TRANS_LOCK_TABLE_LOCK)
DEF_COMMAND(TRANS, remove_lock, 1, "tenant_id ls_id obj_type obj_id lock_mode owner_id create_tx_id op_type # remove_lock")
{
  int ret = OB_SUCCESS;
  string arg_str;
  ObAdminRemoveLockOpArg arg;
  int64_t tenant_id_to_set = OB_INVALID_TENANT_ID;
  int64_t ls_id_to_set = 0;
  int64_t obj_type = 0; // how to change int64_t to be enum class
  int64_t obj_id = 0;
  int64_t lock_mode = 0;
  int64_t owner_id = 0;
  int64_t create_tx_id = 0;
  int64_t op_type = 0;
  int64_t lock_op_status = 1; // does not used.
  ObTxSEQ seq_no;
  int64_t create_timestamp = 0;
  int64_t create_schema_version = 0;

  if (cmd_ == action_name_) {
    ret = OB_INVALID_ARGUMENT;
    ADMIN_WARN("should provide tenant_id, ls_id, obj_type, obj_id, lock_mode, owner_id, create_tx_id, op_type");
  } else {
    arg_str = cmd_.substr(action_name_.length() + 1);
  }

  if (OB_FAIL(ret)) {
  } else if (8 != sscanf(arg_str.c_str(),
                          "%ld %ld %ld %ld %ld %ld %ld %ld",
                          &tenant_id_to_set, &ls_id_to_set, &obj_type,
                          &obj_id, &lock_mode, &owner_id, &create_tx_id, &op_type)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid arg", K(ret), K(arg_str.c_str()), K(cmd_.c_str()),
               K(tenant_id_to_set), K(ls_id_to_set),
               K(obj_type), K(obj_id), K(lock_mode), K(owner_id), K(create_tx_id),
               K(op_type), K(lock_op_status), K(seq_no), K(create_timestamp),
               K(create_schema_version));
  } else {
    share::ObLSID ls_id(ls_id_to_set);
    ObLockID lock_id;
    ObLockOBJType real_obj_type = static_cast<ObLockOBJType>(obj_type);
    ObTableLockMode real_lock_mode = static_cast<ObTableLockMode>(lock_mode);
    ObTableLockOwnerID real_owner_id = static_cast<ObTableLockOwnerID>(owner_id);
    ObTransID real_create_tx_id = create_tx_id;
    ObTableLockOpType real_op_type = static_cast<ObTableLockOpType>(op_type);
    ObTableLockOpStatus real_lock_op_status = static_cast<ObTableLockOpStatus>(lock_op_status);
    ObTableLockOp lock_op;
    lock_id.set(real_obj_type, obj_id);

    lock_op.set(lock_id, real_lock_mode, real_owner_id, real_create_tx_id, real_op_type,
                real_lock_op_status, seq_no, create_timestamp, create_schema_version);
    if (OB_ISNULL(client_)
        || OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id_to_set
                       || !ls_id.is_valid_with_tenant(tenant_id_to_set)
                       || !lock_op.is_valid())) {
      ret = OB_INVALID_ARGUMENT;
      COMMON_LOG(WARN, "invalid argument", K(ret), K(tenant_id_to_set), K(ls_id),
                 K(lock_op), KP(client_));
    } else if (OB_FAIL(arg.set(tenant_id_to_set, ls_id, lock_op))) {
      COMMON_LOG(WARN, "set remove lock op arg failed", K(ret), K(tenant_id_to_set),
                 K(ls_id), K(lock_op));
    } else if (OB_FAIL(client_->admin_remove_lock_op(arg))) {
      COMMON_LOG(ERROR, "send req fail", K(ret));
    }
  }
  COMMON_LOG(INFO, "remove_lock", K(arg));
  return ret;
}

// update_lock
// @params [in]  tenant_id, which tenant to modify
// @params [in]  ls_id, which log stream to modify
// @params [in]  obj_type, lock object type (1:OBJ_TYPE_TABLE, 2:OBJ_TYPE_TABLET)
// @params [in]  obj_id, lock object id
// @params [in]  lock_mode, lock mode (1:EXCLUSIVE, 2:SHARE, 4:ROW_EXCLUSIVE, 6:SHARE_ROW_EXCLUSIVE, 8:ROW_SHARE)
// @params [in]  owner_id, for OUT_TRANS lock and unlock
// @params [in]  create_tx_id, which transaction create this lock
// @params [in]  op_type, (1:IN_TRANS_DML_LOCK; 2:OUT_TRANS_LOCK; 3:OUT_TRANS_UNLOCK; 4:IN_TRANS_LOCK_TABLE_LOCK)
// @params [in]  op_status, (1:LOCK_OP_DOING; 2:LOCK_OP_COMPLETE;)
// @params [in]  commit_version, the lock op transaction commit version
// @params [in]  commit_scn, the lock op transaction commit scn
DEF_COMMAND(TRANS, update_lock, 1, "tenant_id ls_id obj_type obj_id lock_mode owner_id create_tx_id op_type new_op_status commit_version commit_scn# update_lock")
{
  int ret = OB_SUCCESS;
  string arg_str;
  ObAdminUpdateLockOpArg arg;
  int64_t tenant_id_to_set = OB_INVALID_TENANT_ID;
  int64_t ls_id_to_set = 0;
  int64_t obj_type = 0; // how to change int64_t to be enum class
  int64_t obj_id = 0;
  int64_t lock_mode = 0;
  int64_t owner_id = 0;
  int64_t create_tx_id = 0;
  int64_t op_type = 0;
  int64_t lock_op_status = 1;
  int64_t commit_version = 0;
  int64_t commit_scn = 0;
  ObTxSEQ seq_no;
  int64_t create_timestamp = 0;
  int64_t create_schema_version = 0;

  if (cmd_ == action_name_) {
    ret = OB_INVALID_ARGUMENT;
    ADMIN_WARN("should provide tenant_id, ls_id, obj_type, obj_id, lock_mode, owner_id, create_tx_id, op_type, new_op_status, commit_version, commit_scn");
  } else {
    arg_str = cmd_.substr(action_name_.length() + 1);
  }

  if (OB_FAIL(ret)) {
  } else if (11 != sscanf(arg_str.c_str(),
                          "%ld %ld %ld %ld %ld %ld %ld %ld %ld %ld %ld",
                          &tenant_id_to_set, &ls_id_to_set, &obj_type,
                          &obj_id, &lock_mode, &owner_id, &create_tx_id, &op_type,
                          &lock_op_status, &commit_version, &commit_scn)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid arg", K(ret), K(arg_str.c_str()), K(cmd_.c_str()),
               K(tenant_id_to_set), K(ls_id_to_set),
               K(obj_type), K(obj_id), K(lock_mode), K(owner_id), K(create_tx_id),
               K(op_type), K(lock_op_status), K(commit_version), K(commit_scn));
  } else {
    share::ObLSID ls_id(ls_id_to_set);
    ObLockID lock_id;
    ObLockOBJType real_obj_type = static_cast<ObLockOBJType>(obj_type);
    ObTableLockMode real_lock_mode = static_cast<ObTableLockMode>(lock_mode);
    ObTableLockOwnerID real_owner_id = static_cast<ObTableLockOwnerID>(owner_id);
    ObTransID real_create_tx_id = create_tx_id;
    ObTableLockOpType real_op_type = static_cast<ObTableLockOpType>(op_type);
    ObTableLockOpStatus real_lock_op_status = static_cast<ObTableLockOpStatus>(lock_op_status);
    ObTableLockOp lock_op;
    share::SCN real_commit_version;
    share::SCN real_commit_scn;

    lock_id.set(real_obj_type, obj_id);
    lock_op.set(lock_id, real_lock_mode, real_owner_id, real_create_tx_id, real_op_type,
                real_lock_op_status, seq_no, create_timestamp, create_schema_version);
    real_commit_version.convert_for_tx(commit_version);
    real_commit_scn.convert_for_logservice(commit_scn);
    if (OB_ISNULL(client_)
        || OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id_to_set
                       || !ls_id.is_valid_with_tenant(tenant_id_to_set)
                       || !lock_op.is_valid())) {
      ret = OB_INVALID_ARGUMENT;
      COMMON_LOG(WARN, "invalid argument", K(ret), K(tenant_id_to_set), K(ls_id),
                 K(lock_op), KP(client_));
    } else if (OB_FAIL(arg.set(tenant_id_to_set,
                               ls_id,
                               lock_op,
                               real_commit_version,
                               real_commit_scn))) {
      COMMON_LOG(WARN, "set update lock op arg failed", K(ret), K(tenant_id_to_set),
                 K(ls_id), K(lock_op), K(real_commit_version), K(real_commit_scn));
    } else if (OB_FAIL(client_->admin_update_lock_op(arg))) {
      COMMON_LOG(ERROR, "send req fail", K(ret));
    }
  }
  COMMON_LOG(INFO, "update_lock", K(arg));
  return ret;
}

#ifdef OB_BUILD_ARBITRATION
// force_clear_arb_cluster_info
// @params [in]  cluster_id, which cluster to modify
// @params [in]  svr_ip, the arbitration server IP
// @params [in]  svr_port, the arbitration server IP
// ATTENTION:
//    Please make sure let log stream's leader to execute this command
//    For permanant offline, orig_paxos_number should equals to new_paxos_number
DEF_COMMAND(TRANS, force_clear_arb_cluster_info, 1, "cluster_id # force_clear_arb_cluster_info")
{
  int ret = OB_SUCCESS;
  string arg_str;
  int64_t cluster_id_to_clean = OB_INVALID_TENANT_ID;

  if (cmd_ == action_name_) {
    ret = OB_INVALID_ARGUMENT;
    ADMIN_WARN("should provide cluster_id arb_svr_ip arb_svr_port");
  } else {
    arg_str = cmd_.substr(action_name_.length() + 1);
  }

  if (OB_FAIL(ret)) {
  } else if (1 != sscanf(arg_str.c_str(), "%ld", &cluster_id_to_clean)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid arg", K(ret), K(arg_str.c_str()), K(cmd_.c_str()), K(cluster_id_to_clean));
  } else if (false == is_valid_cluster_id(cluster_id_to_clean)) {
    COMMON_LOG(WARN, "invalid cluster_id", K(ret), K(cluster_id_to_clean));
  } else {
    ObForceClearArbClusterInfoArg arg(cluster_id_to_clean);
    if (OB_FAIL(client_->force_clear_arb_cluster_info(arg))) {
      COMMON_LOG(ERROR, "send req fail", K(ret));
    }
  }
  COMMON_LOG(INFO, "force_clear_arb_cluster_info", K(cluster_id_to_clean));
  return ret;
}
#endif

// unlock_member_list
// @params [in]  tenant_id, which tenant to modify
// @params [in]  ls_id, which log stream to modify
DEF_COMMAND(SERVER, unlock_member_list, 1, "tenant_id:ls_id:lock_id # unlock_member_list")
{
  int ret = OB_SUCCESS;
  string arg_str;
  ObAdminUnlockMemberListOpArg arg;
  uint64_t tenant_id_to_set = OB_INVALID_TENANT_ID;
  int64_t ls_id_to_set = 0;
  int64_t lock_id = -1;

  if (cmd_ == action_name_) {
    ret = OB_INVALID_ARGUMENT;
    ADMIN_WARN("should provide tenant_id, ls_id");
  } else {
    arg_str = cmd_.substr(action_name_.length() + 1);
  }

  if (OB_FAIL(ret)) {
  } else if (3 != sscanf(arg_str.c_str(), "%ld:%ld:%ld", &tenant_id_to_set, &ls_id_to_set, &lock_id)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid arg", K(ret), K(arg_str.c_str()), K(cmd_.c_str()),
               K(tenant_id_to_set), K(ls_id_to_set), K(lock_id));
  } else {
    share::ObLSID ls_id(ls_id_to_set);
    if (OB_INVALID_ID == tenant_id_to_set || !ls_id.is_valid()) {
      ret = OB_INVALID_ARGUMENT;
      COMMON_LOG(WARN, "argument is invalid", K(ret), K(tenant_id_to_set), K(ls_id));
    } else if (OB_FAIL(arg.set(tenant_id_to_set, ls_id, lock_id))) {
      COMMON_LOG(WARN, "failed to set unlock member list op arg", K(ret), K(tenant_id_to_set), K(ls_id), K(lock_id));
    } else if (OB_FAIL(client_->admin_unlock_member_list_op(arg))) {
      COMMON_LOG(ERROR, "send req fail", K(ret));
    }
  }
  COMMON_LOG(INFO, "unlock_member_list", K(ret), K(arg));
  return ret;
}
