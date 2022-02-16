/**
 * (C) 2010-2014 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Time-stamp: <>
 * Version: $Id$
 * Filename: ob_admin_routine.cpp
 *
 * Authors:
 *   Shi Yudi <fufeng.syd@alipay.com>
 *
 */

#include "ob_admin_routine.h"

#include <cstring>
#include <cstdlib>
#include <iostream>
#include <iomanip>
#include <unistd.h>
#include "ob_admin_utils.h"
#include "share/ob_rpc_struct.h"

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


DEF_COMMAND(TRANS, force_set_as_single_replica, 1, "table_id:partition_idx:partition_cnt # force set as single replica")
{
  int ret = OB_SUCCESS;
  string pkey_str;
  ObPartitionKey pkey;
  if (cmd_ == action_name_) {
    ret = OB_INVALID_ARGUMENT;
    ADMIN_WARN("should provide partition_key");
  } else {
    pkey_str = cmd_.substr(action_name_.length() + 1);
  }

  if (OB_FAIL(ret)) {
  } else if (OB_SUCCESS != (ret = pkey.parse(pkey_str.c_str()))) {
    COMMON_LOG(ERROR, "pkey.parse fail", K(pkey_str.c_str()), K(ret));
  } else if (OB_SUCCESS != (ret = client_->force_set_as_single_replica(pkey))) {
    COMMON_LOG(ERROR, "send req fail", K(ret));
  }
  return ret;
}

DEF_COMMAND(TRANS, force_remove_replica, 1, "table_id:partition_idx:partition_cnt # force remove replica")
{
  int ret = OB_SUCCESS;
  string pkey_str;
  ObPartitionKey pkey;
  if (cmd_ == action_name_) {
    ret = OB_INVALID_ARGUMENT;
    ADMIN_WARN("should provide partition_key");
  } else {
    pkey_str = cmd_.substr(action_name_.length() + 1);
  }

  if (OB_FAIL(ret)) {
  } else if (OB_SUCCESS != (ret = pkey.parse(pkey_str.c_str()))) {
    COMMON_LOG(ERROR, "pkey.parse fail", K(pkey_str.c_str()), K(ret));
  } else if (OB_SUCCESS != (ret = client_->force_remove_replica(pkey))) {
    COMMON_LOG(ERROR, "send req fail", K(ret));
  }
  return ret;
}

DEF_COMMAND(TRANS, force_set_replica_num, 1, "table_id:partition_idx:partition_cnt replica_num # force set replica_num")
{
  int ret = OB_SUCCESS;
  string pkey_str;
  string replica_num_str;
  int replica_num = 0;
  obrpc::ObSetReplicaNumArg arg;
  ObPartitionKey pkey;
  if (cmd_ == action_name_) {
    ret = OB_INVALID_ARGUMENT;
    ADMIN_WARN("should provide partition_key");
  } else {
    string cmd_tail = cmd_.substr(action_name_.length() + 1);
    string::size_type pos;
    pos = cmd_tail.find(" ");
    if (pos == string::npos) {  // not assign new parent
      ret = OB_INVALID_ARGUMENT;
      ADMIN_WARN("should provide partition_key and new_parent");
    } else {
      pkey_str = cmd_tail.substr(0, pos);
      replica_num_str = cmd_tail.substr(pos + 1);
      replica_num = atoi(replica_num_str.c_str());
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_SUCCESS != (ret = pkey.parse(pkey_str.c_str()))) {
    COMMON_LOG(ERROR, "pkey.parse fail", K(pkey_str.c_str()), K(ret));
  } else {
    arg.partition_key_ = pkey;
    arg.replica_num_ = replica_num;
    if (OB_SUCCESS != (ret = client_->force_set_replica_num(arg))) {
      COMMON_LOG(ERROR, "send req fail", K(ret));
    }
  }
  return ret;
}

DEF_COMMAND(TRANS, force_reset_parent, 1, "table_id:partition_idx:partition_cnt # force reset parent")
{
  int ret = OB_SUCCESS;
  string pkey_str;
  ObPartitionKey pkey;
  if (cmd_ == action_name_) {
    ret = OB_INVALID_ARGUMENT;
    ADMIN_WARN("should provide partition_key");
  } else {
    pkey_str = cmd_.substr(action_name_.length() + 1);
  }

  if (OB_FAIL(ret)) {
  } else if (OB_SUCCESS != (ret = pkey.parse(pkey_str.c_str()))) {
    COMMON_LOG(ERROR, "pkey.parse fail", K(pkey_str.c_str()), K(ret));
  } else if (OB_SUCCESS != (ret = client_->force_reset_parent(pkey))) {
    COMMON_LOG(ERROR, "send req fail", K(ret));
  }
  return ret;
}

DEF_COMMAND(TRANS, force_set_parent, 1, "table_id:partition_idx:partition_cnt ip:port # force set parent")
{
  int ret = OB_SUCCESS;
  string pkey_str;
  string new_parent_str;  // ip:port
  ObPartitionKey pkey;
  ObAddr parent;
  obrpc::ObSetParentArg arg;
  if (cmd_ == action_name_) {
    ret = OB_INVALID_ARGUMENT;
    ADMIN_WARN("should provide partition_key");
  } else {
    string cmd_tail = cmd_.substr(action_name_.length() + 1);
    string::size_type pos;
    pos = cmd_tail.find(" ");
    if (pos == string::npos) {  // not assign new parent
      ret = OB_INVALID_ARGUMENT;
      ADMIN_WARN("should provide partition_key and new_parent");
    } else {
      pkey_str = cmd_tail.substr(0, pos);
      new_parent_str = cmd_tail.substr(pos + 1);
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_SUCCESS != (ret = pkey.parse(pkey_str.c_str()))) {
    COMMON_LOG(ERROR, "pkey.parse fail", K(pkey_str.c_str()), K(ret));
  } else if (OB_SUCCESS != (ret = parent.parse_from_cstring(new_parent_str.c_str()))) {
    COMMON_LOG(ERROR, "parent.parse_from_cstring fail", K(new_parent_str.c_str()), K(ret));
  } else {
    arg.partition_key_ = pkey;
    arg.parent_addr_ = parent;
    if (OB_SUCCESS != (ret = client_->force_set_parent(arg))) {
      COMMON_LOG(ERROR, "send req fail", K(ret));
    }
  }
  return ret;
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


DEF_COMMAND(SERVER, force_switch_leader, 1, "table_id:partition_idx:partition_cnt ip:port # force switch leader")
{
  int ret = OB_SUCCESS;
  string pkey_str;
  string new_parent_str;  // ip:port
  ObPartitionKey pkey;
  ObAddr new_leader;
  obrpc::ObSwitchLeaderArg arg;
  if (cmd_ == action_name_) {
    ret = OB_INVALID_ARGUMENT;
    ADMIN_WARN("should provide partition_key");
  } else {
    string cmd_tail = cmd_.substr(action_name_.length() + 1);
    string::size_type pos;
    pos = cmd_tail.find(" ");
    if (pos == string::npos) {  // not assign new_leader
      ret = OB_INVALID_ARGUMENT;
      ADMIN_WARN("should provide partition_key and new_parent");
    } else {
      pkey_str = cmd_tail.substr(0, pos);
      new_parent_str = cmd_tail.substr(pos + 1);
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_SUCCESS != (ret = pkey.parse(pkey_str.c_str()))) {
    COMMON_LOG(ERROR, "pkey.parse fail", K(pkey_str.c_str()), K(ret));
  } else if (OB_SUCCESS != (ret = new_leader.parse_from_cstring(new_parent_str.c_str()))) {
    COMMON_LOG(ERROR, "new_leader.parse_from_cstring fail", K(new_parent_str.c_str()), K(ret));
  } else {
    arg.partition_key_ = pkey;
    arg.leader_addr_ = new_leader;
    if (OB_SUCCESS != (ret = client_->switch_leader(arg))) {
      COMMON_LOG(ERROR, "send req fail", K(ret));
    }
  }
  return ret;
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
  int ret = OB_SUCCESS;
  string str;
  string pkey;
  string addr;  // ip:port
  ObPartitionKey new_pkey;
  ObAddr new_leader;
  obrpc::ObSwitchLeaderListArg arg;

  if (cmd_ == action_name_) {
    ret = OB_INVALID_ARGUMENT;
    ADMIN_WARN("should provide partition_key");
  } else {
    string cmd_tail = cmd_.substr(action_name_.length() + 1);
    string::size_type pos;
    pos = cmd_tail.find('[');
    if (pos == string::npos) {  // not find pkey lists
      ret = OB_INVALID_ARGUMENT;
      ADMIN_WARN("should provide partition_key lists");
    } else {
      str = cmd_tail.substr(pos + 1);
      while (OB_SUCC(ret) && string::npos != (pos = str.find(';'))) {
        pkey = str.substr(0, pos);
        str = str.substr(pos + 1);

        if (OB_FAIL(new_pkey.parse(pkey.c_str()))) {
          COMMON_LOG(ERROR, "pkey.parse fail", K(pkey.c_str()), K(ret));
        } else {
          arg.partition_key_list_.push_back(new_pkey);
        }
      }

      if (OB_FAIL(ret)) {
      } else if (0 == arg.partition_key_list_.count()) {
        COMMON_LOG(WARN, "empty command", "str", str.c_str(), "action_name", action_name_.c_str(),
            "cmd", cmd_.c_str());
      } else if (string::npos == (pos = str.find(']'))) {
        ret = OB_INVALID_ARGUMENT;
        ADMIN_WARN("should provide new leader addr");
      } else {
        addr = str.substr(pos + 1);
        if (OB_FAIL(new_leader.parse_from_cstring(addr.c_str()))) {
          COMMON_LOG(ERROR, "new_leader.parse_from_cstring fail", K(addr.c_str()), K(ret));
        } else {
          arg.leader_addr_ = new_leader;
          if (OB_SUCCESS != (ret = client_->switch_leader_list_async(arg, NULL))) {
            COMMON_LOG(ERROR, "send req fail", K(ret));
          }
        }
      }
    }
  }

  return ret;
}
