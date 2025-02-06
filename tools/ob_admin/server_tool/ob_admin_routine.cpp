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
#include "ob_admin_utils.h"
#include "../ob_admin_common_utils.h"

#include "share/ob_io_device_helper.h"
#ifdef OB_BUILD_SHARED_STORAGE
#endif
using namespace std;

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
using namespace oceanbase::lib;
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::storage;
using namespace oceanbase::blocksstable;
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
  obrpc::ObForceSetServerListResult result;

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
    if (OB_FAIL(client_->force_set_server_list(arg, result))) {
      COMMON_LOG(ERROR, "force_set_server_list failed", K(ret));
    }
  }

  fprintf(stdout, "-----------------------{force_set_server_list result}-----------------------\n");
  fprintf(stdout, "{\"ob_admin_execute_ret_code\":%d, \"observer_execute_ret_code\":%d, \"result_list\":[", ret, result.ret_);
  ARRAY_FOREACH(result.result_list_, idx) {
    obrpc::ObForceSetServerListResult::ResultInfo result_info = result.result_list_.at(idx);
    fprintf(stdout, "{"); // single result_info start
    fprintf(stdout, "\"tenant_id\":%lu, ", result_info.tenant_id_);
    fprintf(stdout, "\"successful_ls_id\":[");
    if (0 < result_info.successful_ls_.size()) {
      ARRAY_FOREACH(result_info.successful_ls_, idx) {
        share::ObLSID ls = result_info.successful_ls_.at(idx);
        if (idx == result_info.successful_ls_.size() - 1) {
          fprintf(stdout, "%ld", ls.id());
        } else {
          fprintf(stdout, "%ld,", ls.id());
        }
      }
    }
    fprintf(stdout, "], \"failed_ls_info\":[");
    // print failed ls info
    if (0 < result_info.failed_ls_info_.size()) {
      ARRAY_FOREACH(result_info.failed_ls_info_, idx) {
        fprintf(stdout, "{");
        obrpc::ObForceSetServerListResult::LSFailedInfo failed_info = result_info.failed_ls_info_.at(idx);
        fprintf(stdout, "\"ls_id\":%ld, \"failed_ret_code\":%d, \"failed_reason\":\"%s\"", failed_info.ls_id_.id(), failed_info.failed_ret_code_, ob_error_name(failed_info.failed_ret_code_));
        if (idx == result_info.failed_ls_info_.size() - 1) {
          fprintf(stdout, "}");
        } else {
          fprintf(stdout, "}, ");
        }
      }
    }
    fprintf(stdout, "]");  // failed_ls_info end
    fprintf(stdout, "}");  // single result_info end
    if (idx != result.result_list_.size() - 1) {
      fprintf(stdout, ", ");
    }
  }
  fprintf(stdout, "]}\n");

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
    ObTableLockOwnerID real_owner_id;
    ObTransID real_create_tx_id = create_tx_id;
    ObTableLockOpType real_op_type = static_cast<ObTableLockOpType>(op_type);
    ObTableLockOpStatus real_lock_op_status = static_cast<ObTableLockOpStatus>(lock_op_status);
    ObTableLockOp lock_op;
    lock_id.set(real_obj_type, obj_id);

    real_owner_id.convert_from_value(owner_id);
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
    ObTableLockOwnerID real_owner_id;
    ObTransID real_create_tx_id = create_tx_id;
    ObTableLockOpType real_op_type = static_cast<ObTableLockOpType>(op_type);
    ObTableLockOpStatus real_lock_op_status = static_cast<ObTableLockOpStatus>(lock_op_status);
    ObTableLockOp lock_op;
    share::SCN real_commit_version;
    share::SCN real_commit_scn;

    real_owner_id.convert_from_value(owner_id);
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


// remove_ls_replica
// @params [in]  tenant_id, which tenant to modify
// @params [in]  ls_id, which log stream to modify
// @params [in]  server, the server address of the replica to remove
// @params [in]  replica_type, what type of replica to remove
// @params [in]  orig_paxos_number, paxos replica number before this deletion
// @params [in]  new_paxos_number, paxos replica number after this deletion
// @params [in]  leader, leader replica's address
// ATTENTION:
//    Please make sure tenant_id and ls_id are specified.
//    Other parameters are optional, if not specified, it will be automatically caculated
DEF_COMMAND(TRANS, remove_ls_replica, 1, "tenant_id=xxx,ls_id=xxx[server=xxx,replica_type=xxx,orig_paxos_replica_number=xxx,new_paxos_replica_number=xxx,leader=xxx] # remove_ls_replica")
{
  int ret = OB_SUCCESS;
  string arg_str;
  ObAdminCommandArg arg;
  const ObAdminDRTaskType task_type(ObAdminDRTaskType::REMOVE_REPLICA);
  if (cmd_ == action_name_) {
    ret = OB_INVALID_ARGUMENT;
    ADMIN_WARN("should provide tenant_id, ls_id at least");
  } else {
    arg_str = cmd_.substr(action_name_.length() + 1);
  }

  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(client_)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid client", K(ret));
  } else if (OB_FAIL(arg.init(arg_str.c_str(), task_type))) {
    COMMON_LOG(WARN, "fail to construct admin command arg", K(ret), K(arg_str.c_str()), K(task_type));
  } else if (OB_FAIL(client_->ob_exec_drtask_obadmin_command(arg))) {
    COMMON_LOG(ERROR, "send req fail", K(ret), K(arg));
  }
  COMMON_LOG(INFO, "remove_ls_replica", K(arg));
  return ret;
}

// add_ls_replica
// @params [in]  tenant_id, which tenant to modify
// @params [in]  ls_id, which log stream to modify
// @params [in]  server, the server address of the replica to add
// @params [in]  replica_type, what type of replica to add
// @params [in]  data_source, data source replica server
// @params [in]  orig_paxos_number, paxos replica number before this deletion
// @params [in]  new_paxos_number, paxos replica number after this deletion
// ATTENTION:
//    Please make sure tenant_id, ls_id are specified.
//    Other parameters are optional, if not specified, it will be automatically caculated
DEF_COMMAND(TRANS, add_ls_replica, 1, "tenant_id=xxx,ls_id=xxx[,replica_type=xxx,server=xxx,data_source=xxx,orig_paxos_replica_number=xxx,new_paxos_replica_number=xxx] # add_ls_replica")
{
  int ret = OB_SUCCESS;
  string arg_str;
  ObAdminCommandArg arg;
  const ObAdminDRTaskType task_type(ObAdminDRTaskType::ADD_REPLICA);
  if (cmd_ == action_name_) {
    ret = OB_INVALID_ARGUMENT;
    ADMIN_WARN("should provide tenant_id, ls_id at least");
  } else {
    arg_str = cmd_.substr(action_name_.length() + 1);
  }

  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(client_)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid client", K(ret));
  } else if (OB_FAIL(arg.init(arg_str.c_str(), task_type))) {
    COMMON_LOG(WARN, "fail to construct admin command arg", K(ret), K(arg_str.c_str()), K(task_type));
  } else if (OB_FAIL(client_->ob_exec_drtask_obadmin_command(arg))) {
    COMMON_LOG(ERROR, "send req fail", K(ret), K(arg));
  }
  COMMON_LOG(INFO, "add_ls_replica", K(arg));
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

DEF_COMMAND(SERVER, force_set_sys_tenant_log_disk, 1, "log_disk_size=xx# set sys log disk")
{
  string arg_str;
  int ret = OB_SUCCESS;
  if (cmd_ == action_name_) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument, should provide new log disk size");
  } else {
    arg_str = cmd_.substr(action_name_.length() + 1);
    const char *arg_cstr = arg_str.c_str();
    int64_t log_disk_size = 0;
    uint64_t tenant_id = OB_SYS_TENANT_ID;
    if (1 != sscanf(arg_str.c_str(), "log_disk_size=%ld", &log_disk_size)) {
      ret = OB_INVALID_ARGUMENT;
      CLOG_LOG(WARN, "invalid argument", K(arg_cstr));
    } else {
      ObForceSetTenantLogDiskArg arg;
      arg.set(tenant_id, log_disk_size);
      if (OB_FAIL(client_->force_set_tenant_log_disk(arg))) {
        CLOG_LOG(WARN, "force_set_tenant_log_disk failed", K(arg_cstr));
      }
    }
  }
  return ret;
}

DEF_COMMAND(SERVER, dump_server_usage, 1, "output: [server_info]\n\
                                                   log_disk_assigned=xx\n\
                                                   log_disk_capacity=xx\n\
                                                   [unit_info]\n\
                                                   tenant_id=xx\n\
                                                   log_disk_in_use=xx\n\
                                                   log_disk_size=xx")
{
  string arg_str;
  int ret = OB_SUCCESS;
  if (cmd_ != action_name_) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument, no need provide argument");
  } else {
    ObDumpServerUsageRequest arg;
    ObDumpServerUsageResult result;
    if (OB_FAIL(client_->dump_server_usage(arg, result))) {
      CLOG_LOG(WARN, "dump_tenant_log_disk failed");
    } else {
      ObDumpServerUsageResult::ObServerInfo &server_info = result.server_info_;
      ObSArray<ObDumpServerUsageResult::ObUnitInfo> &unit_info = result.unit_info_;
      fprintf(stdout, "[server info]\n");
      fprintf(stdout, "log_disk_assigned=%ld\n", server_info.log_disk_assigned_);
      fprintf(stdout, "log_disk_capacity=%ld\n", server_info.log_disk_capacity_);
      for (int i = 0; i < unit_info.count(); i++) {
        ObDumpServerUsageResult::ObUnitInfo &info = unit_info[i];
        fprintf(stdout, "[unit info]\n");
        fprintf(stdout, "tenant_id=%ld\n", info.tenant_id_);
        fprintf(stdout, "log_disk_assigned=%ld\n", info.log_disk_in_use_);
        fprintf(stdout, "log_disk_capacity=%ld\n", info.log_disk_size_);
      }
    }
  }
  return ret;
}

#ifdef OB_BUILD_SHARED_STORAGE
DEF_COMMAND(SERVER, dump_ss_macro_block, 1,  "tenant_id:ver:mode:obj_type:incar_id:cg_id:second_id:third_id:fourth_id #dump ss_macro_block")
{
  int ret = OB_SUCCESS;
  string arg_str;
  obrpc::ObGetSSMacroBlockArg arg;
  obrpc::ObGetSSMacroBlockResult result;
  if (cmd_ == action_name_) {
    ret = OB_INVALID_ARGUMENT;
    ADMIN_WARN("should provide tenant_id, macro_id");
  } else {
    arg_str = cmd_.substr(action_name_.length() + 1);
  }

  char macro_id_str[1024] = {0};
  if (OB_FAIL(ret)) {
  } else if (2 != sscanf(arg_str.c_str(), "%ld:%s", &arg.tenant_id_, macro_id_str)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid arg", K(ret), K(arg_str.c_str()));
  } else {
    int64_t version = 0;
    int64_t mode = 0;
    int64_t obj_type = 0;
    int64_t incar_id = 0;
    int64_t cg_id = 0;
    int64_t second_id = 0;
    int64_t third_id = 0;
    int64_t fourth_id = 0;
    if (0 == strncmp(macro_id_str, "macro_id", 8)) {
      if (8 != sscanf(macro_id_str, "macro_id%*[=:]{[ver=%ld,mode=%ld,obj_type=%ld,obj_type_str=%*[^,],incar_id=%ld,cg_id=%ld]"
                   "[2nd=%ld][3rd=%ld][4th=%ld]}",
                   &version, &mode, &obj_type, &incar_id, &cg_id, &second_id, &third_id, &fourth_id)) {
        ret = OB_INVALID_ARGUMENT;
        COMMON_LOG(WARN, "invalid arg", KR(ret), K(macro_id_str));
      }
    } else {
      if (8 != sscanf(macro_id_str, "%ld:%ld:%ld:%ld:%ld:%ld:%ld:%ld", &version, &mode,
                   &obj_type, &incar_id, &cg_id, &second_id, &third_id, &fourth_id)) {
        ret = OB_INVALID_ARGUMENT;
        COMMON_LOG(WARN, "invalid arg", KR(ret), K(macro_id_str));
      }
    }

    if (OB_SUCC(ret)) {
      blocksstable::MacroBlockId macro_id;
      macro_id.set_ss_version(version);
      macro_id.set_ss_id_mode(mode);
      macro_id.set_storage_object_type(obj_type);
      macro_id.set_incarnation_id(incar_id);
      macro_id.set_column_group_id(cg_id);
      macro_id.set_second_id(second_id);
      macro_id.set_third_id(third_id);
      macro_id.set_fourth_id(fourth_id);
      arg.size_ = DEFAULT_MACRO_BLOCK_SIZE;
      arg.offset_ = 0;
      if (FALSE_IT(arg.macro_id_ = macro_id)) {
      } else if (OB_FAIL(client_->get_ss_macro_block(arg, result))) {
        COMMON_LOG(ERROR, "send req fail", KR(ret), K(arg), K(result));
      } else {
        int64_t pos = 0;
        char *macro_buf = result.macro_buf_.ptr();
        int64_t buf_size = result.macro_buf_.length();
        ObMacroBlockCommonHeader common_header;
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(common_header.deserialize(macro_buf, buf_size, pos))) {
          COMMON_LOG(ERROR, "deserialize common header fail", KR(ret), K(pos), K(buf_size));
        } else if (OB_FAIL(common_header.check_integrity())) {
          COMMON_LOG(ERROR, "invalid common header", KR(ret), K(common_header));
        } else if (ObMacroBlockCommonHeader::SharedSSTableData == common_header.get_type()) {
          if (OB_FAIL(ObAdminCommonUtils::dump_shared_macro_block(ObDumpMacroBlockContext(), macro_buf, buf_size))) {
            COMMON_LOG(ERROR, "dump shared block fail", KR(ret), K(buf_size));
          }
        } else {
          if (OB_FAIL(ObAdminCommonUtils::dump_single_macro_block(ObDumpMacroBlockContext(), macro_buf, buf_size))) {
            COMMON_LOG(ERROR, "dump single block fail", KR(ret), K(buf_size));
          }
        }
      }
    }
  }
  if (OB_FAIL(ret)) {
    fprintf(stderr, "fail to dump_ss_macro_block, ret=%s\n", ob_error_name(ret));
  }

  COMMON_LOG(INFO, "dump ss_macro_block", K(arg));
  return ret;
}

DEF_COMMAND(SERVER, get_ss_phy_block_info, 1, "tenant_id:phy_block_idx")
{
  int ret = OB_SUCCESS;
  string arg_str;
  ObGetSSPhyBlockInfoArg arg;
  ObGetSSPhyBlockInfoResult result;
  if (cmd_ == action_name_) {
    ret = OB_INVALID_ARGUMENT;
    ADMIN_WARN("should provide tenant_id, phy_block_idx");
  } else {
    arg_str = cmd_.substr(action_name_.length() + 1);
  }

  if (OB_FAIL(ret)) {
  } else if (2 != sscanf(arg_str.c_str(), "%ld:%ld", &arg.tenant_id_, &arg.phy_block_idx_)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid arg", K(ret), K(arg_str.c_str()));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "argument is invalid", K(ret), K(arg));
  } else if (OB_FAIL(client_->get_ss_phy_block_info(arg, result))) {
    COMMON_LOG(ERROR, "send req fail", K(ret));
  } else {
    ObCStringHelper helper;
    ObSSPhysicalBlock &ss_phy_block_info = result.ss_phy_block_info_;
    fprintf(stdout, "ret=%s\n", ob_error_name(result.ret_));
    fprintf(stdout, "phy_block_id=%ld\n", arg.phy_block_idx_);
    fprintf(stdout, "%s\n", helper.convert(ss_phy_block_info));
  }

  if (OB_FAIL(ret)) {
    fprintf(stderr, "fail to get_ss_phy_block_info, ret=%s\n", ob_error_name(ret));
  }

  COMMON_LOG(INFO, "get ss_phy_block_info", K(arg));
  return ret;
}

DEF_COMMAND(SERVER, get_ss_micro_block_meta, 1, "tenant_id:micro_key_mode:micro_id")
{
  int ret = OB_SUCCESS;
  string arg_str;
  ObGetSSMicroBlockMetaArg arg;
  ObGetSSMicroBlockMetaResult result;
  ObSSMicroBlockCacheKey &micro_key = arg.micro_key_;
  if (cmd_ == action_name_) {
    ret = OB_INVALID_ARGUMENT;
    ADMIN_WARN("should provide tenant_id, micro_key");
  } else {
    arg_str = cmd_.substr(action_name_.length() + 1);
  }

  int64_t mode = 0;
  char micro_key_str[1024] = {0};
  if (OB_FAIL(ret)) {
  } else if (3 != sscanf(arg_str.c_str(), "%ld:%ld:%s", &arg.tenant_id_, &mode, micro_key_str)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid arg", K(ret), K(arg_str.c_str()));
  } else {
    micro_key.mode_ = static_cast<ObSSMicroBlockCacheKeyMode>(mode);
    if (micro_key.is_logic_key()) {
      int64_t version = 0;
      int64_t offset = 0;
      int64_t macro_data_seq = 0;
      int64_t logic_version = 0;
      int64_t tablet_id = 0;
      int64_t column_group_idx = 0;
      int64_t micro_crc = 0;
      if (7 != sscanf(micro_key_str,
                   "%ld:%ld:%ld:%ld:%ld:%ld:%ld",
                   &version, &offset, &macro_data_seq, &logic_version,
                   &tablet_id, &column_group_idx, &micro_crc)) {
        ret = OB_INVALID_ARGUMENT;
        COMMON_LOG(WARN, "invalid micro_key_str", K(ret), K(arg_str.c_str()), K(micro_key_str));
      } else {
        ObLogicMicroBlockId logic_micro_id;
        ObLogicMacroBlockId &logic_macro_id = logic_micro_id.logic_macro_id_;
        logic_micro_id.version_ = version;
        logic_micro_id.offset_ = offset;
        logic_macro_id.data_seq_.macro_data_seq_ = macro_data_seq;
        logic_macro_id.logic_version_ = logic_version;
        logic_macro_id.tablet_id_ = tablet_id;
        logic_macro_id.column_group_idx_ = column_group_idx;
        micro_key.logic_micro_id_ = logic_micro_id;
        micro_key.micro_crc_ = micro_crc;
        COMMON_LOG(INFO, "init micro key", K(micro_key), K(micro_key_str));
      }
    } else {
      int64_t ver = 0;
      int64_t mode = 0;
      int64_t obj_type = 0;
      int64_t incar_id = 0;
      int64_t cg_id = 0;
      int64_t second_id = 0;
      int64_t third_id = 0;
      int64_t fourth_id = 0;
      int64_t offset = 0;
      int64_t size = 0;
      int64_t micro_crc = 0;
      if (11 != sscanf(micro_key_str,
                    "%ld:%ld:%ld:%ld:%ld:%ld:%ld:%ld:%ld:%ld:%ld",
                    &ver, &mode, &obj_type, &incar_id, &cg_id, &second_id,
                    &third_id, &fourth_id, &offset, &size, &micro_crc)) {
        ret = OB_INVALID_ARGUMENT;
        COMMON_LOG(WARN, "invalid micro_key_str", K(ret), K(arg_str.c_str()), K(micro_key_str));
      } else {
        ObSSMicroBlockId micro_id;
        MacroBlockId &macro_id = micro_id.macro_id_;
        macro_id.set_ss_version(ver);
        macro_id.set_ss_id_mode(mode);
        macro_id.set_storage_object_type(obj_type);
        macro_id.set_incarnation_id(incar_id);
        macro_id.set_column_group_id(cg_id);
        macro_id.set_second_id(second_id);
        macro_id.set_third_id(third_id);
        macro_id.set_fourth_id(fourth_id);
        micro_id.offset_ = offset;
        micro_id.size_ = size;
        micro_key.micro_id_ = micro_id;
        micro_key.micro_crc_ = micro_crc;
        COMMON_LOG(INFO, "init micro key", K(micro_key), K(micro_key_str));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (!arg.is_valid()) {
      ret = OB_INVALID_ARGUMENT;
      COMMON_LOG(WARN, "argument is invalid", K(ret), K(arg));
    } else if (OB_FAIL(client_->get_ss_micro_block_meta(arg, result))) {
      COMMON_LOG(ERROR, "send req fail", K(ret));
    } else {
      ObCStringHelper helper;
      fprintf(stdout, "ret=%s\n", ob_error_name(result.ret_));
      if (OB_SUCC(result.ret_)) {
        fprintf(stdout, "micro_key=%s\n", helper.convert(arg.micro_key_));
        fprintf(stdout, "micro_meta=%s\n", helper.convert(result.micro_meta_info_));
      }
    }
  }

  if (OB_FAIL(ret)) {
    fprintf(stderr, "fail to get_ss_micro_block_meta, ret=%s\n", ob_error_name(ret));
  }

  COMMON_LOG(INFO, "get ss_micro_block_meta", K(arg));
  return ret;
}

DEF_COMMAND(SERVER, dump_ss_macro_block_by_uri, 1, "tenant_id:uri")
{
  int ret = OB_SUCCESS;
  string arg_str;
  obrpc::ObGetSSMacroBlockByURIArg arg;
  obrpc::ObGetSSMacroBlockByURIResult result;
  if (cmd_ == action_name_) {
    ret = OB_INVALID_ARGUMENT;
    ADMIN_WARN("should provide tenant_id, uri");
  } else {
    arg_str = cmd_.substr(action_name_.length() + 1);
  }

  arg.size_ = DEFAULT_MACRO_BLOCK_SIZE;
  arg.offset_ = 0;
  if (OB_FAIL(ret)) {
  } else if (2 != sscanf(arg_str.c_str(), "%ld:%s", &arg.tenant_id_, arg.uri_)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid arg", K(ret), K_(arg.uri));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "argument is invalid", K(ret), K(arg));
  } else if (OB_FAIL(client_->get_ss_macro_block_by_uri(arg, result))) {
    COMMON_LOG(ERROR, "send req fail", K(ret));
  } else {
    int64_t pos = 0;
    char *macro_buf = result.macro_buf_.ptr();
    int64_t buf_size = result.macro_buf_.length();
    ObMacroBlockCommonHeader common_header;
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(common_header.deserialize(macro_buf, buf_size, pos))) {
      COMMON_LOG(ERROR, "deserialize common header fail", KR(ret), K(pos), K(buf_size));
    } else if (OB_FAIL(common_header.check_integrity())) {
      COMMON_LOG(ERROR, "invalid common header", KR(ret), K(common_header));
    } else if (ObMacroBlockCommonHeader::SharedSSTableData == common_header.get_type()) {
      if (OB_FAIL(ObAdminCommonUtils::dump_shared_macro_block(ObDumpMacroBlockContext(), macro_buf, buf_size))) {
        COMMON_LOG(ERROR, "dump shared block fail", KR(ret), K(buf_size));
      }
    } else {
      if (OB_FAIL(ObAdminCommonUtils::dump_single_macro_block(ObDumpMacroBlockContext(), macro_buf, buf_size))) {
        COMMON_LOG(ERROR, "dump single block fail", KR(ret), K(buf_size));
      }
    }
  }
  if (OB_FAIL(ret)) {
    fprintf(stderr, "fail to dump_ss_macro_block_by_uri, ret=%s\n", ob_error_name(ret));
  }

  COMMON_LOG(INFO, "dump ss_macro_block by uri", K(arg));
  return ret;
}

DEF_COMMAND(SERVER, del_ss_tablet_meta, 1, "tenant_id:tablet_id:compaction_scn")
{
  int ret = OB_SUCCESS;
  string arg_str;
  ObDelSSTabletMetaArg arg;
  if (cmd_ == action_name_) {
    ret = OB_INVALID_ARGUMENT;
    ADMIN_WARN("should provide tenant_id, tablet_id, compaction_scn");
  } else {
    arg_str = cmd_.substr(action_name_.length() + 1);
  }

  int64_t tablet_id = 0;
  int64_t compaction_scn = 0;
  if (OB_FAIL(ret)) {
  } else if (3 != sscanf(arg_str.c_str(), "%ld:%ld:%ld", &arg.tenant_id_, &tablet_id, &compaction_scn)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid arg", K(ret), K(arg_str.c_str()));
  } else {
    MacroBlockId macro_id;
    macro_id.set_ss_version(MacroBlockId::MACRO_BLOCK_ID_VERSION_V2);
    macro_id.set_ss_id_mode(static_cast<uint64_t>(ObMacroBlockIdMode::ID_MODE_SHARE));
    macro_id.set_storage_object_type(static_cast<uint64_t>(ObStorageObjectType::SHARED_MAJOR_TABLET_META));
    macro_id.set_second_id(tablet_id);
    macro_id.set_third_id(compaction_scn);
    arg.macro_id_ = macro_id;
    if (!arg.is_valid()) {
      ret = OB_INVALID_ARGUMENT;
      COMMON_LOG(WARN, "argument is invalid", K(ret), K(arg));
    } else if (OB_FAIL(client_->del_ss_tablet_meta(arg))) {
      COMMON_LOG(ERROR, "send req fail", K(ret));
    } else {
      fprintf(stdout,
          "Successfully delete ss_tablet_meta[tenant_id:%ld, tablet_id:%ld, compaction_scn:%ld]",
          arg.tenant_id_, tablet_id, compaction_scn);
    }
  }
  if (OB_FAIL(ret)) {
    fprintf(stderr, "fail to del_ss_tablet_meta, ret=%s\n", ob_error_name(ret));
  }
  COMMON_LOG(INFO, "del ss_tablet_meta", K(arg));
  return ret;
}

DEF_COMMAND(SERVER, del_ss_local_tmpfile, 1, "tenant_id:tmpfile_id")
{
  int ret = OB_SUCCESS;
  string arg_str;
  ObDelSSLocalTmpFileArg arg;
  if (cmd_ == action_name_) {
    ret = OB_INVALID_ARGUMENT;
    ADMIN_WARN("should provide tenant_id, tmpfile_id");
  } else {
    arg_str = cmd_.substr(action_name_.length() + 1);
  }

  int64_t tmpfile_id = 0;
  if (OB_FAIL(ret)) {
  } else if (2 != sscanf(arg_str.c_str(), "%ld:%ld", &arg.tenant_id_, &tmpfile_id)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid arg", K(ret), K(arg_str.c_str()));
  } else {
    MacroBlockId macro_id;
    macro_id.set_ss_version(MacroBlockId::MACRO_BLOCK_ID_VERSION_V2);
    macro_id.set_ss_id_mode(static_cast<uint64_t>(ObMacroBlockIdMode::ID_MODE_SHARE));
    macro_id.set_storage_object_type(static_cast<uint64_t>(ObStorageObjectType::TMP_FILE));
    macro_id.set_second_id(tmpfile_id);
    arg.macro_id_ = macro_id;
    const int64_t rpc_timeout = 60000000;
    if (!arg.is_valid()) {
      ret = OB_INVALID_ARGUMENT;
      COMMON_LOG(WARN, "argument is invalid", K(ret), K(arg));
    } else if (OB_FALSE_IT(client_->set_timeout(rpc_timeout))) {
    } else if (OB_FAIL(client_->del_ss_local_tmpfile(arg))) {
      COMMON_LOG(ERROR, "send req fail", K(ret));
    } else {
      fprintf(
          stdout, "Successfully del_ss_local_tmpfile [tenant_id:%ld, tmpfile_id:%ld]", arg.tenant_id_, tmpfile_id);
    }
  }
  if (OB_FAIL(ret)) {
    fprintf(stderr, "fail to del_ss_local_tmpfile, ret=%s\n", ob_error_name(ret));
  }
  COMMON_LOG(INFO, "del_ss_local_tmpfile", K(arg));
  return ret;
}

DEF_COMMAND(SERVER, del_ss_local_major, 1, "tenant_id")
{
  int ret = OB_SUCCESS;
  string arg_str;
  ObDelSSLocalMajorArg arg;
  if (cmd_ == action_name_) {
    ret = OB_INVALID_ARGUMENT;
    ADMIN_WARN("should provide tenant_id");
  } else {
    arg_str = cmd_.substr(action_name_.length() + 1);
  }

  const int64_t rpc_timeout = 60000000;
  if (OB_FAIL(ret)) {
  } else if (1 != sscanf(arg_str.c_str(), "%ld", &arg.tenant_id_)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid arg", K(ret), K(arg_str.c_str()));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "argument is invalid", K(ret), K(arg));
  } else if (OB_FALSE_IT(client_->set_timeout(rpc_timeout))) {
  } else if (OB_FAIL(client_->del_ss_local_major(arg))) {
    COMMON_LOG(ERROR, "send req fail", K(ret));
  } else {
    fprintf(stdout, "Successfully del_ss_local_major [tenant_id:%ld]", arg.tenant_id_);
  }

  if (OB_FAIL(ret)) {
    fprintf(stderr, "fail to del_ss_local_major, ret=%s\n", ob_error_name(ret));
  }
  COMMON_LOG(INFO, "del_ss_local_major", K(arg));
  return ret;
}

DEF_COMMAND(SERVER, calibrate_ss_disk_space, 1, "tenant_id")
{
  int ret = OB_SUCCESS;
  string arg_str;
  ObCalibrateSSDiskSpaceArg arg;
  if (cmd_ == action_name_) {
    ret = OB_INVALID_ARGUMENT;
    ADMIN_WARN("should provide tenant_id");
  } else {
    arg_str = cmd_.substr(action_name_.length() + 1);
  }

  const int64_t rpc_timeout = 60000000;
  if (OB_FAIL(ret)) {
  } else if (1 != sscanf(arg_str.c_str(), "%ld", &arg.tenant_id_)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid arg", K(ret), K(arg_str.c_str()));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "argument is invalid", K(ret), K(arg));
  } else if (OB_FALSE_IT(client_->set_timeout(rpc_timeout))) {
  } else if (OB_FAIL(client_->calibrate_ss_disk_space(arg))) {
    COMMON_LOG(ERROR, "send req fail", K(ret));
  } else {
    fprintf(stdout, "Successfully calibrate_ss_disk_space [tenant_id:%ld]", arg.tenant_id_);
  }

  if (OB_FAIL(ret)) {
    fprintf(stderr, "fail to calibrate_ss_disk_space, ret=%s\n", ob_error_name(ret));
  }
  COMMON_LOG(INFO, "calibrate_ss_disk_space", K(arg));
  return ret;
}

DEF_COMMAND(SERVER, del_ss_tablet_micro, 1, "tenant_id:tablet_id")
{
  int ret = OB_SUCCESS;
  string arg_str;
  ObDelSSTabletMicroArg arg;
  if (cmd_ == action_name_) {
    ret = OB_INVALID_ARGUMENT;
    ADMIN_WARN("should provide tenant_id, tablet_id");
  } else {
    arg_str = cmd_.substr(action_name_.length() + 1);
  }

  int64_t tablet_id = 0;
  const int64_t rpc_timeout = 1800000000; // 3min
  if (OB_FAIL(ret)) {
  } else if (2 != sscanf(arg_str.c_str(), "%ld:%ld", &arg.tenant_id_, &tablet_id)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid arg", K(ret), K(arg_str.c_str()));
  } else {
    arg.tablet_id_ = ObTabletID(tablet_id);
    if (!arg.is_valid()) {
      ret = OB_INVALID_ARGUMENT;
      COMMON_LOG(WARN, "argument is invalid", K(ret), K(arg));
    } else if (OB_FALSE_IT(client_->set_timeout(rpc_timeout))) {
    } else if (OB_FAIL(client_->del_ss_tablet_micro(arg))) {
      COMMON_LOG(ERROR, "send req fail", K(ret));
    } else {
      fprintf(stdout, "Successfully del_ss_tablet_micro [tenant_id:%ld, tablet_id:%ld]", arg.tenant_id_, arg.tablet_id_.id());
    }
  }

  if (OB_FAIL(ret)) {
    fprintf(stderr, "fail to del_ss_tablet_micro, ret=%s\n", ob_error_name(ret));
  }
  COMMON_LOG(INFO, "del_ss_tablet_micro", K(arg));
  return ret;
}

DEF_COMMAND(SERVER, download_ss_macro_block, 1,  "tenant_id:ver:mode:obj_type:incar_id:cg_id:second_id:third_id:fourth_id #download ss_macro_block")
{
  int ret = OB_SUCCESS;
  string arg_str;
  obrpc::ObGetSSMacroBlockArg arg;
  if (cmd_ == action_name_) {
    ret = OB_INVALID_ARGUMENT;
    ADMIN_WARN("should provide tenant_id, macro_id");
  } else {
    arg_str = cmd_.substr(action_name_.length() + 1);
  }

  char macro_id_str[1024] = {0};
  if (OB_FAIL(ret)) {
  } else if (2 != sscanf(arg_str.c_str(), "%ld:%s", &arg.tenant_id_, macro_id_str)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid arg", K(ret), K(arg_str.c_str()));
  } else {
    int64_t version = 0;
    int64_t mode = 0;
    int64_t obj_type = 0;
    int64_t incar_id = 0;
    int64_t cg_id = 0;
    int64_t second_id = 0;
    int64_t third_id = 0;
    int64_t fourth_id = 0;
    if (0 == strncmp(macro_id_str, "macro_id", 8)) {
      if (8 != sscanf(macro_id_str, "macro_id%*[=:]{[ver=%ld,mode=%ld,obj_type=%ld,obj_type_str=%*[^,],incar_id=%ld,cg_id=%ld]"
                   "[2nd=%ld][3rd=%ld][4th=%ld]}",
                   &version, &mode, &obj_type, &incar_id, &cg_id, &second_id, &third_id, &fourth_id)) {
        ret = OB_INVALID_ARGUMENT;
        COMMON_LOG(WARN, "invalid arg", KR(ret), K(macro_id_str));
      }
    } else {
      if (8 != sscanf(macro_id_str, "%ld:%ld:%ld:%ld:%ld:%ld:%ld:%ld", &version, &mode,
                   &obj_type, &incar_id, &cg_id, &second_id, &third_id, &fourth_id)) {
        ret = OB_INVALID_ARGUMENT;
        COMMON_LOG(WARN, "invalid arg", KR(ret), K(macro_id_str));
      }
    }

    if (OB_SUCC(ret)) {
      blocksstable::MacroBlockId macro_id;
      macro_id.set_ss_version(version);
      macro_id.set_ss_id_mode(mode);
      macro_id.set_storage_object_type(obj_type);
      macro_id.set_incarnation_id(incar_id);
      macro_id.set_column_group_id(cg_id);
      macro_id.set_second_id(second_id);
      macro_id.set_third_id(third_id);
      macro_id.set_fourth_id(fourth_id);
      arg.macro_id_ = macro_id;
      ObIOFd fd;
      fd.second_id_ = fileno(stdout);
      int64_t cur_offset = 0;
      char *macro_buf = nullptr;
      int64_t buf_size = 0;
      do {
        int64_t write_size = 0;
        arg.offset_ = cur_offset;
        arg.size_ = DEFAULT_MACRO_BLOCK_SIZE;
        obrpc::ObGetSSMacroBlockResult result;
        if (OB_FAIL(client_->get_ss_macro_block(arg, result))) {
          COMMON_LOG(ERROR, "send req fail", KR(ret), K(arg), K(result));
        } else if (OB_FALSE_IT(macro_buf = result.macro_buf_.ptr())) {
        } else if (OB_FALSE_IT(buf_size = result.macro_buf_.length())) {
        } else {
          if (buf_size > 0 && OB_NOT_NULL(macro_buf)) {
            if (OB_FAIL(ObIODeviceLocalFileOp::write(fd, macro_buf, buf_size, write_size))) {
              COMMON_LOG(WARN, "fail to write", KR(ret), K(fd), KP(macro_buf), K(buf_size), K(write_size));
            } else if (OB_UNLIKELY(write_size != buf_size)) {
              ret = OB_ERR_UNEXPECTED;
              COMMON_LOG(WARN, "write_size is wrong", KR(ret), K(write_size), K(buf_size));
            } else {
              cur_offset += buf_size;
            }
          } else if (buf_size == 0 && OB_ISNULL(macro_buf)) {
          } else {
            ret = OB_ERR_UNEXPECTED;
            COMMON_LOG(WARN, "unexpected error", KR(ret), K(buf_size), KP(macro_buf));
          }
        }
      } while (OB_SUCC(ret) && (buf_size == DEFAULT_MACRO_BLOCK_SIZE));
    }
  }
  if (OB_FAIL(ret)) {
    fprintf(stderr, "fail to download_ss_macro_block, ret=%s\n", ob_error_name(ret));
  }
  COMMON_LOG(INFO, "download ss_macro_block", K(arg));
  return ret;
}

DEF_COMMAND(SERVER, download_ss_macro_block_by_uri, 1, "tenant_id:uri")
{
  int ret = OB_SUCCESS;
  string arg_str;
  obrpc::ObGetSSMacroBlockByURIArg arg;
  if (cmd_ == action_name_) {
    ret = OB_INVALID_ARGUMENT;
    ADMIN_WARN("should provide tenant_id, uri");
  } else {
    arg_str = cmd_.substr(action_name_.length() + 1);
  }

  if (OB_FAIL(ret)) {
  } else if (2 != sscanf(arg_str.c_str(), "%ld:%s", &arg.tenant_id_, arg.uri_)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid arg", K(ret), K_(arg.uri));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "argument is invalid", K(ret), K(arg));
  } else {
    ObIOFd fd;
    fd.second_id_ = fileno(stdout);
    int64_t cur_offset = 0;
    char *macro_buf = nullptr;
    int64_t buf_size = 0;
    do {
      int64_t write_size = 0;
      arg.offset_ = cur_offset;
      arg.size_ = DEFAULT_MACRO_BLOCK_SIZE;
      obrpc::ObGetSSMacroBlockByURIResult result;
      if (OB_FAIL(client_->get_ss_macro_block_by_uri(arg, result))) {
        COMMON_LOG(ERROR, "send req fail", KR(ret), K(arg), K(result));
      } else if (OB_FALSE_IT(macro_buf = result.macro_buf_.ptr())) {
      } else if (OB_FALSE_IT(buf_size = result.macro_buf_.length())) {
      } else {
        if (buf_size > 0 && OB_NOT_NULL(macro_buf)) {
          if (OB_FAIL(ObIODeviceLocalFileOp::write(fd, macro_buf, buf_size, write_size))) {
            COMMON_LOG(WARN, "fail to write", KR(ret), K(fd), KP(macro_buf), K(buf_size), K(write_size));
          } else if (OB_UNLIKELY(write_size != buf_size)) {
            ret = OB_ERR_UNEXPECTED;
            COMMON_LOG(WARN, "write_size is wrong", KR(ret), K(write_size), K(buf_size));
          } else {
            cur_offset += buf_size;
          }
        } else if (buf_size == 0 && OB_ISNULL(macro_buf)) {
        } else {
          ret = OB_ERR_UNEXPECTED;
          COMMON_LOG(WARN, "unexpected error", KR(ret), K(buf_size), KP(macro_buf));
        }
      }
    } while (OB_SUCC(ret) && (buf_size == DEFAULT_MACRO_BLOCK_SIZE));
  }
  if (OB_FAIL(ret)) {
    fprintf(stderr, "fail to download_ss_macro_block_by_uri, ret=%s\n", ob_error_name(ret));
  }

  COMMON_LOG(INFO, "download ss_macro_block by uri", K(arg));
  return ret;
}

DEF_COMMAND(SERVER, enable_ss_micro_cache, 1, "tenant_id:is_enabled")
{
  int ret = OB_SUCCESS;
  string arg_str;
  ObEnableSSMicroCacheArg arg;
  if (cmd_ == action_name_) {
    ret = OB_INVALID_ARGUMENT;
    ADMIN_WARN("should provide tenant_id");
  } else {
    arg_str = cmd_.substr(action_name_.length() + 1);
  }

  char is_enabled_str[64] = {0};
  if (OB_FAIL(ret)) {
  } else if (2 != sscanf(arg_str.c_str(), "%ld:%s", &arg.tenant_id_, is_enabled_str)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid arg", K(ret), K(arg_str.c_str()));
  } else {
    if (0 == strncmp(is_enabled_str, "true", 4)) {
      arg.is_enabled_ = true;
    } else if (0 == strncmp(is_enabled_str, "false", 5)) {
      arg.is_enabled_ = false;
    } else {
      ret = OB_INVALID_ARGUMENT;
      COMMON_LOG(WARN, "argument is invalid", K(ret), K(is_enabled_str));
    }

    if (OB_FAIL(ret)) {
    } else if (OB_UNLIKELY(!arg.is_valid())) {
      ret = OB_INVALID_ARGUMENT;
      COMMON_LOG(WARN, "invalid arg", K(ret), K(arg));
    } else if (OB_FAIL(client_->enable_ss_micro_cache(arg))) {
      COMMON_LOG(ERROR, "send req fail", K(ret));
    }
  }

  if (OB_FAIL(ret)) {
    fprintf(stderr, "fail to enable ss_micro_cache, ret=%s\n", ob_error_name(ret));
  }

  COMMON_LOG(INFO, "enable ss_micro_cache", K(arg));
  return ret;
}

DEF_COMMAND(SERVER, get_ss_micro_cache_info, 1, "tenant_id")
{
  int ret = OB_SUCCESS;
  string arg_str;
  ObGetSSMicroCacheInfoArg arg;
  ObGetSSMicroCacheInfoResult result;
  if (cmd_ == action_name_) {
    ret = OB_INVALID_ARGUMENT;
    ADMIN_WARN("should provide tenant_id");
  } else {
    arg_str = cmd_.substr(action_name_.length() + 1);
  }

  if (OB_FAIL(ret)) {
  } else if (1 != sscanf(arg_str.c_str(), "%ld", &arg.tenant_id_)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid arg", K(ret), K(arg_str.c_str()));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "argument is invalid", K(ret), K(arg));
  } else if (OB_FAIL(client_->get_ss_micro_cache_info(arg, result))) {
    COMMON_LOG(ERROR, "send req fail", K(ret));
  } else {
    ObCStringHelper helper;
    fprintf(stdout, "micro_cache_stat=%s\n", helper.convert(result.micro_cache_stat_));
    fprintf(stdout, "super_block=%s\n", helper.convert(result.super_block_));
    fprintf(stdout, "arc_info=%s\n", helper.convert(result.arc_info_));
  }

  if (OB_FAIL(ret)) {
    fprintf(stderr, "fail to get ss_micro_cache_info, ret=%s\n", ob_error_name(ret));
  }

  COMMON_LOG(INFO, "get ss_micro_cache_info", K(arg));
  return ret;
}

DEF_COMMAND(SERVER, set_ss_ckpt_compressor, 1, "tenant_id:ckpt_type:compressor_name")
{
  int ret = OB_SUCCESS;
  string arg_str;
  ObSetSSCkptCompressorArg arg;
  if (cmd_ == action_name_) {
    ret = OB_INVALID_ARGUMENT;
    ADMIN_WARN("should provide tenant_id");
  } else {
    arg_str = cmd_.substr(action_name_.length() + 1);
  }

  char ckpt_type_name[64] = {0};
  char compressor_name[64] = {0};
  if (OB_FAIL(ret)) {
  } else if (3 != sscanf(arg_str.c_str(), "%ld:%[^:]:%s", &arg.tenant_id_, ckpt_type_name, compressor_name)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid arg", K(ret), K(arg_str.c_str()));
  } else {
    if (0 == strncmp(ckpt_type_name, "micro", 5)) {
      arg.block_type_ = ObSSPhyBlockType::SS_MICRO_META_CKPT_BLK;
    } else if (0 == strncmp(ckpt_type_name, "blk", 3)) {
      arg.block_type_ = ObSSPhyBlockType::SS_PHY_BLOCK_CKPT_BLK;
    } else {
      ret = OB_INVALID_ARGUMENT;
      COMMON_LOG(WARN, "ckpt_type is invalid", K(ret), K(ckpt_type_name));
    }

    if (FAILEDx(ObCompressorPool::get_instance().get_compressor_type(compressor_name, arg.compressor_type_))) {
      COMMON_LOG(WARN, "fail to get compressor_type", K(ret), K(compressor_name));
    } else if (!arg.is_valid()) {
      ret = OB_INVALID_ARGUMENT;
      COMMON_LOG(WARN, "argument is invalid", K(ret), K(arg));
    } else if (OB_FAIL(client_->set_ss_ckpt_compressor(arg))) {
      COMMON_LOG(ERROR, "send req fail", K(ret));
    } else {
      fprintf(stdout, "Successfully set_ss_ckpt_compressor [tenant_id:%ld, ckpt_type:%s, compressor_type:%s]",
          arg.tenant_id_, ckpt_type_name, compressor_name);
    }
  }

  if (OB_FAIL(ret)) {
    fprintf(stderr, "fail to set_ss_ckpt_compressor, ret=%s\n", ob_error_name(ret));
  }
  COMMON_LOG(INFO, "set_ss_ckpt_compressor", K(arg));
  return ret;
}
#endif
