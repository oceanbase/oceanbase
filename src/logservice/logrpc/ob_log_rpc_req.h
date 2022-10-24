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

#ifndef OCEANBASE_LOGSERVICE_OB_LOG_RPC_REQ_H_
#define OCEANBASE_LOGSERVICE_OB_LOG_RPC_REQ_H_

#include "lib/utility/ob_unify_serialize.h"                    // OB_UNIS_VERSION
#include "lib/utility/ob_print_utils.h"                        // TO_STRING_KV
#include "common/ob_member_list.h"                             // ObMemberList

namespace oceanbase
{
namespace palf
{
class PalfStat;
}
namespace logservice
{

enum LogConfigChangeCmdType {
  INVALID_CONFIG_CHANGE_CMD = 0,
  CHANGE_REPLICA_NUM_CMD,
  ADD_MEMBER_CMD,
  ADD_ARB_MEMBER_CMD,
  REMOVE_MEMBER_CMD,
  REMOVE_ARB_MEMBER_CMD,
  REPLACE_MEMBER_CMD,
  REPLACE_ARB_MEMBER_CMD,
  ADD_LEARNER_CMD,
  REMOVE_LEARNER_CMD,
  SWITCH_TO_ACCEPTOR_CMD,
  SWITCH_TO_LEARNER_CMD,
};

inline const char *log_config_change_cmd2str(const LogConfigChangeCmdType state)
{
  #define CHECK_CMD_TYPE_STR(x) case(LogConfigChangeCmdType::x): return #x
  switch(state)
  {
    CHECK_CMD_TYPE_STR(ADD_MEMBER_CMD);
    CHECK_CMD_TYPE_STR(ADD_ARB_MEMBER_CMD);
    CHECK_CMD_TYPE_STR(REMOVE_MEMBER_CMD);
    CHECK_CMD_TYPE_STR(REMOVE_ARB_MEMBER_CMD);
    CHECK_CMD_TYPE_STR(REPLACE_MEMBER_CMD);
    CHECK_CMD_TYPE_STR(REPLACE_ARB_MEMBER_CMD);
    CHECK_CMD_TYPE_STR(ADD_LEARNER_CMD);
    CHECK_CMD_TYPE_STR(REMOVE_LEARNER_CMD);
    CHECK_CMD_TYPE_STR(SWITCH_TO_ACCEPTOR_CMD);
    CHECK_CMD_TYPE_STR(SWITCH_TO_LEARNER_CMD);
    CHECK_CMD_TYPE_STR(CHANGE_REPLICA_NUM_CMD);
    default:
      return "Invalid";
  }
  #undef CHECK_CMD_TYPE_STR
}

struct LogConfigChangeCmd {
  OB_UNIS_VERSION(1);
public:
  LogConfigChangeCmd();
  LogConfigChangeCmd(const common::ObAddr &src,
                     const int64_t palf_id,
                     const common::ObMember &added_member,
                     const common::ObMember &removed_member,
                     const int64_t paxos_replica_num,
                     const LogConfigChangeCmdType cmd_type,
                     const int64_t timeout_ns);
  LogConfigChangeCmd(const common::ObAddr &src,
                     const int64_t palf_id,
                     const common::ObMemberList &member_list,
                     const int64_t curr_replica_num,
                     const int64_t new_replica_num,
                     const LogConfigChangeCmdType cmd_type,
                     const int64_t timeout_ns);
  ~LogConfigChangeCmd();
  bool is_valid() const;
  void reset();
  bool is_remove_member_list() const;
  bool is_add_member_list() const;
  TO_STRING_KV("cmd_type", log_config_change_cmd2str(cmd_type_), K_(src), K_(palf_id), \
  K_(added_member), K_(removed_member), K_(curr_member_list), K_(curr_replica_num),      \
  K_(new_replica_num), K_(timeout_ns));
  common::ObAddr src_;
  int64_t palf_id_;
  common::ObMember added_member_;
  common::ObMember removed_member_;
  common::ObMemberList curr_member_list_;
  int64_t curr_replica_num_;
  int64_t new_replica_num_;
  LogConfigChangeCmdType cmd_type_;
  int64_t timeout_ns_;
};

struct LogConfigChangeCmdResp {
  OB_UNIS_VERSION(1);
public:
  LogConfigChangeCmdResp();
  LogConfigChangeCmdResp(const int ret);
  ~LogConfigChangeCmdResp();
  bool is_valid() const;
  void reset();
  TO_STRING_KV(K_(ret));
  int ret_;
};

struct LogGetPalfStatReq {
  OB_UNIS_VERSION(1);
public:
  LogGetPalfStatReq() {};
  LogGetPalfStatReq(const common::ObAddr &src,
                    const int64_t palf_id);
  ~LogGetPalfStatReq();
  bool is_valid() const;
  void reset();
  TO_STRING_KV(K_(src), K_(palf_id));
  common::ObAddr src_;
  int64_t palf_id_;
};

struct LogGetPalfStatResp {
  OB_UNIS_VERSION(1);
public:
  LogGetPalfStatResp() {};
  LogGetPalfStatResp(const int64_t max_ts_ns);
  ~LogGetPalfStatResp();
  bool is_valid() const;
  void reset();
  TO_STRING_KV(K_(max_ts_ns));
  int64_t max_ts_ns_;
};

} // end namespace logservice
}// end namespace oceanbase

#endif