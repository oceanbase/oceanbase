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
#include "lib/container/ob_array_serialization.h"
#include "common/ob_member_list.h"                             // ObMemberList
#include "logservice/palf/palf_options.h"                      // AccessMode
#include "logservice/palf/palf_handle_impl.h"                  // PalfStat
#include "share/ob_ls_id.h"                                    // ObLSID
#include "share/scn.h"

namespace oceanbase
{
namespace logservice
{

enum LogConfigChangeCmdType {
  INVALID_CONFIG_CHANGE_CMD = 0,
  CHANGE_REPLICA_NUM_CMD,
  ADD_MEMBER_CMD,
  REMOVE_MEMBER_CMD,
#ifdef OB_BUILD_ARBITRATION
  ADD_ARB_MEMBER_CMD,
  REMOVE_ARB_MEMBER_CMD,
#endif
  REPLACE_MEMBER_CMD,
  ADD_LEARNER_CMD,
  REMOVE_LEARNER_CMD,
  SWITCH_TO_ACCEPTOR_CMD,//discarded
  SWITCH_TO_LEARNER_CMD,
  FORCE_SINGLE_MEMBER_CMD,
  TRY_LOCK_CONFIG_CHANGE_CMD,
  UNLOCK_CONFIG_CHANGE_CMD,
  GET_CONFIG_CHANGE_LOCK_STAT_CMD,
  REPLACE_LEARNERS_CMD,
  REPLACE_MEMBER_WITH_LEARNER_CMD,
  FORCE_SET_MEMBER_LIST_CMD,
};

inline const char *log_config_change_cmd2str(const LogConfigChangeCmdType state)
{
  #define CHECK_CMD_TYPE_STR(x) case(LogConfigChangeCmdType::x): return #x
  switch(state)
  {
    CHECK_CMD_TYPE_STR(ADD_MEMBER_CMD);
    CHECK_CMD_TYPE_STR(REMOVE_MEMBER_CMD);
#ifdef OB_BUILD_ARBITRATION
    CHECK_CMD_TYPE_STR(ADD_ARB_MEMBER_CMD);
    CHECK_CMD_TYPE_STR(REMOVE_ARB_MEMBER_CMD);
#endif
    CHECK_CMD_TYPE_STR(REPLACE_MEMBER_CMD);
    CHECK_CMD_TYPE_STR(ADD_LEARNER_CMD);
    CHECK_CMD_TYPE_STR(REMOVE_LEARNER_CMD);
    CHECK_CMD_TYPE_STR(SWITCH_TO_ACCEPTOR_CMD);
    CHECK_CMD_TYPE_STR(SWITCH_TO_LEARNER_CMD);
    CHECK_CMD_TYPE_STR(CHANGE_REPLICA_NUM_CMD);
    CHECK_CMD_TYPE_STR(TRY_LOCK_CONFIG_CHANGE_CMD);
    CHECK_CMD_TYPE_STR(UNLOCK_CONFIG_CHANGE_CMD);
    CHECK_CMD_TYPE_STR(GET_CONFIG_CHANGE_LOCK_STAT_CMD);
    CHECK_CMD_TYPE_STR(REPLACE_LEARNERS_CMD);
    CHECK_CMD_TYPE_STR(REPLACE_MEMBER_WITH_LEARNER_CMD);
    CHECK_CMD_TYPE_STR(FORCE_SET_MEMBER_LIST_CMD);
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
                     const int64_t timeout_us);
  LogConfigChangeCmd(const common::ObAddr &src,
                     const int64_t palf_id,
                     const common::ObMemberList &member_list,
                     const int64_t curr_replica_num,
                     const int64_t new_replica_num,
                     const LogConfigChangeCmdType cmd_type,
                     const int64_t timeout_us);
  LogConfigChangeCmd(const common::ObAddr &src,
                     const int64_t palf_id,
                     const int64_t lock_owner,
                     const LogConfigChangeCmdType cmd_type,
                     const int64_t timeout_us);
  LogConfigChangeCmd(const common::ObAddr &src,
                     const int64_t palf_id,
                     const common::ObMemberList &added_list,
                     const common::ObMemberList &removed_list,
                     const LogConfigChangeCmdType cmd_type,
                     const int64_t timeout_us);
  LogConfigChangeCmd(const common::ObAddr &src,
                     const int64_t palf_id,
                     const common::ObMemberList &new_member_list,
                     const int64_t new_replica_num,
                     const LogConfigChangeCmdType cmd_type,
                     const int64_t timeout_us);
  ~LogConfigChangeCmd();
  bool is_valid() const;
  void reset();
  bool is_remove_member_list() const;
  bool is_add_member_list() const;
  void in_leader(const palf::LogConfigVersion &config_version);
  bool is_set_new_replica_num() const;
  TO_STRING_KV("cmd_type", log_config_change_cmd2str(cmd_type_), K_(src), K_(palf_id), \
  K_(added_member), K_(removed_member), K_(curr_member_list), K_(curr_replica_num),    \
  K_(new_replica_num), K_(timeout_us), K_(lock_owner), K_(config_version),             \
  K_(added_list), K_(removed_list), K_(new_member_list));
  common::ObAddr src_;
  int64_t palf_id_;
  common::ObMember added_member_;
  common::ObMember removed_member_;
  common::ObMemberList curr_member_list_;
  int64_t curr_replica_num_;
  int64_t new_replica_num_;
  LogConfigChangeCmdType cmd_type_;
  int64_t timeout_us_;
  int64_t lock_owner_;
  palf::LogConfigVersion config_version_;
  common::ObMemberList added_list_;
  common::ObMemberList removed_list_;
  common::ObMemberList new_member_list_;
};

struct LogConfigChangeCmdResp {
  OB_UNIS_VERSION(1);
public:
  LogConfigChangeCmdResp();
  ~LogConfigChangeCmdResp();
  bool is_valid() const;
  void reset();
  TO_STRING_KV(K_(ret), K_(lock_owner), K_(is_locked));
public:
  int ret_;
  int64_t lock_owner_;
  bool is_locked_;
};

struct LogGetPalfStatReq {
  OB_UNIS_VERSION(1);
public:
  LogGetPalfStatReq(): src_(), palf_id_(-1), is_to_leader_(false) { }
  LogGetPalfStatReq(const common::ObAddr &src,
                    const int64_t palf_id,
                    const bool is_to_leader);
  ~LogGetPalfStatReq();
  bool is_valid() const;
  void reset();
  TO_STRING_KV(K_(src), K_(palf_id), K_(is_to_leader));
  common::ObAddr src_;
  int64_t palf_id_;
  bool is_to_leader_;
};

struct LogGetPalfStatResp {
  OB_UNIS_VERSION(1);
public:
  LogGetPalfStatResp() : palf_stat_()  { }
  LogGetPalfStatResp(const palf::PalfStat &palf_stat);
  ~LogGetPalfStatResp();
  bool is_valid() const;
  void reset();
  TO_STRING_KV(K_(palf_stat));
  palf::PalfStat palf_stat_;
};

struct LogFetchSource
{
  LogFetchSource() : server_(), begin_lsn_(), end_lsn_() {}
  bool is_valid() const
  {
    return server_.is_valid() && begin_lsn_.is_valid() && end_lsn_.is_valid()
        && begin_lsn_ < end_lsn_;
  }
  common::ObAddr server_;
  palf::LSN begin_lsn_;
  palf::LSN end_lsn_;
  TO_STRING_KV(K_(server), K_(begin_lsn), K_(end_lsn));
  OB_UNIS_VERSION(1);
};

// Read-only log source discovery for standby and CDC consumers.
struct LogGetFetchSourcesReq
{
  LogGetFetchSourcesReq() : src_(), tenant_id_(common::OB_INVALID_TENANT_ID), ls_id_() {}
  bool is_valid() const
  {
    return src_.is_valid() && common::is_valid_tenant_id(tenant_id_) && ls_id_.is_valid();
  }
  common::ObAddr src_;
  uint64_t tenant_id_;
  share::ObLSID ls_id_;
  TO_STRING_KV(K_(src), K_(tenant_id), K_(ls_id));
  OB_UNIS_VERSION(1);
};

struct LogGetFetchSourcesResp
{
  LogGetFetchSourcesResp() : sources_() {}
  bool is_valid() const { return !sources_.empty(); }
  void reset() { sources_.reset(); }
  common::ObSArray<LogFetchSource> sources_;
  TO_STRING_KV(K_(sources));
  OB_UNIS_VERSION(1);
};

constexpr int64_t INVALID_GC_TOKEN = 0;

class ObLSGCRequest
{
  OB_UNIS_VERSION(1);
public:
  ObLSGCRequest();
  ~ObLSGCRequest() { reset(); }
  int init(const common::ObAddr &src,
           const share::ObLSID &ls_id,
           const share::SCN &cutoff_scn,
           const share::SCN &end_scn,
           const palf::LogConfigVersion &config_version,
           const int64_t mode_version,
           const palf::LSN &begin_lsn,
           const palf::LSN &end_lsn,
           const int64_t gc_token);
  bool is_valid() const;
  void reset();
  const share::ObLSID &get_ls_id() const { return ls_id_; }
  const share::SCN &get_cutoff_scn() const { return cutoff_scn_; }
  const share::SCN &get_end_scn() const { return end_scn_; }
  const palf::LogConfigVersion &get_config_version() const { return config_version_; }
  int64_t get_mode_version() const { return mode_version_; }
  const palf::LSN &get_begin_lsn() const { return begin_lsn_; }
  const palf::LSN &get_end_lsn() const { return end_lsn_; }
  int64_t get_gc_token() const { return gc_token_; }
  TO_STRING_KV(K_(src), K_(ls_id), K_(cutoff_scn), K_(end_scn),
      K_(config_version), K_(mode_version), K_(begin_lsn), K_(end_lsn), K_(gc_token));
public:
  // Public because DEFINE_LOGSERVICE_SYNC_RPC_PROCESSOR accesses req.src_ directly.
  common::ObAddr src_;
private:
  share::ObLSID ls_id_;
  // Coverage uses [cutoff_scn_, end_scn_); the end boundary is excluded.
  share::SCN cutoff_scn_;
  share::SCN end_scn_;
  // Echo the last GC response's config version, including its leader term.
  // Invalid on the first request.
  palf::LogConfigVersion config_version_;
  // The requester's applied access-mode version; absent on older senders.
  int64_t mode_version_;
  // Valid LSNs certify coverage of [cutoff_scn_, end_scn_) in mode_version_.
  // Keep the actual committed LSN range for log source discovery.
  palf::LSN begin_lsn_;
  palf::LSN end_lsn_;
  // Zero starts a handshake. A formal request echoes the last successful response.
  int64_t gc_token_;
};

class ObLSGCResponse
{
  OB_UNIS_VERSION(1);
public:
  ObLSGCResponse();
  ~ObLSGCResponse() { reset(); }
  int init(const bool allow_gc, const palf::LogConfigVersion &config_version, const int64_t gc_token);
  bool is_valid() const;
  void reset();
  bool is_gc_allowed() const { return allow_gc_; }
  const palf::LogConfigVersion &get_config_version() const { return config_version_; }
  int64_t get_gc_token() const { return gc_token_; }
  TO_STRING_KV(K_(allow_gc), K_(config_version), K_(gc_token));
private:
  bool allow_gc_;
  palf::LogConfigVersion config_version_;
  // Zero denies the request and requires a new handshake.
  int64_t gc_token_;
};

enum LogServerProbeType
{
  PROBE_REQ = 0,
  PROBE_RESP,
};

struct LogServerProbeMsg {
  OB_UNIS_VERSION(1);
public:
  LogServerProbeMsg();
  LogServerProbeMsg(const common::ObAddr &src,
                    const int64_t palf_id,
                    const int64_t req_id,
                    const LogServerProbeType msg_type,
                    const int64_t status);
  ~LogServerProbeMsg();
  bool is_valid() const;
  void reset();
  TO_STRING_KV(K_(src), K_(palf_id), K_(req_id), K_(msg_type), K_(server_status));
  common::ObAddr src_;
  int64_t palf_id_;
  int64_t req_id_;
  LogServerProbeType msg_type_;
  int64_t server_status_;
};

struct LogChangeAccessModeCmd {
  OB_UNIS_VERSION(1);
public:
  LogChangeAccessModeCmd();
  LogChangeAccessModeCmd(const common::ObAddr &src,
                         const int64_t ls_id,
                         const int64_t mode_version,
                         const palf::AccessMode &access_mode,
                         const share::SCN &ref_scn);
  ~LogChangeAccessModeCmd()
  {
    reset();
  }
  bool is_valid() const;
  void reset();
  TO_STRING_KV(K_(src), K_(ls_id), K_(mode_version), \
      K_(access_mode), K_(ref_scn));
  common::ObAddr src_;
  int64_t ls_id_;
  int64_t mode_version_;
  palf::AccessMode access_mode_;
  share::SCN ref_scn_;
};

struct LogFlashbackMsg {
  OB_UNIS_VERSION(1);
public:
  LogFlashbackMsg();
  LogFlashbackMsg(const uint64_t src_tenant_id,
                  const common::ObAddr &src,
                  const int64_t ls_id,
                  const int64_t mode_version,
                  const share::SCN &flashback_scn,
                  const bool is_flashback_req);
  ~LogFlashbackMsg()
  {
    reset();
  }
  bool is_valid() const;
  void reset();
  bool is_flashback_req() const { return is_flashback_req_; }
  TO_STRING_KV(K_(src_tenant_id), K_(src), K_(ls_id), K_(mode_version), K_(flashback_scn), K_(is_flashback_req));
  uint64_t src_tenant_id_;
  common::ObAddr src_;
  int64_t ls_id_;
  int64_t mode_version_;
  share::SCN flashback_scn_;
  bool is_flashback_req_;
};

struct LogProbeRsReq
{
  OB_UNIS_VERSION(1);
public:
  LogProbeRsReq();
  LogProbeRsReq(const common::ObAddr src);
  ~LogProbeRsReq() {
    reset();
  }
  bool is_valid() const;
  void reset();
  TO_STRING_KV(K_(src));
  common::ObAddr src_;
};

struct LogProbeRsResp
{
  OB_UNIS_VERSION(1);
public:
  LogProbeRsResp();
  ~LogProbeRsResp() {
    reset();
  }
  bool is_valid() const;
  void reset();
  TO_STRING_KV(K_(ret));
  int ret_;
};

} // end namespace logservice
}// end namespace oceanbase

#endif
