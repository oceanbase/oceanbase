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

#ifndef OCEANBASE_LOGSERVICE_LOG_META_INFO_
#define OCEANBASE_LOGSERVICE_LOG_META_INFO_

#include "lib/ob_define.h"              // ObReplicaType
#include "lib/utility/ob_print_utils.h" // Print*
#include "common/ob_learner_list.h"     // common::GlobalLearnerList
#include "common/ob_member_list.h"      // ObMemberList
#include "share/scn.h"                        // SCN
#include "lsn.h"                        // LSN
#include "palf_base_info.h"             // LogInfo
#include "palf_options.h"               // AccessMode
#include "log_define.h"                 // ReplicaType

namespace oceanbase
{
namespace palf
{

struct LogVotedFor {
  LogVotedFor();
  ~LogVotedFor();
  LogVotedFor(const ObAddr &addr);
  LogVotedFor(const LogVotedFor &voted_for);
  LogVotedFor(LogVotedFor &&voted_for);
  LogVotedFor &operator=(const LogVotedFor &voted_for);
  bool operator==(const LogVotedFor &voted_for);
  void reset();
  int64_t to_string(char *buf, int64_t buf_len) const;
  static constexpr int64_t COUNT = 3;
  // ipv6 need occupy 16BYTE
  // port need occupy 8BYTE
  //
  // ---  ipv6 ---
  // --- memory layout ----
  // ---- ipv6 higher -----
  // ---- ipv6 lower  -----
  // ---- ipv6 port   -----
  //
  // --- ipv4 ---
  // ---- ip ----
  // --- port ---
  int64_t voted_for_[COUNT];
  NEED_SERIALIZE_AND_DESERIALIZE;
};

// Prepare meta for consenus
struct LogPrepareMeta
{
public:
  LogPrepareMeta();
  ~LogPrepareMeta();

public:
  int generate(const LogVotedFor &voted_for, const int64_t &log_proposal_id);
  bool is_valid() const;
  void reset();
  void operator=(const LogPrepareMeta &log_prepare_meta);
  TO_STRING_KV(K_(version), K_(voted_for), K_(log_proposal_id));
  NEED_SERIALIZE_AND_DESERIALIZE;

  int64_t version_;
  LogVotedFor voted_for_;
  int64_t log_proposal_id_;
  static constexpr int64_t LOG_PREPARE_VERSION = 1;
};

class LogConfigVersion {
public:
  LogConfigVersion();
  ~LogConfigVersion();
  void operator=(const LogConfigVersion &config_version);

public:
  int generate(const int64_t proposal_id, const int64_t config_seq);
  int inc_update_version(const int64_t proposal_id);
  bool is_valid() const;
  void reset();
  int64_t to_string(char *buf, const int64_t buf_len);
  bool operator==(const LogConfigVersion &config_version) const;
  bool operator!=(const LogConfigVersion &config_version) const;
  bool operator>(const LogConfigVersion &config_version) const;
  bool operator<(const LogConfigVersion &config_version) const;
  bool operator>=(const LogConfigVersion &config_version) const;
  bool operator<=(const LogConfigVersion &config_version) const;
  TO_STRING_KV(K_(proposal_id), K_(config_seq));
  NEED_SERIALIZE_AND_DESERIALIZE;
  // Ensure that:
  // 1. For leader don't has any writes, this field used to ensure leader completeness.
  //    (NB: can not use the proposal_id of prepare request, because a lagged replica
  //     may receive the prepare reqeust of new leader).
  // 2. For changing config, this field used to ensure inc config_version monotoniclly.
private:
  int64_t proposal_id_;
  int64_t config_seq_;
};

struct LogConfigInfo
{
public:
  LogConfigInfo();
  ~LogConfigInfo();
  void operator=(const LogConfigInfo &config_info);

public:
  bool is_valid() const;
  void reset();
  int generate(const common::ObMemberList &memberlist,
               const int64_t replica_num,
               const common::GlobalLearnerList &learnerlist,
               const LogConfigVersion &config_version);
  // @brief get the expected paxos member list without arbitraion member,
  // including degraded paxos members.
  // @param[in/out] ObMemberList, the output member list
  // @param[in/out] int64_t, the output replica_num
  // @retval
  //    return OB_SUCCESS if success
  //    else return other errno
  int get_expected_paxos_memberlist(common::ObMemberList &paxos_memberlist,
                                    int64_t &paxos_replica_num) const;
  int convert_to_complete_config(common::ObMemberList &alive_paxos_memberlist,
                                 int64_t &alive_paxos_replica_num,
                                 GlobalLearnerList &all_learners) const;
  // For unittest
  bool operator==(const LogConfigInfo &config_info) const;
  TO_STRING_KV(K_(config_version),
               K_(log_sync_memberlist),
               K_(log_sync_replica_num),
               K_(arbitration_member),
               K_(learnerlist),
               K_(degraded_learnerlist));
  NEED_SERIALIZE_AND_DESERIALIZE;

  // paxos members for log sync, don't include arbitration replica
  common::ObMemberList log_sync_memberlist_;
  // replica number for log sync, don't include arbitration replica
  int64_t log_sync_replica_num_;
  // arbitration replica
  common::ObMember arbitration_member_;
  // all learners, don't include learners which have been degraded from members
  common::GlobalLearnerList learnerlist_;
  // learners which have been degraded from members
  common::GlobalLearnerList degraded_learnerlist_;
  LogConfigVersion config_version_;
};

// Change member log for consenus
struct LogConfigMeta {
public:
  LogConfigMeta();
  ~LogConfigMeta();

public:
  int generate_for_default(const int64_t proposal_id,
                           const LogConfigInfo &prev_config_info,
                           const LogConfigInfo &curr_config_info);
  int generate_for_default_in_arb(const int64_t proposal_id,
                                  const LogConfigInfo &prev_config_info,
                                  const LogConfigInfo &curr_config_info);
  int generate(const int64_t proposal_id,
               const LogConfigInfo &prev_config_info,
               const LogConfigInfo &curr_config_info,
               const int64_t prev_log_proposal_id,
               const LSN &prev_lsn,
               const int64_t prev_mode_pid);
  bool is_valid() const;
  void reset();
  void operator=(const LogConfigMeta &log_config_meta);
  TO_STRING_KV(K_(version), K_(proposal_id), K_(prev), K_(curr), K_(prev_log_proposal_id),
      K_(prev_lsn), K_(prev_mode_pid));
  NEED_SERIALIZE_AND_DESERIALIZE;
  int64_t version_;
  // ====== members in VERSION 1 ========
  int64_t proposal_id_;
  LogConfigInfo prev_;
  LogConfigInfo curr_;
  // ====== added members in VERSION 2 ========
  int64_t prev_log_proposal_id_;
  LSN prev_lsn_;
  int64_t prev_mode_pid_;

  static constexpr int64_t LOG_CONFIG_META_VERSION = 1;
  static constexpr int64_t LOG_CONFIG_META_VERSION_INC = 2;
};

struct LogModeMeta {
public:
  LogModeMeta();
  ~LogModeMeta();
  int generate(
      const int64_t proposal_id,
      const int64_t mode_version,
      const AccessMode &access_mode,
      const share::SCN &ref_scn);
  bool is_valid() const;
  void reset();
  void operator=(const LogModeMeta &mode_meta);
  TO_STRING_KV(K_(version), K_(proposal_id), K_(mode_version), K_(access_mode), K_(ref_scn));
  NEED_SERIALIZE_AND_DESERIALIZE;
public:
  int64_t version_;
  // proposal_id of LogModeMeta, changed by leader switching and change_access_mode
  int64_t proposal_id_;
  // proposal_id of last access_mode_, only changed by change_access_mode
  int64_t mode_version_;
  AccessMode access_mode_;
  // scn lower bound
  // after switching over, scn of all submitted log should be bigger than ref_scn_
  share::SCN ref_scn_;

  static constexpr int64_t LOG_MODE_META_VERSION = 1;
};

// Garbage collect controller
struct LogSnapshotMeta
{
public:
  LogSnapshotMeta();
  ~LogSnapshotMeta();

public:
  int generate(const LSN &lsn);
  int generate(const LSN &lsn, const LogInfo &prev_log_info);
  bool is_valid() const;
  void reset();
  int get_prev_log_info(LogInfo &log_info) const;
  void operator=(const LogSnapshotMeta &log_snapshot_meta);
  TO_STRING_KV(K_(version), K_(base_lsn), K_(prev_log_info));
  NEED_SERIALIZE_AND_DESERIALIZE;

  int64_t version_;
  LSN base_lsn_;
  // prev_log_info_ is invalid by default,
  // it's valid for migrating dest ls.
  // By using this info, the dest ls does not need fetch prev log file
  // of base_lsn from the source ls.
  LogInfo prev_log_info_;

  static constexpr int64_t LOG_SNAPSHOT_META_VERSION = 1;
};

struct LogReplicaPropertyMeta {
public:
  LogReplicaPropertyMeta(): version_(-1), allow_vote_(false), replica_type_(LogReplicaType::INVALID_REPLICA) { }
  ~LogReplicaPropertyMeta() { }

public:
  int generate(const bool allow_vote, const LogReplicaType replica_type);
  bool is_valid() const;
  void reset();
  void operator=(const LogReplicaPropertyMeta &replica_meta);
  TO_STRING_KV(K_(allow_vote), "replica_type", replica_type_2_str(replica_type_));
  NEED_SERIALIZE_AND_DESERIALIZE;

  int64_t version_;
  // a persistent flag which means if this paxos replica
  // is allowed to reply ack when receiving logs. It's true by default.
  bool allow_vote_;
  // persistent replica type flag
  LogReplicaType replica_type_;
  static constexpr int64_t LOG_REPLICA_PROPERTY_META_VERSION = 1;
};

} // end namespace palf
} // end namespace oceanbase

#endif
