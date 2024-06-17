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
  bool is_initial_version() const;
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
  //for unitest
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
  bool is_all_list_unique() const;
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
private:
  template <typename LIST>
  int check_list_unique(GlobalLearnerList &server_list,
                        const LIST &list) const
  {
    int ret = OB_SUCCESS;
    for (int i = 0; OB_SUCC(ret) && i < list.get_member_number(); i++) {
      common::ObMember member;
      if (OB_FAIL(list.get_member_by_index(i, member))) {
        PALF_LOG(WARN, "get_server_by_index failed", K(list));
      } else if (server_list.contains(member.get_server())) {
        ret = OB_INVALID_ARGUMENT;
        PALF_LOG(WARN, "serverlist should not overlap with list", K(server_list), K(list));
      } else if (OB_FAIL(server_list.add_learner(member))) {
        PALF_LOG(WARN, "add_learner failed", K(server_list), K(list));
      }
    }
    return ret;
  }
};

enum ConfigChangeLockType
{
  LOCK_NOTHING = 0x0,
  LOCK_PAXOS_MEMBER_CHANGE = 0x1,         // binary 00001
  LOCK_PAXOS_REPLICA_NUMBER_CHANGE = 0x2,     // binary 00010 (TODO)
  LOCK_LEARNER_CHANGE = 0x4,            // binary 00100(TODO)
  LOCK_ACCESS_MODE_CHANGE = 0x8,          // binary 01000 (TODO)
  LOCK_ARBITRATION_MEMBER_CHANGE = 0x10,      // binary 10000 (TODO)
};

bool is_valid_config_lock_type(int64_t lock_type);

struct LogLockMeta
{
public:
  LogLockMeta() {reset();}
  ~LogLockMeta() {reset();}
  void reset();
  bool is_valid() const;
  int generate(const int64_t lock_owner, const int64_t lock_type);
  void operator=(const LogLockMeta &lock_meta);
  bool operator==(const LogLockMeta &lock_meta) const;
  void reset_as_unlocked();
  void unlock();
  bool is_locked() const;
  bool is_lock_owner_valid() const;
  TO_STRING_KV(K_(version), K_(lock_type), K_(lock_owner), K_(lock_time));
  NEED_SERIALIZE_AND_DESERIALIZE;
public:
  static constexpr int64_t LOG_LOCK_META_VERSION = 1;
  int64_t version_;//for compatibilty
  int64_t lock_owner_;// owner of lock
  int64_t lock_type_;// ConfigChangeLockType
  int64_t lock_time_;// the timestamp of executing locking or unlocking. default as OB_INVALID_TIMESTAMP, using fordebugging
};

struct LogConfigInfoV2
{
public:
  LogConfigInfoV2();
  ~LogConfigInfoV2();
public:
  static constexpr int64_t LOG_CONFIG_INFO_VERSION = 1;
  bool is_valid() const;
  void reset();
  //for init with default
  int generate(const LogConfigVersion &config_version);
  //for serialize compaction
  int generate(const LogConfigInfo &config_info);
  //for deserialization from lower version
  int transform_for_deserialize(const LogConfigInfo &config_info);
  int convert_to_complete_config(common::ObMemberList &all_paxos_memberlist,
                                 int64_t &all_paxos_replica_num,
                                 GlobalLearnerList &all_learners) const;
  bool is_config_change_locked() const;

  //for unitest
  int generate(const common::ObMemberList &memberlist,
               const int64_t replica_num,
               const common::GlobalLearnerList &learnerlist,
               const LogConfigVersion &config_version);
  //for unitest
  int generate(const common::ObMemberList &memberlist,
               const int64_t replica_num,
               const common::GlobalLearnerList &learnerlist,
               const LogConfigVersion &config_version,
               const LogLockMeta &lock_meta);
  // For unittest
  void operator=(const LogConfigInfoV2 &config_info);
  bool operator==(const LogConfigInfoV2 &config_info) const;
  TO_STRING_KV(K_(version), K_(config), K_(lock_meta));
  NEED_SERIALIZE_AND_DESERIALIZE;
public:
  int64_t version_;
  LogConfigInfo config_;
  LogLockMeta lock_meta_;
};

// Change member log for consenus
struct LogConfigMeta {
public:
  LogConfigMeta();
  ~LogConfigMeta();

public:
  // Note: the function will generate a default version_.
  int generate_for_default(const int64_t proposal_id,
                           const LogConfigInfoV2 &prev_config_info,
                           const LogConfigInfoV2 &curr_config_info);
  int generate(const int64_t proposal_id,
               const LogConfigInfoV2 &prev_config_info,
               const LogConfigInfoV2 &curr_config_info,
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
  LogConfigInfoV2 prev_;//modified in version_42
  LogConfigInfoV2 curr_;//modified in version_42
  // ====== added members in VERSION 2 ========
  int64_t prev_log_proposal_id_;
  LSN prev_lsn_;
  int64_t prev_mode_pid_;

  static constexpr int64_t LOG_CONFIG_META_VERSION = 1;
  static constexpr int64_t LOG_CONFIG_META_VERSION_INC = 2;
  static constexpr int64_t LOG_CONFIG_META_VERSION_42 = 3;//LogConfigInfo-->LogConfigInfoV2
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
