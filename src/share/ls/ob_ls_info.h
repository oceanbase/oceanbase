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

#ifndef OCEANBASE_SHARE_LS_OB_LS_INFO_H_
#define OCEANBASE_SHARE_LS_OB_LS_INFO_H_

#include "share/ob_ls_id.h"      // for ObLSID
#include "common/ob_zone.h"      // for ObZone
#include "lib/net/ob_addr.h"     // for ObAddr
#include "lib/ob_replica_define.h" //for ObReplicaProperty
#include "lib/container/ob_array_serialization.h" // for ObSArray
#include "common/ob_role.h"      // for ObRole
#include "common/ob_member_list.h" // for ObMemberList
#include "share/restore/ob_ls_restore_status.h"
#include "common/ob_learner_list.h" // for GlobalLearnerList

namespace oceanbase
{
namespace share
{
class ObLSReplicaFilter;

enum ObReplicaStatus
{
  // replicas can serve normally
  REPLICA_STATUS_NORMAL = 0,
  // replicas cannot serve
  REPLICA_STATUS_OFFLINE,
  // flag replica, insert into partition table before create associated storage,
  // to indicate that we will create replica on the server later.
  REPLICA_STATUS_FLAG,
  REPLICA_STATUS_UNMERGED,
  // invalid value
  REPLICA_STATUS_MAX,
};

const char *ob_replica_status_str(const ObReplicaStatus status);
int get_replica_status(const char* str, ObReplicaStatus &status);
int get_replica_status(const common::ObString &status_str, ObReplicaStatus &status);

// [class_full_name] SimpleMember
// [class_functions] Use this class to build a member_list consists of this simple SimpleMember
// [class_attention] None
class SimpleMember
{
  OB_UNIS_VERSION(1);
public:
  SimpleMember() : server_(), timestamp_(0) {}
  SimpleMember(const common::ObAddr &server, const int64_t timestamp)
      : server_(server), timestamp_(timestamp) {}
  TO_STRING_KV(K_(server), K_(timestamp));
  // functions to set values
  int init(int64_t timestamp, char *member_list);
  // functions to get values
  inline common::ObAddr get_server() const { return server_; }
  inline int64_t get_timestamp() const { return timestamp_; }
  // definition of operators
  operator const common::ObAddr &() const { return server_; }
  bool operator ==(const SimpleMember &o) const
  { return server_ == o.get_server() && timestamp_ == o.get_timestamp(); }
  bool operator !=(const SimpleMember &o) const { return !(*this == o); }

private:
  common::ObAddr server_; //ip and port
  int64_t timestamp_;     //add timstamp
};

// [class_full_name] ObLSReplica
// [class_functions] To record informations about a ls's certain replica
// [class_attention] None
class ObLSReplica
{
  OB_UNIS_VERSION(1);
public:
  static const int64_t DEFAULT_REPLICA_COUNT = 7;
  typedef common::ObSEArray<SimpleMember, DEFAULT_REPLICA_COUNT, ObNullAllocator> MemberList;
  /*---------------------- MemberList related functions begin -----------------------*/
  // format-related functions
  static int member_list2text(const MemberList &member_list, ObSqlString &text);
  static int text2learner_list(const char *text, GlobalLearnerList &learner_list);
  static int text2member_list(const char *text, MemberList &member_list);
  // transform ObMemberList into MemberList
  static int transform_ob_member_list(
      const common::ObMemberList &ob_member_list,
      MemberList &member_list);
  static bool member_list_is_equal(const MemberList &a, const MemberList &b);
  static bool server_is_in_member_list(
      const MemberList &member_list,
      const common::ObAddr &server);
  static bool servers_in_member_list_are_same(const MemberList &a, const MemberList &b);
  static int check_all_servers_in_member_list_are_active(
      const MemberList &member_list,
      bool &all_acitve);
  /*---------------------- MemberList related functions end -------------------------*/

  // initial-related functions
  ObLSReplica();
  explicit ObLSReplica(const ObLSReplica &other) { assign(other); }
  ~ObLSReplica();
  void reset();
  int init(
      const int64_t create_time_us,
      const int64_t modify_time_us,
      const uint64_t tenant_id,
      const ObLSID &ls_id,
      const common::ObAddr &server,
      const int64_t sql_port,
      const common::ObRole &role,
      const common::ObReplicaType &replica_type,
      const int64_t proposal_id,
      const ObReplicaStatus &replica_status,
      const ObLSRestoreStatus &restore_status,
      const int64_t memstore_percent,
      const uint64_t unit_id,
      const ObString &zone,
      const int64_t paxos_replica_number,
      const int64_t data_size,
      const int64_t required_size,
      const MemberList &member_list,
      const GlobalLearnerList &learner_list,
      const bool rebuild);
  // check-related functions
  inline bool is_valid() const;
  inline bool is_strong_leader() const { return common::is_strong_leader(role_); }
  inline bool is_in_service() const { return replica_status_ == share::REPLICA_STATUS_NORMAL; }
  inline bool is_paxos_replica() const { return common::REPLICA_TYPE_ENCRYPTION_LOGONLY == replica_type_
                                                || common::REPLICA_TYPE_FULL == replica_type_
                                                || common::REPLICA_TYPE_LOGONLY == replica_type_; }
  inline bool is_in_restore() const { return !restore_status_.is_restore_none(); }
  int64_t to_string(char *buf, const int64_t buf_len) const;
  // operator-related functions
  int assign(const ObLSReplica &other);
  void operator=(const ObLSReplica &other) { assign(other); }
  bool is_equal_for_report(const ObLSReplica &other) const;

  // functions to get values
  inline int64_t get_create_time_us() const { return create_time_us_; }
  inline int64_t get_modify_time_us() const { return modify_time_us_; }
  inline uint64_t get_tenant_id() const { return tenant_id_; }
  inline ObLSID get_ls_id() const { return ls_id_; }
  inline common::ObAddr get_server() const { return server_; }
  inline int64_t get_sql_port() const { return sql_port_; }
  inline common::ObRole get_role() const { return role_; }
  inline const MemberList &get_member_list() const { return member_list_; }
  inline common::ObReplicaType get_replica_type() const { return replica_type_; }
  inline int64_t get_proposal_id() const { return proposal_id_; }
  inline ObReplicaStatus get_replica_status() const { return replica_status_; }
  inline ObLSRestoreStatus get_restore_status() const { return restore_status_; }
  inline common::ObReplicaProperty get_property() const { return property_; }
  inline int64_t get_memstore_percent() const { return property_.get_memstore_percent();}
  inline uint64_t get_unit_id() const { return unit_id_; }
  inline common::ObZone get_zone() const { return zone_; }
  inline int64_t get_paxos_replica_number() const { return paxos_replica_number_; }
  inline bool get_in_member_list() const { return in_member_list_; }
  inline bool get_in_learner_list() const { return in_learner_list_; }
  inline int64_t get_member_time_us() const { return member_time_us_; }
  inline int64_t get_data_size() const { return data_size_; }
  inline int64_t get_required_size() const { return required_size_; }
  inline bool get_rebuild() const { return rebuild_; }
  inline const common::GlobalLearnerList &get_learner_list() const { return learner_list_; }

  // functions to set values
  // ATTENTION:we use set_x() in cases for special needs below
  //need to set in case: do meta_table report, set to new value when replica changed
  inline void set_modify_time_us(const int64_t modify_time_us) { modify_time_us_ = modify_time_us; }
  //need to set in case: do meta_table report, change role from leader to follower when double-check failed
  inline void set_role(const common::ObRole &role) { role_ = role; }
  //need to set in case: do meta_table report, change status to new one
  inline void set_replica_status(const ObReplicaStatus &replica_status) { replica_status_ = replica_status; }
  //need to set in case: disaster_recovery, for compensate to set new replica type
  inline void set_replica_type(const common::ObReplicaType &replica_type) { replica_type_ = replica_type; }

  inline int add_member(SimpleMember m);
  inline int add_learner(const ObMember &m);
  inline void update_in_member_list_status(const bool in_member_list, const int64_t member_time_us);
  inline void update_in_learner_list_status(const bool in_learner_list, const int64_t learner_time_us) { in_learner_list_ = in_learner_list; member_time_us_ = learner_time_us; }
  //set replica role(FOLLOWER), proposal_id(0), modify_time(now)
  inline void update_to_follower_role();
  bool learner_list_is_equal(const common::GlobalLearnerList &a, const common::GlobalLearnerList &b) const;
private:
  // construct learner addr from a string in IPV$ or IPV6 format
  // @params[in]  input_string: the input
  // @params[out] the_count_of_colon_already_parsed: how many colon is parsed in this function
  // @params[out] learner_addr: the parsing result(ip:port)
  static int parse_addr_from_learner_string_(
             const ObString &input_string,
             int64_t &the_count_of_colon_already_parsed,
             ObAddr &learner_addr);

  // construct negative or positive integer from string
  // @params[in]  input_text: the input
  // @params[out] output_value: the result
  static int parsing_int_from_string_(
             const ObString &input_text,
             int64_t &output_value);
private:
  int64_t create_time_us_;               // store utc time
  int64_t modify_time_us_;               // store utc time
  // location-related infos
  uint64_t tenant_id_;                   // tenant_id
  ObLSID ls_id_;                         // log stream id
  common::ObAddr server_;                // ip and port
  int64_t sql_port_;                     // sql_port
  common::ObRole role_;                  // leader or follower
  MemberList member_list_;               // list to record member
  common::ObReplicaType replica_type_;   // replica type
  int64_t proposal_id_;                 // epoch of last leader time
  ObReplicaStatus replica_status_;       // replica status
  ObLSRestoreStatus restore_status_;     // restore status
  common::ObReplicaProperty property_;   // store memstore_percent
  // meta-related infos
  uint64_t unit_id_;                     // for R-replica
  common::ObZone zone_;                  // zone belonged to
  int64_t paxos_replica_number_;         // paxos_replica_number
  int64_t data_size_;                    // data size of ls_replica
  int64_t required_size_;                // data occupied of ls_replica
  // other infos, not record in table
  // no need to SERIALIZE, can be constructd by ObLSInfo::update_replica_status()
  bool in_member_list_;                  // whether in member_list
  int64_t member_time_us_;               // member_time_us
  common::GlobalLearnerList learner_list_;              // list to record R-replicas
  bool in_learner_list_;                 // whether in learner_list
  bool rebuild_;                         // whether in rebuild
};

// [class_full_name] class ObLSInfo
// [class_functions] To record informations about a ls, including every replicas
// [class_attention] None
class ObLSInfo
{
  OB_UNIS_VERSION(1);

public:
  typedef common::ObIArray<ObLSReplica> ReplicaArray;

  // initial-related functions
  ObLSInfo();
  explicit ObLSInfo(
      const uint64_t tenant_id,
      const ObLSID &ls_id);
  virtual ~ObLSInfo();
  void reset();
  // check-related functions
  inline bool is_valid() const;
  bool is_strong_leader(int64_t index) const;
  int64_t replica_count() const { return replicas_.count(); }
  // format-related functions
  TO_STRING_KV(K_(tenant_id), K_(ls_id), K_(replicas));
  // operator-related functions
  int assign(const ObLSInfo &other);
  // composite with other ls to add missing follower replica
  int composite_with(const ObLSInfo &other);
  // other functions
  virtual int add_replica(const ObLSReplica &replica);
  int update_replica_status();   // TODO: have to make sure actions in this function
  // return OB_ENTRY_NOT_EXIST for not found (set %replica to NULL too).
  // TODO: replace OB_ENTRY_NOT_EXIST with a more clear one like OB_LS_ENTRY_NOT_EXIST
  int find(const common::ObAddr &server, const ObLSReplica *&replica) const;
  int find_leader(const ObLSReplica *&replica) const;

  bool is_self_replica(const ObLSReplica &replica) const;
  int init(const uint64_t tenant_id, const ObLSID &ls_id);
  int init(
      const uint64_t tenant_id,
      const ObLSID &ls_id,
      const ReplicaArray &replicas);
  int init_by_replica(const ObLSReplica &replica);
  int remove(const common::ObAddr &server);

  // set and get related functions
  inline uint64_t get_tenant_id() const { return tenant_id_; }
  inline ObLSID get_ls_id() const { return ls_id_; }
  inline const ReplicaArray &get_replicas() const { return replicas_; }
  inline ReplicaArray &get_replicas() { return replicas_; }
  inline int64_t get_replicas_cnt() const { return replicas_.count(); }
  // If leader exists, return leader's member list. Otherwise, return paxos member in ls_info.
  int get_paxos_member_addrs(common::ObIArray<ObAddr> &addrs);
  int filter(const ObLSReplicaFilter &filter);

private:
  int find_idx_(const common::ObAddr &server, int64_t &idx) const;
  int find_idx_(const ObLSReplica &replica, int64_t &idx) const;

private:
  uint64_t tenant_id_;                      //which tenant's log stream
  share::ObLSID ls_id_;                     //identifier for log stream
  common::ObSArray<ObLSReplica> replicas_;  //replicas of this log stream
  DISALLOW_COPY_AND_ASSIGN(ObLSInfo);
};

inline int ObLSReplica::add_member(SimpleMember m)
{
  return member_list_.push_back(m);
}

inline int ObLSReplica::add_learner(const ObMember &m)
{
  return learner_list_.add_learner(m);
}

inline void ObLSReplica::update_in_member_list_status(
    const bool in_member_list,
    const int64_t member_time_us)
{
  in_member_list_ = in_member_list;
  member_time_us_ = member_time_us;
}

inline void ObLSReplica::update_to_follower_role()
{
  role_ = FOLLOWER;
  proposal_id_ = 0;
  modify_time_us_ = ObTimeUtility::current_time();
}

inline bool ObLSReplica::is_valid() const
{
  return ls_id_.is_valid_with_tenant(tenant_id_)
      && REPLICA_STATUS_FLAG != replica_status_
      && OB_INVALID_INDEX != sql_port_
      && OB_INVALID_ID != unit_id_
      && !zone_.is_empty();
}

inline bool ObLSInfo::is_valid() const
{
  return ls_id_.is_valid_with_tenant(tenant_id_);
}

} // end namespace share
} // end namespace oceanbase
#endif // OCEANBASE_SHARE_LS_OB_LS_INFO_H_
