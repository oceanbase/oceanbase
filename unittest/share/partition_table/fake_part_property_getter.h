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

#ifndef OCEANBASE_PARTITION_TABLE_FAKE_PART_PROPERTY_GETTER_H_
#define OCEANBASE_PARTITION_TABLE_FAKE_PART_PROPERTY_GETTER_H_

#include "share/partition_table/ob_ipartition_table.h"
#include "share/partition_table/ob_partition_info.h"
#include "common/ob_zone.h"
#include "common/ob_region.h"
#include "common/ob_partition_key.h"
#include "share/config/ob_server_config.h"

namespace oceanbase {
namespace share {

namespace host {
const static common::ObAddr A = common::ObAddr(common::ObAddr::IPV4, "8.8.8.1", 80);
const static common::ObAddr B = common::ObAddr(common::ObAddr::IPV4, "8.8.8.2", 80);
const static common::ObAddr C = common::ObAddr(common::ObAddr::IPV4, "8.8.8.3", 80);
const static common::ObAddr D = common::ObAddr(common::ObAddr::IPV4, "8.8.8.4", 80);
const static common::ObAddr E = common::ObAddr(common::ObAddr::IPV4, "8.8.8.5", 80);
const static common::ObAddr F = common::ObAddr(common::ObAddr::IPV4, "8.8.8.6", 80);
const static common::ObAddr G = common::ObAddr(common::ObAddr::IPV4, "8.8.8.7", 80);
const static common::ObAddr H = common::ObAddr(common::ObAddr::IPV4, "8.8.8.8", 80);

const static common::ObAddr A1 = common::ObAddr(common::ObAddr::IPV4, "8.8.1.1", 80);
const static common::ObAddr B1 = common::ObAddr(common::ObAddr::IPV4, "8.8.1.2", 80);
const static common::ObAddr C1 = common::ObAddr(common::ObAddr::IPV4, "8.8.1.3", 80);
const static common::ObAddr D1 = common::ObAddr(common::ObAddr::IPV4, "8.8.1.4", 80);
const static common::ObAddr E1 = common::ObAddr(common::ObAddr::IPV4, "8.8.1.5", 80);

const static common::ObAddr A2 = common::ObAddr(common::ObAddr::IPV4, "8.8.2.1", 80);
const static common::ObAddr B2 = common::ObAddr(common::ObAddr::IPV4, "8.8.2.2", 80);
const static common::ObAddr C2 = common::ObAddr(common::ObAddr::IPV4, "8.8.2.3", 80);
const static common::ObAddr D2 = common::ObAddr(common::ObAddr::IPV4, "8.8.2.4", 80);
const static common::ObAddr E2 = common::ObAddr(common::ObAddr::IPV4, "8.8.2.5", 80);

const static common::ObAddr A3 = common::ObAddr(common::ObAddr::IPV4, "8.8.3.1", 80);
const static common::ObAddr B3 = common::ObAddr(common::ObAddr::IPV4, "8.8.3.2", 80);
const static common::ObAddr C3 = common::ObAddr(common::ObAddr::IPV4, "8.8.3.3", 80);
const static common::ObAddr D3 = common::ObAddr(common::ObAddr::IPV4, "8.8.3.4", 80);
const static common::ObAddr E3 = common::ObAddr(common::ObAddr::IPV4, "8.8.3.5", 80);

const static common::ObAddr A4 = common::ObAddr(common::ObAddr::IPV4, "8.8.4.1", 80);
const static common::ObAddr B4 = common::ObAddr(common::ObAddr::IPV4, "8.8.4.2", 80);
const static common::ObAddr C4 = common::ObAddr(common::ObAddr::IPV4, "8.8.4.3", 80);
const static common::ObAddr D4 = common::ObAddr(common::ObAddr::IPV4, "8.8.4.4", 80);
const static common::ObAddr E4 = common::ObAddr(common::ObAddr::IPV4, "8.8.4.5", 80);

const static common::ObZone ZONE1("zone1");
const static common::ObZone ZONE2("zone2");
const static common::ObZone ZONE3("zone3");
const static common::ObZone ZONE4("zone4");
const static common::ObZone ZONE5("zone5");
const static common::ObZone ZONE6("zone6");

const static common::ObRegion REGION1("region1");
const static common::ObRegion REGION2("region2");
const static common::ObRegion REGION3("region3");

}  // namespace host

// implement ObIPartPropertyGetter for unittesting.
// all inlined for convenience.
class FakePartPropertyGetter : public ObIPartPropertyGetter {
public:
  static uint64_t& TEN()
  {
    static uint64_t id = 1;
    return id;
  }
  static uint64_t& TID()
  {
    static uint64_t tid = common::combine_id(TEN(), 1);
    return tid;
  }
  static int64_t& PID()
  {
    static int64_t pid = 0;
    return pid;
  }
  static int64_t& PCNT()
  {
    static int64_t p_cnt = 0;
    return p_cnt;
  }
  static uint64_t& UID()
  {
    static uint64_t uid = common::OB_INVALID_ID;
    return uid;
  }
  const static int64_t DATA_VERSION = 1;
  const static common::ObZone ZONE;

  typedef common::ObArray<ObPartitionReplica> ReplicaArray;

  FakePartPropertyGetter(){};
  virtual ~FakePartPropertyGetter(){};

  virtual int get_leader_member(const common::ObPartitionKey& key, common::ObIArray<common::ObAddr>& member_list);
  virtual int get_role(const common::ObPartitionKey& part_key, common::ObRole& role);

  virtual int get_member_list_and_leader(const common::ObPartitionKey& part_key, obrpc::ObMemberListAndLeaderArg& arg);
  virtual int get_member_list_and_leader_v2(
      const common::ObPartitionKey& part_key, obrpc::ObGetMemberListAndLeaderResult& arg);
  FakePartPropertyGetter& clear()
  {
    replicas_.reuse();
    return *this;
  }
  FakePartPropertyGetter& add(
      const common::ObAddr& server, const common::ObReplicaType& type, const common::ObZone& zone = common::ObZone(""))
  {
    return add_with_replica_type(TID(), PID(), type, UID(), server, FOLLOWER, zone);
  }
  FakePartPropertyGetter& add(
      const common::ObAddr& server, common::ObRole role, const common::ObZone& zone = common::ObZone(""))
  {
    return add_with_replica_type(TID(), PID(), REPLICA_TYPE_FULL, UID(), server, role, zone);
  }

  FakePartPropertyGetter& add_with_replica_type(const uint64_t tid, const int64_t pid,
      const common::ObReplicaType& type, const uint64_t unit_id, const common::ObAddr& server, common::ObRole role,
      const common::ObZone& zone = common::ObZone(""));

  FakePartPropertyGetter& add(const uint64_t tid, const int64_t pid, const uint64_t unit_id,
      const common::ObAddr& server, common::ObRole role, const common::ObZone& zone = common::ObZone(""));

  FakePartPropertyGetter& add_wrong_checksum(const uint64_t tid, const int64_t pid, const uint64_t unit_id,
      const common::ObAddr& server, common::ObRole role, const common::ObZone& zone = common::ObZone(""));

  FakePartPropertyGetter& add(const uint64_t tid, const int64_t pid, const common::ObAddr& server, common::ObRole role,
      const common::ObZone& zone = common::ObZone(""))
  {
    return add(tid, pid, UID(), server, role, zone);
  }
  FakePartPropertyGetter& add_wrong_checksum(const uint64_t tid, const int64_t pid, const common::ObAddr& server,
      common::ObRole role, const common::ObZone& zone = common::ObZone(""))
  {
    return add_wrong_checksum(tid, pid, UID(), server, role, zone);
  }

  FakePartPropertyGetter& add_flag_replica(
      const common::ObAddr& server, const common::ObZone& zone = common::ObZone(""), const uint64_t unit_id = 1);

  FakePartPropertyGetter& set_data_size(const int64_t data_size);

  int update_all(ObIPartitionTable& pt);
  int remove_all(ObIPartitionTable& pt);

  ReplicaArray& get_replicas()
  {
    return replicas_;
  }

private:
  ReplicaArray replicas_;
  ObPartitionReplica::MemberList member_list_;
};
const common::ObZone FakePartPropertyGetter::ZONE = "1";

inline int FakePartPropertyGetter::get_role(const common::ObPartitionKey& key, common::ObRole& role)
{
  int ret = common::OB_SUCCESS;
  UNUSED(key);
  role = common::LEADER;
  return ret;
}
inline int FakePartPropertyGetter::get_leader_member(
    const common::ObPartitionKey& key, common::ObIArray<common::ObAddr>& member_list)
{
  uint64_t table_id = key.table_id_;
  int64_t partition_id = key.get_partition_id();
  int ret = common::OB_SUCCESS;
  member_list.reuse();
  bool has_leader = false;
  for (int64_t i = 0; i < replicas_.count(); ++i) {
    if (table_id == replicas_.at(i).table_id_ && partition_id == replicas_.at(i).partition_id_) {
      if (common::LEADER == replicas_.at(i).role_) {
        has_leader = true;
      }
      member_list.push_back(replicas_.at(i).server_);
    }
  }
  if (member_list.count() <= 0) {
    ret = common::OB_ENTRY_NOT_EXIST;
    COMMON_LOG(WARN, "failed to get leader member", K(key), K_(replicas), K(ret));
  } else if (!has_leader) {
    ret = common::OB_NOT_MASTER;
  }
  return ret;
}

inline int FakePartPropertyGetter::get_member_list_and_leader(
    const common::ObPartitionKey& key, obrpc::ObMemberListAndLeaderArg& arg)
{
  uint64_t table_id = key.table_id_;
  int64_t partition_id = key.get_partition_id();
  int ret = common::OB_SUCCESS;
  arg.reset();
  bool has_leader = false;
  for (int64_t i = 0; OB_SUCC(ret) && i < replicas_.count(); ++i) {
    if (table_id == replicas_.at(i).table_id_ && partition_id == replicas_.at(i).partition_id_) {
      if (common::LEADER == replicas_.at(i).role_) {
        has_leader = true;
        arg.leader_ = replicas_.at(i).server_;
      }
      if (OB_FAIL(arg.member_list_.push_back(replicas_.at(i).server_))) {
        COMMON_LOG(WARN, "failed to add member", K(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (arg.member_list_.count() <= 0) {
      ret = common::OB_ENTRY_NOT_EXIST;
    } else if (!has_leader) {
      ret = common::OB_NOT_MASTER;
    }
  }
  return ret;
}

int FakePartPropertyGetter::get_member_list_and_leader_v2(
    const common::ObPartitionKey& key, obrpc::ObGetMemberListAndLeaderResult& arg)
{
  uint64_t table_id = key.table_id_;
  int64_t partition_id = key.get_partition_id();
  int ret = common::OB_SUCCESS;
  arg.reset();
  bool has_leader = false;
  for (int64_t i = 0; OB_SUCC(ret) && i < replicas_.count(); ++i) {
    if (table_id == replicas_.at(i).table_id_ && partition_id == replicas_.at(i).partition_id_) {
      if (common::LEADER == replicas_.at(i).role_) {
        has_leader = true;
        arg.leader_ = replicas_.at(i).server_;
      }
      ObMember member(replicas_.at(i).server_, replicas_.at(i).member_time_us_);
      if (OB_FAIL(arg.member_list_.push_back(member))) {
        COMMON_LOG(WARN, "failed to add member", K(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (arg.member_list_.count() <= 0) {
      ret = common::OB_ENTRY_NOT_EXIST;
    } else if (!has_leader) {
      ret = common::OB_NOT_MASTER;
    }
  }
  return ret;
}

FakePartPropertyGetter& FakePartPropertyGetter::add_wrong_checksum(const uint64_t tid, const int64_t pid,
    const uint64_t unit_id, const common::ObAddr& server, common::ObRole role, const common::ObZone& zone)
{
  ObPartitionReplica r;
  r.table_id_ = tid;
  r.partition_id_ = pid;
  r.server_ = server;
  r.role_ = role;
  r.unit_id_ = unit_id;
  r.member_time_us_ = ::oceanbase::common::ObTimeUtility::current_time();
  r.data_checksum_ = r.member_time_us_;
  if (zone == "") {
    r.zone_ = ZONE;
  } else {
    r.zone_ = zone;
  }
  r.partition_cnt_ = PCNT();
  r.data_version_ = DATA_VERSION;
  if (common::LEADER == r.role_) {
    r.to_leader_time_ = common::ObTimeUtility::current_time();
  }

  ObPartitionReplica::Member member;
  member.server_ = server;
  member.timestamp_ = ::oceanbase::common::ObTimeUtility::current_time();
  if (replicas_.count() > 0 && replicas_.at(replicas_.count() - 1).table_id_ == tid &&
      replicas_.at(replicas_.count() - 1).partition_id_ == pid) {
    member_list_.push_back(member);
    const uint64_t table_id = replicas_.at(replicas_.count() - 1).table_id_;
    const int64_t partition_id = replicas_.at(replicas_.count() - 1).partition_id_;
    for (int64_t i = replicas_.count() - 1; i >= 0; --i) {
      if (replicas_.at(i).table_id_ == table_id && replicas_.at(i).partition_id_ == partition_id) {
        replicas_.at(i).member_list_.push_back(member);
      } else {
        break;
      }
    }
  } else {
    member_list_.reset();
    member_list_.push_back(member);
  }
  r.member_list_ = member_list_;
  replicas_.push_back(r);
  return *this;
}

FakePartPropertyGetter& FakePartPropertyGetter::add_with_replica_type(const uint64_t tid, const int64_t pid,
    const common::ObReplicaType& type, const uint64_t unit_id, const common::ObAddr& server, common::ObRole role,
    const common::ObZone& zone)
{
  ObPartitionReplica r;
  r.table_id_ = tid;
  r.partition_id_ = pid;
  r.server_ = server;
  r.role_ = role;
  r.unit_id_ = unit_id;
  r.replica_type_ = type;
  r.member_time_us_ = ::oceanbase::common::ObTimeUtility::current_time();
  if (zone == "") {
    r.zone_ = ZONE;
  } else {
    r.zone_ = zone;
  }
  r.partition_cnt_ = PCNT();
  r.data_version_ = DATA_VERSION;
  if (common::LEADER == r.role_) {
    r.to_leader_time_ = common::ObTimeUtility::current_time();
  }

  ObPartitionReplica::Member member;
  member.server_ = server;
  member.timestamp_ = ::oceanbase::common::ObTimeUtility::current_time();
  if (type == REPLICA_TYPE_FULL) {
    if (replicas_.count() > 0 && replicas_.at(replicas_.count() - 1).table_id_ == tid &&
        replicas_.at(replicas_.count() - 1).partition_id_ == pid) {
      member_list_.push_back(member);
      const uint64_t table_id = replicas_.at(replicas_.count() - 1).table_id_;
      const int64_t partition_id = replicas_.at(replicas_.count() - 1).partition_id_;
      for (int64_t i = replicas_.count() - 1; i >= 0; --i) {
        if (replicas_.at(i).table_id_ == table_id && replicas_.at(i).partition_id_ == partition_id) {
          replicas_.at(i).member_list_.push_back(member);
        } else {
          break;
        }
      }
    } else {
      member_list_.reset();
      member_list_.push_back(member);
    }
    r.member_list_ = member_list_;
  }
  replicas_.push_back(r);
  return *this;
}

FakePartPropertyGetter& FakePartPropertyGetter::add(const uint64_t tid, const int64_t pid, const uint64_t unit_id,
    const common::ObAddr& server, common::ObRole role, const common::ObZone& zone)
{
  ObPartitionReplica r;
  r.table_id_ = tid;
  r.partition_id_ = pid;
  r.server_ = server;
  r.role_ = role;
  r.unit_id_ = unit_id;
  r.replica_type_ = common::REPLICA_TYPE_FULL;
  r.member_time_us_ = ::oceanbase::common::ObTimeUtility::current_time();
  if (zone == "") {
    r.zone_ = ZONE;
  } else {
    r.zone_ = zone;
  }
  r.partition_cnt_ = PCNT();
  r.data_version_ = DATA_VERSION;
  if (common::LEADER == r.role_) {
    r.to_leader_time_ = common::ObTimeUtility::current_time();
  }

  ObPartitionReplica::Member member;
  member.server_ = server;
  member.timestamp_ = ::oceanbase::common::ObTimeUtility::current_time();
  if (replicas_.count() > 0 && replicas_.at(replicas_.count() - 1).table_id_ == tid &&
      replicas_.at(replicas_.count() - 1).partition_id_ == pid) {
    member_list_.push_back(member);
    const uint64_t table_id = replicas_.at(replicas_.count() - 1).table_id_;
    const int64_t partition_id = replicas_.at(replicas_.count() - 1).partition_id_;
    for (int64_t i = replicas_.count() - 1; i >= 0; --i) {
      if (replicas_.at(i).table_id_ == table_id && replicas_.at(i).partition_id_ == partition_id) {
        replicas_.at(i).member_list_.push_back(member);
      } else {
        break;
      }
    }
  } else {
    member_list_.reset();
    member_list_.push_back(member);
  }
  r.member_list_ = member_list_;
  replicas_.push_back(r);
  return *this;
}

FakePartPropertyGetter& FakePartPropertyGetter::add_flag_replica(
    const common::ObAddr& server, const common::ObZone& zone, const uint64_t unit_id)
{
  ObPartitionReplica r;
  if (zone == "") {
    ObIPartitionTable::gen_flag_replica(TID(), PID(), 1, server, ZONE, unit_id, REPLICA_TYPE_FULL, 100, -1, r);
  } else {
    ObIPartitionTable::gen_flag_replica(TID(), PID(), 1, server, zone, unit_id, REPLICA_TYPE_FULL, 100, -1, r);
  }
  replicas_.push_back(r);
  return *this;
}

inline int FakePartPropertyGetter::update_all(ObIPartitionTable& pt)
{
  int ret = common::OB_SUCCESS;
  for (int64_t i = 0; common::OB_SUCCESS == ret && i < replicas_.count(); ++i) {
    GCONF.self_addr_ = replicas_.at(i).server_;
    ret = pt.update(replicas_.at(i));
  }
  return ret;
}

inline int FakePartPropertyGetter::remove_all(ObIPartitionTable& pt)
{
  int ret = common::OB_SUCCESS;
  for (int64_t i = 0; common::OB_SUCCESS == ret && i < replicas_.count(); ++i) {
    ret = pt.remove(replicas_.at(i).table_id_, replicas_.at(i).partition_id_, replicas_.at(i).server_);
  }
  return ret;
}

inline FakePartPropertyGetter& FakePartPropertyGetter::set_data_size(const int64_t data_size)
{
  for (int64_t i = 0; i < replicas_.count(); ++i) {
    replicas_.at(i).data_size_ = data_size;
    replicas_.at(i).row_count_ = data_size / 128;
  }
  return *this;
}

}  // end namespace share
}  // end namespace oceanbase

#endif  // OCEANBASE_PARTITION_TABLE_FAKE_PART_PROPERTY_GETTER_H_
