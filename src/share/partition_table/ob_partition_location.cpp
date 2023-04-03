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

#define USING_LOG_PREFIX SHARE_PT

#include "ob_partition_location.h"

#include "lib/utility/serialization.h"
#include "lib/utility/ob_print_utils.h"

namespace oceanbase
{
using namespace common;
namespace share
{

OB_SERIALIZE_MEMBER(ObPartitionLocation,
                    table_id_,
                    partition_id_,
                    partition_cnt_,
                    replica_locations_,
                    renew_time_,
                    is_mark_fail_);

OB_SERIALIZE_MEMBER(ObReplicaLocation,
                    server_,
                    role_,
                    sql_port_,
                    reserved_,
                    replica_type_,
                    property_);

OB_SERIALIZE_MEMBER(ObPartitionReplicaLocation,
                    table_id_,
                    partition_id_,
                    partition_cnt_,
                    replica_location_,
                    renew_time_);

ObReplicaLocation::ObReplicaLocation()
{
  reset();
}

void ObReplicaLocation::reset()
{
  server_.reset();
  role_ = FOLLOWER;
  sql_port_ = OB_INVALID_INDEX;
  reserved_ = 0;
  replica_type_ = REPLICA_TYPE_FULL;
  property_.reset();
}

ObPartitionLocation::ObPartitionLocation()
    :replica_locations_(ObModIds::OB_MS_LOCATION_CACHE, OB_MALLOC_NORMAL_BLOCK_SIZE)
{
  reset();
}

ObPartitionLocation::ObPartitionLocation(common::ObIAllocator &allocator)
    :replica_locations_(common::OB_MALLOC_NORMAL_BLOCK_SIZE,
                        common::ModulePageAllocator(allocator))
{
  reset();
}

ObPartitionLocation::~ObPartitionLocation()
{
  reset();
}

void ObPartitionLocation::reset()
{
  table_id_ = OB_INVALID_ID;
  partition_id_ = OB_INVALID_INDEX;
  partition_cnt_ = 0;
  replica_locations_.reset();
  renew_time_ = 0;
  sql_renew_time_ = 0;
  is_mark_fail_ = false;
}

int ObPartitionLocation::assign(const ObPartitionLocation &other)
{
  int ret = OB_SUCCESS;

  if (this != &other) {
    table_id_ = other.table_id_;
    partition_id_ = other.partition_id_;
    partition_cnt_ = other.partition_cnt_;
    renew_time_ = other.renew_time_;
    sql_renew_time_ = other.sql_renew_time_;
    is_mark_fail_ = other.is_mark_fail_;
    if (OB_FAIL(replica_locations_.assign(other.replica_locations_))) {
      LOG_WARN("Failed to assign replica locations", K(ret));
    }
  }

  return ret;
}

int ObPartitionLocation::assign_with_only_readable_replica(
    const ObPartitionLocation &other)
{
  int ret = OB_SUCCESS;
  reset();
  table_id_ = other.table_id_;
  partition_id_ = other.partition_id_;
  partition_cnt_ = other.partition_cnt_;
  renew_time_ = other.renew_time_;
  sql_renew_time_ = other.sql_renew_time_;
  is_mark_fail_ = other.is_mark_fail_;
  for (int64_t i = 0; OB_SUCC(ret) && i < other.replica_locations_.count(); ++i) {
    const ObReplicaLocation &replica_loc = other.replica_locations_.at(i);
    if (!ObReplicaTypeCheck::is_readable_replica(replica_loc.replica_type_)) {
      //skip
    } else if (OB_FAIL(replica_locations_.push_back(replica_loc))) {
        LOG_WARN("Failed to push back replica locations",
                 K(ret), K(i), K(replica_loc), K(other.replica_locations_));
    }
  }
  return ret;
}

int ObPartitionLocation::assign(const ObPartitionReplicaLocation &part_rep_loc)
{
  int ret = OB_SUCCESS;
  table_id_ = part_rep_loc.table_id_;
  partition_id_ = part_rep_loc.partition_id_;
  partition_cnt_ = part_rep_loc.partition_cnt_;
  renew_time_ = part_rep_loc.renew_time_;
  //is_mark_fail_ = part_rep_loc.is_mark_fail_; it's no need to assign is_mark_fail_ in deserialization.
  if (OB_FAIL(replica_locations_.push_back(part_rep_loc.replica_location_))) {
    LOG_WARN("Failed to push back replica location", K(ret));
  }
  return ret;
}

bool ObPartitionLocation::is_valid() const
{
  return OB_INVALID_ID != table_id_ && OB_INVALID_INDEX != partition_id_
      && renew_time_ >= 0 && sql_renew_time_ >= 0;
}

bool ObPartitionLocation::operator==(const ObPartitionLocation &other) const
{
  bool equal = true;
  if (!is_valid() || !other.is_valid()) {
    equal = false;
    LOG_WARN_RET(common::OB_INVALID_ARGUMENT, "invalid argument", "self", *this, K(other));
  } else if (replica_locations_.count() != other.replica_locations_.count()) {
    equal = false;
  } else {
    for (int64_t i = 0; i < replica_locations_.count(); ++i) {
      if (replica_locations_.at(i) != other.replica_locations_.at(i)) {
        equal = false;
        break;
      }
    }
    equal = equal && (table_id_ == other.table_id_)
                  && (partition_id_ == other.partition_id_)
                  && (partition_cnt_ == other.partition_cnt_)
                  && (renew_time_ == other.renew_time_)
                  && (sql_renew_time_ == other.sql_renew_time_)
                  && (is_mark_fail_ == other.is_mark_fail_);
  }
  return equal;
}

int ObPartitionLocation::add(const ObReplicaLocation &replica_location)
{
  int ret = OB_SUCCESS;
  // Replica location may be added before table_id/partition_id set,
  // can not check self validity here.
  if (!replica_location.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid replica location", K(replica_location), K(ret));
  } else if (OB_FAIL(add_with_no_check(replica_location))) {
    LOG_WARN("fail to do add replica location", K(replica_location), K(ret));
  }
  return ret;
}

int ObPartitionLocation::add_with_no_check(const ObReplicaLocation &replica_location)
{
  int ret = OB_SUCCESS;
  int64_t idx = OB_INVALID_INDEX;
  if (OB_LIKELY(OB_SUCCESS != (ret = (find(replica_location.server_, idx))))) {
    if (OB_LIKELY(OB_ENTRY_NOT_EXIST == ret)) {
      if (OB_FAIL(replica_locations_.push_back(replica_location))) {
        LOG_WARN("push back replica location failed", K(ret));
      }
    } else {
      LOG_WARN("find server location failed", K(ret), "server", replica_location.server_);
    }
  } else {
    ret = OB_ERR_ALREADY_EXISTS;
    LOG_WARN("replica location already exist, can not add", K(replica_location), K(ret));
  }
  return ret;
}

int ObPartitionLocation::del(const common::ObAddr &server)
{
  int ret = OB_SUCCESS;
  int64_t index = OB_INVALID_INDEX;
  if (!is_valid() || !server.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", "self", *this, K(server), K(ret));
  } else if (OB_FAIL(find(server, index))) {
    LOG_WARN("find server location failed", K(server), K(ret));
  } else if (index < 0 || index >= replica_locations_.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("index index", K(ret), K(index),
        "replica location count", replica_locations_.count());
  } else if (OB_FAIL(replica_locations_.remove(index))) {
    LOG_WARN("remove replica location failed", K(index), K(server), K(ret));
  }
  return ret;
}

int ObPartitionLocation::update(const common::ObAddr &server, ObReplicaLocation &replica_location)
{
  int ret = OB_SUCCESS;
  int64_t index = OB_INVALID_INDEX;
  if (!is_valid() || !server.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", "self", *this, K(server), K(ret));
  } else if (OB_FAIL(find(server, index))) {
    LOG_WARN("find server location failed", K(server), K(ret));
  } else if (index < 0 || index >= replica_locations_.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("index index", K(ret), K(index),
        "replica location count", replica_locations_.count());
  } else {
    replica_locations_.at(index) = replica_location;
  }
  return ret;
}

int ObPartitionLocation::get_strong_leader(ObReplicaLocation &replica_location, int64_t &replica_idx) const
{
  int ret = OB_LOCATION_LEADER_NOT_EXIST;
  if (!is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), "self", *this);
  } else {
    ARRAY_FOREACH_X(replica_locations_, i, cnt, OB_LOCATION_LEADER_NOT_EXIST == ret) {
      if (replica_locations_.at(i).is_strong_leader()) {
        replica_location = replica_locations_.at(i);
        replica_idx = i;
        ret = OB_SUCCESS;
      }
    }
  }
  return ret;
}

int ObPartitionLocation::get_strong_leader(ObReplicaLocation &replica_location) const
{
  int64_t replica_idx = OB_INVALID_INDEX;
  return get_strong_leader(replica_location, replica_idx);
}

int ObPartitionLocation::get_restore_leader(ObReplicaLocation &replica_location) const
{
  int ret = OB_SUCCESS;
  if (!is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), "self", *this);
  } else {
    // Return strong leader first.
    const ObReplicaLocation *leader = NULL;
    ARRAY_FOREACH_X(replica_locations_, i, cnt, OB_SUCC(ret)) {
      if (replica_locations_.at(i).is_leader_like()) {
        leader = &replica_locations_.at(i);
        if (leader->is_strong_leader()) {
          break;
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(leader)) {
      ret = OB_LOCATION_LEADER_NOT_EXIST;
    } else {
      replica_location = *leader;
    }
  }
  return ret;
}

int ObPartitionLocation::get_leader_by_election(ObReplicaLocation &replica_location) const
{
  int ret = OB_SUCCESS;
  if (!is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), "self", *this);
  } else {
    const ObReplicaLocation *leader = NULL;
    ARRAY_FOREACH_X(replica_locations_, i, cnt, OB_SUCC(ret)) {
      if (replica_locations_.at(i).is_leader_by_election()) {
        leader = &replica_locations_.at(i);
        if (leader->is_strong_leader()) {
          break;
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(leader)) {
      ret = OB_LOCATION_LEADER_NOT_EXIST;
    } else {
      replica_location = *leader;
    }
  }
  return ret;
}
int ObPartitionLocation::check_strong_leader_exist() const
{
  int ret = OB_LOCATION_LEADER_NOT_EXIST;
  if (!is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), "self", *this);
  } else {
    FOREACH_CNT_X(location, replica_locations_, OB_LOCATION_LEADER_NOT_EXIST == ret) {
      if (location->is_strong_leader()) {
        ret = OB_SUCCESS;
      }
    }
  }
  return ret;
}

int ObPartitionLocation::find(const ObAddr &server, int64_t &idx) const
{
  int ret = OB_ENTRY_NOT_EXIST;
  idx = OB_INVALID_INDEX;
  // May be used before table_id/partition_id set, can not check self validity here.
  // server(ObAddr) is checked by caller, no need check again.
  for (int64_t i = 0; OB_ENTRY_NOT_EXIST == ret && i < replica_locations_.count(); ++i) {
    if (replica_locations_.at(i).server_ == server) {
      idx = i;
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

int ObPartitionLocation::change_leader(const ObReplicaLocation &new_leader)
{
  int ret = OB_SUCCESS;
  // new_leader could be 0.0.0.0, so should not check new_leader
  if (!is_valid()) {
    ret = OB_NOT_INIT;
    LOG_WARN("cannot change leader for invalid locatin", KR(ret), K(*this));
  } else {
    ObReplicaLocation *new_leader_location = NULL;
    for (int64_t i = 0; i < replica_locations_.count(); ++i) {
      if (replica_locations_.at(i).server_ == new_leader.server_) {
        new_leader_location = &replica_locations_.at(i);
        break;
      }
    }
    if (NULL == new_leader_location && new_leader.server_.is_valid()) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("new leader is not in location, cannot change leader", KR(ret), K(new_leader));
    } else {
      for (int64_t i = 0; i < replica_locations_.count(); ++i) {
        replica_locations_.at(i).role_ = FOLLOWER;
      }
      if (NULL != new_leader_location) {
        new_leader_location ->role_ = new_leader.role_;
      }
    }
  }
  return ret;
}

int ObPartitionLocation::alloc_new_location(
    common::ObIAllocator &allocator,
    ObPartitionLocation *&new_location)
{
  int ret = OB_SUCCESS;
  void *buf = NULL;
  if (OB_ISNULL(buf = allocator.alloc(sizeof(ObPartitionLocation)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc location", K(ret));
  } else {
    new_location = new (buf) ObPartitionLocation(allocator);
  }
  return ret;
}


ObPartitionReplicaLocation::ObPartitionReplicaLocation()
{
  reset();
}

bool ObPartitionReplicaLocation::compare_part_loc_asc(const ObPartitionReplicaLocation &left,
                                                      const ObPartitionReplicaLocation &right)
{
  return left.get_partition_id() < right.get_partition_id();
}

bool ObPartitionReplicaLocation::compare_part_loc_desc(const ObPartitionReplicaLocation &left,
                                                       const ObPartitionReplicaLocation &right)
{
  return left.get_partition_id() > right.get_partition_id();
}

void ObPartitionReplicaLocation::reset()
{
  table_id_ = OB_INVALID_ID;
  partition_id_ = OB_INVALID_INDEX;
  partition_cnt_ = 0;
  replica_location_.reset();
  renew_time_ = 0;
}

int ObPartitionReplicaLocation::assign(const ObPartitionReplicaLocation &other)
{
  int ret = OB_SUCCESS;
  table_id_ = other.table_id_;
  partition_id_ = other.partition_id_;
  partition_cnt_ = other.partition_cnt_;
  replica_location_ = other.replica_location_;
  renew_time_ = other.renew_time_;
  return ret;
}

int ObPartitionReplicaLocation::assign(uint64_t table_id,
                                       int64_t partition_id,
                                       int64_t partition_cnt,
                                       const ObReplicaLocation &replica_location,
                                       int64_t renew_time)
{
  int ret = OB_SUCCESS;
  table_id_ = table_id;
  partition_id_ = partition_id;
  partition_cnt_ = partition_cnt;
  replica_location_ = replica_location;
  renew_time_ = renew_time;
  return ret;
}

int ObPartitionReplicaLocation::assign_strong_leader(const ObPartitionLocation &other)
{
  int ret = OB_SUCCESS;
  table_id_ = other.get_table_id();
  partition_id_ = other.get_partition_id();
  partition_cnt_ = other.get_partition_cnt();
  renew_time_ = other.get_renew_time();
  if (OB_FAIL(other.get_strong_leader(replica_location_))) {
    LOG_WARN("fail to get leader", K(ret), K(other));
  }
  return ret;
}

bool ObPartitionReplicaLocation::is_valid() const
{
  return OB_INVALID_ID != table_id_ && OB_INVALID_INDEX != partition_id_ && renew_time_ >= 0;
}

bool ObPartitionReplicaLocation::operator==(const ObPartitionReplicaLocation &other) const
{
  bool equal = true;
  if (!is_valid() || !other.is_valid()) {
    equal = false;
    LOG_WARN_RET(common::OB_INVALID_ARGUMENT, "invalid argument", "self", *this, K(other));
  } else if (replica_location_ != other.replica_location_) {
    equal = false;
  } else {
    equal = (table_id_ == other.table_id_)
        && (partition_id_ == other.partition_id_)
        && (partition_cnt_ == other.partition_cnt_)
        && (renew_time_ == other.renew_time_);
  }
  return equal;
}

}//end namespace share
}//end namespace oceanbase
