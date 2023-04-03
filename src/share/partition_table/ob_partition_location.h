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

#ifndef OCEANBASE_SHARE_PARTITION_TABLE_OB_PARTITION_LOCATION_H_
#define OCEANBASE_SHARE_PARTITION_TABLE_OB_PARTITION_LOCATION_H_

#include "share/ob_define.h"
#include "lib/net/ob_addr.h"
#include "lib/container/ob_se_array.h"
#include "lib/ob_replica_define.h"
#include "common/ob_role.h"
#include "share/ls/ob_ls_info.h"

namespace oceanbase
{
namespace sql
{
class ObOptTabletLoc;
}
namespace share
{

struct ObPidAddrPair {
public:
  int64_t pid_;
  common::ObAddr addr_;

  ObPidAddrPair(int64_t pid, common::ObAddr addr)
    : pid_(pid), addr_(addr)
  {
  }

  bool operator==(const ObPidAddrPair &other) const
  {
    return pid_ == other.pid_ && addr_ == other.addr_;
  }

  TO_STRING_KV(K_(pid), K_(addr));
};

class ObPartitionReplicaLocation;
struct ObReplicaLocation //TODO(xiumin): delete it
{
  OB_UNIS_VERSION(1);
public:
  common::ObAddr server_;
  common::ObRole role_;
  int64_t sql_port_;
  int64_t reserved_;
  common::ObReplicaType replica_type_;
  common::ObReplicaProperty property_; // memstore_percent is used

  ObReplicaLocation();
  void reset();
  inline bool is_valid() const;
  inline bool operator==(const ObReplicaLocation &other) const;
  inline bool operator!=(const ObReplicaLocation &other) const;
  bool is_leader_like() const { return common::is_leader_like(role_); }
  bool is_leader_by_election() const { return common::is_leader_by_election(role_); }
  bool is_strong_leader() const { return common::is_strong_leader(role_); }
  bool is_standby_leader() const { return common::is_standby_leader(role_); }
  bool is_follower() const { return common::is_follower(role_); }
  TO_STRING_KV(K_(server), K_(role), K_(sql_port), K_(replica_type), K_(reserved), K_(property));
};

inline bool ObReplicaLocation::is_valid() const
{
  return server_.is_valid();
  //TODO:
  //return server_.is_valid() && common::ObReplicaTypeCheck::is_replica_type_valid(replica_type_);
}

bool ObReplicaLocation::operator==(const ObReplicaLocation &other) const
{
  return server_ == other.server_ && role_ == other.role_
      && sql_port_ == other.sql_port_
      && replica_type_ == other.replica_type_
      && property_ == other.property_;
}

bool ObReplicaLocation::operator!=(const ObReplicaLocation &other) const
{
  return !(*this == other);
}

class ObPartitionLocation
{
  OB_UNIS_VERSION(1);
  friend class ObPartitionReplicaLocation;
  friend class sql::ObOptTabletLoc;
public:
  typedef common::ObSEArray<ObReplicaLocation, common::OB_DEFAULT_MEMBER_NUMBER> ObReplicaLocationArray;

  ObPartitionLocation();
  explicit ObPartitionLocation(common::ObIAllocator &allocator);
  virtual ~ObPartitionLocation();

  void reset();
  int assign(const ObPartitionLocation &partition_location);
  int assign_with_only_readable_replica(const ObPartitionLocation &partition_location);
  int assign(const ObPartitionReplicaLocation &partition_replica_location);

  bool is_valid() const;
  bool operator==(const ObPartitionLocation &other) const;
  int add(const ObReplicaLocation &replica_location);
  int add_with_no_check(const ObReplicaLocation &replica_location);
  int del(const common::ObAddr &server);
  int update(const common::ObAddr &server, ObReplicaLocation &replica_location);
  // return OB_LOCATION_LEADER_NOT_EXIST for leader not exist.
  int get_strong_leader(ObReplicaLocation &replica_location, int64_t &replica_idx) const;
  int get_strong_leader(ObReplicaLocation &replica_location) const;
  int get_restore_leader(ObReplicaLocation &replica_location) const;
  int get_leader_by_election(ObReplicaLocation &replica_location) const;
  // return OB_LOCATION_LEADER_NOT_EXIST for leader not exist.
  int check_strong_leader_exist() const;

  int64_t size() const { return replica_locations_.count(); }

  inline uint64_t get_table_id() const { return table_id_; }
  inline void set_table_id(const uint64_t table_id) { table_id_ = table_id; }

  inline int64_t get_partition_id() const { return partition_id_; }
  inline void set_partition_id(const int64_t partition_id) { partition_id_ = partition_id; }

  inline int64_t get_partition_cnt() const { return partition_cnt_; }
  inline void set_partition_cnt(const int64_t partition_cnt) { partition_cnt_ = partition_cnt; }

  inline int64_t get_renew_time() const { return renew_time_; }
  inline void set_renew_time(const int64_t renew_time) { renew_time_ = renew_time; }

  inline int64_t get_sql_renew_time() const { return sql_renew_time_; }
  inline void set_sql_renew_time(const int64_t sql_renew_time) { sql_renew_time_ = sql_renew_time; }

  inline const common::ObIArray<ObReplicaLocation> &get_replica_locations() const { return replica_locations_; }
  inline void mark_fail() { is_mark_fail_ = true; }
  inline void unmark_fail() { is_mark_fail_ = false; }
  inline bool is_mark_fail() const { return is_mark_fail_; }
  int change_leader(const ObReplicaLocation &new_leader);

  static int alloc_new_location(common::ObIAllocator &allocator,
                                ObPartitionLocation *&new_location);
  TO_STRING_KV(KT_(table_id), K_(partition_id), K_(partition_cnt),
      K_(replica_locations), K_(renew_time), K_(sql_renew_time), K_(is_mark_fail));

private:
  // return OB_ENTRY_NOT_EXIST for not found.
  int find(const common::ObAddr &server, int64_t &idx) const;

private:
  uint64_t table_id_;
  int64_t partition_id_;
  int64_t partition_cnt_;
  ObReplicaLocationArray replica_locations_;
  int64_t renew_time_;     // renew time when location_cache is renewed successfully.
  int64_t sql_renew_time_; // renew time when location_cache is renewed successfully by SQL.
  bool is_mark_fail_;
};

class ObPartitionReplicaLocation final
{
  OB_UNIS_VERSION(1);
  friend class ObPartitionLocation;
public:
  static bool compare_part_loc_asc(const ObPartitionReplicaLocation &left,
                                   const ObPartitionReplicaLocation &right);
  static bool compare_part_loc_desc(const ObPartitionReplicaLocation &left,
                                    const ObPartitionReplicaLocation &right);
public:
  ObPartitionReplicaLocation();

  void reset();
  int assign(const ObPartitionReplicaLocation &partition_location);
  int assign(uint64_t table_id,
             int64_t partition_id,
             int64_t partition_cnt,
             const ObReplicaLocation &replica_location,
             int64_t renew_time);
  int assign_strong_leader(const ObPartitionLocation &partition_location);

  bool is_valid() const;
  bool operator==(const ObPartitionReplicaLocation &other) const;

  inline uint64_t get_table_id() const { return table_id_; }
  inline void set_table_id(const uint64_t table_id) { table_id_ = table_id; }

  inline int64_t get_partition_id() const { return partition_id_; }
  inline void set_partition_id(const int64_t partition_id) { partition_id_ = partition_id; }

  inline int64_t get_partition_cnt() const { return partition_cnt_; }
  inline void set_partition_cnt(const int64_t partition_cnt) { partition_cnt_ = partition_cnt; }

  inline int64_t get_renew_time() const { return renew_time_; }
  inline void set_renew_time(const int64_t renew_time) { renew_time_ = renew_time; }

  inline const ObReplicaLocation &get_replica_location() const { return replica_location_; }
  inline void set_replica_location(const ObReplicaLocation &replica_location) { replica_location_ = replica_location; }

  TO_STRING_KV(KT_(table_id), K_(partition_id), K_(partition_cnt),
               K_(replica_location), K_(renew_time));

private:
  uint64_t table_id_;
  int64_t partition_id_;
  int64_t partition_cnt_;
  ObReplicaLocation replica_location_;
  int64_t renew_time_;
};

}//end namespace share
}//end namespace oceanbase

#endif
