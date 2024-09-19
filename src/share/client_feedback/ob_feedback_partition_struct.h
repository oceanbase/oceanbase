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

#ifndef OCEANBASE_SHARE_OB_FEEDBACK_PARTITION_STRUCT_H_
#define OCEANBASE_SHARE_OB_FEEDBACK_PARTITION_STRUCT_H_

#include "common/ob_role.h"
#include "lib/net/ob_addr.h"
#include "lib/container/ob_se_array.h"
#include "lib/ob_define.h"
#include "share/client_feedback/ob_client_feedback_basic.h"

namespace oceanbase
{
namespace share
{

class ObFeedbackReplicaLocation : public ObAbstractFeedbackObject<ObFeedbackReplicaLocation>
{
public:
  common::ObAddr server_;
  common::ObRole role_; // ip && sql_port
  common::ObReplicaType replica_type_;

  ObFeedbackReplicaLocation() : ObAbstractFeedbackObject<ObFeedbackReplicaLocation>(MIN_FB_ELE) { reset(); }
  virtual ~ObFeedbackReplicaLocation() {}

  bool operator==(const ObFeedbackReplicaLocation &other) const
  {
    return (server_ == other.server_)
           && (role_ == other.role_)
           && (replica_type_ == other.replica_type_);
  }

  bool operator!=(const ObFeedbackReplicaLocation &other) const
  {
    return !(*this == other);
  }

  void reset()
  {
    server_.reset();
    role_ = common::FOLLOWER;
    replica_type_ = common::REPLICA_TYPE_FULL;
  }

  TO_STRING_KV(K_(server), K_(role), K_(replica_type));
  FB_OBJ_DEFINE_METHOD;
};

inline bool ObFeedbackReplicaLocation::is_valid_obj() const
{
  return server_.is_valid()
         && (common::INVALID_ROLE != role_)
         && (common::REPLICA_TYPE_MAX != replica_type_);
}

class ObFeedbackPartitionLocation : public ObAbstractFeedbackObject<ObFeedbackPartitionLocation>
{
public:
  typedef common::ObSEArray<ObFeedbackReplicaLocation, 5> ObFeedbackReplicaLocationArray;

  ObFeedbackPartitionLocation() : ObAbstractFeedbackObject<ObFeedbackPartitionLocation>(PARTITION_LOCATION_FB_ELE)
  {
    reset();
  }
  virtual ~ObFeedbackPartitionLocation() {}

  void reset()
  {
    table_id_ = common::OB_INVALID_ID;
    partition_id_ = common::OB_INVALID_INDEX;
    schema_version_ = 0;
    replicas_.reset();
  }

  TO_STRING_KV("type", get_feedback_element_type_str(type_), KT_(table_id), K_(partition_id),
               K_(schema_version), K_(replicas));

  void set_table_id(const uint64_t table_id) { table_id_ = table_id; }
  uint64_t get_table_id() const { return table_id_; }

  void set_partition_id(const int64_t part_id) { partition_id_ = part_id; }
  int64_t get_partition_id() const { return partition_id_; }

  void set_schema_version(const int64_t schema_version) { schema_version_ = schema_version; }
  int64_t get_schema_version() const { return schema_version_; }

  const ObFeedbackReplicaLocationArray *get_replica_array() { return &replicas_; }
  int64_t get_replica_count() const { return replicas_.count(); }

  int add_replica(const ObFeedbackReplicaLocation &replica)
  {
    INIT_SUCC(ret);
    if (OB_FAIL(replicas_.push_back(replica))) {
      SHARE_LOG(ERROR, "fail to push back replica", K(replica), K(ret));
    }
    return ret;
  }

  bool operator==(const ObFeedbackPartitionLocation &other) const
  {
    bool equal = true;
    if (!is_valid_obj() || !other.is_valid_obj()) {
      equal = false;
      SHARE_LOG_RET(WARN, common::OB_INVALID_ARGUMENT, "invalid argument", "self", *this, K(other));
    } else if (type_ != other.type_) {
      equal = false;
    } else if (replicas_.count() != other.replicas_.count()) {
      equal = false;
    } else {
      for (int64_t i = 0; i < replicas_.count(); ++i) {
        if (replicas_.at(i) != other.replicas_.at(i)) {
          equal = false;
          break;
        }
      }
      equal = equal
              && (table_id_ == other.table_id_)
              && (partition_id_ == other.partition_id_)
              && (schema_version_ == other.schema_version_);
    }
    return equal;
  }

  bool operator!=(const ObFeedbackPartitionLocation &other) const
  {
    return !(*this == other);
  }

  FB_OBJ_DEFINE_METHOD;
protected:
  uint64_t table_id_;
  int64_t partition_id_;
  int64_t schema_version_; // table schema version
  ObFeedbackReplicaLocationArray replicas_;
};

// TODO: support a more compatible reroute framework.
// For example, reroute for session (without tbl_name_
// and other related information)
class ObFeedbackRerouteInfo: public ObAbstractFeedbackObject<ObFeedbackRerouteInfo>
{
public:
  ObFeedbackRerouteInfo():
    ObAbstractFeedbackObject<ObFeedbackRerouteInfo>(MIN_FB_ELE) { reset(); }

  virtual ~ObFeedbackRerouteInfo() { reset(); }

  void reset()
  {
    server_.reset();
    role_ = common::FOLLOWER;
    replica_type_ = common::REPLICA_TYPE_FULL;
    MEMSET(tbl_name_, 0, sizeof(tbl_name_));
    tbl_name_len_ = 0;
    tbl_schema_version_ = OB_INVALID_VERSION;
    for_session_reroute_ = false;
  }
  void assign(const ObFeedbackRerouteInfo &info);

  int set_tbl_name(const common::ObString &table_name);

  TO_STRING_KV(K_(server), K_(role), K_(replica_type),
               "FeedbackTableName", common::ObString(tbl_name_len_, tbl_name_),
               K_(tbl_schema_version));
  FB_OBJ_DEFINE_METHOD;
public:
  common::ObAddr server_;
  common::ObRole role_; // ip && sql_port
  common::ObReplicaType replica_type_;
  char tbl_name_[common::OB_MAX_TABLE_NAME_LENGTH]; // used to store table name in reroute feedback
  int64_t tbl_name_len_;
  int64_t tbl_schema_version_;
  bool for_session_reroute_;
};

inline bool ObFeedbackPartitionLocation::is_valid_obj() const
{
  return (common::OB_INVALID_ID != table_id_)
         && (common::OB_INVALID_INDEX != partition_id_);
}

inline bool ObFeedbackRerouteInfo::is_valid_obj() const
{
  return (for_session_reroute_ && server_.is_valid()) ||
         (!for_session_reroute_ && server_.is_valid()
         && (common::INVALID_ROLE != role_)
         && (common::REPLICA_TYPE_MAX != replica_type_)
         && tbl_name_len_ > 0
         && tbl_name_len_ <= common::OB_MAX_TABLE_NAME_LENGTH
         && OB_INVALID_VERSION != tbl_schema_version_);
}

} // end namespace share
} // end namespace oceanbase

#endif // OCEANBASE_SHARE_OB_FEEDBACK_PARTITION_STRUCT_H_
