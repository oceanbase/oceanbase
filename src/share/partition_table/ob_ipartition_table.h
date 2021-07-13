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

#ifndef OCEANBASE_PARTITION_TABLE_OB_IPARTITION_TABLE_H_
#define OCEANBASE_PARTITION_TABLE_OB_IPARTITION_TABLE_H_

#include <stdint.h>
#include "share/ob_define.h"
#include "lib/container/ob_array.h"
#include "lib/ob_replica_define.h"
#include "share/inner_table/ob_inner_table_schema.h"
#include "share/partition_table/ob_partition_info.h"
#include "share/ob_rpc_struct.h"

namespace oceanbase {
namespace common {
class ObAddr;
class ObPartitionKey;
}  // namespace common

namespace share {

class ObIMergeErrorCb {
public:
  virtual int submit_merge_error_task() = 0;
};

class ObPartitionReplica;
class ObPartitionInfo;

// Partition property getter, get partition member list
class ObIPartPropertyGetter {
public:
  ObIPartPropertyGetter(){};
  virtual ~ObIPartPropertyGetter()
  {}

  // get member list from partition leader
  virtual int get_leader_member(
      const common::ObPartitionKey& part_key, common::ObIArray<common::ObAddr>& member_list) = 0;
  virtual int get_member_list_and_leader(
      const common::ObPartitionKey& part_key, obrpc::ObMemberListAndLeaderArg& arg) = 0;
  virtual int get_role(const common::ObPartitionKey& part_key, common::ObRole& role) = 0;
  virtual int get_member_list_and_leader_v2(
      const common::ObPartitionKey& part_key, obrpc::ObGetMemberListAndLeaderResult& arg) = 0;
};

class ObIPartitionTable {
public:
  const static int64_t ALL_CORE_TABLE_PARTITION_ID = 0;
  const static int64_t ALL_CORE_TABLE_PARTITION_NUM = 1;
  // partition table levels: __all_tenant_meta_table, __all_root_table, __all_core_table, memory table
  const static int64_t PARTITION_TABLE_LEVELS = 5;
  // partition table report levels: memory table no need exclusive queue
  const static int64_t PARTITION_TABLE_REPORT_LEVELS = PARTITION_TABLE_LEVELS - 1;

  // FIXME : remove ALL_ROOT_TABLE_PARTITION_ID/NUM after merge master
  const static int64_t ALL_ROOT_TABLE_PARTITION_ID = 0;
  const static int64_t ALL_ROOT_TABLE_PARTITION_NUM = 1;

  const static int64_t REMOVE_LOG_REPLICA_SAFE_TIME = 7LL * 24LL * 3600LL * 1000LL * 1000LL;  // 1 week

  explicit ObIPartitionTable(ObIPartPropertyGetter& prop_getter) : prop_getter_(&prop_getter), merge_error_cb_(NULL){};
  virtual ~ObIPartitionTable()
  {}

  void set_merge_error_cb(ObIMergeErrorCb* cb)
  {
    merge_error_cb_ = cb;
  }

  virtual int get(const uint64_t table_id, const int64_t partition_id, ObPartitionInfo& partition_info,
      const bool need_fetch_faillist = false, const int64_t cluster_id = common::OB_INVALID_ID) = 0;

  virtual int batch_fetch_partition_infos(const common::ObIArray<common::ObPartitionKey>& keys,
      common::ObIAllocator& allocator, common::ObArray<ObPartitionInfo*>& partitions,
      const int64_t cluster_id = common::OB_INVALID_ID) = 0;
  virtual int batch_execute(const common::ObIArray<ObPartitionReplica>& replicas) = 0;
  virtual int batch_report_with_optimization(
      const common::ObIArray<ObPartitionReplica>& replicas, const bool with_role) = 0;
  virtual int batch_report_partition_role(
      const common::ObIArray<share::ObPartitionReplica>& pkey_array, const common::ObRole new_role) = 0;
  virtual int prefetch_by_table_id(const uint64_t tenant_id, const uint64_t table_id, const int64_t partition_id,
      common::ObIArray<ObPartitionInfo>& partition_infos, const bool need_fetch_faillist = false) = 0;

  virtual int prefetch(const uint64_t tenant_id, const uint64_t table_id, const int64_t partition_id,
      common::ObIArray<ObPartitionInfo>& partition_infos, bool ignore_row_checksum,
      const bool need_fetch_faillist = false) = 0;

  virtual int prefetch(const uint64_t pt_table_id, const int64_t pt_partition_id, const uint64_t table_id,
      const int64_t partition_id, common::ObIArray<ObPartitionInfo>& partition_infos,
      const bool need_fetch_faillist = false) = 0;

  // insert on duplicate update
  virtual int update(const ObPartitionReplica& replica) = 0;

  virtual int remove(const uint64_t table_id, const int64_t partition_id, const common::ObAddr& server) = 0;

  virtual int set_unit_id(
      const uint64_t table_id, const int64_t partition_id, const common::ObAddr& server, const uint64_t unit_id) = 0;

  virtual int set_original_leader(
      const uint64_t table_id, const int64_t partition_id, const bool is_original_leader) = 0;

  virtual int update_rebuild_flag(
      const uint64_t table_id, const int64_t partition_id, const common::ObAddr& server, const bool rebuild) = 0;

  virtual int update_fail_list(const uint64_t table_id, const int64_t partition_id, const common::ObAddr& server,
      const ObPartitionReplica::FailList& fail_list) = 0;

  virtual int handover_partition(
      const common::ObPGKey& pg_key, const common::ObAddr& src_addr, const common::ObAddr& dest_addr) = 0;

  virtual int replace_partition(
      const ObPartitionReplica& replica, const common::ObAddr& src_addr, const common::ObAddr& dest_addr) = 0;

  ObIPartPropertyGetter* get_prop_getter()
  {
    return prop_getter_;
  }

  static bool is_valid_key(const uint64_t table_id, const int64_t partition_id);
  // generate replica to log unit_id
  static int gen_flag_replica(const uint64_t table_id, const int64_t partition_id, const int64_t partition_cnt,
      const common::ObAddr& server, const common::ObZone& zone, const uint64_t unit_id,
      const common::ObReplicaType& type, const int64_t memstore_percent, const int64_t quorum,
      ObPartitionReplica& replica);

  static int check_need_update(const ObPartitionReplica& lhs, const ObPartitionReplica& rhs, bool& update);

  // simply infallible function, return bool directly. (same with is_sys_table() ...)
  static bool is_partition_table(const uint64_t pt_table_id)
  {
    return OB_ALL_VIRTUAL_CORE_META_TABLE_TID == common::extract_pure_id(pt_table_id) ||
           OB_ALL_CORE_TABLE_TID == common::extract_pure_id(pt_table_id) ||
           OB_ALL_ROOT_TABLE_TID == common::extract_pure_id(pt_table_id) ||
           OB_ALL_TENANT_META_TABLE_TID == common::extract_pure_id(pt_table_id);
  }

  static bool is_persistent_partition_table(const uint64_t pt_table_id)
  {
    return is_partition_table(pt_table_id) &&
           OB_ALL_VIRTUAL_CORE_META_TABLE_TID != common::extract_pure_id(pt_table_id);
  }

  static int get_partition_table_name(const uint64_t table_id, const char*& pt_name, uint64_t& sql_tenant_id);

  static uint64_t get_partition_table_id(const uint64_t tid);

  static int partition_table_id_to_name(const uint64_t pt_table_id, const char*& table_name);

protected:
  ObIPartPropertyGetter* prop_getter_;
  ObIMergeErrorCb* merge_error_cb_;
};

inline bool ObIPartitionTable::is_valid_key(const uint64_t table_id, const int64_t partition_id)
{
  bool valid = true;
  if (common::OB_INVALID_ID == table_id || common::OB_INVALID_ID == common::extract_tenant_id(table_id) ||
      common::OB_INVALID_ID == common::extract_pure_id(table_id) || partition_id < 0) {
    valid = false;
  } else if (common::combine_id(common::OB_SYS_TENANT_ID, OB_ALL_CORE_TABLE_TID) == table_id &&
             ALL_CORE_TABLE_PARTITION_ID != partition_id) {
    valid = false;
  }
  if (!valid) {
    SHARE_PT_LOG(WARN, "invalid partition key", KT(table_id), K(partition_id));
  }
  return valid;
}

inline int ObIPartitionTable::gen_flag_replica(const uint64_t table_id, const int64_t partition_id,
    const int64_t partition_cnt, const common::ObAddr& server, const common::ObZone& zone, const uint64_t unit_id,
    const common::ObReplicaType& replica_type, const int64_t memstore_percent, const int64_t quorum,
    ObPartitionReplica& replica)
{
  int ret = common::OB_SUCCESS;
  if (!is_valid_key(table_id, partition_id) || !server.is_valid() || zone.is_empty() ||
      common::OB_INVALID_ID == unit_id) {
    // no need to check quorum, when the quorum value is invalid, it means no need to check
    ret = common::OB_INVALID_ARGUMENT;
    SHARE_PT_LOG(WARN,
        "invalid argument",
        KT(table_id),
        K(partition_id),
        K(partition_cnt),
        K(server),
        K(zone),
        K(quorum),
        K(unit_id),
        K(ret),
        K(common::lbt()));
  } else {
    replica.reset();
    replica.table_id_ = table_id;
    replica.partition_id_ = partition_id;
    replica.partition_cnt_ = partition_cnt;
    replica.server_ = server;
    replica.zone_ = zone;
    replica.unit_id_ = unit_id;
    replica.data_version_ = -1;  // mark it log unit_id replica
    replica.replica_type_ = replica_type;
    if (OB_FAIL(replica.set_memstore_percent(memstore_percent))) {
      SHARE_PT_LOG(WARN, "fail to set memstore percent", K(ret));
    } else {
      replica.quorum_ = quorum;
      replica.status_ = REPLICA_STATUS_FLAG;
    }
  }
  return ret;
}

inline int ObIPartitionTable::check_need_update(
    const ObPartitionReplica& lhs, const ObPartitionReplica& rhs, bool& update)
{
  int ret = common::OB_SUCCESS;
  if (!lhs.is_valid() | !rhs.is_valid()) {
    ret = common::OB_INVALID_ARGUMENT;
    SHARE_PT_LOG(WARN, "invalid argument", K(ret), K(lhs), K(rhs));
  } else if (lhs.table_id_ != rhs.table_id_ || lhs.partition_id_ != rhs.partition_id_ || lhs.server_ != rhs.server_) {
    ret = common::OB_INVALID_ARGUMENT;
    SHARE_PT_LOG(WARN, "replica of different partition compared", K(ret), K(lhs), K(rhs));
  } else {
    bool diff = false;
    if (lhs.member_list_.count() != rhs.member_list_.count()) {
      diff = true;
    } else {
      for (int64_t i = 0; i < lhs.member_list_.count(); ++i) {
        if (lhs.member_list_.at(i) != rhs.member_list_.at(i)) {
          diff = true;
          break;
        }
      }
    }
    update = (diff || lhs.data_version_ != rhs.data_version_ || lhs.role_ != rhs.role_ ||
              lhs.partition_cnt_ != rhs.partition_cnt_ || lhs.to_leader_time_ != rhs.to_leader_time_ ||
              lhs.zone_ != rhs.zone_);
  }
  return ret;
}

inline int ObIPartitionTable::get_partition_table_name(
    const uint64_t table_id, const char*& pt_name, uint64_t& sql_tenant_id)
{
  int ret = common::OB_SUCCESS;
  pt_name = NULL;
  sql_tenant_id = common::OB_SYS_TENANT_ID;
  if (0 == common::extract_pure_id(table_id) || 0 == common::extract_tenant_id(table_id)) {
    ret = common::OB_INVALID_ARGUMENT;
    SHARE_PT_LOG(WARN, "invalid table_id", K(ret), KT(table_id));
  } else if (common::combine_id(common::OB_SYS_TENANT_ID, OB_ALL_CORE_TABLE_TID) == table_id) {
    pt_name = OB_ALL_VIRTUAL_CORE_META_TABLE_TNAME;
  } else if (common::combine_id(common::OB_SYS_TENANT_ID, OB_ALL_ROOT_TABLE_TID) == table_id) {
    pt_name = OB_ALL_CORE_TABLE_TNAME;
  } else if (common::is_sys_table(table_id)) {
    pt_name = OB_ALL_ROOT_TABLE_TNAME;
  } else {
    pt_name = OB_ALL_TENANT_META_TABLE_TNAME;
    sql_tenant_id = common::extract_tenant_id(table_id);
  }
  return ret;
}

// simply infallible function, return result directly.
inline uint64_t ObIPartitionTable::get_partition_table_id(const uint64_t tid)
{
  uint64_t partition_table_id = common::OB_INVALID_ID;
  if (common::combine_id(common::OB_SYS_TENANT_ID, OB_ALL_CORE_TABLE_TID) == tid) {
    partition_table_id = OB_ALL_VIRTUAL_CORE_META_TABLE_TID;
  } else if (common::combine_id(common::OB_SYS_TENANT_ID, OB_ALL_ROOT_TABLE_TID) == tid) {
    partition_table_id = OB_ALL_CORE_TABLE_TID;
  } else if (common::is_sys_table(tid)) {
    partition_table_id = OB_ALL_ROOT_TABLE_TID;
  } else {
    partition_table_id = OB_ALL_TENANT_META_TABLE_TID;
  }
  return partition_table_id;
}

inline int ObIPartitionTable::partition_table_id_to_name(const uint64_t pt_table_id, const char*& table_name)
{
  int ret = common::OB_SUCCESS;
  table_name = NULL;
  if (common::combine_id(common::OB_SYS_TENANT_ID, OB_ALL_VIRTUAL_CORE_META_TABLE_TID) == pt_table_id) {
    table_name = OB_ALL_VIRTUAL_CORE_META_TABLE_TNAME;
  } else if (common::combine_id(common::OB_SYS_TENANT_ID, OB_ALL_CORE_TABLE_TID) == pt_table_id) {
    table_name = OB_ALL_CORE_TABLE_TNAME;
  } else if (common::combine_id(common::OB_SYS_TENANT_ID, OB_ALL_ROOT_TABLE_TID) == pt_table_id) {
    table_name = OB_ALL_ROOT_TABLE_TNAME;
  } else if (OB_ALL_TENANT_META_TABLE_TID == common::extract_pure_id(pt_table_id)) {
    table_name = OB_ALL_TENANT_META_TABLE_TNAME;
  } else {
    ret = common::OB_INVALID_ARGUMENT;
    SHARE_PT_LOG(WARN, "invalid pt_table_id", K(ret), KT(pt_table_id));
  }
  return ret;
}

}  // end namespace share
}  // end namespace oceanbase

#endif  // OCEANBASE_PARTITION_TABLE_OB_IPARTITION_TABLE_H_
