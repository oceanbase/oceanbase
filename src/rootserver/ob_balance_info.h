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

#ifndef _OB_BALANCE_INFO_H
#define _OB_BALANCE_INFO_H 1
#include "lib/container/ob_array.h"
#include "lib/container/ob_se_array.h"
#include "lib/net/ob_addr.h"
#include "lib/hash/ob_hashmap.h"
#include "lib/hash/ob_refered_map.h"
#include "share/ob_lease_struct.h"
#include "lib/hash/ob_hashset.h"
#include "common/ob_unit_info.h"
#include "common/ob_partition_key.h"

#include "share/partition_table/ob_partition_info.h"
#include "share/partition_table/ob_partition_table_iterator.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_part_mgr_util.h"
#include "ob_rebalance_task_mgr.h"
#include "ob_root_utils.h"
#include "ob_balancer_interface.h"

namespace oceanbase {
namespace common {
class ObMySQLProxy;
}
namespace share {
class ObPartitionTableOperator;
class ObRemotePartitionTableOperator;
namespace schema {
class ObSchemaGetterGuard;
}
}  // namespace share
namespace rootserver {
class ObUnitManager;
class ObServerManager;
class ObRebalanceTaskMgr;
class ObZoneManager;
struct ObReplicaStat;

class ObDataSourceCandidateChecker {
public:
  ObDataSourceCandidateChecker(common::ObReplicaType type) : this_type_(type)
  {}
  inline bool is_candidate(common::ObReplicaType other_type) const
  {
    // TODO: Use a more refined way, such as this_type_ = F, you can have other_type = R
    return common::ObReplicaTypeCheck::can_as_data_source(this_type_, other_type);
  }

private:
  common::ObReplicaType this_type_;
};

class ObStatisticsCalculator {
public:
  ObStatisticsCalculator();

  void reset();
  int add_value(double v);

  double get_avg();
  double get_standard_deviation();
  int64_t get_count()
  {
    return values_.count();
  }

private:
  common::ObSEArray<double, 8> values_;
  double sum_;
};

struct ObResourceWeight {
  double cpu_weight_;
  double disk_weight_;
  double iops_weight_;
  double memory_weight_;

  ObResourceWeight() : cpu_weight_(0), disk_weight_(0), iops_weight_(0), memory_weight_(0)
  {}
  ~ObResourceWeight() = default;

  inline void reset()
  {
    cpu_weight_ = 0.0;
    disk_weight_ = 0.0;
    iops_weight_ = 0.0;
    memory_weight_ = 0.0;
  }

  inline double sum()
  {
    return cpu_weight_ + disk_weight_ + iops_weight_ + memory_weight_;
  }
  // For compatibility reasons, some weights need to be set to 0,
  // and weights need to be re-adjusted so that the sum is 1
  inline void normalize()
  {
    double s = sum();
    if (OB_LIKELY(s > 0)) {
      cpu_weight_ = cpu_weight_ / s;
      memory_weight_ = memory_weight_ / s;
      disk_weight_ = disk_weight_ / s;
      iops_weight_ = iops_weight_ / s;
    } else {
      cpu_weight_ = 0;
      memory_weight_ = 0;
      disk_weight_ = 0;
      iops_weight_ = 0;
    }
  }
  inline bool is_empty()
  {
    return std::abs(cpu_weight_) < common::OB_DOUBLE_EPSINON && std::abs(disk_weight_) < common::OB_DOUBLE_EPSINON &&
           std::abs(iops_weight_) < common::OB_DOUBLE_EPSINON && std::abs(memory_weight_) < common::OB_DOUBLE_EPSINON;
  }
  inline bool is_valid()
  {
    return (std::abs(sum() - 1.0) < common::OB_FLOAT_EPSINON);
  }

  TO_STRING_KV(K_(cpu_weight), K_(disk_weight), K_(iops_weight), K_(memory_weight));
};

const static int64_t MIN_REBALANCABLE_REPLICA_NUM = 3;
// @see ObReplicaStat and ObReplicaResourceUsage
struct LoadFactor {
public:
  LoadFactor()
  {
    reset();
  }
  void reset();

  void set_disk_used(int64_t disk_used)
  {
    disk_used_ = disk_used;
  }
  void set_resource_usage(ObReplicaStat& replica_stat);
  // for resource usage
  double get_cpu_usage() const;
  double get_disk_usage() const;
  double get_iops_usage() const;
  double get_memory_usage() const;
  double get_net_packet_usage() const;
  double get_net_throughput_usage() const;
  int64_t get_disk_used() const
  {
    return disk_used_;
  }

  double get_weighted_sum(ObResourceWeight& weights) const;
  LoadFactor& operator+=(const LoadFactor& o);
  LoadFactor& operator-=(const LoadFactor& o);

  // for capacity
  void set_disk_capacity(int64_t v)
  {
    disk_used_ = v;
  }
  void set_cpu_capacity(double v)
  {
    cpu_stime_rate_ = v;
  }
  void set_memory_capacity(int64_t v)
  {
    memtable_bytes_ = v;
  }
  void set_iops_capacity(double v)
  {
    sstable_read_rate_ = v;
  }
  double get_cpu_capacity() const
  {
    return cpu_stime_rate_;
  }
  double get_disk_capacity() const
  {
    return static_cast<double>(disk_used_);
  }
  double get_iops_capacity() const
  {
    return sstable_read_rate_;
  }
  double get_memory_capacity() const
  {
    return static_cast<double>(memtable_bytes_);
  }

  TO_STRING_KV(K_(disk_used), K_(sstable_read_rate), K_(sstable_read_bytes_rate), K_(sstable_write_rate),
      K_(sstable_write_bytes_rate), K_(log_write_rate), K_(log_write_bytes_rate), K_(memtable_bytes),
      K_(cpu_utime_rate), K_(cpu_stime_rate), K_(net_in_rate), K_(net_in_bytes_rate), K_(net_out_rate),
      K_(net_out_bytes_rate));

private:
  // DISK capacity
  int64_t disk_used_;
  // DISK IO
  double sstable_read_rate_;
  double sstable_read_bytes_rate_;
  double sstable_write_rate_;
  double sstable_write_bytes_rate_;
  double log_write_rate_;
  double log_write_bytes_rate_;
  // MEMORY
  int64_t memtable_bytes_;
  // CPU (not accurate)
  double cpu_utime_rate_;  // CPU time spent in user mode
  double cpu_stime_rate_;  // CPU time spent in kernel mode
  // NET (not accurate)
  double net_in_rate_;
  double net_in_bytes_rate_;
  double net_out_rate_;
  double net_out_bytes_rate_;
};

inline void LoadFactor::reset()
{
  disk_used_ = 0;
  sstable_read_rate_ = 0;
  sstable_read_bytes_rate_ = 0;
  sstable_write_rate_ = 0;
  sstable_write_bytes_rate_ = 0;
  log_write_rate_ = 0;
  log_write_bytes_rate_ = 0;
  memtable_bytes_ = 0;
  cpu_utime_rate_ = 0;
  cpu_stime_rate_ = 0;
  net_in_rate_ = 0;
  net_in_bytes_rate_ = 0;
  net_out_rate_ = 0;
  net_out_bytes_rate_ = 0;
}

inline LoadFactor& LoadFactor::operator+=(const LoadFactor& o)
{
  disk_used_ += o.disk_used_;
  sstable_read_rate_ += o.sstable_read_rate_;
  sstable_read_bytes_rate_ += o.sstable_read_bytes_rate_;
  sstable_write_rate_ += o.sstable_write_rate_;
  sstable_write_bytes_rate_ += o.sstable_write_bytes_rate_;
  log_write_rate_ += o.log_write_rate_;
  log_write_bytes_rate_ += o.log_write_bytes_rate_;
  memtable_bytes_ += o.memtable_bytes_;
  cpu_utime_rate_ += o.cpu_utime_rate_;
  cpu_stime_rate_ += o.cpu_stime_rate_;
  net_in_rate_ += o.net_in_rate_;
  net_in_bytes_rate_ += o.net_in_bytes_rate_;
  net_out_rate_ += o.net_out_rate_;
  net_out_bytes_rate_ += o.net_out_bytes_rate_;
  return *this;
}

inline LoadFactor& LoadFactor::operator-=(const LoadFactor& o)
{
  disk_used_ -= o.disk_used_;
  sstable_read_rate_ -= o.sstable_read_rate_;
  sstable_read_bytes_rate_ -= o.sstable_read_bytes_rate_;
  sstable_write_rate_ -= o.sstable_write_rate_;
  sstable_write_bytes_rate_ -= o.sstable_write_bytes_rate_;
  log_write_rate_ -= o.log_write_rate_;
  log_write_bytes_rate_ -= o.log_write_bytes_rate_;
  memtable_bytes_ -= o.memtable_bytes_;
  cpu_utime_rate_ -= o.cpu_utime_rate_;
  cpu_stime_rate_ -= o.cpu_stime_rate_;
  net_in_rate_ -= o.net_in_rate_;
  net_in_bytes_rate_ -= o.net_in_bytes_rate_;
  net_out_rate_ -= o.net_out_rate_;
  net_out_bytes_rate_ -= o.net_out_bytes_rate_;
  return *this;
}

inline double LoadFactor::get_cpu_usage() const
{
  return (cpu_stime_rate_ + cpu_utime_rate_) / 1000000;
}

inline double LoadFactor::get_disk_usage() const
{
  return static_cast<double>(disk_used_);
}

inline double LoadFactor::get_iops_usage() const
{
  return (sstable_write_rate_ + sstable_read_rate_ + log_write_rate_);
}

inline double LoadFactor::get_memory_usage() const
{
  return static_cast<double>(memtable_bytes_);
}

inline double LoadFactor::get_net_packet_usage() const
{
  return net_in_rate_ + net_out_rate_;
}

inline double LoadFactor::get_net_throughput_usage() const
{
  return net_in_bytes_rate_ + net_out_bytes_rate_;
}

inline double LoadFactor::get_weighted_sum(ObResourceWeight& weights) const
{
  return weights.cpu_weight_ * get_cpu_usage() + weights.memory_weight_ * get_memory_usage() +
         weights.disk_weight_ * get_disk_usage() + weights.iops_weight_ * get_iops_usage();
}

struct ServerStat {
  common::ObAddr server_;
  bool online_;             // has heartbeat with rs and in service
  bool alive_;              // has heartbeat with rs
  bool permanent_offline_;  // server permanent offline
  bool active_;             // is active (not in deleting or permanent offline status)
  bool blocked_;            // migrate in blocked
  bool stopped_;
  int64_t in_member_replica_cnt_;
  share::ObServerResourceInfo resource_info_;  // cpu,disk,mem
public:
  ServerStat()
      : server_(),
        online_(false),
        alive_(false),
        permanent_offline_(true),
        active_(false),
        blocked_(false),
        stopped_(false),
        in_member_replica_cnt_(0),
        resource_info_()
  {}

  void set_key(const common::ObAddr& server)
  {
    server_ = server;
  };
  const common::ObAddr& get_key() const
  {
    return server_;
  }
  bool is_permanent_offline() const
  {
    return permanent_offline_;
  }
  bool can_migrate_in() const;
  bool need_rebalance() const
  {
    return blocked_ || permanent_offline_;
  }
  bool can_migrate_out() const
  {
    return active_ && online_;
  }
  bool is_stopped() const
  {
    return stopped_;
  }
  TO_STRING_KV(K_(server), K_(online), K_(permanent_offline), K_(active), K_(blocked), K_(stopped),
      K_(in_member_replica_cnt), K_(resource_info));
};
typedef common::hash::ObReferedMap<common::ObAddr, ServerStat> ServerStatMap;

struct UnitStat {
  ServerStat* server_;

  bool in_pool_;            // in tenant resource pool (set to false if unit removed)
  LoadFactor load_factor_;  // resource usage
  LoadFactor capacity_;     // resource capacity
  double load_;             // the load
  // statistics for one tablegroup
  int64_t tg_pg_cnt_;  // partition group count for one tablegroup
  // replicas belong to the unit, but on different server of the unit.
  int64_t outside_replica_cnt_;
  int64_t inside_replica_cnt_;

  share::ObUnitInfo info_;
  // After deducting the load of gts, the service capacity coefficient that the unit can provide
  double capacity_ratio_;

public:
  UnitStat()
      : server_(NULL),
        in_pool_(false),
        load_factor_(),
        capacity_(),
        load_(0),
        tg_pg_cnt_(0),
        outside_replica_cnt_(0),
        inside_replica_cnt_(0),
        info_(),
        capacity_ratio_(0.0)
  {}
  int assign(const UnitStat& other);
  UnitStat& operator=(const UnitStat& that);
  void set_key(const uint64_t unit_id)
  {
    info_.unit_.unit_id_ = unit_id;
  }
  int set_capacity_ratio(const uint64_t tenant_id, const bool is_gts_service_unit);
  const uint64_t& get_key() const
  {
    return info_.unit_.unit_id_;
  }
  uint64_t get_unit_id()
  {
    return info_.unit_.unit_id_;
  }

  // simply infallible function, return result directly.

  double get_load() const
  {
    return load_;
  }
  double get_load_if_plus(ObResourceWeight& weights, const LoadFactor& load_factor) const
  {
    return get_load_if(weights, load_factor, true);
  }
  double get_load_if_minus(ObResourceWeight& weights, const LoadFactor& load_factor) const
  {
    return get_load_if(weights, load_factor, false);
  }
  double get_load_if(ObResourceWeight& weights, const LoadFactor& load_factor, const bool plus) const;
  double calc_load(ObResourceWeight& weights, const LoadFactor& load_factor) const;

  double get_cpu_usage() const
  {
    return load_factor_.get_cpu_usage();
  }
  double get_disk_usage() const
  {
    return load_factor_.get_disk_usage();
  }
  double get_iops_usage() const
  {
    return load_factor_.get_iops_usage();
  }
  double get_memory_usage() const
  {
    return load_factor_.get_memory_usage();
  }

  double get_cpu_limit() const;
  double get_disk_limit() const;
  double get_iops_limit() const;
  double get_memory_limit() const;

  double get_cpu_usage_rate() const;
  double get_disk_usage_rate() const;
  double get_iops_usage_rate() const;
  double get_memory_usage_rate() const;
  TO_STRING_KV(K_(in_pool), K_(load_factor), K_(capacity), K_(tg_pg_cnt), K_(outside_replica_cnt), K_(info));
  // private:
  //  DISALLOW_COPY_AND_ASSIGN(UnitStat);
};

inline double UnitStat::get_cpu_limit() const
{
  return capacity_.get_cpu_capacity();
}

inline double UnitStat::get_cpu_usage_rate() const
{
  return load_factor_.get_cpu_usage() / get_cpu_limit();
}

inline double UnitStat::get_disk_limit() const
{
  return capacity_.get_disk_capacity();
}

inline double UnitStat::get_disk_usage_rate() const
{
  return load_factor_.get_disk_usage() / get_disk_limit();
}

inline double UnitStat::get_iops_limit() const
{
  return capacity_.get_iops_capacity();
}

inline double UnitStat::get_iops_usage_rate() const
{
  return load_factor_.get_iops_usage() / get_iops_limit();
}

inline double UnitStat::get_memory_limit() const
{
  return capacity_.get_memory_capacity();
}

inline double UnitStat::get_memory_usage_rate() const
{
  return load_factor_.get_memory_usage() / get_memory_limit();
}

typedef common::hash::ObReferedMap<uint64_t, UnitStat> UnitStatMap;

struct Replica {
  friend class TenantBalanceStat;
  UnitStat* unit_;
  ServerStat* server_;
  common::ObRegion region_;
  common::ObZone zone_;
  int64_t member_time_us_;
  LoadFactor load_factor_;
  common::ObRole role_;
  share::ObReplicaStatus replica_status_;
  bool rebuild_;
  int64_t is_restore_;
  int64_t data_version_;
  common::ObReplicaType replica_type_;
  int64_t memstore_percent_;
  bool readonly_at_all_;
  int64_t modify_time_us_;
  int64_t quorum_;
  share::ObReplicaStatus additional_replica_status_;
  // Used to record whether the replica can be load balanced
  int64_t failmsg_start_pos_;
  int64_t failmsg_count_;

private:
  bool in_member_list_;  // in leader member list
  // Do not use this domain directly, please use is_in_service();
public:
  Replica()
      : unit_(NULL),
        server_(NULL),
        region_(),
        zone_(),
        member_time_us_(0),
        load_factor_(),
        role_(common::FOLLOWER),
        replica_status_(share::REPLICA_STATUS_NORMAL),
        rebuild_(false),
        is_restore_(share::REPLICA_NOT_RESTORE),
        data_version_(0),
        replica_type_(common::REPLICA_TYPE_MAX),
        memstore_percent_(100),
        readonly_at_all_(false),
        modify_time_us_(0),
        quorum_(OB_INVALID_COUNT),
        additional_replica_status_(share::REPLICA_STATUS_NORMAL),
        failmsg_start_pos_(-1),
        failmsg_count_(0),
        in_member_list_(false)
  {}
  bool operator==(const Replica& that) const;
  bool only_in_member_list() const
  {
    return share::REPLICA_STATUS_FLAG == replica_status_;
  }
  bool need_rebuild_only_in_member_list() const;
  bool is_valid_paxos_member() const
  {
    return in_member_list_ && !only_in_member_list();
  }
  void set_in_member_list(const bool in_member_list)
  {
    in_member_list_ = in_member_list;
  }
  bool is_in_member_list() const
  {
    return in_member_list_;
  }
  int64_t get_memstore_percent() const
  {
    return memstore_percent_;
  }
  bool is_in_service() const
  {
    return replica_status_ == share::REPLICA_STATUS_NORMAL;
    ////  && !OB_ISNULL(server_) && !server_->permanent_offline_;
  }
  uint64_t get_unit_id() const
  {
    return unit_->info_.unit_.unit_id_;
  }
  const ObAddr& get_unit_addr() const
  {
    return unit_->info_.unit_.server_;
  }
  bool is_in_unit() const
  {
    return (!OB_ISNULL(server_) && !OB_ISNULL(unit_) && server_->server_ == unit_->info_.unit_.server_);
  }
  bool need_skip_pg_balance() const
  {
    return readonly_at_all_;
  }

  bool is_in_blacklist(int64_t task_type, ObAddr dest_server, const TenantBalanceStat* tenant_stat) const;
  bool is_leader_like() const
  {
    return common::is_leader_like(role_);
  }
  bool is_leader_by_election() const
  {
    return common::is_leader_by_election(role_);
  }
  bool is_strong_leader() const
  {
    return common::is_strong_leader(role_);
  }
  bool is_restore_leader() const
  {
    return common::is_restore_leader(role_);
  }
  bool is_standby_leader() const
  {
    return common::is_standby_leader(role_);
  }
  bool is_follower() const
  {
    return common::is_follower(role_);
  }
  bool is_standby_restore() const
  {
    return share::REPLICA_RESTORE_STANDBY == is_restore_;
  }

  TO_STRING_KV(K_(region), K_(zone), K_(member_time_us), K_(role), K_(in_member_list), K_(rebuild), K_(data_version),
      K_(replica_type), K_(memstore_percent), K_(readonly_at_all), K_(modify_time_us), "replica_status",
      share::ob_replica_status_str(replica_status_), "server", (NULL != server_ ? server_->server_ : common::ObAddr()),
      "unit_id", (NULL != unit_ ? unit_->get_key() : -1), K_(quorum), K_(failmsg_start_pos), K_(failmsg_count));
};

struct ObReplicaTask {
  uint64_t tenant_id_;
  uint64_t table_id_;
  int64_t partition_id_;
  ObAddr src_;
  ObReplicaType replica_type_;
  int64_t memstore_percent_;
  ObZone zone_;
  ObRegion region_;
  ObAddr dst_;
  ObReplicaType dst_replica_type_;
  int64_t dst_memstore_percent_;
  ObRebalanceTaskType cmd_type_;
  const char* comment_;

public:
  ObReplicaTask();
  ~ObReplicaTask(){};
  void reset();
  bool is_valid();
  TO_STRING_KV(KT_(tenant_id), KT_(table_id), K_(partition_id), K_(src), K_(replica_type), K_(memstore_percent),
      K_(zone), K_(region), K_(dst), K_(dst_replica_type), K_(dst_memstore_percent), K_(cmd_type), "comment",
      NULL == comment_ ? "" : comment_);
};

struct Partition {
  friend class TenantBalanceStat;
  uint64_t table_id_;
  uint64_t tablegroup_id_;
  int64_t partition_id_;
  int64_t partition_idx_;  // same partition group has same partition_idx
  int64_t schema_partition_cnt_;
  // The number of partitions defined in the schema,
  // with the largest number of partitions being the primary table
  int64_t schema_replica_cnt_;
  int64_t schema_full_replica_cnt_;
  int64_t valid_member_cnt_;  // replica count which in leader's member list
  int64_t begin_;             // replica position begin. (%all_replica_ array index)
  int64_t end_;               // replica position end. (%all_replica_ array index)
  bool primary_;              // partition with the smallest table_id of partition group
  int64_t filter_logonly_count_;
  // Record how many logonly replicas of the logonly unit have been filtered
  int64_t all_pg_idx_;  // Used for inverted index all_pg
  bool has_flag_replica_;

private:
  int64_t quorum_;            // Paxos member quorum value recorded in clog
  bool has_leader_;           // at least one of the replicas role is LEADER
  bool can_balance_;          // Replicas that are split and not fully merged cannot be load balanced
  bool can_rereplicate_;      // Able to copy
  bool in_physical_restore_;  // all replica in physical restore status
public:
  Partition()
      : table_id_(common::OB_INVALID_ID),
        tablegroup_id_(common::OB_INVALID_ID),
        partition_id_(common::OB_INVALID_INDEX),
        partition_idx_(common::OB_INVALID_INDEX),
        schema_partition_cnt_(0),
        schema_replica_cnt_(0),
        schema_full_replica_cnt_(0),
        valid_member_cnt_(0),
        begin_(0),
        end_(0),
        primary_(false),
        filter_logonly_count_(0),
        all_pg_idx_(common::OB_INVALID_INDEX),
        has_flag_replica_(false),
        quorum_(OB_INVALID_COUNT),
        has_leader_(false),
        can_balance_(true),
        can_rereplicate_(true),
        in_physical_restore_(false)
  {}

  TO_STRING_KV(KT_(table_id), KT_(tablegroup_id), K_(partition_id), K_(partition_idx), K_(schema_partition_cnt),
      K_(schema_replica_cnt), K_(schema_full_replica_cnt), K_(valid_member_cnt), K_(begin), K_(end),
      K_(can_rereplicate), K_(primary), K_(quorum), K_(has_leader), K_(can_balance), K_(filter_logonly_count),
      K_(all_pg_idx), K_(has_flag_replica));

  // simply infallible function, return bool directly.
  bool is_primary() const
  {
    return primary_ || common::OB_INVALID_ID == tablegroup_id_;
  }
  bool is_leader_valid() const;
  bool can_balance() const;
  int64_t get_quorum() const
  {
    return quorum_;
  }
  bool can_rereplicate() const
  {
    return can_rereplicate_;
  }
  bool in_physical_restore() const
  {
    return in_physical_restore_;
  }
  bool is_valid_quorum() const;
  bool has_leader() const
  {
    return has_leader_;
  }
  bool is_in_spliting() const
  {
    return !can_balance_;
  }
  // Splitting, the source partition has no dumped partition
  // simply infallible function, return result directly
  LoadFactor get_max_load_factor(ObResourceWeight& weights, const common::ObArray<Replica>& all_replica);
  common::ObPartitionKey get_key() const;
  int64_t get_replica_count() const
  {
    return end_ - begin_;
  }
};

inline common::ObPartitionKey Partition::get_key() const
{
  common::ObPartitionKey key(table_id_, partition_id_, 0 /*partition_cnt unidied initialization to 0*/);
  return key;
}

typedef common::hash::ObHashMap<common::ObPartitionKey, int64_t, common::hash::NoPthreadDefendMode> PartitionMap;

struct PartitionGroup {
  uint64_t table_id_;  // minimum table id of the tablegroup
  uint64_t tablegroup_id_;
  int64_t partition_idx_;  // begin from 0 within each table group
  LoadFactor load_factor_;
  int64_t begin_;  // partition position begin. (%sorted_partition_ array index)
  int64_t end_;    // partition position end. (%sorted_partition_ array index)
  int64_t all_tg_idx_;
  // Used for inverted index all_tg_, Used for scenarios where table_id is known and its tablegroup is found
public:
  PartitionGroup()
      : table_id_(common::OB_INVALID_ID),
        tablegroup_id_(common::OB_INVALID_ID),
        partition_idx_(common::OB_INVALID_INDEX),
        load_factor_(),
        begin_(0),
        end_(0),
        all_tg_idx_(common::OB_INVALID_INDEX)
  {}

  TO_STRING_KV(
      KT_(table_id), KT_(tablegroup_id), K_(partition_idx), K_(load_factor), K_(begin), K_(end), K_(all_tg_idx));
};

struct TableGroup {
  uint64_t tablegroup_id_;
  int64_t begin_;  // partition group position begin. ($all_pg_ array index)
  int64_t end_;    // partition group position end. ($all_pg_ array index)
public:
  TableGroup() : tablegroup_id_(common::OB_INVALID_ID), begin_(0), end_(0)
  {}
  TO_STRING_KV(KT_(tablegroup_id), K_(begin), K_(end));
};

struct ZonePaxosInfo {
  common::ObZone zone_;
  int64_t full_replica_num_;
  int64_t logonly_replica_num_;
  TO_STRING_KV(K_(zone), K_(full_replica_num), K_(logonly_replica_num));
};

typedef common::ObArray<UnitStat*> UnitStatArray;
struct ZoneUnit {
  friend class TenantBalanceStat;
  common::ObZone zone_;
  int64_t active_unit_cnt_;
  // for load balance
  double load_imbalance_;  // imbalance metric
  double cpu_imbalance_;
  double disk_imbalance_;
  double iops_imbalance_;
  double memory_imbalance_;

  double load_avg_;
  double cpu_avg_;
  double disk_avg_;
  double iops_avg_;
  double memory_avg_;

  // Each zone has its own resource weight for Unit load balancing
  ObResourceWeight resource_weight_;
  LoadFactor ru_total_;
  LoadFactor ru_capacity_;

  // statistics for one tablegroup
  UnitStatArray all_unit_;

private:
  int64_t tg_pg_cnt_;  // Count the number of pg on the zone
public:
  ZoneUnit()
      : zone_(),
        active_unit_cnt_(0),
        load_imbalance_(0),
        cpu_imbalance_(0),
        disk_imbalance_(0),
        iops_imbalance_(0),
        memory_imbalance_(0),
        load_avg_(0),
        cpu_avg_(0),
        disk_avg_(0),
        iops_avg_(0),
        memory_avg_(0),
        resource_weight_(),
        ru_total_(),
        ru_capacity_(),
        all_unit_(),
        tg_pg_cnt_(0)
  {}
  int assign(const ZoneUnit& other);
  double get_avg_load() const
  {
    return load_avg_;
  }
  int64_t get_pg_count() const
  {
    return tg_pg_cnt_;
  }
  TO_STRING_KV(K_(zone), K_(active_unit_cnt), K_(load_avg), K_(load_imbalance), K_(tg_pg_cnt), K_(resource_weight),
      K_(ru_total), K_(ru_capacity), K_(all_unit));

private:
  DISALLOW_COPY_AND_ASSIGN(ZoneUnit);
};

class ServerReplicaCountMgr {
public:
  ServerReplicaCountMgr() : inited_(false), server_replica_counts_()
  {}
  virtual ~ServerReplicaCountMgr()
  {}
  int init(const common::ObIArray<common::ObAddr>& servers);
  int accumulate(const common::ObAddr& server, const int64_t cnt);
  int get_replica_count(const common::ObAddr& server, int64_t& replica_count);
  int64_t get_server_count() const
  {
    return server_replica_counts_.count();
  }

private:
  struct ServerReplicaCount {
    common::ObAddr server_;
    int64_t replica_count_;

    ServerReplicaCount() : replica_count_(0)
    {}
    void reset()
    {
      *this = ServerReplicaCount();
    }
    TO_STRING_KV(K_(server), K_(replica_count));
  };
  bool inited_;
  common::ObArray<ServerReplicaCount> server_replica_counts_;

  DISALLOW_COPY_AND_ASSIGN(ServerReplicaCountMgr);
};

struct ReadonlyInfo {
  int64_t table_id_;
  ObZone zone_;
  bool readonly_at_all_;
  ReadonlyInfo() : table_id_(OB_INVALID_ID), zone_(), readonly_at_all_(false)
  {}
  TO_STRING_KV(K_(table_id), K_(zone), K_(readonly_at_all));
};

struct ObSimpleSequenceGenerator {
  ObSimpleSequenceGenerator() : seq_(0)
  {}
  int64_t next_seq()
  {
    return seq_++;
  }

private:
  int64_t seq_;
};

struct ObPartitionGroupOrder {
  explicit ObPartitionGroupOrder(int& ret) : ret_(ret)
  {}
  bool operator()(const Partition* left, const Partition* right);

private:
  template <typename T>
  int compare(const T& left, const T& right)
  {
    return left < right ? -1 : (left == right ? 0 : 1);
  }

private:
  int& ret_;
};

/* Here is used for statistics and collection of partition info information for various load balancing.
 * After introducing the concept of pg physical physical partition,
 * the structure is still used to count the information of partition info,
 * but the meaning of the corresponding fields are extended to pg.
 *
 * 1 all_table_array Contains tg_id of binding tablegroup and table_id of standalone table
 * 2 readonly_info_ array, containing locality readonly information of binding tablegroup and standalone table
 * 3 The meaning of the all_tg_ array remains unchanged for the original standalone,
 *   For the binding tablegroup, the tablegroup contains only one binding tablegroup
 * 4 all_pg_ array, for binding tablegroup, the semantics are the same as the original semantics
 * 5 all_partition_ array, for binding tablegroup, only one partition is contained in a pg
 * 6 all_replica_ array, for binding tabelgroup, the semantics are the same as before
 */
struct TenantBalanceStat : public balancer::ITenantStatFinder {
  UnitStatMap unit_stat_map_;
  ServerStatMap server_stat_map_;

  common::ObArray<Replica> all_replica_;
  common::ObArray<Partition> all_partition_;
  common::ObArray<share::ObPartitionReplica::FailMsg> all_failmsg_;
  PartitionMap partition_map_;

  // order by tablegroup_id, partition_idx, schema_partition_cnt, table_id
  common::ObArray<Partition*> sorted_partition_;

  // all partition groups
  common::ObArray<PartitionGroup> all_pg_;
  // partition groups under a unit sorted by load from max to min
  common::ObArray<PartitionGroup> sorted_pg_;

  // all table groups
  common::ObArray<TableGroup> all_tg_;
  // all tables, used to obtain a table id list snapshot
  // Avoid later load balancing to get a different list of tables
  // The table seen before and after is inconsistent
  common::ObArray<uint64_t> all_table_;

  common::ObSEArray<ZoneUnit, common::MAX_ZONE_NUM> all_zone_unit_;
  uint64_t tenant_id_;
  share::schema::ObSchemaGetterGuard* schema_guard_;
  ServerReplicaCountMgr replica_count_mgr_;

  // for imbalance-metric
  LoadFactor ru_total_;
  LoadFactor ru_capacity_;
  ObResourceWeight resource_weight_;
  common::ObArray<ReadonlyInfo> readonly_info_;
  // Record the number of F and L replicas in each zone in the unit of zone
  common::ObArray<ZonePaxosInfo> all_zone_paxos_info_;
  // Only record the locality information of the tenant;
  // relying on tenants and tables are consistent features on paxos
  int64_t min_source_replica_version_;
  // The starting version number of the copy of the standby database, and the copy less than this version is not allowed
public:
  friend class ObRootBalancer;
  const static int64_t MAX_SERVER_CNT = 5000;
  const static int64_t MAX_UNIT_CNT = MAX_SERVER_CNT * 100;

  TenantBalanceStat();
  int init(ObUnitManager& unit_mgr, ObServerManager& server_mgr, share::ObPartitionTableOperator& pt_operator,
      share::ObRemotePartitionTableOperator& remote_pt_operator, ObZoneManager& zone_mgr, ObRebalanceTaskMgr& task_mgr,
      share::ObCheckStopProvider& check_stop_provider, common::ObMySQLProxy& sql_proxy,
      const bool filter_logonly_unit = false);
  void reuse();

  bool is_inited() const
  {
    return inited_;
  }
  // Valid:
  // 1. All statistics gathered
  // 2. All associated pointers are not NULL.
  // 3. All array indexes are valid
  bool is_valid() const
  {
    return valid_;
  }
  // gather all information
  int gather_stat(const uint64_t tenant_id, share::schema::ObSchemaGetterGuard* schema_guard,
      const balancer::HashIndexCollection& hash_index_collection);

  int gather_filter_stat(TenantBalanceStat& output_stat, const balancer::HashIndexCollection& hash_index_collection);
  bool has_replica_locality(const Replica& replica);
  int fill_paxos_info();
  int fill_filter_partitions(TenantBalanceStat& output_stat);
  // Extract load balancing information under a certain unit
  int gather_unit_balance_stat(ZoneUnit& zu, UnitStat& u);

  int gen_partition_key(const Partition& partition, common::ObPartitionKey& pkey) const;

  int check_valid_replica_exist_on_dest_zone(const Partition& partition, OnlineReplica& dst, bool& exist);
  int check_valid_replica_exist_on_dest_server(const Partition& partition, OnlineReplica& dest, bool& exist);
  int gen_partition_key(int64_t table_id, int64_t partition_id, ObPartitionKey& pkey) const;

  // return OB_ENTRY_NOT_EXIST for zone not exist.
  int locate_zone(const common::ObZone& zone, ZoneUnit*& zone_unit, const bool alloc_empty = false);
  int update_tg_pg_stat(const int64_t sorted_partition_index);
  int choose_data_source(const Partition& p, const common::ObReplicaMember& dest_member,
      const common::ObReplicaMember& src_member, bool is_rebuild, common::ObReplicaMember& data_source,
      int64_t& data_size, int64_t task_type, int64_t& cluster_id, const common::ObAddr* hint = NULL);

  static int choose_data_source(ObZoneManager* zone_mgr, ObServerManager* server_mgr,
      share::ObRemotePartitionTableOperator* remote_pt, ObRebalanceTaskMgr* task_mgr, const TenantBalanceStat* self,
      const common::ObIArray<Replica>& replica_infos, const Partition& p, const common::ObReplicaMember& dest_member,
      const common::ObReplicaMember& src_member, bool is_rebuild, common::ObReplicaMember& data_source,
      int64_t& data_size, int64_t task_type, int64_t& cluster_id, common::ObMySQLProxy& sql_proxy_,
      const int64_t min_source_replica_version = 0, const common::ObAddr* hint = NULL);

  static int get_remote_cluster_data_source(share::ObRemotePartitionTableOperator* remote_pt, const Partition& p,
      share::ObPartitionReplica& partition_replica, ObDataSourceCandidateChecker& type_checker,
      const int64_t min_source_replica_version = 0);
  bool has_replica(const Partition& p, const ObAddr& dest) const;
  virtual int get_schema_quorum_size(const Partition& p, int64_t& quorum) const;

  int construct_zone_region_list(common::ObIArray<share::schema::ObZoneRegion>& zone_region_list,
      const common::ObIArray<common::ObZone>& zone_list) const;
  int get_tenant_pool_zone_list(const uint64_t tenant_id, common::ObIArray<common::ObZone>& pool_zone_list) const;
  virtual int get_migrate_replica_quorum_size(const Partition& p, int64_t& quorum) const;
  virtual int get_add_replica_quorum_size(
      const Partition& p, const ObZone& zone, const ObReplicaType type, int64_t& quorum) const;
  virtual int get_transform_quorum_size(const Partition& p, const ObZone& zone, const ObReplicaType src_type,
      const ObReplicaType dst_type, int64_t& quorum) const;
  virtual int get_remove_replica_quorum_size(
      const Partition& p, const ObZone& zone, const ObReplicaType type, int64_t& quorum) const;
  // If there is a replica that the standby restore has not completed, the R replica is not allowed to be supplemented
  bool can_rereplicate_readonly(const Partition& p) const;
  // Determine whether the replica of src will be unowned if the membership changes
  // Four conditions:
  // (1) There is currently a master
  // (2) At least two F replica exist when migrating F
  // (3) The number of paxos replicas is greater than majority
  // (4) Not in a state where the split has not been dumped, unless the member is forcibly deleted
  bool has_leader_while_member_change(
      const Partition& p, const ObAddr& src, const bool ignore_is_in_spliting = false) const;
  virtual int get_full_replica_num(const Partition& p, int64_t& count) const;
  // assign replica with no unit_id or unit_id not in resource pool (unit remove).
  int unit_assign();
  // find dest unit for replicate
  // FIXME : add load factor to check capacity.
  int find_dest_unit_for_unit_assigned(const ObReplicaType& replica_type, ZoneUnit& zone_unit, UnitStat*& us,
      common::hash::ObHashSet<int64_t>& units_assigned);
  int find_dest_unit(ZoneUnit& zone_unit, UnitStat*& us);
  ServerReplicaCountMgr& get_replica_count_mgr()
  {
    return replica_count_mgr_;
  }
  int reuse_replica_count_mgr();
  void print_stat();
  ObSimpleSequenceGenerator& get_seq_generator()
  {
    return seq_generator_;
  }
  // return OB_CANCELED if stop, else return OB_SUCCESS
  int check_stop() const
  {
    return check_stop_provider_->check_stop();
  }
  int add_partition(const int64_t schema_partition_cnt, const int64_t schema_replica_cnt,
      const int64_t schema_full_replica_cnt, const share::ObPartitionInfo& info, const uint64_t tablegroup_id,
      const int64_t partition_idx, const bool can_do_rereplicate,
      const share::schema::ObPartitionSchema* partition_schema);
  int add_replica(const share::ObPartitionReplica& r);
  int try_add_only_in_member_list_replica(const share::ObPartitionReplica*& leader_replica,
      const share::ObPartitionInfo& partition_info, bool& has_flag_replica);
  int get_flag_member_array(const share::ObPartitionReplica* leader_replica,
      const share::ObPartitionInfo& partition_info, common::ObIArray<share::ObPartitionReplica::Member>& flag_members);
  int add_flag_replica(const share::ObPartitionReplica::Member& member, const common::ObReplicaType replica_type,
      const common::ObZone& zone);
  int add_replica(Replica& r, const uint64_t unit_id, const common::ObAddr& server);

  int estimate_part_and_replica_cnt(const common::ObIArray<const share::schema::ObSimpleTableSchemaV2*>& table_schemas,
      const common::ObIArray<const share::schema::ObTablegroupSchema*>& tg_schemas, int64_t& part_cnt,
      int64_t& replica_num);
  template <typename SCHEMA>
  int fill_partition_entity(const uint64_t schema_id, const SCHEMA& schema);
  int fill_standalone_table_partitions(
      const common::ObIArray<const share::schema::ObSimpleTableSchemaV2*>& table_schemas);
  int fill_binding_tablegroup_partitions(const common::ObIArray<const share::schema::ObTablegroupSchema*>& tg_schemas);
  int fill_partitions();
  int fill_servers();
  int fill_units();
  int fill_sorted_partitions();
  int update_partition_statistics();
  int calc_resource_weight();
  int calc_resource_weight(
      const LoadFactor& ru_usage, const LoadFactor& ru_capacity, ObResourceWeight& resource_weight);
  int calc_load();
  int fill_partition_groups();  // for ObBalanceReplica
  int fill_tablegroups();       // for ObBalanceReplica
  int fill_flag_replicas(const balancer::HashIndexCollection& hash_index_collection);

  int get_partition_locality(const Partition& partition, const balancer::HashIndexCollection& hash_index_collection,
      common::ObIArray<share::ObZoneReplicaAttrSet>& zone_locality);

  int do_fill_partition_flag_replicas(
      const Partition& partition, common::ObIArray<share::ObZoneReplicaAttrSet>& zone_locality);

  int add_readonly_info(const int64_t& table_id, const common::ObZone& zone, const bool& readonly_at_all);
  int get_readonly_at_all_info(const int64_t& table_id, const common::ObZone& zone, bool& readonly_at_all);
  template <typename SCHEMA>
  int build_readonly_at_all_info(const uint64_t schema_id, const SCHEMA& schema);
  int check_valid();
  void print_unit_load();
  void print_replica_load();

  /* begin ITenantStatFinder impl. */

  virtual inline uint64_t get_tenant_id() const override
  {
    return tenant_id_;
  }

  virtual int get_all_pg_idx(
      const common::ObPartitionKey& pkey, const int64_t all_tg_idx, int64_t& all_pg_idx) override;

  virtual int get_all_tg_idx(uint64_t tablegroup_id, uint64_t table_id, int64_t& all_tg_idx) override;

  virtual int get_primary_partition_unit(
      const common::ObZone& zone, const int64_t all_tg_idx, const int64_t part_idx, uint64_t& unit_id) override;

  virtual int get_partition_entity_ids_by_tg_idx(
      const int64_t tablegroup_idx, common::ObIArray<uint64_t>& tids) override;

  virtual int get_partition_group_data_size(
      const common::ObZone& zone, const int64_t all_tg_idx, const int64_t part_idx, int64_t& data_size) override;

  virtual int get_gts_switch(bool& on) override;
  virtual int get_primary_partition_key(const int64_t all_pg_idx, common::ObPartitionKey& pkey) override;
  /* end ITenantStatFinder impl. */

  int get_leader_and_member_list(const Partition& partition, const Replica*& leader, ObMemberList& member_list);
  int get_leader_addr(const Partition& partition, common::ObAddr& leader, bool& valid_leader);

  int check_table_locality_changed(
      share::schema::ObSchemaGetterGuard& guard, const uint64_t table_id, bool& locality_changed) const;
  int check_table_schema_changed(
      share::schema::ObSchemaGetterGuard& guard, const uint64_t table_id, bool& schema_changed) const;

private:
  int set_unit_capacity_ratio(UnitStat& unit_stat, Partition& gts_service_partition);
  int sort_unit_pg_by_load(ZoneUnit& zu, UnitStat& u);
  int fill_tables(const common::ObIArray<const share::schema::ObSimpleTableSchemaV2*>& tables,
      const common::ObIArray<const share::schema::ObTablegroupSchema*>& tablegroups);

private:
  int try_assign_unit_on_random_server(
      const common::ObArray<Partition*>::iterator& partition, common::hash::ObHashSet<int64_t>& units_assigned);
  int try_assign_unit_by_ppr(const common::ObArray<Partition*>::iterator& partition, Partition*& primary_partition,
      common::hash::ObHashSet<int64_t>& units_assigned);
  int try_assign_unit_on_same_server(
      const common::ObArray<Partition*>::iterator& partition, common::hash::ObHashSet<int64_t>& units_assigned);
  int calc_unit_load(UnitStat*& us);
  void update_last_run_timestamp();  // save current time for thread checker.
private:
  bool inited_;
  bool valid_;
  ObUnitManager* unit_mgr_;
  ObServerManager* server_mgr_;
  share::ObPartitionTableOperator* pt_operator_;
  share::ObRemotePartitionTableOperator* remote_pt_operator_;
  ObRebalanceTaskMgr* task_mgr_;
  ObZoneManager* zone_mgr_;
  share::ObCheckStopProvider* check_stop_provider_;
  ObSimpleSequenceGenerator seq_generator_;
  common::ObMySQLProxy* sql_proxy_;
  bool filter_logonly_unit_;
  int64_t gts_partition_pos_;
  DISALLOW_COPY_AND_ASSIGN(TenantBalanceStat);
};

template <typename SCHEMA>
int TenantBalanceStat::fill_partition_entity(const uint64_t schema_id, const SCHEMA& schema)
{
  int ret = common::OB_SUCCESS;
  readonly_info_.reset();
  int64_t schema_partition_cnt = 0;
  int64_t schema_full_replica_cnt = 0;
  int64_t paxos_replica_num = 0;
  share::ObTablePartitionIterator iter;
  const bool need_fetch_faillist = true;
  iter.set_need_fetch_faillist(need_fetch_faillist);
  if (OB_FAIL(check_stop())) {
    RS_LOG(WARN, "balancer stop", K(ret));
  } else if (OB_FAIL(iter.init(schema_id, *schema_guard_, *pt_operator_))) {
    RS_LOG(WARN, "table partition iterator init failed", K(ret));
  } else if (FALSE_IT(schema_partition_cnt = schema.get_all_part_num())) {
    RS_LOG(WARN, "fail to get all part num", K(ret), K(schema_id));
  } else if (OB_FAIL(schema.get_full_replica_num(*schema_guard_, schema_full_replica_cnt))) {
    RS_LOG(WARN, "fail to get full replica num", K(ret), K(schema_id));
  } else if (OB_FAIL(schema.get_paxos_replica_num(*schema_guard_, paxos_replica_num))) {
    RS_LOG(WARN, "fail to get paxos replica num", K(ret), K(schema_id));
  } else if (OB_FAIL(build_readonly_at_all_info(schema_id, schema))) {
    RS_LOG(WARN, "fail to build readonly at all info", K(ret), K(schema_id));
  } else {
    bool can_do_rereplicate =
        !schema.is_global_index_table() || (schema.is_global_index_table() && schema.can_read_index()) ||
        (schema.is_global_index_table() && schema.is_mock_global_index_invalid() && !schema.can_read_index());
    share::ObPartitionInfo info;
    int64_t partition_idx = OB_INVALID_INDEX;
    bool check_dropped_partition = true;
    while (OB_SUCC(ret) && OB_SUCC(iter.next(info))) {
      update_last_run_timestamp();  // save current time for thread checker.
      if (OB_FAIL(check_stop())) {
        RS_LOG(WARN, "balancer stop", K(ret));
      } else if (OB_FAIL(share::schema::ObPartMgrUtils::get_partition_idx_by_id(
                     schema, check_dropped_partition, info.get_partition_id(), partition_idx))) {
        RS_LOG(WARN, "get_partition_idx_by_id failed", K(info), K(ret));
      } else if (OB_INVALID_INDEX == partition_idx) {
        RS_LOG(WARN, "invalid partition. ignore", K(info), K(ret));
      } else if (OB_FAIL(add_partition(schema_partition_cnt,
                     paxos_replica_num,
                     schema_full_replica_cnt,
                     info,
                     schema.get_tablegroup_id(),
                     partition_idx,
                     can_do_rereplicate,
                     &schema))) {
        RS_LOG(WARN, "add partition failed", K(ret), K(info));
      }
      info.reuse();
    }
    if (common::OB_ITER_END == ret) {
      ret = common::OB_SUCCESS;
    } else {
      RS_LOG(WARN, "iterator table partition failed", K(ret), K(iter));
    }
  }
  return ret;
}

template <typename SCHEMA>
int TenantBalanceStat::build_readonly_at_all_info(const uint64_t schema_id, const SCHEMA& schema)
{
  int ret = common::OB_SUCCESS;
  common::ObArray<share::ObZoneInfo> zone_infos;
  if (OB_UNLIKELY(!inited_)) {
    ret = common::OB_NOT_INIT;
    RS_LOG(WARN, "not init", K(ret));
  } else if (OB_FAIL(zone_mgr_->get_zone(zone_infos))) {
    RS_LOG(WARN, "fail to get zone", K(ret), K(schema_id));
  } else if (OB_UNLIKELY(common::OB_INVALID_ID == schema_id)) {
    ret = common::OB_INVALID_ARGUMENT;
    RS_LOG(WARN, "invalid argument", K(ret), K(schema_id));
  } else {
    bool readonly_at_all = false;
    common::ObRegion region;
    for (int64_t i = 0; i < zone_infos.count(); i++) {
      if (OB_FAIL(zone_infos.at(i).get_region(region))) {
        RS_LOG(WARN, "fail to get region", K(ret), K(i));
      } else if (OB_FAIL(schema.check_is_readonly_at_all(
                     *schema_guard_, zone_infos.at(i).zone_, region, readonly_at_all))) {
        RS_LOG(WARN, "fail to check is readonly at all", K(ret), K(zone_infos), K(i));
      } else if (OB_FAIL(add_readonly_info(schema_id, zone_infos.at(i).zone_, readonly_at_all))) {
        RS_LOG(WARN, "fail to push back", K(ret), "zone", zone_infos.at(i).zone_);
      }
    }
  }
  return ret;
}

class ObCanMigratePartitionChecker {
public:
  static int can_migrate_out(ObZoneManager& zone_mgr, UnitStat& src_unit, bool& need_balance);
};

namespace balancer {
const static char* const MIGRATE_TEMPORY_OFFLINE_REPLICA = "migrate temporary offline replica";
const static char* const MIGRATE_PERMANENT_OFFLINE_REPLICA = "migrate permanent offline replica";
const static char* const MIGRATE_TO_NEW_SERVER = "migrate to new server";
const static char* const MIGRATE_PG_REPLICA = "migrate pg replica";
const static char* const REPLICATE_PERMANENT_OFFLINE_REPLICA = "replicate permanent offline replica";
const static char* const MIGRATE_ONLY_IN_MEMBERLIST_REPLICA = "migrate only in memberlist replica";
const static char* const REBUILD_ONLY_IN_MEMBERLIST_REPLICA = "add replica only in memberlist replica";
const static char* const MANUAL_MIGRATE_UNIT = "migrete replica due to admin migrate unit";
const static char* const REPLICATE_ENOUGH_REPLICA = "replicate enough replica";
const static char* const REPLICATE_ENOUGH_SPECIFIC_REPLICA = "replicate enough logonly replica on logonly unit";
const static char* const FAST_RECOVERY_TASK = "fast recovery task";
const static char* const ADMIN_MIGRATE_REPLICA = "admin migrate replica";
const static char* const ADMIN_ADD_REPLICA = "admin add replica";
const static char* const ADMIN_REMOVE_MEMBER = "admin remove member";
const static char* const ADMIN_REMOVE_REPLICA = "admin remove non_paxos replica";
const static char* const ADMIN_TYPE_TRANSFORM = "admin change replica type";
const static char* const REBUILD_EMERGENCY_REPLICA = "rebuild emergency replica";
const static char* const REBUILD_REPLICA = "rebuild replica";
const static char* const RESTORE_REPLICA = "restore replica";
const static char* const PHYSICAL_RESTORE_REPLICA = "physical restore replica";
const static char* const RESTORE_FOLLOWER_REPLICA = "restore follower replica using copy sstable";
const static char* const REBUILD_REPLICA_AS_RESTART = "rebuild replica as server restart";
const static char* const ADD_REPLICA_OVER = "add replica over";
const static char* const MIGRATE_REPLICA_OVER = "migrate replica over";
const static char* const TEMPORARY_OFFLINE_REPLICA_ONLINE =
    "temporary offline replica online, add to paxos member list";
const static char* const REMOVE_TEMPORY_OFFLINE_REPLICA = "remove temporay offline pasox replica";
const static char* const REMOVE_PERMANENT_OFFLINE_REPLICA = "remove permanent offline replica";
const static char* const REMOVE_ONLY_IN_MEMBER_LIST_REPLICA = "remove only in member list replica";
const static char* const PG_COORDINATE_MIGRATE = "migrate replica for partition group coordinate";
const static char* const PG_COORDINATE_REMOVE_REPLICA = "remove replica for partition group coordinate";
const static char* const PG_COORDINATE_TYPE_TRANSFORM = "type transform for partition group coordinate";
const static char* const UNIT_PG_BALANCE = "migrate pg for unit load balance";
const static char* const TG_PG_BALANCE = "migrate pg for partition group balance";
const static char* const TG_PG_SHARD_BALANCE = "migrate pg for sharding table partition balance";
const static char* const TG_PG_SHARD_PARTITION_BALANCE = "migrate pg for same range sharding table partition balance";
const static char* const TG_PG_GROUP_BALANCE = "migrate pg for grouped partition table balance";
const static char* const TG_NON_PARTITION_TABLE_BALANCE = "migrate partition for non partition table balance";
const static char* const TG_PG_TABLE_BALANCE = "migrate pg for ungrouped partition table balance";
const static char* const LOCALITY_TYPE_TRANSFORM = "type transform according to locality";
const static char* const LOCALITY_MODIFY_QUORUM = "modify quorum according to locality";
const static char* const LOCALITY_REMOVE_REDUNDANT_MEMBER = "remove redundant member according to locality";
const static char* const LOCALITY_REMOVE_REDUNDANT_REPLICA = "remove redundant non_paxos replica according to locality";
const static char* const REMOVE_MIGRATE_UNIT_BLOCK_REPLICA =
    "remove non_paxos replica for blocked migration destination";
const static char* const REMOVE_MIGRATE_UNIT_BLOCK_MEMBER = "remove member for blocked migration destination";
const static char* const REMOVE_NOT_IN_POOL_NON_PAXOS_REPLICA = "remove not in pool non_paxos replica";
const static char* const TRANSFER_POSITION_FOR_NOT_IN_POOL_PAXOS_REPLICA =
    "transfer position for not in pool paxos replica";
const static char* const COPY_GLOBAL_INDEX_SSTABLE = "copy global index sstable";
const static char* const COPY_LOCAL_INDEX_SSTABLE = "copy local index sstable";
const static char* const SQL_BACKGROUND_DIST_TASK_COMMENT = "background executing sql distributed task";
const static char* const SINGLE_ZONE_STOP_SERVER = "single zone multi replica stop server";
const static char* const STANDBY_CUT_DATA_TASK = "standby cut data task";
};  // namespace balancer

}  // end namespace rootserver
}  // end namespace oceanbase

#endif /* _OB_BALANCE_INFO_H */
