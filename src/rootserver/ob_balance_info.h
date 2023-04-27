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
#include "share/unit/ob_unit_info.h"

#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_part_mgr_util.h"
#include "ob_root_utils.h"

namespace oceanbase
{
namespace common
{
class ObMySQLProxy;
}
namespace share
{
namespace schema
{
class ObSchemaGetterGuard;
}
}
namespace rootserver
{
class ObUnitManager;

class ObZoneManager;

class ObDataSourceCandidateChecker
{
public:
  ObDataSourceCandidateChecker(common::ObReplicaType type) : this_type_(type) {}
  inline bool is_candidate(common::ObReplicaType other_type) const
  {
    // TODO: Use a more refined way, such as this_type_ = F, you can have other_type = R
    return common::ObReplicaTypeCheck::can_as_data_source(this_type_, other_type);
  }
private:
  common::ObReplicaType this_type_;
};

class ObStatisticsCalculator
{
public:
  ObStatisticsCalculator();

  void reset();
  int add_value(double v);

  double get_avg();
  double get_standard_deviation();
  int64_t get_count() { return values_.count(); }
private:
  common::ObSEArray<double, 8> values_;
  double sum_;
};

struct ObResourceWeight
{
  double cpu_weight_;
  double disk_weight_;
  double iops_weight_;
  double memory_weight_;

  ObResourceWeight() : cpu_weight_(0),disk_weight_(0),iops_weight_(0),memory_weight_(0) {}
  ~ObResourceWeight() = default;

  inline void reset() {
    cpu_weight_     = 0.0;
    disk_weight_    = 0.0;
    iops_weight_    = 0.0;
    memory_weight_  = 0.0;
  }

  inline double sum() {
    return cpu_weight_ + disk_weight_ + iops_weight_ + memory_weight_;
  }
  // For compatibility reasons, some weights need to be set to 0,
  // and weights need to be re-adjusted so that the sum is 1
  inline void normalize() {
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
  inline bool is_empty() {
    return std::abs(cpu_weight_) < common::OB_DOUBLE_EPSINON
        && std::abs(disk_weight_) < common::OB_DOUBLE_EPSINON
        && std::abs(iops_weight_) < common::OB_DOUBLE_EPSINON
        && std::abs(memory_weight_)  < common::OB_DOUBLE_EPSINON;
  }
  inline bool is_valid() {
    return (std::abs(sum() - 1.0) < common::OB_FLOAT_EPSINON);
  }

  TO_STRING_KV(K_(cpu_weight),
               K_(disk_weight),
               K_(iops_weight),
               K_(memory_weight));
};

const static int64_t MIN_REBALANCABLE_REPLICA_NUM = 3;
struct LoadFactor
{
public:
  LoadFactor() { reset(); }
  void reset();

  void set_disk_used(int64_t disk_used) { disk_used_ = disk_used; }
  // for resource usage
  double get_cpu_usage() const;
  double get_disk_usage() const;
  double get_iops_usage() const;
  double get_memory_usage() const;
  double get_net_packet_usage() const;
  double get_net_throughput_usage() const;
  int64_t get_disk_used() const { return disk_used_; }

  double get_weighted_sum(ObResourceWeight &weights) const;
  LoadFactor &operator+=(const LoadFactor &o);
  LoadFactor &operator-=(const LoadFactor &o);

  // for capacity
  void set_disk_capacity(int64_t v) { disk_used_ = v; }
  void set_cpu_capacity(double v) { cpu_stime_rate_ = v; }
  void set_memory_capacity(int64_t v) { memtable_bytes_ = v; }
  void set_iops_capacity(double v) { sstable_read_rate_ = v; }
  double get_cpu_capacity() const { return cpu_stime_rate_; }
  double get_disk_capacity() const { return static_cast<double>(disk_used_); }
  double get_iops_capacity() const { return sstable_read_rate_; }
  double get_memory_capacity() const { return static_cast<double>(memtable_bytes_); }

  TO_STRING_KV(K_(disk_used),
               K_(sstable_read_rate),
               K_(sstable_read_bytes_rate),
               K_(sstable_write_rate),
               K_(sstable_write_bytes_rate),
               K_(log_write_rate),
               K_(log_write_bytes_rate),
               K_(memtable_bytes),
               K_(cpu_utime_rate),
               K_(cpu_stime_rate),
               K_(net_in_rate),
               K_(net_in_bytes_rate),
               K_(net_out_rate),
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

inline LoadFactor &LoadFactor::operator+=(const LoadFactor &o)
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

inline LoadFactor &LoadFactor::operator-=(const LoadFactor &o)
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
  return (cpu_stime_rate_+cpu_utime_rate_)/1000000;
}

inline double LoadFactor::get_disk_usage() const
{
  return static_cast<double>(disk_used_);
}

inline double LoadFactor::get_iops_usage() const
{
  return (sstable_write_rate_+sstable_read_rate_+log_write_rate_);
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

inline double LoadFactor::get_weighted_sum(ObResourceWeight &weights) const
{
  return weights.cpu_weight_ * get_cpu_usage()
      + weights.memory_weight_ * get_memory_usage()
      + weights.disk_weight_ * get_disk_usage()
      + weights.iops_weight_ * get_iops_usage();
}

struct ServerStat
{
  common::ObAddr server_;
  bool online_; // has heartbeat with rs and in service
  bool alive_;  // has heartbeat with rs
  bool permanent_offline_; // server permanent offline
  bool active_; // is active (not in deleting or permanent offline status)
  bool blocked_; // migrate in blocked
  bool stopped_;
  bool taken_over_by_rs_;
  int64_t in_member_replica_cnt_;
  share::ObServerResourceInfo resource_info_; // cpu,disk,mem
public:
  ServerStat()
      : server_(), online_(false), alive_(false),
        permanent_offline_(true),
        active_(false), blocked_(false), stopped_(false),
        taken_over_by_rs_(false),
        in_member_replica_cnt_(0), resource_info_() {}

  void set_key(const common::ObAddr &server) { server_ = server;};
  const common::ObAddr &get_key() const { return server_; }
  bool is_permanent_offline() const { return permanent_offline_; }
  bool can_migrate_in() const;
  bool need_rebalance() const { return blocked_ || permanent_offline_; }
  bool can_migrate_out() const { return active_ && online_; }
  bool is_stopped() const { return stopped_; }
  bool taken_over_by_rs() const { return taken_over_by_rs_; }
  TO_STRING_KV(K_(server), K_(online), K_(permanent_offline),
               K_(active), K_(blocked), K_(stopped), K_(taken_over_by_rs),
               K_(in_member_replica_cnt), K_(resource_info));
};
typedef common::hash::ObReferedMap<common::ObAddr, ServerStat> ServerStatMap;

struct UnitStat
{
  ServerStat *server_;

  bool in_pool_; // in tenant resource pool (set to false if unit removed)
  LoadFactor load_factor_;  // resource usage
  LoadFactor capacity_;     // resource capacity
  double load_;       // the load
  // statistics for one tablegroup
  int64_t tg_pg_cnt_; // partition group count for one tablegroup
  // replicas belong to the unit, but on different server of the unit.
  int64_t outside_replica_cnt_;
  int64_t inside_replica_cnt_;

  share::ObUnitInfo info_;
  // After deducting the load of gts, the service capacity coefficient that the unit can provide
  double capacity_ratio_;
public:
  UnitStat() : server_(NULL), in_pool_(false),
               load_factor_(), capacity_(), load_(0), tg_pg_cnt_(0),
               outside_replica_cnt_(0), inside_replica_cnt_(0), info_(), capacity_ratio_(0.0) {}
  int assign(const UnitStat &other);
  UnitStat &operator=(const UnitStat &that);
  void set_key(const uint64_t unit_id) { info_.unit_.unit_id_ = unit_id; }
  const uint64_t &get_key() const { return info_.unit_.unit_id_; }
  uint64_t get_unit_id() { return info_.unit_.unit_id_; }

  // simply infallible function, return result directly.

  double get_load() const { return load_; }
  double get_load_if_plus(ObResourceWeight &weights, const LoadFactor &load_factor) const
  { return get_load_if(weights, load_factor, true); }
  double get_load_if_minus(ObResourceWeight &weights, const LoadFactor &load_factor) const
  { return get_load_if(weights, load_factor, false); }
  double get_load_if(ObResourceWeight &weights, const LoadFactor &load_factor, const bool plus) const;
  double calc_load(ObResourceWeight &weights, const LoadFactor &load_factor) const;

  double get_cpu_usage() const { return load_factor_.get_cpu_usage(); }
  double get_disk_usage() const { return load_factor_.get_disk_usage(); }
  double get_iops_usage() const { return load_factor_.get_iops_usage(); }
  double get_memory_usage() const { return load_factor_.get_memory_usage(); }

  double get_cpu_limit() const;
  double get_disk_limit() const;
  double get_iops_limit() const;
  double get_memory_limit() const;

  double get_cpu_usage_rate() const;
  double get_disk_usage_rate() const;
  double get_iops_usage_rate() const;
  double get_memory_usage_rate() const;
  TO_STRING_KV(K_(in_pool), K_(load_factor),
               K_(capacity), K_(tg_pg_cnt), K_(outside_replica_cnt), K_(info));
//private:
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

struct ZonePaxosInfo
{
  common::ObZone zone_;
  int64_t full_replica_num_;
  int64_t logonly_replica_num_;
  int64_t encryption_logonly_replica_num_;
  TO_STRING_KV(K_(zone),
               K_(full_replica_num),
               K_(logonly_replica_num),
               K_(encryption_logonly_replica_num));
};

typedef common::ObArray<UnitStat *> UnitStatArray;
struct ZoneUnit
{
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
  int64_t tg_pg_cnt_; //Count the number of pg on the zone
public:
  ZoneUnit()
      :zone_(),
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
  int assign(const ZoneUnit &other);
  double get_avg_load() const { return load_avg_; }
  int64_t get_pg_count() const { return tg_pg_cnt_; }
  TO_STRING_KV(K_(zone),
               K_(active_unit_cnt),
               K_(load_avg),
               K_(load_imbalance),
               K_(tg_pg_cnt),
               K_(resource_weight),
               K_(ru_total),
               K_(ru_capacity),
               K_(all_unit));
private:
  DISALLOW_COPY_AND_ASSIGN(ZoneUnit);
};

typedef common::ObArray<share::ObUnitInfo> UnitArray;
typedef common::ObArray<share::ObUnitInfo *> UnitPtrArray;
typedef common::ObSEArray<UnitPtrArray, 4> ZoneUnitPtrArray;
typedef common::ObSEArray<ZoneUnit, 4> ZoneUnitArray;

class ServerReplicaCountMgr
{
public:
  ServerReplicaCountMgr() : inited_(false), server_replica_counts_() {}
  virtual ~ServerReplicaCountMgr() {}
  int init(const common::ObIArray<common::ObAddr> &servers);
  int accumulate(const common::ObAddr &server, const int64_t cnt);
  int get_replica_count(const common::ObAddr &server, int64_t &replica_count);
  int64_t get_server_count() const { return server_replica_counts_.count(); }
private:
  struct ServerReplicaCount
  {
    common::ObAddr server_;
    int64_t replica_count_;

    ServerReplicaCount() : replica_count_(0) {}
    void reset() { *this = ServerReplicaCount(); }
    TO_STRING_KV(K_(server), K_(replica_count));
  };
  bool inited_;
  common::ObArray<ServerReplicaCount> server_replica_counts_;

  DISALLOW_COPY_AND_ASSIGN(ServerReplicaCountMgr);
};

struct ReadonlyInfo
{
  int64_t table_id_;
  ObZone zone_;
  bool readonly_at_all_;
  ReadonlyInfo() : table_id_(OB_INVALID_ID), zone_(), readonly_at_all_(false) {}
  TO_STRING_KV(K_(table_id), K_(zone), K_(readonly_at_all));
};


struct ObSimpleSequenceGenerator
{
  ObSimpleSequenceGenerator()
      :seq_(0)
  {}
  int64_t next_seq() { return seq_++; }
private:
  int64_t seq_;
};

namespace balancer
{
  const static char * const MIGRATE_TEMPORY_OFFLINE_REPLICA = "migrate temporary offline replica";
  const static char * const MIGRATE_PERMANENT_OFFLINE_REPLICA = "migrate permanent offline replica";
  const static char * const MIGRATE_TO_NEW_SERVER = "migrate to new server";
  const static char * const MIGRATE_PG_REPLICA = "migrate pg replica";
  const static char * const REPLICATE_PERMANENT_OFFLINE_REPLICA = "replicate permanent offline replica";
  const static char * const MIGRATE_ONLY_IN_MEMBERLIST_REPLICA = "migrate only in memberlist replica";
  const static char * const REBUILD_ONLY_IN_MEMBERLIST_REPLICA = "add replica only in memberlist replica";
  const static char * const MANUAL_MIGRATE_UNIT = "migrate replica due to admin migrate unit";
  const static char * const REPLICATE_ENOUGH_REPLICA = "replicate enough replica";
  const static char * const REPLICATE_ENOUGH_SPECIFIC_REPLICA = "replicate enough logonly replica on logonly unit";
  const static char * const FAST_RECOVERY_TASK = "fast recovery task";
  const static char * const ADMIN_MIGRATE_REPLICA = "admin migrate replica";
  const static char * const ADMIN_ADD_REPLICA = "admin add replica";
  const static char * const ADMIN_REMOVE_MEMBER= "admin remove member";
  const static char * const ADMIN_REMOVE_REPLICA = "admin remove non_paxos replica";
  const static char * const ADMIN_TYPE_TRANSFORM = "admin change replica type";
  const static char * const REBUILD_EMERGENCY_REPLICA = "rebuild emergency replica";
  const static char * const REBUILD_REPLICA = "rebuild replica";
  const static char * const RESTORE_REPLICA = "restore replica";
  const static char * const PHYSICAL_RESTORE_REPLICA = "physical restore replica";
  const static char * const RESTORE_FOLLOWER_REPLICA = "restore follower replica using copy sstable";
  const static char * const REBUILD_REPLICA_AS_RESTART = "rebuild replica as server restart";
  const static char * const ADD_REPLICA_OVER = "add replica over";
  const static char * const MIGRATE_REPLICA_OVER = "migrate replica over";
  const static char * const TEMPORARY_OFFLINE_REPLICA_ONLINE = "temporary offline replica online, add to paxos member list";
  const static char * const REMOVE_TEMPORY_OFFLINE_REPLICA = "remove temporay offline pasox replica";
  const static char * const REMOVE_PERMANENT_OFFLINE_REPLICA = "remove permanent offline replica";
  const static char * const REMOVE_ONLY_IN_MEMBER_LIST_REPLICA = "remove only in member list replica";
  const static char * const PG_COORDINATE_MIGRATE = "migrate replica for partition group coordinate";
  const static char * const PG_COORDINATE_REMOVE_REPLICA = "remove replica for partition group coordinate";
  const static char * const PG_COORDINATE_TYPE_TRANSFORM = "type transform for partition group coordinate";
  const static char * const UNIT_PG_BALANCE = "migrate pg for unit load balance";
  const static char * const TG_PG_BALANCE = "migrate pg for partition group balance";
  const static char * const TG_PG_SHARD_BALANCE = "migrate pg for sharding table partition balance";
  const static char * const TG_PG_SHARD_PARTITION_BALANCE = "migrate pg for same range sharding table partition balance";
  const static char * const TG_PG_GROUP_BALANCE = "migrate pg for grouped partition table balance";
  const static char * const TG_NON_PARTITION_TABLE_BALANCE = "migrate partition for non partition table balance";
  const static char * const TG_PG_TABLE_BALANCE = "migrate pg for ungrouped partition table balance";
  const static char * const LOCALITY_TYPE_TRANSFORM = "type transform according to locality";
  const static char * const LOCALITY_REMOVE_REDUNDANT_MEMBER = "remove redundant member according to locality";
  const static char * const LOCALITY_REMOVE_REDUNDANT_REPLICA = "remove redundant non_paxos replica according to locality";
  const static char * const REMOVE_MIGRATE_UNIT_BLOCK_REPLICA = "remove non_paxos replica for blocked migration destination";
  const static char * const REMOVE_MIGRATE_UNIT_BLOCK_MEMBER = "remove member for blocked migration destination";
  const static char * const REMOVE_NOT_IN_POOL_NON_PAXOS_REPLICA = "remove not in pool non_paxos replica";
  const static char * const TRANSFER_POSITION_FOR_NOT_IN_POOL_PAXOS_REPLICA = "transfer position for not in pool paxos replica";
  const static char * const COPY_GLOBAL_INDEX_SSTABLE = "copy global index sstable";
  const static char * const COPY_LOCAL_INDEX_SSTABLE = "copy local index sstable";
  const static char * const SQL_BACKGROUND_DIST_TASK_COMMENT = "background executing sql distributed task";
  const static char * const SINGLE_ZONE_STOP_SERVER = "single zone multi replica stop server";
  const static char * const STANDBY_CUT_DATA_TASK = "standby cut data task";
};

} // end namespace rootserver
} // end namespace oceanbase

#endif /* _OB_BALANCE_INFO_H */
