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

#ifndef __OCEANBASE_ROOTSERVER_OB_ALLOC_REPLICA_STRATEGY_H__
#define __OCEANBASE_ROOTSERVER_OB_ALLOC_REPLICA_STRATEGY_H__

#include "share/ob_define.h"
#include "lib/list/ob_dlist.h"
#include "lib/container/ob_iarray.h"
#include "lib/container/ob_array.h"
#include "lib/container/ob_se_array.h"
#include "share/ob_rpc_struct.h"
#include "common/ob_unit_info.h"
#include "share/schema/ob_table_schema.h"
#include "share/schema/ob_schema_struct.h"
#include "share/ob_replica_info.h"
#include "ob_replica_addr.h"
#include "ob_zone_unit_provider.h"
#include "ob_root_utils.h"
#include "ob_balance_info.h"
#include "ob_rebalance_task_mgr.h"
#include "share/ob_unit_replica_counter.h"

namespace oceanbase {
namespace share {
class ObPartitionInfo;
}
namespace rootserver {
namespace balancer {
class ObSinglePtBalanceContainer;
};
class ObZoneManager;

// this is used for replica_creator/root_balancer
struct ZoneReplicaDistTask {
public:
  enum class ReplicaNature {
    PAXOS = 0,
    NON_PAXOS,
    INVALID,
  };

public:
  ZoneReplicaDistTask()
      : replica_nature_(ReplicaNature::INVALID), zone_set_(), replica_task_set_(), multi_zone_dist_(false)
  {}
  virtual ~ZoneReplicaDistTask()
  {}

public:
  int tmp_compatible_generate(const ReplicaNature replica_nature, const common::ObZone& zone,
      const ObReplicaType replica_type, const int64_t memstore_percent);
  int generate(const ReplicaNature replica_nature, const share::ObZoneReplicaAttrSet& zone_replica_attr_set,
      const common::ObIArray<rootserver::ObReplicaAddr>& exist_addr);
  int check_valid(bool& is_valid);
  int check_has_task(const common::ObZone& zone, const ObReplicaType replica_type, const int64_t memstore_percent,
      bool& has_this_task);
  int erase_task(const common::ObZone& zone, const int64_t memstore_percent, const ObReplicaType replica_type);
  int check_empty(bool& is_empty);
  ReplicaNature get_replica_nature() const
  {
    return replica_nature_;
  }
  int assign(const ZoneReplicaDistTask& that);
  TO_STRING_KV(K_(replica_nature), K_(zone_set), K_(replica_task_set), K_(multi_zone_dist));
  const common::ObIArray<common::ObZone>& get_zone_set() const
  {
    return zone_set_;
  }
  const share::ObReplicaAttrSet& get_replica_task_set() const
  {
    return replica_task_set_;
  }

private:
  int generate_paxos_replica_dist_task(const share::ObZoneReplicaAttrSet& zone_replica_attr_set,
      const common::ObIArray<rootserver::ObReplicaAddr>& exist_addr);
  int generate_non_paxos_replica_dist_task(const share::ObZoneReplicaAttrSet& zone_replica_attr_set,
      const common::ObIArray<rootserver::ObReplicaAddr>& exist_addr);

private:
  ReplicaNature replica_nature_;
  common::ObSEArray<common::ObZone, 7, common::ObNullAllocator> zone_set_;
  share::ObReplicaAttrSet replica_task_set_;
  bool multi_zone_dist_;
};

// types and constants
// typedef common::ObArray<share::ObUnitInfo> UnitArray;
// typedef common::ObSEArray<UnitArray, common::MAX_ZONE_NUM> ZoneUnitArray;
class ObLocalityUtility {
public:
  ObLocalityUtility(const ObZoneManager& zone_mgr, const share::schema::ZoneLocalityIArray& zloc,
      const common::ObIArray<common::ObZone>& zone_list);
  virtual ~ObLocalityUtility();
  int init(bool with_paxos, bool with_readonly);
  void reset();

protected:
  template <typename Scope>
  struct AllocTask {
    AllocTask() : replica_type_(common::REPLICA_TYPE_MAX), memstore_percent_(100)
    {}
    AllocTask(const Scope& name, common::ObReplicaType type, const int64_t memstore_percent)
        : name_(name), replica_type_(type), memstore_percent_(memstore_percent)  // safe to copy region/zone
    {}
    int assign(const AllocTask& that)
    {
      int ret = common::OB_SUCCESS;
      name_ = that.name_;  // safe
      replica_type_ = that.replica_type_;
      memstore_percent_ = that.memstore_percent_;
      if (OB_FAIL(inner_task_.assign(that.inner_task_))) {
        RS_LOG(WARN, "fail to assign", K(ret));
      }
      return ret;
    }
    void reset()
    {
      name_.reset();
      replica_type_ = common::REPLICA_TYPE_MAX;
      memstore_percent_ = 100;
    }
    TO_STRING_KV(K_(name), K_(replica_type), K_(memstore_percent));

    Scope name_;  // region/zone name
    common::ObReplicaType replica_type_;
    int64_t memstore_percent_;
    ZoneReplicaDistTask inner_task_;
  };
  typedef AllocTask<common::ObZone> ZoneTask;

protected:
  int init_zone_task(const bool with_paxos, const bool with_readonly);

protected:
  const share::schema::ZoneLocalityIArray& zloc_;
  // By corresponding idx to record
  // how many replicas the current partition has allocated according to locality
  common::ObArray<ZoneTask> zone_tasks_;
  common::ObArray<ZoneTask> shadow_zone_tasks_;
  common::ObArray<common::ObZone> all_server_zones_;
  const ObZoneManager& zone_mgr_;
  const common::ObIArray<common::ObZone>& zone_list_;
};

class ObAllocReplicaByLocality : public ObLocalityUtility {
public:
  ObAllocReplicaByLocality(const ObZoneManager& zone_mgr, ObZoneUnitsProvider& zone_units_provider,
      const share::schema::ZoneLocalityIArray& zloc, obrpc::ObCreateTableMode create_mode,
      const common::ObIArray<common::ObZone>& zone_list);
  virtual ~ObAllocReplicaByLocality();

public:
  void reset();

protected:
  int init(bool with_paxos, bool with_readonly);
  int alloc_replica_in_zone(const int64_t pos, const common::ObZone& zone, const common::ObReplicaType replica_type,
      const int64_t memstore_percent, ObReplicaAddr& replica_addr);

protected:
  obrpc::ObCreateTableMode create_mode_;
  ObZoneUnitsProvider& zone_units_provider_;
  // Constraint: no two partition replicas will be allocated on the same server
  // Record the server that has allocated replica
  common::hash::ObHashSet<int64_t> unit_set_;
  int64_t zone_task_idx_;
  // In order to avoid always assigning resources from the same location,
  // the following pointer is introduced
  int64_t saved_zone_pos_;  // Iteration pointer in region
  bool need_update_pg_cnt_;
};

struct SingleReplica {
  SingleReplica(const ObReplicaType replica_type, const int64_t memstore_percent)
      : replica_type_(replica_type), memstore_percent_(memstore_percent)
  {}
  SingleReplica() : replica_type_(common::REPLICA_TYPE_MAX), memstore_percent_(100)
  {}
  TO_STRING_KV(K_(replica_type), K_(memstore_percent));
  common::ObReplicaType replica_type_;
  int64_t memstore_percent_;
};

struct SingleReplicaSort {
  SingleReplicaSort() : ret_(common::OB_SUCCESS)
  {}
  bool operator()(const SingleReplica& left, const SingleReplica& right)
  {
    bool bool_ret = false;
    if (OB_UNLIKELY(common::OB_SUCCESS != ret_)) {
      // bypass
    } else if (!ObReplicaTypeCheck::is_replica_type_valid(left.replica_type_) ||
               !ObReplicaTypeCheck::is_replica_type_valid(right.replica_type_)) {
      ret_ = common::OB_ERR_UNEXPECTED;
    } else if (left.replica_type_ == right.replica_type_) {
      bool_ret = left.memstore_percent_ > right.memstore_percent_;
    } else if (REPLICA_TYPE_FULL == left.replica_type_) {
      bool_ret = true;
    } else if (REPLICA_TYPE_LOGONLY == left.replica_type_) {
      if (REPLICA_TYPE_FULL == right.replica_type_) {
        bool_ret = false;
      } else {
        bool_ret = true;
      }
    } else {
      bool_ret = false;
    }
    return bool_ret;
  }
  int get_ret() const
  {
    return ret_;
  }
  int ret_;
};

class ObCreateTableReplicaByLocality : public ObAllocReplicaByLocality {
public:
  ObCreateTableReplicaByLocality(const ObZoneManager& zone_mgr, ObZoneUnitsProvider& zone_units_provider,
      const share::schema::ZoneLocalityIArray& zloc, obrpc::ObCreateTableMode create_mode,
      const common::ObIArray<common::ObZone>& zone_list, const int64_t seed,
      balancer::ObSinglePtBalanceContainer* pt_balance_container = nullptr,
      common::ObSEArray<common::ObZone, 7>* high_priority_zone_array = nullptr);
  virtual ~ObCreateTableReplicaByLocality()
  {}
  int init();
  int prepare_for_next_partition(const ObPartitionAddr& paddr);
  int get_next_replica(const ObPartitionKey& pkey, ObIArray<share::TenantUnitRepCnt*>& ten_unit_arr,
      const bool non_partition_table, ObReplicaAddr& replica_addr);
  int fill_all_rest_server_with_replicas(ObPartitionAddr& paddr);

private:
  int prepare_for_next_task(const ZoneTask& zone_task);
  int get_xy_index(const bool is_multiple_zone, const common::ObPartitionKey& pkey,
      const common::ObIArray<common::ObZone>& zone_array, int64_t& x_index, int64_t& y_index);
  int map_y_axis(const int64_t axis, const int64_t scope, const int64_t bucket_num, int64_t& map_axis);
  int recalculate_y_axis(const int64_t axis, const int64_t scope, const int64_t bucket_num, int64_t& recal_axis);
  int alloc_single_zone_paxos_replica(const SingleReplica& single_replica, const common::ObZone& zone,
      const int64_t y_index, ObReplicaAddr& replica_addr);
  int alloc_single_zone_nonpaxos_replica(const SingleReplica& single_replica, const common::ObZone& zone,
      const int64_t y_index, ObReplicaAddr& replica_addr);
  int alloc_non_part_single_zone_replica(const SingleReplica& single_replica, const common::ObPartitionKey& pkey,
      ObIArray<share::TenantUnitRepCnt*>& ten_unit_arr, ObReplicaAddr& replica_addr);
  int alloc_non_part_multiple_zone_paxos_replica(const SingleReplica& mix_single_replica,
      const common::ObPartitionKey& pkey, const common::ObIArray<common::ObZone>& zone_array, const int64_t x_index,
      ObIArray<share::TenantUnitRepCnt*>& ten_unit_arr, ObReplicaAddr& replica_addr);
  int do_alloc_non_part_replica(const SingleReplica& single_replica, const common::ObPartitionKey& pkey,
      const common::ObSEArray<UnitPtrArray, common::MAX_ZONE_NUM>& unit_array, const common::ObZone& zone,
      ObIArray<share::TenantUnitRepCnt*>& ten_unit_arr, ObReplicaAddr& replica_addr);
  int gen_replica_addr_by_leader_arr(const SingleReplica& single_replica,
      const common::ObIArray<share::ObUnitInfo*>& unit_ptr_arr,
      const common::ObIArray<share::TenantUnitRepCnt*>& leader_rep_unit_arr, ObReplicaAddr& replica_addr,
      bool& find_unit);
  int gen_replica_addr_by_unit_arr(const SingleReplica& single_replica,
      const common::ObIArray<share::ObUnitInfo*>& unit_ptr_arr, ObReplicaAddr& replica_addr, bool& find_unit);
  int prepare_replica_info(const common::ObSEArray<UnitPtrArray, common::MAX_ZONE_NUM>& unit_array,
      const common::ObZone& zone, ObIArray<share::ObUnitInfo*>& unit_ptr_arr, ObIArray<common::ObZone>& zone_list);
  int get_random_zone_num(
      const int64_t zone_list_count, const uint64_t tenant_id, const int64_t index_zone_id, int64_t& zone_id);
  bool is_equal_zero_zone(const int64_t zone_list_count, const uint64_t tenant_id, const int64_t zone_id);
  int get_replica_addr_array(const SingleReplica& single_replica, ObIArray<share::TenantUnitRepCnt*>& ten_unit_arr,
      const common::ObZone& zone, common::ObArray<share::TenantUnitRepCnt*>& leader_rep_unit_arr,
      const common::ObPartitionKey& pkey, common::ObIArray<share::ObUnitInfo*>& unit_ptr_arr,
      const common::ObIArray<common::ObZone>& zone_list);
  int get_final_replica_addr_array(const SingleReplica& single_replica,
      common::ObIArray<share::TenantUnitRepCnt*>& ten_unit_arr,
      common::ObArray<share::TenantUnitRepCnt*>& leader_rep_unit_map_arr);
  int alloc_multiple_zone_paxos_replica(const SingleReplica& single_replica,
      const common::ObIArray<common::ObZone>& zone_array, const int64_t x_index, const int64_t y_index,
      ObReplicaAddr& replica_addr);
  int get_alive_zone_list(const ObSEArray<UnitPtrArray, common::MAX_ZONE_NUM>& unit_array,
      const common::ObIArray<common::ObZone>& zone_array, common::ObIArray<common::ObZone>& alive_zone_list);
  int alloc_single_zone_task(const common::ObPartitionKey& pkey, const SingleReplica& single_replica,
      ObIArray<share::TenantUnitRepCnt*>& ten_unit_arr, const bool non_partition_table, ObReplicaAddr& replica_addr);
  int alloc_multiple_zone_task(const common::ObPartitionKey& pkey, const SingleReplica& single_replica,
      ObIArray<share::TenantUnitRepCnt*>& ten_unit_arr, const bool non_partition_table, ObReplicaAddr& replica_addr);
  struct TenParCmp {
    TenParCmp(const ObReplicaType replica_type, const int64_t memstore_percent)
    {
      replica_type_ = replica_type;
      memstore_percent_ = memstore_percent;
    }
    bool operator()(share::TenantUnitRepCnt*& ltur, share::TenantUnitRepCnt*& rtur)
    {
      bool bret = ltur->unit_rep_cnt_.get_all_replica_cnt() < rtur->unit_rep_cnt_.get_all_replica_cnt();
      if (!bret && ltur->unit_rep_cnt_.get_all_replica_cnt() == rtur->unit_rep_cnt_.get_all_replica_cnt()) {
        if (replica_type_ == REPLICA_TYPE_FULL && memstore_percent_ == 100) {
          bret = ltur->unit_rep_cnt_.get_full_replica_cnt() < rtur->unit_rep_cnt_.get_full_replica_cnt();
        } else if (replica_type_ == REPLICA_TYPE_FULL && memstore_percent_ == 0) {
          bret = ltur->unit_rep_cnt_.get_d_replica_cnt() < rtur->unit_rep_cnt_.get_d_replica_cnt();
        } else if (replica_type_ == REPLICA_TYPE_LOGONLY) {
          bret = ltur->unit_rep_cnt_.get_logonly_replica_cnt() < rtur->unit_rep_cnt_.get_logonly_replica_cnt();
        } else if (replica_type_ == REPLICA_TYPE_READONLY) {
          bret = ltur->unit_rep_cnt_.get_readonly_replica_cnt() < rtur->unit_rep_cnt_.get_readonly_replica_cnt();
        }
      }
      return bret;
    }
    ObReplicaType replica_type_;
    int64_t memstore_percent_;
  };
  struct PairCmpRepLeaderAsc {
    bool operator()(share::TenantUnitRepCnt*& left, share::TenantUnitRepCnt*& right)
    {
      bool bool_ret = false;
      if (left->unit_rep_cnt_.get_leader_cnt() < right->unit_rep_cnt_.get_leader_cnt()) {
        bool_ret = true;  // small to big
      } else {
        bool_ret = false;
      }
      return bool_ret;
    }
  };
  struct PairCmpRepLeaderDes {
    bool operator()(share::TenantUnitRepCnt*& left, share::TenantUnitRepCnt*& right)
    {
      bool bool_ret = false;
      if (left->unit_rep_cnt_.get_leader_cnt() <= right->unit_rep_cnt_.get_leader_cnt()) {
        bool_ret = false;  // big to small
      } else {
        bool_ret = true;
      }
      return bool_ret;
    }
  };

private:
  balancer::ObSinglePtBalanceContainer* pt_balance_container_;
  common::ObSEArray<common::ObZone, 7>* high_priority_zone_array_;
  common::ObArray<common::ObZone> curr_part_zone_array_;
  common::ObArray<SingleReplica> curr_part_task_array_;
  int64_t curr_part_task_idx_;
  int64_t seed_;  // table-level seed
private:
  DISALLOW_COPY_AND_ASSIGN(ObCreateTableReplicaByLocality);
};

// Used to process the replica of L on logonly_unit
// Check the units and TASK, if logonly_unit exists in the units,
// save the logonly_task on the zone where the logonly_unit is located.
//
// This strategy is used to supplement copies of this logonly_task
// TODO: Unprocessed region_task
class ObAddSpecificReplicaByLocality : public ObAllocReplicaByLocality {
public:
  ObAddSpecificReplicaByLocality(const ObZoneManager& zone_mgr, TenantBalanceStat& ts,
      ObZoneUnitsProvider& zone_units_provider, const share::schema::ZoneLocalityIArray& zloc,
      const common::ObIArray<common::ObZone>& zone_list);
  virtual ~ObAddSpecificReplicaByLocality()
  {}
  int init();
  int prepare_for_next_partition(Partition& partition, const bool need_update_pg_cnt);
  int get_next_replica(const common::ObArray<Partition*>::iterator& p, ObReplicaAddr& replica_addr);
  int align_add_replica(const common::ObArray<Partition*>::iterator& p, ObReplicaAddr& replica_addr,
      const int64_t& next, bool& need_random_add_replica);

private:
  TenantBalanceStat& ts_;
  common::ObArray<ObZone> logonly_zones_;              // The zone where the logonly unit is located
  common::ObArray<ZoneTask> shadow_zone_tasks_local_;  // logonly task located on logonly unit
  common::ObArray<ZoneTask> logonly_zone_task_;
  // The object of the comparison check operation of each partition,
  // the initial value is shadow_logonly_zone_task_

  DISALLOW_COPY_AND_ASSIGN(ObAddSpecificReplicaByLocality);
};

class ObAddPaxosReplicaByLocality : public ObAllocReplicaByLocality {
public:
  ObAddPaxosReplicaByLocality(const ObZoneManager& zone_mgr, TenantBalanceStat& ts,
      ObZoneUnitsProvider& zone_units_provider, ObZoneUnitsProvider& logonly_zone_units_provider,
      const share::schema::ZoneLocalityIArray& zloc, const common::ObIArray<common::ObZone>& zone_list);
  virtual ~ObAddPaxosReplicaByLocality()
  {}
  int init();
  int prepare_for_next_partition(Partition& partition, const bool need_update_pg_cnt);
  int get_next_replica(const common::ObArray<Partition*>::iterator& pp, ObReplicaAddr& replica_addr);
  int align_add_replica(const common::ObArray<Partition*>::iterator& pp, ObReplicaAddr& replica_addr,
      const int64_t& next, bool& need_random_add_replica);

private:
  TenantBalanceStat& ts_;
  ObZoneUnitsProvider& logonly_zone_unit_provider_;

  DISALLOW_COPY_AND_ASSIGN(ObAddPaxosReplicaByLocality);
};

class ObAddReadonlyReplicaByLocality : public ObAllocReplicaByLocality {
public:
  ObAddReadonlyReplicaByLocality(const ObZoneManager& zone_mgr, TenantBalanceStat& ts,
      ObZoneUnitsProvider& zone_units_provider, const share::schema::ZoneLocalityIArray& zloc,
      const common::ObIArray<common::ObZone>& zone_list);
  virtual ~ObAddReadonlyReplicaByLocality()
  {}
  int init();
  int prepare_for_next_partition(Partition& partition, const bool need_update_pg_cnt);
  int get_next_replica(const common::ObArray<Partition*>::iterator& p, ObReplicaAddr& replica_addr);
  int align_add_replica(const common::ObArray<Partition*>::iterator& p, ObReplicaAddr& replica_addr,
      const int64_t& next, bool& need_random_add_replica);

private:
  TenantBalanceStat& ts_;
  DISALLOW_COPY_AND_ASSIGN(ObAddReadonlyReplicaByLocality);
};

struct FilterResult {
public:
  FilterResult()
      : cmd_type_(ObRebalanceTaskType::MAX_TYPE),
        replica_(),
        zone_(),
        region_(),
        dest_type_(REPLICA_TYPE_MAX),
        dest_memstore_percent_(100),
        invalid_unit_(false)
  {}
  ~FilterResult()
  {}
  common::ObReplicaType get_dest_type() const
  {
    return dest_type_;
  }
  const Replica& get_replica() const
  {
    return replica_;
  }
  const common::ObZone& get_zone() const
  {
    return zone_;
  }
  const common::ObRegion& get_region() const
  {
    return region_;
  }
  ObRebalanceTaskType get_cmd_type() const
  {
    return cmd_type_;
  }
  int64_t get_dest_memstore_percent() const
  {
    return dest_memstore_percent_;
  }
  bool is_valid() const;
  void build_type_transform_task(
      const Replica& replica, const ObReplicaType& dest_type, const int64_t memstore_percent);
  void build_add_task(const common::ObRegion& region, const common::ObZone& zone, const ObReplicaType& dest_type,
      const int64_t dest_memstore_percent);
  void build_delete_task(const uint64_t tenant_id, const uint64_t table_id, const bool is_standby_cluster,
      const Replica& replica, bool invalid_unit = false);
  bool is_type_transform_task() const
  {
    return cmd_type_ == ObRebalanceTaskType::TYPE_TRANSFORM;
  }
  bool is_add_task() const
  {
    return cmd_type_ == ObRebalanceTaskType::ADD_REPLICA;
  }
  bool is_delete_task_with_invalid_unit() const
  {
    return (cmd_type_ == ObRebalanceTaskType::MEMBER_CHANGE ||
               cmd_type_ == ObRebalanceTaskType::REMOVE_NON_PAXOS_REPLICA) &&
           invalid_unit_;
  }
  int build_task(const Partition& partition, ObReplicaTask& task) const;
  TO_STRING_KV(K_(cmd_type), K_(replica), K_(zone), K_(dest_type));

private:
  ObRebalanceTaskType cmd_type_;
  Replica replica_;
  common::ObZone zone_;
  common::ObRegion region_;
  common::ObReplicaType dest_type_;
  int64_t dest_memstore_percent_;
  bool invalid_unit_;
};

// This function is used to check the similarities and differences between locality and partition;
// By comparing locality and replica one by one,
// what is finally returned to the user is where the replica and locality are inconsistent
class ObFilterLocalityUtility : public ObLocalityUtility {
public:
  struct ZoneReplicaInfo {
    common::ObZone zone_;
    int64_t non_readonly_replica_count_;
    ZoneReplicaInfo() : zone_(), non_readonly_replica_count_(0)
    {}
    TO_STRING_KV(K_(zone), K_(non_readonly_replica_count));
  };
  ObFilterLocalityUtility(const ObZoneManager& zone_mgr, TenantBalanceStat& ts,
      const share::schema::ZoneLocalityIArray& zloc, const common::ObIArray<common::ObZone>& zone_list);
  ObFilterLocalityUtility(const ObZoneManager& zone_mgr, const share::schema::ZoneLocalityIArray& zloc,
      const ObUnitManager& unit_mgr, const uint64_t tenant_id, const common::ObIArray<common::ObZone>& zone_list);
  virtual ~ObFilterLocalityUtility()
  {}
  int init();
  int filter_locality(const Partition& partition);
  int filter_locality(const share::ObPartitionInfo& partition_info);
  int do_filter_locality();
  // Return the result of the comparison
  const ObIArray<FilterResult>& get_filter_result() const
  {
    return results_;
  }
  ObIArray<FilterResult>& get_filter_result()
  {
    return results_;
  }
  // Determine whether there is a task that needs add_replica in the results
  static bool has_add_task(ObIArray<FilterResult>& results);
  // Determine whether there is a task of type_transform in results;
  static bool has_type_transform_task(ObIArray<FilterResult>& results);

private:
  int inner_init(const Partition& partition);
  int inner_init(const share::ObPartitionInfo& partition_info);
  int filter_readonly_at_all();
  int process_remain_info();
  int delete_redundant_replica(const ObZone& zone, const int64_t delete_cnt);
  int choose_one_replica_to_delete(const ObZone& zone);

protected:
  int get_readonly_replica_count(const common::ObZone& zone, int64_t& readonly_count);

private:
  int build_readonly_info();
  int get_non_readonly_replica_count(const common::ObZone& zone, int64_t& non_readonly_replica_count);
  int get_unit_count(const common::ObZone& zone, int64_t& unit_count);
  int add_type_transform_task(
      const Replica& replica, const common::ObReplicaType& type, const int64_t memstore_percent);
  int add_rereplicate_task(const common::ObRegion& region, const common::ObZone& zone,
      const common::ObReplicaType& type, const int64_t memstore_percent);
  int add_remove_task(const Replica& replica, bool invalid_unit = false);

public:
  static const int64_t SAFE_REMOVE_REDUNDANCY_REPLICA_TIME = 60 * 1000 * 1000L;

protected:
  TenantBalanceStat* ts_;
  int64_t table_id_;
  int64_t partition_id_;
  common::ObArray<Replica> replicas_;
  common::ObArray<Replica> bak_replicas_;
  ObArray<FilterResult> results_;
  const ObUnitManager* unit_mgr_;
  const uint64_t tenant_id_;
  const ObZoneManager* zone_mgr_;
  common::ObArray<ZoneReplicaInfo> zone_replica_info_;
  bool has_balance_info_;
};

class ObDeleteReplicaUtility : public ObFilterLocalityUtility {
public:
  ObDeleteReplicaUtility(const ObZoneManager& zone_mgr, TenantBalanceStat& ts,
      const share::schema::ZoneLocalityIArray& zloc, const common::ObIArray<common::ObZone>& zone_list);
  virtual ~ObDeleteReplicaUtility()
  {}
  int init();
  // Compare locality and delete redundant copies;
  // The premise of deletion is that there is no task of add_replica and type_transform;
  int get_delete_task(const Partition& partition, bool& need_delete, Replica& replica);

  void reset();

private:
  int get_one_delete_task(const ObIArray<FilterResult>& results, FilterResult& result);

private:
  DISALLOW_COPY_AND_ASSIGN(ObDeleteReplicaUtility);
};

class ObReplicaTypeTransformUtility : public ObFilterLocalityUtility {
public:
  ObReplicaTypeTransformUtility(const ObZoneManager& zone_mgr, TenantBalanceStat& ts,
      const share::schema::ZoneLocalityIArray& zloc, const common::ObIArray<common::ObZone>& zone_list,
      const ObServerManager& server_mgr);
  virtual ~ObReplicaTypeTransformUtility()
  {}
  int init();
  void reset();
  int get_transform_task(const Partition& partition, bool& need_change, Replica& replica, ObReplicaType& dest_type,
      int64_t& dest_memstore_percent);
  static int get_one_type_transform_task(const ObIArray<FilterResult>& results, FilterResult& result);

private:
  int get_unit_count(const ObZone& zone, int64_t& active_unit_count, int64_t& inactive_unit_count);

  bool can_migrate_unit(const ObZone& zone);
  // Check the number of valid replicas of the partition on the zone
  int get_replica_count(const Partition& partition, const ObZone& zone, int64_t& replica_count);
  // Check whether the partition has a replica in each unit on the zone
  bool all_unit_have_replica(const Partition& partition, const ObZone& zone);
  // Check whether all units have replicas of non-paxos types.
  // Type conversions are needed to supplement paxos replica.
  int check_paxos_replica(const Partition& partition, const ObIArray<FilterResult>& results, FilterResult& result);
  // Choose a replica as the source to convert to a paxos replica
  // Directly select the replica with the largest version number
  int choose_replica_for_type_transform(
      const Partition& partition, const common::ObZone& zone, const ObReplicaType& dest_replica_type, Replica& replica);

private:
  const ObServerManager* server_mgr_;
};
}  // namespace rootserver
}  // namespace oceanbase
#endif /* __OCEANBASE_ROOTSERVER_OB_ALLOC_REPLICA_STRATEGY_H__ */
//// end of header file
