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

#ifndef _OB_ROOT_UTILS_H
#define _OB_ROOT_UTILS_H 1
#include "common/ob_unit_info.h"
#include "share/ob_check_stop_provider.h"
#include "share/ob_replica_info.h"
#include "lib/container/ob_iarray.h"
#include "lib/container/ob_array.h"
#include "lib/container/ob_array_iterator.h"
#include "lib/hash/ob_hashmap.h"
#include "share/ob_define.h"
#include "share/ob_common_rpc_proxy.h"
#include "rootserver/ob_replica_addr.h"
#include "rootserver/ob_locality_util.h"
#include "rootserver/ob_zone_manager.h"
#include "share/ob_cluster_type.h"
namespace oceanbase {
namespace share {
class ObIPartPropertyGetter;
class ObZoneReplicaAttrSet;
namespace schema {
class ObMultiVersionSchemaService;
class ObTableSchema;
class ObLocality;
class ObSchemaGetterGuard;
}  // namespace schema
}  // namespace share

namespace rootserver {
namespace balancer {
class HashIndexCollection;
class ITenantStatFinder;
}  // namespace balancer
class ObDDLService;
class ObRebalanceTaskMgr;
class TenantBalanceStat;
struct Partition;
struct Replica;
class ObUnitManager;
class ObZoneManager;
class ObLocalityDistribution;
template <typename T>
inline T majority(const T n)
{
  return n / 2 + 1;
}

template <typename T>
inline bool is_same_tg(const T& left, const T& right)
{
  bool same = false;
  if (left.tablegroup_id_ == right.tablegroup_id_) {
    if (common::OB_INVALID_ID != left.tablegroup_id_) {
      same = true;
    } else {
      same = left.table_id_ == right.table_id_;
    }
  }
  return same;
}

template <typename T>
inline bool is_same_pg(const T& left, const T& right)
{
  return is_same_tg(left, right) && left.partition_idx_ == right.partition_idx_;
}

enum ObResourceType { RES_CPU = 0, RES_MEM = 1, RES_DISK = 2, RES_MAX };

class ObIServerResource {
public:
  ObIServerResource() = default;
  virtual ~ObIServerResource() = default;
  // return -1 if resource_type is invalid
  virtual double get_assigned(ObResourceType resource_type) const = 0;
  virtual double get_capacity(ObResourceType resource_type) const = 0;
  virtual double get_max_assigned(ObResourceType resource_type) const = 0;
};

class ObIServerResourceDemand {
public:
  ObIServerResourceDemand() = default;
  virtual ~ObIServerResourceDemand() = default;
  // return -1 if resource_type is invalid
  virtual double get_demand(ObResourceType resource_type) const = 0;
};

class ObResourceUtils {
public:
  // the weight of the i-th resource is equal to the average usage of i-th resource
  template <class T>
  static int calc_server_resource_weight(const common::ObArray<T>& servers, double* weights, int64_t count)
  {
    int ret = common::OB_SUCCESS;
    if (count != RES_MAX || servers.count() <= 0) {
      ret = common::OB_INVALID_ARGUMENT;
    } else {
      memset(weights, 0, count * sizeof(double));
      ARRAY_FOREACH(servers, i)
      {
        const T& server_resource = servers.at(i);
        for (int j = RES_CPU; j < RES_MAX; ++j) {
          ObResourceType res_type = static_cast<ObResourceType>(j);
          if (res_type == RES_DISK) {
            weights[res_type] = 0.0;
          } else if (server_resource.get_capacity(res_type) <= 0 || server_resource.get_assigned(res_type) <= 0) {
            weights[res_type] += 0.0;
          } else if (server_resource.get_assigned(res_type) > server_resource.get_capacity(res_type)) {
            weights[res_type] += 1.0;
          } else {
            RS_LOG(DEBUG,
                "weight factor",
                K(res_type),
                "assigned",
                server_resource.get_assigned(res_type),
                "capacity",
                server_resource.get_capacity(res_type),
                "factor",
                server_resource.get_assigned(res_type) / server_resource.get_capacity(res_type));
            weights[res_type] += server_resource.get_assigned(res_type) / server_resource.get_capacity(res_type);
          }
        }
      }
      const int64_t N = servers.count();
      double sum = 0;
      for (int j = RES_CPU; j < RES_MAX; ++j) {
        weights[j] /= static_cast<double>(N);
        sum += weights[j];
        // sanity check
        if (weights[j] < 0 || weights[j] > 1) {
          ret = common::OB_ERR_UNEXPECTED;
          RS_LOG(ERROR, "weight should be in [0,1]", K(j), "w", weights[j]);
        }
      }
      if (OB_SUCC(ret) && sum > 0) {
        // normalization
        for (int j = RES_CPU; j < RES_MAX; ++j) {
          weights[j] /= sum;
        }
      }
    }
    return ret;
  }

  template <typename T>
  static int calc_load(T& resource, double* weights, int64_t weights_count, double& load)
  {
    int ret = common::OB_SUCCESS;
    if (weights_count != RES_MAX) {
      ret = common::OB_INVALID_ARGUMENT;
      RS_LOG(WARN, "invalid weight vector", K(ret), K(weights_count));
    } else {
      load = 0.0;
      for (int j = RES_CPU; j < RES_MAX; ++j) {
        ObResourceType res_type = static_cast<ObResourceType>(j);
        if (resource.get_capacity(res_type) <= 0 || resource.get_assigned(res_type) <= 0) {
          // continue
          // load += weights_[j] * 0.0;
        } else if (resource.get_assigned(res_type) > resource.get_capacity(res_type)) {
          load += weights[j] * 1.0;
        } else {
          load += weights[j] * (resource.get_assigned(res_type) / resource.get_capacity(res_type));
        }
      }
    }
    return ret;
  }

  template <typename T1, typename T2>
  static int calc_load(T1& demand, T2& usage, double* weights, int64_t weights_count, double& load)
  {
    int ret = common::OB_SUCCESS;
    if (weights_count != RES_MAX) {
      ret = common::OB_INVALID_ARGUMENT;
      RS_LOG(WARN, "invalid weight vector", K(ret), K(weights_count));
    } else {
      load = 0.0;
      for (int j = RES_CPU; j < RES_MAX; ++j) {  // foreach resource type
        ObResourceType res_type = static_cast<ObResourceType>(j);
        if (usage.get_capacity(res_type) <= 0 || demand.get_demand(res_type) <= 0) {
          // continue
          // load += weights_[j] * 0.0;
        } else if (demand.get_demand(res_type) > usage.get_capacity(res_type)) {
          load += weights[j] * 1.0;
        } else {
          load += weights[j] * (demand.get_demand(res_type) / usage.get_capacity(res_type));
        }
      }
    }
    return ret;
  }

  template <typename ServerResource>
  static bool have_enough_resource(
      const ServerResource& server, const share::ObUnitConfig& unit_config, double hard_limit)
  {
    bool is_enough = true;
    if (unit_config.min_cpu_ + server.get_assigned(RES_CPU) > server.get_capacity(RES_CPU) ||
        unit_config.max_cpu_ + server.get_max_assigned(RES_CPU) > server.get_capacity(RES_CPU) * hard_limit ||
        static_cast<double>(unit_config.min_memory_) + server.get_assigned(RES_MEM) > server.get_capacity(RES_CPU) ||
        static_cast<double>(unit_config.max_memory_) + server.get_max_assigned(RES_MEM) >
            server.get_capacity(RES_MEM) * hard_limit) {
      is_enough = false;
    }
    return is_enough;
  }

private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObResourceUtils);
  ObResourceUtils();
  ~ObResourceUtils();
};

class ObTenantUtils {
public:
  static int get_tenant_ids(
      share::schema::ObMultiVersionSchemaService* schema_service, common::ObIArray<uint64_t>& tenant_ids);
  static int get_tenant_ids(share::schema::ObMultiVersionSchemaService* schema_service, int64_t& sys_schema_version,
      common::ObIArray<uint64_t>& tenant_ids);

  static int check_small_tenant(const uint64_t tenant_id, bool& small_tenant);
  static int remove_ineffective_task(common::ObMySQLTransaction& trans, const uint64_t tenant_id);
  static bool is_balance_target_schema(const share::schema::ObTableSchema& table_schema);

private:
  static int check_small_tenant_primary_zone_and_locality_condition(
      share::schema::ObSchemaGetterGuard& schema_guard, const uint64_t tenant_id, bool& small_tenant);
};

class ObBalanceTaskBuilder {
public:
  // check method: F or L paxos replicas are enough to tolerate single server disaster.
  static bool can_do_type_transform(TenantBalanceStat& ts, const Partition& partition, const Replica& replica,
      common::ObReplicaType dest_type, int64_t dest_memstore_percent);
};

class ObRootServiceRoleChecker {
public:
  static bool is_rootserver(share::ObIPartPropertyGetter* prop_getter);
};

class ObRootBalanceHelp {
public:
  enum BalanceControllerItem {
    ENABLE_REBUILD = 0,
    ENABLE_EMERGENCY_REPLICATE,
    ENABLE_TYPE_TRANSFORM,
    ENABLE_DELETE_REDUNDANT,
    ENABLE_REPLICATE_TO_UNIT,
    ENABLE_SHRINK,
    ENABLE_REPLICATE,
    ENABLE_COORDINATE_PG,
    ENABLE_MIGRATE_TO_UNIT,
    ENABLE_PARTITION_BALANCE,
    ENABLE_UNIT_BALANCE,
    ENABLE_SERVER_BALANCE,
    ENABLE_CANCEL_UNIT_MIGRATION,
    ENABLE_MODIFY_QUORUM,
    ENABLE_STOP_SERVER,
    MAX_BALANCE_ITEM
  };
  class BalanceController {
  public:
    BalanceController()
    {
      init();
    }
    virtual ~BalanceController()
    {}
    void init()
    {
      infos_[ENABLE_REBUILD] = true;
      infos_[ENABLE_TYPE_TRANSFORM] = true;
      infos_[ENABLE_DELETE_REDUNDANT] = true;
      infos_[ENABLE_REPLICATE_TO_UNIT] = true;
      infos_[ENABLE_SHRINK] = true;
      infos_[ENABLE_EMERGENCY_REPLICATE] = false;
      infos_[ENABLE_REPLICATE] = false;
      infos_[ENABLE_COORDINATE_PG] = false;
      infos_[ENABLE_MIGRATE_TO_UNIT] = false;
      infos_[ENABLE_PARTITION_BALANCE] = false;
      infos_[ENABLE_UNIT_BALANCE] = false;
      infos_[ENABLE_SERVER_BALANCE] = false;
      infos_[ENABLE_CANCEL_UNIT_MIGRATION] = false;
      infos_[ENABLE_MODIFY_QUORUM] = true;
      infos_[ENABLE_STOP_SERVER] = true;
    }
    void reset()
    {
      for (int64_t i = 0; i < MAX_BALANCE_ITEM; i++) {
        infos_[i] = false;
      }
    }
    inline bool at(const int64_t idx)
    {
      OB_ASSERT(0 <= idx && idx < MAX_BALANCE_ITEM);
      return infos_[idx];
    }
    inline void set(const int64_t idx, bool result)
    {
      OB_ASSERT(0 <= idx && idx < MAX_BALANCE_ITEM);
      infos_[idx] = result;
    }

  private:
    bool infos_[MAX_BALANCE_ITEM];
  };
  const static char* BalanceItem[];
  static int parse_balance_info(const common::ObString& json_str, BalanceController& switch_info);
};

class ObTenantGroupParser {
public:
  struct TenantNameGroup {
    TenantNameGroup() : row_(0), column_(0), tenants_()
    {}
    int64_t row_;
    int64_t column_;
    common::ObSEArray<common::ObString, 128> tenants_;
    TO_STRING_KV(K_(row), K(column_), K(tenants_));
  };

public:
  ObTenantGroupParser() : all_tenant_names_()
  {}
  virtual ~ObTenantGroupParser()
  {}

public:
  int parse_tenant_groups(const common::ObString& ttg_str, common::ObIArray<TenantNameGroup>& tenant_groups);

private:
  int get_next_tenant_name(
      int64_t& pos, const int64_t end, const common::ObString& ttg_str, common::ObString& tenant_name);
  int jump_to_next_tenant_name(int64_t& pos, const int64_t end, const common::ObString& ttg_str);
  int get_next_tenant_group(int64_t& pos, const int64_t end, const common::ObString& ttg_str,
      common::ObIArray<TenantNameGroup>& tenant_groups);
  int jump_to_next_ttg(int64_t& pos, const int64_t end, const common::ObString& ttg_str);
  int parse_tenant_vector(int64_t& pos, const int64_t end, const common::ObString& ttg_str,
      common::ObIArray<common::ObString>& tenant_names);
  int jump_to_next_tenant_vector(int64_t& pos, const int64_t end, const common::ObString& ttg_str);
  int parse_vector_tenant_group(int64_t& pos, const int64_t end, const common::ObString& ttg_str,
      common::ObIArray<TenantNameGroup>& tenant_groups);
  int parse_matrix_tenant_group(int64_t& pos, const int64_t end, const common::ObString& ttg_str,
      common::ObIArray<TenantNameGroup>& tenant_groups);
  void jump_over_space(int64_t& pos, const int64_t end, const common::ObString& ttg_str);

private:
  common::ObArray<common::ObString> all_tenant_names_;
};

class ObLocalityUtil {
public:
  ObLocalityUtil()
  {}
  virtual ~ObLocalityUtil()
  {}

public:
  static int parse_zone_list_from_locality_str(
      common::ObString& locality_locality, common::ObIArray<common::ObZone>& zone_list);
  template <typename X, typename Y>
  static int check_locality_completed_match(share::schema::ObSchemaGetterGuard& schema_guard, const X& left_schema,
      const Y& right_schema, bool& is_completed_match);

  static int generate_designated_zone_locality(const bool duplicate_table_compensate_readonly_all_server,
      const uint64_t tablegroup_id, const int64_t all_pg_idx,
      const balancer::HashIndexCollection& hash_index_collection, balancer::ITenantStatFinder& tenant_stat,
      const common::ObIArray<share::ObZoneReplicaAttrSet>& actual_zone_locality,
      common::ObIArray<share::ObZoneReplicaAttrSet>& designated_zone_locality);
  static int generate_designated_zone_locality(const bool duplicate_table_compensate_readonly_all_server,
      const uint64_t tablegroup_id, const common::ObPartitionKey& pkey,
      const balancer::HashIndexCollection& hash_index_collection,
      const common::ObIArray<share::ObZoneReplicaAttrSet>& actual_zone_locality,
      common::ObIArray<share::ObZoneReplicaAttrSet>& designated_zone_locality);

  static int64_t gen_locality_seed(const int64_t seed)
  {
    int32_t tmp_seed = 0;
    int64_t my_seed = seed;
    tmp_seed = murmurhash2(&my_seed, sizeof(&my_seed), tmp_seed);
    tmp_seed = fnv_hash2(&my_seed, sizeof(&my_seed), tmp_seed);
    my_seed = (tmp_seed < 0 ? -tmp_seed : tmp_seed);
    return my_seed;
  }

private:
  /*
   * the follow interfaces are for generating designated zone locality
   * interfaces that have ObPartitionKey parameter are for leader_coordinator
   * interfaces like **_pure_** are the actual calculation function.
   * */
  static int do_generate_designated_zone_locality(const uint64_t tablegroup_id, const int64_t all_pg_idx,
      const balancer::HashIndexCollection& hash_index_collection, balancer::ITenantStatFinder& tenant_stat,
      const share::ObZoneReplicaAttrSet& this_mix_locality,
      common::ObIArray<share::ObZoneReplicaAttrSet>& designated_zone_locality);
  static int do_generate_designated_zone_locality(const uint64_t tablegroup_id, const common::ObPartitionKey& key,
      const balancer::HashIndexCollection& hash_index_collection, const share::ObZoneReplicaAttrSet& this_mix_locality,
      common::ObIArray<share::ObZoneReplicaAttrSet>& designated_zone_locality);
  static int do_generate_pure_designated_zone_locality(const int64_t x_axis,
      const share::ObZoneReplicaAttrSet& this_mix_locality,
      common::ObIArray<share::ObZoneReplicaAttrSet>& designated_zone_locality);
};

template <typename X, typename Y>
int ObLocalityUtil::check_locality_completed_match(share::schema::ObSchemaGetterGuard& schema_guard,
    const X& left_schema, const Y& right_schema, bool& is_completed_match)
{
  int ret = common::OB_SUCCESS;
  common::ObArray<share::ObZoneReplicaAttrSet> left_zone_locality;
  common::ObArray<share::ObZoneReplicaAttrSet> right_zone_locality;
  if (OB_FAIL(left_schema.get_zone_replica_attr_array_inherit(schema_guard, left_zone_locality))) {
    RS_LOG(WARN, "fail to get zone replica num array", K(ret));
  } else if (OB_FAIL(right_schema.get_zone_replica_attr_array_inherit(schema_guard, right_zone_locality))) {
    RS_LOG(WARN, "fail to get zone replica num array", K(ret));
  } else if (left_zone_locality.count() != right_zone_locality.count()) {
    is_completed_match = false;
  } else {
    is_completed_match = true;
    for (int64_t i = 0; i < left_zone_locality.count() && is_completed_match; ++i) {
      if (left_zone_locality.at(i) != right_zone_locality.at(i)) {
        is_completed_match = false;
      } else {
      }  // go on next
    }
  }
  return ret;
}

class ObLocalityTaskHelp {
public:
  ObLocalityTaskHelp()
  {}
  virtual ~ObLocalityTaskHelp()
  {}
  static int filter_logonly_task(const common::ObIArray<share::ObResourcePoolName>& pools, ObUnitManager& unit_mgr,
      common::ObIArray<share::ObZoneReplicaAttrSet>& zone_locality);

  static int filter_logonly_task(const uint64_t tenant_id, ObUnitManager& unit_manager,
      share::schema::ObSchemaGetterGuard& schema_guard, common::ObIArray<share::ObZoneReplicaAttrSet>& zone_locality);

  static int alloc_logonly_replica(ObUnitManager& unit_manager,
      const common::ObIArray<share::ObResourcePoolName>& pools,
      const common::ObIArray<share::ObZoneReplicaAttrSet>& zone_locality, ObPartitionAddr& partition_addr);

  static int get_logonly_task_with_logonly_unit(const uint64_t tenant_id, ObUnitManager& unit_mgr,
      share::schema::ObSchemaGetterGuard& schema_guard, common::ObIArray<share::ObZoneReplicaAttrSet>& zone_locality);
};

enum QuorumTaskType {
  NOP_QUORUM = 0,
  ADD_QUORUM,
  SUB_QUORUM,
  INVALID_QUORUM_TASK,
};

struct AlterPaxosLocalityTask {
  QuorumTaskType task_type_;
  common::ObSEArray<common::ObZone, common::OB_MAX_MEMBER_NUMBER, common::ObNullAllocator> zone_set_;
  common::ObSEArray<common::ObReplicaType, common::OB_MAX_MEMBER_NUMBER, common::ObNullAllocator>
      associated_replica_type_set_;
  AlterPaxosLocalityTask() : task_type_(INVALID_QUORUM_TASK), zone_set_(), associated_replica_type_set_()
  {}
  void reset()
  {
    task_type_ = INVALID_QUORUM_TASK;
    zone_set_.reset();
    associated_replica_type_set_.reset();
  }
  TO_STRING_KV(K_(task_type), K_(zone_set), K_(associated_replica_type_set));
};

class ObLocalityCheckHelp {
public:
  typedef common::hash::ObHashMap<common::ObZone, share::ObZoneReplicaAttrSet, common::hash::NoPthreadDefendMode>
      ZoneReplicaMap;

public:
  ObLocalityCheckHelp()
  {}
  virtual ~ObLocalityCheckHelp()
  {}

  static int check_alter_locality(const common::ObIArray<share::ObZoneReplicaAttrSet>& pre_zone_locality,
      const common::ObIArray<share::ObZoneReplicaAttrSet>& cur_zone_locality,
      common::ObIArray<AlterPaxosLocalityTask>& alter_paxos_tasks, bool& non_paxos_locality_modified,
      int64_t& pre_paxos_num, int64_t& cur_paxos_num);
  static int calc_paxos_replica_num(
      const common::ObIArray<share::ObZoneReplicaAttrSet>& zone_locality, int64_t& paxos_num);

private:
  enum class SingleZoneLocalitySearch : int64_t {
    SZLS_IN_ASSOC_SINGLE = 0,
    SZLS_IN_ASSOC_MULTI,
    SZLS_NOT_FOUND,
    SZLS_INVALID,
  };

  struct XyIndex {
    XyIndex() : x_(-1), y_(-1)
    {}
    XyIndex(const int64_t x, const int64_t y) : x_(x), y_(y)
    {}
    int64_t x_;
    int64_t y_;
    TO_STRING_KV(K_(x), K_(y));
  };

  struct YIndexCmp {
    bool operator()(const XyIndex& left, const XyIndex& right)
    {
      bool cmp = true;
      if (left.y_ < right.y_) {
        cmp = true;
      } else {
        cmp = false;
      }
      return cmp;
    }
  };

  static int get_alter_quorum_replica_task(const common::ObIArray<share::ObZoneReplicaAttrSet>& pre_zone_locality,
      const common::ObIArray<share::ObZoneReplicaAttrSet>& cur_zone_locality,
      common::ObIArray<AlterPaxosLocalityTask>& alter_paxos_tasks, bool& non_paxos_locality_modified);
  static int split_single_and_multi_zone_locality(const common::ObIArray<share::ObZoneReplicaAttrSet>& zone_locality,
      common::ObIArray<share::ObZoneReplicaAttrSet>& single_zone_locality,
      common::ObIArray<share::ObZoneReplicaAttrSet>& multi_zone_locality);
  static int single_zone_locality_search(const share::ObZoneReplicaAttrSet& this_locality,
      const common::ObIArray<share::ObZoneReplicaAttrSet>& single_zone_locality,
      const common::ObIArray<share::ObZoneReplicaAttrSet>& multi_zone_locality, SingleZoneLocalitySearch& search_flag,
      int64_t& search_index, const share::ObZoneReplicaAttrSet*& search_zone_locality);
  static int try_add_single_zone_alter_paxos_task(const share::ObZoneReplicaAttrSet& pre_locality,
      const share::ObZoneReplicaAttrSet& cur_locality, common::ObIArray<AlterPaxosLocalityTask>& alter_paxos_tasks,
      bool& non_paxos_locality_modified);
  static int process_pre_single_zone_locality(
      const common::ObIArray<share::ObZoneReplicaAttrSet>& single_pre_zone_locality,
      const common::ObIArray<share::ObZoneReplicaAttrSet>& single_cur_zone_locality,
      const common::ObIArray<share::ObZoneReplicaAttrSet>& multi_cur_zone_locality,
      common::ObArray<XyIndex>& pre_in_cur_multi_indexes, common::ObIArray<AlterPaxosLocalityTask>& alter_paxos_tasks,
      bool& non_paxos_locality_modified);
  static int process_cur_single_zone_locality(
      const common::ObIArray<share::ObZoneReplicaAttrSet>& single_cur_zone_locality,
      const common::ObIArray<share::ObZoneReplicaAttrSet>& single_pre_zone_locality,
      const common::ObIArray<share::ObZoneReplicaAttrSet>& multi_pre_zone_locality,
      common::ObArray<XyIndex>& cur_in_pre_multi_indexes, common::ObIArray<AlterPaxosLocalityTask>& alter_paxos_tasks,
      bool& non_paxos_locality_modified);
  static int check_multi_zone_locality_intersect(
      const common::ObIArray<share::ObZoneReplicaAttrSet>& multi_pre_zone_locality,
      const common::ObIArray<share::ObZoneReplicaAttrSet>& multi_cur_zone_locality, bool& intersect);
  static bool check_zone_set_intersect(
      const common::ObIArray<common::ObZone>& pre_zone_set, const common::ObIArray<common::ObZone>& cur_zone_set);
  static int process_single_in_multi(const common::ObIArray<share::ObZoneReplicaAttrSet>& single_left_zone_locality,
      const common::ObIArray<XyIndex>& left_in_multi_indexes,
      const common::ObIArray<share::ObZoneReplicaAttrSet>& multi_right_zone_locality,
      common::ObIArray<AlterPaxosLocalityTask>& alter_paxos_tasks);
  static bool has_exist_in_yindex_array(const common::ObIArray<XyIndex>& index_array, const int64_t y);
  static int process_pre_multi_locality(const common::ObIArray<XyIndex>& pre_in_cur_multi_indexes,
      const common::ObIArray<XyIndex>& cur_in_pre_multi_indexes,
      const common::ObIArray<share::ObZoneReplicaAttrSet>& multi_pre_zone_locality,
      const common::ObIArray<share::ObZoneReplicaAttrSet>& multi_cur_zone_locality,
      common::ObIArray<AlterPaxosLocalityTask>& alter_paxos_tasks);
  static int process_cur_multi_locality(const common::ObIArray<XyIndex>& pre_in_cur_multi_indexes,
      const common::ObIArray<XyIndex>& cur_in_pre_multi_indexes,
      const common::ObIArray<share::ObZoneReplicaAttrSet>& multi_pre_zone_locality,
      const common::ObIArray<share::ObZoneReplicaAttrSet>& multi_cur_zone_locality,
      common::ObIArray<AlterPaxosLocalityTask>& alter_paxos_tasks);
  static int check_alter_single_zone_locality_valid(const share::ObZoneReplicaAttrSet& new_locality,
      const share::ObZoneReplicaAttrSet& orig_locality, bool& non_paxos_locality_modified);
  static int check_alter_locality_match(
      const share::ObZoneReplicaAttrSet& in_locality, const share::ObZoneReplicaAttrSet& out_locality);
  static int check_alter_locality_valid(
      common::ObIArray<AlterPaxosLocalityTask>& alter_paxos_tasks, int64_t pre_paxos_num, int64_t cur_paxos_num);
  static int add_multi_zone_locality_task(common::ObIArray<AlterPaxosLocalityTask>& alter_paxos_tasks,
      const share::ObZoneReplicaAttrSet& multi_zone_locality, const QuorumTaskType quorum_task_type);
  static int check_single_and_multi_zone_locality_match(const int64_t start, const int64_t end,
      const common::ObIArray<share::ObZoneReplicaAttrSet>& single_zone_locality,
      const common::ObIArray<XyIndex>& in_multi_indexes, const share::ObZoneReplicaAttrSet& multi_zone_locality);
};

ObTraceEventRecorder* get_rs_trace_recorder();
inline ObTraceEventRecorder* get_rs_trace_recorder()
{
  auto* ptr = GET_TSI_MULT(ObTraceEventRecorder, 2);
  return ptr;
}

class ObQuorumGetter {
public:
  ObQuorumGetter(share::schema::ObSchemaGetterGuard& schema_guard, const ObUnitManager& unit_manager,
      const ObZoneManager& zone_manager)
      : schema_guard_(&schema_guard), unit_mgr_(&unit_manager), zone_mgr_(&zone_manager)
  {}
  virtual ~ObQuorumGetter()
  {}

public:
  virtual int get_migrate_replica_quorum_size(
      const uint64_t table_id, const int64_t orig_quorum, int64_t& quorum) const;
  virtual int get_add_replica_quorum_size(const uint64_t table_id, const int64_t orig_quorum, const ObZone& zone,
      const ObReplicaType type, int64_t& quorum) const;
  virtual int get_transform_quorum_size(const uint64_t table_id, const int64_t orig_quorum, const ObZone& zone,
      const ObReplicaType src_type, const ObReplicaType dst_type, int64_t& quorum) const;
  virtual int get_remove_replica_quorum_size(const uint64_t table_id, const int64_t orig_quorum, const ObZone& zone,
      const ObReplicaType type, int64_t& quorum) const;

private:
  int get_table_alter_quorum_replica_task(const uint64_t table_id, const int64_t orig_quorum,
      common::ObIArray<AlterPaxosLocalityTask>& alter_paxos_tasks, int64_t& add_paxos_num) const;
  int get_tablegroup_alter_quorum_replica_task(const uint64_t tablegroup_id, const int64_t orig_quorum,
      common::ObIArray<AlterPaxosLocalityTask>& alter_paxos_tasks, int64_t& add_paxos_num) const;
  int get_alter_quorum_replica_task(const uint64_t table_id, const int64_t orig_quorum,
      common::ObIArray<AlterPaxosLocalityTask>& alter_paxos_tasks, int64_t& add_paxos_num) const;
  template <typename SCHEMA>
  int inner_get_alter_quorum_replica_task(const int64_t table_id, const SCHEMA& schema,
      common::ObIArray<AlterPaxosLocalityTask>& alter_paxos_tasks, const int64_t orig_quorum,
      int64_t& add_paxos_num) const;
  int construct_zone_region_list(common::ObIArray<share::schema::ObZoneRegion>& zone_region_list,
      const common::ObIArray<common::ObZone>& zone_list) const;
  int get_tenant_pool_zone_list(const uint64_t tenant_id, common::ObIArray<common::ObZone>& pool_zone_list) const;
  int get_schema_paxos_replica_num(const uint64_t schema_id, int64_t& cur_paxos_count) const;

private:
  share::schema::ObSchemaGetterGuard* schema_guard_;
  const ObUnitManager* unit_mgr_;
  const ObZoneManager* zone_mgr_;

  DISALLOW_COPY_AND_ASSIGN(ObQuorumGetter);
};

class ObRootUtils {
public:
  ObRootUtils()
  {}
  virtual ~ObRootUtils()
  {}

  static int get_rs_default_timeout_ctx(ObTimeoutCtx& ctx);
  static int get_invalid_server_list(
      ObZoneManager& zone_mgr, ObServerManager& server_mgr, common::ObIArray<common::ObAddr>& invalid_server_list);

  static int get_stopped_zone_list(ObZoneManager& zone_mgr, ObServerManager& server_mgr,
      common::ObIArray<common::ObZone>& stopped_zone_list, common::ObIArray<common::ObAddr>& stopped_server_list);
  static int check_primary_region_in_zonelist(share::schema::ObMultiVersionSchemaService* schema_service,
      ObDDLService* ddl_service, ObUnitManager& unit_mgr, ObZoneManager& zone_mgr,
      const common::ObIArray<uint64_t>& tenant_ids, const common::ObIArray<common::ObZone>& zone_list, bool& is_in);

  static int check_left_f_outside_zonelist(
      const share::schema::ObTenantSchema& tenant_info, const common::ObIArray<common::ObZone>& zone_list, bool& has);
  static int get_primary_zone(ObZoneManager& zone_mgr,
      const common::ObIArray<share::schema::ObZoneScore>& zone_score_array,
      common::ObIArray<common::ObZone>& primary_zone);

  template <class T>
  static int check_left_f_in_primary_zone(ObZoneManager& zone_mgr, share::schema::ObSchemaGetterGuard& schema_guard,
      const T& schema_info, const common::ObIArray<common::ObZone>& zone_list, bool& has);
  static int check_tenant_schema_primary_region_in_zonelist(ObZoneManager& zone_mgr,
      const share::schema::ObTenantSchema& tenant_info, share::schema::ObSchemaGetterGuard& schema_guard,
      const common::ObIArray<common::ObZone>& zone_list, const int64_t primary_zone_count, bool& is_in);
  template <class T>
  static bool is_subset(const common::ObIArray<T>& superset_array, const common::ObIArray<T>& array);
  template <class T>
  static bool has_intersection(const common::ObIArray<T>& this_array, const common::ObIArray<T>& other_array);
  static int get_tenant_intersection(ObUnitManager& unit_mgr, common::ObIArray<common::ObAddr>& this_server_list,
      common::ObIArray<common::ObAddr>& other_server_list, common::ObIArray<uint64_t>& tenant_ids);
};

class ObClusterInfoGetter {
public:
  ObClusterInfoGetter()
  {}
  virtual ~ObClusterInfoGetter()
  {}
  static common::ObClusterType get_cluster_type_v2();
  static common::ObClusterType get_cluster_type();
};
}  // end namespace rootserver
}  // end namespace oceanbase

#ifndef FOR_BEGIN_END_E
#define FOR_BEGIN_END_E(it, obj, array, extra_condition)                                   \
  for (__typeof__((array).begin()) it = (array).begin() + (obj).begin_;                    \
       (extra_condition) && (it != (array).end()) && (it != (array).begin() + (obj).end_); \
       ++it)
#endif

#ifndef FOR_BEGIN_END
#define FOR_BEGIN_END(it, obj, array) FOR_BEGIN_END_E(it, (obj), (array), true)
#endif

// record trace events into THE one recorder
#define THE_RS_TRACE ::oceanbase::rootserver::get_rs_trace_recorder()

#define RS_TRACE(...)                         \
  do {                                        \
    if (OB_LIKELY(THE_RS_TRACE != nullptr)) { \
      REC_TRACE(*THE_RS_TRACE, __VA_ARGS__);  \
    }                                         \
  } while (0)

#define RS_TRACE_EXT(...)                        \
  do {                                           \
    if (OB_LIKELY(THE_RS_TRACE != nullptr)) {    \
      REC_TRACE_EXT(*THE_RS_TRACE, __VA_ARGS__); \
    }                                            \
  } while (0)

#endif /* _OB_ROOT_UTILS_H */
