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

#define USING_LOG_PREFIX RS_LB

#include "ob_balance_group_analyzer.h"
#include "lib/string/ob_string.h"
#include "lib/allocator/ob_allocator.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/schema/ob_table_schema.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_part_mgr_util.h"
#include "share/ob_primary_zone_util.h"
#include "ob_balance_info.h"
#include "ob_balancer_interface.h"
#include "observer/ob_server_struct.h"

using namespace oceanbase::common;
using namespace oceanbase::common::hash;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;
using namespace oceanbase::rootserver::balancer;

int DistributionUtil::check_and_set(const uint64_t id, ObHashSet<uint64_t>& processed_ids)
{
  int ret = processed_ids.exist_refactored(id);
  if (OB_HASH_EXIST == ret) {
    ret = OB_ENTRY_EXIST;
    LOG_DEBUG("id already set by others", "id", id);
  } else if (OB_HASH_NOT_EXIST == ret) {
    if (OB_FAIL(processed_ids.set_refactored(id))) {
      LOG_WARN("fail set_refactored", K(id));
    }
  } else {
    // throw out
  }
  return ret;
}

int ShardGroupAnalyzer::extract_table_name_prefix(const ObString& table, TablePrefixKey& prefix_info)
{
  int ret = OB_SUCCESS;
  // History library shadow table : "_t"
  ObString rest;
  if (OB_FAIL(BalancerStringUtil::substract_extra_suffix(table, rest, prefix_info.suffix_))) {
    LOG_WARN("fail substract extra suffix", K(table), K(ret));
  } else if (OB_FAIL(
                 BalancerStringUtil::substract_numeric_suffix(rest, prefix_info.prefix_, prefix_info.digit_length_))) {
    LOG_WARN("fail substract numeric suffix", K(table), K(ret));
  }
  return ret;
}

int ShardGroupAnalyzer::analysis(uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  const int64_t HASHMAP_SIZE = 1024;
  if (OB_FAIL(prefix_map_.create(HASHMAP_SIZE, ObModIds::OB_HASH_BUCKET_BALANCER))) {
    LOG_WARN("fail create prefix map", K(ret));
  } else if (OB_FAIL(build_prefix_map(tenant_id, prefix_map_))) {
    LOG_WARN("fail build prefix map", K(tenant_id), K(ret));
  } else if (OB_FAIL(build_shardgroup(prefix_map_))) {
    LOG_WARN("fail build shard groups", K(tenant_id), K(ret));
  }
  return ret;
}

// Prerequisites for forming a ShardGroup:
// 1. Same prefix
// 2. The suffix is a constant-width number
// 3. Either there is a tablegroup, or there is no tablegroup.
//    If there is a tablegroup, the tablegroup id must all be **not equal**
// 4. Equal partition level
// 5. The partition function types are equal
//    (the specific implementation of the function may vary)
//
// To form a ShardGroup, each table is not required:
// 1. Partition function does not require equality
// 2. The number of partitions is not required to be equal
//    (this is to ensure that the ShardGroup will not be broken up when adding partitions)
int ShardGroupAnalyzer::build_prefix_key(const ObTableSchema& table, TablePrefixKey& prefix_key)
{
  int ret = OB_SUCCESS;
  const ObString& table_name = table.get_table_name_str();
  if (OB_FAIL(extract_table_name_prefix(table_name, prefix_key))) {
    LOG_WARN("fail extract prefix", K(ret), K(table_name), K(ret));
  }
  return ret;
}

int ShardGroupAnalyzer::build_prefix_map(uint64_t tenant_id, ShardGroupPrefixMap& prefix_map)
{
  int ret = OB_SUCCESS;
  ObArray<const ObTableSchema*> table_schemas;
  // In the case of shardgroup, just process standlone table, not process tablegroup
  if (OB_FAIL(StatFinderUtil::get_need_balance_table_schemas_in_tenant(schema_guard_, tenant_id, table_schemas))) {
    LOG_WARN("fail get schemas in tenant", K(tenant_id), K(ret));
  }

  for (int64_t i = 0; i < table_schemas.count() && OB_SUCC(ret); ++i) {
    ObIArray<TablePrefixValue>* prefix_info_array = NULL;
    TablePrefixKey prefix_key;
    const ObTableSchema& table = *table_schemas.at(i);
    const ObString& table_name = table.get_table_name_str();
    if (OB_FAIL(build_prefix_key(table, prefix_key))) {
      LOG_WARN("fail extract prefix", K(ret), K(table_name), K(ret));
    } else if (prefix_key.digit_length_ <= 0) {
      continue;  // Without number suffix, it is impossible to form a shard group
    } else if (OB_FAIL(ob_write_string(allocator_, prefix_key.prefix_, prefix_key.prefix_))) {
      LOG_WARN("fail deep copy prefix", K(ret), K(prefix_key));
    } else if (OB_FAIL(ob_write_string(allocator_, prefix_key.suffix_, prefix_key.suffix_))) {
      LOG_WARN("fail deep copy suffix", K(ret), K(prefix_key));
    } else {
      TablePrefixValue prefix_value;
      prefix_value.table_id_ = table.get_table_id();
      prefix_value.tablegroup_id_ = table.get_tablegroup_id();
      prefix_value.schema_ = &table;
      if (OB_FAIL(prefix_map.get_refactored(prefix_key, prefix_info_array))) {
        if (OB_HASH_NOT_EXIST == ret) {
          void* ptr = NULL;
          if (NULL == (ptr = allocator_.alloc(sizeof(ObArray<TablePrefixValue>)))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("fail alloc memory", K(ret));
          } else if (NULL == (prefix_info_array = new (ptr) ObArray<TablePrefixValue, ObIAllocator&>(
                                  OB_MALLOC_NORMAL_BLOCK_SIZE, allocator_))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("fail create array", K(ret));
          } else if (OB_FAIL(prefix_info_array->push_back(prefix_value))) {
            LOG_WARN("fail push back table_id to array", K(prefix_value), K(ret));
          } else if (OB_FAIL(prefix_map.set_refactored(prefix_key, prefix_info_array))) {
            LOG_WARN("fail create item in hashmap", K(prefix_key), K(ret));
          } else {
            LOG_DEBUG("created a prefix bucket", K(table_name), K(prefix_key), K(prefix_value));
          }
        } else {
          LOG_WARN("fail get item from hashmap", K(prefix_key), K(ret));
        }
      } else if (OB_FAIL(prefix_info_array->push_back(prefix_value))) {
        LOG_WARN("fail push back table_id to array", K(prefix_value), K(ret));
      } else if (OB_FAIL(prefix_map.set_refactored(prefix_key, prefix_info_array, 1))) {
        LOG_WARN("fail create item in hashmap", K(prefix_key), K(ret));
      } else {
        LOG_DEBUG("pattern recall", K(table_name), K(prefix_key), K(prefix_value), K(*prefix_info_array));
      }
    }
  }
  return ret;
}

class SortedShardGroupArray {
public:
  SortedShardGroupArray(ShardGroupPrefixMap& prefix_map);
  int sort();
  ObArray<ObIArray<TablePrefixValue>*>& array()
  {
    return prefix_infos_;
  }

private:
  ShardGroupPrefixMap& prefix_map_;
  ObArray<TablePrefixKey*> sorted_prefix_;
  ObArray<ObIArray<TablePrefixValue>*> prefix_infos_;
  bool sorted_;
};

SortedShardGroupArray::SortedShardGroupArray(ShardGroupPrefixMap& prefix_map) : prefix_map_(prefix_map), sorted_(false)
{}

struct ShardGroupPrefixOrder {
  explicit ShardGroupPrefixOrder(int& ret, ShardGroupPrefixMap& prefix_map) : ret_(ret), prefix_map_(prefix_map)
  {}

  bool operator()(const TablePrefixKey* left, const TablePrefixKey* right)
  {
    int cmp = -1;
    int& ret = ret_;
    ObIArray<TablePrefixValue>* left_array = NULL;
    ObIArray<TablePrefixValue>* right_array = NULL;
    if (OB_UNLIKELY(OB_SUCCESS != ret)) {
      // error happen, do nothing
    } else if (OB_ISNULL(left) || OB_ISNULL(right)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL unexpected", KP(left), KP(right), K(ret));
    } else if (OB_FAIL(prefix_map_.get_refactored(*left, left_array))) {
      LOG_WARN("fail get from map", K(*left), K(ret));
    } else if (OB_FAIL(prefix_map_.get_refactored(*right, right_array))) {
      LOG_WARN("fail get from map", K(*right), K(ret));
    } else if (OB_ISNULL(left_array) || OB_ISNULL(right_array)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL unexpected", KP(left_array), KP(right_array), K(ret));
    } else {
      uint64_t min_left_tid = get_min_tid(*left_array);
      uint64_t min_right_tid = get_min_tid(*right_array);
      cmp = min_left_tid < min_right_tid ? -1 : (min_left_tid == min_right_tid ? 0 : 1);
    }
    return cmp < 0;
  }

private:
  uint64_t get_min_tid(ObIArray<TablePrefixValue>& tables)
  {
    uint64_t tid = OB_INVALID_ID;
    ARRAY_FOREACH_NORET(tables, idx)
    {
      if (tables.at(idx).table_id_ < tid) {
        tid = tables.at(idx).table_id_;
      }
    }
    return tid;
  }

private:
  int& ret_;
  ShardGroupPrefixMap& prefix_map_;
};

int SortedShardGroupArray::sort()
{
  int ret = OB_SUCCESS;
  if (!sorted_) {
    FOREACH_X(item, prefix_map_, OB_SUCC(ret))
    {
      if (OB_FAIL(sorted_prefix_.push_back(&item->first))) {
        LOG_WARN("fail push to sorted prefix", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      std::sort(sorted_prefix_.begin(), sorted_prefix_.end(), ShardGroupPrefixOrder(ret, prefix_map_));
    }
    if (OB_SUCC(ret)) {
      FOREACH_X(prefix_key, sorted_prefix_, OB_SUCC(ret))
      {
        ObIArray<TablePrefixValue>* prefix_info_array = NULL;
        if (OB_ISNULL(*prefix_key)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("NULL unexpected", K(ret));
        } else if (OB_FAIL(prefix_map_.get_refactored(**prefix_key, prefix_info_array))) {
          LOG_WARN("fail get from map", K(**prefix_key), K(ret));
        } else if (OB_ISNULL(prefix_info_array)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("NULL unexpected", K(ret));
        } else if (OB_FAIL(prefix_infos_.push_back(prefix_info_array))) {
          LOG_WARN("fail push data", K(ret));
        }
      }
    }
    sorted_ = true;
  }
  return ret;
}

int ShardGroupAnalyzer::build_shardgroup(ShardGroupPrefixMap& prefix_map)
{
  int ret = OB_SUCCESS;
  const int64_t MIN_SHARDING_GROUP_TABLE_COUNT = 2;

  // Reasons for sorting shard group:
  // 1. When constructing a shardgroup,
  //    choose the shard with the smallest table id as the leader,
  //    The rest of the same tablegroup as a follower.
  // 2. Choose the table with the smallest table id
  //    instead of the table with the least or most partitions is for sorting stability,
  //    and will not be broken up due to adding new tables or changing the number of partitions
  //
  SortedShardGroupArray all_shardgroups(prefix_map);
  if (OB_FAIL(all_shardgroups.sort())) {
    LOG_WARN("fail build prefix array", K(ret));
  }

  ObArray<ShardGroup*> shardgroups;
  if (OB_SUCC(ret) && all_shardgroups.array().count() > 0) {
    if (OB_FAIL(build_shardgroups(all_shardgroups.array(), shardgroups))) {
      LOG_WARN("fail build shard shardgroup", K(ret));
    }
  }

  FOREACH_X(shardgroup, shardgroups, OB_SUCC(ret))
  {
    bool skip = false;
    // Add shardgroup leader to shardgroups for external iteration
    if ((*shardgroup)->count() > 0) {
      int64_t leader_shard_idx = (*shardgroup)->at(0);
      if (leader_shard_idx >= all_shardgroups.array().count()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Unexpected index", K(leader_shard_idx), K(ret));
      } else if (all_shardgroups.array().at(leader_shard_idx)->count() < MIN_SHARDING_GROUP_TABLE_COUNT) {
        skip = true;
        // skip
      } else if (OB_FAIL(shard_groups_.push_back(all_shardgroups.array().at(leader_shard_idx)))) {
        LOG_WARN("fail push back shardgroup", K(ret));
      }
    }
    // Both the shardgroup leader and follower are marked as processed,
    // and the following TableGroupIdMap should not balance these tables
    if (OB_SUCC(ret) && !skip && (*shardgroup)->count() > 0) {
      ShardGroup& f = **shardgroup;
      // Mark the entire shardgroup table to be processed
      ARRAY_FOREACH_X(f, idx, cnt, OB_SUCC(ret))
      {
        int64_t shard_idx = f.at(idx);
        if (shard_idx >= all_shardgroups.array().count()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("Unexpected index", K(shard_idx), K(ret));
        } else {
          ObIArray<TablePrefixValue>& tables = *all_shardgroups.array().at(shard_idx);
          ARRAY_FOREACH_X(tables, idx, cnt, OB_SUCC(ret))
          {
            if (OB_FAIL(DistributionUtil::check_and_set(tables.at(idx).table_id_, processed_tids_))) {
              LOG_WARN("fail check and set table idset", K(ret));
            }
          }
        }
      }
    }
  }
  return ret;
}

int ShardGroupAnalyzer::build_shardgroups(
    ObArray<ObIArray<TablePrefixValue>*>& shards, ObIArray<ShardGroup*>& shardgroups)
{
  int ret = OB_SUCCESS;

  ObHashMap<uint64_t, int64_t> shardgroup_map;  // tablegroup_id => shardgroup_idx
  if (OB_FAIL(shardgroup_map.create(shards.count(), ObModIds::OB_HASH_BUCKET_BALANCER))) {
    LOG_WARN("fail create table id set", "size", shards.count(), K(ret));
  }
  ARRAY_FOREACH_X(shards, idx, cnt, OB_SUCC(ret))
  {
    if (OB_FAIL(add_to_shardgroup(*shards.at(idx), idx, shardgroup_map, shardgroups))) {
      LOG_WARN("fail build shard shardgroup", K(idx), K(cnt), K(ret));
    }
  }
  return ret;
}

int ShardGroupAnalyzer::add_to_shardgroup(ObIArray<TablePrefixValue>& cur_shard, int64_t cur_shard_idx,
    ObHashMap<uint64_t, int64_t>& shardgroup_map, ObIArray<ShardGroup*>& shardgroups)
{
  int ret = OB_SUCCESS;
  int64_t shardgroup_idx = OB_INVALID_INDEX_INT64;
  // Check the tablegroup_id of each table in cur_shard,
  // if you find a shardgroup with it, add its subscript to the corresponding shardgroup.
  // If all tablegroup_id does not belong to any known shardgroup, create a shardgroup and join
  bool is_follower = false;
  ARRAY_FOREACH_X(cur_shard, idx, cnt, OB_SUCC(ret))
  {
    TablePrefixValue& v = cur_shard.at(idx);
    if (OB_INVALID_ID == v.tablegroup_id_) {
      // skip
    } else if (OB_SUCC(shardgroup_map.get_refactored(v.tablegroup_id_, shardgroup_idx))) {
      ShardGroup* shardgroup = NULL;
      if (shardgroup_idx >= shardgroups.count()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("idx unexpected", K(shardgroup_idx), "size", shardgroups.count(), K(ret));
      } else if (NULL == (shardgroup = shardgroups.at(shardgroup_idx))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL unexpected", K(ret));
      } else if (OB_FAIL(shardgroup->push_back(cur_shard_idx))) {
        LOG_WARN("fail add same shard group shard to shardgroup", K(ret));
      }
      is_follower = true;
      // Note: As long as there is a table whose tablegroup is the same as the leader,
      //       it will be gathered into the shardgroup
      break;
    } else if (OB_HASH_NOT_EXIST == ret) {
      // nop
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail lookup hash map", K(v), K(ret));
    }
  }

  // new shardgroup leader
  if (OB_SUCC(ret) && false == is_follower) {
    void* ptr = NULL;
    ShardGroup* shardgroup = NULL;
    if (NULL == (ptr = allocator_.alloc(sizeof(ShardGroup)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("OOM", K(ret));
    } else if (NULL == (shardgroup = new (ptr) ShardGroup(OB_MALLOC_NORMAL_BLOCK_SIZE, allocator_))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Should never reach here", K(ret));
    } else if (OB_FAIL(shardgroup->push_back(cur_shard_idx))) {
      LOG_WARN("fail add idx to shardgroup", K(ret));
    } else if (OB_FAIL(shardgroups.push_back(shardgroup))) {
      LOG_WARN("fail push back shardgroup ptr", K(ret));
    } else {
      int64_t new_shardgroup_idx = shardgroups.count() - 1;
      ARRAY_FOREACH_X(cur_shard, idx, cnt, OB_SUCC(ret))
      {
        TablePrefixValue& v = cur_shard.at(idx);
        if (OB_INVALID_ID == v.tablegroup_id_) {
          // skip
        } else if (OB_FAIL(shardgroup_map.set_refactored(v.tablegroup_id_, new_shardgroup_idx))) {
          if (OB_HASH_EXIST == ret) {
            ret = OB_SUCCESS;
            LOG_WARN("Table in same shard have same tablegroup, WRONG FORMAT. IGNORE!", K(v), K(ret));
          } else {
            LOG_WARN("fail set tablegroup id to shardgroup map", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ShardGroupAnalyzer::next(ObIArray<const ObTableSchema*>& schemas)
{
  int ret = OB_SUCCESS;
  if (cur_shard_group_idx_ >= shard_groups_.count()) {
    ret = OB_ITER_END;
  } else {
    if (OB_ISNULL(shard_groups_.at(cur_shard_group_idx_))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null ptr", K(ret));
      // don't move forward cur_shard_group_idx_;
    } else {
      ObIArray<TablePrefixValue>& arr = *shard_groups_.at(cur_shard_group_idx_);
      schemas.reset();
      ARRAY_FOREACH_X(arr, idx, cnt, OB_SUCC(ret))
      {
        if (OB_FAIL(schemas.push_back(arr.at(idx).schema_))) {
          LOG_WARN("fail push back value", K(ret));
        }
      }
      cur_shard_group_idx_++;
    }
  }
  return ret;
}

int TableGroupAnalyzer::pick_sample_table_schema(
    const common::ObIArray<const share::schema::ObSimpleTableSchemaV2*>& table_schemas,
    const share::schema::ObSimpleTableSchemaV2*& sample_table_schema, bool& tg_processed)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(table_schemas.count() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else {
    tg_processed = false;
    sample_table_schema = table_schemas.at(0);
    for (int64_t i = 0; !tg_processed && OB_SUCC(ret) && i < table_schemas.count(); ++i) {
      const share::schema::ObSimpleTableSchemaV2* this_schema = table_schemas.at(i);
      if (OB_UNLIKELY(NULL == this_schema || NULL == sample_table_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("this schema ptr is null", K(ret), KP(this_schema), KP(sample_table_schema));
      } else if (OB_FAIL(check_table_already_processed(this_schema->get_table_id(), tg_processed))) {
        LOG_WARN("fail to check table already processed", K(ret));
      } else if (tg_processed) {
        // jump out
      } else {
        int64_t part_num = 0;
        int64_t sample_part_num = 0;
        if (OB_FAIL(this_schema->get_all_partition_num(true, part_num))) {
          LOG_WARN("fail to get all partition num", K(ret), KPC(this_schema));
        } else if (OB_FAIL(sample_table_schema->get_all_partition_num(true, sample_part_num))) {
          LOG_WARN("fail to get all partition num", K(ret), KPC(sample_table_schema));
        } else if (part_num > sample_part_num) {
          sample_table_schema = this_schema;
        } else if (part_num == sample_part_num) {
          if (this_schema->get_table_id() < sample_table_schema->get_table_id()) {
            sample_table_schema = this_schema;
          }
        } else {  // part_num < sample_part_num
          // do not update sample table schema
        }
      }
    }
  }
  return ret;
}

int TableGroupAnalyzer::check_table_already_processed(const uint64_t partition_entity_id, bool& table_processed)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_INVALID_ID == partition_entity_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(partition_entity_id));
  } else {
    int tmp_ret = processed_tids_.exist_refactored(partition_entity_id);
    if (OB_HASH_NOT_EXIST == tmp_ret) {
      table_processed = false;
    } else if (OB_HASH_EXIST == tmp_ret) {
      table_processed = true;
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to do processed tid exist refactored", K(ret));
    }
  }
  return ret;
}

int TableGroupAnalyzer::analysis(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  common::ObArray<uint64_t> tablegroup_ids;
  if (OB_UNLIKELY(OB_INVALID_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id));
  } else if (OB_FAIL(schema_guard_.get_tablegroup_ids_in_tenant(tenant_id, tablegroup_ids))) {
    if (OB_TENANT_NOT_EXIST == ret && GCTX.is_standby_cluster()) {
      // When building a tenant in the standby cluster,
      // the tenant-level system table partition needs to be supplemented by RS.
      // At this time, the tablegroup schema cannot be obtained and needs to be ignored.
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to get tablegroup ids in tenant", K(ret), K(tenant_id));
    }
  } else {
    sample_partition_schemas_.reset();
    common::ObArray<const share::schema::ObSimpleTableSchemaV2*> table_schemas;
    for (int64_t i = 0; OB_SUCC(ret) && i < tablegroup_ids.count(); ++i) {
      bool tg_processed = false;
      table_schemas.reset();
      const share::schema::ObSimpleTableSchemaV2* sample_table_schema = nullptr;
      const share::schema::ObTablegroupSchema* this_tg_schema = nullptr;
      const uint64_t tablegroup_id = tablegroup_ids.at(i);
      int64_t part_num = 0;
      if (OB_UNLIKELY(OB_INVALID_ID == tablegroup_id)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("tablegroup id unexpected", K(ret));
      } else if (extract_pure_id(tablegroup_id) == OB_SYS_TABLEGROUP_ID) {
        // Ignore all_dummy and related tables
      } else if (OB_FAIL(schema_guard_.get_tablegroup_schema(tablegroup_id, this_tg_schema))) {
        LOG_WARN("fail to get tablegroup schema", K(ret), K(tablegroup_id));
      } else if (OB_UNLIKELY(nullptr == this_tg_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("this tg schema ptr is null", K(ret));
      } else if (this_tg_schema->has_self_partition()) {
        // no need to check processed tids
        if (OB_FAIL(this_tg_schema->get_all_partition_num(true, part_num))) {
          LOG_WARN("fail to get all partition num", K(ret), K(part_num));
        } else if (part_num <= 1) {
          // Single partition pg is processed in NonPartition
          // bypass
        } else if (OB_FAIL(sample_partition_schemas_.push_back(this_tg_schema))) {
          LOG_WARN("fail to push back", K(ret));
        }
      } else if (OB_FAIL(schema_guard_.get_table_schemas_in_tablegroup(tenant_id, tablegroup_id, table_schemas))) {
        LOG_WARN("fail to get table schemas in tablegroup", K(ret));
      } else if (table_schemas.count() <= 0) {
        // bypass
      } else if (OB_FAIL(pick_sample_table_schema(table_schemas, sample_table_schema, tg_processed))) {
        LOG_WARN("fail to pick sample table schema", K(ret));
      } else if (tg_processed) {
        // bypass, this tg has been processed in shardgroup
      } else if (OB_UNLIKELY(nullptr == sample_table_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("sample table schema ptr is null", K(ret));
      } else if (OB_FAIL(sample_table_schema->get_all_partition_num(true, part_num))) {
        LOG_WARN("fail to get all partition num", K(ret), K(part_num));
      } else if (part_num <= 0) {
        // bypass
      } else if (OB_FAIL(sample_partition_schemas_.push_back(sample_table_schema))) {
        LOG_WARN("fail to push back", K(ret));
      }
    }
  }
  return ret;
}

int TableGroupAnalyzer::next(const share::schema::ObPartitionSchema*& partition_schema)
{
  int ret = OB_SUCCESS;
  partition_schema = nullptr;
  if (cur_idx_ < sample_partition_schemas_.count()) {
    partition_schema = sample_partition_schemas_.at(cur_idx_++);
  } else {
    ret = OB_ITER_END;
  }
  return ret;
}

int PartitionTableAnalyzer::analysis(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  common::ObArray<const share::schema::ObTableSchema*> my_table_schemas;
  if (OB_UNLIKELY(OB_INVALID_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id));
  } else if (OB_FAIL(StatFinderUtil::get_need_balance_table_schemas_in_tenant(
                 schema_guard_, tenant_id, my_table_schemas))) {
    LOG_WARN("fail to get need balance table schemas");
  } else {
    table_schemas_.reset();
    for (int64_t i = 0; OB_SUCC(ret) && i < my_table_schemas.count(); ++i) {
      const share::schema::ObTableSchema* table_schema = my_table_schemas.at(i);
      int64_t part_num = 0;
      if (OB_UNLIKELY(nullptr == table_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table schema ptr is null", K(ret));
      } else if (OB_FAIL(table_schema->get_all_partition_num(true, part_num))) {
        LOG_WARN("fail to get all partition num", K(ret), KPC(table_schema));
      } else if (part_num > 1 && OB_INVALID_ID == table_schema->get_tablegroup_id() &&
                 rootserver::ObTenantUtils::is_balance_target_schema(*table_schema)) {
        ret = processed_tids_.exist_refactored(table_schema->get_table_id());
        if (OB_HASH_EXIST == ret) {
          ret = OB_SUCCESS;  // already processed
        } else if (OB_HASH_NOT_EXIST == ret) {
          if (OB_FAIL(table_schemas_.push_back(table_schema))) {
            LOG_WARN("fail to push back", K(ret));
          }
        } else {
          LOG_WARN("set processed table id unexpected error", K(ret));
        }
      }
    }
  }
  return ret;
}

int PartitionTableAnalyzer::next(const share::schema::ObTableSchema*& table_schema)
{
  int ret = OB_SUCCESS;
  table_schema = nullptr;
  if (cur_table_idx_ < table_schemas_.count()) {
    table_schema = table_schemas_.at(cur_table_idx_++);
  } else {
    ret = OB_ITER_END;
  }
  return ret;
}

int NonPartitionTableAnalyzer::analysis(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  common::ObArray<const share::schema::ObTableSchema*> table_schemas;
  common::ObArray<const share::schema::ObTablegroupSchema*> tg_schemas;
  common::ObArray<const share::schema::ObPartitionSchema*> target_schemas;
  if (OB_UNLIKELY(OB_INVALID_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id));
  } else if (OB_FAIL(
                 StatFinderUtil::get_need_balance_table_schemas_in_tenant(schema_guard_, tenant_id, table_schemas))) {
    LOG_WARN("fail to get need balance table schemas in tenant", K(ret), K(tenant_id));
  } else if (OB_FAIL(schema_guard_.get_tablegroup_schemas_in_tenant(tenant_id, tg_schemas))) {
    LOG_WARN("fail to get tablegroup schemas in tenant", K(ret), K(tenant_id));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < table_schemas.count(); ++i) {
      const ObTableSchema* schema = table_schemas.at(i);
      int64_t part_num = 0;
      if (OB_UNLIKELY(NULL == schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("schema ptr is null", K(ret));
      } else if (OB_FAIL(schema->get_all_partition_num(true, part_num))) {
        LOG_WARN("fail to get all partition num", K(ret), KPC(schema));
      } else if (part_num <= 1 && schema->has_self_partition() &&
                 schema->get_duplicate_scope() == ObDuplicateScope::DUPLICATE_SCOPE_NONE &&
                 share::OB_ALL_DUMMY_TID != extract_pure_id(schema->get_table_id()) &&
                 OB_INVALID_ID == schema->get_tablegroup_id() &&
                 rootserver::ObTenantUtils::is_balance_target_schema(*schema)) {
        ret = processed_tids_.exist_refactored(schema->get_table_id());
        if (OB_HASH_EXIST == ret) {
          ret = OB_SUCCESS;  // already processed
        } else if (OB_HASH_NOT_EXIST == ret) {
          if (OB_FAIL(target_schemas.push_back(schema))) {
            LOG_WARN("fail to push back", K(ret));
          } else if (OB_FAIL(DistributionUtil::check_and_set(schema->get_table_id(), processed_tids_))) {
            LOG_WARN("fail to check and set processed id", K(ret));
          }
        } else {
          LOG_WARN("set processed table id unexpected error", K(ret));
        }
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < tg_schemas.count(); ++i) {
      const ObTablegroupSchema* schema = tg_schemas.at(i);
      int64_t part_num = 0;
      if (OB_UNLIKELY(nullptr == schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("schema ptr is null", K(ret));
      } else if (OB_FAIL(schema->get_all_partition_num(true, part_num))) {
        LOG_WARN("fail to get all partition num", K(ret), KPC(schema));
      } else if (part_num > 1 || !schema->has_self_partition()) {
        // by pass
      } else {
        if (OB_FAIL(target_schemas.push_back(schema))) {
          LOG_WARN("fail to push back", K(ret));
        } else if (OB_FAIL(DistributionUtil::check_and_set(schema->get_tablegroup_id(), processed_tids_))) {
          LOG_WARN("fail to check and set processed id", K(ret));
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else if (target_schemas.count() <= 0) {
      // bypass
    } else if (OB_FAIL(inner_array_.push_back(target_schemas))) {
      LOG_WARN("fail to push back", K(ret));
    } else {
    }  // no more
  }
  return ret;
}

int NonPartitionTableAnalyzer::next(common::ObIArray<const share::schema::ObPartitionSchema*>& partition_schemas)
{
  int ret = OB_SUCCESS;
  if (cur_iter_idx_ < inner_array_.count()) {
    partition_schemas.reset();
    if (OB_FAIL(partition_schemas.assign(inner_array_.at(cur_iter_idx_++)))) {
      LOG_WARN("fail to assign table schema array", K(ret));
    }
  } else {
    ret = OB_ITER_END;
  }
  return ret;
}
