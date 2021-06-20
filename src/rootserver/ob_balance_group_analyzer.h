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

#ifndef _OB_BALANCE_GROUP_ANALYZER_H
#define _OB_BALANCE_GROUP_ANALYZER_H 1

#include "ob_root_utils.h"
#include "lib/container/ob_se_array.h"
#include "lib/container/ob_array.h"
#include "lib/hash/ob_hashset.h"
#include "lib/hash/ob_hashmap.h"
#include "lib/hash/ob_refered_map.h"
#include "share/schema/ob_schema_struct.h"

namespace oceanbase {

namespace share {
namespace schema {
class ObTableSchema;
class ObSchemaGetterGuard;
}  // namespace schema
}  // namespace share

namespace common {
class ObIAllocator;
}

namespace rootserver {
namespace balancer {

class ITenantStatFinder;

// Prefix_prefix_..._prefix_number_suffix
class TablePrefixKey {
public:
  TablePrefixKey() : prefix_(), suffix_(), digit_length_(0)
  {}

  common::ObString prefix_;
  common::ObString suffix_;
  int64_t digit_length_;
  int64_t hash() const
  {
    return prefix_.hash() ^ suffix_.hash() ^ (digit_length_);
  }
  bool operator==(const TablePrefixKey& other) const
  {
    return digit_length_ == other.digit_length_ && prefix_ == other.prefix_ && suffix_ == other.suffix_;
  }
  TO_STRING_KV(K_(prefix), K_(suffix), K_(digit_length));
};

class TablePrefixValue {
public:
  uint64_t tablegroup_id_;
  uint64_t table_id_;
  const share::schema::ObTableSchema* schema_;
  TO_STRING_KV(K_(tablegroup_id), K_(table_id));
};

class DistributionUtil {
public:
  static int check_and_set(const uint64_t id, common::hash::ObHashSet<uint64_t>& processed_tids);
};

typedef common::ObArray<int64_t, common::ObIAllocator&> ShardGroup;
typedef common::hash::ObHashMap<TablePrefixKey, common::ObIArray<TablePrefixValue>*> ShardGroupPrefixMap;

class ShardGroupAnalyzer {
public:
  ShardGroupAnalyzer(share::schema::ObSchemaGetterGuard& schema_guard, ITenantStatFinder& stat_finder,
      common::ObIAllocator& allocator, common::hash::ObHashSet<uint64_t>& processed_tids)
      : schema_guard_(schema_guard),
        stat_finder_(stat_finder),
        allocator_(allocator),
        processed_tids_(processed_tids),
        prefix_map_(),
        shard_groups_(),
        cur_shard_group_idx_(0)
  {}
  int analysis(uint64_t tenant_id);
  // Iterate the ShardGroup.
  // Note: If there are multiple tables belonging to the same table group,
  //       only the primary table in the table group is taken as a representative and stored in tids
  int next(common::ObIArray<const share::schema::ObTableSchema*>& schemas);

private:
  int build_prefix_map(uint64_t tenant_id, ShardGroupPrefixMap& prefix_map);
  int build_shardgroup(ShardGroupPrefixMap& prefix_map);
  int build_shardgroups(
      common::ObArray<common::ObIArray<TablePrefixValue>*>& shards, common::ObIArray<ShardGroup*>& shardgroups);
  int add_to_shardgroup(common::ObIArray<TablePrefixValue>& cur_shard, int64_t cur_shard_idx,
      common::hash::ObHashMap<uint64_t, int64_t>& shardgroup_map, common::ObIArray<ShardGroup*>& shardgroups);
  int extract_table_name_prefix(const common::ObString& table, TablePrefixKey& prefix_info);
  int build_prefix_key(const share::schema::ObTableSchema& table, TablePrefixKey& prefix_key);

private:
  share::schema::ObSchemaGetterGuard& schema_guard_;
  ITenantStatFinder& stat_finder_;
  common::ObIAllocator& allocator_;
  common::hash::ObHashSet<uint64_t>& processed_tids_;
  common::hash::ObHashMap<TablePrefixKey, common::ObIArray<TablePrefixValue>*> prefix_map_;
  common::ObArray<common::ObIArray<TablePrefixValue>*> shard_groups_;
  int64_t cur_shard_group_idx_;
};

class TableGroupAnalyzer {
public:
  TableGroupAnalyzer(share::schema::ObSchemaGetterGuard& schema_guard, ITenantStatFinder& stat_finder,
      common::ObIAllocator& allocator, common::hash::ObHashSet<uint64_t>& processed_tids)
      : schema_guard_(schema_guard),
        stat_finder_(stat_finder),
        allocator_(allocator),
        processed_tids_(processed_tids),
        sample_partition_schemas_(),
        cur_idx_(0)
  {}
  int analysis(const uint64_t tenant_id);
  int next(const share::schema::ObPartitionSchema*& leader_schema);

private:
  int pick_sample_table_schema(const common::ObIArray<const share::schema::ObSimpleTableSchemaV2*>& table_schemas,
      const share::schema::ObSimpleTableSchemaV2*& sample_table_schema, bool& tg_processed);
  int check_table_already_processed(const uint64_t partition_entity_id, bool& table_processed);

private:
  share::schema::ObSchemaGetterGuard& schema_guard_;
  ITenantStatFinder& stat_finder_;
  common::ObIAllocator& allocator_;
  common::hash::ObHashSet<uint64_t>& processed_tids_;
  common::ObArray<const share::schema::ObPartitionSchema*> sample_partition_schemas_;
  int64_t cur_idx_;
};

class PartitionTableAnalyzer {
public:
  PartitionTableAnalyzer(share::schema::ObSchemaGetterGuard& schema_guard, ITenantStatFinder& stat_finder,
      common::ObIAllocator& allocator, common::hash::ObHashSet<uint64_t>& processed_tids)
      : schema_guard_(schema_guard),
        stat_finder_(stat_finder),
        allocator_(allocator),
        processed_tids_(processed_tids),
        table_schemas_(),
        cur_table_idx_(0)
  {}
  int analysis(const uint64_t tenant_id);
  int next(const share::schema::ObTableSchema*& table_schema);

private:
  share::schema::ObSchemaGetterGuard& schema_guard_;
  ITenantStatFinder& stat_finder_;
  common::ObIAllocator& allocator_;
  common::hash::ObHashSet<uint64_t>& processed_tids_;
  common::ObArray<const share::schema::ObTableSchema*> table_schemas_;
  int64_t cur_table_idx_;
};

class NonPartitionTableAnalyzer {
public:
  NonPartitionTableAnalyzer(share::schema::ObSchemaGetterGuard& schema_guard, ITenantStatFinder& stat_finder,
      common::ObIAllocator& allocator, common::hash::ObHashSet<uint64_t>& processed_tids)
      : schema_guard_(schema_guard),
        stat_finder_(stat_finder),
        allocator_(allocator),
        processed_tids_(processed_tids),
        inner_array_(),
        cur_iter_idx_(0)
  {}
  int analysis(const uint64_t tenant_id);
  int next(common::ObIArray<const share::schema::ObPartitionSchema*>& schemas);

private:
  typedef common::ObArray<const share::schema::ObPartitionSchema*> NonPartitionArray;

private:
  share::schema::ObSchemaGetterGuard& schema_guard_;
  ITenantStatFinder& stat_finder_;
  common::ObIAllocator& allocator_;
  common::hash::ObHashSet<uint64_t>& processed_tids_;
  common::ObArray<NonPartitionArray> inner_array_;
  int64_t cur_iter_idx_;
};
}  // end namespace balancer
}  // end namespace rootserver
}  // end namespace oceanbase

#endif /* _OB_BALANCE_GROUP_ANALYZER_H */
