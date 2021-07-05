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

#ifndef _OB_BALANCE_GROUP_CONTAINER_H_
#define _OB_BALANCE_GROUP_CONTAINER_H_ 1

#include "ob_root_utils.h"
#include "ob_balancer_interface.h"
#include "ob_balance_group_data.h"
#include "lib/container/ob_array.h"
#include "lib/container/ob_se_array.h"
#include "lib/hash/ob_hashmap.h"
#include "lib/hash/ob_hashset.h"
#include "lib/allocator/page_arena.h"
#include "common/ob_zone.h"
#include "common/rowkey/ob_rowkey.h"
#include "common/rowkey/ob_rowkey_info.h"
#include "common/ob_partition_key.h"
#include "share/schema/ob_schema_struct.h"

namespace oceanbase {

namespace share {
namespace schema {
class ObTableSchema;
class ObSchemaGetterGuard;
class ObPartition;
}  // namespace schema
}  // namespace share
namespace common {
class ObIAllocator;
}

namespace rootserver {
namespace balancer {

class ITenantStatFinder;

enum PartitionDistributePolicy { DISTRIBUTE_BY_AUTO = 0, DISTRIBUTE_BY_LEVEL_ONE, DISTRIBUTE_BY_LEVEL_TWO };

class PartitionSchemaChecker {
public:
  static int check_same_primary_zone(const share::schema::ObPartitionSchema& ref,
      const share::schema::ObPartitionSchema& other, share::schema::ObSchemaGetterGuard& schema_guard, bool& same);
  static int check_same_primary_zone(const common::ObIArray<const share::schema::ObPartitionSchema*>& partition_schemas,
      share::schema::ObSchemaGetterGuard& schema_guard, bool& same);
  static int check_same_primary_zone(const common::ObIArray<const share::schema::ObTableSchema*>& partition_schemas,
      share::schema::ObSchemaGetterGuard& schema_guard, bool& same);
  static bool is_same_partition_schema(
      const share::schema::ObTableSchema& ref, const share::schema::ObTableSchema& other);
  static bool is_same_schema_partition_key_info(const common::ObRowkeyInfo& left, const common::ObRowkeyInfo& right);
  static bool is_one_level_and_partition_by_range(common::ObIArray<const share::schema::ObTableSchema*>& schemas);
};

class ShardGroupValidator {
public:
  static int check_table_schemas_compatible(ITenantStatFinder& stat_finder,
      share::schema::ObSchemaGetterGuard& schema_guard,
      const common::ObIArray<const share::schema::ObTableSchema*>& tables, bool& compatible,
      const bool check_primary_zone = false);
};

class IBalanceGroupContainer;

class IBalanceGroupContainerBuilder {
public:
  IBalanceGroupContainerBuilder(share::schema::ObSchemaGetterGuard& schema_guard, ITenantStatFinder& stat_finder,
      common::hash::ObHashSet<uint64_t>& processed_tids, int64_t& max_used_balance_group_id,
      common::ObIAllocator& allocator, IBalanceGroupContainer& balance_group_container)
      : inited_(false),
        tenant_id_(common::OB_INVALID_ID),
        schema_guard_(schema_guard),
        stat_finder_(stat_finder),
        processed_tids_(processed_tids),
        max_used_balance_group_id_(max_used_balance_group_id),
        allocator_(allocator),
        balance_group_container_(balance_group_container)
  {}
  virtual ~IBalanceGroupContainerBuilder()
  {}

public:
  int init(const uint64_t tenant_id);
  virtual int build() = 0;

protected:
  bool inited_;
  uint64_t tenant_id_;
  share::schema::ObSchemaGetterGuard& schema_guard_;
  ITenantStatFinder& stat_finder_;
  common::hash::ObHashSet<uint64_t>& processed_tids_;
  int64_t& max_used_balance_group_id_;
  common::ObIAllocator& allocator_;
  IBalanceGroupContainer& balance_group_container_;
};

class PartitionTableContainerBuilder : public IBalanceGroupContainerBuilder {
public:
  PartitionTableContainerBuilder(share::schema::ObSchemaGetterGuard& schema_guard, ITenantStatFinder& stat_finder,
      common::hash::ObHashSet<uint64_t>& processed_tids, int64_t& max_used_balance_group_id,
      common::ObIAllocator& allocator, IBalanceGroupContainer& balance_group_container)
      : IBalanceGroupContainerBuilder(
            schema_guard, stat_finder, processed_tids, max_used_balance_group_id, allocator, balance_group_container)
  {}
  virtual ~PartitionTableContainerBuilder()
  {}

public:
  int build() override;

private:
  int build_partition_table_container(const share::schema::ObTableSchema* table_schema);
};

class NonPartitionTableContainerBuilder : public IBalanceGroupContainerBuilder {
public:
  NonPartitionTableContainerBuilder(share::schema::ObSchemaGetterGuard& schema_guard, ITenantStatFinder& stat_finder,
      common::hash::ObHashSet<uint64_t>& processed_tids, int64_t& max_used_balance_group_id,
      common::ObIAllocator& allocator, IBalanceGroupContainer& balance_group_container)
      : IBalanceGroupContainerBuilder(
            schema_guard, stat_finder, processed_tids, max_used_balance_group_id, allocator, balance_group_container)
  {}
  virtual ~NonPartitionTableContainerBuilder()
  {}

public:
  int build() override;

private:
  int build_non_partition_table_container(
      const common::ObIArray<const share::schema::ObPartitionSchema*>& partition_schemas);
};

class ShardGroupContainerBuilder : public IBalanceGroupContainerBuilder {
public:
  ShardGroupContainerBuilder(share::schema::ObSchemaGetterGuard& schema_guard, ITenantStatFinder& stat_finder,
      common::hash::ObHashSet<uint64_t>& processed_tids, int64_t& max_used_balance_group_id,
      common::ObIAllocator& allocator, IBalanceGroupContainer& balance_group_container)
      : IBalanceGroupContainerBuilder(
            schema_guard, stat_finder, processed_tids, max_used_balance_group_id, allocator, balance_group_container)
  {}
  virtual ~ShardGroupContainerBuilder()
  {}

public:
  int build() override;

private:
  struct SameRangePartInfo {
    SameRangePartInfo()
        : pkey_(),
          tablegroup_id_(common::OB_INVALID_ID),
          table_id_(common::OB_INVALID_ID),
          all_tg_idx_(common::OB_INVALID_INDEX_INT64),
          part_idx_(common::OB_INVALID_INDEX_INT64)
    {}
    SameRangePartInfo(const common::ObPartitionKey& pkey, const uint64_t tablegroup_id, const uint64_t table_id,
        const int64_t all_tg_idx, const int64_t part_idx)
        : pkey_(pkey), tablegroup_id_(tablegroup_id), table_id_(table_id), all_tg_idx_(all_tg_idx), part_idx_(part_idx)
    {}
    common::ObPartitionKey pkey_;
    uint64_t tablegroup_id_;
    uint64_t table_id_;
    int64_t all_tg_idx_;
    int64_t part_idx_;
    TO_STRING_KV(K_(pkey), K_(tablegroup_id), K_(table_id), K_(all_tg_idx), K_(part_idx));
  };
  typedef common::ObArray<SameRangePartInfo, common::ObIAllocator&> SameRangeArray;
  typedef common::hash::ObHashMap<common::ObRowkey, SameRangeArray*> SameRangeMap;
  class TableSchemaPartitionCntCmp {
  public:
    TableSchemaPartitionCntCmp(common::ObArray<const share::schema::ObTableSchema*>& shardgroup_schemas)
        : shardgroup_schemas_(shardgroup_schemas), ret_(common::OB_SUCCESS)
    {}
    int sort();

  public:
    bool operator()(const share::schema::ObTableSchema* l, const share::schema::ObTableSchema* r);

  private:
    common::ObArray<const share::schema::ObTableSchema*>& shardgroup_schemas_;
    int ret_;
  };

private:
  int build_shardgroup_container(common::ObArray<const share::schema::ObTableSchema*>& shardgroup_table_schemas);
  int build_one_level_range_shard_partition_container(
      common::ObArray<const share::schema::ObTableSchema*>& shardgroup_table_schemas, const bool primary_zone_match,
      const common::ObZone& integrated_primary_zone);
  int build_shardgroup_partition_container(
      common::ObArray<const share::schema::ObTableSchema*>& shargroup_table_schemas, const bool primary_zone_match,
      const common::ObZone& integrated_primary_zone);
  int set_same_range_array(SameRangeMap& same_range_map, const share::schema::ObPartition& partition,
      const int64_t part_idx, const share::schema::ObTableSchema& table_schema);
  int locate_same_range_array(
      SameRangeMap& same_range_map, const share::schema::ObPartition& partition, SameRangeArray*& same_range_array);
  int do_build_one_level_range_partition_container(SameRangeMap& same_range_map, const uint64_t base_index_group_id,
      const bool primary_zone_match, const common::ObZone& integrated_primary_zone);
};

class TableGroupContainerBuilder : public IBalanceGroupContainerBuilder {
public:
  TableGroupContainerBuilder(share::schema::ObSchemaGetterGuard& schema_guard, ITenantStatFinder& stat_finder,
      common::hash::ObHashSet<uint64_t>& processed_tids, int64_t& max_used_balance_group_id,
      common::ObIAllocator& allocator, IBalanceGroupContainer& balance_group_container)
      : IBalanceGroupContainerBuilder(
            schema_guard, stat_finder, processed_tids, max_used_balance_group_id, allocator, balance_group_container)
  {}
  virtual ~TableGroupContainerBuilder()
  {}

public:
  int build() override;

private:
  int build_tablegroup_container(const share::schema::ObPartitionSchema* partition_schema);
};

class SinglePtBalanceContainerBuilder {
public:
  SinglePtBalanceContainerBuilder(BalanceGroupType balance_group_type, share::schema::ObSchemaGetterGuard& schema_guard,
      ITenantStatFinder& stat_finder, common::ObIAllocator& allocator, IBalanceGroupContainer& balance_group_container,
      int64_t& max_used_balance_group_id)
      : balance_group_type_(balance_group_type),
        schema_guard_(schema_guard),
        stat_finder_(stat_finder),
        allocator_(allocator),
        balance_group_container_(balance_group_container),
        max_used_balance_group_id_(max_used_balance_group_id)
  {}
  virtual ~SinglePtBalanceContainerBuilder()
  {}

public:
  int build(const share::schema::ObPartitionSchema& partition_schema);

private:
  int build_one_level_partition_table_container(const share::schema::ObPartitionSchema& partition_schema);
  int build_two_level_partition_table_container(const share::schema::ObPartitionSchema& partition_schema);
  int build_two_level_partition_table_container_by_first_level(
      const share::schema::ObPartitionSchema& partition_schema);
  int build_two_level_partition_table_container_by_second_level(
      const share::schema::ObPartitionSchema& partition_schema);
  PartitionDistributePolicy get_partition_schema_distribute_policy(
      const share::schema::ObPartitionSchema& partition_schema);

private:
  BalanceGroupType balance_group_type_;
  share::schema::ObSchemaGetterGuard& schema_guard_;
  ITenantStatFinder& stat_finder_;
  common::ObIAllocator& allocator_;
  IBalanceGroupContainer& balance_group_container_;
  int64_t& max_used_balance_group_id_;
};

class IBalanceGroupContainer {
public:
  IBalanceGroupContainer() : container_type_array_()
  {}
  virtual ~IBalanceGroupContainer()
  {}

public:
  virtual int collect_balance_group_box(const common::ObIArray<BalanceGroupBox*>& balance_group_box_array) = 0;
  const common::ObIArray<BalanceGroupContainerType>& get_container_type_array() const
  {
    return container_type_array_;
  }

protected:
  common::ObSEArray<BalanceGroupContainerType, 2, common::ObNullAllocator> container_type_array_;
};

class ObSinglePtBalanceContainer : public IBalanceGroupContainer {
public:
  ObSinglePtBalanceContainer(const share::schema::ObPartitionSchema& partition_schema,
      share::schema::ObSchemaGetterGuard& schema_guard, ITenantStatFinder& stat_finder, common::ObIAllocator& allocator)
      : IBalanceGroupContainer(),
        inited_(false),
        partition_schema_(partition_schema),
        schema_guard_(schema_guard),
        stat_finder_(stat_finder),
        allocator_(allocator),
        hash_index_collection_()
  {}
  virtual ~ObSinglePtBalanceContainer()
  {}

public:
  int init(const int64_t item_size);
  int build();
  virtual int collect_balance_group_box(const common::ObIArray<BalanceGroupBox*>& balance_group_box_array) override;
  const HashIndexCollection& get_hash_index() const
  {
    return hash_index_collection_;
  }

private:
  bool inited_;
  const share::schema::ObPartitionSchema& partition_schema_;
  share::schema::ObSchemaGetterGuard& schema_guard_;
  ITenantStatFinder& stat_finder_;
  common::ObIAllocator& allocator_;
  HashIndexCollection hash_index_collection_;
};

class ObBalanceGroupContainer : public IBalanceGroupContainer {
public:
  const int64_t PROCESSED_TABLE_MAP_SIZE = 100 * 1024;

public:
  ObBalanceGroupContainer(
      share::schema::ObSchemaGetterGuard& schema_guard, ITenantStatFinder& stat_finder, common::ObIAllocator& allocator)
      : IBalanceGroupContainer(),
        inited_(false),
        tenant_id_(common::OB_INVALID_ID),
        max_used_balance_group_id_(-1),
        schema_guard_(schema_guard),
        stat_finder_(stat_finder),
        allocator_(allocator),
        processed_tids_(),
        shardgroup_container_builder_(
            schema_guard_, stat_finder_, processed_tids_, max_used_balance_group_id_, allocator_, *this),
        tablegroup_container_builder_(
            schema_guard_, stat_finder_, processed_tids_, max_used_balance_group_id_, allocator_, *this),
        partition_container_builder_(
            schema_guard_, stat_finder_, processed_tids_, max_used_balance_group_id_, allocator_, *this),
        non_partition_container_builder_(
            schema_guard_, stat_finder_, processed_tids_, max_used_balance_group_id_, allocator_, *this)
  {}
  virtual ~ObBalanceGroupContainer()
  {}

public:
  int init(const uint64_t tenant_id);
  virtual int build();
  virtual int get_gts_switch(bool& on);
  const common::hash::ObHashSet<uint64_t>& get_processed_tids() const
  {
    return processed_tids_;
  }

protected:
  bool inited_;
  uint64_t tenant_id_;
  int64_t max_used_balance_group_id_;
  share::schema::ObSchemaGetterGuard& schema_guard_;
  ITenantStatFinder& stat_finder_;
  common::ObIAllocator& allocator_;
  common::hash::ObHashSet<uint64_t> processed_tids_;
  ShardGroupContainerBuilder shardgroup_container_builder_;
  TableGroupContainerBuilder tablegroup_container_builder_;
  PartitionTableContainerBuilder partition_container_builder_;
  NonPartitionTableContainerBuilder non_partition_container_builder_;
};

class BalanceGroupBoxBuilder {
public:
  BalanceGroupBoxBuilder(const BalanceGroupType& balance_group_type,
      const common::ObIArray<BalanceGroupContainerType>& container_type_array,
      share::schema::ObSchemaGetterGuard& schema_guard, ITenantStatFinder& stat_finder, common::ObIAllocator& allocator)
      : inited_(false),
        balance_group_type_(balance_group_type),
        hash_index_map_(),
        container_type_array_(container_type_array),
        output_box_array_(),
        schema_guard_(schema_guard),
        stat_finder_(stat_finder),
        allocator_(allocator)
  {}
  virtual ~BalanceGroupBoxBuilder()
  {}

public:
  int init(const int64_t row_size, const int64_t col_size, const bool ignore_leader_balance,
      const common::ObZone& primary_zone);
  int set_item(const BalanceGroupBoxItem& item);
  const common::ObIArray<BalanceGroupBox*>& get_balance_group_box() const
  {
    return output_box_array_;
  }

private:
  int init_square_id_map(const int64_t row_size, const int64_t col_size, const bool ignore_leader_balance,
      const common::ObZone& primary_zone);
  int init_hash_index_map(const int64_t row_size, const int64_t col_size);

protected:
  bool inited_;
  BalanceGroupType balance_group_type_;
  // Since the memory life cycle of hash index map is different from that of SquareIdMap,
  // the memory management of the hash_index_map object and the square_id_map object are handled separately,
  // The hash_index_map object is a member variable of the build,
  // and the square_id_map object is allocated by the allocator
  HashIndexMap hash_index_map_;
  const common::ObIArray<BalanceGroupContainerType>& container_type_array_;
  // Currently there are only two container
  common::ObSEArray<BalanceGroupBox*, 2, common::ObNullAllocator> output_box_array_;
  share::schema::ObSchemaGetterGuard& schema_guard_;
  ITenantStatFinder& stat_finder_;
  common::ObIAllocator& allocator_;
};

class ObLeaderBalanceGroupContainer : public ObBalanceGroupContainer {
public:
  ObLeaderBalanceGroupContainer(
      share::schema::ObSchemaGetterGuard& schema_guard, ITenantStatFinder& stat_finder, common::ObIAllocator& allocator)
      : ObBalanceGroupContainer(schema_guard, stat_finder, allocator), hash_index_collection_()
  {}
  virtual ~ObLeaderBalanceGroupContainer()
  {}

public:
  int init(const uint64_t tenant_id);
  virtual int collect_balance_group_box(const common::ObIArray<BalanceGroupBox*>& balance_group_box_array) override;
  const HashIndexCollection& get_hash_index() const
  {
    return hash_index_collection_;
  }

private:
  HashIndexCollection hash_index_collection_;
};

class ObPartitionBalanceGroupContainer : public ObBalanceGroupContainer {
public:
  ObPartitionBalanceGroupContainer(
      share::schema::ObSchemaGetterGuard& schema_guard, ITenantStatFinder& stat_finder, common::ObIAllocator& allocator)
      : ObBalanceGroupContainer(schema_guard, stat_finder, allocator),
        hash_index_collection_(),
        square_id_map_collection_()
  {}
  virtual ~ObPartitionBalanceGroupContainer()
  {}

public:
  int init(const uint64_t tenant_id);
  virtual int collect_balance_group_box(const common::ObIArray<BalanceGroupBox*>& balance_group_box_array) override;
  common::ObIArray<SquareIdMap*>& get_square_id_map_array()
  {
    return square_id_map_collection_.get_square_id_map_array();
  }
  const HashIndexCollection& get_hash_index() const
  {
    return hash_index_collection_;
  }
  int calc_leader_balance_statistic(const common::ObZone& zone);

private:
  HashIndexCollection hash_index_collection_;
  SquareIdMapCollection square_id_map_collection_;
};
}  // end namespace balancer
}  // end namespace rootserver
}  // end namespace oceanbase
#endif  // _OB_BALANCE_GROUP_CONTAINER_H_
