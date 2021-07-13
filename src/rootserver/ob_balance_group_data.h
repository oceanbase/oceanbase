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

#ifndef _OB_BALANCE_GROUP_DATA_H
#define _OB_BALANCE_GROUP_DATA_H 1

#include "ob_root_utils.h"
#include "lib/container/ob_se_array.h"
#include "lib/container/ob_array.h"
#include "lib/hash/ob_hashset.h"
#include "lib/hash/ob_hashmap.h"
#include "lib/hash/ob_refered_map.h"
#include "common/ob_zone.h"
#include "share/schema/ob_schema_struct.h"
#include "share/ob_primary_zone_util.h"
#include "share/config/ob_server_config.h"
#include "ob_balancer_interface.h"
#include "observer/omt/ob_tenant_config_mgr.h"

namespace oceanbase {
namespace share {
namespace schema {
class ObTableSchema;
class ObSchemaGetterGuard;
}  // namespace schema
}  // namespace share
namespace common {
class ObIAllocator;
class ObPartitionKey;
}  // namespace common

namespace rootserver {
namespace balancer {
enum BalanceGroupContainerType {
  HASH_INDEX_TYPE = 0,
  SQUARE_MAP_TYPE,
  INVALID_BGC_TYPE,
};

enum BalanceGroupType {
  INVALID_BALANCE_GROUP_TYPE = 0,
  SHARD_GROUP_BALANCE_GROUP,
  TABLE_GROUP_BALANCE_GROUP,
  PARTITION_TABLE_BALANCE_GROUP,
  SHARD_PARTITION_BALANCE_GROUP,
  NON_PARTITION_TABLE_BALANCE_GROUP,
  MAX_BALANCE_GROUP,
};

struct BalanceGroupBoxItem {
  BalanceGroupBoxItem()
      : pkey_(),
        in_group_index_(OB_INVALID_INDEX_INT64),
        group_count_(0),
        group_id_(OB_INVALID_ID),
        balance_group_type_(INVALID_BALANCE_GROUP_TYPE),
        row_(-1),
        col_(-1),
        all_tg_idx_(OB_INVALID_INDEX_INT64),
        all_pg_idx_(OB_INVALID_INDEX_INT64),
        tablegroup_id_(OB_INVALID_ID),
        table_id_(OB_INVALID_ID),
        part_idx_(OB_INVALID_INDEX_INT64)
  {}
  BalanceGroupBoxItem(const common::ObPartitionKey& pkey, const int64_t in_group_index, const int64_t group_count,
      const uint64_t group_id, const BalanceGroupType balance_group_type, const int64_t row, const int64_t col,
      const int64_t all_tg_idx, const int64_t all_pg_idx, const uint64_t tablegroup_id, const uint64_t table_id,
      const int64_t part_idx, const int64_t balance_group_id)
      : pkey_(pkey),
        in_group_index_(in_group_index),
        group_count_(group_count),
        group_id_(group_id),
        balance_group_type_(balance_group_type),
        row_(row),
        col_(col),
        all_tg_idx_(all_tg_idx),
        all_pg_idx_(all_pg_idx),
        tablegroup_id_(tablegroup_id),
        table_id_(table_id),
        part_idx_(part_idx),
        balance_group_id_(balance_group_id)
  {}
  common::ObPartitionKey pkey_;
  // use for hash_index_map
  int64_t in_group_index_;
  int64_t group_count_;
  uint64_t group_id_;
  BalanceGroupType balance_group_type_;
  // use for square_id_map
  int64_t row_;
  int64_t col_;
  int64_t all_tg_idx_;
  int64_t all_pg_idx_;
  uint64_t tablegroup_id_;
  uint64_t table_id_;
  int64_t part_idx_;
  int64_t balance_group_id_;
};

class BalanceGroupBox {
public:
  BalanceGroupBox()
  {}
  virtual ~BalanceGroupBox()
  {}

public:
  virtual BalanceGroupContainerType get_container_type() const = 0;
  virtual int set_item(const BalanceGroupBoxItem& item) = 0;
  virtual int64_t to_string(char* buf, const int64_t buf_len) const = 0;
};

class BalanceGroupCollection {
public:
  BalanceGroupCollection()
  {}
  virtual ~BalanceGroupCollection()
  {}

public:
  virtual BalanceGroupContainerType get_container_type() const = 0;
  virtual int collect_box(const BalanceGroupBox* balance_group_box) = 0;
};

struct SquareIdMapItem {
  SquareIdMapItem()
      : pkey_(),
        designated_leader_(false),
        all_tg_idx_(common::OB_INVALID_INDEX),
        all_pg_idx_(common::OB_INVALID_INDEX),
        tablegroup_id_(common::OB_INVALID_ID),
        table_id_(common::OB_INVALID_ID),
        part_idx_(common::OB_INVALID_INDEX),
        unit_id_(common::OB_INVALID_ID),
        dest_unit_id_(common::OB_INVALID_ID),
        replica_type_(common::REPLICA_TYPE_MAX),
        memstore_percent_(-1)
  {}
  common::ObPartitionKey pkey_;
  bool designated_leader_;
  int64_t all_tg_idx_;
  int64_t all_pg_idx_;
  uint64_t tablegroup_id_;
  uint64_t table_id_;
  int64_t part_idx_;
  uint64_t unit_id_;
  uint64_t dest_unit_id_;
  // The load balancing of mixed locality will use replica_type_ and memstore_percent_
  ObReplicaType replica_type_;
  int64_t memstore_percent_;

  bool operator==(const SquareIdMapItem& other) const
  {
    return all_tg_idx_ == other.all_tg_idx_ && part_idx_ == other.part_idx_;
  }

  const common::ObPartitionKey& get_partition_key() const
  {
    return pkey_;
  }
  bool is_designated_leader() const
  {
    return designated_leader_;
  }
  int64_t get_all_tg_idx() const
  {
    return all_tg_idx_;
  }
  int64_t get_all_pg_idx() const
  {
    return all_pg_idx_;
  }
  uint64_t get_tablegroup_id() const
  {
    return tablegroup_id_;
  }
  uint64_t get_table_id() const
  {
    return table_id_;
  }
  int64_t get_partition_idx() const
  {
    return part_idx_;
  }
  uint64_t get_unit_id() const
  {
    return unit_id_;
  }
  uint64_t get_dest_unit_id() const
  {
    return dest_unit_id_;
  }
  ObReplicaType get_replica_type() const
  {
    return replica_type_;
  }
  int64_t get_memstore_percent() const
  {
    return memstore_percent_;
  }

  TO_STRING_KV(K_(pkey), K_(designated_leader), K_(all_tg_idx), K_(all_pg_idx), K_(part_idx), K_(unit_id),
      K_(dest_unit_id), K_(tablegroup_id), K_(table_id), K_(replica_type), K_(memstore_percent));
};

template <class T>
class IdMapIterator {
public:
  typedef IdMapIterator<T> self_t;

public:
  typedef T* value_ptr_t;
  typedef T* pointer;
  typedef T& reference;

public:
  IdMapIterator() : value_ptr_(NULL)
  {}
  explicit IdMapIterator(value_ptr_t value_ptr)
  {
    value_ptr_ = value_ptr;
  }
  IdMapIterator(const self_t& other)
  {
    *this = other;
  }
  self_t& operator=(const self_t& other)
  {
    value_ptr_ = other.value_ptr_;
    return *this;
  }
  reference operator*() const
  {
    return *value_ptr_;
  }
  value_ptr_t operator->() const
  {
    return value_ptr_;
  }
  self_t& operator++()
  {
    value_ptr_++;
    return *this;
  }
  self_t operator++(int)
  {
    self_t tmp = *this;
    value_ptr_++;
    return tmp;
  }
  bool operator==(const self_t& other) const
  {
    return (value_ptr_ == (other.value_ptr_));
  }
  bool operator!=(const self_t& other) const
  {
    return (value_ptr_ != (other.value_ptr_));
  }

private:
  value_ptr_t value_ptr_;
};

class HashIndexCollection;
class AverageCountBalancer;
class PartitionLeaderCountBalancer;
class BalanceStat;
class RowBalanceStat;
class OverallBalanceStat;
class RowLeaderBalanceStat;
struct HashIndexMapItem;
struct SquareIdMapItem;

class SquareIdMap : public BalanceGroupBox {
public:
  friend AverageCountBalancer;
  friend PartitionLeaderCountBalancer;
  friend BalanceStat;
  friend RowBalanceStat;
  friend OverallBalanceStat;
  friend RowLeaderBalanceStat;

public:
  typedef SquareIdMapItem Item;
  typedef IdMapIterator<SquareIdMapItem> iterator;
  typedef IdMapIterator<const SquareIdMapItem> const_iterator;

public:
  SquareIdMap(share::schema::ObSchemaGetterGuard& schema_guard, ITenantStatFinder& stat_finder,
      common::ObIAllocator& allocator);
  virtual ~SquareIdMap()
  {}

public:
  int init(const int64_t row, const int64_t col);
  virtual BalanceGroupContainerType get_container_type() const override
  {
    return SQUARE_MAP_TYPE;
  }
  virtual int set_item(const BalanceGroupBoxItem& item) override;
  int get(
      const int64_t row, const int64_t col, common::ObPartitionKey& pkey, int64_t& all_tg_idx, int64_t& part_idx) const;
  int get(const int64_t row, const int64_t col, Item*& item);
  int get(const int64_t row, const int64_t col, Item& item) const;
  int get(const int64_t map_idx, Item& item) const;
  int set(const int64_t row, const int64_t col, const common::ObPartitionKey& pkey, const int64_t all_tg_idx,
      const int64_t all_pg_idx, const uint64_t tablegroup_id, const uint64_t table_id, const int64_t part_idx);
  int set_unit(const int64_t row, const int64_t col, const uint64_t unit_id, const uint64_t dest_unit_id);
  int assign_unit(const IUnitProvider& unit_provider);
  int update_leader_balance_info(const common::ObZone& zone, const HashIndexCollection& hash_index_collection);
  int update_item_leader_balance_info(const common::ObZone& zone, SquareIdMapItem& id_map_item,
      const common::ObIArray<share::ObRawPrimaryZoneUtil::ZoneScore>& zone_score,
      const common::ObIArray<share::ObRawPrimaryZoneUtil::RegionScore>& region_score,
      const HashIndexMapItem& hash_index_item);

  bool need_balance() const;

  void set_is_valid(bool v)
  {
    is_valid_ = v;
  }
  bool is_valid() const
  {
    return is_valid_;
  }
  int64_t size() const
  {
    return row_size_ * col_size_;
  }
  int64_t get_row_size() const
  {
    return row_size_;
  }
  int64_t get_col_size() const
  {
    return col_size_;
  }
  void set_map_id(int64_t map_id)
  {
    map_id_ = map_id;
  }
  int64_t get_map_id() const
  {
    return map_id_;
  }
  uint64_t get_tenant_id() const
  {
    return tenant_id_;
  }
  bool ignore_leader_balance() const
  {
    return ignore_leader_balance_;
  }
  const common::ObZone& get_primary_zone() const
  {
    return primary_zone_;
  }
  void set_ignore_leader_balance(const bool ignore)
  {
    ignore_leader_balance_ = ignore;
  }
  void set_primary_zone(const common::ObZone& primary_zone)
  {
    primary_zone_ = primary_zone;
  }

  // Traverse all in the order of a one-dimensional array SquareIdMapItem
  inline iterator begin();
  inline iterator end();
  inline const_iterator begin() const;
  inline const_iterator end() const;
  int dump(const char* dump_banner, bool force_dump = false);
  int dump2(const char* dump_banner, bool force_dump = false);
  int dump_unit(const char* dump_banner);

  virtual const char* get_comment() const = 0;
  virtual BalanceGroupType get_map_type() const = 0;

  TO_STRING_KV(K_(map_id), K_(ignore_leader_balance), K_(primary_zone), K_(tenant_id), K_(row_size), K_(col_size),
      "type", get_map_type(), K_(is_valid));

protected:
  int get_all_tg_idx(uint64_t tablegroup_id, uint64_t table_id, int64_t& all_tg_idx);
  SquareIdMapItem* alloc_id_map_item_array(common::ObIAllocator& allocator, int64_t size);

protected:
  bool inited_;
  share::schema::ObSchemaGetterGuard& schema_guard_;
  ITenantStatFinder& stat_finder_;
  common::ObIAllocator& allocator_;
  SquareIdMapItem* map_;
  uint64_t tenant_id_;
  int64_t row_size_;  // SquareIdMap Number of rows in the matrix
  int64_t col_size_;  // SquareIdMap Number of columns in the matrix
  bool is_valid_;
  bool ignore_leader_balance_;
  common::ObZone primary_zone_;

private:
  int64_t map_id_;    // debug only
  bool enable_dump_;  // debug only
};

inline SquareIdMap::iterator SquareIdMap::begin()
{
  return iterator(map_);
}
inline SquareIdMap::iterator SquareIdMap::end()
{
  return iterator(map_ + row_size_ * col_size_);
}
inline SquareIdMap::const_iterator SquareIdMap::begin() const
{
  return const_iterator(map_);
}
inline SquareIdMap::const_iterator SquareIdMap::end() const
{
  return const_iterator(map_ + row_size_ * col_size_);
}

class ShardGroupIdMap : public SquareIdMap {
public:
  ShardGroupIdMap(
      share::schema::ObSchemaGetterGuard& schema_guard, ITenantStatFinder& stat_finder, common::ObIAllocator& allocator)
      : SquareIdMap(schema_guard, stat_finder, allocator)
  {}
  virtual ~ShardGroupIdMap()
  {}

public:
  virtual const char* get_comment() const;
  virtual BalanceGroupType get_map_type() const;
};

class ShardPartitionIdMap : public SquareIdMap {
public:
  ShardPartitionIdMap(
      share::schema::ObSchemaGetterGuard& schema_guard, ITenantStatFinder& stat_finder, common::ObIAllocator& allocator)
      : SquareIdMap(schema_guard, stat_finder, allocator)
  {}
  virtual ~ShardPartitionIdMap()
  {}

public:
  virtual const char* get_comment() const;
  virtual BalanceGroupType get_map_type() const;
};

class TableGroupIdMap : public SquareIdMap {
public:
  TableGroupIdMap(
      share::schema::ObSchemaGetterGuard& schema_guard, ITenantStatFinder& stat_finder, common::ObIAllocator& allocator)
      : SquareIdMap(schema_guard, stat_finder, allocator)
  {}
  virtual ~TableGroupIdMap()
  {}

public:
  virtual const char* get_comment() const;
  virtual BalanceGroupType get_map_type() const;
};

class PartitionTableIdMap : public SquareIdMap {
public:
  PartitionTableIdMap(
      share::schema::ObSchemaGetterGuard& schema_guard, ITenantStatFinder& stat_finder, common::ObIAllocator& allocator)
      : SquareIdMap(schema_guard, stat_finder, allocator)
  {}
  virtual ~PartitionTableIdMap()
  {}

public:
  virtual const char* get_comment() const;
  virtual BalanceGroupType get_map_type() const;
};

class NonPartitionTableIdMap : public SquareIdMap {
public:
  NonPartitionTableIdMap(
      share::schema::ObSchemaGetterGuard& schema_guard, ITenantStatFinder& stat_finder, common::ObIAllocator& allocator)
      : SquareIdMap(schema_guard, stat_finder, allocator)
  {}
  virtual ~NonPartitionTableIdMap()
  {}

public:
  virtual const char* get_comment() const;
  virtual BalanceGroupType get_map_type() const;
};

class SquareIdMapCollection : public BalanceGroupCollection {
public:
  SquareIdMapCollection() : BalanceGroupCollection(), inited_(false), square_id_map_array_()
  {}
  virtual ~SquareIdMapCollection()
  {}

public:
  int init();

public:
  virtual BalanceGroupContainerType get_container_type() const override
  {
    return SQUARE_MAP_TYPE;
  }
  virtual int collect_box(const BalanceGroupBox* balance_group_box) override;
  int calc_leader_balance_statistic(const common::ObZone& zone, const HashIndexCollection& hash_index_collection);
  common::ObIArray<SquareIdMap*>& get_square_id_map_array()
  {
    return square_id_map_array_;
  }

private:
  bool inited_;
  common::ObArray<SquareIdMap*> square_id_map_array_;
};

struct HashIndexMapItem {
public:
  HashIndexMapItem()
      : in_group_index_(-1),
        group_count_(0),
        group_id_(common::OB_INVALID_ID),
        balance_group_type_(INVALID_BALANCE_GROUP_TYPE),
        tenant_id_(OB_INVALID_ID),
        balance_group_id_(-1)
  {}

  HashIndexMapItem(const int64_t in_group_index, const int64_t group_count, const uint64_t group_id,
      const BalanceGroupType balance_group_type, const uint64_t tenant_id, const int64_t balance_group_id)
      : in_group_index_(in_group_index),
        group_count_(group_count),
        group_id_(group_id),
        balance_group_type_(balance_group_type),
        tenant_id_(tenant_id),
        balance_group_id_(balance_group_id)
  {}

  bool is_valid() const
  {
    return in_group_index_ >= 0 && group_count_ > 0 && in_group_index_ < group_count_ && balance_group_id_ >= 0 &&
           group_id_ != common::OB_INVALID_ID && balance_group_type_ > INVALID_BALANCE_GROUP_TYPE &&
           balance_group_type_ < MAX_BALANCE_GROUP;
  }
  // The method of using hash for tablegroup and partition table
  // and then modulo array count to find the offset.
  // Other ways of using divisible array count
  bool calc_balance_group_zone_by_hash_offset() const
  {
    bool tenant_config_by_hash_offset = true;
    if (1 == GCONF._create_table_partition_distribution_strategy) {
      tenant_config_by_hash_offset = true;
    } else {
      tenant_config_by_hash_offset = false;
    }
    return (TABLE_GROUP_BALANCE_GROUP == balance_group_type_ || PARTITION_TABLE_BALANCE_GROUP == balance_group_type_ ||
               SHARD_GROUP_BALANCE_GROUP == balance_group_type_ ||
               SHARD_PARTITION_BALANCE_GROUP == balance_group_type_) &&
           tenant_config_by_hash_offset;
  }

  int get_balance_group_zone_x_axis(const int64_t array_capacity, int64_t& x_axis) const
  {
    int ret = common::OB_SUCCESS;
    int64_t new_group_idx = -1;
    if (OB_UNLIKELY(!is_valid())) {
      ret = OB_INVALID_ARGUMENT;
      RS_LOG(WARN, "invalid balance group index", K(ret));
    } else if (array_capacity <= 0) {
      ret = OB_INVALID_ARGUMENT;
      RS_LOG(WARN, "invalid argument", K(ret), K(array_capacity));
    } else if (balance_group_type_ >= MAX_BALANCE_GROUP || balance_group_type_ <= INVALID_BALANCE_GROUP_TYPE) {
      ret = OB_ERR_UNEXPECTED;
      RS_LOG(WARN, "balance group type unexpected", K(ret), K(balance_group_type_));
    } else if (OB_FAIL(calc_in_balance_group_new_idx(new_group_idx))) {
      RS_LOG(WARN, "fail to calc in balance group new idx", K(ret), K(new_group_idx));
    } else if (new_group_idx < 0 || new_group_idx >= group_count_) {
      ret = OB_ERR_UNEXPECTED;
      RS_LOG(WARN, "new group idx unexpected", K(ret), K(new_group_idx), K(group_count_));
    } else {
      // itl is the abbreviation of interval
      const int64_t min_itl = group_count_ / array_capacity;
      const int64_t max_itl = (group_count_ == min_itl * array_capacity ? min_itl : min_itl + 1);
      const int64_t max_itl_cnt = group_count_ - min_itl * array_capacity;
      const int64_t min_itl_cnt = ((min_itl == 0) ? 0 : ((group_count_ - max_itl * max_itl_cnt) / min_itl));
      if (calc_balance_group_zone_by_hash_offset()) {
        x_axis = new_group_idx % array_capacity;
      } else {
        const int64_t base_offset = ((min_itl == 0) ? 0 : (new_group_idx / min_itl));
        if (base_offset < min_itl_cnt) {
          x_axis = base_offset;
        } else {
          const int64_t base_sum = min_itl_cnt * min_itl;
          const int64_t remain = new_group_idx - base_sum;
          x_axis = min_itl_cnt + (remain / max_itl);
        }
      }
      x_axis = (x_axis + group_id_) % array_capacity;
    }
    return ret;
  }

  int get_balance_group_zone_y_axis(const bool is_multiple_zone, const int64_t array_capacity, const int64_t task_idx,
      int64_t& y_axis, int64_t& y_capacity, int64_t& offset) const
  {
    int ret = common::OB_SUCCESS;
    int64_t new_group_idx = -1;
    int64_t x_axis = -1;
    if (OB_UNLIKELY(!is_valid())) {
      ret = OB_INVALID_ARGUMENT;
      RS_LOG(WARN, "invalid balance group index", K(ret));
    } else if (array_capacity <= 0) {
      ret = OB_INVALID_ARGUMENT;
      RS_LOG(WARN, "invalid argument", K(ret), K(array_capacity));
    } else if (balance_group_type_ >= MAX_BALANCE_GROUP || balance_group_type_ <= INVALID_BALANCE_GROUP_TYPE) {
      ret = OB_ERR_UNEXPECTED;
      RS_LOG(WARN, "balance group type unexpected", K(ret), K(balance_group_type_));
    } else if (OB_FAIL(calc_in_balance_group_new_idx(new_group_idx))) {
      RS_LOG(WARN, "fail to calc in balance group new idx", K(ret), K(new_group_idx));
    } else if (new_group_idx < 0 || new_group_idx >= group_count_) {
      ret = OB_ERR_UNEXPECTED;
      RS_LOG(WARN, "new group idx unexpected", K(ret), K(new_group_idx), K(group_count_));
    } else {
      // itl is the abbreviation of interval
      const int64_t min_itl = group_count_ / array_capacity;
      const int64_t max_itl = (group_count_ == min_itl * array_capacity ? min_itl : min_itl + 1);
      const int64_t max_itl_cnt = group_count_ - min_itl * array_capacity;
      const int64_t min_itl_cnt = ((min_itl == 0) ? 0 : ((group_count_ - max_itl * max_itl_cnt) / min_itl));
      if (calc_balance_group_zone_by_hash_offset()) {
        x_axis = new_group_idx % array_capacity;
        y_axis = new_group_idx / array_capacity;
        if (!is_multiple_zone) {
          if (x_axis >= max_itl_cnt) {
            offset = (x_axis - max_itl_cnt) * min_itl + max_itl_cnt * max_itl;
          } else {
            offset = x_axis * max_itl;
          }
        } else {
          offset = 0;
          for (int64_t i = task_idx; i > 0; --i) {
            const int64_t true_task_idx = (x_axis + i) % array_capacity;
            if (true_task_idx < max_itl_cnt) {
              offset += max_itl;
            } else {
              offset += min_itl;
            }
          }
        }
        if (x_axis >= max_itl_cnt) {
          y_capacity = min_itl;
        } else {
          y_capacity = max_itl;
        }
      } else {
        const int64_t base_offset = ((min_itl == 0) ? 0 : (new_group_idx / min_itl));
        if (base_offset < min_itl_cnt) {
          x_axis = base_offset;
          y_axis = new_group_idx % min_itl;
        } else {
          const int64_t base_sum = min_itl_cnt * min_itl;
          const int64_t remain = new_group_idx - base_sum;
          x_axis = min_itl_cnt + (remain / max_itl);
          y_axis = remain % max_itl;
        }
        if (x_axis >= min_itl_cnt) {
          offset = (x_axis - min_itl_cnt) * max_itl + min_itl_cnt * min_itl;
          y_capacity = max_itl;
        } else {
          offset = x_axis * min_itl;
          y_capacity = min_itl;
        }
      }
    }
    return ret;
  }

  template <typename ARRAY>
  int get_balance_group_zone(ARRAY& array, common::ObZone& balance_group_zone) const
  {
    int ret = common::OB_SUCCESS;
    if (OB_UNLIKELY(!is_valid())) {
      ret = OB_INVALID_ARGUMENT;
      RS_LOG(WARN, "invalid balance group index", K(ret));
    } else if (array.count() <= 0) {
      ret = OB_INVALID_ARGUMENT;
      RS_LOG(WARN, "invalid argument", K(ret));
    } else if (balance_group_type_ >= MAX_BALANCE_GROUP || balance_group_type_ <= INVALID_BALANCE_GROUP_TYPE) {
      ret = OB_ERR_UNEXPECTED;
      RS_LOG(WARN, "balance group type unexpected", K(ret), K(balance_group_type_));
    } else {
      int64_t x_axis = -1;
      std::sort(array.begin(), array.end());
      if (OB_FAIL(get_balance_group_zone_x_axis(array.count(), x_axis))) {
        RS_LOG(WARN, "fail to get balance group zone axis", K(ret));
      } else if (x_axis < 0 || x_axis >= array.count()) {
        ret = OB_ERR_UNEXPECTED;
        RS_LOG(WARN, "x_axis unexpected", K(ret), K(x_axis), K(*this), "array_count", array.count());
      } else {
        balance_group_zone = array.at(x_axis);
      }
    }
    return ret;
  }

  TO_STRING_KV(
      K_(in_group_index), K_(group_count), K_(group_id), K_(balance_group_id), K_(balance_group_type), K_(tenant_id));

  /*
   * Recalculate in_group_idx for external use on the basis of in_group_index_
   */
  int calc_in_balance_group_new_idx(int64_t& offset) const
  {
    int ret = common::OB_SUCCESS;
    if (OB_UNLIKELY(!is_valid())) {
      ret = OB_INVALID_ARGUMENT;
      RS_LOG(WARN, "invalid balance group index", K(ret));
    } else if (balance_group_type_ >= MAX_BALANCE_GROUP || balance_group_type_ <= INVALID_BALANCE_GROUP_TYPE) {
      ret = OB_ERR_UNEXPECTED;
      RS_LOG(WARN, "balance group type unexpected", K(ret), K(balance_group_type_));
    } else if (calc_balance_group_zone_by_hash_offset()) {
      uint32_t offset_base = 0;
      offset_base = murmurhash2(&group_id_, sizeof(group_id_), offset_base);
      offset_base = fnv_hash2(&group_id_, sizeof(group_id_), offset_base);
      offset = ((int64_t)offset_base + in_group_index_) % group_count_;
    } else {
      offset = in_group_index_;
    }
    return ret;
  }

  int64_t in_group_index_;
  int64_t group_count_;
  uint64_t group_id_;
  BalanceGroupType balance_group_type_;
  uint64_t tenant_id_;
  int64_t balance_group_id_;
};

class HashIndexCollection;
typedef common::hash::ObHashMap<common::ObPartitionKey, HashIndexMapItem> IndexMap;
class HashIndexMap : public BalanceGroupBox {
public:
  friend HashIndexCollection;
  typedef HashIndexMapItem Item;

public:
  HashIndexMap() : BalanceGroupBox(), inited_(false), index_map_()
  {}
  virtual ~HashIndexMap()
  {}
  TO_STRING_EMPTY();

public:
  int init(const int64_t map_size);
  virtual BalanceGroupContainerType get_container_type() const override
  {
    return HASH_INDEX_TYPE;
  }
  virtual int set_item(const BalanceGroupBoxItem& item) override;

private:
  bool inited_;
  IndexMap index_map_;
};

class HashIndexCollection : public BalanceGroupCollection {
public:
  HashIndexCollection() : BalanceGroupCollection(), inited_(false), index_map_()
  {}
  virtual ~HashIndexCollection()
  {}

public:
  int init(const int64_t item_size);
  void reuse()
  {
    index_map_.reuse();
  }

public:
  virtual BalanceGroupContainerType get_container_type() const override
  {
    return HASH_INDEX_TYPE;
  }
  virtual int collect_box(const BalanceGroupBox* balance_group_box) override;
  int get_partition_index(const common::ObPartitionKey& pkey, HashIndexMapItem& hash_index_item) const;
  int assign(const HashIndexCollection& that);

private:
  bool inited_;
  IndexMap index_map_;
};
}  // end namespace balancer
}  // end namespace rootserver
}  // end namespace oceanbase

#endif /* _OB_BALANCE_GROUP_DATA_H */
