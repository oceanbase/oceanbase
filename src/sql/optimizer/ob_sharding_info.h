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

#ifndef OCEANBASE_SQL_OPTIMIZER_OB_SHARDING_INFO_H
#define OCEANBASE_SQL_OPTIMIZER_OB_SHARDING_INFO_H 1
#include "lib/container/ob_array.h"
#include "lib/container/ob_se_array.h"
#include "lib/container/ob_fixed_array.h"
#include "lib/allocator/ob_allocator.h"
#include "lib/allocator/page_arena.h"
#include "share/partition_table/ob_partition_location.h"
#include "share/partition_table/ob_partition_location_cache.h"
#include "sql/optimizer/ob_table_partition_info.h"
#include "sql/resolver/expr/ob_raw_expr.h"

namespace oceanbase {
namespace sql {
class ObRawExpr;
class ObOptimizerContext;
class ObDMLStmt;
class ObLogPlan;
typedef common::ObFixedArray<ObRawExpr*, common::ObIAllocator> ObRawExprSet;
typedef common::ObSEArray<ObRawExprSet*, 8, common::ModulePageAllocator, true> ObRawExprSets;
typedef ObRawExprSets EqualSets;

class ObShardingInfo {
public:
  ObShardingInfo()
      : part_level_(share::schema::PARTITION_LEVEL_ZERO),
        part_func_type_(share::schema::PARTITION_FUNC_TYPE_MAX),
        subpart_func_type_(share::schema::PARTITION_FUNC_TYPE_MAX),
        part_num_(0),
        subpart_num_(0),
        partition_keys_(),
        sub_partition_keys_(),
        location_type_(OB_TBL_LOCATION_UNINITIALIZED),
        phy_table_location_info_(NULL),
        partition_array_(NULL),
        subpartition_array_(NULL),
        can_reselect_replica_(false),
        ref_table_id_(OB_INVALID_ID),
        all_partition_ids_(),
        all_partition_indexes_(),
        all_subpartition_indexes_(),
        is_sub_part_template_(true),
        is_partition_single_(false),
        is_subpartition_sinlge_(false)
  {}
  virtual ~ObShardingInfo()
  {}
  void reset();
  int assign(const ObShardingInfo& other);
  /**
   *  Set the partition key and num based on table schema
   */
  int init_partition_info(ObOptimizerContext& ctx, ObDMLStmt& stmt, const uint64_t table_id,
      const uint64_t ref_table_id, const ObPhyTableLocationInfo& phy_table_location_info);
  const common::ObIArray<ObRawExpr*>& get_partition_keys() const
  {
    return partition_keys_;
  }
  common::ObIArray<ObRawExpr*>& get_partition_keys()
  {
    return partition_keys_;
  }
  const common::ObIArray<ObRawExpr*>& get_sub_partition_keys() const
  {
    return sub_partition_keys_;
  }
  common::ObIArray<ObRawExpr*>& get_sub_partition_keys()
  {
    return sub_partition_keys_;
  }
  const common::ObIArray<ObRawExpr*>& get_partition_func() const
  {
    return part_func_exprs_;
  }
  common::ObIArray<ObRawExpr*>& get_partition_func()
  {
    return part_func_exprs_;
  }
  inline ObPhyTableLocationInfo* get_phy_table_location_info()
  {
    return const_cast<ObPhyTableLocationInfo*>(static_cast<const ObShardingInfo&>(*this).get_phy_table_location_info());
  }
  inline const ObPhyTableLocationInfo* get_phy_table_location_info() const
  {
    return phy_table_location_info_;
  }

  inline share::schema::ObPartitionLevel get_part_level() const
  {
    return part_level_;
  }
  inline share::schema::ObPartitionFuncType get_part_func_type() const
  {
    return part_func_type_;
  }
  inline share::schema::ObPartitionFuncType get_sub_part_func_type() const
  {
    return subpart_func_type_;
  }
  inline bool get_is_sub_part_template() const
  {
    return is_sub_part_template_;
  }
  inline int64_t get_part_number() const
  {
    return part_num_;
  }
  inline int64_t get_sub_part_number() const
  {
    return subpart_num_;
  }
  inline share::schema::ObPartition** get_partition_array() const
  {
    return partition_array_;
  }
  inline share::schema::ObSubPartition** get_subpartition_array() const
  {
    return subpartition_array_;
  }
  const common::ObIArray<int64_t>& get_all_partition_ids() const
  {
    return all_partition_ids_;
  }
  const common::ObIArray<int64_t>& get_all_partition_indexes() const
  {
    return all_partition_indexes_;
  }
  const common::ObIArray<int64_t>& get_all_subpartition_indexes() const
  {
    return all_subpartition_indexes_;
  }
  inline bool is_partition_single() const
  {
    return is_partition_single_;
  }
  inline bool is_subpartition_single() const
  {
    return is_subpartition_sinlge_;
  }

  /*
   * Requirements of partition wise join:
   * 1 two inputs are logically equal partitioned
   * 2 partition keys are covered by join keys
   * 3 two inputs are physically equal partitioned
   */
  static int check_if_match_partition_wise(ObLogPlan& log_plan, const EqualSets& equal_sets,
      const common::ObIArray<ObRawExpr*>& left_keys, const common::ObIArray<ObRawExpr*>& right_keys,
      const ObShardingInfo& left_sharding, const ObShardingInfo& right_sharding, bool& is_partition_wise);

  static int check_if_match_repart(const EqualSets& equal_sets, const common::ObIArray<ObRawExpr*>& src_join_keys,
      const common::ObIArray<ObRawExpr*>& target_join_keys, const common::ObIArray<ObRawExpr*>& target_part_keys,
      bool& is_match);

  /*
   * physicall_equal_partitioned: partitions of two inputs are placed using the same policy
   */
  static int is_physically_equal_partitioned(ObLogPlan& log_plan, const ObShardingInfo& left_sharding,
      const ObShardingInfo& right_sharding, bool& is_physical_equal);

  /*
   * for enhanced partition wise joind
   */
  static int is_physically_equal_partitioned(
      const ObShardingInfo& left_sharding, const ObShardingInfo& right_sharding, bool& is_physical_equal);

  bool is_distributed() const
  {
    return OB_TBL_LOCATION_DISTRIBUTED == location_type_;
  }
  ObTableLocationType get_location_type() const
  {
    return location_type_;
  }

  inline bool is_sharding() const
  {
    return (OB_TBL_LOCATION_REMOTE == location_type_ || OB_TBL_LOCATION_DISTRIBUTED == location_type_);
  }

  inline bool is_remote_or_distribute() const
  {
    return (OB_TBL_LOCATION_REMOTE == location_type_ || OB_TBL_LOCATION_DISTRIBUTED == location_type_);
  }

  inline bool is_match_all() const
  {
    return (OB_TBL_LOCATION_ALL == location_type_);
  }
  inline bool is_remote() const
  {
    return (OB_TBL_LOCATION_REMOTE == location_type_);
  }
  bool is_local() const
  {
    return OB_TBL_LOCATION_LOCAL == location_type_;
  }
  bool is_single() const
  {
    return (OB_TBL_LOCATION_LOCAL == location_type_ || OB_TBL_LOCATION_ALL == location_type_ ||
            OB_TBL_LOCATION_REMOTE == location_type_);
  }

  bool is_uninitial() const
  {
    return OB_TBL_LOCATION_UNINITIALIZED == location_type_;
  }

  bool is_calculated() const
  {
    return OB_TBL_LOCATION_UNINITIALIZED != location_type_;
  }
  inline int64_t get_part_cnt() const
  {
    return NULL == phy_table_location_info_ ? 0 : phy_table_location_info_->get_partition_cnt();
  }

  void set_location_type(ObTableLocationType location_type)
  {
    location_type_ = location_type;
  }
  int copy_with_part_keys(const ObShardingInfo& other);
  int copy_without_part_keys(const ObShardingInfo& other);
  void set_can_reselect_replica(const bool b)
  {
    can_reselect_replica_ = b;
  }
  bool get_can_reselect_replica() const
  {
    return can_reselect_replica_;
  }

  // get number of physical partition touched
  int get_total_part_cnt(int64_t& total_part_cnt) const;

  int64_t get_ref_table_id() const
  {
    return ref_table_id_;
  }
  void set_ref_table_id(int64_t ref_table_id)
  {
    ref_table_id_ = ref_table_id;
  }

  int get_all_partition_keys(common::ObIArray<ObRawExpr*>& out_part_keys, bool ignore_single_partition = false) const;

  TO_STRING_KV(K(is_sharding()), K(is_local()), K(is_remote_or_distribute()), K(is_match_all()), K_(part_level),
      K_(part_func_type), K_(subpart_func_type), K_(part_num), K_(subpart_num), K_(location_type),
      K_(can_reselect_replica), K_(phy_table_location_info));

private:
  int set_partition_key(ObRawExpr* part_expr, const share::schema::ObPartitionFuncType part_func_type,
      common::ObIArray<ObRawExpr*>& partition_keys);

  int adjust_key_partition_type(const ObRawExpr* part_expr, share::schema::ObPartitionFuncType& type);

  // check whether all the partition keys are covered by join keys
  static int is_join_key_cover_partition_key(const EqualSets& equal_sets,
      const common::ObIArray<ObRawExpr*>& first_keys, const common::ObIArray<ObRawExpr*>& first_part_keys,
      const common::ObIArray<ObRawExpr*>& second_keys, const common::ObIArray<ObRawExpr*>& second_part_keys,
      bool& is_cover);

  static int is_compatible_partition_key(const common::ObIArray<ObRawExpr*>& first_part_keys,
      const common::ObIArray<ObRawExpr*>& second_part_keys, bool& is_compatible);

private:
  share::schema::ObPartitionLevel part_level_;

  share::schema::ObPartitionFuncType part_func_type_;

  share::schema::ObPartitionFuncType subpart_func_type_;

  int64_t part_num_;

  int64_t subpart_num_;

  common::ObSEArray<ObRawExpr*, 4, common::ModulePageAllocator, true> partition_keys_;

  common::ObSEArray<ObRawExpr*, 4, common::ModulePageAllocator, true> sub_partition_keys_;

  common::ObSEArray<ObRawExpr*, 4, common::ModulePageAllocator, true> part_func_exprs_;
  ObTableLocationType location_type_;
  const ObPhyTableLocationInfo* phy_table_location_info_;

  share::schema::ObPartition** partition_array_;

  share::schema::ObSubPartition** subpartition_array_;
  bool can_reselect_replica_;
  int64_t ref_table_id_;

  common::ObSEArray<int64_t, 32, common::ModulePageAllocator, true> all_partition_ids_;

  common::ObSEArray<int64_t, 32, common::ModulePageAllocator, true> all_partition_indexes_;

  common::ObSEArray<int64_t, 32, common::ModulePageAllocator, true> all_subpartition_indexes_;
  bool is_sub_part_template_;
  bool is_partition_single_;
  bool is_subpartition_sinlge_;
  DISALLOW_COPY_AND_ASSIGN(ObShardingInfo);
};
}  // namespace sql
}  // namespace oceanbase
#endif
