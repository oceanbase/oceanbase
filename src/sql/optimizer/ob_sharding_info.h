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
#include "sql/optimizer/ob_table_partition_info.h"
#include "sql/resolver/expr/ob_raw_expr.h"
#include "sql/optimizer/ob_phy_table_location_info.h"

namespace oceanbase
{
namespace sql
{
class ObRawExpr;
class ObOptimizerContext;
class ObDMLStmt;
class ObLogPlan;
typedef common::ObFixedArray<ObRawExpr*, common::ObIAllocator> ObRawExprSet;
typedef common::ObSEArray<ObRawExprSet*, 8, common::ModulePageAllocator, true> ObRawExprSets;
typedef ObRawExprSets EqualSets;

class ObShardingInfo
{
public:
  ObShardingInfo()
    : part_level_(share::schema::PARTITION_LEVEL_MAX),
      part_func_type_(share::schema::PARTITION_FUNC_TYPE_MAX),
      subpart_func_type_(share::schema::PARTITION_FUNC_TYPE_MAX),
      location_type_(OB_TBL_LOCATION_UNINITIALIZED),
      part_num_(0),
      partition_keys_(),
      sub_partition_keys_(),
      phy_table_location_info_(NULL),
      partition_array_ (NULL),
      all_tablet_ids_(),
      all_partition_indexes_(),
      all_subpartition_indexes_(),
      is_partition_single_(false),
      is_subpartition_sinlge_(false),
      can_reselect_replica_(false)
  {}
  ObShardingInfo(ObTableLocationType type)
  : part_level_(share::schema::PARTITION_LEVEL_MAX),
    part_func_type_(share::schema::PARTITION_FUNC_TYPE_MAX),
    subpart_func_type_(share::schema::PARTITION_FUNC_TYPE_MAX),
    location_type_(type),
    part_num_(0),
    partition_keys_(),
    sub_partition_keys_(),
    phy_table_location_info_(NULL),
    partition_array_ (NULL),
    all_tablet_ids_(),
    all_partition_indexes_(),
    all_subpartition_indexes_(),
    is_partition_single_(false),
    is_subpartition_sinlge_(false),
    can_reselect_replica_(false)
  {}

  virtual ~ObShardingInfo()
  {}
  int assign(const ObShardingInfo &other);
  /**
   *  Set the partition key and num based on table schema
   */
  int init_partition_info(ObOptimizerContext &ctx,
                          const ObDMLStmt &stmt,
                          const uint64_t table_id,
                          const uint64_t ref_table_id,
                          const ObCandiTableLoc &phy_table_location_info);
  const common::ObIArray<ObRawExpr*> &get_partition_keys() const { return partition_keys_; }
  common::ObIArray<ObRawExpr*> &get_partition_keys() { return partition_keys_; }
  const common::ObIArray<ObRawExpr*> &get_sub_partition_keys() const { return sub_partition_keys_; }
  common::ObIArray<ObRawExpr*> &get_sub_partition_keys() { return sub_partition_keys_; }
  const common::ObIArray<ObRawExpr*> &get_partition_func() const { return part_func_exprs_; }
  common::ObIArray<ObRawExpr*> &get_partition_func() { return part_func_exprs_; }
  inline ObCandiTableLoc *get_phy_table_location_info()
  {
    return const_cast<ObCandiTableLoc *>
      (static_cast<const ObShardingInfo&>(*this).get_phy_table_location_info());
  }
  inline const ObCandiTableLoc *get_phy_table_location_info() const
  { return phy_table_location_info_; }

  inline void set_phy_table_location_info(ObCandiTableLoc *phy_loc_info)
  { phy_table_location_info_ = phy_loc_info; }

  inline int64_t get_part_cnt() const
  {
    return NULL == phy_table_location_info_ ? 0 : phy_table_location_info_->get_partition_cnt();
  }

  // 分区相关的getter
  inline share::schema::ObPartitionLevel get_part_level() const { return part_level_;}
  inline share::schema::ObPartitionFuncType get_part_func_type() const { return part_func_type_; }
  inline share::schema::ObPartitionFuncType get_sub_part_func_type() const
  { return subpart_func_type_; }
  inline int64_t get_part_number() const { return part_num_; }
  inline share::schema::ObPartition ** get_partition_array() const
  { return partition_array_; }
  const common::ObIArray<uint64_t> &get_all_tablet_ids() const { return all_tablet_ids_; }
  const common::ObIArray<int64_t> &get_all_partition_indexes() const { return all_partition_indexes_; }
  const common::ObIArray<int64_t> &get_all_subpartition_indexes() const { return all_subpartition_indexes_; }
  inline bool is_partition_single() const { return is_partition_single_; }
  inline bool is_subpartition_single() const { return is_subpartition_sinlge_; }
  // end 分区相关的getter

  // for set-op and insert-op to check partition wise
  static int check_if_match_partition_wise(const EqualSets &equal_sets,
                                           const common::ObIArray<ObRawExpr*> &left_keys,
                                           const common::ObIArray<ObRawExpr*> &right_keys,
                                           ObShardingInfo *left_strong_sharding,
                                           ObShardingInfo *right_strong_sharding,
                                           bool &is_partition_wise);

  // for join and subplan filter op to check partition wise
  static int check_if_match_partition_wise(const EqualSets &equal_sets,
                                           const common::ObIArray<ObRawExpr*> &left_keys,
                                           const common::ObIArray<ObRawExpr*> &right_keys,
                                           const common::ObIArray<bool> &null_safe_info,
                                           ObShardingInfo *left_strong_sharding,
                                           const common::ObIArray<ObShardingInfo *> &left_weak_sharding,
                                           ObShardingInfo *right_strong_sharding,
                                           const common::ObIArray<ObShardingInfo *> &right_weak_sharding,
                                           bool &is_partition_wise);

  static int check_if_match_partition_wise(const EqualSets &equal_sets,
                                           const common::ObIArray<ObRawExpr*> &left_keys,
                                           const common::ObIArray<ObRawExpr*> &right_keys,
                                           const common::ObIArray<ObShardingInfo *> &left_sharding,
                                           const common::ObIArray<ObShardingInfo *> &right_sharding,
                                           bool &is_partition_wise);

  static int check_if_match_extended_partition_wise(const EqualSets &equal_sets,
                                                    ObIArray<ObAddr> &left_server_list,
                                                    ObIArray<ObAddr> &right_server_list,
                                                    const common::ObIArray<ObRawExpr*> &left_keys,
                                                    const common::ObIArray<ObRawExpr*> &right_keys,
                                                    const common::ObIArray<ObShardingInfo *> &left_sharding,
                                                    const common::ObIArray<ObShardingInfo *> &right_sharding,
                                                    bool &is_ext_partition_wise);

  static int check_if_match_extended_partition_wise(const EqualSets &equal_sets,
                                                    ObIArray<ObAddr> &left_server_list,
                                                    ObIArray<ObAddr> &right_server_list,
                                                    const common::ObIArray<ObRawExpr*> &left_keys,
                                                    const common::ObIArray<ObRawExpr*> &right_keys,
                                                    const common::ObIArray<bool> &null_safe_info,
                                                    ObShardingInfo *left_strong_sharding,
                                                    const common::ObIArray<ObShardingInfo *> &left_weak_sharding,
                                                    ObShardingInfo *right_strong_sharding,
                                                    const common::ObIArray<ObShardingInfo *> &right_weak_sharding,
                                                    bool &is_ext_partition_wise);

  static int check_if_match_extended_partition_wise(const EqualSets &equal_sets,
                                                    ObIArray<ObAddr> &left_server_list,
                                                    ObIArray<ObAddr> &right_server_list,
                                                    const common::ObIArray<ObRawExpr*> &left_keys,
                                                    const common::ObIArray<ObRawExpr*> &right_keys,
                                                    ObShardingInfo *left_strong_sharding,
                                                    ObShardingInfo *right_strong_sharding,
                                                    bool &is_ext_partition_wise);

  static int check_if_match_repart_or_rehash(const EqualSets &equal_sets,
                                             const common::ObIArray<ObRawExpr *> &src_join_keys,
                                             const common::ObIArray<ObRawExpr *> &target_join_keys,
                                             const common::ObIArray<ObRawExpr *> &target_part_keys,
                                             bool &is_match_join_keys);

  static int is_physically_both_shuffled_serverlist(ObIArray<ObAddr> &left_server_list,
                                                    ObIArray<ObAddr> &right_server_list,
                                                    bool &is_both_shuffled_serverlist);

  static int is_physically_equal_serverlist(ObIArray<ObAddr> &left_server_list,
                                            ObIArray<ObAddr> &right_server_list,
                                            bool &is_equal_serverlist);

  static bool is_shuffled_server_list(const ObIArray<ObAddr> &server_list);

  static int is_sharding_equal(const ObShardingInfo *left_strong_sharding,
                               const ObIArray<ObShardingInfo*> &left_weak_shardings,
                               const ObShardingInfo *right_strong_sharding,
                               const ObIArray<ObShardingInfo*> &right_weak_shardings,
                               const EqualSets &equal_sets,
                               bool &is_equal);

  static int is_sharding_equal(const ObIArray<ObShardingInfo*> &left_sharding,
                               const ObIArray<ObShardingInfo*> &right_sharding,
                               const EqualSets &equal_sets,
                               bool &is_equal);

  static int is_subset_sharding(const ObIArray<ObShardingInfo*> &subset_sharding,
                                const ObIArray<ObShardingInfo*> &target_sharding,
                                const EqualSets &equal_sets,
                                bool &is_subset);

  static int is_sharding_equal(const ObShardingInfo *left_sharding,
                               const ObShardingInfo *right_sharding,
                               const EqualSets &equal_sets,
                               bool &is_equal);

  static int extract_partition_key(const common::ObIArray<ObShardingInfo *> &input_shardings,
                                   ObIArray<ObSEArray<ObRawExpr*, 8>> &partition_key_list);

  static int get_serverlist_from_sharding(const ObShardingInfo &sharding,
                                          ObIArray<common::ObAddr> &server_list);

  inline void set_location_type(ObTableLocationType location_type)
  {
    location_type_ = location_type;
  }
  inline ObTableLocationType get_location_type() const
  {
    return location_type_;
  }
  inline bool is_sharding() const
  {
    return (OB_TBL_LOCATION_REMOTE == location_type_ ||
            OB_TBL_LOCATION_DISTRIBUTED == location_type_);
  }
  inline bool is_distributed() const
  {
    return OB_TBL_LOCATION_DISTRIBUTED == location_type_;
  }
  inline void set_distributed()
  {
    location_type_ = OB_TBL_LOCATION_DISTRIBUTED;
  }
  inline bool is_match_all() const
  {
    return (OB_TBL_LOCATION_ALL == location_type_);
  }
  inline void set_match_all()
  {
    location_type_ = OB_TBL_LOCATION_ALL;
  }
  inline bool is_remote() const
  {
    return (OB_TBL_LOCATION_REMOTE == location_type_);
  }
  inline void set_remote()
  {
    location_type_ = OB_TBL_LOCATION_REMOTE;
  }
  bool is_local() const
  {
    return OB_TBL_LOCATION_LOCAL == location_type_;
  }
  inline void set_local()
  {
    location_type_ = OB_TBL_LOCATION_LOCAL;
  }
  bool is_single() const
  {
    return (OB_TBL_LOCATION_LOCAL == location_type_ ||
            OB_TBL_LOCATION_ALL == location_type_ ||
            OB_TBL_LOCATION_REMOTE == location_type_);
  }
  bool is_uninitial() const {
    return OB_TBL_LOCATION_UNINITIALIZED == location_type_;
  }
  void set_can_reselect_replica(const bool b) { can_reselect_replica_ = b; }
  inline bool get_can_reselect_replica() const
  {
    bool ret = false;
    if (!can_reselect_replica_) {
      ret = false;
    } else if (NULL == phy_table_location_info_ ||
        (1 != phy_table_location_info_->get_partition_cnt())) {
      ret = false;
    } else {
      ret = phy_table_location_info_->get_phy_part_loc_info_list().at(0)
                                      .get_partition_location()
                                      .get_replica_locations().count() > 0;
    }
    return ret;
  }

  inline bool is_distributed_without_table_location() const {
    return is_distributed() && NULL == phy_table_location_info_;
  }
  inline bool is_distributed_with_table_location() const {
    return is_distributed() && NULL != phy_table_location_info_;
  }
  inline bool is_distributed_without_partitioning() const {
    return is_distributed() && partition_keys_.empty();
  }
  inline bool is_distributed_with_partitioning() const {
    return is_distributed() && !partition_keys_.empty();
  }
  inline bool is_distributed_with_table_location_and_partitioning()
  {
    return is_distributed() && NULL != phy_table_location_info_ && !partition_keys_.empty();
  }
  inline bool is_distributed_without_table_location_with_partitioning()
  {
    return is_distributed() && NULL == phy_table_location_info_ && !partition_keys_.empty();
  }
  /**
   * 获取所有的分区键
   * @param ignore_single_partition: 是否忽略掉只涉及一个分区的分区登记上的分区键
   */
  int get_all_partition_keys(common::ObIArray<ObRawExpr*> &out_part_keys,
                             bool ignore_single_partition = false) const;

  int copy_with_part_keys(const ObShardingInfo &other);
  int copy_without_part_keys(const ObShardingInfo &other);
  int get_remote_addr(ObAddr &remote) const;
  int get_total_part_cnt(int64_t &total_part_cnt) const;
  TO_STRING_KV(K_(part_level),
               K_(part_func_type),
               K_(subpart_func_type),
               K_(part_num),
               K_(location_type),
               K_(can_reselect_replica),
               K_(phy_table_location_info));

private:
  int set_partition_key(ObRawExpr *part_expr,
                        const share::schema::ObPartitionFuncType part_func_type,
                        common::ObIArray<ObRawExpr*> &partition_keys);

  // check whether all partition keys are of the same type
  static int is_compatible_partition_key(const ObShardingInfo *first_sharding,
                                         const ObIArray<ObSEArray<ObRawExpr*, 8>> &first_part_keys_list,
                                         const ObIArray<ObSEArray<ObRawExpr*, 8>> &second_part_keys_list,
                                         bool &is_compatible);
  static int is_compatible_partition_key(const ObShardingInfo &first_sharding,
                                         const ObShardingInfo &second_sharding,
                                         bool &is_compatible);

  // check whether all the partition keys are covered by join keys
  static int is_join_key_cover_partition_key(const EqualSets &equal_sets,
                                             const common::ObIArray<ObRawExpr *> &first_keys,
                                             const common::ObIArray<ObShardingInfo *> &first_shardings,
                                             const common::ObIArray<ObRawExpr *> &second_keys,
                                             const common::ObIArray<ObShardingInfo *> &second_shardins,
                                             bool &is_cover);

  static int is_expr_equivalent(const EqualSets &equal_sets,
                                const sql::ObShardingInfo &first_sharding,
                                const common::ObIArray<ObRawExpr*> &first_part_exprs,
                                const common::ObIArray<ObRawExpr*> &second_part_exprs,
                                ObRawExpr *first_key,
                                ObRawExpr *second_key,
                                bool &is_equal);

  static int is_lossless_column_cast(const ObRawExpr *expr,
                                     const sql::ObShardingInfo &sharding_info, bool &is_lossless);

  static bool is_part_func_scale_sensitive(const sql::ObShardingInfo &sharding_info,
                                           const common::ObObjType obj_type);

  static inline bool is_shuffled_addr(ObAddr addr) { return UINT32_MAX == addr.get_port(); }

private:
  // 分区级别
  share::schema::ObPartitionLevel part_level_;
  // 一级分区类型
  share::schema::ObPartitionFuncType part_func_type_;
  // 二级分区类型
  share::schema::ObPartitionFuncType subpart_func_type_;
  ObTableLocationType location_type_;
  // 一级分区数量
  int64_t part_num_;
  //一级分区的分区键
  common::ObSEArray<ObRawExpr *, 4, common::ModulePageAllocator, true> partition_keys_;
  //二级分区的分区键
  common::ObSEArray<ObRawExpr *, 4, common::ModulePageAllocator, true> sub_partition_keys_;
  //分区方法expr
  common::ObSEArray<ObRawExpr *, 4, common::ModulePageAllocator, true> part_func_exprs_;
  const ObCandiTableLoc *phy_table_location_info_;
  // 一级分区array
  share::schema::ObPartition **partition_array_;
  // phy_table_location_info_中所有的partition_id(物理分区id)
  common::ObSEArray<uint64_t, 8, common::ModulePageAllocator, true> all_tablet_ids_;
  // phy_table_location_info_中每一个partition_id(物理分区id)的
  // part_id(一级逻辑分区id)在part_array中的偏移
  common::ObSEArray<int64_t, 8, common::ModulePageAllocator, true> all_partition_indexes_;
  // phy_table_location_info_中每一个partition_id(物理分区id)的
  // subpart_id(二级逻辑分区id)在subpart_array中的偏移
  common::ObSEArray<int64_t, 8, common::ModulePageAllocator, true> all_subpartition_indexes_;
  // 二级分区表的phy_table_location_info_中，是否只涉及到一个一级分区
  bool is_partition_single_;
  // 二级分区表的phy_table_location_info_中，是否每个一级分区都只涉及一个二级分区
  bool is_subpartition_sinlge_;
  bool can_reselect_replica_; //能否重新选择partition的leader, 仅最下层的复制表允许, 继承后不再允许
  DISALLOW_COPY_AND_ASSIGN(ObShardingInfo);
};
}
}
#endif
