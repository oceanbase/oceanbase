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

#ifndef OCEANBASE_SQL_OPTIMIZER_OB_PWJ_COMPARER_H
#define OCEANBASE_SQL_OPTIMIZER_OB_PWJ_COMPARER_H 1
#include "lib/container/ob_array.h"
#include "share/partition_table/ob_partition_location.h"
#include "share/partition_table/ob_partition_location_cache.h"
#include "sql/optimizer/ob_table_partition_info.h"
#include "sql/resolver/expr/ob_raw_expr.h"
#include "sql/optimizer/ob_sharding_info.h"

namespace oceanbase {
namespace sql {
class ObRawExpr;
class ObOptimizerContext;
class ObDMLStmt;
class ObLogPlan;

struct PwjTable {
  PwjTable()
      : ref_table_id_(OB_INVALID_ID),
        phy_table_loc_info_(NULL),
        part_level_(share::schema::PARTITION_LEVEL_ZERO),
        part_type_(share::schema::PARTITION_FUNC_TYPE_MAX),
        subpart_type_(share::schema::PARTITION_FUNC_TYPE_MAX),
        partition_array_(NULL),
        def_subpartition_array_(NULL),
        is_sub_part_template_(true),
        part_number_(-1),
        def_subpart_number_(-1),
        is_partition_single_(false),
        is_subpartition_single_(false)
  {}

  virtual ~PwjTable()
  {}

  int assign(const PwjTable& other);

  int init(const ObShardingInfo& info);
  int init(const share::schema::ObTableSchema& table_schema, const ObPhyTableLocationInfo& phy_tbl_info);

  TO_STRING_KV(K_(ref_table_id), K_(part_level), K_(part_type), K_(subpart_type), K_(is_sub_part_template),
      K_(part_number), K_(def_subpart_number), K_(is_partition_single), K_(is_subpartition_single),
      K_(all_partition_indexes), K_(all_subpartition_indexes));

  uint64_t ref_table_id_;
  const ObPhyTableLocationInfo* phy_table_loc_info_;
  share::schema::ObPartitionLevel part_level_;
  share::schema::ObPartitionFuncType part_type_;
  share::schema::ObPartitionFuncType subpart_type_;
  share::schema::ObPartition** partition_array_;
  share::schema::ObSubPartition** def_subpartition_array_;
  bool is_sub_part_template_;
  int64_t part_number_;
  int64_t def_subpart_number_;
  bool is_partition_single_;
  bool is_subpartition_single_;
  common::ObSEArray<int64_t, 16, common::ModulePageAllocator, true> ordered_partition_ids_;
  common::ObSEArray<int64_t, 16, common::ModulePageAllocator, true> all_partition_indexes_;
  common::ObSEArray<int64_t, 16, common::ModulePageAllocator, true> all_subpartition_indexes_;
};

typedef common::hash::ObHashMap<uint64_t, PartitionIdArray, common::hash::NoPthreadDefendMode> PWJPartitionIdMap;

class ObPwjComparer {
public:
  ObPwjComparer(bool is_strict)
      : is_strict_(is_strict),
        pwj_tables_(),
        part_id_map_(),
        part_index_map_(),
        subpart_id_map_(),
        phy_part_map_(),
        partition_id_group_()
  {}
  virtual ~ObPwjComparer()
  {}

  void reset();

  int add_table(PwjTable& table, bool& is_match_pwj);

  static int extract_all_partition_indexes(const ObPhyTableLocationInfo& phy_table_location_info,
      const share::schema::ObTableSchema& table_schema, ObIArray<int64_t>& all_partition_ids,
      ObIArray<int64_t>& all_partition_indexes, ObIArray<int64_t>& all_subpartition_indexes, bool& is_partition_single,
      bool& is_subpartition_single);

  int get_used_partition_indexes(const int64_t part_count, const ObIArray<int64_t>& all_partition_indexes,
      ObIArray<int64_t>& used_partition_indexes);

  int check_logical_equal_and_calc_match_map(const PwjTable& l_table, const PwjTable& r_table, bool& is_match);

  int is_first_partition_logically_equal(const PwjTable& l_table, const PwjTable& r_table, bool& is_equal);

  int is_sub_partition_logically_equal(const PwjTable& l_table, const PwjTable& r_table, bool& is_equal);

  int get_subpartition_indexes_by_part_index(
      const PwjTable& table, const int64_t part_index, ObIArray<int64_t>& used_subpart_indexes);

  int check_hash_partition_equal(const PwjTable& l_table, const PwjTable& r_table, const ObIArray<int64_t>& l_indexes,
      const ObIArray<int64_t>& r_indexes, ObIArray<std::pair<int64_t, int64_t> >& part_id_map,
      ObIArray<std::pair<int64_t, int64_t> >& part_index_map, bool& is_equal);

  int check_hash_subpartition_equal(share::schema::ObSubPartition** l_subpartition_array,
      share::schema::ObSubPartition** r_subpartition_array, const ObIArray<int64_t>& l_indexes,
      const ObIArray<int64_t>& r_indexes, ObIArray<std::pair<int64_t, int64_t> >& sub_part_id_map, bool& is_equal);

  static int check_range_partition_equal(share::schema::ObPartition** left_partition_array,
      share::schema::ObPartition** right_partition_array, const ObIArray<int64_t>& left_indexes,
      const ObIArray<int64_t>& right_indexes, ObIArray<std::pair<int64_t, int64_t> >& part_id_map,
      ObIArray<std::pair<int64_t, int64_t> >& part_index_map, bool& is_equal);

  static int check_range_subpartition_equal(share::schema::ObSubPartition** left_subpartition_array,
      share::schema::ObSubPartition** right_subpartition_array, const ObIArray<int64_t>& left_indexes,
      const ObIArray<int64_t>& right_indexes, ObIArray<std::pair<int64_t, int64_t> >& sub_part_id_map, bool& is_equal);

  static int check_list_partition_equal(share::schema::ObPartition** left_partition_array,
      share::schema::ObPartition** right_partition_array, const ObIArray<int64_t>& left_indexes,
      const ObIArray<int64_t>& right_indexes, ObIArray<std::pair<int64_t, int64_t> >& part_id_map,
      ObIArray<std::pair<int64_t, int64_t> >& part_index_map, bool& is_equal);

  static int check_list_subpartition_equal(share::schema::ObSubPartition** left_subpartition_array,
      share::schema::ObSubPartition** right_subpartition_array, const ObIArray<int64_t>& left_indexes,
      const ObIArray<int64_t>& right_indexes, ObIArray<std::pair<int64_t, int64_t> >& sub_part_id_map, bool& is_equal);

  static int is_partition_equal(const share::schema::ObPartition* l_partition,
      const share::schema::ObPartition* r_partition, const bool is_range_partition, bool& is_equal);

  static int is_partition_equal_old(const share::schema::ObPartition* l_partition,
      const share::schema::ObPartition* r_partition, const bool is_range_partition, bool& is_equal);

  static int is_subpartition_equal(const share::schema::ObSubPartition* l_subpartition,
      const share::schema::ObSubPartition* r_subpartition, const bool is_range_partition, bool& is_equal);

  static int is_subpartition_equal_old(const share::schema::ObSubPartition* l_subpartition,
      const share::schema::ObSubPartition* r_subpartition, const bool is_range_partition, bool& is_equal);

  static int is_row_equal(const ObRowkey& first_row, const ObRowkey& second_row, bool& is_equal);

  static int is_list_partition_equal(const share::schema::ObBasePartition* first_partition,
      const share::schema::ObBasePartition* second_partition, bool& is_equal);

  static int is_row_equal(const common::ObNewRow& first_row, const common::ObNewRow& second_row, bool& is_equal);

  static int is_obj_equal(const common::ObObj& first_obj, const common::ObObj& second_obj, bool& is_equal);

  int check_if_match_partition_wise(const PwjTable& l_table, const PwjTable& r_table, bool& is_partition_wise);

  int is_simple_physically_equal_partitioned(const PwjTable& l_table, const PwjTable& r_table, bool& is_physical_equal);

  int is_physically_equal_partitioned(const PwjTable& l_table, const PwjTable& r_table, bool& is_physical_equal);

  static int check_replica_location_match(const ObPhyPartitionLocationInfo& left_location,
      const ObPhyPartitionLocationInfoIArray& right_locations, const int64_t partition_id, bool& is_physical_equal);

  int get_part_id_by_part_index(const PwjTable& table, const int64_t part_index, int64_t& part_id);

  int get_def_subpart_id_by_subpart_index(const PwjTable& table, const int64_t subpart_index, int64_t& subpart_id);
  int get_sub_part_id(const PwjTable& table, const int64_t& part_index, int64_t& sub_part_id);

  int64_t get_matched_phy_part_id(const int64_t l_phy_part_id);

  inline common::ObIArray<PwjTable>& get_pwj_tables()
  {
    return pwj_tables_;
  }
  inline common::ObIArray<PartitionIdArray>& get_partition_id_group()
  {
    return partition_id_group_;
  }

  TO_STRING_KV(K_(is_strict), K_(pwj_tables), K_(partition_id_group));

private:
  bool is_strict_;
  common::ObSEArray<PwjTable, 32, common::ModulePageAllocator, true> pwj_tables_;
  common::ObSEArray<std::pair<int64_t, int64_t>, 32, common::ModulePageAllocator, true> part_id_map_;
  common::ObSEArray<std::pair<int64_t, int64_t>, 32, common::ModulePageAllocator, true> part_index_map_;
  common::ObSEArray<std::pair<int64_t, int64_t>, 32, common::ModulePageAllocator, true> subpart_id_map_;
  common::ObSEArray<std::pair<int64_t, int64_t>, 32, common::ModulePageAllocator, true> phy_part_map_;
  common::ObSEArray<PartitionIdArray, 4, common::ModulePageAllocator, true> partition_id_group_;

  DISALLOW_COPY_AND_ASSIGN(ObPwjComparer);
};
}  // namespace sql
}  // namespace oceanbase
#endif
