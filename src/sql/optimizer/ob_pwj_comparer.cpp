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

#define USING_LOG_PREFIX SQL_OPT
#include "sql/optimizer/ob_pwj_comparer.h"
#include "share/schema/ob_table_schema.h"
#include "share/schema/ob_schema_struct.h"
#include "sql/optimizer/ob_optimizer_context.h"
#include "sql/optimizer/ob_logical_operator.h"
#include "sql/optimizer/ob_opt_est_utils.h"
#include "sql/resolver/dml/ob_dml_stmt.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"

using namespace oceanbase::sql;
using namespace oceanbase::common;
using namespace oceanbase::share::schema;

const int64_t ObPwjComparer::MIN_ID_LOCATION_BUCKET_NUMBER = 100;
const int64_t ObPwjComparer::DEFAULT_ID_ID_BUCKET_NUMBER = 1000;

int PwjTable::init(const ObShardingInfo &sharding)
{
  int ret = OB_SUCCESS;
  phy_table_loc_info_ = sharding.get_phy_table_location_info();
  part_level_ = sharding.get_part_level();
  part_type_ = sharding.get_part_func_type();
  subpart_type_ = sharding.get_sub_part_func_type();
  partition_array_ = sharding.get_partition_array();
  part_number_ = sharding.get_part_number();
  is_partition_single_ = sharding.is_partition_single();
  is_subpartition_single_ = sharding.is_subpartition_single();
  if (OB_FAIL(ordered_tablet_ids_.assign(sharding.get_all_tablet_ids()))) {
    LOG_WARN("failed to assign partition ids", K(ret));
  } else if (OB_FAIL(all_partition_indexes_.assign(sharding.get_all_partition_indexes()))) {
    LOG_WARN("failed to assign all partition indexes", K(ret));
  } else if (OB_FAIL(all_subpartition_indexes_.assign(sharding.get_all_subpartition_indexes()))) {
    LOG_WARN("failed to assign all subpartition indexes", K(ret));
  }
  return ret;
}

int PwjTable::init(const ObTableSchema &table_schema, const ObCandiTableLoc &phy_tbl_info)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObPwjComparer::extract_all_partition_indexes(phy_tbl_info,
                                                           table_schema,
                                                           ordered_tablet_ids_,
                                                           all_partition_indexes_,
                                                           all_subpartition_indexes_,
                                                           is_partition_single_,
                                                           is_subpartition_single_))) {
    LOG_WARN("failed to extract used partition indexes", K(ret));
  } else {
    phy_table_loc_info_ = &phy_tbl_info;
    part_level_ = table_schema.get_part_level();
    part_type_ = table_schema.get_part_option().get_part_func_type();
    partition_array_ = table_schema.get_part_array();
    part_number_ = table_schema.get_partition_num();
    if (share::schema::PARTITION_LEVEL_TWO == part_level_) {
      subpart_type_ = table_schema.get_sub_part_option().get_part_func_type();
    }
  }
  if (OB_SUCC(ret)
      && ((share::schema::PARTITION_LEVEL_TWO == part_level_) || (share::schema::PARTITION_LEVEL_ONE == part_level_))
      && 0 == part_number_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected part number", K(table_schema), K(phy_tbl_info));
  }
  return ret;
}

int PwjTable::init(const ObIArray<ObAddr> &server_list)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(server_list_.assign(server_list))) {
    LOG_WARN("failed to assign server list for non-strict cons", K(ret));
  }
  return ret;
}

int PwjTable::assign(const PwjTable &other)
{
  int ret = OB_SUCCESS;
  phy_table_loc_info_ = other.phy_table_loc_info_;
  part_level_ = other.part_level_;
  part_type_ = other.part_type_;
  subpart_type_ = other.subpart_type_;
  partition_array_ = other.partition_array_;
  part_number_ = other.part_number_;
  is_partition_single_ = other.is_partition_single_;
  is_subpartition_single_ = other.is_subpartition_single_;
  if (OB_FAIL(ordered_tablet_ids_.assign(other.ordered_tablet_ids_))) {
    LOG_WARN("failed to assign ordered partition indexes", K(ret));
  } else if (OB_FAIL(all_partition_indexes_.assign(other.all_partition_indexes_))) {
    LOG_WARN("failed to assign used partition indexes", K(ret));
  } else if (OB_FAIL(all_subpartition_indexes_.assign(other.all_subpartition_indexes_))) {
    LOG_WARN("failed to assign used subpartition indexes", K(ret));
  } else if (OB_FAIL(server_list_.assign(other.server_list_))) {
    LOG_WARN("failed to assign server list for non-strict cons", K(ret));
  }
  return ret;
}

void ObPwjComparer::reset()
{
  pwj_tables_.reset();
}

int ObPwjComparer::add_table(PwjTable &table, bool &is_match_pwj)
{
  int ret = OB_SUCCESS;
  UNUSED(table);
  is_match_pwj = false;
  return ret;
}

int ObPwjComparer::extract_all_partition_indexes(const ObCandiTableLoc &phy_table_location_info,
                                                  const ObTableSchema &table_schema,
                                                  ObIArray<uint64_t> &all_tablet_ids,
                                                  ObIArray<int64_t> &all_partition_indexes,
                                                  ObIArray<int64_t> &all_subpartition_indexes,
                                                  bool &is_partition_single,
                                                  bool &is_subpartition_single)
{
  int ret = OB_SUCCESS;
  const ObCandiTabletLocIArray &phy_partitions =
      phy_table_location_info.get_phy_part_loc_info_list();
  ObPartitionLevel part_level = table_schema.get_part_level();
  is_partition_single = false;
  is_subpartition_single = false;
  if (share::schema::PARTITION_LEVEL_ZERO == part_level) {
    // do nothing
  } else if (share::schema::PARTITION_LEVEL_ONE == part_level) {
    for (int64_t i = 0; OB_SUCC(ret) && i < phy_partitions.count(); ++i) {
      // 对于一级分区表part_id(一级逻辑分区id) = partition_id(物理分区id)
      ObTabletID tablet_id = phy_partitions.at(i).get_partition_location().get_tablet_id();
      int64_t part_index = -1;
      int64_t subpart_index = -1;
      if (table_schema.is_external_table()) {
        if (OB_FAIL(all_tablet_ids.push_back(tablet_id.id()))) { //mock
        LOG_WARN("failed to push back partition id", K(ret));
        } else if (OB_FAIL(all_partition_indexes.push_back(part_index))) {
          LOG_WARN("failed to push back partition index", K(ret));
        }
      } else if (OB_FAIL(table_schema.get_part_idx_by_tablet(tablet_id, part_index, subpart_index))) {
        LOG_WARN("failed to get part idx by tablet", K(ret));
      } else if (OB_FAIL(all_tablet_ids.push_back(tablet_id.id()))) {
        LOG_WARN("failed to push back partition id", K(ret));
      } else if (OB_FAIL(all_partition_indexes.push_back(part_index))) {
        LOG_WARN("failed to push back partition index", K(ret));
      }
    }
  } else if (share::schema::PARTITION_LEVEL_TWO == part_level) {
    for (int64_t i = 0; OB_SUCC(ret) && i < phy_partitions.count(); ++i) {
      int64_t partition_id = phy_partitions.at(i).get_partition_location().get_partition_id();
      ObTabletID tablet_id = phy_partitions.at(i).get_partition_location().get_tablet_id();
      int64_t part_index = -1;
      int64_t subpart_index = -1;
      if (OB_FAIL(table_schema.get_part_idx_by_tablet(tablet_id, part_index, subpart_index))) {
        LOG_WARN("failed to get part idx by tablet", K(ret));
      } else if (OB_FAIL(all_tablet_ids.push_back(tablet_id.id()))) {
        LOG_WARN("failed to push back partition id", K(ret));
      } else if (OB_FAIL(all_partition_indexes.push_back(part_index))) {
        LOG_WARN("failed to push back part index", K(ret));
      } else if (OB_FAIL(all_subpartition_indexes.push_back(subpart_index))) {
        LOG_WARN("failed to push back subpart index");
      }
    }
  }
  if (OB_SUCC(ret) && share::schema::PARTITION_LEVEL_TWO == part_level) {
    if (all_partition_indexes.count() > 0) {
      // 检查是否只涉及到一个一级分区
      const int64_t first_part_id = all_partition_indexes.at(0);
      is_partition_single = true;
      for (int64_t i = 1; i < all_partition_indexes.count(); ++i) {
        if (first_part_id != all_partition_indexes.at(i)) {
          is_partition_single = false;
          break;
        }
      }
    }
    if (all_subpartition_indexes.count() > 0) {
      // 是否每个一级分区都只涉及一个二级分区
      is_subpartition_single = true;
      ObSqlBitSet<> part_ids;
      for (int64_t i = 0; OB_SUCC(ret) && i < all_partition_indexes.count(); ++i) {
        if (part_ids.has_member(all_partition_indexes.at(i))) {
          is_subpartition_single = false;
          break;
        } else if (OB_FAIL(part_ids.add_member(all_partition_indexes.at(i)))) {
          LOG_WARN("failed to add member", K(ret));
        }
      }
    }
  }
  LOG_TRACE("success extract all partition indexes",
      K(phy_partitions), K(all_tablet_ids), K(all_partition_indexes),
      K(all_subpartition_indexes), K(is_partition_single), K(is_subpartition_single));
  return ret;
}

int ObPwjComparer::is_partition_equal(const ObPartition *l_partition,
                                      const ObPartition *r_partition,
                                      const bool is_range_partition,
                                      bool &is_equal)
{
  int ret = OB_SUCCESS;
  is_equal = false;
  if (OB_ISNULL(l_partition) || OB_ISNULL(r_partition)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(l_partition), K(r_partition), K(ret));
  } else if (is_range_partition) {
    if (OB_FAIL(is_row_equal(l_partition->get_high_bound_val(),
                             r_partition->get_high_bound_val(),
                             is_equal))) {
      LOG_WARN("failed to check is row equal", K(ret));
    }
  } else if (OB_FAIL(is_list_partition_equal(l_partition, r_partition, is_equal))) {
    LOG_WARN("failed to check is list partition equal", K(ret));
  }
  return ret;
}

int ObPwjComparer::is_subpartition_equal(const ObSubPartition *l_subpartition,
                                         const ObSubPartition *r_subpartition,
                                         const bool is_range_partition,
                                         bool &is_equal)
{
  int ret = OB_SUCCESS;
  is_equal = false;
  if (OB_ISNULL(l_subpartition) || OB_ISNULL(r_subpartition)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(l_subpartition), K(r_subpartition), K(ret));
  } else if (is_range_partition) {
    if (OB_FAIL(is_row_equal(l_subpartition->get_high_bound_val(),
                             r_subpartition->get_high_bound_val(),
                             is_equal))) {
      LOG_WARN("failed to check is row equal", K(ret));
    }
  } else if (OB_FAIL(is_list_partition_equal(l_subpartition, r_subpartition, is_equal))) {
    LOG_WARN("failed to check is list partition equal", K(ret));
  }
  return ret;
}

int ObPwjComparer::is_row_equal(const ObRowkey &first_row,
                                const ObRowkey &second_row,
                                bool &is_equal)
{
  int ret = OB_SUCCESS;
  is_equal = false;
  if (OB_ISNULL(first_row.get_obj_ptr()) || OB_ISNULL(second_row.get_obj_ptr())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(first_row.get_obj_ptr()),
        K(second_row.get_obj_ptr()), K(ret));
  } else if (first_row.get_obj_cnt() != second_row.get_obj_cnt()) {
    is_equal = false;
  } else {
    is_equal = true;
    for (int i = 0; OB_SUCC(ret) && is_equal && i < first_row.get_obj_cnt(); i++) {
      if (OB_FAIL(is_obj_equal(first_row.get_obj_ptr()[i],
                               second_row.get_obj_ptr()[i],
                               is_equal))) {
        LOG_WARN("failed to check obj equal", K(ret));
      } else { /*do nothing*/ }
    }
  }
  return ret;
}

int ObPwjComparer::is_list_partition_equal(const ObBasePartition *first_part,
                                           const ObBasePartition *second_part,
                                           bool &is_equal)
{
  int ret = OB_SUCCESS;
  is_equal = false;
  if (OB_ISNULL(first_part) || OB_ISNULL(second_part)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("null part expr", K(first_part), K(second_part), K(ret));
  } else if (first_part->get_list_row_values().count() != second_part->get_list_row_values().count()) {
    is_equal = false;
  } else {
    is_equal = true;
    const ObIArray<ObNewRow> &first_values = first_part->get_list_row_values();
    const ObIArray<ObNewRow> &second_values = second_part->get_list_row_values();
    for (int64_t i = 0; OB_SUCC(ret) && is_equal && i < first_values.count(); i++) {
      bool is_find = false;
      for (int64_t j = 0; OB_SUCC(ret) && !is_find && j < second_values.count(); j++) {
        if (OB_FAIL(is_row_equal(first_values.at(i), second_values.at(j), is_find))) {
          LOG_WARN("failed to check row equal", K(ret));
        } else { /*do nothing*/ }
      }
      if (OB_SUCC(ret) && !is_find) {
        is_equal = false;
      }
    }
  }
  return ret;
}

int ObPwjComparer::is_row_equal(const common::ObNewRow &first_row,
                                const common::ObNewRow &second_row,
                                bool &is_equal)
{
  int ret = OB_SUCCESS;
  is_equal = false;
  if (OB_ISNULL(first_row.cells_) || OB_ISNULL(second_row.cells_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("null cells", K(first_row.cells_), K(second_row.cells_), K(ret));
  } else if (first_row.count_ != second_row.count_) {
    is_equal = false;
  } else {
    is_equal = true;
    for (int64_t i = 0; OB_SUCC(ret) && is_equal && i < first_row.count_; i++) {
      if (OB_FAIL(is_obj_equal(first_row.cells_[i], second_row.cells_[i], is_equal))) {
        LOG_WARN("failed to check obj equal", K(ret));
      } else { /*do nothing*/ }
    }
  }
  return ret;
}

int ObPwjComparer::is_obj_equal(const common::ObObj &first_value,
                                const common::ObObj &second_value,
                                bool &is_equal)
{
  int ret = OB_SUCCESS;
  is_equal = false;
  if (first_value.is_null() || second_value.is_null()) {
    is_equal = false;
  } else if (first_value.is_min_value() || second_value.is_min_value()) {
    is_equal = (first_value.is_min_value() && second_value.is_min_value());
  } else if (first_value.is_max_value() || second_value.is_max_value()) {
    is_equal = (first_value.is_max_value() && second_value.is_max_value());
  } else if (OB_UNLIKELY(first_value.get_meta() != second_value.get_meta())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("first value and second value meta not same", K(ret),
        K(first_value.get_meta()), K(second_value.get_meta()));
  } else {
    is_equal = (first_value == second_value);
  }
  return ret;
}

ObStrictPwjComparer::ObStrictPwjComparer()
  : ObPwjComparer(true),
    part_tablet_id_map_(),
    part_index_map_(),
    subpart_tablet_id_map_(),
    phy_part_map_(),
    tablet_id_group_()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(phy_part_map_.create(DEFAULT_ID_ID_BUCKET_NUMBER, "SqlPwjCompare"))) {
    BACKTRACE(ERROR, true, "failed to create hash map");
  }
}

ObStrictPwjComparer::~ObStrictPwjComparer()
{
  phy_part_map_.destroy();
}

void ObStrictPwjComparer::reset()
{
  ObPwjComparer::reset();
  tablet_id_group_.reset();
}

int ObStrictPwjComparer::add_table(PwjTable &table, bool &is_match_pwj)
{
  int ret = OB_SUCCESS;
  is_match_pwj = true;
  if (OB_ISNULL(table.phy_table_loc_info_)) {
    // sharding to check partition wise join maybe reset
    is_match_pwj = false;
    LOG_TRACE("sharding to check partition wise is invalid", K(table));
  } else if (OB_FAIL(pwj_tables_.push_back(table))) {
    LOG_WARN("failed to push back pwj table", K(ret));
  } else if (pwj_tables_.count() <= 1) {
    TabletIdArray *part_array = NULL;
    // init first partition id group
    if (OB_ISNULL(part_array = tablet_id_group_.alloc_place_holder())){
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to alloc place holder", K(ret));
    } else if (OB_FAIL(part_array->assign(table.ordered_tablet_ids_))) {
      LOG_WARN("failed to assign ordered partition ids", K(ret));
    }
  } else if (OB_FAIL(check_logical_equal_and_calc_match_map(pwj_tables_.at(0),
                                                            pwj_tables_.at(pwj_tables_.count() - 1),
                                                            is_match_pwj))) {
    LOG_WARN("failed to calc match map", K(ret));
  } else if (!is_match_pwj) {
    // do nothing
  } else if (OB_FAIL(is_physically_equal_partitioned(pwj_tables_.at(0),
                                                     pwj_tables_.at(pwj_tables_.count() - 1),
                                                     is_match_pwj))) {
    LOG_WARN("failed to check is physically equal partitioned", K(ret));
  }
  return ret;
}

int ObStrictPwjComparer::is_physically_equal_partitioned(const PwjTable &l_table,
                                                         const PwjTable &r_table,
                                                         bool &is_physical_equal)
{
  int ret = OB_SUCCESS;
  is_physical_equal = true;
  const ObCandiTabletLocIArray &left_locations =
      l_table.phy_table_loc_info_->get_phy_part_loc_info_list();
  const ObCandiTabletLocIArray &right_locations =
      r_table.phy_table_loc_info_->get_phy_part_loc_info_list();
  TabletIdArray *r_array = NULL;
  if (l_table.ordered_tablet_ids_.count() != left_locations.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected partition count", K(ret),
                K(l_table.ordered_tablet_ids_.count()), K(left_locations.count()));
  } else if (left_locations.count() != right_locations.count()) {
    is_physical_equal = false;
  } else if (OB_ISNULL(r_array = tablet_id_group_.alloc_place_holder())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to alloc place holder", K(ret));
  } else if (OB_FAIL(r_array->prepare_allocate(left_locations.count()))) {
    LOG_WARN("failed to prepare allocate", K(ret));
  } else {
    TabletIdLocationMap l_tablet_id_map;
    TabletIdLocationMap r_tablet_id_map;
    const int64_t N = left_locations.count();
    if (OB_FAIL(l_tablet_id_map.create(std::max(N, MIN_ID_LOCATION_BUCKET_NUMBER),
                                     "SqlPwjCompare"))) {
      LOG_WARN("failed to create hash map", K(ret));
    } else if (OB_FAIL(r_tablet_id_map.create(std::max(N, MIN_ID_LOCATION_BUCKET_NUMBER),
                                            "SqlPwjCompare"))) {
      LOG_WARN("failed to create hash map", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < N; ++i) {
        const ObCandiTabletLoc &left_location = left_locations.at(i);
        const ObCandiTabletLoc &right_locaiton = right_locations.at(i);
        if (OB_FAIL(l_tablet_id_map.set_refactored(
                    left_location.get_partition_location().get_tablet_id().id(), &left_location)) ||
            OB_FAIL(r_tablet_id_map.set_refactored(
                    right_locaiton.get_partition_location().get_tablet_id().id(), &right_locaiton))) {
          LOG_WARN("failed to set refactored", K(ret));
        }
      }
      for (int64_t i = 0; OB_SUCC(ret) && is_physical_equal && i < N; ++i) {
        uint64_t l_tablet_id = l_table.ordered_tablet_ids_.at(i);
        uint64_t r_tablet_id = 0;
        share::ObLSReplicaLocation l_replica_loc;
        share::ObLSReplicaLocation r_replica_loc;
        const ObCandiTabletLoc *left_location = NULL;
        const ObCandiTabletLoc *right_location = NULL;
        if (OB_FAIL(phy_part_map_.get_refactored(l_tablet_id, r_tablet_id))) {
          LOG_WARN("failed to get refactored", K(ret));
        } else if (OB_FAIL(l_tablet_id_map.get_refactored(l_tablet_id, left_location)) ||
                  OB_FAIL(r_tablet_id_map.get_refactored(r_tablet_id, right_location))) {
          LOG_WARN("failed to get refactored", K(ret));
        } else if (OB_FAIL(left_location->get_selected_replica(l_replica_loc))) {
          LOG_WARN("failed to get selected replica", K(ret), K(left_location));
        } else if (OB_FAIL(right_location->get_selected_replica(r_replica_loc))) {
          LOG_WARN("failed to get selected replica", K(ret), K(right_locations.at(i)));
        } else if (!l_replica_loc.is_valid() || !r_replica_loc.is_valid()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("replica location is invalid", K(ret), K(i), K(l_replica_loc), K(r_replica_loc));
        } else {
          is_physical_equal = l_replica_loc.get_server() == r_replica_loc.get_server();
        }

        if (OB_SUCC(ret) && is_physical_equal) {
          r_array->at(i) = r_tablet_id;
        }
      }
      // destroy hash map anyway
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = l_tablet_id_map.destroy()) ||
          OB_SUCCESS != (tmp_ret = r_tablet_id_map.destroy())) {
        LOG_WARN("failed to destroy hash map", K(ret));
      }
    }
  }
  return ret;
}

int ObStrictPwjComparer::get_used_partition_indexes(const int64_t part_count,
                                                    const ObIArray<int64_t> &all_partition_indexes,
                                                    ObIArray<int64_t> &used_partition_indexes)
{
  int ret = OB_SUCCESS;
  bool part_idx[part_count];
  MEMSET(part_idx, 0, part_count);
  for (int64_t i = 0; i < all_partition_indexes.count(); ++i) {
    if (part_count <= all_partition_indexes.at(i)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected part count", K(part_count), K(all_partition_indexes));
    } else {
      part_idx[all_partition_indexes.at(i)] = true;
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < part_count; ++i) {
    if (part_idx[i]) {
      ret = used_partition_indexes.push_back(i);
    }
  }
  // for (int64_t i = 0; OB_SUCC(ret) && i < part_count; ++i) {
  //   if (ObOptimizerUtil::find_item(all_partition_indexes, i)) {
  //     if (OB_FAIL(used_partition_indexes.push_back(i))) {
  //       LOG_WARN("failed to push back ob partition", K(ret));
  //     }
  //   }
  // }
  return ret;
}

int ObStrictPwjComparer::check_logical_equal_and_calc_match_map(const PwjTable &l_table,
                                                                const PwjTable &r_table,
                                                                bool &is_match)
{
  int ret = OB_SUCCESS;
  is_match = true;
  part_tablet_id_map_.reuse();
  part_index_map_.reuse();
  subpart_tablet_id_map_.reuse();
  phy_part_map_.reuse();

  LOG_TRACE("start to check logical equal for l_table and r_table", K(l_table), K(r_table));

  if (l_table.part_level_ != r_table.part_level_ ||
      l_table.is_partition_single_ != r_table.is_partition_single_ ||
      l_table.is_subpartition_single_ != r_table.is_subpartition_single_) {
    is_match = false;
  } else if (l_table.is_partition_single_ && r_table.is_partition_single_) {
    std::pair<int64_t, int64_t> part_index_pair;
    std::pair<uint64_t, uint64_t> part_tablet_id_pair;
    part_index_pair.first = l_table.all_partition_indexes_.at(0);
    part_index_pair.second = r_table.all_partition_indexes_.at(0);
    if (OB_FAIL(get_part_tablet_id_by_part_index(l_table, part_index_pair.first, part_tablet_id_pair.first))) {
      LOG_WARN("failed to get part id by part index", K(ret));
    } else if (OB_FAIL(get_part_tablet_id_by_part_index(r_table, part_index_pair.second, part_tablet_id_pair.second))) {
      LOG_WARN("failed to get part id by part index", K(ret));
    } else if (OB_FAIL(part_tablet_id_map_.push_back(part_tablet_id_pair))) {
      LOG_WARN("failed to push back part id pair", K(ret));
    } else if (OB_FAIL(part_index_map_.push_back(part_index_pair))) {
      LOG_WARN("failed to push back part index pair", K(ret));
    }
  } else if (OB_FAIL(is_first_partition_logically_equal(l_table, r_table, is_match))) {
    LOG_WARN("failed to compare logical equal partition", K(ret));
  }

  if (OB_SUCC(ret) && is_match) {
    if (PARTITION_LEVEL_TWO == l_table.part_level_) {
      if (l_table.is_subpartition_single_ && r_table.is_subpartition_single_) {
        uint64_t l_subpart_tablet_id = 0;
        uint64_t r_subpart_tablet_id = 0;
        for (int64_t i = 0; OB_SUCC(ret) && i < part_index_map_.count(); ++i) {
          if (OB_FAIL(get_sub_part_tablet_id(l_table, part_index_map_.at(i).first, l_subpart_tablet_id))) {
            LOG_WARN("failed to get sub part id", K(ret));
          } else if (OB_FAIL(get_sub_part_tablet_id(r_table, part_index_map_.at(i).second, r_subpart_tablet_id))) {
            LOG_WARN("failed to get sub part id", K(ret));
          } else if (OB_FAIL(phy_part_map_.set_refactored(l_subpart_tablet_id,
                                                          r_subpart_tablet_id))) {
            LOG_WARN("failed to set refactored", K(ret));
          }
        }
      } else if (OB_FAIL(is_sub_partition_logically_equal(l_table, r_table, is_match))) {
        LOG_WARN("failed to compare logical equal subpartition", K(ret));
      }
    // 一级分区表partition_id(物理分区id) = part_id(一级逻辑分区id)
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < part_tablet_id_map_.count(); ++i) {
        if (OB_FAIL(phy_part_map_.set_refactored(part_tablet_id_map_.at(i).first, part_tablet_id_map_.at(i).second))) {
          LOG_WARN("failed to set refactored", K(ret));
        }
      }
    }
  }
  return ret;
}

bool ObStrictPwjComparer::is_same_part_type(const ObPartitionFuncType part_type1,
                                            const ObPartitionFuncType part_type2)
{
  bool ret = false;
  if (part_type1 != part_type2)
  {
    if (PARTITION_FUNC_TYPE_RANGE_COLUMNS == part_type1
        && is_interval_part(part_type2)) {
      ret = true;
    } else if (PARTITION_FUNC_TYPE_RANGE_COLUMNS == part_type2
               && is_interval_part(part_type1)) {
      ret = true;
    }
  } else {
    ret = true;
  }

  return ret;
}

int ObStrictPwjComparer::is_first_partition_logically_equal(const PwjTable &l_table,
                                                            const PwjTable &r_table,
                                                            bool &is_equal)
{
  int ret = OB_SUCCESS;
  is_equal = true;
  ObSEArray<int64_t, 128> l_used_partition_indexes;
  ObSEArray<int64_t, 128> r_used_partition_indexes;
  if (!is_same_part_type(l_table.part_type_, r_table.part_type_)) {
    is_equal = false;
  } else if (OB_FAIL(get_used_partition_indexes(l_table.part_number_,
                                                l_table.all_partition_indexes_,
                                                l_used_partition_indexes))) {
    LOG_WARN("failed to get used partition indexes", K(ret));
  } else if (OB_FAIL(get_used_partition_indexes(r_table.part_number_,
                                                r_table.all_partition_indexes_,
                                                r_used_partition_indexes))) {
    LOG_WARN("failed to get used partition indexes", K(ret));
  } else if (l_used_partition_indexes.count() != r_used_partition_indexes.count()) {
    is_equal = false;
  } else if (is_hash_like_part(l_table.part_type_)) {
    // hash/key分区, 要求分区数量必须一致, 且左表每一个part_index都有一个相等的右表part_index
    if (OB_FAIL(check_hash_partition_equal(l_table,
                                           r_table,
                                           l_used_partition_indexes,
                                           r_used_partition_indexes,
                                           part_tablet_id_map_,
                                           part_index_map_,
                                           is_equal))) {
      LOG_WARN("failed to check hash partition equal", K(ret));
    }
  } else if (is_range_part(l_table.part_type_)) {
    // range分区, 要求对应的分区上界一致
    if (OB_FAIL(check_range_partition_equal(l_table.partition_array_,
                                            r_table.partition_array_,
                                            l_used_partition_indexes,
                                            r_used_partition_indexes,
                                            part_tablet_id_map_,
                                            part_index_map_,
                                            is_equal))) {
      LOG_WARN("failed to get range partition match map", K(ret));
    }
  } else if (is_list_part(l_table.part_type_)) {
    // list分区, 要求左侧的每一个分区能在右侧找到对应的分区
    if (OB_FAIL(check_list_partition_equal(l_table.partition_array_,
                                           r_table.partition_array_,
                                           l_used_partition_indexes,
                                           r_used_partition_indexes,
                                           part_tablet_id_map_,
                                           part_index_map_,
                                           is_equal))) {
      LOG_WARN("failed to get list partition match map", K(ret));
    }
  } else {
    is_equal = false;
  }
  LOG_TRACE("succeed to check is first partition logical equal",
              K(is_equal), K(part_tablet_id_map_), K(part_index_map_));
  return ret;
}

int ObStrictPwjComparer::is_sub_partition_logically_equal(const PwjTable &l_table,
                                                          const PwjTable &r_table,
                                                          bool &is_equal)
{
  int ret = OB_SUCCESS;
  is_equal = true;
  ObSEArray<int64_t, 128> l_used_partition_indexes;
  ObSEArray<int64_t, 128> r_used_partition_indexes;
  if (l_table.subpart_type_ != r_table.subpart_type_) {
    is_equal = false;
  } else {
    ObPartition *l_part = NULL;
    ObPartition *r_part = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && is_equal && i < part_tablet_id_map_.count(); ++i) {
      int64_t l_part_index = part_index_map_.at(i).first;
      int64_t r_part_index = part_index_map_.at(i).second;
      l_used_partition_indexes.reuse();
      r_used_partition_indexes.reuse();
      subpart_tablet_id_map_.reuse();
      if (OB_ISNULL(l_part = l_table.partition_array_[part_index_map_.at(i).first]) ||
          OB_ISNULL(r_part = r_table.partition_array_[part_index_map_.at(i).second])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (OB_FAIL(get_subpartition_indexes_by_part_index(l_table, l_part_index,
                                                                l_used_partition_indexes))) {
        LOG_WARN("failed to get subpartition indexes by part index", K(ret));
      } else if (OB_FAIL(get_subpartition_indexes_by_part_index(r_table, r_part_index,
                                                                r_used_partition_indexes))) {
        LOG_WARN("failed to get subpartition indexes by part index", K(ret));
      } else if (l_used_partition_indexes.count() != r_used_partition_indexes.count()) {
        is_equal = false;
      } else if (is_hash_like_part(l_table.subpart_type_)) {
        // hash/key分区, 要求分区数量必须一致, 且左表每一个part_index都有一个相等的右表part_index
        if (l_part->get_sub_part_num() != r_part->get_sub_part_num()) {
          is_equal = false;
        } else if (OB_FAIL(check_hash_subpartition_equal(l_part->get_subpart_array(),
                                                         r_part->get_subpart_array(),
                                                         l_used_partition_indexes,
                                                         r_used_partition_indexes,
                                                         subpart_tablet_id_map_,
                                                         is_equal))) {
          LOG_WARN("failed to get hash subpartition match map", K(ret));
        }
      } else if (is_range_part(l_table.subpart_type_)) {
        // range分区, 要求使用到的分区数一致, 且对应的分区上界一致
        if (OB_FAIL(check_range_subpartition_equal(l_part->get_subpart_array(),
                                                   r_part->get_subpart_array(),
                                                   l_used_partition_indexes,
                                                   r_used_partition_indexes,
                                                   subpart_tablet_id_map_,
                                                   is_equal))) {
          LOG_WARN("failed to get range subpartition match map", K(ret));
        }
      } else if (is_list_part(l_table.subpart_type_)) {
        // list分区, 要求使用到的分区数一致, 且左侧的每一个分区能在右侧找到对应的分区
        if (OB_FAIL(check_list_subpartition_equal(l_part->get_subpart_array(),
                                                  r_part->get_subpart_array(),
                                                  l_used_partition_indexes,
                                                  r_used_partition_indexes,
                                                  subpart_tablet_id_map_,
                                                  is_equal))) {
          LOG_WARN("failed to get list subpartition match map", K(ret));
        }
      } else {
        is_equal = false;
      }
      if (OB_SUCC(ret) && is_equal) {
        for (int64_t j = 0; OB_SUCC(ret) && j < subpart_tablet_id_map_.count(); ++j) {
          if (OB_FAIL(phy_part_map_.set_refactored(subpart_tablet_id_map_.at(j).first,
                                                   subpart_tablet_id_map_.at(j).second))) {
            LOG_WARN("failed to set refactored", K(ret));
          }
        }
      }
    }
  }
  LOG_TRACE("succeed to check is sub partition logical equal", K(is_equal), K(l_table), K(r_table));
  return ret;
}

int ObStrictPwjComparer::get_subpartition_indexes_by_part_index(const PwjTable &table,
                                                                const int64_t part_index,
                                                                ObIArray<int64_t> &used_subpart_indexes)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < table.all_partition_indexes_.count(); ++i) {
    if (part_index != table.all_partition_indexes_.at(i)) {
      // do nothing
    } else if (OB_FAIL(used_subpart_indexes.push_back(table.all_subpartition_indexes_.at(i)))) {
      LOG_WARN("failed to push back subpartition indexes", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    lib::ob_sort(&used_subpart_indexes.at(0), &used_subpart_indexes.at(0) + used_subpart_indexes.count());
  }
  return ret;
}

int ObStrictPwjComparer::check_hash_partition_equal(const PwjTable &l_table,
                                                    const PwjTable &r_table,
                                                    const ObIArray<int64_t> &l_indexes,
                                                    const ObIArray<int64_t> &r_indexes,
                                                    ObIArray<std::pair<uint64_t,uint64_t> > &part_tablet_id_map,
                                                    ObIArray<std::pair<int64_t,int64_t> > &part_index_map,
                                                    bool &is_equal)
{
  int ret = OB_SUCCESS;
  is_equal = true;
  // hash分区还不支持分区管理，part_id一定等于part_index，不需要遍历part_array
  if (l_table.part_number_ != r_table.part_number_) {
    is_equal = false;
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && is_equal && i < l_indexes.count(); ++i) {
      std::pair<uint64_t, uint64_t> part_tablet_id_pair;
      std::pair<int64_t, int64_t> part_index_pair;
      if (l_indexes.at(i) == r_indexes.at(i)) {
        part_index_pair.first = l_indexes.at(i);
        part_index_pair.second = r_indexes.at(i);
        if (OB_ISNULL(l_table.partition_array_) ||
            OB_ISNULL(r_table.partition_array_) ||
            OB_ISNULL(l_table.partition_array_[l_indexes.at(i)]) ||
            OB_ISNULL(r_table.partition_array_[r_indexes.at(i)])) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null", K(ret), K(l_table.partition_array_),
                                          K(r_table.partition_array_));
        } else {
          part_tablet_id_pair.first = l_table.partition_array_[l_indexes.at(i)]->get_tablet_id().id();
          part_tablet_id_pair.second = r_table.partition_array_[r_indexes.at(i)]->get_tablet_id().id();
          if (OB_FAIL(part_tablet_id_map.push_back(part_tablet_id_pair))) {
            LOG_WARN("failed to push back part id pair", K(ret));
          } else if (OB_FAIL(part_index_map.push_back(part_index_pair))) {
            LOG_WARN("failed to push back part index pair", K(ret));
          } else { /* do nothing*/ }
        }
      } else {
        is_equal = false;
      }
    }
  }
  return ret;
}

int ObStrictPwjComparer::check_hash_subpartition_equal(ObSubPartition **l_subpartition_array,
                                                       ObSubPartition **r_subpartition_array,
                                                       const ObIArray<int64_t> &l_indexes,
                                                       const ObIArray<int64_t> &r_indexes,
                                                       ObIArray<std::pair<uint64_t,uint64_t> > &subpart_tablet_id_map,
                                                       bool &is_equal)
{
  int ret = OB_SUCCESS;
  is_equal = true;
  // hash分区还不支持分区管理，part_id一定等于part_index，不需要遍历part_array
  UNUSED(l_subpartition_array);
  UNUSED(r_subpartition_array);
  for (int64_t i = 0; OB_SUCC(ret) && is_equal && i < l_indexes.count(); ++i) {
    std::pair<uint64_t, uint64_t> subpart_tablet_id_pair;
    if (l_indexes.at(i) == r_indexes.at(i)) {
      if (OB_ISNULL(l_subpartition_array) || OB_ISNULL(r_subpartition_array) ||
          OB_ISNULL(l_subpartition_array[l_indexes.at(i)]) ||
          OB_ISNULL(r_subpartition_array[r_indexes.at(i)])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected error", K(ret), K(l_subpartition_array), K(r_subpartition_array));
      } else {
        subpart_tablet_id_pair.first = l_subpartition_array[l_indexes.at(i)]->get_tablet_id().id();
        subpart_tablet_id_pair.second = r_subpartition_array[r_indexes.at(i)]->get_tablet_id().id();
        if (OB_FAIL(subpart_tablet_id_map.push_back(subpart_tablet_id_pair))) {
          LOG_WARN("failed to push back part id pair", K(ret));
        }
      }
    } else {
      is_equal = false;
    }
  }
  return ret;
}

int ObStrictPwjComparer::check_range_partition_equal(ObPartition **l_partition_array,
                                                     ObPartition **r_partition_array,
                                                     const ObIArray<int64_t> &l_indexes,
                                                     const ObIArray<int64_t> &r_indexes,
                                                     ObIArray<std::pair<uint64_t,uint64_t> > &part_tablet_id_map,
                                                     ObIArray<std::pair<int64_t,int64_t> > &part_index_map,
                                                     bool &is_equal)
{
  int ret = OB_SUCCESS;
  is_equal = true;
  const ObPartition *l_partition = NULL;
  const ObPartition *r_partition = NULL;
  if (OB_ISNULL(l_partition_array) || OB_ISNULL(r_partition_array)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(l_partition_array), K(r_partition_array));
  }
  for (int64_t i = 0; OB_SUCC(ret) && is_equal && i < l_indexes.count(); ++i) {
    std::pair<uint64_t, uint64_t> part_tablet_id_pair;
    std::pair<int64_t, int64_t> part_index_pair;
    if (OB_ISNULL(l_partition = l_partition_array[l_indexes.at(i)]) ||
        OB_ISNULL(r_partition = r_partition_array[r_indexes.at(i)])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret), K(l_partition), K(r_partition), K(i));
    } else if (OB_FAIL(is_partition_equal(l_partition, r_partition, true, is_equal))) {
      LOG_WARN("failed to check range partition equal", K(ret));
    } else if (is_equal) {
      part_index_pair.first = l_indexes.at(i);
      part_index_pair.second = r_indexes.at(i);
      part_tablet_id_pair.first = l_partition->get_tablet_id().id();
      part_tablet_id_pair.second = r_partition->get_tablet_id().id();
      if (OB_FAIL(part_tablet_id_map.push_back(part_tablet_id_pair))) {
        LOG_WARN("failed to push back part id pair", K(ret));
      } else if (OB_FAIL(part_index_map.push_back(part_index_pair))) {
        LOG_WARN("failed to push back part index pair", K(ret));
      } else { /* do nothing*/ }
    }
  }
  return ret;
}

int ObStrictPwjComparer::check_range_subpartition_equal(ObSubPartition **l_subpartition_array,
                                                        ObSubPartition **r_subpartition_array,
                                                        const ObIArray<int64_t> &l_indexes,
                                                        const ObIArray<int64_t> &r_indexes,
                                                        ObIArray<std::pair<uint64_t,uint64_t> > &subpart_tablet_id_map,
                                                        bool &is_equal)
{
  int ret = OB_SUCCESS;
  is_equal = true;
  const ObSubPartition *l_partition = NULL;
  const ObSubPartition *r_partition = NULL;
  if (OB_ISNULL(l_subpartition_array) || OB_ISNULL(r_subpartition_array)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(l_subpartition_array), K(r_subpartition_array));
  }
  for (int64_t i = 0; OB_SUCC(ret) && is_equal && i < l_indexes.count(); ++i) {
    std::pair<uint64_t, uint64_t> subpart_tablet_id_pair;
    if (OB_ISNULL(l_partition = l_subpartition_array[l_indexes.at(i)]) ||
        OB_ISNULL(r_partition = r_subpartition_array[r_indexes.at(i)])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (OB_FAIL(is_subpartition_equal(l_partition, r_partition, true, is_equal))) {
      LOG_WARN("failed to check range subpartition equal", K(ret));
    } else if (is_equal) {
      subpart_tablet_id_pair.first = l_partition->get_tablet_id().id();
      subpart_tablet_id_pair.second = r_partition->get_tablet_id().id();
      if (OB_FAIL(subpart_tablet_id_map.push_back(subpart_tablet_id_pair))) {
        LOG_WARN("failed to push back part id pair", K(ret));
      }
    }
  }
  return ret;
}

int ObStrictPwjComparer::check_list_partition_equal(ObPartition **l_partition_array,
                                                    ObPartition **r_partition_array,
                                                    const ObIArray<int64_t> &l_indexes,
                                                    const ObIArray<int64_t> &r_indexes,
                                                    ObIArray<std::pair<uint64_t,uint64_t> > &part_tablet_id_map,
                                                    ObIArray<std::pair<int64_t,int64_t> > &part_index_map,
                                                    bool &is_equal)
{
  int ret = OB_SUCCESS;
  is_equal = true;
  const ObPartition *l_partition = NULL;
  const ObPartition *r_partition = NULL;
  ObSqlBitSet<> matched_partitions;
  if (OB_ISNULL(l_partition_array) || OB_ISNULL(r_partition_array)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(l_partition_array), K(r_partition_array));
  }
  for (int64_t i = 0; OB_SUCC(ret) && is_equal && i < l_indexes.count(); ++i) {
    bool find = false;
    bool is_part_equal = false;
    std::pair<uint64_t, uint64_t> part_tablet_id_pair;
    std::pair<int64_t, int64_t> part_index_pair;
    if (OB_ISNULL(l_partition = l_partition_array[l_indexes.at(i)])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    }
    for (int64_t j = 0; OB_SUCC(ret) && !find && j < r_indexes.count(); ++j) {
      if (matched_partitions.has_member(j)) {
        // do nothing
      } else if (OB_ISNULL(r_partition = r_partition_array[r_indexes.at(j)])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (OB_FAIL(is_partition_equal(l_partition, r_partition, false, is_part_equal))) {
        LOG_WARN("failed to check list partition equal", K(ret));
      } else if (is_part_equal) {
        find = true;
        part_index_pair.first = l_indexes.at(i);
        part_index_pair.second = r_indexes.at(j);
        part_tablet_id_pair.first = l_partition->get_tablet_id().id();
        part_tablet_id_pair.second = r_partition->get_tablet_id().id();
        if (OB_FAIL(matched_partitions.add_member(j))) {
          LOG_WARN("failed to add member", K(ret));
        } else if (OB_FAIL(part_tablet_id_map.push_back(part_tablet_id_pair))) {
          LOG_WARN("failed to push back part id pair", K(ret));
        } else if (OB_FAIL(part_index_map.push_back(part_index_pair))) {
          LOG_WARN("failed to push back part index pair", K(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (!find) {
        is_equal = false;
      }
    }
  }
  return ret;
}

int ObStrictPwjComparer::check_list_subpartition_equal(ObSubPartition **l_subpartition_array,
                                                       ObSubPartition **r_subpartition_array,
                                                       const ObIArray<int64_t> &l_indexes,
                                                       const ObIArray<int64_t> &r_indexes,
                                                       ObIArray<std::pair<uint64_t,uint64_t> > &subpart_tablet_id_map,
                                                       bool &is_equal)
{
  int ret = OB_SUCCESS;
  is_equal = true;
  const ObSubPartition *l_partition = NULL;
  const ObSubPartition *r_partition = NULL;
  ObSqlBitSet<> matched_partitions;
  if (OB_ISNULL(l_subpartition_array) || OB_ISNULL(l_subpartition_array)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(l_subpartition_array), K(r_subpartition_array));
  }
  for (int64_t i = 0; OB_SUCC(ret) && is_equal && i < l_indexes.count(); ++i) {
    bool find = false;
    bool is_subpart_equal = false;
    std::pair<uint64_t, uint64_t> subpart_tablet_id_pair;
    if (OB_ISNULL(l_partition = l_subpartition_array[l_indexes.at(i)])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    }
    for (int64_t j = 0; OB_SUCC(ret) && !find && j < r_indexes.count(); ++j) {
      if (matched_partitions.has_member(j)) {
        // do nothing
      } else if (OB_ISNULL(r_partition = r_subpartition_array[r_indexes.at(j)])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (OB_FAIL(is_subpartition_equal(l_partition, r_partition, false, is_subpart_equal))) {
        LOG_WARN("failed to check list partition equal", K(ret));
      } else if (is_subpart_equal) {
        find = true;
        subpart_tablet_id_pair.first = l_partition->get_tablet_id().id();
        subpart_tablet_id_pair.second = r_partition->get_tablet_id().id();
        if (OB_FAIL(matched_partitions.add_member(j))) {
          LOG_WARN("failed to add member", K(ret));
        } else if (OB_FAIL(subpart_tablet_id_map.push_back(subpart_tablet_id_pair))) {
          LOG_WARN("failed to push back part id pair", K(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (!find) {
        is_equal = false;
      }
    }
  }
  return ret;
}

int ObStrictPwjComparer::get_part_tablet_id_by_part_index(const PwjTable &table,
                                                          const int64_t part_index,
                                                          uint64_t &tablet_id)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(table.partition_array_) ||
      OB_ISNULL(table.partition_array_[part_index])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(table.partition_array_), K(part_index));
  } else {
    tablet_id = table.partition_array_[part_index]->get_tablet_id().id();
  }
  return ret;
}

int ObStrictPwjComparer::get_sub_part_tablet_id(const PwjTable &table,
                                                const int64_t &part_index,
                                                uint64_t &sub_part_tablet_id)
{
  int ret = OB_SUCCESS;
  ObPartition *part = NULL;
  if (OB_ISNULL(table.partition_array_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(table.partition_array_));
  } else {
    bool find = false;
    for (int64_t i = 0; OB_SUCC(ret) && !find && i < table.all_partition_indexes_.count(); ++i) {
      if (part_index == table.all_partition_indexes_.at(i)) {
        int64_t subpart_index = table.all_subpartition_indexes_.at(i);
        if (OB_ISNULL(part = table.partition_array_[part_index]) ||
            OB_ISNULL(part->get_subpart_array()) ||
            OB_ISNULL(part->get_subpart_array()[subpart_index])) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null", K(ret));
        } else {
          sub_part_tablet_id = part->get_subpart_array()[subpart_index]->get_tablet_id().id();
          find = true;
        }
      }
    }
    if (OB_SUCC(ret) && !find) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to find part_index in all_partition_indexes", K(ret),
                  K(part_index), K(table.all_partition_indexes_));
    }
  }
  return ret;
}

int ObNonStrictPwjComparer::add_table(PwjTable &table, bool &is_match_nonstrict_pw)
{
  int ret = OB_SUCCESS;
  is_match_nonstrict_pw = true;
  if (table.server_list_.empty()) {
    is_match_nonstrict_pw = false;
    LOG_DEBUG("invalid non strict pwj input", K(table));
  } else if (OB_FAIL(pwj_tables_.push_back(table))) {
    LOG_WARN("failed to push back pwj table", K(ret));
  } else if (pwj_tables_.count() <= 1) {
    is_match_nonstrict_pw = true;
  } else if (OB_FAIL(is_match_non_strict_partition_wise(pwj_tables_.at(0),
                                                        pwj_tables_.at(pwj_tables_.count() - 1),
                                                        is_match_nonstrict_pw))) {
    LOG_WARN("failed to check non strict pw", K(ret));
  }
  return ret;
}

int ObNonStrictPwjComparer::is_match_non_strict_partition_wise(PwjTable &l_table,
                                                               PwjTable &r_table,
                                                               bool &is_match_nonstrict_pw)
{
  int ret = OB_SUCCESS;
  is_match_nonstrict_pw = false;
  if (OB_FAIL(ObShardingInfo::is_physically_equal_serverlist(l_table.server_list_,
                                                             r_table.server_list_,
                                                             is_match_nonstrict_pw))) {
    LOG_WARN("failed to check physically equal server list", K(ret));
  } else if (is_match_nonstrict_pw) {
    // do nothing
  } else if (OB_FAIL(ObShardingInfo::is_physically_both_shuffled_serverlist(l_table.server_list_,
                                                                            r_table.server_list_,
                                                                            is_match_nonstrict_pw))) {
    LOG_WARN("failed to check both shuffled server list", K(ret));
  } else {
    // do nothing
  }
  return ret;
}