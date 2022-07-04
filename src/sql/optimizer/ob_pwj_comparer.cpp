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

int PwjTable::init(const ObShardingInfo& sharding)
{
  int ret = OB_SUCCESS;
  ref_table_id_ = OB_INVALID_ID;
  phy_table_loc_info_ = sharding.get_phy_table_location_info();
  part_level_ = sharding.get_part_level();
  part_type_ = sharding.get_part_func_type();
  subpart_type_ = sharding.get_sub_part_func_type();
  partition_array_ = sharding.get_partition_array();
  def_subpartition_array_ = sharding.get_subpartition_array();
  is_sub_part_template_ = sharding.get_is_sub_part_template();
  part_number_ = sharding.get_part_number();
  def_subpart_number_ = sharding.get_sub_part_number();
  is_partition_single_ = sharding.is_partition_single();
  is_subpartition_single_ = sharding.is_subpartition_single();
  if (OB_FAIL(ordered_partition_ids_.assign(sharding.get_all_partition_ids()))) {
    LOG_WARN("failed to assign partition ids", K(ret));
  } else if (OB_FAIL(all_partition_indexes_.assign(sharding.get_all_partition_indexes()))) {
    LOG_WARN("failed to assign all partition indexes", K(ret));
  } else if (OB_FAIL(all_subpartition_indexes_.assign(sharding.get_all_subpartition_indexes()))) {
    LOG_WARN("failed to assign all subpartition indexes", K(ret));
  }
  return ret;
}

int PwjTable::init(const ObTableSchema& table_schema, const ObPhyTableLocationInfo& phy_tbl_info)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObPwjComparer::extract_all_partition_indexes(phy_tbl_info,
          table_schema,
          ordered_partition_ids_,
          all_partition_indexes_,
          all_subpartition_indexes_,
          is_partition_single_,
          is_subpartition_single_))) {
    LOG_WARN("failed to extract used partition indexes", K(ret));
  } else {
    ref_table_id_ = phy_tbl_info.get_ref_table_id();
    phy_table_loc_info_ = &phy_tbl_info;
    part_level_ = table_schema.get_part_level();
    part_type_ = table_schema.get_part_option().get_part_func_type();
    partition_array_ = table_schema.get_part_array();
    part_number_ = table_schema.get_partition_num();
    if (share::schema::PARTITION_LEVEL_TWO == part_level_) {
      is_sub_part_template_ = table_schema.is_sub_part_template();
      subpart_type_ = table_schema.get_sub_part_option().get_part_func_type();
      if (is_sub_part_template_) {
        def_subpartition_array_ = table_schema.get_def_subpart_array();
        def_subpart_number_ = table_schema.get_def_sub_part_num();
      }
    }
  }
  if (OB_SUCC(ret)
      && ((share::schema::PARTITION_LEVEL_TWO == part_level_) || (share::schema::PARTITION_LEVEL_ONE == part_level_))
      && 0 == part_number_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected part number", K(ref_table_id_), K(table_schema), K(phy_tbl_info));
  }
  return ret;
}

int PwjTable::assign(const PwjTable& other)
{
  int ret = OB_SUCCESS;
  ref_table_id_ = other.ref_table_id_;
  phy_table_loc_info_ = other.phy_table_loc_info_;
  part_level_ = other.part_level_;
  part_type_ = other.part_type_;
  subpart_type_ = other.subpart_type_;
  partition_array_ = other.partition_array_;
  def_subpartition_array_ = other.def_subpartition_array_;
  is_sub_part_template_ = other.is_sub_part_template_;
  part_number_ = other.part_number_;
  def_subpart_number_ = other.def_subpart_number_;
  is_partition_single_ = other.is_partition_single_;
  is_subpartition_single_ = other.is_subpartition_single_;
  if (OB_FAIL(ordered_partition_ids_.assign(other.ordered_partition_ids_))) {
    LOG_WARN("failed to assign ordered partition indexes", K(ret));
  } else if (OB_FAIL(all_partition_indexes_.assign(other.all_partition_indexes_))) {
    LOG_WARN("failed to assign used partition indexes", K(ret));
  } else if (OB_FAIL(all_subpartition_indexes_.assign(other.all_subpartition_indexes_))) {
    LOG_WARN("failed to assign used subpartition indexes", K(ret));
  }
  return ret;
}

void ObPwjComparer::reset()
{
  pwj_tables_.reset();
  partition_id_group_.reset();
}

int ObPwjComparer::add_table(PwjTable& table, bool& is_match_pwj)
{
  int ret = OB_SUCCESS;
  is_match_pwj = true;
  if (OB_ISNULL(table.phy_table_loc_info_)) {
    // sharding to check partition wise join maybe reset
    is_match_pwj = false;
    LOG_TRACE("sharding to check partition wise join has not been initialized/reset", K(table));
  } else if (OB_FAIL(pwj_tables_.push_back(table))) {
    LOG_WARN("failed to push back pwj table", K(ret));
  } else if (pwj_tables_.count() <= 1) {
    is_match_pwj = true;
    PartitionIdArray* part_array = NULL;
    // init first partition id group
    if (OB_ISNULL(part_array = partition_id_group_.alloc_place_holder())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to alloc place holder", K(ret));
    } else if (OB_FAIL(part_array->assign(table.ordered_partition_ids_))) {
      LOG_WARN("failed to assign ordered partition ids", K(ret));
    }
  } else if (is_strict_ && OB_FAIL(check_logical_equal_and_calc_match_map(
                               pwj_tables_.at(0), pwj_tables_.at(pwj_tables_.count() - 1), is_match_pwj))) {
    LOG_WARN("failed to calc match map", K(ret));
  } else if (!is_match_pwj) {
    // do nothing
  } else if (OB_FAIL(check_if_match_partition_wise(
                 pwj_tables_.at(0), pwj_tables_.at(pwj_tables_.count() - 1), is_match_pwj))) {
    LOG_WARN("failed to check if match partition wise", K(ret));
  }
  return ret;
}

int ObPwjComparer::extract_all_partition_indexes(const ObPhyTableLocationInfo& phy_table_location_info,
    const ObTableSchema& table_schema, ObIArray<int64_t>& all_partition_ids, ObIArray<int64_t>& all_partition_indexes,
    ObIArray<int64_t>& all_subpartition_indexes, bool& is_partition_single, bool& is_subpartition_single)
{
  int ret = OB_SUCCESS;
  const ObPhyPartitionLocationInfoIArray& phy_partitions = phy_table_location_info.get_phy_part_loc_info_list();
  ObPartitionLevel part_level = table_schema.get_part_level();
  is_partition_single = false;
  is_subpartition_single = false;
  if (share::schema::PARTITION_LEVEL_ZERO == part_level) {
    // do nothing
  } else if (share::schema::PARTITION_LEVEL_ONE == part_level) {
    for (int64_t i = 0; OB_SUCC(ret) && i < phy_partitions.count(); ++i) {
      int64_t part_id = phy_partitions.at(i).get_partition_location().get_partition_id();
      int64_t part_index = -1;
      if (OB_FAIL(table_schema.get_partition_index_by_id(part_id,
              false,  // check_dropped_partition
              part_index))) {
        LOG_WARN("failed to get partition idx", K(ret));
      } else if (OB_FAIL(all_partition_ids.push_back(part_id))) {
        LOG_WARN("failed to push back partition id", K(ret));
      } else if (OB_FAIL(all_partition_indexes.push_back(part_index))) {
        LOG_WARN("failed to push back partition index", K(ret));
      }
    }
  } else if (share::schema::PARTITION_LEVEL_TWO == part_level) {
    for (int64_t i = 0; OB_SUCC(ret) && i < phy_partitions.count(); ++i) {
      int64_t partition_id = phy_partitions.at(i).get_partition_location().get_partition_id();
      int64_t part_id = extract_part_idx(partition_id);
      int64_t subpart_id = extract_subpart_idx(partition_id);
      int64_t part_index = -1;
      if (OB_FAIL(table_schema.get_partition_index_by_id(part_id,
              false,  // check_dropped_partition
              part_index))) {
        LOG_WARN("failed to get partition idx", K(ret));
      } else if (OB_FAIL(all_partition_ids.push_back(partition_id))) {
        LOG_WARN("failed to push back partition id", K(ret));
      } else if (OB_FAIL(all_partition_indexes.push_back(part_index))) {
        LOG_WARN("failed to push back part index", K(ret));
      } else if (OB_FAIL(all_subpartition_indexes.push_back(subpart_id))) {
        LOG_WARN("failed to push back subpart index");
      }
    }
  }
  if (OB_SUCC(ret) && share::schema::PARTITION_LEVEL_TWO == part_level) {
    if (all_partition_indexes.count() > 0) {
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
      K(phy_partitions),
      K(all_partition_ids),
      K(all_partition_indexes),
      K(all_subpartition_indexes),
      K(is_partition_single),
      K(is_subpartition_single));
  return ret;
}

int ObPwjComparer::get_used_partition_indexes(
    const int64_t part_count, const ObIArray<int64_t>& all_partition_indexes, ObIArray<int64_t>& used_partition_indexes)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < part_count; ++i) {
    if (ObOptimizerUtil::find_item(all_partition_indexes, i)) {
      if (OB_FAIL(used_partition_indexes.push_back(i))) {
        LOG_WARN("failed to push back ob partition", K(ret));
      }
    }
  }
  return ret;
}

int ObPwjComparer::check_logical_equal_and_calc_match_map(
    const PwjTable& l_table, const PwjTable& r_table, bool& is_match)
{
  int ret = OB_SUCCESS;
  is_match = true;
  part_id_map_.reuse();
  part_index_map_.reuse();
  subpart_id_map_.reuse();
  phy_part_map_.reuse();

  LOG_TRACE("start to check logical equal for l_table and r_table", K(l_table), K(r_table));

  if (l_table.is_partition_single_ && r_table.is_partition_single_) {
    std::pair<int64_t, int64_t> part_index_pair;
    std::pair<int64_t, int64_t> part_id_pair;
    part_index_pair.first = l_table.all_partition_indexes_.at(0);
    part_index_pair.second = r_table.all_partition_indexes_.at(0);
    if (OB_FAIL(get_part_id_by_part_index(l_table, part_index_pair.first, part_id_pair.first))) {
      LOG_WARN("failed to get part id by part index", K(ret));
    } else if (OB_FAIL(get_part_id_by_part_index(r_table, part_index_pair.second, part_id_pair.second))) {
      LOG_WARN("failed to get part id by part index", K(ret));
    } else if (OB_FAIL(part_id_map_.push_back(part_id_pair))) {
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
        int64_t l_subpart_id = -1;
        int64_t r_subpart_id = -1;
        for (int64_t i = 0; OB_SUCC(ret) && i < part_index_map_.count(); ++i) {
          if (OB_FAIL(get_sub_part_id(l_table, part_index_map_.at(i).first, l_subpart_id))) {
            LOG_WARN("failed to get sub part id", K(ret));
          } else if (OB_FAIL(get_sub_part_id(r_table, part_index_map_.at(i).second, r_subpart_id))) {
            LOG_WARN("failed to get sub part id", K(ret));
          } else {
            std::pair<int64_t, int64_t> phy_part_pair;
            phy_part_pair.first = generate_phy_part_id(part_id_map_.at(i).first, l_subpart_id);
            phy_part_pair.second = generate_phy_part_id(part_id_map_.at(i).second, r_subpart_id);
            if (OB_FAIL(phy_part_map_.push_back(phy_part_pair))) {
              LOG_WARN("failed to push back phy part pair", K(ret));
            }
          }
        }
      } else if (OB_FAIL(is_sub_partition_logically_equal(l_table, r_table, is_match))) {
        LOG_WARN("failed to compare logical equal subpartition", K(ret));
      }
    } else if (OB_FAIL(phy_part_map_.assign(part_id_map_))) {
      LOG_WARN("failed to assign part id map", K(ret));
    }
  }
  return ret;
}

int ObPwjComparer::is_first_partition_logically_equal(const PwjTable& l_table, const PwjTable& r_table, bool& is_equal)
{
  int ret = OB_SUCCESS;
  is_equal = true;
  ObSEArray<int64_t, 128> l_used_partition_indexes;
  ObSEArray<int64_t, 128> r_used_partition_indexes;
  if (l_table.part_type_ != r_table.part_type_) {
    is_equal = false;
  } else if (OB_FAIL(get_used_partition_indexes(
                 l_table.part_number_, l_table.all_partition_indexes_, l_used_partition_indexes))) {
    LOG_WARN("failed to get used partition indexes", K(ret));
  } else if (OB_FAIL(get_used_partition_indexes(
                 r_table.part_number_, r_table.all_partition_indexes_, r_used_partition_indexes))) {
    LOG_WARN("failed to get used partition indexes", K(ret));
  } else if (l_used_partition_indexes.count() != r_used_partition_indexes.count()) {
    is_equal = false;
  } else if (is_hash_like_part(l_table.part_type_)) {
    if (check_hash_partition_equal(l_table,
            r_table,
            l_used_partition_indexes,
            r_used_partition_indexes,
            part_id_map_,
            part_index_map_,
            is_equal)) {
      LOG_WARN("failed to check hash partition equal", K(ret));
    }
  } else if (is_range_part(l_table.part_type_)) {
    if (OB_FAIL(check_range_partition_equal(l_table.partition_array_,
            r_table.partition_array_,
            l_used_partition_indexes,
            r_used_partition_indexes,
            part_id_map_,
            part_index_map_,
            is_equal))) {
      LOG_WARN("failed to get range partition match map", K(ret));
    }
  } else if (is_list_part(l_table.part_type_)) {
    if (OB_FAIL(check_list_partition_equal(l_table.partition_array_,
            r_table.partition_array_,
            l_used_partition_indexes,
            r_used_partition_indexes,
            part_id_map_,
            part_index_map_,
            is_equal))) {
      LOG_WARN("failed to get list partition match map", K(ret));
    }
  } else {
    is_equal = false;
  }
  LOG_TRACE("succeed to check is first partition logical equal", K(is_equal), K(part_id_map_), K(part_index_map_));
  return ret;
}

int ObPwjComparer::is_sub_partition_logically_equal(const PwjTable& l_table, const PwjTable& r_table, bool& is_equal)
{
  int ret = OB_SUCCESS;
  is_equal = true;
  ObSEArray<int64_t, 128> l_used_partition_indexes;
  ObSEArray<int64_t, 128> r_used_partition_indexes;
  if (l_table.subpart_type_ != r_table.subpart_type_ ||
      l_table.is_sub_part_template_ != r_table.is_sub_part_template_) {
    is_equal = false;
  } else if (l_table.is_sub_part_template_) {
    if (OB_FAIL(get_used_partition_indexes(
            l_table.def_subpart_number_, l_table.all_subpartition_indexes_, l_used_partition_indexes))) {
      LOG_WARN("failed to get used partition indexes", K(ret));
    } else if (OB_FAIL(get_used_partition_indexes(
                   r_table.def_subpart_number_, r_table.all_subpartition_indexes_, r_used_partition_indexes))) {
      LOG_WARN("failed to get used partition indexes", K(ret));
    } else if (l_used_partition_indexes.count() != r_used_partition_indexes.count()) {
      is_equal = false;
    } else if (is_hash_like_part(l_table.subpart_type_)) {
      if (l_table.def_subpart_number_ != r_table.def_subpart_number_) {
        is_equal = false;
      } else if (check_hash_subpartition_equal(l_table.def_subpartition_array_,
                     r_table.def_subpartition_array_,
                     l_used_partition_indexes,
                     r_used_partition_indexes,
                     subpart_id_map_,
                     is_equal)) {
        LOG_WARN("failed to get hash subpartition match map", K(ret));
      }
    } else if (is_range_part(l_table.subpart_type_)) {
      if (OB_FAIL(check_range_subpartition_equal(l_table.def_subpartition_array_,
              r_table.def_subpartition_array_,
              l_used_partition_indexes,
              r_used_partition_indexes,
              subpart_id_map_,
              is_equal))) {
        LOG_WARN("failed to get range subpartition match map", K(ret));
      }
    } else if (is_list_part(l_table.subpart_type_)) {
      if (OB_FAIL(check_list_subpartition_equal(l_table.def_subpartition_array_,
              r_table.def_subpartition_array_,
              l_used_partition_indexes,
              r_used_partition_indexes,
              subpart_id_map_,
              is_equal))) {
        LOG_WARN("failed to get list subpartition match map", K(ret));
      }
    } else {
      is_equal = false;
    }
    if (OB_SUCC(ret) && is_equal) {
      for (int64_t i = 0; OB_SUCC(ret) && i < part_id_map_.count(); ++i) {
        for (int64_t j = 0; OB_SUCC(ret) && j < subpart_id_map_.count(); ++j) {
          std::pair<int64_t, int64_t> phy_part_pair;
          phy_part_pair.first = generate_phy_part_id(part_id_map_.at(i).first, subpart_id_map_.at(j).first);
          phy_part_pair.second = generate_phy_part_id(part_id_map_.at(i).second, subpart_id_map_.at(j).second);
          if (OB_FAIL(phy_part_map_.push_back(phy_part_pair))) {
            LOG_WARN("failed to push back phy part pair", K(ret));
          }
        }
      }
    }
  } else {
    ObPartition* l_part = NULL;
    ObPartition* r_part = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && is_equal && i < part_id_map_.count(); ++i) {
      int64_t l_part_index = part_index_map_.at(i).first;
      int64_t r_part_index = part_index_map_.at(i).second;
      l_used_partition_indexes.reuse();
      r_used_partition_indexes.reuse();
      subpart_id_map_.reuse();
      if (OB_ISNULL(l_part = l_table.partition_array_[part_index_map_.at(i).first]) ||
          OB_ISNULL(r_part = r_table.partition_array_[part_index_map_.at(i).second])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (OB_FAIL(get_subpartition_indexes_by_part_index(l_table, l_part_index, l_used_partition_indexes))) {
        LOG_WARN("faield to get subpartition indexes by part index", K(ret));
      } else if (OB_FAIL(get_subpartition_indexes_by_part_index(r_table, r_part_index, r_used_partition_indexes))) {
        LOG_WARN("faield to get subpartition indexes by part index", K(ret));
      } else if (l_used_partition_indexes.count() != r_used_partition_indexes.count()) {
        is_equal = false;
      } else if (is_hash_like_part(l_table.subpart_type_)) {
        if (l_part->get_sub_part_num() != r_part->get_sub_part_num()) {
          is_equal = false;
        } else if (check_hash_subpartition_equal(l_part->get_subpart_array(),
                       r_part->get_subpart_array(),
                       l_used_partition_indexes,
                       r_used_partition_indexes,
                       subpart_id_map_,
                       is_equal)) {
          LOG_WARN("failed to get hash subpartition match map", K(ret));
        }
      } else if (is_range_part(l_table.subpart_type_)) {
        if (OB_FAIL(check_range_subpartition_equal(l_part->get_subpart_array(),
                r_part->get_subpart_array(),
                l_used_partition_indexes,
                r_used_partition_indexes,
                subpart_id_map_,
                is_equal))) {
          LOG_WARN("failed to get range subpartition match map", K(ret));
        }
      } else if (is_list_part(l_table.subpart_type_)) {
        if (OB_FAIL(check_list_subpartition_equal(l_part->get_subpart_array(),
                r_part->get_subpart_array(),
                l_used_partition_indexes,
                r_used_partition_indexes,
                subpart_id_map_,
                is_equal))) {
          LOG_WARN("failed to get list subpartition match map", K(ret));
        }
      } else {
        is_equal = false;
      }
      if (OB_SUCC(ret) && is_equal) {
        for (int64_t j = 0; OB_SUCC(ret) && j < subpart_id_map_.count(); ++j) {
          std::pair<int64_t, int64_t> phy_part_pair;
          phy_part_pair.first = generate_phy_part_id(part_id_map_.at(i).first, subpart_id_map_.at(j).first);
          phy_part_pair.second = generate_phy_part_id(part_id_map_.at(i).second, subpart_id_map_.at(j).second);
          if (OB_FAIL(phy_part_map_.push_back(phy_part_pair))) {
            LOG_WARN("failed to push back phy part pair", K(ret));
          }
        }
      }
    }
  }
  LOG_TRACE("succeed to check is sub partition logical equal", K(is_equal), K(phy_part_map_), K(l_table), K(r_table));
  return ret;
}

int ObPwjComparer::get_subpartition_indexes_by_part_index(
    const PwjTable& table, const int64_t part_index, ObIArray<int64_t>& used_subpart_indexes)
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
    std::sort(&used_subpart_indexes.at(0), &used_subpart_indexes.at(0) + used_subpart_indexes.count());
  }
  return ret;
}

int ObPwjComparer::check_hash_partition_equal(const PwjTable& l_table, const PwjTable& r_table,
    const ObIArray<int64_t>& l_indexes, const ObIArray<int64_t>& r_indexes,
    ObIArray<std::pair<int64_t, int64_t> >& part_id_map, ObIArray<std::pair<int64_t, int64_t> >& part_index_map,
    bool& is_equal)
{
  int ret = OB_SUCCESS;
  is_equal = true;
  if (l_table.part_number_ != r_table.part_number_) {
    is_equal = false;
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && is_equal && i < l_indexes.count(); ++i) {
      std::pair<int64_t, int64_t> part_id_pair;
      std::pair<int64_t, int64_t> part_index_pair;
      if (l_indexes.at(i) == r_indexes.at(i)) {
        part_index_pair.first = l_indexes.at(i);
        part_index_pair.second = r_indexes.at(i);
        part_id_pair.first = l_indexes.at(i);
        part_id_pair.second = r_indexes.at(i);
        if (OB_FAIL(part_id_map.push_back(part_id_pair))) {
          LOG_WARN("failed to push back part id pair", K(ret));
        } else if (OB_FAIL(part_index_map.push_back(part_index_pair))) {
          LOG_WARN("failed to push back part index pair", K(ret));
        } else { /* do nothing*/
        }
      } else {
        is_equal = false;
      }
    }
  }
  return ret;
}

int ObPwjComparer::check_hash_subpartition_equal(ObSubPartition** l_subpartition_array,
    ObSubPartition** r_subpartition_array, const ObIArray<int64_t>& l_indexes, const ObIArray<int64_t>& r_indexes,
    ObIArray<std::pair<int64_t, int64_t> >& sub_part_id_map, bool& is_equal)
{
  int ret = OB_SUCCESS;
  is_equal = true;
  UNUSED(l_subpartition_array);
  UNUSED(r_subpartition_array);
  for (int64_t i = 0; OB_SUCC(ret) && is_equal && i < l_indexes.count(); ++i) {
    std::pair<int64_t, int64_t> sub_part_id_pair;
    if (l_indexes.at(i) == r_indexes.at(i)) {
      sub_part_id_pair.first = l_indexes.at(i);
      sub_part_id_pair.second = l_indexes.at(i);
      if (OB_FAIL(sub_part_id_map.push_back(sub_part_id_pair))) {
        LOG_WARN("failed to push back part id pair", K(ret));
      }
    } else {
      is_equal = false;
    }
  }
  return ret;
}

int ObPwjComparer::check_range_partition_equal(ObPartition** l_partition_array, ObPartition** r_partition_array,
    const ObIArray<int64_t>& l_indexes, const ObIArray<int64_t>& r_indexes,
    ObIArray<std::pair<int64_t, int64_t> >& part_id_map, ObIArray<std::pair<int64_t, int64_t> >& part_index_map,
    bool& is_equal)
{
  int ret = OB_SUCCESS;
  is_equal = true;
  const ObPartition* l_partition = NULL;
  const ObPartition* r_partition = NULL;
  if (OB_ISNULL(l_partition_array) || OB_ISNULL(r_partition_array)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(l_partition_array), K(r_partition_array));
  }
  for (int64_t i = 0; OB_SUCC(ret) && is_equal && i < l_indexes.count(); ++i) {
    std::pair<int64_t, int64_t> part_id_pair;
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
      part_id_pair.first = l_partition->get_part_id();
      part_id_pair.second = r_partition->get_part_id();
      if (OB_FAIL(part_id_map.push_back(part_id_pair))) {
        LOG_WARN("failed to push back part id pair", K(ret));
      } else if (OB_FAIL(part_index_map.push_back(part_index_pair))) {
        LOG_WARN("failed to push back part index pair", K(ret));
      } else { /* do nothing*/
      }
    }
  }
  return ret;
}

int ObPwjComparer::check_range_subpartition_equal(ObSubPartition** l_subpartition_array,
    ObSubPartition** r_subpartition_array, const ObIArray<int64_t>& l_indexes, const ObIArray<int64_t>& r_indexes,
    ObIArray<std::pair<int64_t, int64_t> >& sub_part_id_map, bool& is_equal)
{
  int ret = OB_SUCCESS;
  is_equal = true;
  const ObSubPartition* l_partition = NULL;
  const ObSubPartition* r_partition = NULL;
  if (OB_ISNULL(l_subpartition_array) || OB_ISNULL(r_subpartition_array)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(l_subpartition_array), K(r_subpartition_array));
  }
  for (int64_t i = 0; OB_SUCC(ret) && is_equal && i < l_indexes.count(); ++i) {
    std::pair<int64_t, int64_t> sub_part_id_pair;
    if (OB_ISNULL(l_partition = l_subpartition_array[l_indexes.at(i)]) ||
        OB_ISNULL(r_partition = r_subpartition_array[r_indexes.at(i)])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (OB_FAIL(is_subpartition_equal(l_partition, r_partition, true, is_equal))) {
      LOG_WARN("failed to check range subpartition equal", K(ret));
    } else if (is_equal) {
      sub_part_id_pair.first = l_partition->get_sub_part_id();
      sub_part_id_pair.second = r_partition->get_sub_part_id();
      if (OB_FAIL(sub_part_id_map.push_back(sub_part_id_pair))) {
        LOG_WARN("failed to push back part id pair", K(ret));
      }
    }
  }
  return ret;
}

int ObPwjComparer::check_list_partition_equal(ObPartition** l_partition_array, ObPartition** r_partition_array,
    const ObIArray<int64_t>& l_indexes, const ObIArray<int64_t>& r_indexes,
    ObIArray<std::pair<int64_t, int64_t> >& part_id_map, ObIArray<std::pair<int64_t, int64_t> >& part_index_map,
    bool& is_equal)
{
  int ret = OB_SUCCESS;
  is_equal = true;
  const ObPartition* l_partition = NULL;
  const ObPartition* r_partition = NULL;
  ObSqlBitSet<> matched_partitions;
  if (OB_ISNULL(l_partition_array) || OB_ISNULL(r_partition_array)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(l_partition_array), K(r_partition_array));
  }
  for (int64_t i = 0; OB_SUCC(ret) && is_equal && i < l_indexes.count(); ++i) {
    bool find = false;
    bool is_part_equal = false;
    std::pair<int64_t, int64_t> part_id_pair;
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
        part_id_pair.first = l_partition->get_part_id();
        part_id_pair.second = r_partition->get_part_id();
        if (OB_FAIL(matched_partitions.add_member(j))) {
          LOG_WARN("failed to add member", K(ret));
        } else if (OB_FAIL(part_id_map.push_back(part_id_pair))) {
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

int ObPwjComparer::check_list_subpartition_equal(ObSubPartition** l_subpartition_array,
    ObSubPartition** r_subpartition_array, const ObIArray<int64_t>& l_indexes, const ObIArray<int64_t>& r_indexes,
    ObIArray<std::pair<int64_t, int64_t> >& sub_part_id_map, bool& is_equal)
{
  int ret = OB_SUCCESS;
  is_equal = true;
  const ObSubPartition* l_partition = NULL;
  const ObSubPartition* r_partition = NULL;
  ObSqlBitSet<> matched_partitions;
  if (OB_ISNULL(l_subpartition_array) || OB_ISNULL(l_subpartition_array)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(l_subpartition_array), K(r_subpartition_array));
  }
  for (int64_t i = 0; OB_SUCC(ret) && is_equal && i < l_indexes.count(); ++i) {
    bool find = false;
    bool is_subpart_equal = false;
    std::pair<int64_t, int64_t> sub_part_id_pair;
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
        sub_part_id_pair.first = l_partition->get_sub_part_id();
        sub_part_id_pair.second = r_partition->get_sub_part_id();
        if (OB_FAIL(matched_partitions.add_member(j))) {
          LOG_WARN("failed to add member", K(ret));
        } else if (OB_FAIL(sub_part_id_map.push_back(sub_part_id_pair))) {
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

int ObPwjComparer::is_partition_equal(
    const ObPartition* l_partition, const ObPartition* r_partition, const bool is_range_partition, bool& is_equal)
{
  int ret = OB_SUCCESS;
  is_equal = false;
  if (OB_ISNULL(l_partition) || OB_ISNULL(r_partition)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(l_partition), K(r_partition), K(ret));
  } else if (is_range_partition) {
    if (OB_FAIL(is_row_equal(l_partition->get_high_bound_val(), r_partition->get_high_bound_val(), is_equal))) {
      LOG_WARN("failed to check is row equal", K(ret));
    }
  } else if (OB_FAIL(is_list_partition_equal(l_partition, r_partition, is_equal))) {
    LOG_WARN("failed to check is list partition equal", K(ret));
  }
  return ret;
}

int ObPwjComparer::is_partition_equal_old(
    const ObPartition* l_partition, const ObPartition* r_partition, const bool is_range_partition, bool& is_equal)
{
  int ret = OB_SUCCESS;
  is_equal = false;
  if (OB_ISNULL(l_partition) || OB_ISNULL(r_partition)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(l_partition), K(r_partition), K(ret));
  } else if (l_partition->get_part_id() != r_partition->get_part_id()) {
    is_equal = false;
  } else if (is_range_partition) {
    if (OB_FAIL(is_row_equal(l_partition->get_high_bound_val(), r_partition->get_high_bound_val(), is_equal))) {
      LOG_WARN("failed to check is row equal", K(ret));
    }
  } else if (OB_FAIL(is_list_partition_equal(l_partition, r_partition, is_equal))) {
    LOG_WARN("failed to check is list partition equal", K(ret));
  }
  return ret;
}

int ObPwjComparer::is_subpartition_equal(const ObSubPartition* l_subpartition, const ObSubPartition* r_subpartition,
    const bool is_range_partition, bool& is_equal)
{
  int ret = OB_SUCCESS;
  is_equal = false;
  if (OB_ISNULL(l_subpartition) || OB_ISNULL(r_subpartition)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(l_subpartition), K(r_subpartition), K(ret));
  } else if (is_range_partition) {
    if (OB_FAIL(is_row_equal(l_subpartition->get_high_bound_val(), r_subpartition->get_high_bound_val(), is_equal))) {
      LOG_WARN("failed to check is row equal", K(ret));
    }
  } else if (OB_FAIL(is_list_partition_equal(l_subpartition, r_subpartition, is_equal))) {
    LOG_WARN("failed to check is list partition equal", K(ret));
  }
  return ret;
}

int ObPwjComparer::is_subpartition_equal_old(const ObSubPartition* l_subpartition, const ObSubPartition* r_subpartition,
    const bool is_range_partition, bool& is_equal)
{
  int ret = OB_SUCCESS;
  is_equal = false;
  if (OB_ISNULL(l_subpartition) || OB_ISNULL(r_subpartition)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(l_subpartition), K(r_subpartition), K(ret));
  } else if (l_subpartition->get_part_id() != r_subpartition->get_part_id() ||
             l_subpartition->get_sub_part_id() != r_subpartition->get_sub_part_id()) {
    is_equal = false;
  } else if (is_range_partition) {
    if (OB_FAIL(is_row_equal(l_subpartition->get_high_bound_val(), r_subpartition->get_high_bound_val(), is_equal))) {
      LOG_WARN("failed to check is row equal", K(ret));
    }
  } else if (OB_FAIL(is_list_partition_equal(l_subpartition, r_subpartition, is_equal))) {
    LOG_WARN("failed to check is list partition equal", K(ret));
  }
  return ret;
}

int ObPwjComparer::is_row_equal(const ObRowkey& first_row, const ObRowkey& second_row, bool& is_equal)
{
  int ret = OB_SUCCESS;
  is_equal = false;
  if (OB_ISNULL(first_row.get_obj_ptr()) || OB_ISNULL(second_row.get_obj_ptr())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(first_row.get_obj_ptr()), K(second_row.get_obj_ptr()), K(ret));
  } else if (first_row.get_obj_cnt() != second_row.get_obj_cnt()) {
    is_equal = false;
  } else {
    is_equal = true;
    for (int i = 0; OB_SUCC(ret) && is_equal && i < first_row.get_obj_cnt(); i++) {
      if (OB_FAIL(is_obj_equal(first_row.get_obj_ptr()[i], second_row.get_obj_ptr()[i], is_equal))) {
        LOG_WARN("failed to check obj equal", K(ret));
      } else { /*do nothing*/
      }
    }
  }
  return ret;
}

int ObPwjComparer::is_list_partition_equal(
    const ObBasePartition* first_part, const ObBasePartition* second_part, bool& is_equal)
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
    const ObIArray<ObNewRow>& first_values = first_part->get_list_row_values();
    const ObIArray<ObNewRow>& second_values = second_part->get_list_row_values();
    for (int64_t i = 0; OB_SUCC(ret) && is_equal && i < first_values.count(); i++) {
      bool is_find = false;
      for (int64_t j = 0; OB_SUCC(ret) && !is_find && j < second_values.count(); j++) {
        if (OB_FAIL(is_row_equal(first_values.at(i), second_values.at(j), is_find))) {
          LOG_WARN("failed to check row equal", K(ret));
        } else { /*do nothing*/
        }
      }
      if (OB_SUCC(ret) && !is_find) {
        is_equal = false;
      }
    }
  }
  return ret;
}

int ObPwjComparer::is_row_equal(const common::ObNewRow& first_row, const common::ObNewRow& second_row, bool& is_equal)
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
      } else { /*do nothing*/
      }
    }
  }
  return ret;
}

int ObPwjComparer::is_obj_equal(const common::ObObj& first_value, const common::ObObj& second_value, bool& is_equal)
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
    LOG_WARN(
        "first value and second value meta not same", K(ret), K(first_value.get_meta()), K(second_value.get_meta()));
  } else {
    is_equal = (first_value == second_value);
  }
  return ret;
}

int ObPwjComparer::check_if_match_partition_wise(
    const PwjTable& l_table, const PwjTable& r_table, bool& is_partition_wise)
{
  int ret = OB_SUCCESS;
  bool is_physical_equal = false;
  is_partition_wise = false;
  if (is_strict_) {
    if (OB_FAIL(is_physically_equal_partitioned(l_table, r_table, is_physical_equal))) {
      LOG_WARN("failed to check is physically equal partitioned", K(ret));
    }
  } else if (OB_FAIL(is_simple_physically_equal_partitioned(l_table, r_table, is_physical_equal))) {
    LOG_WARN("failed to check is simple physically equal partitioned", K(ret));
  }
  LOG_TRACE("succeed to check is physically equal partitioned", K(is_physical_equal));

  if (OB_SUCC(ret) && is_physical_equal) {
    is_partition_wise = true;
  }
  return ret;
}

int ObPwjComparer::is_simple_physically_equal_partitioned(
    const PwjTable& l_table, const PwjTable& r_table, bool& is_physical_equal)
{
  int ret = OB_SUCCESS;
  is_physical_equal = true;
  const ObPhyPartitionLocationInfoIArray& left_locations = l_table.phy_table_loc_info_->get_phy_part_loc_info_list();
  const ObPhyPartitionLocationInfoIArray& right_locations = r_table.phy_table_loc_info_->get_phy_part_loc_info_list();
  PartitionIdArray* r_array = NULL;

  if (left_locations.count() != right_locations.count()) {
    is_physical_equal = false;
  } else if (OB_ISNULL(r_array = partition_id_group_.alloc_place_holder())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to alloc place holder", K(ret));
  } else if (OB_FAIL(r_array->prepare_allocate(left_locations.count()))) {
    LOG_WARN("failed to prepare allocate", K(ret));
  } else {
    ObSEArray<common::ObAddr, 32> l_servers;
    ObSEArray<common::ObAddr, 32> r_servers;
    for (int64_t i = 0; OB_SUCC(ret) && i < left_locations.count(); ++i) {
      share::ObReplicaLocation l_replica_loc;
      share::ObReplicaLocation r_replica_loc;
      if (OB_FAIL(left_locations.at(i).get_selected_replica(l_replica_loc))) {
        LOG_WARN("fail to get selected replica", K(ret), K(left_locations.at(i)));
      } else if (OB_FAIL(right_locations.at(i).get_selected_replica(r_replica_loc))) {
        LOG_WARN("fail to get selected replica", K(ret), K(right_locations.at(i)));
      } else if (!l_replica_loc.is_valid() || !r_replica_loc.is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("replica location is invalid", K(ret), K(i), K(l_replica_loc), K(r_replica_loc));
      } else if (OB_FAIL(l_servers.push_back(l_replica_loc.server_))) {
        LOG_WARN("failed to push back server addr", K(ret));
      } else if (OB_FAIL(r_servers.push_back(r_replica_loc.server_))) {
        LOG_WARN("failed to push back server addr", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      ObSqlBitSet<> matched_partitions;
      for (int64_t i = 0; is_physical_equal && i < l_servers.count(); ++i) {
        bool find = false;
        for (int64_t j = 0; OB_SUCC(ret) && !find && j < r_servers.count(); ++j) {
          if (matched_partitions.has_member(j) || (l_servers.at(i) != r_servers.at(j))) {
            // do nothing
          } else if (OB_FAIL(matched_partitions.add_member(j))) {
            LOG_WARN("failed to add member", K(ret));
          } else {
            find = true;
            int64_t offset = -1;
            if (ObOptimizerUtil::find_item(l_table.ordered_partition_ids_,
                    left_locations.at(i).get_partition_location().get_partition_id(),
                    &offset)) {
              r_array->at(offset) = right_locations.at(j).get_partition_location().get_partition_id();
            }
          }
        }
        if (OB_SUCC(ret) && !find) {
          is_physical_equal = false;
        }
      }
    }
  }
  return ret;
}

int ObPwjComparer::is_physically_equal_partitioned(
    const PwjTable& l_table, const PwjTable& r_table, bool& is_physical_equal)
{
  int ret = OB_SUCCESS;
  is_physical_equal = true;
  const ObPhyPartitionLocationInfoIArray& left_locations = l_table.phy_table_loc_info_->get_phy_part_loc_info_list();
  const ObPhyPartitionLocationInfoIArray& right_locations = r_table.phy_table_loc_info_->get_phy_part_loc_info_list();
  PartitionIdArray* r_array = NULL;
  if (l_table.ordered_partition_ids_.count() != left_locations.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN(
        "get unexpected partition count", K(ret), K(l_table.ordered_partition_ids_.count()), K(left_locations.count()));
  } else if (left_locations.count() != right_locations.count()) {
    is_physical_equal = false;
  } else if (OB_ISNULL(r_array = partition_id_group_.alloc_place_holder())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to alloc place holder", K(ret));
  } else if (OB_FAIL(r_array->prepare_allocate(left_locations.count()))) {
    LOG_WARN("failed to prepare allocate", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && is_physical_equal && i < left_locations.count(); ++i) {
      const ObPhyPartitionLocationInfo& left_location = left_locations.at(i);
      int64_t l_partition_id = left_location.get_partition_location().get_partition_id();
      int64_t r_partition_id = get_matched_phy_part_id(l_partition_id);
      if (OB_FAIL(check_replica_location_match(left_location, right_locations, r_partition_id, is_physical_equal))) {
        LOG_WARN("failed to check replica location match", K(ret));
      } else if (is_physical_equal) {
        int64_t offset = -1;
        if (ObOptimizerUtil::find_item(l_table.ordered_partition_ids_, l_partition_id, &offset)) {
          r_array->at(offset) = r_partition_id;
        }
      }
    }
  }

  return ret;
}

int ObPwjComparer::check_replica_location_match(const ObPhyPartitionLocationInfo& left_location,
    const ObPhyPartitionLocationInfoIArray& right_locations, const int64_t partition_id, bool& is_physical_equal)
{
  int ret = OB_SUCCESS;
  is_physical_equal = false;
  bool find = false;
  share::ObReplicaLocation replica_loc;
  share::ObReplicaLocation other_replica_loc;
  for (int64_t i = 0; OB_SUCC(ret) && !find && i < right_locations.count(); ++i) {
    if (right_locations.at(i).get_partition_location().get_partition_id() == partition_id) {
      find = true;
      if (OB_FAIL(left_location.get_selected_replica(replica_loc))) {
        LOG_WARN("failed to get selected replica", K(ret), K(left_location));
      } else if (OB_FAIL(right_locations.at(i).get_selected_replica(other_replica_loc))) {
        LOG_WARN("failed to get selected replica", K(ret), K(right_locations.at(i)));
      } else if (!replica_loc.is_valid() || !other_replica_loc.is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("replica location is invalid", K(ret), K(i), K(replica_loc), K(other_replica_loc));
      } else if (replica_loc.server_ == other_replica_loc.server_) {
        is_physical_equal = true;
      }
    }
  }
  return ret;
}

int ObPwjComparer::get_part_id_by_part_index(const PwjTable& table, const int64_t part_index, int64_t& part_id)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(table.partition_array_)) {
    if (OB_ISNULL(table.partition_array_[part_index])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret), K(part_index));
    } else {
      part_id = table.partition_array_[part_index]->get_part_id();
    }
  } else if (!is_hash_like_part(table.part_type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("only hash like part table's partition array can be null", K(ret));
  } else {
    part_id = part_index;
  }
  return ret;
}

int ObPwjComparer::get_def_subpart_id_by_subpart_index(
    const PwjTable& table, const int64_t subpart_index, int64_t& subpart_id)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(table.def_subpartition_array_)) {
    if (OB_ISNULL(table.def_subpartition_array_[subpart_index])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret), K(subpart_index));
    } else {
      subpart_id = table.def_subpartition_array_[subpart_index]->get_sub_part_id();
    }
  } else if (!is_hash_like_part(table.subpart_type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("only hash like part table's partition array can be null", K(ret));
  } else {
    subpart_id = subpart_index;
  }
  return ret;
}

int ObPwjComparer::get_sub_part_id(const PwjTable& table, const int64_t& part_index, int64_t& sub_part_id)
{
  int ret = OB_SUCCESS;
  ObPartition* part = NULL;
  if (table.is_sub_part_template_) {
    int64_t subpart_index = table.all_subpartition_indexes_.at(0);
    if (OB_FAIL(get_def_subpart_id_by_subpart_index(table, subpart_index, sub_part_id))) {
      LOG_WARN("failed to get def subpart id by part index", K(ret));
    }
  } else if (OB_ISNULL(table.partition_array_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(table.partition_array_));
  } else {
    bool find = false;
    for (int64_t i = 0; OB_SUCC(ret) && !find && i < table.all_partition_indexes_.count(); ++i) {
      if (part_index == table.all_partition_indexes_.at(i)) {
        int64_t subpart_index = table.all_subpartition_indexes_.at(i);
        if (OB_ISNULL(part = table.partition_array_[part_index]) || OB_ISNULL(part->get_subpart_array()) ||
            OB_ISNULL(part->get_subpart_array()[subpart_index])) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null", K(ret));
        } else {
          sub_part_id = part->get_subpart_array()[subpart_index]->get_sub_part_id();
          find = true;
        }
      }
    }
    if (OB_SUCC(ret) && !find) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN(
          "failed to find part_index in all_partition_idnexes", K(ret), K(part_index), K(table.all_partition_indexes_));
    }
  }
  return ret;
}

int64_t ObPwjComparer::get_matched_phy_part_id(const int64_t l_phy_part_id)
{
  int64_t phy_part_id = -1;
  for (int64_t i = 0; i < phy_part_map_.count(); ++i) {
    if (l_phy_part_id == phy_part_map_.at(i).first) {
      phy_part_id = phy_part_map_.at(i).second;
      break;
    }
  }
  return phy_part_id;
}
