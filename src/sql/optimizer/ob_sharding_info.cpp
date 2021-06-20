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
#include "sql/optimizer/ob_sharding_info.h"
#include "share/schema/ob_table_schema.h"
#include "share/schema/ob_schema_struct.h"
#include "sql/optimizer/ob_optimizer_context.h"
#include "sql/optimizer/ob_logical_operator.h"
#include "sql/optimizer/ob_log_plan.h"
#include "sql/optimizer/ob_opt_est_utils.h"
#include "sql/resolver/dml/ob_dml_stmt.h"
#include "sql/resolver/dml/ob_raw_expr_sets.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"
#include "sql/optimizer/ob_pwj_comparer.h"

using namespace oceanbase::sql;
using namespace oceanbase::common;
using namespace oceanbase::share::schema;

void ObShardingInfo::reset()
{
  part_level_ = PARTITION_LEVEL_ZERO;
  part_func_type_ = PARTITION_FUNC_TYPE_MAX;
  subpart_func_type_ = PARTITION_FUNC_TYPE_MAX;
  part_num_ = 0;
  subpart_num_ = 0;
  partition_keys_.reset();
  sub_partition_keys_.reset();
  location_type_ = OB_TBL_LOCATION_UNINITIALIZED;
  phy_table_location_info_ = NULL;
  partition_array_ = NULL;
  subpartition_array_ = NULL;
  can_reselect_replica_ = false;
  all_partition_ids_.reset();
  all_partition_indexes_.reset();
  all_subpartition_indexes_.reset();
  is_sub_part_template_ = true;
  is_partition_single_ = false;
  is_subpartition_sinlge_ = false;
}

int ObShardingInfo::assign(const ObShardingInfo& other)
{
  int ret = OB_SUCCESS;
  part_level_ = other.part_level_;
  part_func_type_ = other.part_func_type_;
  subpart_func_type_ = other.subpart_func_type_;
  part_num_ = other.part_num_;
  subpart_num_ = other.subpart_num_;
  location_type_ = other.location_type_;
  phy_table_location_info_ = other.phy_table_location_info_;
  partition_array_ = other.partition_array_;
  subpartition_array_ = other.subpartition_array_;
  can_reselect_replica_ = other.can_reselect_replica_;
  is_sub_part_template_ = other.is_sub_part_template_;
  is_partition_single_ = other.is_partition_single_;
  is_subpartition_sinlge_ = other.is_subpartition_sinlge_;
  if (OB_FAIL(partition_keys_.assign(other.partition_keys_))) {
    LOG_WARN("failed to assign exprs", K(ret));
  } else if (OB_FAIL(sub_partition_keys_.assign(other.sub_partition_keys_))) {
    LOG_WARN("failed to assign exprs", K(ret));
  } else if (OB_FAIL(part_func_exprs_.assign(other.part_func_exprs_))) {
    LOG_WARN("failed to assign exprs", K(ret));
  } else if (OB_FAIL(all_partition_ids_.assign(other.all_partition_ids_))) {
    LOG_WARN("failed to assign partition ids");
  } else if (OB_FAIL(all_partition_indexes_.assign(other.all_partition_indexes_))) {
    LOG_WARN("failed to assign all partitions", K(ret));
  } else if (OB_FAIL(all_subpartition_indexes_.assign(other.all_subpartition_indexes_))) {
    LOG_WARN("failed to assign all subpartitions", K(ret));
  } else { /*do nothing*/
  }
  return ret;
}

int ObShardingInfo::init_partition_info(ObOptimizerContext& ctx, ObDMLStmt& stmt, const uint64_t table_id,
    const uint64_t ref_table_id, const ObPhyTableLocationInfo& phy_table_location_info)
{
  int ret = OB_SUCCESS;
  ObSqlSchemaGuard* schema_guard = NULL;
  const ObTableSchema* table_schema = NULL;
  if (OB_ISNULL(schema_guard = ctx.get_sql_schema_guard())) {
    ret = OB_SCHEMA_ERROR;
    LOG_WARN("failed to get table schema", K(ref_table_id));
  } else if (OB_FAIL(schema_guard->get_table_schema(ref_table_id, table_schema))) {
    ret = OB_SCHEMA_ERROR;
    LOG_WARN("failed to get table schema", K(ref_table_id));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_SCHEMA_ERROR;
    LOG_WARN("failed to get table schema", K(ref_table_id));
  }
  if (OB_SUCC(ret)) {
    ref_table_id_ = ref_table_id;
    part_num_ = table_schema->get_part_option().get_part_num();
    part_level_ = table_schema->get_part_level();
    if (PARTITION_LEVEL_ZERO == part_level_) {
      // no partition expr
    } else if (PARTITION_LEVEL_ONE != part_level_ && PARTITION_LEVEL_TWO != part_level_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Expected part level be one or two", K(ret), K(part_num_), K(part_level_));
    } else {
      ObRawExpr* part_expr = NULL;
      ObRawExpr* subpart_expr = NULL;
      part_func_type_ = table_schema->get_part_option().get_part_func_type();
      partition_array_ = table_schema->get_part_array();
      if (OB_ISNULL(part_expr = stmt.get_part_expr(table_id, ref_table_id))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("There is no part expr in stmt", K(table_id), K(ret));
      } else if ((PARTITION_FUNC_TYPE_KEY == part_func_type_ || PARTITION_FUNC_TYPE_KEY_V2 == part_func_type_ ||
                     PARTITION_FUNC_TYPE_KEY_V3 == part_func_type_) &&
                 OB_FAIL(adjust_key_partition_type(part_expr, part_func_type_))) {
        LOG_WARN("failed to adjust key partition type", K(ret));
      } else if (OB_FAIL(part_func_exprs_.push_back(part_expr))) {
        LOG_WARN("Failed to push back to part expr");
      } else if (OB_FAIL(set_partition_key(part_expr, part_func_type_, partition_keys_))) {
        LOG_WARN("Failed to set partition key", K(ret), K(table_id), K(ref_table_id));
      } else if (PARTITION_LEVEL_TWO == part_level_) {
        subpart_func_type_ = table_schema->get_sub_part_option().get_part_func_type();
        is_sub_part_template_ = table_schema->is_sub_part_template();
        if (is_sub_part_template_) {
          subpart_num_ = table_schema->get_sub_part_option().get_part_num();
          subpartition_array_ = table_schema->get_def_subpart_array();
        }
        if (OB_ISNULL(subpart_expr = stmt.get_subpart_expr(table_id, ref_table_id))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("There is no subpart expr in stmt", K(ret));
        } else if ((PARTITION_FUNC_TYPE_KEY == subpart_func_type_ || PARTITION_FUNC_TYPE_KEY_V2 == subpart_func_type_ ||
                       PARTITION_FUNC_TYPE_KEY_V3 == subpart_func_type_) &&
                   OB_FAIL(adjust_key_partition_type(subpart_expr, subpart_func_type_))) {
          LOG_WARN("failed to adjust key partition type", K(ret));
        } else if (OB_FAIL(set_partition_key(subpart_expr, subpart_func_type_, sub_partition_keys_))) {
          LOG_WARN("Failed to set sub partition key", K(ret), K(table_id), K(ref_table_id));
        } else if (OB_FAIL(part_func_exprs_.push_back(subpart_expr))) {
          LOG_WARN("Failed to set key partition func", K(ret), K(table_id), K(ref_table_id));
        } else {
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    phy_table_location_info_ = &phy_table_location_info;
    if (OB_FAIL(ObPwjComparer::extract_all_partition_indexes(phy_table_location_info,
            *table_schema,
            all_partition_ids_,
            all_partition_indexes_,
            all_subpartition_indexes_,
            is_partition_single_,
            is_subpartition_sinlge_))) {
      LOG_WARN("failed to extract all partition indexes", K(ret));
    }
  }
  return ret;
}

// for key partitioning, the type in the schema is not right, we need to derive the part type from the partition expr
int ObShardingInfo::adjust_key_partition_type(const ObRawExpr* part_expr, share::schema::ObPartitionFuncType& part_type)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(part_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null expr", K(ret));
  } else if (OB_UNLIKELY(part_type != PARTITION_FUNC_TYPE_KEY && part_type != PARTITION_FUNC_TYPE_KEY_V2 &&
                         part_type != PARTITION_FUNC_TYPE_KEY_V3)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid partition type", K(part_type), K(ret));
  } else if (T_FUN_SYS_PART_KEY_V1 == part_expr->get_expr_type()) {
    part_type = PARTITION_FUNC_TYPE_KEY;
  } else if (T_FUN_SYS_PART_KEY_V2 == part_expr->get_expr_type()) {
    part_type = PARTITION_FUNC_TYPE_KEY_V2;
  } else if (T_FUN_SYS_PART_KEY_V3 == part_expr->get_expr_type()) {
    part_type = PARTITION_FUNC_TYPE_KEY_V3;
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected partition type", K(ret), K(part_expr->get_expr_type()));
  }
  return ret;
}

int ObShardingInfo::set_partition_key(
    ObRawExpr* part_expr, const ObPartitionFuncType part_func_type, ObIArray<ObRawExpr*>& partition_keys)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(part_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Part expr should not be NULL", K(ret));
  } else {
    if ((PARTITION_FUNC_TYPE_KEY == part_func_type || PARTITION_FUNC_TYPE_KEY_IMPLICIT == part_func_type ||
            PARTITION_FUNC_TYPE_KEY_IMPLICIT_V2 == part_func_type || PARTITION_FUNC_TYPE_KEY_V2 == part_func_type ||
            PARTITION_FUNC_TYPE_KEY_V3 == part_func_type) ||
        ((PARTITION_FUNC_TYPE_HASH == part_func_type || PARTITION_FUNC_TYPE_HASH_V2 == part_func_type) &&
            share::is_oracle_mode()) ||
        (PARTITION_FUNC_TYPE_RANGE_COLUMNS == part_func_type && T_OP_ROW == part_expr->get_expr_type()) ||
        (PARTITION_FUNC_TYPE_LIST_COLUMNS == part_func_type && T_OP_ROW == part_expr->get_expr_type())) {
      int64_t param_num = part_expr->get_param_count();
      for (int64_t i = 0; OB_SUCC(ret) && i < param_num; ++i) {
        ObRawExpr* expr = part_expr->get_param_expr(i);
        if (OB_FAIL(partition_keys.push_back(expr))) {
          LOG_PRINT_EXPR(WARN, "failed to add partition column expr to partition keys", expr);
        } else {
          LOG_PRINT_EXPR(DEBUG, "succ to add partition column expr to partition keys", expr);
        }
      }
    } else if (OB_FAIL(partition_keys.push_back(part_expr))) {
      LOG_WARN("Failed to add partiton column expr", K(ret));
    } else {
    }
  }
  return ret;
}

int ObShardingInfo::is_join_key_cover_partition_key(const EqualSets& equal_sets, const ObIArray<ObRawExpr*>& first_keys,
    const ObIArray<ObRawExpr*>& first_part_keys, const ObIArray<ObRawExpr*>& second_keys,
    const ObIArray<ObRawExpr*>& second_part_keys, bool& is_cover)
{
  int ret = OB_SUCCESS;
  is_cover = false;
  if (0 == first_keys.count() || first_keys.count() != second_keys.count() || 0 == first_part_keys.count() ||
      first_part_keys.count() != second_part_keys.count()) {
    is_cover = false;
  } else if (OB_FAIL(is_compatible_partition_key(first_part_keys, second_part_keys, is_cover))) {
    LOG_WARN("failed to check if partition keys are compatible", K(ret));
  } else if (!is_cover) {
    /*do nothing*/
  } else {
    int64_t M = first_part_keys.count();
    int64_t N = first_keys.count();
    is_cover = true;
    for (int64_t i = 0; is_cover && i < M; ++i) {
      bool find = false;
      for (int64_t j = 0; !find && j < N; ++j) {
        if ((ObOptimizerUtil::is_expr_equivalent(first_part_keys.at(i), first_keys.at(j), equal_sets) &&
                ObOptimizerUtil::is_expr_equivalent(second_part_keys.at(i), second_keys.at(j), equal_sets)) ||
            (ObOptimizerUtil::is_expr_equivalent(first_part_keys.at(i), second_keys.at(j), equal_sets) &&
                ObOptimizerUtil::is_expr_equivalent(second_part_keys.at(i), first_keys.at(j), equal_sets))) {
          find = true;
        }
      }
      if (!find) {
        is_cover = false;
      }
    }
  }
  LOG_TRACE("is join key cover partition key",
      K(equal_sets),
      K(first_keys),
      K(first_part_keys),
      K(second_keys),
      K(second_part_keys),
      K(is_cover));
  return ret;
}

int ObShardingInfo::is_compatible_partition_key(const common::ObIArray<ObRawExpr*>& first_part_keys,
    const common::ObIArray<ObRawExpr*>& second_part_keys, bool& is_compatible)
{
  int ret = OB_SUCCESS;
  is_compatible = false;
  if (first_part_keys.count() != second_part_keys.count()) {
    is_compatible = false;
  } else {
    is_compatible = true;
    for (int64_t i = 0; OB_SUCC(ret) && is_compatible && i < first_part_keys.count(); i++) {
      if (OB_ISNULL(first_part_keys.at(i)) || OB_ISNULL(second_part_keys.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(first_part_keys.at(i)), K(second_part_keys.at(i)), K(ret));
      } else if (first_part_keys.at(i)->get_data_type() != second_part_keys.at(i)->get_data_type() ||
                 first_part_keys.at(i)->get_collation_type() != second_part_keys.at(i)->get_collation_type()) {
        is_compatible = false;
      } else { /*do nothing*/
      }
    }
  }
  return ret;
}

int ObShardingInfo::check_if_match_partition_wise(ObLogPlan& log_plan __attribute__((unused)),
    const EqualSets& equal_sets, const common::ObIArray<ObRawExpr*>& left_keys,
    const common::ObIArray<ObRawExpr*>& right_keys, const ObShardingInfo& left_sharding,
    const ObShardingInfo& right_sharding, bool& is_partition_wise)
{
  int ret = OB_SUCCESS;
  bool is_key_covered = false;
  bool is_equal = false;
  ObSEArray<ObRawExpr*, 4> left_part_keys;
  ObSEArray<ObRawExpr*, 4> right_part_keys;
  is_partition_wise = false;
  if (left_sharding.is_partition_single() && right_sharding.is_partition_single()) {
    // do nothing
  } else if (OB_FAIL(append(left_part_keys, left_sharding.get_partition_keys()))) {
    LOG_WARN("failed to get parititon keys", K(ret));
  } else if (OB_FAIL(append(right_part_keys, right_sharding.get_partition_keys()))) {
    LOG_WARN("failed to get partition keys", K(ret));
  }
  if (OB_SUCC(ret)) {
    if (left_sharding.is_subpartition_single() && right_sharding.is_subpartition_single()) {
      // do nothing
    } else if (OB_FAIL(append(left_part_keys, left_sharding.get_sub_partition_keys()))) {
      LOG_WARN("failed to get sub parititon keys", K(ret));
    } else if (OB_FAIL(append(right_part_keys, right_sharding.get_sub_partition_keys()))) {
      LOG_WARN("failed to get sub partition keys", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(is_join_key_cover_partition_key(
            equal_sets, left_keys, left_part_keys, right_keys, right_part_keys, is_key_covered))) {
      LOG_WARN("failed to check is join key cover partition key", K(ret));
    } else if (!is_key_covered) {
      /*do nothing*/
    } else {
      ObPwjComparer pwj_comparer(true);
      PwjTable l_table;
      PwjTable r_table;
      if (OB_FAIL(l_table.init(left_sharding))) {
        LOG_WARN("failed to init pwj table with sharding info", K(ret));
      } else if (OB_FAIL(r_table.init(right_sharding))) {
        LOG_WARN("failed to init pwj table with sharding info", K(ret));
      } else if (OB_FAIL(pwj_comparer.add_table(l_table, is_equal))) {
        LOG_WARN("failed to add table", K(ret));
      } else if (!is_equal) {
        // do nothing
      } else if (OB_FAIL(pwj_comparer.add_table(r_table, is_equal))) {
        LOG_WARN("failed to add table", K(ret));
      } else if (is_equal) {
        is_partition_wise = true;
      }
    }
  }
  LOG_TRACE("succeed check if match partition wise", K(left_sharding), K(right_sharding), K(is_partition_wise));
  return ret;
}

int ObShardingInfo::check_if_match_repart(const EqualSets& equal_sets, const ObIArray<ObRawExpr*>& src_join_keys,
    const ObIArray<ObRawExpr*>& target_join_keys, const ObIArray<ObRawExpr*>& target_part_keys, bool& is_match)
{
  int ret = OB_SUCCESS;
  is_match = false;
  if (OB_UNLIKELY(src_join_keys.count() != target_join_keys.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected array count", K(src_join_keys.count()), K(target_join_keys.count()), K(ret));
  } else if (0 == target_part_keys.count()) {
    is_match = false;
  } else {
    is_match = true;
    for (int64_t i = 0; OB_SUCC(ret) && is_match && i < target_part_keys.count(); ++i) {
      ObRawExpr* right_part_key = NULL;
      if (OB_ISNULL(right_part_key = target_part_keys.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(right_part_key), K(ret));
      } else {
        bool is_found = false;
        for (int64_t j = 0; OB_SUCC(ret) && !is_found && j < target_join_keys.count(); ++j) {
          ObRawExpr* left_key = NULL;
          ObRawExpr* right_key = NULL;
          if (OB_ISNULL(left_key = src_join_keys.at(j)) || OB_ISNULL(right_key = target_join_keys.at(j))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("get unexpected null", K(left_key), K(right_key), K(ret));
          } else if (ObOptimizerUtil::is_expr_equivalent(right_part_key, right_key, equal_sets) &&
                     right_part_key->get_result_type().get_type_class() ==
                         left_key->get_result_type().get_type_class() &&
                     right_part_key->get_result_type().get_collation_type() ==
                         left_key->get_result_type().get_collation_type() &&
                     !ObObjCmpFuncs::is_otimestamp_cmp(
                         right_part_key->get_result_type().get_type(), left_key->get_result_type().get_type()) &&
                     !ObObjCmpFuncs::is_datetime_timestamp_cmp(
                         right_part_key->get_result_type().get_type(), left_key->get_result_type().get_type())) {
            is_found = true;
          } else { /*do nothing*/
          }
        }
        if (OB_SUCC(ret) && !is_found) {
          is_match = false;
        }
      }
    }
  }
  return ret;
}

int ObShardingInfo::is_physically_equal_partitioned(ObLogPlan& log_plan __attribute__((unused)),
    const ObShardingInfo& left_sharding, const ObShardingInfo& right_sharding, bool& is_physical_equal)
{
  int ret = OB_SUCCESS;
  ret = is_physically_equal_partitioned(left_sharding, right_sharding, is_physical_equal);
  return ret;
}

int ObShardingInfo::is_physically_equal_partitioned(
    const ObShardingInfo& left_sharding, const ObShardingInfo& right_sharding, bool& is_physical_equal)
{
  int ret = OB_SUCCESS;
  is_physical_equal = true;
  if (OB_ISNULL(left_sharding.phy_table_location_info_) || OB_ISNULL(right_sharding.phy_table_location_info_)) {
    // some sharding info maybe reset
    is_physical_equal = false;
    LOG_TRACE("sharding info has not been initialized/reset",
        K(ret),
        K(left_sharding.phy_table_location_info_),
        K(right_sharding.phy_table_location_info_));
  } else {
    const ObPhyPartitionLocationInfoIArray& left_locations =
        left_sharding.phy_table_location_info_->get_phy_part_loc_info_list();
    const ObPhyPartitionLocationInfoIArray& right_locations =
        right_sharding.phy_table_location_info_->get_phy_part_loc_info_list();
    if (left_sharding.is_remote() && right_sharding.is_remote()) {
      // When left_sharding and right_sharding are both remote,
      // if they are on same physical server, remote plan should be generated.
      share::ObReplicaLocation replica_loc;
      share::ObReplicaLocation other_replica_loc;
      if (0 == left_locations.count() || 0 == right_locations.count()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("location.count() is 0 unexpectedly", K(ret), K(left_locations.count()), K(right_locations.count()));
      } else if (OB_FAIL(left_locations.at(0).get_selected_replica(replica_loc))) {
        LOG_WARN("fail to get selected replica", K(ret), K(left_locations.at(0)));
      } else if (OB_FAIL(right_locations.at(0).get_selected_replica(other_replica_loc))) {
        LOG_WARN("fail to get selected replica", K(ret), K(right_locations.at(0)));
      } else if (!replica_loc.is_valid() || !other_replica_loc.is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("replica location is invalid", K(ret), K(replica_loc), K(other_replica_loc));
      } else if (replica_loc.server_ != other_replica_loc.server_) {
        is_physical_equal = false;
      } else { /*do nothing*/
      }
    } else if (left_locations.count() == right_locations.count()) {

      ObSEArray<common::ObAddr, 32> left_servers;
      ObSEArray<common::ObAddr, 32> right_servers;
      for (int64_t i = 0; OB_SUCC(ret) && i < left_locations.count(); ++i) {
        share::ObReplicaLocation left_replica_loc;
        share::ObReplicaLocation right_replica_loc;
        if (OB_FAIL(left_locations.at(i).get_selected_replica(left_replica_loc))) {
          LOG_WARN("fail to get selected replica", K(ret), K(left_locations.at(i)));
        } else if (OB_FAIL(right_locations.at(i).get_selected_replica(right_replica_loc))) {
          LOG_WARN("fail to get selected replica", K(ret), K(right_locations.at(i)));
        } else if (!left_replica_loc.is_valid() || !right_replica_loc.is_valid()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("replica location is invalid", K(ret), K(i), K(left_replica_loc), K(right_replica_loc));
        } else if (OB_FAIL(left_servers.push_back(left_replica_loc.server_))) {
          LOG_WARN("failed to push back server addr", K(ret));
        } else if (OB_FAIL(right_servers.push_back(right_replica_loc.server_))) {
          LOG_WARN("failed to push back server addr", K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        std::sort(&left_servers.at(0), &left_servers.at(0) + left_servers.count());
        std::sort(&right_servers.at(0), &right_servers.at(0) + right_servers.count());
        for (int64_t i = 0; is_physical_equal && i < left_servers.count(); ++i) {
          if (left_servers.at(i) != right_servers.at(i)) {
            is_physical_equal = false;
          }
        }
      }
    } else {
      is_physical_equal = false;
    }
  }
  return ret;
}

int ObShardingInfo::copy_with_part_keys(const ObShardingInfo& other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(copy_without_part_keys(other))) {
  } else if (OB_FAIL(append(partition_keys_, other.get_partition_keys()))) {
    LOG_WARN("failed to append partition key", K(ret));
  } else if (OB_FAIL(append(sub_partition_keys_, other.get_sub_partition_keys()))) {
    LOG_WARN("failed to append sub partition key", K(ret));
  } else if (OB_FAIL(append(part_func_exprs_, other.get_partition_func()))) {
    LOG_WARN("Failed to append partition function", K(ret));
  } else { /*do nothing*/
  }
  return ret;
}

// todo this interface is strange, should be removed some day
int ObShardingInfo::copy_without_part_keys(const ObShardingInfo& other)
{
  int ret = OB_SUCCESS;
  part_level_ = other.part_level_;
  part_func_type_ = other.part_func_type_;
  subpart_func_type_ = other.subpart_func_type_;
  part_num_ = other.part_num_;
  subpart_num_ = other.subpart_num_;
  location_type_ = other.location_type_;
  phy_table_location_info_ = other.phy_table_location_info_;
  partition_array_ = other.partition_array_;
  subpartition_array_ = other.subpartition_array_;
  can_reselect_replica_ = other.can_reselect_replica_;
  ref_table_id_ = other.ref_table_id_;
  is_sub_part_template_ = other.is_sub_part_template_;
  is_partition_single_ = other.is_partition_single_;
  is_subpartition_sinlge_ = other.is_subpartition_sinlge_;
  if (OB_FAIL(all_partition_ids_.assign(other.all_partition_ids_))) {
    LOG_WARN("failed to assign partition ids");
  } else if (OB_FAIL(all_partition_indexes_.assign(other.all_partition_indexes_))) {
    LOG_WARN("failed to assign used partitions", K(ret));
  } else if (OB_FAIL(all_subpartition_indexes_.assign(other.all_subpartition_indexes_))) {
    LOG_WARN("failed to assign used subpartitions", K(ret));
  }
  return ret;
}

int ObShardingInfo::get_all_partition_keys(
    common::ObIArray<ObRawExpr*>& out_part_keys, bool ignore_single_partition /* = false */) const
{
  int ret = OB_SUCCESS;
  if (!(ignore_single_partition && is_partition_single()) && OB_FAIL(out_part_keys.assign(partition_keys_))) {
    LOG_WARN("failed to assign array", K(ret));
  } else if (!(ignore_single_partition && is_subpartition_single()) && append(out_part_keys, sub_partition_keys_)) {
    LOG_WARN("failed to append array", K(ret));
  } else { /*do nothing*/
  }
  return ret;
}

int ObShardingInfo::get_total_part_cnt(int64_t& total_part_cnt) const
{
  int ret = OB_SUCCESS;
  total_part_cnt = 1;
  if (share::schema::PARTITION_LEVEL_ONE == part_level_) {
    total_part_cnt = part_num_;
  } else if (share::schema::PARTITION_LEVEL_TWO == part_level_) {
    if (is_sub_part_template_) {
      total_part_cnt = part_num_ * subpart_num_;
    } else if (OB_ISNULL(partition_array_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else {
      total_part_cnt = 0;
      ObPartition* cur_part = NULL;
      for (int64_t i = 0; OB_SUCC(ret) && i < part_num_; ++i) {
        if (OB_ISNULL(cur_part = partition_array_[i])) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null", K(ret));
        } else {
          total_part_cnt += cur_part->get_sub_part_num();
        }
      }
    }
  }
  return ret;
}
