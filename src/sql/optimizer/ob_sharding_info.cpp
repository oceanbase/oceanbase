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

int ObShardingInfo::assign(const ObShardingInfo &other)
{
  int ret = OB_SUCCESS;
  part_level_ = other.part_level_;
  part_func_type_ = other.part_func_type_;
  subpart_func_type_ = other.subpart_func_type_;
  part_num_ = other.part_num_;
  location_type_ = other.location_type_;
  phy_table_location_info_ = other.phy_table_location_info_;
  partition_array_ = other.partition_array_;
  can_reselect_replica_ = other.can_reselect_replica_;
  is_partition_single_ = other.is_partition_single_;
  is_subpartition_sinlge_ = other.is_subpartition_sinlge_;
  if (OB_FAIL(partition_keys_.assign(other.partition_keys_))) {
    LOG_WARN("failed to assign exprs", K(ret));
  } else if (OB_FAIL(sub_partition_keys_.assign(other.sub_partition_keys_))) {
    LOG_WARN("failed to assign exprs", K(ret));
  } else if (OB_FAIL(part_func_exprs_.assign(other.part_func_exprs_))) {
    LOG_WARN("failed to assign exprs", K(ret));
  } else if (OB_FAIL(all_tablet_ids_.assign(other.all_tablet_ids_))) {
    LOG_WARN("failed to assign partition ids");
  } else if (OB_FAIL(all_partition_indexes_.assign(other.all_partition_indexes_))) {
    LOG_WARN("failed to assign all partitions", K(ret));
  } else if (OB_FAIL(all_subpartition_indexes_.assign(other.all_subpartition_indexes_))) {
    LOG_WARN("failed to assign all subpartitions", K(ret));
  } else { /*do nothing*/ }
  return ret;
}

int ObShardingInfo::init_partition_info(ObOptimizerContext &ctx,
                                        const ObDMLStmt &stmt,
                                        const uint64_t table_id,
                                        const uint64_t ref_table_id,
                                        const ObCandiTableLoc &phy_table_location_info)
{
  int ret = OB_SUCCESS;
  ObSqlSchemaGuard *schema_guard = NULL;
  const ObTableSchema *table_schema = NULL;
  if (OB_ISNULL(schema_guard = ctx.get_sql_schema_guard())) {
    ret = OB_SCHEMA_ERROR;
    LOG_WARN("failed to get table schema", K(ref_table_id));
  } else if (OB_FAIL(schema_guard->get_table_schema(table_id, ref_table_id, &stmt, table_schema))) {
  	ret = OB_SCHEMA_ERROR;
  	LOG_WARN("failed to get table schema", K(ref_table_id));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_SCHEMA_ERROR;
    LOG_WARN("failed to get table schema", K(ref_table_id));
  }
  if (OB_SUCC(ret)) {
    part_num_ = table_schema->get_part_option().get_part_num();
    part_level_ = table_schema->get_part_level();
    if (PARTITION_LEVEL_ZERO == part_level_) {
      //no partition expr
    } else if (PARTITION_LEVEL_ONE != part_level_ && PARTITION_LEVEL_TWO != part_level_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Expected part level be one or two", K(ret), K(part_num_), K(part_level_));
    } else {
      ObRawExpr *part_expr = NULL;
      ObRawExpr *subpart_expr = NULL;
      part_func_type_ = table_schema->get_part_option().get_part_func_type();
      partition_array_ = table_schema->get_part_array();
      if (OB_ISNULL(part_expr = stmt.get_part_expr(table_id, ref_table_id))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("There is no part expr in stmt", K(table_id), K(ret));
      } else if (OB_FAIL(part_func_exprs_.push_back(part_expr))) {
      	LOG_WARN("Failed to push back to part expr");
    	} else if (OB_FAIL(set_partition_key(part_expr, part_func_type_, partition_keys_))) {
        LOG_WARN("Failed to set partition key", K(ret), K(table_id), K(ref_table_id));
      } else if (PARTITION_LEVEL_TWO == part_level_) {
        subpart_func_type_ = table_schema->get_sub_part_option().get_part_func_type();
        if (OB_ISNULL(subpart_expr = stmt.get_subpart_expr(table_id, ref_table_id))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("There is no subpart expr in stmt", K(ret));
        } else if (OB_FAIL(set_partition_key(subpart_expr, subpart_func_type_, sub_partition_keys_))) {
          LOG_WARN("Failed to set sub partition key", K(ret), K(table_id), K(ref_table_id));
        } else if (OB_FAIL(part_func_exprs_.push_back(subpart_expr))) {
        	LOG_WARN("Failed to set key partition func", K(ret), K(table_id), K(ref_table_id));
        } else { }
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (!(is_virtual_table(ref_table_id) && OB_TBL_LOCATION_DISTRIBUTED == location_type_)) {
      phy_table_location_info_ = &phy_table_location_info;
    }
    // virtual table can't get part idx from mock list table schema
    // so don't call extract_all_partition_indexes,
    // and not used pwj enhance on virtual table
    if (is_virtual_table(ref_table_id)) {
      // do nothing
    } else if (OB_FAIL(ObPwjComparer::extract_all_partition_indexes(phy_table_location_info,
                                                              *table_schema,
                                                              all_tablet_ids_,
                                                              all_partition_indexes_,
                                                              all_subpartition_indexes_,
                                                              is_partition_single_,
                                                              is_subpartition_sinlge_))) {
      LOG_WARN("failed to extract all partition indexes", K(ret));
    }
  }
  return ret;
}

int ObShardingInfo::set_partition_key(
    ObRawExpr *part_expr,
    const ObPartitionFuncType part_func_type,
    ObIArray<ObRawExpr*> &partition_keys)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(part_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Part expr should not be NULL", K(ret));
  } else {
    if ((PARTITION_FUNC_TYPE_KEY == part_func_type
        || PARTITION_FUNC_TYPE_KEY_IMPLICIT == part_func_type)
        || (PARTITION_FUNC_TYPE_HASH == part_func_type
            && lib::is_oracle_mode())
        || (PARTITION_FUNC_TYPE_RANGE_COLUMNS == part_func_type
               && T_OP_ROW == part_expr->get_expr_type())
        || (PARTITION_FUNC_TYPE_LIST_COLUMNS == part_func_type &&
            T_OP_ROW == part_expr->get_expr_type())) {
      int64_t param_num = part_expr->get_param_count();
      for (int64_t i = 0; OB_SUCC(ret) && i < param_num; ++i) {
        ObRawExpr *expr = part_expr->get_param_expr(i);
        if (OB_FAIL(partition_keys.push_back(expr))) {
          LOG_PRINT_EXPR(
              WARN, "failed to add partition column expr to partition keys", expr);
        } else {
          LOG_PRINT_EXPR(
              DEBUG, "succ to add partition column expr to partition keys", expr);
        }
      }
    } else if (OB_FAIL(partition_keys.push_back(part_expr))) {
      LOG_WARN("Failed to add partition column expr", K(ret));
    } else { }
  }
  return ret;
}

int ObShardingInfo::is_compatible_partition_key(const ObShardingInfo *first_sharding,
                                                const ObIArray<ObSEArray<ObRawExpr*, 8>> &first_part_keys_list,
                                                const ObIArray<ObSEArray<ObRawExpr*, 8>> &second_part_keys_list,
                                                bool &is_compatible)
{
  int ret = OB_SUCCESS;
  is_compatible = false;
  if (first_part_keys_list.count() != second_part_keys_list.count() || first_part_keys_list.empty()) {
    is_compatible = false;
  } else {
    is_compatible = true;
    ObRawExpr *l_part_key = NULL;
    ObRawExpr *r_part_key = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && is_compatible && i < first_part_keys_list.count(); i++) {
      if (OB_UNLIKELY(first_part_keys_list.at(i).empty() || second_part_keys_list.at(i).empty())
          || OB_ISNULL(l_part_key = first_part_keys_list.at(i).at(0))
          || OB_ISNULL(r_part_key = second_part_keys_list.at(i).at(0))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(l_part_key), K(r_part_key), K(ret));
      } else if (l_part_key->get_data_type() == ObDecimalIntType
                 && r_part_key->get_data_type() == ObDecimalIntType
                 && first_sharding != NULL) {
        LOG_DEBUG("compatible partition key",
                  K(l_part_key->get_result_type()),
                  K(r_part_key->get_result_type()),
                  K(l_part_key->get_result_type().get_accuracy()),
                  K(r_part_key->get_result_type().get_accuracy()));
        if (is_part_func_scale_sensitive(*first_sharding,
                                         l_part_key->get_data_type())) {
          is_compatible =
            (l_part_key->get_scale() == r_part_key->get_scale());
        } else {
          is_compatible = true;
        }
      } else if (l_part_key->get_data_type() != r_part_key->get_data_type() ||
                  l_part_key->get_collation_type() != r_part_key->get_collation_type()) {
        is_compatible = false;
      }
    }
  }
  return ret;
}

int ObShardingInfo::is_compatible_partition_key(const ObShardingInfo &first_sharding,
                                                const ObShardingInfo &second_sharding,
                                                bool &is_compatible)
{
  int ret = OB_SUCCESS;
  is_compatible = false;
  ObSEArray<ObRawExpr*, 8> first_part_keys;
  ObSEArray<ObRawExpr*, 8> second_part_keys;
  if (OB_FAIL(first_sharding.get_all_partition_keys(first_part_keys, true))) {
    LOG_WARN("failed to get sharding partition keys", K(ret));
  } else if (OB_FAIL(second_sharding.get_all_partition_keys(second_part_keys, true))) {
    LOG_WARN("failed to get sharding partition keys", K(ret));
  } else if ((first_part_keys.count() != second_part_keys.count()) || first_part_keys.empty()) {
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
      } else { /*do nothing*/ }
    }
  }
  return ret;
}

int ObShardingInfo::is_join_key_cover_partition_key(const EqualSets &equal_sets,
                                                    const ObIArray<ObRawExpr *> &first_keys,
                                                    const ObIArray<ObShardingInfo *> &first_shardings,
                                                    const ObIArray<ObRawExpr *> &second_keys,
                                                    const ObIArray<ObShardingInfo *> &second_shardings,
                                                    bool &is_cover)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObSEArray<ObRawExpr*, 8>, 8> first_part_keys;
  ObSEArray<ObSEArray<ObRawExpr*, 8>, 8> second_part_keys;
  is_cover = false;
  bool is_compatible = false;
  ObShardingInfo *first_left_sharding = NULL;
  if (OB_UNLIKELY(first_keys.count() != second_keys.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected array count", K(first_keys.count()), K(second_keys.count()), K(ret));
  } else if (OB_FAIL(extract_partition_key(first_shardings,
                                           first_part_keys))) {
    LOG_WARN("failed to extract partition keys", K(ret));
  } else if (OB_FAIL(extract_partition_key(second_shardings,
                                           second_part_keys))) {
    LOG_WARN("failed to extract partition keys", K(ret));
  } else if (!first_shardings.empty() && OB_ISNULL(first_left_sharding = first_shardings.at(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(first_shardings), K(ret));
  } else if (OB_FAIL(is_compatible_partition_key(first_left_sharding,
                                                 first_part_keys,
                                                 second_part_keys,
                                                 is_compatible))) {
    LOG_WARN("failed to check if is comptiable keys", K(ret));
  } else if (!is_compatible) {
    /* do nothing */
  } else {
    is_cover = true;
    for (int64_t i = 0; OB_SUCC(ret) && is_cover && i < first_part_keys.count(); i++) {
      bool is_equal = false;
      for(int64_t j = 0; OB_SUCC(ret) && !is_equal && j < first_keys.count(); j++) {
        if (OB_FAIL(is_expr_equivalent(equal_sets,
                                       *first_left_sharding,
                                       first_part_keys.at(i),
                                       second_part_keys.at(i),
                                       first_keys.at(j),
                                       second_keys.at(j),
                                       is_equal))) {
          LOG_WARN("failed to check expr equivalent", K(ret));
        } else { /*do nothing*/ }
      }
      if (OB_SUCC(ret) && !is_equal) {
        is_cover = false;
      }
    }
  }
  return ret;
}

int ObShardingInfo::is_lossless_column_cast(const ObRawExpr *expr,
                                            const ObShardingInfo &sharding_info, bool &is_lossless)
{
  int ret = OB_SUCCESS;
  is_lossless = false;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null raw expr", K(ret));
  } else if (OB_FAIL(ObOptimizerUtil::is_lossless_column_cast(expr, is_lossless))) {
    LOG_WARN("check lossless column cast failed", K(ret));
  } else if (is_lossless) {
    if (expr->get_result_type().is_decimal_int()
        && expr->get_param_expr(0)->get_result_type().is_decimal_int()
        && is_part_func_scale_sensitive(sharding_info, ObDecimalIntType)) {
      is_lossless = (expr->get_scale() == expr->get_param_expr(0)->get_scale());
    }
  }
  LOG_DEBUG("sharding info, is_lossless column_cast", K(*expr), K(is_lossless), K(sharding_info));
  return ret;
}
int ObShardingInfo::is_expr_equivalent(const EqualSets &equal_sets,
                                       const ObShardingInfo &first_sharding,
                                       const ObIArray<ObRawExpr*> &first_part_keys,
                                       const ObIArray<ObRawExpr*> &second_part_keys,
                                       ObRawExpr *first_key,
                                       ObRawExpr *second_key,
                                       bool &is_equal)
{
  int ret = OB_SUCCESS;
  is_equal = false;
  bool first_key_lossless = false, second_key_lossless = false;
  if (OB_ISNULL(first_key) || OB_ISNULL(second_key)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(first_key), K(second_key), K(ret));
  } else if (OB_FAIL(is_lossless_column_cast(first_key, first_sharding, first_key_lossless))) {
    LOG_WARN("check lossless cast failed", K(ret));
  } else if (OB_FAIL(is_lossless_column_cast(second_key, first_sharding, second_key_lossless))) {
    LOG_WARN("check lossless cast failed", K(ret));
  } else {
    if (first_key_lossless) {
      first_key = first_key->get_param_expr(0);
    }
    if (second_key_lossless) {
      second_key = second_key->get_param_expr(0);
    }
  }
  if (OB_FAIL(ret)) {
    // do nothing
  } else {
    bool left_is_equal = false;
    bool right_is_equal = false;
    for (int64_t i = 0; OB_SUCC(ret) && !left_is_equal && i < first_part_keys.count(); i++) {
      if (OB_ISNULL(first_part_keys.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (ObOptimizerUtil::is_expr_equivalent(first_part_keys.at(i), first_key, equal_sets)) {
        left_is_equal = true;
      } else { /*do nothing*/ }
    }
    for (int64_t i = 0; OB_SUCC(ret) && !right_is_equal && i < second_part_keys.count(); i++) {
      if (OB_ISNULL(second_part_keys.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (ObOptimizerUtil::is_expr_equivalent(second_part_keys.at(i), second_key, equal_sets)) {
        right_is_equal = true;
      } else { /*do nothing*/ }
    }
    if (OB_SUCC(ret)) {
      is_equal = left_is_equal && right_is_equal;
    }
  }
  return ret;
}

bool ObShardingInfo::is_part_func_scale_sensitive(
  const sql::ObShardingInfo &sharding_info, const common::ObObjType obj_type)
{
  bool ret = false;
  ObPartitionFuncType part_func_type = PARTITION_FUNC_TYPE_MAX;
  if (sharding_info.get_part_level() == PARTITION_LEVEL_ONE) {
    part_func_type = sharding_info.get_part_func_type();
  } else if (sharding_info.get_part_level() == PARTITION_LEVEL_TWO) {
    part_func_type = sharding_info.get_sub_part_func_type();
  }
  // PARTITION_FUNC_TYPE_MAX need to be scale sensitive. Consider PWJ:
  //
  //  Hash Join(equal_conds([cast(c.deptno, DECIMAL_INT(5, 2)) = p.c1]))
  //    EXCHANGE HASH(#key = deptno)
  //      SUB_PLAN0
  //    EXCHANGE HASH(#key = c1)
  //      SUB_PLAN1
  //
  // It's a extended partition wise join, part_func_type is PARTITION_FUNC_TYPE_MAX.
  // Partitions hashed by `deptno` must be same with partitions hashed by `cast(c.deptno, DECIMAL_INT(5, 2))`
  if (PARTITION_FUNC_TYPE_HASH == part_func_type || PARTITION_FUNC_TYPE_KEY == part_func_type
      || PARTITION_FUNC_TYPE_KEY_IMPLICIT == part_func_type
      || PARTITION_FUNC_TYPE_MAX == part_func_type) {
    ret = ob_is_decimal_int(obj_type);
  }
  return ret;
}

int ObShardingInfo::extract_partition_key(const ObIArray<ObShardingInfo *> &input_shardings,
                                          ObIArray<ObSEArray<ObRawExpr*, 8>> &partition_key_list)
{
  int ret = OB_SUCCESS;
  int64_t partition_cnt = 0;
  ObSEArray<ObRawExpr*, 8> partition_keys;
  for (int64_t i = 0; OB_SUCC(ret) && i < input_shardings.count(); i++) {
    partition_keys.reuse();
    if (OB_ISNULL(input_shardings.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (OB_FAIL(input_shardings.at(i)->get_all_partition_keys(partition_keys, true))) {
      LOG_WARN("failed to get partition keys", K(ret));
    } else if (partition_keys.empty()) {
      /* do nothing */
    } else if (0 == partition_cnt) {
      partition_cnt = partition_keys.count();
      ObSEArray<ObRawExpr*, 8> temp_keys;
      for (int64_t j = 0; OB_SUCC(ret) && j < partition_cnt; j++) {
        temp_keys.reuse();
        if (OB_FAIL(temp_keys.push_back(partition_keys.at(j)))) {
          LOG_WARN("failed to push back expr", K(ret));
        } else if (OB_FAIL(partition_key_list.push_back(temp_keys))) {
          LOG_WARN("failed to push back expr array", K(ret));
        } else { /*do nothing*/ }
      }
    } else if (OB_UNLIKELY(partition_cnt != partition_keys.count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected array cnt", K(partition_cnt), K(partition_keys.count()), K(ret));
    } else {
      for (int64_t j = 0; OB_SUCC(ret) && j < partition_cnt; j++) {
        if (OB_FAIL(partition_key_list.at(j).push_back(partition_keys.at(j)))) {
          LOG_WARN("failed to push back expr", K(ret));
        } else { /*do nothing*/ }
      }
    }
  }
  return ret;
}

int ObShardingInfo::check_if_match_partition_wise(const EqualSets &equal_sets,
                                                  const common::ObIArray<ObRawExpr*> &left_keys,
                                                  const common::ObIArray<ObRawExpr*> &right_keys,
                                                  ObShardingInfo *left_strong_sharding,
                                                  ObShardingInfo *right_strong_sharding,
                                                  bool &is_partition_wise)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObShardingInfo*, 1> dummy_sharding;
  ObSEArray<bool, 4> null_safe_info;
  for (int64_t i = 0; OB_SUCC(ret) && i < left_keys.count(); i++) {
    if (OB_FAIL(null_safe_info.push_back(true))) {
      LOG_WARN("failed to push back null safe info", K(ret));
    } else { /*do nothing*/ }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(check_if_match_partition_wise(equal_sets,
                                              left_keys,
                                              right_keys,
                                              null_safe_info,
                                              left_strong_sharding,
                                              dummy_sharding,
                                              right_strong_sharding,
                                              dummy_sharding,
                                              is_partition_wise))) {
      LOG_WARN("failed to check if match partition wise", K(ret));
    } else {
      LOG_TRACE("succeed to check if match partition wise", K(is_partition_wise));
    }
  }
  return ret;
}

int ObShardingInfo::check_if_match_extended_partition_wise(const EqualSets &equal_sets,
                                                           ObIArray<ObAddr> &left_server_list,
                                                           ObIArray<ObAddr> &right_server_list,
                                                           const common::ObIArray<ObRawExpr*> &left_keys,
                                                           const common::ObIArray<ObRawExpr*> &right_keys,
                                                           ObShardingInfo *left_strong_sharding,
                                                           ObShardingInfo *right_strong_sharding,
                                                           bool &is_ext_partition_wise)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObShardingInfo*, 1> dummy_sharding;
  ObSEArray<bool, 4> null_safe_info;
  for (int64_t i = 0; OB_SUCC(ret) && i < left_keys.count(); i++) {
    if (OB_FAIL(null_safe_info.push_back(true))) {
      LOG_WARN("failed to push back null safe info", K(ret));
    } else { /*do nothing*/ }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(check_if_match_extended_partition_wise(equal_sets,
                                                      left_server_list,
                                                      right_server_list,
                                                      left_keys,
                                                      right_keys,
                                                      null_safe_info,
                                                      left_strong_sharding,
                                                      dummy_sharding,
                                                      right_strong_sharding,
                                                      dummy_sharding,
                                                      is_ext_partition_wise))) {
      LOG_WARN("failed to check if match extended partition wise", K(ret));
    } else {
      LOG_TRACE("succeed to check if match extended partition wise", K(is_ext_partition_wise));
    }
  }
  return ret;
}

int ObShardingInfo::check_if_match_partition_wise(const EqualSets &equal_sets,
                                                  const common::ObIArray<ObRawExpr*> &left_keys,
                                                  const common::ObIArray<ObRawExpr*> &right_keys,
                                                  const common::ObIArray<bool> &null_safe_info,
                                                  ObShardingInfo *left_strong_sharding,
                                                  const common::ObIArray<ObShardingInfo *> &left_weak_sharding,
                                                  ObShardingInfo *right_strong_sharding,
                                                  const common::ObIArray<ObShardingInfo *> &right_weak_sharding,
                                                  bool &is_partition_wise)
{
  int ret = OB_SUCCESS;
  is_partition_wise = false;
  if (OB_UNLIKELY(left_keys.count() != null_safe_info.count()) ||
      OB_UNLIKELY(right_keys.count() != null_safe_info.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected array count", K(left_keys.count()), K(right_keys.count()),
        K(null_safe_info.count()), K(ret));
  } else {
    bool has_null_safe = false;
    ObSEArray<ObShardingInfo*, 8> left_sharding;
    ObSEArray<ObShardingInfo*, 8> right_sharding;
    ObSEArray<ObRawExpr*, 8> strong_left_keys;
    ObSEArray<ObRawExpr*, 8> strong_right_keys;
    for (int64_t i = 0; OB_SUCC(ret) && i < null_safe_info.count(); i++) {
      if (OB_ISNULL(left_keys.at(i)) || OB_ISNULL(right_keys.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(left_keys.at(i)), K(right_keys.at(i)), K(ret));
      } else if (null_safe_info.at(i)) {
        has_null_safe = true;
      } else if (OB_FAIL(strong_left_keys.push_back(left_keys.at(i))) ||
                 OB_FAIL(strong_right_keys.push_back(right_keys.at(i)))) {
        LOG_WARN("failed to push back keys", K(ret));
      } else { /*do nothing*/ }
    }
    if (OB_SUCC(ret)) {
      if (NULL != left_strong_sharding &&
          NULL != left_strong_sharding->get_phy_table_location_info() &&
          OB_FAIL(left_sharding.push_back(left_strong_sharding))) {
        LOG_WARN("failed to push back sharding info", K(ret));
      } else if (NULL != right_strong_sharding &&
                 NULL != right_strong_sharding->get_phy_table_location_info() &&
                 OB_FAIL(right_sharding.push_back(right_strong_sharding))) {
        LOG_WARN("failed to push back sharding info", K(ret));
      } else if (has_null_safe &&
                 OB_FAIL(check_if_match_partition_wise(equal_sets,
                                                       left_keys,
                                                       right_keys,
                                                       left_sharding,
                                                       right_sharding,
                                                       is_partition_wise))) {
        LOG_WARN("failed to check if match partition wise join", K(ret));
      } else if (is_partition_wise) {
        LOG_TRACE("succeed to check if match partition wise join", K(is_partition_wise));
      } else if (OB_FAIL(append(left_sharding, left_weak_sharding)) ||
                 OB_FAIL(append(right_sharding, right_weak_sharding))) {
        LOG_WARN("failed to append sharding info", K(ret));
      } else if (OB_FAIL(check_if_match_partition_wise(equal_sets,
                                                       strong_left_keys,
                                                       strong_right_keys,
                                                       left_sharding,
                                                       right_sharding,
                                                       is_partition_wise))) {
        LOG_WARN("failed to check if match partition wise join", K(ret));
      } else {
        LOG_TRACE("succeed to check if match partition wise join", K(is_partition_wise));
      }
    }
  }
  return ret;
}

int ObShardingInfo::check_if_match_extended_partition_wise(const EqualSets &equal_sets,
                                                           ObIArray<ObAddr> &left_server_list,
                                                           ObIArray<ObAddr> &right_server_list,
                                                           const common::ObIArray<ObRawExpr*> &left_keys,
                                                           const common::ObIArray<ObRawExpr*> &right_keys,
                                                           const common::ObIArray<bool> &null_safe_info,
                                                           ObShardingInfo *left_strong_sharding,
                                                           const common::ObIArray<ObShardingInfo *> &left_weak_sharding,
                                                           ObShardingInfo *right_strong_sharding,
                                                           const common::ObIArray<ObShardingInfo *> &right_weak_sharding,
                                                           bool &is_ext_partition_wise)
{
  int ret = OB_SUCCESS;
  is_ext_partition_wise = false;
  if (OB_UNLIKELY(left_keys.count() != null_safe_info.count()) ||
      OB_UNLIKELY(right_keys.count() != null_safe_info.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected array count", K(left_keys.count()), K(right_keys.count()),
        K(null_safe_info.count()), K(ret));
  } else {
    bool has_null_safe = false;
    ObSEArray<ObShardingInfo*, 8> left_sharding;
    ObSEArray<ObShardingInfo*, 8> right_sharding;
    ObSEArray<ObRawExpr*, 8> strong_left_keys;
    ObSEArray<ObRawExpr*, 8> strong_right_keys;
    for (int64_t i = 0; OB_SUCC(ret) && i < null_safe_info.count(); i++) {
      if (OB_ISNULL(left_keys.at(i)) || OB_ISNULL(right_keys.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(left_keys.at(i)), K(right_keys.at(i)), K(ret));
      } else if (null_safe_info.at(i)) {
        has_null_safe = true;
      } else if (OB_FAIL(strong_left_keys.push_back(left_keys.at(i))) ||
                 OB_FAIL(strong_right_keys.push_back(right_keys.at(i)))) {
        LOG_WARN("failed to push back keys", K(ret));
      } else { /*do nothing*/ }
    }
    if (OB_SUCC(ret)) {
      if (NULL != left_strong_sharding &&
          left_strong_sharding->is_distributed_without_table_location_with_partitioning() &&
          OB_FAIL(left_sharding.push_back(left_strong_sharding))) {
        LOG_WARN("failed to push back sharding info", K(ret));
      } else if (NULL != right_strong_sharding &&
                 right_strong_sharding->is_distributed_without_table_location_with_partitioning() &&
                 OB_FAIL(right_sharding.push_back(right_strong_sharding))) {
        LOG_WARN("failed to push back sharding info", K(ret));
      } else if (has_null_safe &&
                 OB_FAIL(check_if_match_extended_partition_wise(equal_sets,
                                                                left_server_list,
                                                                right_server_list,
                                                                left_keys,
                                                                right_keys,
                                                                left_sharding,
                                                                right_sharding,
                                                                is_ext_partition_wise))) {
        LOG_WARN("failed to check if match extended partition wise join", K(ret));
      } else if (is_ext_partition_wise) {
        LOG_TRACE("succeed to check if match extended partition wise join", K(is_ext_partition_wise));
      } else if (OB_FAIL(append(left_sharding, left_weak_sharding)) ||
                 OB_FAIL(append(right_sharding, right_weak_sharding))) {
        LOG_WARN("failed to append sharding info", K(ret));
      } else if (OB_FAIL(check_if_match_extended_partition_wise(equal_sets,
                                                                left_server_list,
                                                                right_server_list,
                                                                strong_left_keys,
                                                                strong_right_keys,
                                                                left_sharding,
                                                                right_sharding,
                                                                is_ext_partition_wise))) {
        LOG_WARN("failed to check if match extended partition wise join", K(ret));
      } else {
        LOG_TRACE("succeed to check if match extended partition wise join", K(is_ext_partition_wise));
      }
    }
  }
  return ret;
}

int ObShardingInfo::check_if_match_partition_wise(const EqualSets &equal_sets,
                                                  const common::ObIArray<ObRawExpr*> &left_keys,
                                                  const common::ObIArray<ObRawExpr*> &right_keys,
                                                  const common::ObIArray<ObShardingInfo *> &left_sharding,
                                                  const common::ObIArray<ObShardingInfo *> &right_sharding,
                                                  bool &is_partition_wise)
{
  int ret = OB_SUCCESS;
  bool is_key_covered = false;
  ObShardingInfo *first_left_sharding = NULL;
  ObShardingInfo *first_right_sharding = NULL;
  is_partition_wise = false;
  if (left_keys.empty() || right_keys.empty() || left_sharding.empty() || right_sharding.empty()) {
    is_partition_wise = false;
  } else if (OB_ISNULL(first_left_sharding = left_sharding.at(0)) ||
             OB_ISNULL(first_right_sharding = right_sharding.at(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(first_left_sharding), K(first_right_sharding), K(ret));
  } else if (!first_left_sharding->is_distributed() || !first_right_sharding->is_distributed()) {
    is_partition_wise = false;
  } else if (first_left_sharding->part_level_ != first_right_sharding->part_level_ ||
             first_left_sharding->is_partition_single_ != first_right_sharding->is_partition_single_ ||
             first_left_sharding->is_subpartition_sinlge_ != first_right_sharding->is_subpartition_sinlge_) {
    is_partition_wise = false;
  } else if (OB_FAIL(is_join_key_cover_partition_key(equal_sets,
                                                     left_keys,
                                                     left_sharding,
                                                     right_keys,
                                                     right_sharding,
                                                     is_key_covered))) {
    LOG_WARN("failed to check is join key cover partition key", K(ret));
  } else if (!is_key_covered) {
    /*do nothing*/
  } else {
    bool is_equal = false;
    PwjTable l_table;
    PwjTable r_table;
    SMART_VAR(ObStrictPwjComparer, pwj_comparer) {
      if (OB_FAIL(l_table.init(*first_left_sharding))) {
        LOG_WARN("failed to init pwj table with sharding info", K(ret));
      } else if (OB_FAIL(r_table.init(*first_right_sharding))) {
        LOG_WARN("failed to init pwj table with sharding info", K(ret));
      } else if (OB_FAIL(pwj_comparer.add_table(l_table, is_equal))) {
        LOG_WARN("failed to add table", K(ret));
      } else if (!is_equal) {
        // do nothing
      } else if (OB_FAIL(pwj_comparer.add_table(r_table, is_equal))) {
        LOG_WARN("failed to add table", K(ret));
      } else if (is_equal){
        is_partition_wise = true;
      }
    }
  }
  LOG_TRACE("succeed check if match partition wise",
      K(left_sharding), K(right_sharding), K(is_partition_wise), K(is_key_covered));
  return ret;
}

int ObShardingInfo::check_if_match_extended_partition_wise(const EqualSets &equal_sets,
                                                           ObIArray<ObAddr> &left_server_list,
                                                           ObIArray<ObAddr> &right_server_list,
                                                           const common::ObIArray<ObRawExpr*> &left_keys,
                                                           const common::ObIArray<ObRawExpr*> &right_keys,
                                                           const common::ObIArray<ObShardingInfo *> &left_sharding,
                                                           const common::ObIArray<ObShardingInfo *> &right_sharding,
                                                           bool &is_ext_partition_wise)
{
  int ret = OB_SUCCESS;
  bool is_key_covered = false;
  ObShardingInfo *first_left_sharding = NULL;
  ObShardingInfo *first_right_sharding = NULL;
  is_ext_partition_wise = false;
  if (left_keys.empty() || right_keys.empty() || left_sharding.empty() || right_sharding.empty()) {
    is_ext_partition_wise = false;
  } else if (OB_ISNULL(first_left_sharding = left_sharding.at(0)) ||
             OB_ISNULL(first_right_sharding = right_sharding.at(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(first_left_sharding), K(first_right_sharding), K(ret));
  } else if (!first_left_sharding->is_distributed() || !first_right_sharding->is_distributed()) {
    is_ext_partition_wise = false;
  } else if (!first_left_sharding->is_distributed_without_table_location_with_partitioning() ||
             !first_right_sharding->is_distributed_without_table_location_with_partitioning()) {
    is_ext_partition_wise = false;
  } else if (OB_FAIL(is_join_key_cover_partition_key(equal_sets,
                                                     left_keys,
                                                     left_sharding,
                                                     right_keys,
                                                     right_sharding,
                                                     is_key_covered))) {
    LOG_WARN("failed to check is join key cover partition key", K(ret));
  } else if (!is_key_covered) {
    /*do nothing*/
  } else {
    bool is_equal = false;
    PwjTable l_table;
    PwjTable r_table;
    ObNonStrictPwjComparer pwj_comparer;
    if (OB_FAIL(l_table.init(left_server_list))) {
      LOG_WARN("failed to init pwj table with sharding info", K(ret));
    } else if (OB_FAIL(r_table.init(right_server_list))) {
      LOG_WARN("failed to init pwj table with sharding info", K(ret));
    } else if (OB_FAIL(pwj_comparer.add_table(l_table, is_equal))) {
      LOG_WARN("failed to add table", K(ret));
    } else if (!is_equal) {
      // do nothing
    } else if (OB_FAIL(pwj_comparer.add_table(r_table, is_equal))) {
      LOG_WARN("failed to add table", K(ret));
    } else if (is_equal) {
      is_ext_partition_wise = true;
    }
  }
  LOG_TRACE("succeed check if match extended partition wise",
      K(left_sharding), K(right_sharding), K(is_ext_partition_wise));
  return ret;
}

int ObShardingInfo::check_if_match_repart_or_rehash(const EqualSets &equal_sets,
                                                    const ObIArray<ObRawExpr *> &src_keys,
                                                    const ObIArray<ObRawExpr *> &target_keys,
                                                    const ObIArray<ObRawExpr *> &target_part_keys,
                                                    bool &is_match_join_keys)
{
  int ret = OB_SUCCESS;
  is_match_join_keys = false;
  if (OB_UNLIKELY(src_keys.count() != target_keys.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected array count", K(src_keys.count()),
        K(target_keys.count()), K(ret));
  } else if (0 == target_part_keys.count()) {
    is_match_join_keys = false;
  } else {
    is_match_join_keys = true;
    for (int64_t i = 0; OB_SUCC(ret) && is_match_join_keys && i < target_part_keys.count(); ++i) {
      ObRawExpr* right_part_key = NULL;
      if (OB_ISNULL(right_part_key = target_part_keys.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(right_part_key), K(ret));
      } else {
        bool is_found = false;
        for (int64_t j = 0; OB_SUCC(ret) && !is_found && j < target_keys.count(); ++j) {
          ObRawExpr* left_key = NULL;
          ObRawExpr* right_key = NULL;
          if (OB_ISNULL(left_key = src_keys.at(j)) ||
              OB_ISNULL(right_key = target_keys.at(j))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("get unexpected null", K(left_key), K(right_key), K(ret));
          } else if (ObOptimizerUtil::is_expr_equivalent(right_part_key, right_key, equal_sets)
              && right_part_key->get_result_type().get_type_class()
                  == left_key->get_result_type().get_type_class()
              && right_part_key->get_result_type().get_collation_type()
                  == left_key->get_result_type().get_collation_type()
              && !ObObjCmpFuncs::is_otimestamp_cmp(right_part_key->get_result_type().get_type(),
                  left_key->get_result_type().get_type())
              && !ObObjCmpFuncs::is_datetime_timestamp_cmp(
                  right_part_key->get_result_type().get_type(),
                  left_key->get_result_type().get_type())) {
            is_found = true;
          } else { /*do nothing*/ }
        }
        if (OB_SUCC(ret) && !is_found) {
          is_match_join_keys = false;
        }
      }
    }
  }
  return ret;
}

int ObShardingInfo::is_physically_equal_serverlist(ObIArray<ObAddr> &left_server_list,
                                                   ObIArray<ObAddr> &right_server_list,
                                                   bool &is_equal_serverlist)
{
  int ret = OB_SUCCESS;
  is_equal_serverlist = false;
  if (left_server_list.empty() || right_server_list.empty()) {
    // do nothing
  } else if (left_server_list.count() != right_server_list.count()) {
    // do nothing
  } else if (is_shuffled_server_list(left_server_list) ||
             is_shuffled_server_list(right_server_list)) {
    // do nothing
  } else {
    is_equal_serverlist = true;
    lib::ob_sort(&left_server_list.at(0), &left_server_list.at(0) + left_server_list.count());
    lib::ob_sort(&right_server_list.at(0), &right_server_list.at(0) + right_server_list.count());
    for (int64_t i = 0; OB_SUCC(ret) && is_equal_serverlist && i < left_server_list.count(); i ++) {
      if (left_server_list.at(i) != right_server_list.at(i)) {
        is_equal_serverlist = false;
      }
    }
  }
  return ret;
}

int ObShardingInfo::is_physically_both_shuffled_serverlist(ObIArray<ObAddr> &left_server_list,
                                                            ObIArray<ObAddr> &right_server_list,
                                                            bool &is_both_shuffled_serverlist)
{
  int ret = OB_SUCCESS;
  is_both_shuffled_serverlist = false;
  if (left_server_list.empty() || right_server_list.empty()) {
    // do nothing
  } else if (left_server_list.count() != right_server_list.count()) {
    // do nothing
  } else if (is_shuffled_server_list(left_server_list) &&
             is_shuffled_server_list(right_server_list)) {
    // mark as non-strict pw for shuffle case
    is_both_shuffled_serverlist = true;
  } else {
    // do nothing
  }
  return ret;
}

bool ObShardingInfo::is_shuffled_server_list(const ObIArray<ObAddr> &server_list)
{
  return server_list.count() == 1 && is_shuffled_addr(server_list.at(0));
}

int ObShardingInfo::copy_with_part_keys(const ObShardingInfo &other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(copy_without_part_keys(other))) {
  } else if (OB_FAIL(partition_keys_.assign(other.get_partition_keys()))) {
    LOG_WARN("failed to append partition key", K(ret));
  } else if (OB_FAIL(sub_partition_keys_.assign(other.get_sub_partition_keys()))) {
    LOG_WARN("failed to append sub partition key", K(ret));
  } else if (OB_FAIL(part_func_exprs_.assign(other.get_partition_func()))) {
    LOG_WARN("Failed to append partition function", K(ret));
  } else {/*do nothing*/}
  return ret;
}

int ObShardingInfo::copy_without_part_keys(const ObShardingInfo &other)
{
  int ret = OB_SUCCESS;
  part_level_ = other.part_level_;
  part_func_type_ = other.part_func_type_;
  subpart_func_type_ = other.subpart_func_type_;
  part_num_ = other.part_num_;
  location_type_ = other.location_type_;
  phy_table_location_info_ = other.phy_table_location_info_;
  partition_array_ = other.partition_array_;
  can_reselect_replica_ = other.can_reselect_replica_;
  is_partition_single_ = other.is_partition_single_;
  is_subpartition_sinlge_ = other.is_subpartition_sinlge_;
  if (OB_FAIL(all_tablet_ids_.assign(other.all_tablet_ids_))) {
    LOG_WARN("failed to assign partition ids");
  } else if (OB_FAIL(all_partition_indexes_.assign(other.all_partition_indexes_))) {
    LOG_WARN("failed to assign used partitions", K(ret));
  } else if (OB_FAIL(all_subpartition_indexes_.assign(other.all_subpartition_indexes_))) {
    LOG_WARN("failed to assign used subpartitions", K(ret));
  }
  return ret;
}

int ObShardingInfo::get_all_partition_keys(common::ObIArray<ObRawExpr*> &out_part_keys,
                                           bool ignore_single_partition/* = false */) const
{
  int ret = OB_SUCCESS;
  if (!(ignore_single_partition && is_partition_single()) &&
      OB_FAIL(out_part_keys.assign(partition_keys_))) {
    LOG_WARN("failed to assign array", K(ret));
  } else if (!(ignore_single_partition && is_subpartition_single()) &&
             append(out_part_keys, sub_partition_keys_)) {
    LOG_WARN("failed to append array", K(ret));
  } else {/*do nothing*/}
  return ret;
}

int ObShardingInfo::get_remote_addr(ObAddr &remote) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_remote()) || OB_ISNULL(phy_table_location_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(location_type_), K(phy_table_location_info_), K(ret));
  } else {
    share::ObLSReplicaLocation replica_loc;
    const ObCandiTabletLocIArray &phy_partition_loc =
        phy_table_location_info_->get_phy_part_loc_info_list();
    if (OB_UNLIKELY(1 != phy_partition_loc.count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected partition count", K(phy_partition_loc.count()), K(ret));
    } else if (OB_FAIL(phy_partition_loc.at(0).get_selected_replica(replica_loc))) {
      LOG_WARN("fail to get selected replica", K(ret), K(phy_partition_loc.at(0)));
    } else if (!replica_loc.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("replica location is invalid", K(ret));
    } else {
      remote = replica_loc.get_server();
    }
  }
  return ret;
}

int ObShardingInfo::get_total_part_cnt(int64_t &total_part_cnt) const
{
  int ret = OB_SUCCESS;
  total_part_cnt = 1;
  if (share::schema::PARTITION_LEVEL_ONE == part_level_) {
    total_part_cnt = part_num_;
  } else if  (share::schema::PARTITION_LEVEL_TWO == part_level_) {
    total_part_cnt = 0;
    ObPartition *cur_part = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && i < part_num_; ++i) {
      if (OB_ISNULL(cur_part = partition_array_[i])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else {
        total_part_cnt += cur_part->get_sub_part_num();
      }
    }
  }
  return ret;
}

int ObShardingInfo::is_sharding_equal(const ObShardingInfo *left_strong_sharding,
                                      const ObIArray<ObShardingInfo*> &left_weak_shardings,
                                      const ObShardingInfo *right_strong_sharding,
                                      const ObIArray<ObShardingInfo*> &right_weak_shardings,
                                      const EqualSets &equal_sets,
                                      bool &is_equal)
{
  int ret = OB_SUCCESS;
  is_equal = false;
  if (OB_FAIL(is_sharding_equal(left_strong_sharding,
                                right_strong_sharding,
                                equal_sets,
                                is_equal))) {
    LOG_WARN("failed to check if sharding is equal", K(ret));
  } else if (!is_equal) {
    /*do nothing*/
  } else if (OB_FAIL(is_sharding_equal(left_weak_shardings,
                                       right_weak_shardings,
                                       equal_sets,
                                       is_equal))) {
    LOG_WARN("failed to check if sharding is equal", K(ret));
  } else { /*do nothing*/ }
  return ret;
}

int ObShardingInfo::is_sharding_equal(const ObIArray<ObShardingInfo*> &left_sharding,
                                      const ObIArray<ObShardingInfo*> &right_sharding,
                                      const EqualSets &equal_sets,
                                      bool &is_equal)
{
  int ret = OB_SUCCESS;
  bool is_left_subset = false;
  bool is_right_subset = false;
  is_equal = false;
  if (OB_FAIL(is_subset_sharding(left_sharding,
                                 right_sharding,
                                 equal_sets,
                                 is_left_subset))) {
    LOG_WARN("failed to check is subset sharding", K(ret));
  } else if (OB_FAIL(is_subset_sharding(right_sharding,
                                        left_sharding,
                                        equal_sets,
                                        is_right_subset))) {
    LOG_WARN("failed to check is subset sharding", K(ret));
  } else {
    is_equal = is_left_subset && is_right_subset;
  }
  return ret;
}

int ObShardingInfo::is_subset_sharding(const ObIArray<ObShardingInfo*> &subset_sharding,
                                       const ObIArray<ObShardingInfo*> &target_sharding,
                                       const EqualSets &equal_sets,
                                       bool &is_subset)
{
  int ret = OB_SUCCESS;
  is_subset = true;
  for (int64_t i = 0; OB_SUCC(ret) && is_subset && i < subset_sharding.count(); i++) {
    bool is_equal = false;
    for (int64_t j = 0; OB_SUCC(ret) && !is_equal && j < target_sharding.count(); j++) {
      if (OB_FAIL(is_sharding_equal(subset_sharding.at(i),
                                    target_sharding.at(j),
                                    equal_sets,
                                    is_equal))) {
        LOG_WARN("failed to check sharding is equal", K(ret));
      } else { /*do nothing*/ }
    }
    if (OB_SUCC(ret) && !is_equal) {
      is_subset = false;
    }
  }
  return ret;
}

int ObShardingInfo::is_sharding_equal(const ObShardingInfo *left_sharding,
                                      const ObShardingInfo *right_sharding,
                                      const EqualSets &equal_sets,
                                      bool &is_equal)
{
  int ret = OB_SUCCESS;
  is_equal = false;
  if (left_sharding == right_sharding) {
    is_equal = true;
  } else if (NULL == left_sharding || NULL == right_sharding) {
    is_equal = false;
  } else if (left_sharding->get_location_type() != right_sharding->get_location_type()) {
    is_equal = false;
  } else if (left_sharding->get_can_reselect_replica() != right_sharding->get_can_reselect_replica()) {
    is_equal = false;
  } else if (left_sharding->is_remote() || left_sharding->get_can_reselect_replica()) {
    ObSEArray<common::ObAddr, 2> left_servers;
    ObSEArray<common::ObAddr, 2> right_servers;
    if (OB_FAIL(get_serverlist_from_sharding(*left_sharding, left_servers))) {
      LOG_WARN("failed to get server list from left sharding", K(ret));
    } else if (OB_FAIL(get_serverlist_from_sharding(*right_sharding, right_servers))) {
      LOG_WARN("failed to get server list from right sharding", K(ret));
    } else if (OB_FAIL(is_physically_equal_serverlist(left_servers,
                                                      right_servers,
                                                      is_equal))) {
      LOG_WARN("failed to check equal server list", K(ret));
    } else { /*do nothing*/ }
  } else if (left_sharding->is_match_all() || left_sharding->is_local()) {
    is_equal = true;
  } else if (!ObOptimizerUtil::same_exprs(left_sharding->get_partition_keys(),
                                          right_sharding->get_partition_keys(),
                                          equal_sets) ||
             !ObOptimizerUtil::same_exprs(left_sharding->get_sub_partition_keys(),
                                          right_sharding->get_sub_partition_keys(),
                                          equal_sets) ||
             !ObOptimizerUtil::same_exprs(left_sharding->get_partition_func(),
                                          right_sharding->get_partition_func(),
                                          equal_sets)) {
    is_equal = false;
  } else if (NULL == left_sharding->get_phy_table_location_info() &&
             NULL == right_sharding->get_phy_table_location_info()) {
    is_equal = true;
  } else if (OB_FAIL(is_compatible_partition_key(*left_sharding, *right_sharding, is_equal))) {
    LOG_WARN("failed to check if is comptiable keys", K(ret));
  } else if (!is_equal) {
    /* do nothing */
  } else  {
    PwjTable l_table;
    PwjTable r_table;
    SMART_VAR(ObStrictPwjComparer, pwj_comparer) {
      if (OB_FAIL(l_table.init(*left_sharding))) {
        LOG_WARN("failed to init pwj table with sharding info", K(ret));
      } else if (OB_FAIL(r_table.init(*right_sharding))) {
        LOG_WARN("failed to init pwj table with sharding info", K(ret));
      } else if (OB_FAIL(pwj_comparer.add_table(l_table, is_equal))) {
        LOG_WARN("failed to add table", K(ret));
      } else if (!is_equal) {
        // do nothing
      } else if (OB_FAIL(pwj_comparer.add_table(r_table, is_equal))) {
        LOG_WARN("failed to add table", K(ret));
      } else { /*do nothing*/ }
    }
  }
  if (OB_SUCC(ret)) {
    LOG_TRACE("succeed to check whether sharding info is equal", K(is_equal));
  }
  return ret;
}

int ObShardingInfo::get_serverlist_from_sharding(const ObShardingInfo &sharding,
                                                 ObIArray<common::ObAddr> &server_list)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(sharding.phy_table_location_info_)) {
    // do nothing
  } else {
    const ObCandiTabletLocIArray &locations = sharding.phy_table_location_info_->get_phy_part_loc_info_list();
    for (int64_t i = 0; OB_SUCC(ret) && i < locations.count(); ++i) {
      share::ObLSReplicaLocation replica_loc;
      if (OB_FAIL(locations.at(i).get_selected_replica(replica_loc))) {
        LOG_WARN("fail to get selected replica", K(ret), K(locations.at(i)));
      } else if (!replica_loc.is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("replica location is invalid", K(ret), K(i), K(replica_loc));
      } else if (OB_FAIL(server_list.push_back(replica_loc.get_server()))) {
        LOG_WARN("failed to push back server addr", K(ret));
      }
    }
  }
  return ret;
}
