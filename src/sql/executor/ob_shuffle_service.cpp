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

#define USING_LOG_PREFIX SQL_EXE
#include "ob_shuffle_service.h"
#include "common/row/ob_row.h"
#include "share/schema/ob_table_schema.h"
#include "share/schema/ob_schema_struct.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/expr/ob_expr_func_part_hash.h"

using namespace oceanbase::common;
using namespace oceanbase::share::schema;
using namespace oceanbase::observer;
namespace oceanbase {
namespace sql {

int ObShuffleService::get_partition_ids(ObExecContext& exec_ctx, const share::schema::ObTableSchema& table_schema,
    const common::ObNewRow& row, const ObSqlExpression& part_func, const ObSqlExpression& subpart_func,
    const ObIArray<ObTransmitRepartColumn>& repart_columns, const ObIArray<ObTransmitRepartColumn>& repart_sub_columns,
    int64_t& part_id, int64_t& subpart_id, bool& no_match_partiton)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(init_expr_ctx(exec_ctx))) {
    LOG_WARN("Failed to init expr calculation context", K(ret));
  } else if (OB_FAIL(get_part_id(exec_ctx, table_schema, row, part_func, repart_columns, part_id))) {
    LOG_WARN("failed to get part id", K(ret));
  } else if (PARTITION_LEVEL_TWO != table_schema.get_part_level() || NO_MATCH_PARTITION == part_id) {
    // do nothing
  } else if (OB_FAIL(
                 get_subpart_id(exec_ctx, table_schema, row, part_id, subpart_func, repart_sub_columns, subpart_id))) {
    LOG_WARN("failed to get subpart id", K(ret));
  }
  if (OB_SUCC(ret)) {
    if (part_id == NO_MATCH_PARTITION || subpart_id == NO_MATCH_PARTITION) {
      no_match_partiton = true;
    }
  }
  LOG_DEBUG("get part id and subpart id", K(part_id), K(subpart_id), K(no_match_partiton));
  return ret;
}

int ObShuffleService::get_partition_ids(ObExecContext& exec_ctx, const share::schema::ObTableSchema& table_schema,
    const common::ObNewRow& row, const ObSqlExpression& part_func, const ObSqlExpression& subpart_func,
    const ObIArray<ObTransmitRepartColumn>& repart_columns, const ObIArray<ObTransmitRepartColumn>& repart_sub_columns,
    const ObPxPartChMap& ch_map, int64_t& part_id, int64_t& subpart_id, bool& no_match_partiton,
    ObRepartitionType part_type)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(init_expr_ctx(exec_ctx))) {
    LOG_WARN("Failed to init expr calculation context", K(ret));
  } else if (OB_REPARTITION_ONE_SIDE_ONE_LEVEL_SUB == part_type &&
             OB_FAIL(ObShuffleService::get_part_id_by_ch_map(ch_map, part_id))) {
    LOG_WARN("fail to get part id", K(ret));
  } else if (OB_REPARTITION_ONE_SIDE_ONE_LEVEL_SUB != part_type &&
             OB_FAIL(get_part_id(exec_ctx, table_schema, row, part_func, repart_columns, part_id))) {
    LOG_WARN("failed to get part id", K(ret));
  } else if (PARTITION_LEVEL_TWO != table_schema.get_part_level() || NO_MATCH_PARTITION == part_id) {
    // do nothing
  } else if (OB_REPARTITION_ONE_SIDE_ONE_LEVEL_FIRST == part_type &&
             OB_FAIL(ObShuffleService::get_sub_part_id_by_ch_map(ch_map, part_id, subpart_id))) {
    LOG_WARN("fail to get sub part id", K(ret));
  } else if (OB_REPARTITION_ONE_SIDE_ONE_LEVEL_FIRST != part_type &&
             OB_FAIL(
                 get_subpart_id(exec_ctx, table_schema, row, part_id, subpart_func, repart_sub_columns, subpart_id))) {
    LOG_WARN("failed to get subpart id", K(ret));
  }
  if (OB_SUCC(ret)) {
    if (part_id == NO_MATCH_PARTITION || subpart_id == NO_MATCH_PARTITION) {
      no_match_partiton = true;
    }
  }
  LOG_DEBUG("get part id and subpart id", K(part_id), K(subpart_id), K(no_match_partiton));
  return ret;
}

int ObShuffleService::get_part_id(ObExecContext& exec_ctx, const share::schema::ObTableSchema& table_schema,
    const common::ObNewRow& row, const ObSqlExpression& part_func,
    const ObIArray<ObTransmitRepartColumn>& repart_columns, int64_t& part_id)
{
  int ret = OB_SUCCESS;
  if (table_schema.is_key_part()) {
    if (OB_FAIL(get_key_part_id(exec_ctx, table_schema, row, part_func, part_id))) {
      LOG_WARN("get key part id failed");
    }
  } else if (table_schema.is_list_part() || table_schema.is_range_part() || table_schema.is_hash_part()) {
    if (OB_FAIL(get_non_key_partition_part_id(exec_ctx, table_schema, row, repart_columns, part_id))) {
      LOG_WARN("failed to get non key partition part id", K(ret));
    }
  } else {
    ret = OB_NOT_IMPLEMENT;
    LOG_WARN("this type of partition is not implement", K(ret));
  }
  return ret;
}

int ObShuffleService::get_key_part_id(ObExecContext& exec_ctx, const share::schema::ObTableSchema& table_schema,
    const common::ObNewRow& row, const ObSqlExpression& part_func, int64_t& part_id)
{
  int ret = OB_SUCCESS;
  UNUSED(exec_ctx);
  int64_t calc_result = 0;
  ObObj func_result;
  int64_t part_count = table_schema.get_part_option().get_part_num();
  if (part_count <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the part num can not be null", K(part_count), K(part_func), K(ret));
  } else if (OB_FAIL(part_func.calc(expr_ctx_, row, func_result))) {
    LOG_WARN("Failed to calc hash expr", K(ret), K(row));
  } else if (OB_FAIL(func_result.get_int(calc_result))) {
    LOG_WARN("Fail to get int64 from result", K(func_result), K(ret));
  } else if (calc_result < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Input arguments is invalid", K(calc_result), K(part_count), K(ret));
  } else if (OB_FAIL(ObPartitionUtils::calc_hash_part_idx(calc_result, part_count, part_id))) {
    LOG_WARN("calc_hash_part_idx failed", K(ret));
  }
  return ret;
}

int ObShuffleService::get_non_key_partition_part_id(ObExecContext& exec_ctx, const ObTableSchema& table_schema,
    const ObNewRow& row, const ObIArray<ObTransmitRepartColumn>& repart_columns, int64_t& part_id)
{
  int ret = OB_SUCCESS;
  ObNewRow part_row;
  ObSEArray<int64_t, 1> part_ids;
  bool is_hash_v2 = (PARTITION_FUNC_TYPE_HASH_V2 == table_schema.get_part_option().get_part_func_type());
  if (OB_FAIL(get_repart_row(exec_ctx, row, repart_columns, part_row, table_schema.is_hash_part(), is_hash_v2))) {
    LOG_WARN("fail to get part and subpart obj idxs", K(ret));
  } else {
    if (OB_FAIL(table_schema.get_part(part_row, part_ids))) {
      LOG_WARN("get part idxs from table schema error", K(ret));
    } else if (0 == part_ids.count()) {
      part_id = NO_MATCH_PARTITION;
    } else if (1 != part_ids.count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("the part idxs is invalid", K(ret), K(part_ids.count()));
    } else {
      part_id = part_ids.at(0);
    }
    if (OB_NOT_NULL(part_row.cells_)) {
      exec_ctx.get_allocator().free(part_row.cells_);
      part_row.cells_ = NULL;
    }
  }
  return ret;
}

int ObShuffleService::get_subpart_id(ObExecContext& exec_ctx, const share::schema::ObTableSchema& table_schema,
    const common::ObNewRow& row, int64_t part_id, const ObSqlExpression& subpart_func,
    const ObIArray<ObTransmitRepartColumn>& repart_sub_columns, int64_t& subpart_id)
{
  int ret = OB_SUCCESS;
  if (table_schema.is_key_subpart()) {
    if (OB_FAIL(get_key_subpart_id(exec_ctx, table_schema, row, part_id, subpart_func, subpart_id))) {
      LOG_WARN("get key subpart id failed");
    }
  } else if (table_schema.is_list_subpart() || table_schema.is_range_subpart() || table_schema.is_hash_subpart()) {
    if (OB_FAIL(get_non_key_subpart_id(exec_ctx, table_schema, row, part_id, repart_sub_columns, subpart_id))) {
      LOG_WARN("get range or hash subpart id failed");
    }
  } else {
    ret = OB_NOT_IMPLEMENT;
    LOG_WARN("we only support the range, range column, key, hash repartition exe", K(ret));
  }
  return ret;
}

int ObShuffleService::get_key_subpart_id(ObExecContext& exec_ctx, const ObTableSchema& table_schema,
    const ObNewRow& row, int64_t part_id, const ObSqlExpression& subpart_func, int64_t& subpart_id)
{
  int ret = OB_SUCCESS;
  UNUSED(exec_ctx);
  int64_t sub_calc_result = 0;
  ObObj sub_func_result;
  int64_t sub_part_num = 0;
  const ObPartition* partition = nullptr;
  if (PARTITION_LEVEL_TWO != table_schema.get_part_level()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("should not call this func without sub partition", K(ret), "level", table_schema.get_part_level());
    // do nothing
  } else if (OB_FAIL(table_schema.get_partition_by_part_id(part_id, false, partition))) {
    LOG_WARN("fail get partition", K(ret), K(part_id));
  } else if (OB_ISNULL(partition)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("partition should exist", K(sub_part_num), K(part_id), K(ret));
  } else if ((sub_part_num = partition->get_sub_part_num()) <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the part num can not be null", K(sub_part_num), K(subpart_func));
  } else if (OB_FAIL(subpart_func.calc(expr_ctx_, row, sub_func_result))) {
    LOG_WARN("Failed to calc hash expr", K(ret), K(row));
  } else if (OB_FAIL(sub_func_result.get_int(sub_calc_result))) {
    LOG_WARN("Fail to get int64 from result", K(sub_func_result), K(ret));
  } else if (sub_calc_result < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Input arguments is invalid", K(sub_calc_result), K(ret));
  } else {
    subpart_id = sub_calc_result % sub_part_num;
  }
  return ret;
}

int ObShuffleService::get_non_key_subpart_id(ObExecContext& exec_ctx, const ObTableSchema& table_schema,
    const ObNewRow& row, int64_t part_id, const ObIArray<ObTransmitRepartColumn>& repart_sub_columns,
    int64_t& subpart_id)
{
  int ret = OB_SUCCESS;
  ObNewRow subpart_row;
  ObSEArray<int64_t, 1> subpart_ids;
  bool is_hash_v2 = (PARTITION_FUNC_TYPE_HASH_V2 == table_schema.get_sub_part_option().get_sub_part_func_type());
  if (OB_FAIL(
          get_repart_row(exec_ctx, row, repart_sub_columns, subpart_row, table_schema.is_hash_subpart(), is_hash_v2))) {
    LOG_WARN("fail to get part and subpart obj idxs", K(ret));
  } else {
    if (OB_FAIL(table_schema.get_subpart(part_id, subpart_row, subpart_ids))) {
      LOG_WARN("get part idxs from table schema error", K(part_id), K(ret));
    } else if (0 == subpart_ids.count()) {
      subpart_id = NO_MATCH_PARTITION;
    } else if (1 != subpart_ids.count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("the part idxs is invalid", K(ret), K(subpart_ids.count()));
    } else {
      subpart_id = subpart_ids.at(0);
    }
    if (OB_NOT_NULL(subpart_row.cells_)) {
      exec_ctx.get_allocator().free(subpart_row.cells_);
      subpart_row.cells_ = NULL;
    }
  }
  return ret;
}

int ObShuffleService::get_repart_row(ObExecContext& exec_ctx, const common::ObNewRow& in_row,
    const ObIArray<ObTransmitRepartColumn>& repart_columns, common::ObNewRow& out_row, bool hash_part, bool is_hash_v2)
{
  int ret = OB_SUCCESS;
  UNUSED(exec_ctx);
  if (current_cell_count_ < repart_columns.count()) {
    current_cell_count_ = repart_columns.count();
    row_cache_.cells_ = static_cast<ObObj*>(allocator_.alloc(sizeof(ObObj) * current_cell_count_));
  }
  row_cache_.count_ = repart_columns.count();
  if (OB_ISNULL(row_cache_.cells_)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc mem failed", K(ret));
  } else {
    for (int64_t i = 0; i < repart_columns.count() && OB_SUCC(ret); ++i) {
      int64_t idx = (repart_columns.at(i).index_);
      row_cache_.cells_[i] = in_row.get_cell(idx);
    }
    if (OB_SUCC(ret)) {
      out_row = row_cache_;
    }
  }
  if (OB_SUCC(ret) && hash_part) {
    ObObj result;
    if (is_hash_v2 && OB_FAIL(ObExprFuncPartHash::calc_value(expr_ctx_, out_row.cells_, out_row.count_, result))) {
      LOG_WARN("Failed to calc hash value", K(ret));
    } else if (!is_hash_v2 &&
               OB_FAIL(ObExprFuncPartOldHash::calc_value(expr_ctx_, out_row.cells_, out_row.count_, result))) {
      LOG_WARN("Failed to calc hash value", K(ret));
    } else {
      out_row.cells_[0] = result;
      out_row.count_ = 1;
    }
  }
  return ret;
}

int ObShuffleService::init_expr_ctx(ObExecContext& exec_ctx)
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo* my_session = NULL;
  const ObTimeZoneInfo* tz_info = NULL;
  int64_t tz_offset = 0;
  if (nullptr != expr_ctx_.exec_ctx_) {
    // Has been inited, do nothing.
  } else if (OB_ISNULL(my_session = exec_ctx.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is NULL", K(ret));
  } else if (OB_ISNULL(tz_info = get_timezone_info(my_session))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get tz info pointer failed", K(ret));
  } else if (OB_FAIL(get_tz_offset(tz_info, tz_offset))) {
    LOG_WARN("get tz offset failed", K(ret));
  } else {
    expr_ctx_.cast_mode_ = CM_WARN_ON_FAIL;
    expr_ctx_.exec_ctx_ = &exec_ctx;
    expr_ctx_.calc_buf_ = &exec_ctx.get_allocator();
    expr_ctx_.phy_plan_ctx_ = exec_ctx.get_physical_plan_ctx();
    expr_ctx_.my_session_ = my_session;
    expr_ctx_.tz_offset_ = tz_offset;
    EXPR_SET_CAST_CTX_MODE(expr_ctx_);
  }
  return ret;
}

int ObShuffleService::get_hash_part_id(
    const ObNewRow& row, const int64_t part_num, ObIArray<int64_t>& part_ids, const PartIdx2PartIdMap& part_map)
{

  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(row.is_invalid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("row is invalid", K(ret), K(row));
  } else if (1 != row.get_count() || (!row.get_cell(0).is_int() && !row.get_cell(0).is_null())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("row is invalid", K(ret), K(row));
  } else {
    int64_t value = row.get_cell(0).is_int() ? row.get_cell(0).get_int() : 0;
    int64_t part_idx = -1;
    int64_t part_id = -1;
    if (value < 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("value should not be less than 0", K(ret), K(row));
    } else if (OB_FAIL(ObPartitionUtils::calc_hash_part_idx(value, part_num, part_idx))) {
      LOG_WARN("fail to calc hash part idx", K(ret));
    } else if (OB_FAIL(part_map.get_refactored(part_idx, part_id))) {
      LOG_WARN("fail to get part id by idx", K(ret), K(part_idx));
    } else if (OB_FAIL(part_ids.push_back(part_id))) {
      LOG_WARN("fail to add part id", K(ret));
    } else {
      // do nothing
    }
  }
  return ret;
}

int ObShuffleService::get_hash_subpart_id(const common::ObNewRow& row, const int64_t subpart_num,
    common::ObIArray<int64_t>& subpart_ids, const SubPartIdx2SubPartIdMap& subpart_map)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(row.is_invalid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("row is invalid", K(ret), K(row));
  } else if (1 != row.get_count() || (!row.get_cell(0).is_int() && !row.get_cell(0).is_null())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("row is invalid", K(ret), K(row));
  } else {
    int64_t value = row.get_cell(0).is_int() ? row.get_cell(0).get_int() : 0;
    int64_t subpart_idx = -1;
    int64_t subpart_id = -1;
    if (value < 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("value should not be less than 0", K(ret), K(row));
    } else if (OB_FAIL(ObPartitionUtils::calc_hash_part_idx(value, subpart_num, subpart_idx))) {
      LOG_WARN("fail to calc hash subpart idx", K(ret), K(value), K(subpart_num));
    } else if (OB_FAIL(subpart_map.get_refactored(subpart_idx, subpart_id))) {
      LOG_WARN("fail to get subpart id by idx", K(ret), K(subpart_idx));
    } else if (OB_FAIL(subpart_ids.push_back(subpart_id))) {
      LOG_WARN("fail to push back subpart id to array", K(ret));
    } else {
      // do nothing
    }
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
