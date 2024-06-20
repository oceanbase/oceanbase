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
namespace oceanbase
{
namespace sql
{

// 问：为什么 ObShuffleService 里，key 分区的处理总是走一个单独路径？
//
// 答：实现原因。理论上二者可以统一。
//     2017年本逻辑的作者没有努力把二者融合起来，做了两段分支逻辑，导致了现在的局面。
//
//     key 分区依赖于 key 表达式对多列做计算，然后对计算结果取模，
//     hash 分区对一个 hash function 做计算，然后对计算结果取模。
//
//     part_func, subpart_func 这两个参数是专门给 key 分区使用的，用他们计算出
//     一个值，然后再调用 hash 函数计算出最终的 part id。
//
//     对于 hash 分区，只需要传入 repart_columns, repart_sub_columns 即可，
//     其计算 hash 值的表达式是固定的函数，函数的参数就是 repart_columns 指定
//
//


int ObShuffleService::get_partition_ids(ObExecContext &exec_ctx,
                                        const share::schema::ObTableSchema &table_schema,
                                        const common::ObNewRow &row,
                                        const ObSqlExpression &part_func,
                                        const ObSqlExpression &subpart_func,
                                        const ObIArray<ObTransmitRepartColumn> &repart_columns,
                                        const ObIArray<ObTransmitRepartColumn> &repart_sub_columns,
                                        int64_t &part_id,
                                        int64_t &subpart_id,
                                        bool &no_match_partiton)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(init_expr_ctx(exec_ctx))) {
    LOG_WARN("Failed to init expr calculation context", K(ret));
  } else if (OB_FAIL(get_part_id(exec_ctx, table_schema, row,
                                 part_func, repart_columns, part_id))) {
    LOG_WARN("failed to get part id", K(ret));
  } else if (PARTITION_LEVEL_TWO != table_schema.get_part_level() ||
             NO_MATCH_PARTITION == part_id) {
    // do nothing
  } else if (OB_FAIL(get_subpart_id(exec_ctx, table_schema, row, part_id, subpart_func,
                                    repart_sub_columns, subpart_id))) {
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


int ObShuffleService::get_part_id(ObExecContext &exec_ctx,
                                  const share::schema::ObTableSchema &table_schema,
                                  const common::ObNewRow &row,
                                  const ObSqlExpression &part_func,
                                  const ObIArray<ObTransmitRepartColumn> &repart_columns,
                                  int64_t &part_id)
{
  int ret = OB_SUCCESS;
  if (table_schema.is_key_part()) {
    if (OB_FAIL(get_key_part_id(exec_ctx, table_schema, row, part_func, part_id))) {
      LOG_WARN("get key part id failed");
    }
  } else if (table_schema.is_list_part() ||
             table_schema.is_range_part() ||
             table_schema.is_hash_part()) {
    if (OB_FAIL(get_non_key_partition_part_id(exec_ctx, table_schema, row,
                                              repart_columns, part_id))) {
      LOG_WARN("failed to get non key partition part id", K(ret));
    }
  } else {
    ret = OB_NOT_IMPLEMENT;
    LOG_WARN("this type of partition is not implement", K(ret));
  }
  return ret;
}

int ObShuffleService::get_key_part_id(ObExecContext &exec_ctx,
                                      const share::schema::ObTableSchema &table_schema,
                                      const common::ObNewRow &row,
                                      const ObSqlExpression &part_func,
                                      int64_t &part_id)
{
  int ret = OB_SUCCESS;
  UNUSED(exec_ctx);
  int64_t calc_result = 0;
  ObObj func_result;
  int64_t part_count = table_schema.get_part_option().get_part_num();
  //一级分区
  if (part_count <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the part num can not be null", K(part_count), K(part_func), K(ret));
  } else if (OB_FAIL(part_func.calc(expr_ctx_, row, func_result))) {
    LOG_WARN("Failed to calc hash expr", K(ret), K(row));
  } else if (OB_FAIL(func_result.get_int(calc_result))) {
    LOG_WARN("Fail to get int64 from result", K(func_result), K(ret));
  } else if (calc_result < 0) {
    ret =  OB_INVALID_ARGUMENT;
    LOG_WARN("Input arguments is invalid", K(calc_result), K(part_count), K(ret));
  } else if (OB_FAIL(ObPartitionUtils::calc_hash_part_idx(calc_result, part_count, part_id))) {
    LOG_WARN("calc_hash_part_idx failed", K(ret));
  }
  return ret;
}

int ObShuffleService::get_non_key_partition_part_id(ObExecContext &exec_ctx,
                                                    const ObTableSchema &table_schema,
                                                    const ObNewRow &row,
                                                    const ObIArray<ObTransmitRepartColumn> &repart_columns,
                                                    int64_t &part_id)
{
  return OB_NOT_SUPPORTED;
}

int ObShuffleService::get_subpart_id(ObExecContext &exec_ctx,
                                     const share::schema::ObTableSchema &table_schema,
                                     const common::ObNewRow &row,
                                     int64_t part_id,
                                     const ObSqlExpression &subpart_func,
                                     const ObIArray<ObTransmitRepartColumn> &repart_sub_columns,
                                     int64_t &subpart_id)
{
  int ret = OB_SUCCESS;
  if (table_schema.is_key_subpart()) {
    if (OB_FAIL(get_key_subpart_id(exec_ctx, table_schema, row,
                                   part_id, subpart_func, subpart_id))) {
      LOG_WARN("get key subpart id failed");
    }
  } else if (table_schema.is_list_subpart()
             || table_schema.is_range_subpart()
             || table_schema.is_hash_subpart()) {
    if (OB_FAIL(get_non_key_subpart_id(exec_ctx, table_schema, row, part_id,
                                       repart_sub_columns, subpart_id))) {
      LOG_WARN("get range or hash subpart id failed");
    }
  } else {
    ret = OB_NOT_IMPLEMENT;
    LOG_WARN("we only support the range, range column, key, hash repartition exe", K(ret));
  }
  return ret;
}

// FIXME: 支持非模版化二级分区
int ObShuffleService::get_key_subpart_id(ObExecContext &exec_ctx,
                                         const ObTableSchema &table_schema,
                                         const ObNewRow &row,
                                         int64_t part_id,
                                         const ObSqlExpression &subpart_func,
                                         int64_t &subpart_id)
{
  return OB_NOT_SUPPORTED;
}

//FIXME:此处和22x实现不一致，需要check一下
int ObShuffleService::get_non_key_subpart_id(ObExecContext &exec_ctx,
                                             const ObTableSchema &table_schema,
                                             const ObNewRow &row,
                                             int64_t part_id,
                                             const ObIArray<ObTransmitRepartColumn> &repart_sub_columns,
                                             int64_t &subpart_id)
{
  return OB_NOT_SUPPORTED;
}

int ObShuffleService::get_repart_row(ObExecContext &exec_ctx,
                                     const common::ObNewRow &in_row,
                                     const ObIArray<ObTransmitRepartColumn> &repart_columns,
                                     common::ObNewRow &out_row,
                                     bool hash_part)
{
  int ret = OB_SUCCESS;
  UNUSED(exec_ctx);
  if (current_cell_count_ < repart_columns.count()) {
    current_cell_count_ = repart_columns.count();
    row_cache_.cells_ = static_cast<ObObj*>(allocator_.alloc(sizeof(ObObj)*current_cell_count_));
  }
  row_cache_.count_ = repart_columns.count();
  if (OB_ISNULL(row_cache_.cells_)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc mem failed", K(ret));
  } else {
    for (int64_t i = 0; i < repart_columns.count() && OB_SUCC(ret); ++i) {
      //为什么远端传过来的是int64?
      int64_t idx = (repart_columns.at(i).index_);
      row_cache_.cells_[i] = in_row.get_cell(idx);
    }
    if (OB_SUCC(ret)) {
      out_row = row_cache_;
    }
  }
  if (OB_SUCC(ret) && hash_part) {
    /*
     * create table t1 (c1 int, c2 int);
     * insert into t1 values(1,1);
     * 这里的1，1在进入之后会被先解析称为int64_t，也就是int type。
     * 在真正插入存储之前会cast为真实类型也就是int32 type。
     * 重分区时候会使用到这个接口，所以这里的类型也要打开int32 type类型。
     *
     * */
    ObObj result;
    if (OB_FAIL(ObExprFuncPartHash::calc_value(expr_ctx_, out_row.cells_, out_row.count_, result))) {
      LOG_WARN("Failed to calc hash value", K(ret));
    } else {
      out_row.cells_[0] = result;
      out_row.count_ = 1;
    }
  }
  return ret;
}

int ObShuffleService::init_expr_ctx(ObExecContext &exec_ctx)
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *my_session = NULL;
  const ObTimeZoneInfo *tz_info = NULL;
  int64_t tz_offset = 0;
  if (nullptr != expr_ctx_.exec_ctx_) {
    //Has been inited, do nothing.
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

}
}

