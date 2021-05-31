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

#define USING_LOG_PREFIX SQL_ENG
#include "ob_expr_calc_partition_id.h"
#include "share/part/ob_part_mgr_ad.h"
#include "sql/code_generator/ob_static_engine_expr_cg.h"
#include "sql/resolver/expr/ob_raw_expr.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/expr/ob_expr_func_part_hash.h"

namespace oceanbase {
using namespace common;
using namespace share::schema;
namespace sql {

OB_SERIALIZE_MEMBER(
    CalcPartitionIdInfo, ref_table_id_, part_level_, part_type_, subpart_type_, part_num_, subpart_num_);

int CalcPartitionIdInfo::deep_copy(
    common::ObIAllocator& allocator, const ObExprOperatorType type, ObIExprExtraInfo*& copied_info) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObExprExtraInfoFactory::alloc(allocator, type, copied_info))) {
    LOG_WARN("failed to alloc extra info", K(ret));
  } else {
    *copied_info = *this;
  }
  return ret;
}

ObExprCalcPartitionId::ObExprCalcPartitionId(ObIAllocator& alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_CALC_PARTITION_ID, N_CALC_PARTITION_ID, PARAM_NUM_UNKNOWN, NOT_ROW_DIMENSION)
{}

ObExprCalcPartitionId::~ObExprCalcPartitionId()
{}

int ObExprCalcPartitionId::calc_result_typeN(
    ObExprResType& type, ObExprResType* types_array, int64_t param_num, common::ObExprTypeCtx& type_ctx) const
{
  UNUSED(types_array);
  UNUSED(param_num);
  UNUSED(type_ctx);
  type.set_int();
  type.set_precision(ObAccuracy::DDL_DEFAULT_ACCURACY[ObIntType].precision_);
  type.set_scale(DEFAULT_SCALE_FOR_INTEGER);
  return OB_SUCCESS;
}

int ObExprCalcPartitionId::cg_expr(ObExprCGCtx& expr_cg_ctx, const ObRawExpr& raw_expr, ObExpr& rt_expr) const
{
  int ret = OB_SUCCESS;
  uint64_t ref_table_id = raw_expr.get_extra();
  CalcPartitionIdInfo* calc_part_info = NULL;
  const ObTableSchema* table_schema = NULL;
  ObSqlCtx* sql_ctx = NULL;
  ;
  OptRouteType opt_route = OPT_ROUTE_NONE;
  if (OB_ISNULL(expr_cg_ctx.exec_ctx_) || OB_ISNULL(sql_ctx = expr_cg_ctx.exec_ctx_->get_sql_ctx()) ||
      OB_ISNULL(sql_ctx->schema_guard_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(expr_cg_ctx.exec_ctx_));
  } else if (0 == ref_table_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid ref table id", K(ref_table_id), K(ret));
  } else if (OB_FAIL(sql_ctx->schema_guard_->get_table_schema(ref_table_id, table_schema))) {
    LOG_WARN("fail to get table schema", K(ref_table_id), K(ret));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("Table not exist", K(ref_table_id), K(ret));
  } else if (OB_FAIL(init_calc_part_info(expr_cg_ctx, *table_schema, calc_part_info))) {
    LOG_WARN("fail to init tl expr info", K(ret));
  } else if (OB_ISNULL(calc_part_info)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("fail to init tl expr info", K(ret), K(calc_part_info));
  } else if (OB_FAIL(get_opt_route(*table_schema, raw_expr, opt_route))) {
    LOG_WARN("fail to check use opt", K(ret));
  } else {
    rt_expr.extra_info_ = calc_part_info;
    int64_t param_cnt = raw_expr.get_param_count();
    if (OPT_ROUTE_HASH_ONE == opt_route) {
      rt_expr.eval_func_ = ObExprCalcPartitionId::calc_opt_route_hash_one;
    } else if (0 == param_cnt) {
      OB_ASSERT(PARTITION_LEVEL_ZERO == calc_part_info->part_level_);
      rt_expr.eval_func_ = ObExprCalcPartitionId::calc_no_partition_location;
    } else if (1 == param_cnt) {
      OB_ASSERT(PARTITION_LEVEL_ONE == calc_part_info->part_level_);
      rt_expr.eval_func_ = ObExprCalcPartitionId::calc_partition_level_one;
    } else if (2 == param_cnt) {
      OB_ASSERT(PARTITION_LEVEL_TWO == calc_part_info->part_level_);
      rt_expr.eval_func_ = ObExprCalcPartitionId::calc_partition_level_two;
    } else {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid param cnt", K(ret), K(param_cnt));
    }
  }

  return ret;
}

int ObExprCalcPartitionId::init_calc_part_info(
    ObExprCGCtx& expr_cg_ctx, const ObTableSchema& table_schema, CalcPartitionIdInfo*& calc_part_info)
{
  int ret = OB_SUCCESS;
  calc_part_info = NULL;
  CK(OB_NOT_NULL(expr_cg_ctx.allocator_));
  if (OB_SUCC(ret)) {
    void* buf = expr_cg_ctx.allocator_->alloc(sizeof(CalcPartitionIdInfo));
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc memory", K(ret));
    } else {
      calc_part_info = new (buf) CalcPartitionIdInfo(*expr_cg_ctx.allocator_, T_FUN_SYS_CALC_PARTITION_ID);
      calc_part_info->ref_table_id_ = table_schema.get_table_id();
      calc_part_info->part_level_ = table_schema.get_part_level();
      calc_part_info->part_type_ = table_schema.get_part_option().get_part_func_type();
      calc_part_info->subpart_type_ = table_schema.get_sub_part_option().get_sub_part_func_type();
      calc_part_info->part_num_ = table_schema.get_first_part_num();
      calc_part_info->subpart_num_ = OB_INVALID_ID;  // unsed for now
      LOG_DEBUG("table location expr info", K(*calc_part_info), K(ret));
    }
  }

  return ret;
}

int ObExprCalcPartitionId::calc_no_partition_location(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& res_datum)
{
  int ret = OB_SUCCESS;
  UNUSED(expr);
  UNUSED(ctx);
  res_datum.set_int(0);

  return ret;
}

int ObExprCalcPartitionId::calc_partition_level_one(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& res_datum)
{
  int ret = OB_SUCCESS;
  OB_ASSERT(1 == expr.arg_cnt_);
  OB_ASSERT(expr.extra_ > 0);
  CalcPartitionIdInfo* calc_part_info = reinterpret_cast<CalcPartitionIdInfo*>(expr.extra_);
  if (OB_FAIL(calc_partition_id(*expr.args_[0],
          ctx,
          *calc_part_info,
          NONE_PARTITION_ID, /*first_part_id*/
          res_datum))) {
    LOG_WARN("fail to calc partitoin id", K(ret));
  }

  return ret;
}

int ObExprCalcPartitionId::calc_partition_level_two(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& res_datum)
{
  int ret = OB_SUCCESS;
  OB_ASSERT(2 == expr.arg_cnt_);
  OB_ASSERT(expr.extra_ > 0);
  CalcPartitionIdInfo* calc_part_info = reinterpret_cast<CalcPartitionIdInfo*>(expr.extra_);
  PartitionIdCalcType calc_type = ctx.exec_ctx_.get_partition_id_calc_type();
  if (CALC_IGNORE_FIRST_PART == calc_type) {
    int64_t first_part_id = ctx.exec_ctx_.get_fixed_id();
    if (OB_FAIL(calc_partition_id(*expr.args_[1], ctx, *calc_part_info, first_part_id, res_datum))) {
      LOG_WARN("fail to calc partitoin id", K(ret));
    }
  } else if (CALC_IGNORE_SUB_PART == calc_type) {
    if (OB_FAIL(calc_partition_id(*expr.args_[0],
            ctx,
            *calc_part_info,
            NONE_PARTITION_ID, /*first_part_id*/
            res_datum))) {
      LOG_WARN("fail to calc partitoin id", K(ret));
    }
  } else if (OB_FAIL(calc_partition_id(*expr.args_[0],
                 ctx,
                 *calc_part_info,
                 NONE_PARTITION_ID, /*first_part_id*/
                 res_datum))) {
    LOG_WARN("fail to calc partitoin id", K(ret));
  } else {
    int64_t first_part_id = res_datum.get_int();
    if (NONE_PARTITION_ID == first_part_id) {
      // do nothing
      // The first level partition is not calculated, return NONE_PARTITION_ID
    } else {
      if (OB_FAIL(calc_partition_id(*expr.args_[1], ctx, *calc_part_info, first_part_id, res_datum))) {
        LOG_WARN("fail to calc partitoin id", K(ret));
      } else if (NONE_PARTITION_ID == res_datum.get_int()) {
        // do nothing
      } else {
        res_datum.set_int(generate_phy_part_id(first_part_id, res_datum.get_int(), PARTITION_LEVEL_TWO));
      }
    }
  }

  return ret;
}

int ObExprCalcPartitionId::build_row(ObEvalCtx& ctx, ObIAllocator& allocator, const ObExpr& expr, ObNewRow& row)
{
  int ret = OB_SUCCESS;
  OB_ASSERT(T_OP_ROW == expr.type_);
  OB_ASSERT(expr.arg_cnt_ > 0);
  if (OB_ISNULL(row.cells_ = static_cast<ObObj*>(allocator.alloc(sizeof(ObObj) * expr.arg_cnt_)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memory failed", K(ret));
  } else {
    for (int64_t i = 0; i < expr.arg_cnt_; i++) {
      new (&row.cells_[i]) ObObj();
    }
    row.count_ = expr.arg_cnt_;
    row.projector_size_ = 0;
    row.projector_ = NULL;
  }
  if (OB_SUCC(ret)) {
    for (int64_t i = 0; OB_SUCC(ret) && i < expr.arg_cnt_; i++) {
      ObExpr* col_expr = expr.args_[i];
      ObDatum& col_datum = col_expr->locate_expr_datum(ctx);
      if (OB_FAIL(col_datum.to_obj(row.cells_[i], col_expr->obj_meta_, col_expr->obj_datum_map_))) {
        LOG_WARN("convert datum to obj failed", K(ret));
      }
    }
  }

  return ret;
}

int ObExprCalcPartitionId::calc_partition_id(const ObExpr& part_expr, ObEvalCtx& ctx,
    const CalcPartitionIdInfo& calc_part_info, int64_t first_part_id, ObDatum& res_datum)
{
  int ret = OB_SUCCESS;
  ObTaskExecutorCtx* tctx = GET_TASK_EXECUTOR_CTX(ctx.exec_ctx_);
  ObSqlCtx* sql_ctx = ctx.exec_ctx_.get_sql_ctx();
  int64_t res_part_id = NONE_PARTITION_ID;
  ObSEArray<int64_t, 1> partition_ids;
  ObPartitionLevel part_level = (NONE_PARTITION_ID == first_part_id) ? PARTITION_LEVEL_ONE : PARTITION_LEVEL_TWO;
  ObPartitionFuncType part_type =
      (PARTITION_LEVEL_ONE == part_level) ? calc_part_info.part_type_ : calc_part_info.subpart_type_;
  if (OB_ISNULL(tctx) || OB_ISNULL(sql_ctx) || OB_ISNULL(sql_ctx->schema_guard_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(tctx), K(sql_ctx), K(ret));
  } else if (is_virtual_table(calc_part_info.ref_table_id_) &&
             OB_FAIL(tctx->init_calc_virtual_part_id_params(calc_part_info.ref_table_id_))) {
    LOG_WARN("fail to init_calc_virtual_part_id_params", K(ret), K(calc_part_info));
    // As long as init_calc_virtual_part_id_params is run, it must be run
    // reset_calc_virtual_part_id_params
    tctx->reset_calc_virtual_part_id_params();
  } else if (T_OP_ROW == part_expr.type_) {
    ObDatum* tmp_datum = NULL;
    // Calculate the value of expr child in advance, instead of calling eval directly in the build row,
    // It is to avoid reset tmp alloc used in eval calculation, which affects the following allocation row
    // cell memory usage of reset tmp alloc
    for (int64_t i = 0; OB_SUCC(ret) && i < part_expr.arg_cnt_; i++) {
      if (OB_FAIL(part_expr.args_[i]->eval(ctx, tmp_datum))) {
        LOG_WARN("fail to eval part expr", K(ret), K(part_expr));
      }
    }
    if (OB_SUCC(ret)) {
      ObNewRow row;
      ObIAllocator& allocator = ctx.get_reset_tmp_alloc();
      if (OB_FAIL(build_row(ctx, allocator, part_expr, row))) {
        LOG_WARN("fail to build row", K(ret));
      } else if (OB_FAIL(sql_ctx->schema_guard_->get_part(
                     calc_part_info.ref_table_id_, part_level, first_part_id, row, partition_ids))) {
        LOG_WARN("Failed to get part id", K(ret), K(row));
      } else if (partition_ids.count() != 0 && partition_ids.count() != 1) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid partition cnt", K(ret), K(partition_ids));
      } else {
        res_part_id = (0 == partition_ids.count()) ? NONE_PARTITION_ID : partition_ids.at(0);
      }
    }
  } else {  // not list/range columns
    ObObj func_value;
    ObObj result;
    ObDatum* datum = NULL;
    if (OB_FAIL(part_expr.eval(ctx, datum))) {
      LOG_WARN("part expr evaluate failed", K(ret));
    } else if (OB_FAIL(datum->to_obj(func_value, part_expr.obj_meta_, part_expr.obj_datum_map_))) {
      LOG_WARN("convert datum to obj failed", K(ret));
    } else if (!is_inner_table(calc_part_info.ref_table_id_)) {
      result = func_value;
      if (PARTITION_FUNC_TYPE_HASH == part_type || PARTITION_FUNC_TYPE_HASH_V2 == part_type) {
        if (share::is_oracle_mode()) {
          // do nothing
        } else if (PARTITION_FUNC_TYPE_HASH == part_type) {
          if (OB_FAIL(ObExprFuncPartOldHash::calc_value_for_mysql(func_value, result))) {
            LOG_WARN("Failed to calc hash value mysql mode", K(ret));
          }
        } else if (OB_FAIL(ObExprFuncPartHash::calc_value_for_mysql(func_value, result))) {
          LOG_WARN("Failed to calc hash value mysql mode", K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        ObRowkey rowkey(const_cast<ObObj*>(&result), 1);
        ObNewRange range;
        if (OB_FAIL(range.build_range(calc_part_info.ref_table_id_, rowkey))) {
          LOG_WARN("Failed to build range", K(ret));
        } else if (OB_FAIL(sql_ctx->schema_guard_->get_part(
                       calc_part_info.ref_table_id_, part_level, first_part_id, range, false, partition_ids))) {
          LOG_WARN("Failed to get part id", K(ret));
        } else if (partition_ids.count() != 0 && partition_ids.count() != 1) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid partition cnt", K(ret), K(partition_ids));
        } else {
          res_part_id = (0 == partition_ids.count()) ? NONE_PARTITION_ID : partition_ids.at(0);
        }
      }
    } else {  // inner table
      int64_t calc_result = 0;
      if (OB_FAIL(func_value.get_int(calc_result))) {
        LOG_WARN("Fail to get int64 from result", K(result), K(ret));
      } else if (OB_INVALID_PARTITION_ID == calc_result && is_virtual_table(calc_part_info.ref_table_id_)) {
        res_part_id = NONE_PARTITION_ID;
        // addr is invalid, and partition id calculated by this addr is also invalid, so, do nothing
      } else if (calc_result < 0 || 0 == calc_part_info.part_num_) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid arguments", K(calc_result), K(calc_part_info), K(ret));
      } else {
        res_part_id = calc_result % calc_part_info.part_num_;
      }
    }
  }
  if (OB_SUCC(ret)) {
    res_datum.set_int(res_part_id);
    int64_t part_idx = (NONE_PARTITION_ID == res_part_id) ? OB_INVALID_INDEX : 0;
    ctx.exec_ctx_.get_part_row_manager().set_part_idx(part_idx);
    if (is_virtual_table(calc_part_info.ref_table_id_)) {
      tctx->reset_calc_virtual_part_id_params();
    }
  }

  return ret;
}

int ObExprCalcPartitionId::get_opt_route(
    const ObTableSchema& table_schema, const ObRawExpr& raw_expr, OptRouteType& opt_route_type)
{
  int ret = OB_SUCCESS;
  bool enable_opt = false;
  if (OB_FAIL(enable_opt_route_hash_one(table_schema, raw_expr, enable_opt))) {
    LOG_WARN("fail to check enable use opt route hash one", K(ret));
  } else if (enable_opt) {
    opt_route_type = OPT_ROUTE_HASH_ONE;
  }
  // If there are other optimized execution paths, add judgment after this

  return ret;
}

// Determine the optimal path in different partition definition scenarios:
// 1.OPT_ROUTE_HASH_ONE optimization
// 1) Conditions:
// a. The access is not the internal table
// b. First-level partition
// c. hash partition (non-columns partition)
// d. All part_idx_ and part_id in ObPartition are the same, that is,
//    no partition management related operations have been done,
// 2) Perform optimization:
// The number of hash partitions is stored in CalcPartitionIdInfo, no need to obtain schema,
// After calculating the partition part_idx according to the partition definition,
// there is no need to look up the mapping table of part_idx-->part_id, and return to part_idx directly
// 2. For other optimization scenarios,
//    we will continue to sort out the part_idx-->part id mapping table in the expression for optimization calculations
int ObExprCalcPartitionId::enable_opt_route_hash_one(
    const ObTableSchema& table_schema, const ObRawExpr& raw_expr, bool& enable_opt_route_hash_one)
{
  int ret = OB_SUCCESS;
  enable_opt_route_hash_one = true;
  const ObRawExpr* part_expr = NULL;
  if (is_inner_table(table_schema.get_table_id())) {
    enable_opt_route_hash_one = false;
  } else if (PARTITION_LEVEL_ONE != table_schema.get_part_level()) {
    enable_opt_route_hash_one = false;
  } else if (PARTITION_FUNC_TYPE_HASH != table_schema.get_part_option().get_part_func_type() &&
             PARTITION_FUNC_TYPE_HASH_V2 != table_schema.get_part_option().get_part_func_type()) {
    enable_opt_route_hash_one = false;
  } else if (1 != raw_expr.get_param_count() || (NULL == (part_expr = raw_expr.get_param_expr(0)))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(raw_expr), K(table_schema), K(ret));
  } else if (T_OP_ROW == part_expr->get_expr_type()) {
    enable_opt_route_hash_one = false;
  } else {
    ObPartition** part_array = table_schema.get_part_array();
    bool same = true;
    for (int64_t i = 0; same && OB_SUCC(ret) && i < table_schema.get_partition_num(); i++) {
      if (OB_ISNULL(part_array[i])) {
        ret = OB_SCHEMA_ERROR;
        LOG_WARN("get invalid partiton array", K(ret), K(i));
      } else if (part_array[i]->get_part_idx() != part_array[i]->get_part_id()) {
        same = false;
      }
    }
    if (OB_SUCC(ret) && !same) {
      enable_opt_route_hash_one = false;
    }
  }

  return ret;
}

// 1.OPT_ROUTE_HASH_ONE optimization
// 1) Conditions:
// a. The access is not the internal table
// b. First-level partition
// c. hash partition (non-columns partition)
// d. All part_idx_ and part_id in ObPartition are the same, that is,
//    no partition management related operations have been done,
// 2) Perform optimization:
// The number of hash partitions is stored in CalcPartitionIdInfo, no need to obtain schema,
// After calculating the partition part_idx according to the partition definition,
// there is no need to look up the mapping table of part_idx-->part_id, and return to part_idx directly
int ObExprCalcPartitionId::calc_opt_route_hash_one(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& res_datum)
{
  int ret = OB_SUCCESS;
  OB_ASSERT(1 == expr.arg_cnt_);
  OB_ASSERT(NULL != expr.args_[0]);
  OB_ASSERT(expr.extra_ > 0);
  CalcPartitionIdInfo* calc_part_info = reinterpret_cast<CalcPartitionIdInfo*>(expr.extra_);
  ObDatum* datum = NULL;
  int64_t part_idx = 0;
  if (OB_FAIL(expr.args_[0]->eval(ctx, datum))) {
    LOG_WARN("part expr evaluate failed", K(ret));
  } else {
    int64_t value = 0;
    if (datum->is_null()) {
      // do nothing
    } else {
      value = datum->get_int();
      if (OB_UNLIKELY(INT64_MIN == value)) {
        value = INT64_MAX;
      } else {
        value = value < 0 ? -value : value;
      }
    }
    if (OB_FAIL(ObPartitionUtils::calc_hash_part_idx(value, calc_part_info->part_num_, part_idx))) {
      LOG_WARN("fail to calc hash part idx", K(ret), K(*calc_part_info), K(datum->get_int()));
    } else {
      res_datum.set_int(part_idx);
      LOG_TRACE("calc part id use opt route hash one", K(res_datum), K(*datum), K(*calc_part_info), K(value));
    }
  }

  return ret;
}

}  // namespace sql
}  // namespace oceanbase
