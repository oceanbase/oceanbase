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
#include "sql/code_generator/ob_static_engine_expr_cg.h"
#include "sql/resolver/expr/ob_raw_expr.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/expr/ob_expr_func_part_hash.h"
#include "sql/engine/expr/ob_expr_calc_partition_id.h"

namespace oceanbase
{
using namespace common;
using namespace share::schema;
namespace sql
{

OB_SERIALIZE_MEMBER(CalcPartitionBaseInfo,
                    ref_table_id_,
                    related_table_ids_,
                    part_level_,
                    part_type_,
                    subpart_type_,
                    part_num_,
                    subpart_num_,
                    partition_id_calc_type_,
                    may_add_interval_part_,
                    calc_id_type_);

int CalcPartitionBaseInfo::deep_copy(common::ObIAllocator &allocator,
                                     const ObExprOperatorType type,
                                     ObIExprExtraInfo *&copied_info) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObExprExtraInfoFactory::alloc(allocator, type,
                                            copied_info))) {
    LOG_WARN("failed to alloc extra info", K(ret));
  } else {
    CalcPartitionBaseInfo *base_info = static_cast<CalcPartitionBaseInfo*>(copied_info);
    if (OB_FAIL(base_info->related_table_ids_.assign(related_table_ids_))) {
      LOG_WARN("assign related table ids failed", K(ret));
    } else {
      base_info->ref_table_id_ = ref_table_id_;
      base_info->part_level_ = part_level_;
      base_info->part_type_ = part_type_;
      base_info->subpart_type_ = subpart_type_;
      base_info->part_num_ = part_num_;
      base_info->subpart_num_ = subpart_num_;
      base_info->partition_id_calc_type_ = partition_id_calc_type_;
      base_info->may_add_interval_part_ = may_add_interval_part_;
      base_info->calc_id_type_ = calc_id_type_;
      base_info->first_part_id_ = first_part_id_;
    }
  }
  return ret;
}

int ObExprCalcPartitionBase::set_may_add_interval_part(ObExpr *expr,
                                                       const MayAddIntervalPart info)
{
  int ret = OB_SUCCESS;
  CalcPartitionBaseInfo *calc_part_info = NULL;
  CK (OB_NOT_NULL(expr));
  OX (calc_part_info = reinterpret_cast<CalcPartitionBaseInfo *>(expr->extra_info_));
  CK (OB_NOT_NULL(calc_part_info));
  OX (calc_part_info->may_add_interval_part_ = info);
  return ret;
}

int ObExprCalcPartitionBase::calc_result_typeN(ObExprResType &type,
                                               ObExprResType *types_array,
                                               int64_t param_num,
                                               common::ObExprTypeCtx &type_ctx) const
{
  UNUSED(types_array);
  UNUSED(param_num);
  UNUSED(type_ctx);
  if (CALC_PARTITION_TABLET_ID == get_calc_id_type()) {
    type.set_binary();
    type.set_length(sizeof(uint64_t) * 2);
  } else {
    type.set_int();
    type.set_precision(ObAccuracy::DDL_DEFAULT_ACCURACY[ObIntType].precision_);
    type.set_scale(DEFAULT_SCALE_FOR_INTEGER);
  }
  return OB_SUCCESS;
}

int ObExprCalcPartitionBase::cg_expr(ObExprCGCtx &expr_cg_ctx,
                                   const ObRawExpr &raw_expr,
                                   ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  ObTableID ref_table_id = reinterpret_cast<ObTableID>(raw_expr.get_extra());
  CalcPartitionBaseInfo *calc_part_info = NULL;
  const ObTableSchema *table_schema = NULL;
  OptRouteType opt_route = OPT_ROUTE_NONE;
  if (OB_ISNULL(expr_cg_ctx.schema_guard_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (0 == ref_table_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid ref table id", K(ref_table_id), K(ret));
  } else if (OB_FAIL(expr_cg_ctx.schema_guard_->get_table_schema(
             MTL_ID(), ref_table_id, table_schema))) {
    LOG_WARN("fail to get table schema", K(ref_table_id), K(ret));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("Table not exist", K(ref_table_id), K(ret));
  } else if (OB_FAIL(init_calc_part_info(expr_cg_ctx.allocator_,
                                         *table_schema,
                                         raw_expr.get_partition_id_calc_type(),
                                         raw_expr.get_may_add_interval_part(),
                                         calc_part_info))) {
    LOG_WARN("fail to init tl expr info", K(ret));
  } else if (OB_ISNULL(calc_part_info)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("fail to init tl expr info", K(ret), K(calc_part_info));
  } else {
    rt_expr.extra_info_ = calc_part_info;
    int64_t param_cnt = raw_expr.get_param_count();
    if (0 == param_cnt) {
      OB_ASSERT(PARTITION_LEVEL_ZERO == calc_part_info->part_level_);
      rt_expr.eval_func_ = ObExprCalcPartitionBase::calc_no_partition_location;
    } else if (1 == param_cnt) {
      OB_ASSERT(PARTITION_LEVEL_ONE == calc_part_info->part_level_);
      rt_expr.eval_func_ = ObExprCalcPartitionBase::calc_partition_level_one;
    } else if (2 == param_cnt) {
      OB_ASSERT(PARTITION_LEVEL_TWO == calc_part_info->part_level_);
      rt_expr.eval_func_ = ObExprCalcPartitionBase::calc_partition_level_two;
    } else {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid param cnt", K(ret), K(param_cnt));
    }
  }

  return ret;
}

int ObExprCalcPartitionBase::init_calc_part_info(ObIAllocator *allocator,
                                                 const ObTableSchema &table_schema,
                                                 PartitionIdCalcType calc_type,
                                                 MayAddIntervalPart add_part,
                                                 CalcPartitionBaseInfo *&calc_part_info) const
{
  int ret = OB_SUCCESS;
  calc_part_info = NULL;
  CK(OB_NOT_NULL(allocator));
  if (OB_SUCC(ret)) {
    void *buf = allocator->alloc(sizeof(CalcPartitionBaseInfo));
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc memory", K(ret));
    } else {
      calc_part_info = new(buf) CalcPartitionBaseInfo(*allocator, get_type());
      calc_part_info->ref_table_id_ = table_schema.get_table_id();
      calc_part_info->part_level_ = table_schema.get_part_level();
      calc_part_info->part_type_ = table_schema.get_part_option().get_part_func_type();
      calc_part_info->subpart_type_ = table_schema.get_sub_part_option().get_sub_part_func_type();
      calc_part_info->part_num_ = table_schema.get_first_part_num();
      calc_part_info->subpart_num_ = OB_INVALID_ID; // 目前未使用，要使用的话需要考虑二级分区个数异构
      calc_part_info->partition_id_calc_type_ = calc_type;
      calc_part_info->may_add_interval_part_ = add_part;
      calc_part_info->calc_id_type_ = get_calc_id_type();
      LOG_DEBUG("table location expr info", KPC(calc_part_info), K(ret));
    }
  }

  return ret;
}

int ObExprCalcPartitionBase::calc_no_partition_location(const ObExpr &expr,
                                                        ObEvalCtx &ctx,
                                                        ObDatum &res_datum)
{
  int ret = OB_SUCCESS;
  UNUSED(expr);
  ObDASTabletMapper tablet_mapper;
  ObSEArray<ObTabletID, 1> tablet_ids;
  ObSEArray<ObObjectID, 1> partition_ids;
  CalcPartitionBaseInfo *calc_part_info = reinterpret_cast<CalcPartitionBaseInfo *>(expr.extra_info_);
  if (OB_FAIL(ctx.exec_ctx_.get_das_ctx().get_das_tablet_mapper(calc_part_info->ref_table_id_,
                                                                tablet_mapper,
                                                                &calc_part_info->related_table_ids_))) {
    LOG_WARN("get das tablet mapper failed", K(ret), K(calc_part_info));
  } else if (OB_FAIL(tablet_mapper.get_non_partition_tablet_id(tablet_ids, partition_ids))) {
    LOG_WARN("fail to get non partition tablet id", K(ret));
  } else {
    if (CALC_TABLET_ID == calc_part_info->calc_id_type_) {
      if (0 == tablet_ids.count()) {
        res_datum.set_int(ObTabletID::INVALID_TABLET_ID);
      } else {
        res_datum.set_int(tablet_ids.at(0).id());
      }
    } else if (CALC_PARTITION_ID == calc_part_info->calc_id_type_) {
      if (0 == partition_ids.count()) {
        res_datum.set_int(OB_INVALID_ID);
      } else {
        res_datum.set_int(partition_ids.at(0));
      }
    } else if (CALC_PARTITION_TABLET_ID == calc_part_info->calc_id_type_) {
      if (OB_FAIL(concat_part_and_tablet_id(expr, ctx, res_datum,
                    (0 == partition_ids.count()) ? OB_INVALID_ID : partition_ids.at(0),
                    (0 == tablet_ids.count()) ? OB_INVALID_ID : tablet_ids.at(0).id()))) {
        LOG_WARN("fail to concat partition id and tablet id", K(ret));
      }
    }
  }

  return ret;
}

int ObExprCalcPartitionBase::calc_partition_level_one(const ObExpr &expr,
                                                      ObEvalCtx &ctx,
                                                      ObDatum &res_datum)
{
  int ret = OB_SUCCESS;
  OB_ASSERT(1 == expr.arg_cnt_);
  CalcPartitionBaseInfo *calc_part_info = reinterpret_cast<CalcPartitionBaseInfo *>(expr.extra_info_);
  CK (OB_NOT_NULL(calc_part_info));
  ObTabletID tablet_id(ObTabletID::INVALID_TABLET_ID);
  ObObjectID partition_id = OB_INVALID_ID;
  OZ (calc_partition_id(*expr.args_[0],
                        ctx,
                        *calc_part_info,
                        OB_INVALID_ID, /*first_part_id*/
                        tablet_id,
                        partition_id));
  if (OB_SUCC(ret)) {
    if (CALC_TABLET_ID == calc_part_info->calc_id_type_) {
      res_datum.set_int(tablet_id.id());
    } else if (CALC_PARTITION_ID == calc_part_info->calc_id_type_) {
      res_datum.set_int(partition_id);
    } else if (CALC_PARTITION_TABLET_ID == calc_part_info->calc_id_type_) {
      if (OB_FAIL(concat_part_and_tablet_id(expr, ctx, res_datum, partition_id, tablet_id.id()))) {
        LOG_WARN("fail to concat partition id and tablet id", K(ret));
      }
    }
  }
  return ret;
}

int ObExprCalcPartitionBase::calc_partition_level_two(const ObExpr &expr,
                                                      ObEvalCtx &ctx,
                                                      ObDatum &res_datum)
{
  int ret = OB_SUCCESS;
  OB_ASSERT(2 == expr.arg_cnt_);
  OB_ASSERT(nullptr != expr.extra_info_);
  CalcPartitionBaseInfo *calc_part_info = reinterpret_cast<CalcPartitionBaseInfo *>(expr.extra_info_);
  PartitionIdCalcType calc_type = CALC_INVALID == calc_part_info->partition_id_calc_type_ ?
                                    ctx.exec_ctx_.get_partition_id_calc_type() :
                                    calc_part_info->partition_id_calc_type_;
  ObObjectID first_part_id = OB_INVALID_ID;
  ObTabletID tablet_id(ObTabletID::INVALID_TABLET_ID);
  ObObjectID partition_id = OB_INVALID_ID;
  if (CALC_IGNORE_FIRST_PART == calc_type) {
    if (OB_FAIL(calc_partition_id(*expr.args_[1],
                                  ctx,
                                  *calc_part_info,
                                  calc_part_info->first_part_id_,
                                  tablet_id,
                                  partition_id))) {
      LOG_WARN("fail to calc partitoin id", K(ret));
    }
  } else if (CALC_IGNORE_SUB_PART == calc_type) {
    if (OB_FAIL(calc_partition_id(*expr.args_[0],
                                  ctx,
                                  *calc_part_info,
                                  OB_INVALID_ID, /*first_part_id*/
                                  tablet_id,
                                  partition_id))) {
      LOG_WARN("fail to calc partitoin id", K(ret));
    } else {
      // FIXME @YISHEN
      tablet_id = ObTabletID(partition_id);
    }
  } else if (OB_FAIL(calc_partition_id(*expr.args_[0],
                                       ctx,
                                       *calc_part_info,
                                       OB_INVALID_ID, /*first_part_id*/
                                       tablet_id,
                                       first_part_id))) {
    LOG_WARN("fail to calc partitoin id", K(ret));
  } else {
    if (OB_INVALID_ID == first_part_id) {
      // do nothing
    } else {
      if (OB_FAIL(calc_partition_id(*expr.args_[1],
                                    ctx,
                                    *calc_part_info,
                                    first_part_id,
                                    tablet_id,
                                    partition_id))) {
        LOG_WARN("fail to calc partitoin id", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (CALC_TABLET_ID == calc_part_info->calc_id_type_) {
      res_datum.set_int(tablet_id.id());
    } else if (CALC_PARTITION_ID == calc_part_info->calc_id_type_) {
      res_datum.set_int(partition_id);
    } else if (CALC_PARTITION_TABLET_ID == calc_part_info->calc_id_type_) {
      if (OB_FAIL(concat_part_and_tablet_id(expr, ctx, res_datum, partition_id, tablet_id.id()))) {
        LOG_WARN("fail to concat partition id and tablet id", K(ret));
      }
    }
  }

  return ret;
}

int ObExprCalcPartitionBase::concat_part_and_tablet_id(const ObExpr &expr,
                                                       ObEvalCtx &ctx,
                                                       ObDatum &res_datum,
                                                       uint64_t partition_id,
                                                       uint64_t tablet_id)
{
  int ret = OB_SUCCESS;
  uint64_t buf_len = sizeof(uint64_t) * 2;
  uint64_t *buf = reinterpret_cast<uint64_t *>(expr.get_str_res_mem(ctx, buf_len));
  if (OB_ISNULL(buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory", K(ret));
  } else {
    buf[0] = partition_id;
    buf[1] = tablet_id;
    res_datum.set_string(reinterpret_cast<char *>(buf), buf_len);
  }
  return ret;
}

int ObExprCalcPartitionBase::extract_part_and_tablet_id(const ObDatum &part_datum,
                                                        ObObjectID &part_id,
                                                        ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  const ObString &part_str = part_datum.get_string();
  if (part_str.length() < sizeof(uint64_t) * 2) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the partition string need 16 byte at least", K(ret));
  } else {
    const uint64_t *id_array = reinterpret_cast<const uint64_t*>(part_str.ptr());
    part_id = id_array[0];
    tablet_id = id_array[1];
  }
  return ret;
}

int ObExprCalcPartitionBase::calc_part_and_tablet_id(const ObExpr *calc_part_id,
                                                     ObEvalCtx &eval_ctx,
                                                     ObObjectID &partition_id,
                                                     ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  ObDatum *partition_id_datum = NULL;
  if (OB_ISNULL(calc_part_id) || !calc_part_id->datum_meta_.is_binary()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("calc part id is invalid", K(ret), KPC(calc_part_id));
  } else if (OB_FAIL(calc_part_id->eval(eval_ctx, partition_id_datum))) {
    LOG_WARN("calc part id expr failed", K(ret));
  } else if (OB_FAIL(extract_part_and_tablet_id(*partition_id_datum, partition_id, tablet_id))) {
    LOG_WARN("extract part and tablet id failed", K(ret));
  } else if (ObExprCalcPartitionId::NONE_PARTITION_ID == partition_id) {
    ret = OB_NO_PARTITION_FOR_GIVEN_VALUE;
    LOG_DEBUG("no partition matched", K(ret), KPC(calc_part_id), KPC(partition_id_datum));
  }
  return ret;
}

int ObExprCalcPartitionBase::build_row(ObEvalCtx &ctx,
                                       ObIAllocator &allocator,
                                       const ObExpr &expr,
                                       ObNewRow &row)
{
  int ret = OB_SUCCESS;
  OB_ASSERT(T_OP_ROW == expr.type_);
  OB_ASSERT(expr.arg_cnt_ > 0);
  //TODO shengle 这里后面可以将第一次分配的cells_内存放入expr_ctx,
  // 重复使用进行优化;
  if (OB_ISNULL(row.cells_ = static_cast<ObObj *>(
                allocator.alloc(sizeof(ObObj) * expr.arg_cnt_)))) {
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
      ObExpr *col_expr = expr.args_[i];
      ObDatum &col_datum = col_expr->locate_expr_datum(ctx);
      if (OB_FAIL(col_datum.to_obj(row.cells_[i],
                                   col_expr->obj_meta_,
                                   col_expr->obj_datum_map_))) {
        LOG_WARN("convert datum to obj failed", K(ret));
      }
    }
  }

  return ret;
}

int ObExprCalcPartitionBase::add_interval_part(ObExecContext &exec_ctx,
                                             const CalcPartitionBaseInfo &calc_part_info,
                                             ObIAllocator &allocator, ObNewRow &row)
{
  int ret = OB_SUCCESS;
  if (MayAddIntervalPart::YES == calc_part_info.may_add_interval_part_) {
    const uint64_t tenant_id = MTL_ID();
    const ObTableSchema *table_schema = NULL;
    bool is_interval = false;
    CK (OB_NOT_NULL(exec_ctx.get_sql_ctx()));
    OZ (exec_ctx.get_sql_ctx()->schema_guard_->get_table_schema(
        tenant_id, calc_part_info.ref_table_id_, table_schema));
    CK (OB_NOT_NULL(table_schema));
    OX (is_interval = table_schema->is_interval_part());
    if (OB_SUCC(ret) && is_interval) {
      if (OB_FAIL(ObTableLocation::send_add_interval_partition_rpc_new_engine(
                  allocator, exec_ctx.get_sql_ctx()->session_info_,
                  exec_ctx.get_sql_ctx()->schema_guard_, table_schema, row))) {
        if (is_need_retry_interval_part_error(ret)) {
          set_interval_partition_insert_error(ret);
        }
        LOG_WARN("failed to send add interval partition rpc", K(ret));
      } else {
        set_interval_partition_insert_error(ret);
      }
    }
  } else if (MayAddIntervalPart::PART_CHANGE_ERR == calc_part_info.may_add_interval_part_) {
    ret = OB_ERR_UPD_CAUSE_PART_CHANGE;
    LOG_WARN("cause partition movement", K(ret));
  }
  return ret;
}

/*
  when the table is partitioned by interval, call this function may have three different action
  when the partition id is not exist.
  1. set part id to 0 indicate part not found, this is default action. used by normal part calc such
  as join repart
  2. add interval partition and set an error code force stmt retry. used by dml such as pdml shuffle
  3. set and error code and make the query stop and report and error msg to client, used by update
  partition key column which may cause partition changed. ex: update t1 set c1 = c1 + 201; t1 is
  interval partitioned which only has one partition created (0-200), and partitioned by c1.

  function add_interval_part handle case 2, 3. case 1 is default.
*
*/
int ObExprCalcPartitionBase::calc_partition_id(const ObExpr &part_expr,
                                               ObEvalCtx &ctx,
                                               const CalcPartitionBaseInfo &calc_part_info,
                                               ObObjectID first_part_id,
                                               ObTabletID &tablet_id,
                                               ObObjectID &partition_id)
{
  int ret = OB_SUCCESS;
  ObSqlCtx *sql_ctx = ctx.exec_ctx_.get_sql_ctx();
  tablet_id.reset();
  partition_id = OB_INVALID_ID;
  ObPartitionLevel part_level = (OB_INVALID_ID == first_part_id)
                                ? PARTITION_LEVEL_ONE : PARTITION_LEVEL_TWO;
  ObPartitionFuncType part_type = (PARTITION_LEVEL_ONE == part_level)
                                  ? calc_part_info.part_type_ : calc_part_info.subpart_type_;
  ObDASTabletMapper tablet_mapper;
  if (OB_FAIL(ctx.exec_ctx_.get_das_ctx().get_das_tablet_mapper(calc_part_info.ref_table_id_,
                                                                tablet_mapper,
                                                                &calc_part_info.related_table_ids_))) {
    LOG_WARN("get das tablet mapper failed", K(ret), K(calc_part_info));
  } else if (T_OP_ROW == part_expr.type_) {
    ObDatum *tmp_datum = NULL;
    //这里提前计算下expr child值, 而不是在build row中直接调用eval，
    //是为了避免eval计算里面会使用reset tmp alloc, 影响下面分配row中
    //cell内存对reset tmp alloc的使用
    for (int64_t i = 0; OB_SUCC(ret) && i < part_expr.arg_cnt_; i++) {
      if (OB_FAIL(part_expr.args_[i]->eval(ctx, tmp_datum))) {
        LOG_WARN("fail to eval part expr", K(ret), K(part_expr));
      }
    }
    if (OB_SUCC(ret)) {
      ObNewRow row;
      ObEvalCtx::TempAllocGuard alloc_guard(ctx);
      ObIAllocator &allocator = alloc_guard.get_allocator();
      if (OB_FAIL(build_row(ctx, allocator, part_expr, row))) {
        LOG_WARN("fail to build row", K(ret));
      } else if (OB_FAIL(tablet_mapper.get_tablet_and_object_id(
                                               part_level,
                                               first_part_id,
                                               row,
                                               tablet_id,
                                               partition_id))) {
        LOG_WARN("Failed to get part id", K(ret), K(row));
      } else {
        if ((OB_INVALID_ID == partition_id) &&
            PARTITION_LEVEL_ONE == part_level &&
            NULL != ctx.exec_ctx_.get_my_session() &&
            ORACLE_MODE == ctx.exec_ctx_.get_my_session()->get_compatibility_mode()) {
          OZ (add_interval_part(ctx.exec_ctx_, calc_part_info, allocator, row),
                                                 calc_part_info, first_part_id);
        }
      }
    }
  } else { // not list/range columns
    ObObj func_value;
    ObObj result;
    ObDatum *datum = NULL;
    if (OB_FAIL(part_expr.eval(ctx, datum))) {
      LOG_WARN("part expr evaluate failed", K(ret));
    } else if (OB_FAIL(datum->to_obj(func_value,
                                     part_expr.obj_meta_,
                                     part_expr.obj_datum_map_))) {
      LOG_WARN("convert datum to obj failed", K(ret));
    } else {
      result = func_value;
      if (PARTITION_FUNC_TYPE_HASH == part_type) {
        if (lib::is_oracle_mode()) {
          // do nothing
        } else if (OB_FAIL(ObExprFuncPartHash::calc_value_for_mysql(func_value, result,
                    func_value.get_type()))) {
          LOG_WARN("Failed to calc hash value mysql mode", K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        ObSEArray<ObTabletID, 1> tablet_ids;
        ObSEArray<ObObjectID, 1> partition_ids;
        //这里也可以统一使用上面的ObNewRow接口, 并把calc_value_for_mysql
        // 用datum实现下,  暂时和以前的方式保持一致
        ObRowkey rowkey(const_cast<ObObj*>(&result), 1);
        ObNewRange range;
        if (OB_FAIL(range.build_range(calc_part_info.ref_table_id_, rowkey))) {
          LOG_WARN("Failed to build range", K(ret));
        } else if (OB_FAIL(tablet_mapper.get_tablet_and_object_id(
                                          part_level,
                                          first_part_id,
                                          range,
                                          tablet_ids,
                                          partition_ids))) {
          LOG_WARN("Failed to get part id", K(ret));
        } else if (partition_ids.count() != 0 && partition_ids.count() != 1) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid partition cnt", K(ret), K(part_expr), K(partition_ids), K(range), K(rowkey));
        } else {
          if (0 == partition_ids.count() &&
             PARTITION_LEVEL_ONE == part_level &&
             NULL != ctx.exec_ctx_.get_my_session() &&
             ORACLE_MODE == ctx.exec_ctx_.get_my_session()->get_compatibility_mode()) {
            ObEvalCtx::TempAllocGuard alloc_guard(ctx);
            ObIAllocator &allocator = alloc_guard.get_allocator();
            ObNewRow row(const_cast<ObObj*>(&result), 1);
            OZ (add_interval_part(ctx.exec_ctx_, calc_part_info, allocator, row),
                                           calc_part_info, first_part_id);
          }
          if (OB_SUCC(ret) && 1 == partition_ids.count()) {
            partition_id = partition_ids.at(0);
            if (1 == tablet_ids.count()) {
              tablet_id = tablet_ids.at(0);
            }
          }
        }
      }
    }
  }

  return ret;
}

//calc partition id
ObExprCalcPartitionId::ObExprCalcPartitionId(ObIAllocator &alloc)
    : ObExprCalcPartitionBase(alloc,
                              T_FUN_SYS_CALC_PARTITION_ID,
                              N_CALC_PARTITION_ID,
                              PARAM_NUM_UNKNOWN,
                              NOT_ROW_DIMENSION)
{
}

ObExprCalcPartitionId::~ObExprCalcPartitionId()
{
}

//calc tablet id
ObExprCalcTabletId::ObExprCalcTabletId(ObIAllocator &alloc)
    : ObExprCalcPartitionBase(alloc,
                              T_FUN_SYS_CALC_TABLET_ID,
                              N_CALC_TABLET_ID,
                              PARAM_NUM_UNKNOWN,
                               NOT_ROW_DIMENSION)
{
}

ObExprCalcTabletId::~ObExprCalcTabletId()
{
}

//calc partition id and tablet id
ObExprCalcPartitionTabletId::ObExprCalcPartitionTabletId(ObIAllocator &alloc)
    : ObExprCalcPartitionBase(alloc,
                              T_FUN_SYS_CALC_PARTITION_TABLET_ID,
                              N_CALC_PARTITION_TABLET_ID,
                              PARAM_NUM_UNKNOWN,
                              NOT_ROW_DIMENSION)
{
}

ObExprCalcPartitionTabletId::~ObExprCalcPartitionTabletId()
{
}

}
}
