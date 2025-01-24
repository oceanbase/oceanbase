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
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/expr/ob_expr_func_part_hash.h"
#include "share/vector/expr_cmp_func.h"

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

typedef int64_t (*GetPayloadFunc)(const char *payload);
static int64_t get_64_len_val(const char *payload)
{
  return *reinterpret_cast<const int64_t *>(payload);
}

static int64_t get_8_len_val(const char *payload)
{
  return static_cast<int64_t>(*(reinterpret_cast<const uint8_t *>(payload)));
}

static GetPayloadFunc FAST_CALC_PART_GET_VAL_FUNCS[2] = {get_64_len_val, get_8_len_val};

REG_SER_FUNC_ARRAY(OB_SFA_FAST_CALC_PART_VEC,
                   FAST_CALC_PART_GET_VAL_FUNCS,
                   sizeof(FAST_CALC_PART_GET_VAL_FUNCS) / sizeof(void *));

ERRSIM_POINT_DEF(ERRSIM_USE_FAST_CALC_PART);
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
      OB_ASSERT(1 == rt_expr.arg_cnt_);
      OB_ASSERT(PARTITION_LEVEL_ONE == calc_part_info->part_level_);
      rt_expr.eval_func_ = ObExprCalcPartitionBase::calc_partition_level_one;
      bool fallback = false;
      const ObExpr *part_expr = rt_expr.args_[0];
      if (T_OP_ROW != part_expr->type_ &&
          !table_schema->is_external_table() &&
          (CALC_TABLET_ID == calc_part_info->calc_id_type_ ||
           CALC_PARTITION_ID == calc_part_info->calc_id_type_) &&
          ERRSIM_USE_FAST_CALC_PART == OB_SUCCESS) {
        ObDASTabletMapper tablet_mapper;
        if (OB_ISNULL(expr_cg_ctx.session_)) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid expr_cg_ctx session", K(ret));
        } else if (OB_ISNULL(expr_cg_ctx.session_->get_cur_exec_ctx())) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid cur_exec_ctx", K(ret));
        } else if (OB_FAIL(expr_cg_ctx.session_->get_cur_exec_ctx()->
                    get_das_ctx().get_das_tablet_mapper(calc_part_info->ref_table_id_,
                                                        tablet_mapper,
                                                        &calc_part_info->related_table_ids_))) {
          LOG_WARN("get das tablet mapper failed", K(ret), K(calc_part_info));
        } else {
          share::schema::RelatedTableInfo *related_info_ptr = nullptr;
          if (tablet_mapper.get_related_table_info().related_tids_ != nullptr &&
              !tablet_mapper.get_related_table_info().related_tids_->empty()) {
            related_info_ptr = &tablet_mapper.get_related_table_info();
          }
          if (OB_NOT_NULL(related_info_ptr)) {
            fallback = true;
          }
        }
        if (OB_FAIL(ret)) {
        } else if (fallback) {
        } else if (table_schema->is_hash_part()) {
          // The execution period involves inner_functions;
          // disable this path during the upgrade.
          if (GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_4_3_5_0) {
            fallback = true;
          } else {
            rt_expr.inner_func_cnt_ = 1;
            void **func_buf = NULL;
            int64_t func_buf_size = sizeof(void *);
            if (OB_ISNULL(func_buf = (void **)expr_cg_ctx.allocator_->alloc(func_buf_size))) {
              ret = OB_ALLOCATE_MEMORY_FAILED;
              LOG_WARN("failed to allocate memory", K(ret));
            } else {
              rt_expr.inner_functions_ = func_buf;
              if (lib::is_oracle_mode()) {
                if (OB_UNLIKELY(!part_expr->obj_meta_.is_int())) {
                  ret = OB_ERR_UNEXPECTED;
                  LOG_WARN("oracle type invalid", K(part_expr->obj_meta_), KR(ret));
                } else {
                  rt_expr.inner_functions_[0] = (void *)(FAST_CALC_PART_GET_VAL_FUNCS[0]);
                }
              } else {
                switch (ob_obj_type_class(part_expr->obj_meta_.get_type())) {
                  case ObIntTC:
                  case ObUIntTC:
                  case ObBitTC: {
                    rt_expr.inner_functions_[0] = (void *)(FAST_CALC_PART_GET_VAL_FUNCS[0]);
                    break;
                  }
                  case ObYearTC: {
                    rt_expr.inner_functions_[0] = (void *)(FAST_CALC_PART_GET_VAL_FUNCS[1]);
                    break;
                  }
                  default: {
                    ret = OB_INVALID_ARGUMENT;
                    LOG_WARN("type is wrong", K(ret), K(part_expr->obj_meta_.get_type()));
                    break;
                  }
                }
              }
            }
            if (OB_SUCC(ret)) {
              rt_expr.eval_vector_func_ =
                  ObExprCalcPartitionBase::fast_calc_partition_level_one_vector;
            }
          }
        } else if (table_schema->is_key_part()) {
          if (OB_UNLIKELY(!part_expr->obj_meta_.is_int())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("type invalid", K(part_expr->obj_meta_), KR(ret));
          } else {
            rt_expr.eval_vector_func_ =
                ObExprCalcPartitionBase::fast_calc_partition_level_one_vector;
          }
        } else if (table_schema->is_range_part()) {
          if (GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_4_3_5_0) {
            fallback = true;
          } else {
            ObPartition * const* part_array = table_schema->get_part_array();
            if (OB_ISNULL(part_array) || OB_ISNULL(part_array[0]) ||
                OB_ISNULL(part_array[0]->get_high_bound_val().get_obj_ptr())) {
                ret = OB_INVALID_ARGUMENT;
                LOG_WARN("partition_array is null", K(ret));
            } else if (part_expr->obj_meta_.get_type() !=
                part_array[0]->get_high_bound_val().get_obj_ptr()->get_meta().get_type()) {
              fallback = true;
            } else {
              rt_expr.eval_vector_func_ =
                ObExprCalcPartitionBase::fast_calc_partition_level_one_vector;
            }
          }
        } else if (table_schema->is_list_part()) {
          if (GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_4_3_5_0) {
            fallback = true;
          } else {
            ObObjType part_expr_type = rt_expr.args_[0]->datum_meta_.type_;
            // The following types are not supported for hash.
            if (ob_is_json(part_expr_type) || ob_is_urowid(part_expr_type)) {
              fallback = true;
            } else {
              ObPartition * const* part_array = table_schema->get_part_array();
              if (OB_ISNULL(part_array) || OB_ISNULL(part_array[0])) {
                  ret = OB_INVALID_ARGUMENT;
                  LOG_WARN("partition_array is null", K(ret));
              } else {
                const ObIArray<common::ObNewRow> &list_row_values =
                    part_array[0]->get_list_row_values();
                ObObj *list_part_obj = list_row_values.at(0).cells_;
                if (OB_ISNULL(list_part_obj)) {
                  ret = OB_INVALID_ARGUMENT;
                  LOG_WARN("list_part_obj is null", K(ret));
                } else if (part_expr->obj_meta_.get_type() !=
                           list_part_obj->get_meta().get_type()) {
                  fallback = true;
                } else {
                  rt_expr.eval_vector_func_ =
                      ObExprCalcPartitionBase::fast_calc_partition_level_one_vector;
                }
              }
            }
          }
        } else {
          fallback = true;
        }
      } else {
        fallback = true;
      }
      if (OB_SUCC(ret) && fallback) {
        rt_expr.eval_vector_func_ = ObExprCalcPartitionBase::calc_partition_level_one_vector;
      }
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
  CalcPartitionBaseInfo *calc_part_info = reinterpret_cast<CalcPartitionBaseInfo *>(expr.extra_info_);
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

template <typename ArgVec, bool IsOracleMode>
static int inner_fast_calc_hash_partition_level_one_vector(ArgVec *vec,
                                                          const int64_t row_idx,
                                                          const int64_t part_num,
                                                          ObPartition * const* part_array,
                                                          ObPartition *&partition,
                                                          const int64_t powN,
                                                          GetPayloadFunc get_payload_func)
{
  int ret = OB_SUCCESS;
  int64_t part_idx = OB_INVALID_INDEX;
  int64_t val = 0;
  if (OB_UNLIKELY(vec->is_null(row_idx))) {
    val = 0;
  } else {
    val = (get_payload_func)(vec->get_payload(row_idx));
    if (!IsOracleMode) {
      if (OB_UNLIKELY(INT64_MIN == val)) {
        val = INT64_MAX;
      } else {
        val = val < 0 ? -val : val;
      }
    } else {
      if (OB_UNLIKELY(val < 0)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("val is invalid", KR(ret), K(val), K(part_num));
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (!IsOracleMode) {
      part_idx = val % part_num;
    } else {
      part_idx = val & (powN - 1); //pow(2, N));
      if (part_idx + powN < part_num && (val & powN) == powN) {
        part_idx += powN;
      }
    }
    if (OB_SUCC(ret) && OB_ISNULL(partition = part_array[part_idx])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("partition is null", K(ret), K(part_idx));
    }
  }
  return ret;
}

template <typename ArgVec, bool IsOracleMode>
static int inner_fast_calc_key_partition_level_one_vector(ArgVec *vec,
                                                          const int64_t row_idx,
                                                          const int64_t part_num,
                                                          ObPartition * const* part_array,
                                                          ObPartition *&partition,
                                                          const int64_t powN)
{
  int ret = OB_SUCCESS;
  int64_t part_idx = OB_INVALID_INDEX;
  int64_t val = 0;
  if (OB_UNLIKELY(vec->is_null(row_idx))) {
    val = 0;
  } else {
    val = *reinterpret_cast<const int64_t *>(vec->get_payload(row_idx));
    if (OB_UNLIKELY(val < 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("val is invalid", KR(ret), K(val), K(part_num));
    }
  }
  if (OB_SUCC(ret)) {
    if (!IsOracleMode) {
      part_idx = val % part_num;
    } else {
      part_idx = val & (powN - 1); //pow(2, N));
      if (part_idx + powN < part_num && (val & powN) == powN) {
        part_idx += powN;
      }
    }
    if (OB_SUCC(ret) && OB_ISNULL(partition = part_array[part_idx])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("partition is null", K(ret), K(part_idx));
    }
  }
  return ret;
}

template <typename ArgVec>
static int inner_fast_calc_range_partition_level_one_vector(ArgVec *vec,
                                                          const int64_t row_idx,
                                                          const int64_t part_num,
                                                          ObPartition * const* part_array,
                                                          ObPartition *&partition,
                                                          const ObExprCalcPartitionBase::ObExprCalcPartCtx *calc_part_ctx)
{
  int ret = OB_SUCCESS;
  ObObj part_val_obj;
  const char *payload = NULL;
  ObLength len = 0;
  bool is_null = false;
  vec->get_payload(row_idx, is_null, payload, len);
  ObDatum datum(payload, len, is_null);

  ObFixedArray<RangePartition, common::ObIAllocator>::const_iterator upper_res =
        std::upper_bound(calc_part_ctx->range_partitions_.begin(),
                        calc_part_ctx->range_partitions_.end(),
                        datum,
                        calc_part_ctx->part_cmp_);
  if (OB_FAIL(calc_part_ctx->part_cmp_.ret_)) {
    LOG_WARN("partition_cmp failed", K(calc_part_ctx->part_cmp_.ret_));
  } else {
    int64_t part_idx = upper_res - calc_part_ctx->range_partitions_.begin();
    if (OB_UNLIKELY(part_idx < 0 || part_idx >= part_num)) {
      partition = nullptr;
    } else if (OB_ISNULL(partition = part_array[part_idx])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("partition is null", KR(ret));
    }
  }
  return ret;
}

template <typename ArgVec>
static int inner_fast_calc_list_partition_level_one_vector(ArgVec *vec,
                                                          const int64_t row_idx,
                                                          const int64_t part_num,
                                                          ObPartition * const* part_array,
                                                          ObPartition *&partition,
                                                          const ObExpr *part_expr,
                                                          const ObExprCalcPartitionBase::ObExprCalcPartCtx *calc_part_ctx)
{
  int ret = OB_SUCCESS;
  ObObj part_val_obj;
  const char *payload = NULL;
  ObLength len = 0;
  bool is_null = false;
  vec->get_payload(row_idx, is_null, payload, len);
  ObDatum datum(payload, len, is_null);
  PartValKey tmp_row(datum, part_expr->basic_funcs_->murmur_hash_v2_,
        part_expr->basic_funcs_->null_first_cmp_);
  int64_t part_idx = OB_INVALID_INDEX;
  if (OB_FAIL(calc_part_ctx->list_part_map_.get_refactored(tmp_row, part_idx))) {
    if (ret == OB_HASH_NOT_EXIST) {
      ret = OB_SUCCESS;
      part_idx = calc_part_ctx->default_list_part_idx_;
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_UNLIKELY(OB_INVALID_INDEX == part_idx)) {
      partition = nullptr;
    } else if (OB_ISNULL(partition = part_array[part_idx])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("partition is null", KR(ret), K(part_idx));
    }
  }
  return ret;
}

template <typename ArgVec, typename ResVec,
          ObExprCalcPartitionBase::PartType Type,
          CalcPartIdType CalcIdType,
          bool IsOracleMode>
static int inner_fast_calc_partition_level_one_vector(const ObExpr &expr,
                                 ObEvalCtx &ctx,
                                 const ObBitVector &skip,
                                 const EvalBound &bound,
                                 const share::schema::ObTableSchema *table_schema)
{
  int ret = OB_SUCCESS;
  const ObExpr *part_expr = expr.args_[0];
  ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
  ArgVec *vec = static_cast<ArgVec *>(part_expr->get_vector(ctx));
  ResVec *res_vec = static_cast<ResVec *>(expr.get_vector(ctx));
  const int64_t part_num = table_schema->get_partition_num();
  ObPartition * const* part_array = table_schema->get_part_array();
  ObExprCalcPartitionBase::ObExprCalcPartCtx *calc_part_ctx = NULL;
  if (OB_UNLIKELY(OB_ISNULL(part_array) || part_num <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("partition_array is null or partition_num is invalid",
            KR(ret), KP(part_array), K(part_num));
  }
  int64_t powN = 0;
  if (OB_SUCC(ret) && IsOracleMode &&
      (Type == ObExprCalcPartitionBase::PartType::HASH ||
       Type == ObExprCalcPartitionBase::PartType::KEY)) {
    int64_t N = static_cast<int64_t>(std::log(part_num) / std::log(2));
    const static int64_t max_part_num_log2 = 64;
    if (N >= max_part_num_log2) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("result is too big", K(N), K(part_num));
    }
    powN = (1ULL << N);
  }
  if (OB_SUCC(ret) && (Type == ObExprCalcPartitionBase::PartType::RANGE ||
      Type == ObExprCalcPartitionBase::PartType::LIST)) {
    uint64_t calc_part_ctx_id = static_cast<uint64_t>(expr.expr_ctx_id_);
    if (OB_ISNULL(calc_part_ctx = static_cast<ObExprCalcPartitionBase::ObExprCalcPartCtx *>(
          ctx.exec_ctx_.get_expr_op_ctx(calc_part_ctx_id)))
        && OB_FAIL(ctx.exec_ctx_.create_expr_op_ctx(calc_part_ctx_id, calc_part_ctx))) {
      LOG_WARN("failed to create operator ctx", K(ret));
    } else if (calc_part_ctx->inited_) {
      // base info for calc range/list partition inited already
    } else if (Type == ObExprCalcPartitionBase::PartType::RANGE) {
      if (OB_FAIL(calc_part_ctx->init_calc_range_partition_base_info(*table_schema,
                  *part_expr, ctx.exec_ctx_.get_allocator()))) {
        LOG_WARN("Fail to init range partition base info", K(ret));
      } else {
        calc_part_ctx->inited_ = true;
      }
    } else { // LIST
      if (OB_FAIL(calc_part_ctx->init_calc_list_partition_base_info(*table_schema,
                  *part_expr, ctx.exec_ctx_.get_allocator()))) {
        LOG_WARN("Fail to init list partition base info", K(ret));
      } else {
        calc_part_ctx->inited_ = true;
      }
    }
  }
  for (int64_t row_idx = bound.start(); row_idx < bound.end() && OB_SUCC(ret); ++row_idx) {
    if (skip.at(row_idx) || eval_flags.at(row_idx)) {
      continue;
    }
    ObPartition *partition = NULL;
    if (Type == ObExprCalcPartitionBase::PartType::HASH) {
      ret = inner_fast_calc_hash_partition_level_one_vector<ArgVec, IsOracleMode>(
        vec, row_idx, part_num, part_array, partition, powN, (GetPayloadFunc)(expr.inner_functions_[0]));
    } else if(Type == ObExprCalcPartitionBase::PartType::KEY) {
      ret = inner_fast_calc_key_partition_level_one_vector<ArgVec, IsOracleMode>(
        vec, row_idx, part_num, part_array, partition, powN);
    } else if (Type == ObExprCalcPartitionBase::PartType::RANGE) {
      ret = inner_fast_calc_range_partition_level_one_vector<ArgVec>(
        vec, row_idx, part_num, part_array, partition, calc_part_ctx);
    } else if (Type == ObExprCalcPartitionBase::PartType::LIST) {
      ret = inner_fast_calc_list_partition_level_one_vector<ArgVec>(
        vec, row_idx, part_num, part_array, partition, part_expr, calc_part_ctx);
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected part_type", K(Type), K(ret));
    }
    if (OB_SUCC(ret)) {
      // Fill in the result
      if (CalcIdType == CALC_TABLET_ID) {
        if (OB_NOT_NULL(partition)) {
          res_vec->set_int(row_idx, partition->get_tablet_id().id());
        } else {
          res_vec->set_int(row_idx, ObTabletID::INVALID_TABLET_ID);
        }
        eval_flags.set(row_idx);
      } else if (CalcIdType == CALC_PARTITION_ID) {
        if (OB_NOT_NULL(partition)) {
          res_vec->set_int(row_idx, partition->get_part_id());
        } else {
          res_vec->set_int(row_idx, OB_INVALID_ID);
        }
        eval_flags.set(row_idx);
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected calc_id_type", K(CalcIdType), K(ret));
      }
    }
  }
  return ret;
}

#define PART_DISPATCH_VECTOR_IN_ARG_FORMAT(func_name, is_oracle_mode, part_type, calc_id_type, res_vec)    \
if (arg_format == VEC_FIXED) {                                                                             \
  ret = func_name<ObFixedLengthBase, res_vec, part_type, calc_id_type, is_oracle_mode>(                    \
                  expr, ctx, skip, bound, table_schema);                                                   \
} else if (arg_format == VEC_UNIFORM) {                                                                    \
  ret = func_name<ObUniformFormat<false>, res_vec, part_type, calc_id_type, is_oracle_mode>(               \
                  expr, ctx, skip, bound, table_schema);                                                   \
} else if (arg_format == VEC_DISCRETE) {                                                                   \
  ret = func_name<ObDiscreteFormat, res_vec, part_type, calc_id_type, is_oracle_mode>(                     \
                  expr, ctx, skip, bound, table_schema);                                                   \
} else if (arg_format == VEC_CONTINUOUS) {                                                                 \
  ret = func_name<ObContinuousFormat, res_vec, part_type, calc_id_type, is_oracle_mode>(                   \
                  expr, ctx, skip, bound, table_schema);                                                   \
} else if (arg_format == VEC_UNIFORM_CONST) {                                                              \
  ret = func_name<ObUniformFormat<true>, res_vec, part_type, calc_id_type, is_oracle_mode>(                \
                  expr, ctx, skip, bound, table_schema);                                                   \
} else {                                                                                                   \
  ret = func_name<ObVectorBase, res_vec, part_type, calc_id_type, is_oracle_mode>(                         \
                  expr, ctx, skip, bound, table_schema);                                                   \
}

#define PART_DISPATCH_VECTOR_IN_RES_FORMAT(func_name, is_oracle_mode, part_type, calc_id_type)                          \
if (res_format == VEC_FIXED) {                                                                                          \
  PART_DISPATCH_VECTOR_IN_ARG_FORMAT(func_name, is_oracle_mode, part_type, calc_id_type, ObFixedLengthBase);            \
} else if (res_format == VEC_UNIFORM) {                                                                                 \
  PART_DISPATCH_VECTOR_IN_ARG_FORMAT(func_name, is_oracle_mode, part_type, calc_id_type, ObUniformFormat<false>);       \
} else if (res_format == VEC_UNIFORM_CONST) {                                                                           \
  PART_DISPATCH_VECTOR_IN_ARG_FORMAT(func_name, is_oracle_mode, part_type, calc_id_type, ObUniformFormat<true>);        \
} else {                                                                                                                \
  PART_DISPATCH_VECTOR_IN_ARG_FORMAT(func_name, is_oracle_mode, part_type, calc_id_type, ObVectorBase);                 \
}

#define PART_DISPATCH_VECTOR_IN_CALC_ID_TYPE(func_name, is_oracle_mode, part_type)             \
if (calc_part_info->calc_id_type_ == CALC_TABLET_ID) {                                         \
  PART_DISPATCH_VECTOR_IN_RES_FORMAT(func_name, is_oracle_mode, part_type, CALC_TABLET_ID);    \
} else if (calc_part_info->calc_id_type_ == CALC_PARTITION_ID) {                               \
  PART_DISPATCH_VECTOR_IN_RES_FORMAT(func_name, is_oracle_mode, part_type, CALC_PARTITION_ID); \
} else {                                                                                       \
  ret = OB_ERR_UNEXPECTED;                                                                     \
  LOG_WARN("unexpected calc id type", K(ret), K(calc_part_info->calc_id_type_));               \
}

#define PART_DISPATCH_VECTOR_IN_PART_TYPE(func_name, is_oracle_mode)                                           \
if (table_schema->is_hash_part()) {                                                                            \
  PART_DISPATCH_VECTOR_IN_CALC_ID_TYPE(func_name, is_oracle_mode, ObExprCalcPartitionBase::PartType::HASH);    \
} else if (table_schema->is_key_part()) {                                                                      \
  PART_DISPATCH_VECTOR_IN_CALC_ID_TYPE(func_name, is_oracle_mode, ObExprCalcPartitionBase::PartType::KEY);     \
} else if (table_schema->is_range_part()) {                                                                    \
  PART_DISPATCH_VECTOR_IN_CALC_ID_TYPE(func_name, is_oracle_mode, ObExprCalcPartitionBase::PartType::RANGE);   \
} else if (table_schema->is_list_part()) {                                                                     \
  PART_DISPATCH_VECTOR_IN_CALC_ID_TYPE(func_name, is_oracle_mode, ObExprCalcPartitionBase::PartType::LIST);    \
} else {                                                                                                       \
  ret = OB_ERR_UNEXPECTED;                                                                                     \
  LOG_WARN("unexpected part type", K(ret));                                                                    \
}

#define PART_DISPATCH_VECTOR_IN_ORACLE_MODE(func_name)    \
if (lib::is_oracle_mode()) {                              \
  PART_DISPATCH_VECTOR_IN_PART_TYPE(func_name, true);     \
} else {                                                  \
  PART_DISPATCH_VECTOR_IN_PART_TYPE(func_name, false);    \
}

int ObExprCalcPartitionBase::fast_calc_partition_level_one_vector(const ObExpr &expr,
                                                             ObEvalCtx &ctx,
                                                             const ObBitVector &skip,
                                                             const EvalBound &bound)
{
  int ret = OB_SUCCESS;
  // The value is invalid,
  // indicating that the corresponding plan was CG by an observer with a lower version.
  // Revert to using the non-fast path directly.
  if (expr.expr_ctx_id_ == ObExpr::INVALID_EXP_CTX_ID) {
    if (OB_FAIL(calc_partition_level_one_vector(expr, ctx, skip, bound))) {
      LOG_WARN("Fail to calc partition level one vector", K(ret));
    }
  } else {
    CalcPartitionBaseInfo *calc_part_info = reinterpret_cast<CalcPartitionBaseInfo *>(expr.extra_info_);
    ObDASTabletMapper tablet_mapper;
    const share::schema::ObTableSchema *table_schema = nullptr;
    const ObExpr *part_expr = expr.args_[0];
    if (OB_FAIL(ctx.exec_ctx_.get_das_ctx().get_das_tablet_mapper(calc_part_info->ref_table_id_,
                                                                  tablet_mapper,
                                                                  &calc_part_info->related_table_ids_))) {
      LOG_WARN("get das tablet mapper failed", K(ret), K(calc_part_info));
    } else if (OB_ISNULL(table_schema = tablet_mapper.get_table_schema())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table_schema is null.", K(ret));
    } else if (OB_FAIL(part_expr->eval_vector(ctx, skip, bound))) {
      LOG_WARN("calc part expr failed", K(ret));
    } else {
      VectorFormat res_format = expr.get_format(ctx);
      VectorFormat arg_format = expr.args_[0]->get_format(ctx);
      PART_DISPATCH_VECTOR_IN_ORACLE_MODE(inner_fast_calc_partition_level_one_vector);
    }
  }
  return ret;
}

int ObExprCalcPartitionBase::calc_partition_level_one_vector(const ObExpr &expr,
                                                             ObEvalCtx &ctx,
                                                             const ObBitVector &skip,
                                                             const EvalBound &bound)
{
  int ret = OB_SUCCESS;
  CalcPartitionBaseInfo *calc_part_info = reinterpret_cast<CalcPartitionBaseInfo *>(expr.extra_info_);
  ObPartitionLevel part_level = PARTITION_LEVEL_ONE;
  ObPartitionFuncType part_type = calc_part_info->part_type_;
  ObDASTabletMapper tablet_mapper;
  const ObExpr *part_expr = expr.args_[0];
  const bool is_oracle_mode = NULL != ctx.exec_ctx_.get_my_session() &&
                            ORACLE_MODE == ctx.exec_ctx_.get_my_session()->get_compatibility_mode();
  if (OB_FAIL(ctx.exec_ctx_.get_das_ctx().get_das_tablet_mapper(calc_part_info->ref_table_id_,
                                                                tablet_mapper,
                                                                &calc_part_info->related_table_ids_))) {
    LOG_WARN("get das tablet mapper failed", K(ret), K(calc_part_info));
  } else if (T_OP_ROW == part_expr->type_) {
    for (int64_t i = 0; OB_SUCC(ret) && i < part_expr->arg_cnt_; i++) {
      if (OB_FAIL(part_expr->args_[i]->eval_vector(ctx, skip, bound))) {
        LOG_WARN("fail to eval child of part expr", K(ret), K(i), KPC(part_expr));
      }
    }
    ObNewRow row;
    ObEvalCtx::TempAllocGuard alloc_guard(ctx);
    ObIAllocator &allocator = alloc_guard.get_allocator();
    ObSEArray<ObIVector *, 8> part_child_vecs;
    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(row.cells_ = static_cast<ObObj *>(
                allocator.alloc(sizeof(ObObj) * part_expr->arg_cnt_)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate memory failed", K(ret));
    } else {
      row.count_ = part_expr->arg_cnt_;
      row.projector_size_ = 0;
      row.projector_ = NULL;
    }
    for (int64_t i = 0; i < part_expr->arg_cnt_ && OB_SUCC(ret); i++) {
      ObIVector *vec = part_expr->args_[i]->get_vector(ctx);
      if (OB_FAIL(part_child_vecs.push_back(vec))) {
        LOG_WARN("push back failed", K(ret));
      }
    }
    ObIVector *res_vec = expr.get_vector(ctx);
    res_vec->reset_has_null();
    ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
    for (int64_t row_idx = bound.start(); row_idx < bound.end() && OB_SUCC(ret); row_idx++) {
      if (skip.contain(row_idx) || eval_flags.at(row_idx)) {
        continue;
      }
      ObTabletID tablet_id(ObTabletID::INVALID_TABLET_ID);
      ObObjectID partition_id = OB_INVALID_ID;
      for (int64_t i = 0; i < part_expr->arg_cnt_ && OB_SUCC(ret); i++) {
        new (&row.cells_[i]) ObObj();
        ObIVector *vec = part_child_vecs.at(i);
        bool is_null = vec->is_null(row_idx);
        const char *payload = NULL;
        ObLength len = 0;
        vec->get_payload(row_idx, payload, len);
        ObDatum datum(payload, len, is_null);
        if (OB_FAIL(datum.to_obj(row.cells_[i], part_expr->args_[i]->obj_meta_,
                    part_expr->args_[i]->obj_datum_map_))) {
          LOG_WARN("to obj failed", K(ret));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(tablet_mapper.get_tablet_and_object_id(part_level, OB_INVALID_ID, row,
                                                                tablet_id, partition_id))) {
        LOG_WARN("Failed to get part id", K(ret), K(row));
      } else if (OB_INVALID_ID == partition_id && PARTITION_LEVEL_ONE == part_level
                 && is_oracle_mode
                 && OB_FAIL(add_interval_part(ctx.exec_ctx_, *calc_part_info, allocator, row))) {
        LOG_WARN("add interval part failed", K(ret), KPC(calc_part_info), K(row));
      } else {
        res_vec->unset_null(row_idx);
        eval_flags.set(row_idx);
        if (CALC_TABLET_ID == calc_part_info->calc_id_type_) {
          res_vec->set_int(row_idx, tablet_id.id());
        } else if (CALC_PARTITION_ID == calc_part_info->calc_id_type_) {
          res_vec->set_int(row_idx, partition_id);
        } else if (CALC_PARTITION_TABLET_ID == calc_part_info->calc_id_type_) {
          uint64_t *payload = reinterpret_cast<uint64_t *>(const_cast<char *>(res_vec->get_payload(row_idx)));
          payload[0] = partition_id;
          payload[1] = tablet_id.id();
          res_vec->set_payload_shallow(row_idx, payload, sizeof(uint64_t) * 2);
        }
      }
    }
  } else {
    if (OB_FAIL(part_expr->eval_vector(ctx, skip, bound))) {
      LOG_WARN("calc part expr failed", K(ret));
    } else {
      ObIVector *vec = part_expr->get_vector(ctx);
      ObIVector *res_vec = expr.get_vector(ctx);
      res_vec->reset_has_null();
      ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
      const bool is_oracle_mode = NULL != ctx.exec_ctx_.get_my_session() &&
                            ORACLE_MODE == ctx.exec_ctx_.get_my_session()->get_compatibility_mode();
      for (int64_t row_idx = bound.start(); row_idx < bound.end() && OB_SUCC(ret); row_idx++) {
        if (skip.contain(row_idx) || eval_flags.at(row_idx)) {
          continue;
        }
        ObObj func_value;
        ObObj result;
        const char *payload = NULL;
        ObLength len = 0;
        vec->get_payload(row_idx, payload, len);
        ObDatum datum(payload, len, vec->is_null(row_idx));
        if (OB_FAIL(datum.to_obj(func_value,
                                 part_expr->obj_meta_,
                                 part_expr->obj_datum_map_))) {
          LOG_WARN("convert datum to obj failed", K(ret));
        } else {
          result = func_value;
          if (PARTITION_FUNC_TYPE_HASH == part_type) {
            if (is_oracle_mode) {
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
            if (OB_FAIL(range.build_range(calc_part_info->ref_table_id_, rowkey))) {
              LOG_WARN("Failed to build range", K(ret));
            } else if (OB_FAIL(tablet_mapper.get_tablet_and_object_id(
                                              part_level,
                                              OB_INVALID_ID,
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
                  is_oracle_mode) {
                ObEvalCtx::TempAllocGuard alloc_guard(ctx);
                ObIAllocator &allocator = alloc_guard.get_allocator();
                ObNewRow row(const_cast<ObObj*>(&result), 1);
                OZ (add_interval_part(ctx.exec_ctx_, *calc_part_info, allocator, row));
              }
              ObTabletID tablet_id(ObTabletID::INVALID_TABLET_ID);
              ObObjectID partition_id = OB_INVALID_ID;
              if (OB_SUCC(ret) && 1 == partition_ids.count()) {
                partition_id = partition_ids.at(0);
                if (1 == tablet_ids.count()) {
                  tablet_id = tablet_ids.at(0);
                }
              }
              if (OB_SUCC(ret)) {
                res_vec->unset_null(row_idx);
                eval_flags.set(row_idx);
                if (CALC_TABLET_ID == calc_part_info->calc_id_type_) {
                  res_vec->set_int(row_idx, tablet_id.id());
                } else if (CALC_PARTITION_ID == calc_part_info->calc_id_type_) {
                  res_vec->set_int(row_idx, partition_id);
                } else if (CALC_PARTITION_TABLET_ID == calc_part_info->calc_id_type_) {
                  uint64_t *payload = reinterpret_cast<uint64_t *>(const_cast<char *>(res_vec->get_payload(row_idx)));
                  payload[0] = partition_id;
                  payload[1] = tablet_id.id();
                  res_vec->set_payload_shallow(row_idx, payload, sizeof(uint64_t) * 2);
                }
              }
            }
          }
        }
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
  PartitionIdCalcType calc_type = calc_part_info->partition_id_calc_type_;
  ObObjectID first_part_id = OB_INVALID_ID;
  ObTabletID tablet_id(ObTabletID::INVALID_TABLET_ID);
  ObObjectID partition_id = OB_INVALID_ID;
  if (CALC_IGNORE_FIRST_PART == calc_type) {
    int64_t first_part_id = OB_INVALID_ID;
    if (OB_FAIL(get_first_part_id(ctx.exec_ctx_, expr, first_part_id))) {
      LOG_WARN("get first part id failed", K(ret));
    } else if (OB_FAIL(calc_partition_id(*expr.args_[1],
                                        ctx,
                                        *calc_part_info,
                                        first_part_id,
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

bool PartValKey::operator==(const PartValKey &other) const
{
  int res = true;
  cmp_func_(datum_, other.datum_, res);
  return res == 0;
}

int PartValKey::hash(uint64_t &hash_val, uint64_t seed) const
{
  return hash_func_(datum_, seed, hash_val);
}

bool RangePartCmp::operator()(const ObDatum &l, const RangePartition &r) {
  int ret = OB_SUCCESS;
  int cmp_ret = 0;
  bool res = false;

  if (r.is_max_range_part()) {
    res = true;
  } else if (l.is_null()) {
    // In part calc, MySQL treats null values as infinitely small,
    // while Oracle treats them as infinitely large.
    res = is_oracle_mode_ ? false : true;
  } else {
    ret_ = (row_cmp_func_)(part_expr_obj_meta_,
                          part_array_obj_meta_,
                          (const void *)l.ptr_, l.len_,
                          (const void *)r.datum_.ptr_, r.datum_.len_, cmp_ret);
    res = cmp_ret < 0;
  }
  return res;
}

int ObExprCalcPartitionBase::ObExprCalcPartCtx::init_calc_range_partition_base_info(
                                                const share::schema::ObTableSchema &table_schema,
                                                const ObExpr &part_expr,
                                                common::ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  const int64_t part_num = table_schema.get_partition_num();
  ObPartition * const* part_array = table_schema.get_part_array();
  range_partitions_.set_allocator(&allocator);
  if (OB_FAIL(range_partitions_.prepare_allocate(part_num))) {
    LOG_WARN("Fail to prepare_allocate", K(ret), K(part_num));
  }
  ObDatum tmp_datum;
  char buf[OBJ_DATUM_MAX_RES_SIZE];
  tmp_datum.ptr_ = buf;
  for (int i = 0; OB_SUCC(ret) && i < part_num; ++i) {
    if (part_array[i]->get_high_bound_val().is_max_row()) {
      range_partitions_.at(i).set_max_range_part();
    } else if (OB_FAIL(tmp_datum.from_obj(
            *(part_array[i]->get_high_bound_val().get_obj_ptr())))) {
      LOG_WARN("Fail to from obj", K(ret), K(i));
    } else if (OB_FAIL(range_partitions_.at(i).datum_.deep_copy(tmp_datum,
                                        allocator))) {
      LOG_WARN("failed to deep copy datum");
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_ISNULL(part_cmp_.row_cmp_func_ = VectorCmpExprFuncsHelper::get_row_cmp_func(
                                          part_expr.datum_meta_,
                                          part_expr.datum_meta_))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("row_cmp_func is null", K(ret), K(part_expr.datum_meta_));
    } else {
      part_cmp_.part_expr_obj_meta_ = part_expr.obj_meta_;
      part_cmp_.part_array_obj_meta_ =
          part_array[0]->get_high_bound_val().get_obj_ptr()->get_meta();
      part_cmp_.is_oracle_mode_ = lib::is_oracle_mode();
    }
  }
  return ret;
}

int ObExprCalcPartitionBase::ObExprCalcPartCtx::init_calc_list_partition_base_info(
                                            const share::schema::ObTableSchema &table_schema,
                                            const ObExpr &part_expr,
                                            common::ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  const int64_t part_num = table_schema.get_partition_num();
  ObPartition * const* part_array = table_schema.get_part_array();
  int64_t list_val_cnt = 0;
  for (int64_t i = 0; OB_SUCC(ret) && i < part_num; ++i) {
    const ObIArray<common::ObNewRow> &list_row_values =
            part_array[i]->get_list_row_values();
    list_val_cnt += list_row_values.count();
    // calc default value position
    if (list_row_values.count() == 1
    && list_row_values.at(0).get_count() >= 1
    && list_row_values.at(0).get_cell(0).is_max_value()) {
      default_list_part_idx_ = i;
    }
  }
  if (OB_SUCC(ret)) {
    ObMemAttr list_part_map_attr(MTL_ID(), "LISTPART");
    if (OB_FAIL(list_part_map_.create(list_val_cnt * 2,
                                  list_part_map_attr, list_part_map_attr))) {
      LOG_WARN("create interm_res hash table failed", K(ret));
    } else {
      ObDatum list_part_datum;
      char buf[OBJ_DATUM_MAX_RES_SIZE];
      list_part_datum.ptr_ = buf;
      for (int64_t i = 0; OB_SUCC(ret) && i < part_num; ++i) {
        if (i == default_list_part_idx_) {
          continue;
        }
        const ObIArray<common::ObNewRow> &list_row_values =
              part_array[i]->get_list_row_values();
        for (int64_t j = 0; OB_SUCC(ret) && j < list_row_values.count(); ++j) {
          ObObj *list_part_obj = list_row_values.at(j).cells_;
          if (OB_FAIL(list_part_datum.from_obj(*list_part_obj))) {
            LOG_WARN("Fail to from obj", K(ret), K(i));
          } else {
            PartValKey list_part_row;
            if (OB_FAIL(list_part_row.datum_.deep_copy(list_part_datum,
                                        allocator))) {
              LOG_WARN("failed to deep copy datum");
            } else {
              list_part_row.hash_func_ = part_expr.basic_funcs_->murmur_hash_v2_;
              list_part_row.cmp_func_ = part_expr.basic_funcs_->null_first_cmp_;
              if (OB_FAIL(list_part_map_.set_refactored(list_part_row, i))) {
                LOG_WARN("Fail to set_refactored", K(ret), K(i), K(j));
              }
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObExprCalcPartitionBase::get_first_part_id(ObExecContext &ctx, const ObExpr &expr, int64_t &first_part_id)
{
  int ret = OB_SUCCESS;
  first_part_id = OB_INVALID_ID;
  uint64_t expr_ctx_id = static_cast<uint64_t>(expr.expr_ctx_id_);
  if (ObExpr::INVALID_EXP_CTX_ID == expr_ctx_id) {
    // during upgrade, expr ctx not exist.
    CalcPartitionBaseInfo *calc_part_info = reinterpret_cast<CalcPartitionBaseInfo *>(expr.extra_info_);
    if (OB_ISNULL(calc_part_info)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("extra info is null", K(ret));
    } else {
      first_part_id = calc_part_info->first_part_id_;
    }
  } else {
    ObExprCalcPartCtx *calc_part_ctx = NULL;
    if (OB_ISNULL(calc_part_ctx = static_cast<ObExprCalcPartCtx *>(ctx.get_expr_op_ctx(expr_ctx_id)))
        && OB_FAIL(ctx.create_expr_op_ctx(expr_ctx_id, calc_part_ctx))) {
      LOG_WARN("create expr op ctx failed", K(ret));
    } else {
      first_part_id = calc_part_ctx->first_part_id_;
    }
  }
  return ret;
}

int ObExprCalcPartitionBase::set_first_part_id(ObExecContext &ctx, const ObExpr &expr, const int64_t first_part_id)
{
  int ret = OB_SUCCESS;
  uint64_t expr_ctx_id = static_cast<uint64_t>(expr.expr_ctx_id_);
  if (ObExpr::INVALID_EXP_CTX_ID == expr_ctx_id) {
    // during upgrade, expr ctx not exist.
    CalcPartitionBaseInfo *calc_part_info = reinterpret_cast<CalcPartitionBaseInfo *>(expr.extra_info_);
    if (OB_ISNULL(calc_part_info)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("extra info is null", K(ret));
    } else {
      calc_part_info->first_part_id_ = first_part_id;
    }
  } else {
    ObExprCalcPartCtx *calc_part_ctx = NULL;
    if (OB_ISNULL(calc_part_ctx = static_cast<ObExprCalcPartCtx *>(ctx.get_expr_op_ctx(expr_ctx_id)))
        && OB_FAIL(ctx.create_expr_op_ctx(expr_ctx_id, calc_part_ctx))) {
      LOG_WARN("create expr op ctx failed", K(ret));
    } else {
      calc_part_ctx->first_part_id_ = first_part_id;
    }
  }
  return ret;
}

int ObExprCalcPartitionBase::update_part_id_calc_type_for_upgrade(
    ObExecContext &ctx,
    const ObExpr &expr,
    PartitionIdCalcType calc_type)
{
  int ret = OB_SUCCESS;
  uint64_t expr_ctx_id = static_cast<uint64_t>(expr.expr_ctx_id_);
  if (ObExpr::INVALID_EXP_CTX_ID == expr_ctx_id) {
    // 混跑要动态改partition_id_calc_type，434以下不会设置这个expr_ctx_id_
    CalcPartitionBaseInfo *calc_part_info = reinterpret_cast<CalcPartitionBaseInfo *>(expr.extra_info_);
    if (OB_ISNULL(calc_part_info)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("extra info is null", K(ret));
    } else {
      calc_part_info->partition_id_calc_type_ = calc_type;
    }
  }
  return ret;
}

}
}
