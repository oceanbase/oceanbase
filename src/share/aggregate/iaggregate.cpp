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
#include "iaggregate.h"
#include "share/datum/ob_datum_util.h"
#include "share/aggregate/first_row.h"

namespace oceanbase
{
namespace share
{
namespace aggregate
{
namespace helper
{
using namespace sql;
using namespace common;

extern int init_count_aggregate(RuntimeContext &agg_ctx,
                                const int64_t agg_col_id, ObIAllocator &allocator,
                                IAggregate *&agg);
extern int init_min_aggregate(RuntimeContext &agg_ctx, const int64_t agg_col_id,
                              ObIAllocator &allocator, IAggregate *&agg);
extern int init_max_aggregate(RuntimeContext &agg_ctx, const int64_t agg_col_id,
                              ObIAllocator &allocator, IAggregate *&agg);
extern int init_sum_aggregate(RuntimeContext &agg_ctx, const int64_t agg_col_id,
                              ObIAllocator &allocator, IAggregate *&agg, int32 *tmp_res_size = NULL);
extern int init_count_sum_aggregate(RuntimeContext &agg_ctx, const int64_t agg_col_id,
                                    ObIAllocator &allocator, IAggregate *&agg);
extern int init_approx_count_distinct_aggregate(RuntimeContext &agg_ctx, const int64_t agg_col_id,
                                                ObIAllocator &allocator, IAggregate *&agg);
extern int init_sysbit_aggregate(RuntimeContext &agg_ctx, const int64_t agg_col_id,
                                 ObIAllocator &allocator, IAggregate *&agg);
#define INIT_AGGREGATE_CASE(OP_TYPE, func_name, col_id)                                            \
  case (OP_TYPE): {                                                                                \
    ret = init_##func_name##_aggregate(agg_ctx, col_id, allocator, aggregate);                     \
  } break
int init_aggregates(RuntimeContext &agg_ctx, ObIAllocator &allocator,
                    ObIArray<IAggregate *> &aggregates)
{
  int ret = OB_SUCCESS;
  ObEvalCtx &ctx= agg_ctx.eval_ctx_;
  if (OB_FAIL(agg_ctx.init_row_meta(agg_ctx.aggr_infos_, allocator))) {
    SQL_LOG(WARN, "init row meta failed", K(ret));
  }
  for (int i = 0; OB_SUCC(ret) && i < agg_ctx.aggr_infos_.count(); i++) {
    ObAggrInfo &aggr_info = agg_ctx.locate_aggr_info(i);
    IAggregate *aggregate = nullptr;
    if (aggr_info.is_implicit_first_aggr()) {
      if (OB_FAIL(init_first_row_aggregate(agg_ctx, i, allocator, aggregate))) {
        SQL_LOG(WARN, "init first row aggregate failed", K(ret));
      }
    } else {
      ObExprOperatorType fun_type =
        (aggr_info.expr_->type_ == T_WINDOW_FUNCTION ? aggr_info.real_aggr_type_ :
                                                       aggr_info.expr_->type_);
      switch (fun_type) {
        INIT_AGGREGATE_CASE(T_FUN_MIN, min, i);
        INIT_AGGREGATE_CASE(T_FUN_MAX, max, i);
        INIT_AGGREGATE_CASE(T_FUN_COUNT, count, i);
        INIT_AGGREGATE_CASE(T_FUN_SUM, sum, i);
        INIT_AGGREGATE_CASE(T_FUN_COUNT_SUM, count_sum, i);
        INIT_AGGREGATE_CASE(T_FUN_APPROX_COUNT_DISTINCT, approx_count_distinct, i);
        INIT_AGGREGATE_CASE(T_FUN_APPROX_COUNT_DISTINCT_SYNOPSIS, approx_count_distinct, i);
        INIT_AGGREGATE_CASE(T_FUN_APPROX_COUNT_DISTINCT_SYNOPSIS_MERGE, approx_count_distinct, i);
        INIT_AGGREGATE_CASE(T_FUN_SYS_BIT_OR, sysbit, i);
        INIT_AGGREGATE_CASE(T_FUN_SYS_BIT_AND, sysbit, i);
        INIT_AGGREGATE_CASE(T_FUN_SYS_BIT_XOR, sysbit, i);
      default: {
        ret = OB_NOT_SUPPORTED;
        SQL_LOG(WARN, "not supported aggregate function", K(ret), K(aggr_info.expr_->type_));
      }
      }
      if (OB_FAIL(ret)) {
        SQL_LOG(WARN, "init aggregate failed", K(ret));
      } else if (OB_ISNULL(aggregate)) {
        ret = OB_ERR_UNEXPECTED;
        SQL_LOG(WARN, "unexpected null aggregate", K(ret));
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(aggregates.push_back(aggregate))) {
      SQL_LOG(WARN, "push back element failed", K(ret));
    }
  } // end for
  return ret;
}

#undef INIT_AGGREGATE_CASE

static int32_t agg_cell_tmp_res_size(RuntimeContext &agg_ctx, int64_t agg_col_id)
{
  int ret = OB_SUCCESS;
  int ret_size = 0;
  int32_t tmp_res_size = 0;
  char buffer[1] = {0};
  IAggregate *agg = nullptr;
  ObDataBuffer local_allocator(buffer, 1);
  ObAggrInfo &info = agg_ctx.aggr_infos_.at(agg_col_id);
  if (info.is_implicit_first_aggr()) {
    // do nothing
  } else if (info.get_expr_type() == T_FUN_MIN || info.get_expr_type() == T_FUN_MAX) {
    VecValueTypeClass vec_tc = info.expr_->get_vec_value_tc();
    if (is_var_len_agg_cell(vec_tc)) {
      ret_size = sizeof(char *) + sizeof(int32_t); // <char *, int32_t>
    }
  } else if (info.get_expr_type() == T_FUN_SUM) {
    if (OB_FAIL( // ugly code, works just fine
          init_sum_aggregate(agg_ctx, agg_col_id, local_allocator, agg, &tmp_res_size))) {
      SQL_LOG(ERROR, "get sum tmp res size failed", K(ret));
      ob_abort();
    } else {
      ret_size = tmp_res_size;
    }
  } else if (info.get_expr_type() == T_FUN_APPROX_COUNT_DISTINCT) {
    ret_size = sizeof(char *);
  }
  return ret_size;
}

static int32_t reserved_agg_col_size(RuntimeContext &agg_ctx, int64_t agg_col_id)
{
  int ret_size = 0;
  ObAggrInfo &aggr_info = agg_ctx.aggr_infos_.at(agg_col_id);
  const int64_t constexpr string_reserved_size = sizeof(char *) + sizeof(int32_t); // <char *, len>
#define RTSIZE(vec_tc) sizeof(RTCType<vec_tc>)
  static const int32_t reserved_sizes[] = {
    0,                                    // NULL
    RTSIZE(VEC_TC_INTEGER),               // integer
    RTSIZE(VEC_TC_UINTEGER),              // uinteger
    RTSIZE(VEC_TC_FLOAT),                 // float
    RTSIZE(VEC_TC_DOUBLE),                // double
    RTSIZE(VEC_TC_FIXED_DOUBLE),          // fixed_double
    number::ObNumber::MAX_CALC_BYTE_LEN,  // number
    RTSIZE(VEC_TC_DATETIME),              // datetime
    RTSIZE(VEC_TC_DATE),                  // date
    RTSIZE(VEC_TC_TIME),                  // time
    RTSIZE(VEC_TC_YEAR),                  // year
    string_reserved_size,                 // extend
    0,                                    // unknown
    string_reserved_size,                 // string
    RTSIZE(VEC_TC_BIT),                   // bit
    RTSIZE(VEC_TC_ENUM_SET),              // enum set
    0,                                    // enum set inner
    RTSIZE(VEC_TC_TIMESTAMP_TZ),          // timestamp tz
    RTSIZE(VEC_TC_TIMESTAMP_TINY),        // timestamp tiny
    string_reserved_size,                 // raw
    RTSIZE(VEC_TC_INTERVAL_YM),           // interval ym
    RTSIZE(VEC_TC_INTERVAL_DS),           // interval ds
    string_reserved_size,                 // rowid
    string_reserved_size,                 // lob
    string_reserved_size,                 // json
    string_reserved_size,                 // geo
    string_reserved_size,                 // udt
    RTSIZE(VEC_TC_DEC_INT32),             // dec_int32
    RTSIZE(VEC_TC_DEC_INT64),             // dec_int64
    RTSIZE(VEC_TC_DEC_INT128),            // dec_int128
    RTSIZE(VEC_TC_DEC_INT256),            // dec_int256
    RTSIZE(VEC_TC_DEC_INT512),            // dec_int512
    string_reserved_size,                 // collection
    string_reserved_size,                 // roaringbitmap
  };
  static_assert(sizeof(reserved_sizes) / sizeof(reserved_sizes[0]) == MAX_VEC_TC, "");
  OB_ASSERT(aggr_info.expr_ != NULL);
  VecValueTypeClass res_tc = get_vec_value_tc(aggr_info.expr_->datum_meta_.type_,
                                              aggr_info.expr_->datum_meta_.scale_,
                                              aggr_info.expr_->datum_meta_.precision_);
  if (aggr_info.is_implicit_first_aggr()) {
    ret_size += string_reserved_size; // <char *, len>;
  } else if (aggr_info.get_expr_type() == T_FUN_COUNT) {
    // count returns ObNumberType in oracle mode,
    // we use int64_t as row counts recording type, and cast int64_t to ObNumberType in
    // `collect_group_result`
    ret_size += sizeof(int64_t);
  } else if (is_var_len_agg_cell(res_tc)) {
    ret_size += string_reserved_size;
  } else {
    ret_size += reserved_sizes[res_tc];
  }
  ret_size += agg_cell_tmp_res_size(agg_ctx, agg_col_id);
  return ret_size;
}

inline bool has_extra_info(ObAggrInfo &info)
{
  bool has = false;
  switch (info.get_expr_type()) {
  case T_FUN_GROUP_CONCAT:
  case T_FUN_GROUP_RANK:
  case T_FUN_GROUP_DENSE_RANK:
  case T_FUN_GROUP_PERCENT_RANK:
  case T_FUN_GROUP_CUME_DIST:
  case T_FUN_MEDIAN:
  case T_FUN_GROUP_PERCENTILE_CONT:
  case T_FUN_GROUP_PERCENTILE_DISC:
  case T_FUN_KEEP_MAX:
  case T_FUN_KEEP_MIN:
  case T_FUN_KEEP_SUM:
  case T_FUN_KEEP_COUNT:
  case T_FUN_KEEP_WM_CONCAT:
  case T_FUN_WM_CONCAT:
  case T_FUN_PL_AGG_UDF:
  case T_FUN_JSON_ARRAYAGG:
  case T_FUN_ORA_JSON_ARRAYAGG:
  case T_FUN_JSON_OBJECTAGG:
  case T_FUN_ORA_JSON_OBJECTAGG:
  case T_FUN_ORA_XMLAGG:
  case T_FUN_HYBRID_HIST:
  case T_FUN_TOP_FRE_HIST:
  case T_FUN_AGG_UDF: {
    has = true;
    break;
  }
  default: {
    break;
  }
  }
  has = has || info.has_distinct_;
  return has;
}
} // end namespace helper

int RuntimeContext::init_row_meta(ObIArray<ObAggrInfo> &aggr_infos, ObIAllocator &alloc)
{
  int ret = OB_SUCCESS;
  agg_row_meta_.row_size_ = 0;
  agg_row_meta_.col_cnt_ = aggr_infos.count();
  agg_row_meta_.extra_cnt_ = 0;
  int32_t offset = 0;
  bool has_extra = false;
  int64_t bit_vec_size =
    ((aggr_infos.count() + AggBitVector::word_bits() - 1) / AggBitVector::word_bits())
    * AggBitVector::word_size();
  uint8_t *bit_payload = nullptr;
  if (OB_ISNULL(agg_row_meta_.col_offsets_ = (int32_t *)alloc.alloc(
                  sizeof(int32_t)
                  * (aggr_infos.count() + 1)))) { // one extra offset to calculate column size
    ret = OB_ALLOCATE_MEMORY_FAILED;
    SQL_LOG(WARN, "allocate memory failed", K(ret));
  } else if (OB_ISNULL(agg_row_meta_.extra_idxes_ =
                         (int32_t *)alloc.alloc(sizeof(int32_t) * aggr_infos.count()))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    SQL_LOG(WARN, "allocate memory failed", K(ret), K(aggr_infos.count()));
  } else if (OB_ISNULL(bit_payload = (uint8_t *)alloc.alloc(bit_vec_size))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    SQL_LOG(WARN, "allocate memory failed", K(ret));
  } else if (OB_ISNULL(agg_row_meta_.tmp_res_sizes_ =
                         (int32_t *)alloc.alloc(sizeof(int32_t) * aggr_infos.count()))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    SQL_LOG(WARN, "allocate memory failed", K(ret));
  } else {
    MEMSET(agg_row_meta_.col_offsets_, -1, sizeof(int32_t) * (aggr_infos.count() + 1));
    MEMSET(agg_row_meta_.extra_idxes_, -1, sizeof(int32_t) * aggr_infos.count());
    MEMSET(bit_payload, 0, bit_vec_size);
    MEMSET(agg_row_meta_.tmp_res_sizes_, 0, sizeof(int32_t) * aggr_infos.count());
    agg_row_meta_.use_var_len_ = reinterpret_cast<AggBitVector *>(bit_payload);
  }
  int32_t agg_extra_id = 0;
  for (int i = 0; OB_SUCC(ret) && i < aggr_infos.count(); i++) {
    ObAggrInfo &info = aggr_infos.at(i);
    agg_row_meta_.row_size_ += helper::reserved_agg_col_size(*this, i);
    agg_row_meta_.tmp_res_sizes_[i] = helper::agg_cell_tmp_res_size(*this, i);
    agg_row_meta_.col_offsets_[i] = offset;
    offset = agg_row_meta_.row_size_;
    VecValueTypeClass vec_tc =
      get_vec_value_tc(info.expr_->datum_meta_.type_, info.expr_->datum_meta_.scale_,
                       info.expr_->datum_meta_.precision_);
    if (info.is_implicit_first_aggr() || helper::is_var_len_agg_cell(vec_tc)) {
      agg_row_meta_.use_var_len_->set(i);
    }
    if (helper::has_extra_info(info)) {
      has_extra = true;
      agg_row_meta_.extra_idxes_[i] = agg_extra_id++;
    }
  }
  agg_row_meta_.extra_cnt_ = agg_extra_id;
  agg_row_meta_.col_offsets_[aggr_infos.count()] = agg_row_meta_.row_size_;
  if (has_extra) {
    agg_row_meta_.row_size_ += sizeof(int32_t);
    agg_row_meta_.extra_idx_offset_ = offset;
    offset = agg_row_meta_.row_size_;
  } else {
    agg_row_meta_.extra_idx_offset_ = -1;
  }
  agg_row_meta_.row_size_ += bit_vec_size;
  agg_row_meta_.nullbits_offset_ = offset;
  return ret;
}

namespace helper
{
void print_input_rows(const RowSelector &row_sel, const sql::ObBitVector &skip,
                      const sql::EvalBound &bound, const sql::ObAggrInfo &aggr_info,
                      bool is_first_row, sql::ObEvalCtx &ctx, IAggregate *agg, int64_t col_id)
{
  const char *payload = nullptr;
  int32_t len = 0;
  ObDatum d;
  sql::ObEvalCtx::BatchInfoScopeGuard batch_guard(ctx);
  if (row_sel.is_empty()) {
    for (int i = bound.start(); i < bound.end(); i++) {
      if (skip.at(i)) { continue; }
      batch_guard.set_batch_idx(i);
      if (is_first_row) {
        ObIVector *data_vec = aggr_info.expr_->get_vector(ctx);
        if (VEC_INVALID == aggr_info.expr_->get_format(ctx)) { // do nothing
        } else if (data_vec->is_null(i)) {
          d.set_null();
        } else {
          aggr_info.expr_->get_vector(ctx)->get_payload(i, payload, len);
          d.ptr_ = payload;
          d.len_ = len;
        }
        SQL_LOG(DEBUG, "add row", K(DATUM2STR(*aggr_info.expr_, d)), K(*agg), K(col_id));
      } else {
        SQL_LOG(DEBUG, "add row", K(VEC_ROWEXPR2STR(ctx, aggr_info.param_exprs_)), K(*agg),
                K(col_id));
      }
    }
  } else {
    for (int i = 0; i < row_sel.size(); i++) {
      int idx = row_sel.index(i);
      batch_guard.set_batch_idx(idx);
      if (is_first_row) {
        ObIVector *data_vec = aggr_info.expr_->get_vector(ctx);
        if (VEC_INVALID == aggr_info.expr_->get_format(ctx)) { // do nothing
        } else if (data_vec->is_null(idx)) {
          d.set_null();
        } else {
          aggr_info.expr_->get_vector(ctx)->get_payload(idx, payload, len);
          d.ptr_ = payload;
          d.len_ = len;
        }
        SQL_LOG(DEBUG, "add row", K(DATUM2STR(*aggr_info.expr_, d)), K(*agg), K(col_id));
      } else {
        SQL_LOG(DEBUG, "add row", K(VEC_ROWEXPR2STR(ctx, aggr_info.param_exprs_)), K(*agg),
                K(col_id));
      }
    }
  }
}
} // namespace helper

} // end namespace aggregate
} // end namespace share
} // end namespace oceanbase