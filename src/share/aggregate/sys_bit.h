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
#ifndef OCEANBASE_SHARE_SYSBIT_AGGREGATE_H_
#define OCEANBASE_SHARE_SYSBIT_AGGREGATE_H_

#include "share/aggregate/iaggregate.h"

namespace oceanbase
{
namespace share
{
namespace aggregate
{
template <ObExprOperatorType agg_func, VecValueTypeClass in_tc, VecValueTypeClass out_tc>
struct SysBitAggregate final
  : public BatchAggregateWrapper<SysBitAggregate<agg_func, in_tc, out_tc>>
{
public:
  static const constexpr VecValueTypeClass IN_TC = in_tc;
  static const constexpr VecValueTypeClass OUT_TC = out_tc;
public:
  SysBitAggregate() {}

  template <typename ColumnFmt>
  inline int add_row(RuntimeContext &agg_ctx, ColumnFmt &columns, const int32_t row_num,
                     const int32_t agg_col_id, char *agg_cell, void *tmp_res, int64_t &calc_info)
  {
    int ret = OB_SUCCESS;
    setup_inital_value(agg_ctx, agg_cell, agg_col_id);
    uint64_t &res_uint = *reinterpret_cast<uint64_t *>(agg_cell);
    uint64_t cur_uint = *reinterpret_cast<const uint64_t *>(columns.get_payload(row_num));
    if (agg_func == T_FUN_SYS_BIT_AND) {
      res_uint &= cur_uint;
    } else if (agg_func == T_FUN_SYS_BIT_OR) {
      res_uint |= cur_uint;
    } else if (agg_func == T_FUN_SYS_BIT_XOR) {
      res_uint ^= cur_uint;
    } else {
      // impossible
      ob_assert(false);
    }
    return ret;
  }

  template <typename ColumnFmt>
  inline int add_nullable_row(RuntimeContext &agg_ctx, ColumnFmt &columns, const int32_t row_num,
                       const int32_t agg_col_id, char *agg_cell, void *tmp_res, int64_t &calc_info)
  {
    int ret = OB_SUCCESS;
    setup_inital_value(agg_ctx, agg_cell, agg_col_id);
    if (OB_LIKELY(!columns.is_null(row_num))) {
      ret = add_row(agg_ctx, columns, row_num, agg_col_id, agg_cell, tmp_res, calc_info);
    }
    return ret;
  }

  template <typename ColumnFmt>
  int collect_group_result(RuntimeContext &agg_ctx, const sql::ObExpr &agg_expr,
                           const int32_t agg_col_id, const char *agg_cell,
                           const int32_t agg_cell_len)
  {
    int ret = OB_SUCCESS;
    int64_t output_idx = agg_ctx.eval_ctx_.get_batch_idx();
    ColumnFmt *res_vec = static_cast<ColumnFmt *>(agg_expr.get_vector(agg_ctx.eval_ctx_));
    res_vec->set_payload(output_idx, agg_cell, sizeof(uint64_t));
    return ret;
  }

  int add_one_row(RuntimeContext &agg_ctx, int64_t batch_idx, int64_t batch_size,
                  const bool is_null, const char *data, const int32_t data_len, int32_t agg_col_idx,
                  char *agg_cell) override
  {
    int ret = OB_SUCCESS;
    setup_inital_value(agg_ctx, agg_cell, agg_col_idx);
    if (!is_null) {
      uint64_t cur_uint = *reinterpret_cast<const uint64_t *>(data);
      uint64_t &res_uint = *reinterpret_cast<uint64_t *>(agg_cell);
      if (agg_func == T_FUN_SYS_BIT_AND) {
        res_uint &= cur_uint;
      } else if (agg_func == T_FUN_SYS_BIT_OR) {
        res_uint |= cur_uint;
      } else if (agg_func == T_FUN_SYS_BIT_XOR) {
        res_uint ^= cur_uint;
      } else {
        // impossible
        ob_assert(false);
      }
    }
    return ret;
  }
  TO_STRING_KV("aggregate", "sysbit_ops", K(in_tc), K(out_tc), K(agg_func));
private:
  void setup_inital_value(RuntimeContext &agg_ctx, char *agg_cell, const int32_t agg_col_id)
  {
    NotNullBitVector &not_nulls = agg_ctx.locate_notnulls_bitmap(agg_col_id, agg_cell);
    if (OB_UNLIKELY(!not_nulls.at(agg_col_id))) {
      uint64_t initial_val = (agg_func == T_FUN_SYS_BIT_AND ? UINT_MAX_VAL[ObUInt64Type] : 0);
      *reinterpret_cast<uint64_t *>(agg_cell) = initial_val;
      not_nulls.set(agg_col_id);
    }
  }
};
} // namespace aggregate
} // namespace share
} // namespace oceanbase

#endif // OCEANBASE_SHARE_SYSBIT_AGGREGATE_H_