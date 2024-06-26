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

#ifndef OCEANBASE_SHARE_AGGREGATE_MIN_MAX_H_
#define OCEANBASE_SHARE_AGGREGATE_MIN_MAX_H_

#include "share/aggregate/iaggregate.h"

#include <utility>

namespace oceanbase
{
namespace share
{
namespace aggregate
{
using namespace sql;

struct CmpCalcInfo
{
  CmpCalcInfo(ObObjMeta obj_meta, int16_t cell_len) :
    obj_meta_(obj_meta), agg_cell_len_(cell_len), calc_flags_(0) {}
  CmpCalcInfo() : obj_meta_(), agg_cell_len_(0), calc_flags_(0) {}
  operator int64_t() const { return flags_; }
  inline void set_calculated()
  {
    calculated_ = true;
  }
  inline void set_min_max_idx_changed()
  {
    min_max_idx_changed_ = true;
  }
  inline bool calculated() const { return calculated_ == 1; }
  union {
    struct {
      ObObjMeta obj_meta_;
      int16_t agg_cell_len_; // for fixed length type only
      union {
        struct {
        uint16_t calculated_: 1;
        uint16_t min_max_idx_changed_: 1;
        uint16_t reserved_: 14;
        };
        uint16_t calc_flags_;
      };
    };
    int64_t flags_;
  };
};

static_assert(sizeof(CmpCalcInfo) == sizeof(int64_t), "");
// fixed length type
// exampl of min/max cell in aggr_row:
//         min(int64)
//  --------------------
// ... |   int64     |...
//  --------------------
//
// variable length type
// example of min/max cell in aggr_row
//         max(str)
// -------------------------------------------
// ...| <char *, int32>,  <char *, int32> |...
// -------------------------------------------
// second pair of <char *, int32> is used to store tmp result of aggregate
template<VecValueTypeClass vec_tc, bool is_min>
class MinMaxAggregate final : public BatchAggregateWrapper<MinMaxAggregate<vec_tc, is_min>>
{
  using buf_node = std::pair<char *, int32_t>;
  static const int32_t BUF_BLOCK_SIZE = 512;
public:
  static const constexpr VecValueTypeClass IN_TC = vec_tc;
  static const constexpr VecValueTypeClass OUT_TC = vec_tc;
public:

// TODO: remove info for window function optimization
public:
  MinMaxAggregate() {}

  int init(RuntimeContext &agg_ctx, const int64_t agg_col_id, ObIAllocator &allocator) override
  {
    UNUSEDx(agg_col_id, allocator);
    int ret = OB_SUCCESS;
    return ret;
  }

  int add_one_row(RuntimeContext &agg_ctx, int64_t row_num, int64_t batch_size, const bool is_null,
                  const char *data, const int32_t data_len, int32_t agg_col_idx,
                  char *agg_cell) override
  {
    int ret = OB_SUCCESS;
    NotNullBitVector &not_nulls = agg_ctx.locate_notnulls_bitmap(agg_col_idx, agg_cell);
    if (!is_null) {
      int cmp_ret = 0;
      ObObjMeta obj_meta;
      if (not_nulls.at(agg_col_idx)) {
        if (VecTCCmpCalc<vec_tc, vec_tc>::cmp == BasicCmpCalc<VEC_TC_INTEGER, VEC_TC_INTEGER>::cmp
            || vec_tc == VEC_TC_FLOAT
            || vec_tc == VEC_TC_DOUBLE
            || vec_tc == VEC_TC_NUMBER
            || vec_tc == VEC_TC_TIMESTAMP_TINY
            || vec_tc == VEC_TC_TIMESTAMP_TZ
            || vec_tc == VEC_TC_INTERVAL_DS) {
          // no need for obj meta
        } else {
          obj_meta = agg_ctx.aggr_infos_.at(agg_col_idx).param_exprs_.at(0)->obj_meta_;
        }
        if (!helper::is_var_len_agg_cell(vec_tc)) {
          ret = VecTCCmpCalc<vec_tc, vec_tc>::cmp(
            obj_meta, obj_meta, agg_cell,
            VEC_TC_NUMBER == vec_tc ? number::ObNumber::MAX_CALC_BYTE_LEN : sizeof(RTCType<vec_tc>),
            data, data_len, cmp_ret);
        } else {
          int32_t agg_cell_len = *reinterpret_cast<int32_t *>(agg_cell + sizeof(char *));
          const char *agg_data = reinterpret_cast<const char *>(*reinterpret_cast<int64_t *>(agg_cell));
          ret = VecTCCmpCalc<vec_tc, vec_tc>::cmp(obj_meta, obj_meta, agg_data, agg_cell_len, data, data_len, cmp_ret);
        }
        if ((is_min && cmp_ret > 0) || (!is_min && cmp_ret < 0)) {
          if (!helper::is_var_len_agg_cell(vec_tc)) {
            MEMCPY(agg_cell, data, data_len);
          } else {
            *reinterpret_cast<int64_t *>(agg_cell) = reinterpret_cast<int64_t>(data);
            *reinterpret_cast<int32_t *>(agg_cell + sizeof(char *)) = data_len;
            if (OB_FAIL(set_tmp_var_agg_data(agg_ctx, agg_col_idx, agg_cell))) {
              SQL_LOG(WARN, "set var aggregate data failed", K(ret));
            }
          }
        }
      } else if (helper::is_var_len_agg_cell(vec_tc)) {
        *reinterpret_cast<int64_t *>(agg_cell) = reinterpret_cast<int64_t>(data);
        *reinterpret_cast<int32_t *>(agg_cell + sizeof(char *)) = data_len;
        if (OB_FAIL(set_tmp_var_agg_data(agg_ctx, agg_col_idx, agg_cell))) {
          SQL_LOG(WARN, "set agg data failed", K(ret));
        }
      } else {
        MEMCPY(agg_cell, data, data_len);
      }
      not_nulls.set(agg_col_idx);
    }
    return ret;
  }
  template <typename ColumnFmt>
  int add_row(RuntimeContext &agg_ctx, ColumnFmt &columns, const int64_t row_num,
              const int32_t agg_col_id, char *agg_cell, void *tmp_res, int64_t &calc_info)
  {
    UNUSED(tmp_res);
    int ret = OB_SUCCESS;
    const char *row_data = nullptr;
    int32_t row_len = 0;
    int cmp_ret = 0;
    columns.get_payload(row_num, row_data, row_len);
    CmpCalcInfo &cmp_info = reinterpret_cast<CmpCalcInfo &>(calc_info);
    if (!helper::is_var_len_agg_cell(vec_tc)) {
      if (cmp_info.calculated()) {
        // do not need to read not_nulls bitmap for fixed length (include ObNUmber) types
        ret = VecTCCmpCalc<vec_tc, vec_tc>::cmp(cmp_info.obj_meta_, cmp_info.obj_meta_, agg_cell,
                                                cmp_info.agg_cell_len_, row_data, row_len, cmp_ret);
        if (OB_FAIL(ret)) {
          SQL_LOG(WARN, "compare failed", K(ret));
        } else if ((is_min && cmp_ret > 0) || (!is_min && cmp_ret < 0)) {
          MEMCPY(agg_cell, row_data, row_len);
          cmp_info.set_min_max_idx_changed();
        }
      } else {
        MEMCPY(agg_cell, row_data, row_len);
        cmp_info.set_calculated();
        cmp_info.set_min_max_idx_changed();
      }
    } else {
      int32_t agg_cell_len = *reinterpret_cast<int32_t *>(agg_cell + sizeof(char *));
      const char *agg_data = reinterpret_cast<const char *>(*reinterpret_cast<int64_t *>(agg_cell));
      if (cmp_info.calculated()) {
        ret = VecTCCmpCalc<vec_tc, vec_tc>::cmp(cmp_info.obj_meta_, cmp_info.obj_meta_, agg_data,
                                                agg_cell_len, row_data, row_len, cmp_ret);
        if (OB_FAIL(ret)) {
          SQL_LOG(WARN, "compare failed", K(ret));
        } else if ((is_min && cmp_ret > 0) || (!is_min && cmp_ret < 0)) {
           *reinterpret_cast<int64_t *>(agg_cell) = reinterpret_cast<int64_t>(row_data);
           *reinterpret_cast<int32_t *>(agg_cell + sizeof(char *)) = row_len;
           cmp_info.set_min_max_idx_changed();
        }
      } else {
        *reinterpret_cast<int64_t *>(agg_cell) = reinterpret_cast<int64_t>(row_data);
        *reinterpret_cast<int32_t *>(agg_cell + sizeof(char *)) = row_len;
        cmp_info.set_calculated();
        cmp_info.set_min_max_idx_changed();
      }
    }
    return ret;
  }

  template <typename ColumnFmt>
  int add_nullable_row(RuntimeContext &agg_ctx, ColumnFmt &columns, const int32_t row_num,
                       const int32_t agg_col_id, char *agg_cell, void *tmp_res, int64_t &calc_info)
  {
    int ret = OB_SUCCESS;
    if (columns.is_null(row_num)) {
      // do nothing
      SQL_LOG(DEBUG, "add null row", K(is_min), K(agg_col_id), K(row_num));
    } else if (OB_FAIL(
                 add_row(agg_ctx, columns, row_num, agg_col_id, agg_cell, tmp_res, calc_info))) {
      SQL_LOG(WARN, "add row failed", K(ret));
    } else {
      NotNullBitVector &not_nulls = agg_ctx.locate_notnulls_bitmap(agg_col_id, agg_cell);
      not_nulls.set(agg_col_id);
    }
    return ret;
  }

  template <typename ColumnFmt>
  int collect_group_result(RuntimeContext &agg_ctx, const sql::ObExpr &agg_expr,
                           const int32_t agg_col_id, const char *agg_cell,
                           const int32_t agg_cell_len)
  {
    int ret = OB_SUCCESS;
    ObEvalCtx &ctx = agg_ctx.eval_ctx_;
    int64_t output_idx = ctx.get_batch_idx();
    ColumnFmt *res_vec = static_cast<ColumnFmt *>(agg_expr.get_vector(ctx));
    const NotNullBitVector &not_nulls = agg_ctx.locate_notnulls_bitmap(agg_col_id, agg_cell);
    if (OB_LIKELY(not_nulls.at(agg_col_id))) {
      if (helper::is_var_len_agg_cell(vec_tc)) {
        char *res_buf = agg_expr.get_str_res_mem(ctx, agg_cell_len);
        if (OB_ISNULL(res_buf)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          SQL_LOG(WARN, "allocate memory failed", K(ret));
        } else {
          const char *data =
            reinterpret_cast<const char *>(*reinterpret_cast<const int64_t *>(agg_cell));
          CellWriter<AggCalcType<vec_tc>>::set(data, agg_cell_len, res_vec, output_idx, res_buf);
        }
      } else {
        CellWriter<AggCalcType<vec_tc>>::set(agg_cell, agg_cell_len, res_vec, output_idx, nullptr);
      }
    } else {
      res_vec->set_null(output_idx);
    }
    return ret;
  }

  int collect_tmp_result(RuntimeContext &agg_ctx, const int32_t agg_col_id, char *agg_cell)
  {
    int ret = OB_SUCCESS;
    NotNullBitVector &not_nulls = agg_ctx.locate_notnulls_bitmap(agg_col_id, agg_cell);
    // if result value has variable result length, e.g. string value,
    // we need copy value to tmp buffer in case value ptr stored was changed after next batch loop.

    if (agg_ctx.win_func_agg_) {
      // do nothing
    } else if (not_nulls.at(agg_col_id) && helper::is_var_len_agg_cell(vec_tc)) {
      if (OB_FAIL(set_tmp_var_agg_data(agg_ctx, agg_col_id, agg_cell))) {
        SQL_LOG(WARN, "set variable aggregate data failed", K(ret));
      }
    }
    return ret;
  }

  void reuse() override
  {
  }

  void destroy() override
  {
  }

  inline int64_t get_batch_calc_info(RuntimeContext &agg_ctx, int32_t agg_col_idx,
                                     char *agg_cell) override
  {
    ObObjMeta &obj_meta = agg_ctx.locate_aggr_info(agg_col_idx).param_exprs_.at(0)->obj_meta_;
    NotNullBitVector &not_nulls = agg_ctx.locate_notnulls_bitmap(agg_col_idx, agg_cell);
    int32_t agg_cell_len = 0;
    if (!helper::is_var_len_agg_cell(vec_tc)) {
      agg_cell_len = agg_ctx.row_meta().get_cell_len(agg_col_idx, nullptr/*not used*/);
    }
    CmpCalcInfo info = CmpCalcInfo(obj_meta, static_cast<int16_t>(agg_cell_len));
    if (not_nulls.at(agg_col_idx)) { info.set_calculated(); }
    return info;
  }

  TO_STRING_KV("aggregate", (is_min ? "min" : "max"), K(vec_tc));

  template <typename ColumnFmt>
  int add_or_sub_row(RuntimeContext &agg_ctx, ColumnFmt &columns, const int32_t row_num,
                     const int32_t agg_col_id, char *agg_cell, void *tmp_res, int64_t &calc_info)
  {
    int ret = OB_SUCCESS;
    bool is_trans = !agg_ctx.removal_info_.is_inverse_agg_;
    if (!columns.is_null(row_num)) {
      CmpCalcInfo &cmp_info = reinterpret_cast<CmpCalcInfo &>(calc_info);
      cmp_info.min_max_idx_changed_ = 0;
      if (OB_FAIL(add_row(agg_ctx, columns, row_num, agg_col_id, agg_cell, tmp_res, calc_info))) {
        SQL_LOG(WARN, "add row failed", K(ret));
      } else if (cmp_info.min_max_idx_changed_) {
        agg_ctx.removal_info_.max_min_index_ = row_num;
        agg_ctx.removal_info_.is_max_min_idx_changed_ = true;
      }
      agg_ctx.locate_notnulls_bitmap(agg_col_id, agg_cell).set(0);
    } else if (is_trans) {
      agg_ctx.removal_info_.null_cnt_++;
    } else {
      agg_ctx.removal_info_.null_cnt_--;
    }
    // do nothing
    return ret;
  }
private:
  int set_tmp_var_agg_data(RuntimeContext &agg_ctx, const int32_t agg_col_id, char *agg_cell)
  {
    int ret = OB_SUCCESS;
    if (helper::is_var_len_agg_cell(vec_tc)) {
      char *agg_data = reinterpret_cast<char *>(*reinterpret_cast<int64_t *>(agg_cell));
      int32_t agg_data_len = *reinterpret_cast<int32_t *>(agg_cell + sizeof(char *));
      char *tmp_buf = reinterpret_cast<char *>(*reinterpret_cast<int64_t *>(agg_cell + sizeof(char *) + sizeof(int32_t)));
      int32_t &cap = *reinterpret_cast<int32_t *>(agg_cell + sizeof(int32_t) + sizeof(char *) * 2);
      if (cap < agg_data_len) {
        int32_t new_cap = 2 *((agg_data_len + BUF_BLOCK_SIZE - 1) / BUF_BLOCK_SIZE) * BUF_BLOCK_SIZE;
        void *new_buf = nullptr;
        if (OB_ISNULL(new_buf = agg_ctx.allocator_.alloc(new_cap))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          SQL_LOG(WARN, "allocate memory failed", K(ret));
        } else {
          tmp_buf = (char *)new_buf;
          cap = new_cap;
          *reinterpret_cast<int64_t *>(agg_cell + sizeof(char *) + sizeof(int32_t)) = reinterpret_cast<int64_t>(tmp_buf);
        }
      }
      if (OB_SUCC(ret)) {
        MEMCPY(tmp_buf, agg_data, agg_data_len);
        *reinterpret_cast<int64_t *>(agg_cell) = reinterpret_cast<int64_t>(tmp_buf);
      }
    }
    return ret;
  }
};

} // end namespace aggregate
} // end namespace share
} // end namespace oceanbase
#endif // OCEANBASE_SHARE_AGGREGATE_MIN_MAX_H_