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

#ifndef OCEANBASE_SHARE_AGGREGATE_MIN_MAX_IPP_
#define OCEANBASE_SHARE_AGGREGATE_MIN_MAX_IPP_

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
        obj_meta = agg_ctx.aggr_infos_.at(agg_col_idx).param_exprs_.at(0)->obj_meta_;
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
  OB_INLINE int add_row(RuntimeContext &agg_ctx, ColumnFmt &columns, const int64_t row_num,
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
  OB_INLINE int add_nullable_row(RuntimeContext &agg_ctx, ColumnFmt &columns, const int32_t row_num,
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

  virtual int rollup_aggregation(RuntimeContext &agg_ctx, const int32_t agg_col_idx,
                                 AggrRowPtr group_row, AggrRowPtr rollup_row,
                                 int64_t cur_rollup_group_idx,
                                 int64_t max_group_cnt = INT64_MIN) override
  {
    int ret = OB_SUCCESS;
    UNUSEDx(cur_rollup_group_idx, max_group_cnt);
    int cmp_ret = 0;
    char *curr_agg_cell = agg_ctx.row_meta().locate_cell_payload(agg_col_idx, group_row);
    char *rollup_agg_cell = agg_ctx.row_meta().locate_cell_payload(agg_col_idx, rollup_row);
    int64_t curr_info = get_batch_calc_info(agg_ctx, agg_col_idx, curr_agg_cell);
    int64_t rollup_info = get_batch_calc_info(agg_ctx, agg_col_idx, rollup_agg_cell);
    const CmpCalcInfo &curr_calc_info = reinterpret_cast<const CmpCalcInfo &>(curr_info);
    CmpCalcInfo &rollup_calc_info = reinterpret_cast<CmpCalcInfo &>(rollup_info);
    const NotNullBitVector &curr_not_nulls = agg_ctx.locate_notnulls_bitmap(agg_col_idx, curr_agg_cell);
    NotNullBitVector &rollup_not_nulls = agg_ctx.locate_notnulls_bitmap(agg_col_idx, rollup_agg_cell);
    if (curr_not_nulls.at(agg_col_idx) && rollup_not_nulls.at(agg_col_idx)) {
      if (!helper::is_var_len_agg_cell(vec_tc)) {
        ret = VecTCCmpCalc<vec_tc, vec_tc>::cmp(
          curr_calc_info.obj_meta_, curr_calc_info.obj_meta_, rollup_agg_cell,
          rollup_calc_info.agg_cell_len_, curr_agg_cell, curr_calc_info.agg_cell_len_, cmp_ret);
        if (OB_FAIL(ret)) {
          SQL_LOG(WARN, "compare failed", K(ret));
        } else if ((is_min && cmp_ret > 0) || (!is_min && cmp_ret < 0)) {
          MEMCPY(rollup_agg_cell, curr_agg_cell, curr_calc_info.agg_cell_len_);
        }
      } else {
        int32_t rollup_agg_cell_len = *reinterpret_cast<int32_t *>(rollup_agg_cell + sizeof(char *));
        const char *rollup_agg_data =
          reinterpret_cast<const char *>(*reinterpret_cast<int64_t *>(rollup_agg_cell));
        const char *cur_agg_data =
          reinterpret_cast<const char *>(*reinterpret_cast<int64_t *>(curr_agg_cell));
        int32_t cur_agg_cell_len = *reinterpret_cast<int32_t *>(curr_agg_cell + sizeof(char *));
        ret = VecTCCmpCalc<vec_tc, vec_tc>::cmp(
          curr_calc_info.obj_meta_, curr_calc_info.obj_meta_, rollup_agg_data, rollup_agg_cell_len,
          cur_agg_data, cur_agg_cell_len, cmp_ret);
        if (OB_FAIL(ret)) {
          SQL_LOG(WARN, "compare failed", K(ret));
        } else if ((is_min && cmp_ret > 0) || (!is_min && cmp_ret < 0)) {
          if (OB_FAIL(set_rollup_var_agg_data(agg_ctx, rollup_agg_cell, cur_agg_data, cur_agg_cell_len))) {
            SQL_LOG(WARN, "set rollup variable aggregate data failed", K(ret));
          }
        }
      }
      if (OB_SUCC(ret)) {
        rollup_calc_info.set_calculated();
        rollup_calc_info.set_min_max_idx_changed();
      }
    } else if (curr_not_nulls.at(agg_col_idx)) {
      int32_t curr_agg_cell_len = agg_ctx.row_meta().get_cell_len(agg_col_idx, group_row);
      if (helper::is_var_len_agg_cell(vec_tc)) {
        const char *cur_agg_data =
          reinterpret_cast<const char *>(*reinterpret_cast<const int64_t *>(curr_agg_cell));
        if (OB_FAIL(set_rollup_var_agg_data(agg_ctx, rollup_agg_cell, cur_agg_data, curr_agg_cell_len))) {
          SQL_LOG(WARN, "set rollup variable aggregate data failed", K(ret));
        }
      } else {
        MEMCPY(rollup_agg_cell, curr_agg_cell, curr_agg_cell_len);
      }
      if (OB_SUCC(ret)) {
        rollup_calc_info.set_calculated();
        rollup_calc_info.set_min_max_idx_changed();
      }
    } else {
      // do nothing
    }
    if (OB_FAIL(ret)) {
    } else if (rollup_calc_info.calculated()) {
      rollup_not_nulls.set(agg_col_idx);
    }
    return ret;
  }

  TO_STRING_KV("aggregate", (is_min ? "min" : "max"), K(vec_tc));

  template <typename ColumnFmt>
  OB_INLINE int add_or_sub_row(RuntimeContext &agg_ctx, ColumnFmt &columns, const int32_t row_num,
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

  int eval_group_extra_result(RuntimeContext &agg_ctx, const int32_t agg_col_id, const int32_t cur_group_id) override
  {
    int ret = OB_SUCCESS;
    char *agg_cell = nullptr;
    int32_t agg_cell_len = 0;
    if (agg_ctx.win_func_agg_ && helper::is_var_len_agg_cell(vec_tc)) {
      agg_ctx.get_agg_payload(agg_col_id, cur_group_id, agg_cell, agg_cell_len);
      if (OB_FAIL(set_tmp_var_agg_data(agg_ctx, agg_col_id, agg_cell))) {
        SQL_LOG(WARN, "store tmp result failed", K(ret));
      }
    }
    return ret;
  }
private:
  OB_INLINE int set_rollup_var_agg_data(RuntimeContext &agg_ctx, char *rollup_agg_cell,
                                        const char *cur_agg_data, int32_t curr_agg_cell_len)
  {
    int ret = OB_SUCCESS;
    char *tmp_buf =
      reinterpret_cast<char *>(*reinterpret_cast<int64_t *>(rollup_agg_cell + sizeof(char *) + sizeof(int32_t)));
    int32_t &cap = *reinterpret_cast<int32_t *>(rollup_agg_cell + sizeof(int32_t) + sizeof(char *) * 2);
    if (OB_NOT_NULL(tmp_buf) && cap >= curr_agg_cell_len) {
      // reuse tmp buffer
      MEMCPY(tmp_buf, cur_agg_data, curr_agg_cell_len);
      *reinterpret_cast<int64_t *>(rollup_agg_cell + sizeof(char *) + sizeof(int32_t)) =
        reinterpret_cast<int64_t>(tmp_buf);
      *reinterpret_cast<int64_t *>(rollup_agg_cell) = reinterpret_cast<int64_t>(tmp_buf);
      *reinterpret_cast<int32_t *>(rollup_agg_cell + sizeof(char *)) = curr_agg_cell_len;
    } else {
      int32_t new_cap = ((curr_agg_cell_len + BUF_BLOCK_SIZE - 1) / BUF_BLOCK_SIZE) * BUF_BLOCK_SIZE;
      void *new_buf = nullptr;
      if (OB_ISNULL(new_buf = agg_ctx.allocator_.alloc(new_cap))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        SQL_LOG(WARN, "allocate memory failed", K(ret), K(new_cap));
      } else {
        MEMCPY(new_buf, cur_agg_data, curr_agg_cell_len);
        cap = new_cap;
        *reinterpret_cast<int64_t *>(rollup_agg_cell + sizeof(char *) + sizeof(int32_t)) =
          reinterpret_cast<int64_t>(new_buf);
        *reinterpret_cast<int64_t *>(rollup_agg_cell) = reinterpret_cast<int64_t>(new_buf);
        *reinterpret_cast<int32_t *>(rollup_agg_cell + sizeof(char *)) = curr_agg_cell_len;
      }
    }
    return ret;
  }
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

/*----------------------------------------------ArgMinMaxAggregate---------------------------------------*/

template <VecValueTypeClass vec_tc, typename Enable = void>
struct ArgMinMaxAggCell;

template <VecValueTypeClass vec_tc>
struct ArgMinMaxAggCell<vec_tc, typename std::enable_if<helper::is_fixed_len<vec_tc>::value>::type> {
public:
  static constexpr int32_t fixed_len =
      VEC_TC_NUMBER == vec_tc ? number::ObNumber::MAX_CALC_BYTE_LEN : sizeof(RTCType<vec_tc>);
  using CompactNumberType = char[number::ObNumber::MAX_CALC_BYTE_LEN];
  using StoreType = std::conditional_t<VEC_TC_NUMBER == vec_tc, CompactNumberType, RTCType<vec_tc>>;
  static const int32_t BUF_BLOCK_SIZE = 512;
  static const int32_t CMP_PARAM_IDX = 1;
  static const int32_t ARG_PARAM_IDX = 0;
  OB_INLINE const char *get_data_ptr() const
  {
    return reinterpret_cast<const char *>(&val_);
  }
  OB_INLINE int32_t get_data_len() const
  {
    return fixed_len;
  }
  OB_INLINE void set_val(const char *data_ptr, const int32_t &data_len)
  {
    MEMCPY(&val_, data_ptr, data_len);
  }
  OB_INLINE void set_cell(const ArgMinMaxAggCell<vec_tc> &src_cell)
  {
    MEMCPY(&val_, &src_cell.val_, fixed_len);
    output_ptr_ = src_cell.output_ptr_;
    output_len_ = src_cell.output_len_;
  }
  OB_INLINE int compare(ObObjMeta obj_meta, const char *data, const int32_t data_len, int &cmp_ret)
  {
    return VecTCCmpCalc<vec_tc, vec_tc>::cmp(
        obj_meta, obj_meta, reinterpret_cast<const char *>(&val_), data_len, data, data_len, cmp_ret);
  }
  OB_INLINE int set_tmp_var(RuntimeContext &agg_ctx)
  {
    return OB_SUCCESS;
  }
  OB_INLINE int set_output_data(RuntimeContext &agg_ctx, const int32_t agg_col_idx, int32_t row_idx)
  {
    int ret = OB_SUCCESS;
    const ObExpr *arg_expr = agg_ctx.aggr_infos_.at(agg_col_idx).param_exprs_.at(ARG_PARAM_IDX);
    VectorFormat arg_fmt = arg_expr->get_format(agg_ctx.eval_ctx_);
    ObIVector *arg_vector = arg_expr->get_vector(agg_ctx.eval_ctx_);
    const char *data = nullptr;
    int32_t data_len = 0;
    arg_vector->get_payload(row_idx, data, data_len);
    if (data_len > output_cap_) {
      int32_t new_cap =
          output_len_ == 0 ? data_len : 2 * ((data_len + BUF_BLOCK_SIZE - 1) / BUF_BLOCK_SIZE) * BUF_BLOCK_SIZE;
      void *new_buf = nullptr;
      if (OB_ISNULL(new_buf = agg_ctx.allocator_.alloc(new_cap))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        SQL_LOG(WARN, "allocate memory failed", K(ret));
      } else {
        output_ptr_ = (char *)new_buf;
        output_cap_ = new_cap;
      }
    }
    if (OB_SUCC(ret)) {
      output_len_ = data_len;
      MEMCPY(output_ptr_, data, data_len);
    }
    return ret;
  }

public:
  StoreType val_;
  char *output_ptr_;
  int32_t output_len_;
  int32_t output_cap_;
} __attribute__ ((packed));

template <VecValueTypeClass vec_tc>
struct ArgMinMaxAggCell<vec_tc, typename std::enable_if<helper::is_var_len<vec_tc>::value>::type> {
  static const int32_t BUF_BLOCK_SIZE = 512;
  static const int32_t CMP_PARAM_IDX = 1;
  static const int32_t ARG_PARAM_IDX = 0;
public:
  OB_INLINE const char *get_data_ptr() const
  {
    return val_ptr_;
  }
  OB_INLINE int32_t get_data_len() const
  {
    return val_len_;
  }
  OB_INLINE void set_val(const char *data_ptr, const int32_t &data_len)
  {
    val_ptr_ = data_ptr;
    val_len_ = data_len;
  }
  OB_INLINE void set_cell(const ArgMinMaxAggCell<vec_tc> &src_cell)
  {
    val_ptr_ = src_cell.val_ptr_;
    val_len_ = src_cell.val_len_;
    output_ptr_ = src_cell.output_ptr_;
    output_len_ = src_cell.output_len_;
  }
  OB_INLINE int compare(ObObjMeta obj_meta, const char *data, const int32_t data_len, int &cmp_ret)
  {
    return VecTCCmpCalc<vec_tc, vec_tc>::cmp(obj_meta, obj_meta, val_ptr_, val_len_, data, data_len, cmp_ret);
  }
  OB_INLINE int set_tmp_var(RuntimeContext &agg_ctx)
  {
    int ret = OB_SUCCESS;
    if (tmp_val_cap_ < val_len_) {
      int32_t new_cap = 2 * ((val_len_ + BUF_BLOCK_SIZE - 1) / BUF_BLOCK_SIZE) * BUF_BLOCK_SIZE;
      void *new_buf = nullptr;
      if (OB_ISNULL(new_buf = agg_ctx.allocator_.alloc(new_cap))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        SQL_LOG(WARN, "allocate memory failed", K(ret));
      } else {
        tmp_val_ptr_ = (char *)new_buf;
        tmp_val_cap_ = new_cap;
      }
    }
    if (OB_SUCC(ret)) {
      MEMCPY(tmp_val_ptr_, val_ptr_, val_len_);
      val_ptr_ = tmp_val_ptr_;
    }
    return ret;
  }
  OB_INLINE int set_output_data(RuntimeContext &agg_ctx, const int32_t agg_col_idx, int32_t row_idx)
  {
    int ret = OB_SUCCESS;
    const ObExpr *arg_expr = agg_ctx.aggr_infos_.at(agg_col_idx).param_exprs_.at(ARG_PARAM_IDX);
    VectorFormat arg_fmt = arg_expr->get_format(agg_ctx.eval_ctx_);
    ObIVector *arg_vector = arg_expr->get_vector(agg_ctx.eval_ctx_);
    const char *data = nullptr;
    int32_t data_len = 0;
    arg_vector->get_payload(row_idx, data, data_len);
    if (data_len > output_cap_) {
      int32_t new_cap =
          output_len_ == 0 ? data_len : 2 * ((data_len + BUF_BLOCK_SIZE - 1) / BUF_BLOCK_SIZE) * BUF_BLOCK_SIZE;
      void *new_buf = nullptr;
      if (OB_ISNULL(new_buf = agg_ctx.allocator_.alloc(new_cap))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        SQL_LOG(WARN, "allocate memory failed", K(ret));
      } else {
        output_ptr_ = (char *)new_buf;
        output_cap_ = new_cap;
      }
    }
    if (OB_SUCC(ret)) {
      output_len_ = data_len;
      MEMCPY(output_ptr_, data, data_len);
    }
    return ret;
  }

public:
  const char *val_ptr_;
  int32_t val_len_;
  char *tmp_val_ptr_;
  int32_t tmp_val_cap_;
  char *output_ptr_;
  int32_t output_len_;
  int32_t output_cap_;
} __attribute__ ((packed));
template<VecValueTypeClass vec_tc, bool is_arg_min>
class ArgMinMaxAggregate final : public BatchAggregateWrapper<ArgMinMaxAggregate<vec_tc, is_arg_min>>
{
  using buf_node = std::pair<char *, int32_t>;
public:
  static const int32_t CMP_PARAM_IDX = 1;
  static const int32_t ARG_PARAM_IDX = 0;
  static const constexpr VecValueTypeClass IN_TC = vec_tc;
  static const constexpr VecValueTypeClass OUT_TC = VEC_TC_NULL;
  using AggCellType = ArgMinMaxAggCell<vec_tc>;

public:

// TODO: remove info for window function optimization
public:
  ArgMinMaxAggregate() {}

  int init(RuntimeContext &agg_ctx, const int64_t agg_col_id, ObIAllocator &allocator) override
  {
    UNUSEDx(agg_col_id, allocator);
    int ret = OB_SUCCESS;
    return ret;
  }

  int add_one_row(RuntimeContext &agg_ctx, int64_t row_num, int64_t batch_size, const bool is_null,
                  const char *data, const int32_t data_len, int32_t agg_col_idx,
                  char *agg_cell)
  {
    int ret = OB_SUCCESS;
    const ObExpr *arg_expr = agg_ctx.aggr_infos_.at(agg_col_idx).param_exprs_.at(ARG_PARAM_IDX);
    const ObExpr *cmp_expr = agg_ctx.aggr_infos_.at(agg_col_idx).param_exprs_.at(CMP_PARAM_IDX);
    VectorFormat arg_fmt = arg_expr->get_format(agg_ctx.eval_ctx_);
    ObIVector *vector = arg_expr->get_vector(agg_ctx.eval_ctx_);
    bool need_skip = false;
    if (arg_fmt == VEC_UNIFORM || arg_fmt == VEC_UNIFORM_CONST) {
      const uint64_t idx_mask = VEC_UNIFORM_CONST == arg_fmt ? 0 : UINT64_MAX;
      const ObDatum *datums = static_cast<ObUniformBase *>(vector)->get_datums();
      need_skip = datums[row_num & idx_mask].is_null();
    } else {
      need_skip = static_cast<ObBitmapNullVectorBase *>(vector)->get_nulls()->at(row_num);
    }
    NotNullBitVector &not_nulls = agg_ctx.locate_notnulls_bitmap(agg_col_idx, agg_cell);
    if (!is_null && !need_skip) {
      int cmp_ret = 0;
      bool need_update_output = false;
      ObObjMeta obj_meta;
      AggCellType *cell = reinterpret_cast<AggCellType *>(agg_cell);
      if (not_nulls.at(agg_col_idx)) {
        obj_meta = cmp_expr->obj_meta_;
        if (OB_FAIL(cell->compare(obj_meta, data, data_len, cmp_ret))) {
          SQL_LOG(WARN, "agg cell compare failed", K(ret));
        } else if ((is_arg_min && cmp_ret > 0) || (!is_arg_min && cmp_ret < 0)) {
          cell->set_val(data, data_len);
          agg_ctx.set_minmax_row_idx(agg_col_idx, row_num);
          if (OB_FAIL(cell->set_tmp_var(agg_ctx))) {
            SQL_LOG(WARN, "set var aggregate data failed", K(ret));
          }
          need_update_output = true;
        }
      } else {
        cell->set_val(data, data_len);
        agg_ctx.set_minmax_row_idx(agg_col_idx, row_num);
        if (OB_FAIL(cell->set_tmp_var(agg_ctx))) {
          SQL_LOG(WARN, "set agg data failed", K(ret));
        }
        need_update_output = true;
      }
      if (OB_SUCC(ret) && need_update_output
          && OB_FAIL(cell->set_output_data(agg_ctx, agg_col_idx, agg_ctx.get_minmax_row_idx(agg_col_idx)))) {
        SQL_LOG(WARN, "set var aggregate data fro arg_min/max failed", K(ret));
      }
      not_nulls.set(agg_col_idx);
    }
    return ret;
  }

  template <typename ColumnFmt>
  OB_INLINE int add_row(RuntimeContext &agg_ctx, ColumnFmt &columns, const int64_t row_num,
                        const int32_t agg_col_id, char *agg_cell, void *tmp_res, int64_t &calc_info)
  {
    UNUSED(tmp_res);
    int ret = OB_SUCCESS;
    const ObExpr *arg_expr = agg_ctx.aggr_infos_.at(agg_col_id).param_exprs_.at(ARG_PARAM_IDX);
    VectorFormat arg_fmt = arg_expr->get_format(agg_ctx.eval_ctx_);
    ObIVector *vector = arg_expr->get_vector(agg_ctx.eval_ctx_);
    bool need_skip = false;
    if (arg_fmt == VEC_UNIFORM || arg_fmt == VEC_UNIFORM_CONST) {
      const uint64_t idx_mask = VEC_UNIFORM_CONST == arg_fmt ? 0 : UINT64_MAX;
      const ObDatum *datums = static_cast<ObUniformBase *>(vector)->get_datums();
      need_skip = datums[row_num & idx_mask].is_null();
    } else {
      need_skip = static_cast<ObBitmapNullVectorBase *>(vector)->get_nulls()->at(row_num);
    }
    if (need_skip) {
      SQL_LOG(DEBUG, "skip null value", K(is_arg_min), K(agg_col_id), K(row_num));
    } else {
      const char *row_data = nullptr;
      int32_t row_len = 0;
      int cmp_ret = 0;
      columns.get_payload(row_num, row_data, row_len);
      CmpCalcInfo &cmp_info = reinterpret_cast<CmpCalcInfo &>(agg_ctx.argminmax_calc_info_);
      AggCellType *cell = reinterpret_cast<AggCellType *>(agg_cell);
      if (cmp_info.calculated()) {
        if (OB_FAIL(cell->compare(cmp_info.obj_meta_, row_data, row_len, cmp_ret))) {
          SQL_LOG(WARN, "agg cell compare failed", K(ret));
        } else if ((is_arg_min && cmp_ret > 0) || (!is_arg_min && cmp_ret < 0)) {
          cell->set_val(row_data, row_len);
          agg_ctx.set_minmax_row_idx(agg_col_id, row_num);
          cmp_info.set_min_max_idx_changed(); // in collect_tmp_result() use this flag to check if update output data.
        }
      } else {
        cell->set_val(row_data, row_len);
        agg_ctx.set_minmax_row_idx(agg_col_id, row_num);
        cmp_info.set_calculated();
        cmp_info.set_min_max_idx_changed();
      }
      if (OB_SUCC(ret)) {
        NotNullBitVector &not_nulls = agg_ctx.locate_notnulls_bitmap(agg_col_id, agg_cell);
        not_nulls.set(agg_col_id);
      }
      SQL_LOG(DEBUG, "add row", K(agg_col_id), K(row_num), K(agg_ctx.get_minmax_row_idx(agg_col_id)));
    }
    return ret;
  }

  template <typename ColumnFmt>
  OB_INLINE int add_nullable_row(RuntimeContext &agg_ctx, ColumnFmt &columns, const int32_t row_num,
                       const int32_t agg_col_id, char *agg_cell, void *tmp_res, int64_t &calc_info)
  {
    int ret = OB_SUCCESS;
    if (columns.is_null(row_num)) {
      // do nothing
      SQL_LOG(DEBUG, "add null row", K(is_arg_min), K(agg_col_id), K(row_num));
    } else {
      if (OB_FAIL(add_row(agg_ctx, columns, row_num, agg_col_id, agg_cell, tmp_res, calc_info))) {
        SQL_LOG(WARN, "add row failed", K(ret));
      }
    }
    return ret;
  }

  template <typename ColumnFmt>
  int collect_group_result(RuntimeContext &agg_ctx, const sql::ObExpr &agg_expr, const int32_t agg_col_id,
      const char *agg_cell, const int32_t agg_cell_len)
  {
    int ret = OB_SUCCESS;
    ObEvalCtx &ctx = agg_ctx.eval_ctx_;
    int64_t output_idx = ctx.get_batch_idx();
    ColumnFmt *res_vec = static_cast<ColumnFmt *>(agg_expr.get_vector(ctx));
    const NotNullBitVector &not_nulls = agg_ctx.locate_notnulls_bitmap(agg_col_id, agg_cell);
    const AggCellType *cell = reinterpret_cast<const AggCellType *>(agg_cell);
    if (OB_LIKELY(not_nulls.at(agg_col_id))) {
      char *res_buf = agg_expr.get_str_res_mem(ctx, cell->output_len_);
      if (OB_ISNULL(res_buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        SQL_LOG(WARN, "allocate memory failed", K(ret), K(agg_cell_len));
      } else {
        cell_to_vec<ColumnFmt>(agg_ctx, agg_col_id, cell, res_vec, output_idx, res_buf);
      }
    } else {
      res_vec->set_null(output_idx);
    }
    SQL_LOG(
        DEBUG, "collect group result", K(cell->output_ptr_), K(agg_col_id), K(agg_ctx.get_minmax_row_idx(agg_col_id)));
    return ret;
  }
  int collect_tmp_result(RuntimeContext &agg_ctx, const int32_t agg_col_id, char *agg_cell)
  {
    int ret = OB_SUCCESS;
    NotNullBitVector &not_nulls = agg_ctx.locate_notnulls_bitmap(agg_col_id, agg_cell);
    // if result value has variable result length, e.g. string value,
    // we need copy value to tmp buffer in case value ptr stored was changed after next batch loop.

    AggCellType *cell = reinterpret_cast<AggCellType *>(agg_cell);
    if (not_nulls.at(agg_col_id)
        && reinterpret_cast<CmpCalcInfo &>(agg_ctx.argminmax_calc_info_).min_max_idx_changed_) {
      if (OB_FAIL(cell->set_tmp_var(agg_ctx))) {
        SQL_LOG(WARN, "set agg data failed", K(ret));
      } else if (OB_FAIL(cell->set_output_data(agg_ctx, agg_col_id, agg_ctx.get_minmax_row_idx(agg_col_id)))) {
        SQL_LOG(WARN, "set output data in argmin/max failed", K(ret));
      } else {
        agg_ctx.set_minmax_row_idx(agg_col_id, -1);
      }
    }
    SQL_LOG(
        DEBUG, "collect tmp result", K(cell->output_ptr_), K(agg_col_id), K(agg_ctx.get_minmax_row_idx(agg_col_id)));
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
    ObObjMeta &obj_meta = agg_ctx.locate_aggr_info(agg_col_idx).param_exprs_.at(CMP_PARAM_IDX)->obj_meta_;
    NotNullBitVector &not_nulls = agg_ctx.locate_notnulls_bitmap(agg_col_idx, agg_cell);
    AggCellType *cell = reinterpret_cast<AggCellType *>(agg_cell);
    CmpCalcInfo info = CmpCalcInfo(obj_meta, 0);
    if (not_nulls.at(agg_col_idx)) {
      info.set_calculated();
    }
    agg_ctx.argminmax_calc_info_ = info;
    return agg_ctx.argminmax_calc_info_;
  }

  template <typename Vector>
  inline void cell_to_vec(RuntimeContext &agg_ctx, const int32_t agg_col_idx, const AggCellType *cell,
                          Vector *vec, const int64_t output_idx, char *res_buf)
  {
    OB_ASSERT(vec != NULL);
    bool is_var_len = agg_ctx.agg_row_meta_.is_var_len(agg_col_idx);
    if (is_var_len) {
      MEMCPY(res_buf, cell->output_ptr_, cell->output_len_);
      vec->set_payload_shallow(output_idx, res_buf , cell->output_len_);
    } else {
      vec->set_payload(output_idx, cell->output_ptr_, cell->output_len_);
    }
    SQL_LOG(DEBUG, "cell to vec", K(cell->output_ptr_), K(output_idx), K(agg_ctx.get_minmax_row_idx(agg_col_idx)));
  }

  virtual int rollup_aggregation(RuntimeContext &agg_ctx, const int32_t agg_col_idx, AggrRowPtr group_row,
      AggrRowPtr rollup_row, int64_t cur_rollup_group_idx, int64_t max_group_cnt = INT64_MIN) override
  {
    int ret = OB_SUCCESS;
    UNUSEDx(cur_rollup_group_idx, max_group_cnt);
    int cmp_ret = 0;
    AggCellType *curr_agg_cell =
        reinterpret_cast<AggCellType *>(agg_ctx.row_meta().locate_cell_payload(agg_col_idx, group_row));
    AggCellType *rollup_agg_cell =
        reinterpret_cast<AggCellType *>(agg_ctx.row_meta().locate_cell_payload(agg_col_idx, rollup_row));
    int64_t curr_info = get_batch_calc_info(agg_ctx, agg_col_idx, reinterpret_cast<char *>(curr_agg_cell));
    int64_t rollup_info = get_batch_calc_info(agg_ctx, agg_col_idx, reinterpret_cast<char *>(rollup_agg_cell));
    const CmpCalcInfo &curr_calc_info = reinterpret_cast<const CmpCalcInfo &>(curr_info);
    CmpCalcInfo &rollup_calc_info = reinterpret_cast<CmpCalcInfo &>(rollup_info);
    const NotNullBitVector &curr_not_nulls =
        agg_ctx.locate_notnulls_bitmap(agg_col_idx, reinterpret_cast<char *>(curr_agg_cell));
    NotNullBitVector &rollup_not_nulls =
        agg_ctx.locate_notnulls_bitmap(agg_col_idx, reinterpret_cast<char *>(rollup_agg_cell));
    if (curr_not_nulls.at(agg_col_idx) && rollup_not_nulls.at(agg_col_idx)) {
      if (OB_FAIL(rollup_agg_cell->compare(
              curr_calc_info.obj_meta_, curr_agg_cell->get_data_ptr(), curr_agg_cell->get_data_len(), cmp_ret))) {
        SQL_LOG(WARN, "agg cell compare failed", K(ret));
      } else if ((is_arg_min && cmp_ret > 0) || (!is_arg_min && cmp_ret < 0)) {
        rollup_agg_cell->set_cell(*curr_agg_cell);
      }
      rollup_calc_info.set_calculated();
      rollup_calc_info.set_min_max_idx_changed();
    } else if (curr_not_nulls.at(agg_col_idx)) {
      rollup_agg_cell->set_cell(*curr_agg_cell);
      rollup_calc_info.set_calculated();
      rollup_calc_info.set_min_max_idx_changed();
    } else {
      // do nothing
    }
    if (OB_FAIL(ret)) {
    } else if (rollup_calc_info.calculated()) {
      rollup_not_nulls.set(agg_col_idx);
    }
    SQL_LOG(DEBUG,
        "rollup aggregation",
        K(rollup_agg_cell->output_ptr_),
        K(agg_col_idx),
        K(agg_ctx.get_minmax_row_idx(agg_col_idx)));
    return ret;
  }

  TO_STRING_KV("aggregate", (is_arg_min ? "arg_min" : "arg_max"), K(vec_tc));

};

namespace helper
{
template <ObItemType func_type>
inline int init_min_max_agg(RuntimeContext &agg_ctx, const int64_t agg_col_id,
                            ObIAllocator &allocator, IAggregate *&agg)
{
#define INIT_AGGREGATE_CASE(vec_tc)                                                                \
  case (vec_tc): {                                                                                 \
    ret = init_agg_func<MinMaxAggregate<vec_tc, T_FUN_MIN == func_type>>(                          \
      agg_ctx, agg_col_id, aggr_info.has_distinct_, allocator, agg);                               \
    if (OB_FAIL(ret)) { SQL_LOG(WARN, "init aggregate failed", K(ret)); }                          \
  } break
#define INIT_ARG_MIN_MAX_AGGREGATE_CASE(vec_tc)                                                     \
  case (vec_tc): {                                                                                  \
    ret = init_agg_func<ArgMinMaxAggregate<vec_tc, T_FUN_ARG_MIN == func_type>>(                    \
        agg_ctx, agg_col_id, aggr_info.has_distinct_, allocator, agg);                              \
  } break

  int ret = OB_SUCCESS;
  agg = nullptr;

  ObAggrInfo &aggr_info = agg_ctx.locate_aggr_info(agg_col_id);
  if (OB_ISNULL(aggr_info.expr_)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_LOG(WARN, "invalid null expr", K(ret));
  } else if ((T_FUN_ARG_MAX == func_type || T_FUN_ARG_MIN == func_type)) {
    if (OB_UNLIKELY(aggr_info.param_exprs_.count() != 2)) {
      ret = OB_ERR_UNEXPECTED;
      SQL_LOG(WARN, "unexpected param count of arg_min/max aggregate", K(ret), K(aggr_info.param_exprs_.count()));
    } else {
      const int32_t CMP_PARAM_IDX = 1;
      const ObExpr *cmp_expr = aggr_info.param_exprs_.at(CMP_PARAM_IDX);
      VecValueTypeClass res_vec =
                        get_vec_value_tc(cmp_expr->datum_meta_.type_, cmp_expr->datum_meta_.scale_,
                                         cmp_expr->datum_meta_.precision_);
      switch (res_vec) {
        LST_DO_CODE(INIT_ARG_MIN_MAX_AGGREGATE_CASE, AGG_VEC_TC_LIST);
      default: {
        ret = OB_ERR_UNEXPECTED;
        SQL_LOG(WARN, "unexpected result type of arg_min/max aggregate", K(ret), K(res_vec));
      }
      }
      if (OB_FAIL(ret)) {
        // do nothing
      } else if (OB_ISNULL(agg_ctx.minmax_row_idxes_)) {
        void *mem = nullptr;
        int32_t agg_cnt = agg_ctx.aggr_infos_.count();
        if (OB_ISNULL(mem = allocator.alloc(sizeof(ObFixedArray<int32_t, ObIAllocator>)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          SQL_LOG(WARN, "failed to alloc type info", K(ret));
        } else {
          agg_ctx.minmax_row_idxes_ = new(mem) ObFixedArray<int32_t, ObIAllocator>(allocator);
          if (OB_FAIL(agg_ctx.minmax_row_idxes_->init(agg_cnt))) {
            SQL_LOG(WARN, "fail to init array", K(ret));
          } else {
            agg_ctx.minmax_row_idxes_->at(agg_col_id) = -1;
          }
        }
      } else {
        agg_ctx.minmax_row_idxes_->at(agg_col_id) = -1;
      }
    }
  } else {
    VecValueTypeClass res_vec =
      get_vec_value_tc(aggr_info.expr_->datum_meta_.type_, aggr_info.expr_->datum_meta_.scale_,
                       aggr_info.expr_->datum_meta_.precision_);

    switch (res_vec) {
      LST_DO_CODE(INIT_AGGREGATE_CASE, AGG_VEC_TC_LIST);
    default: {
      ret = OB_ERR_UNEXPECTED;
      SQL_LOG(WARN, "unexpected result type of min/max aggregate", K(ret), K(res_vec));
    }
    }
  }
  return ret;

#undef INIT_AGGREGATE_CASE
#undef INIT_ARG_MIN_MAX_AGGREGATE_CASE
}
} // namespace helper

} // end namespace aggregate
} // end namespace share
} // end namespace oceanbase
#endif // OCEANBASE_SHARE_AGGREGATE_MIN_MAX_IPP_