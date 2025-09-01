/**
 * Copyright (c) 2025 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
*/
#ifndef OCEANBASE_SHARE_AGGREGATE_STR_PREFIX_MAX_H
#define OCEANBASE_SHARE_AGGREGATE_STR_PREFIX_MAX_H

#include "share/aggregate/iaggregate.h"
namespace oceanbase
{
namespace share
{
namespace aggregate
{

inline constexpr bool is_valid_tc_for_str_prefix_max(VecValueTypeClass vec_tc)
{
  return vec_tc == VEC_TC_STRING || vec_tc == VEC_TC_LOB;
}
template<VecValueTypeClass vec_tc>
class StrPrefixMax final : public BatchAggregateWrapper<StrPrefixMax<vec_tc>>
{
public:
  static const VecValueTypeClass IN_TC = vec_tc;
  static const VecValueTypeClass OUT_TC = vec_tc;
  static const int32_t BUF_BLOCK_SIZE = 512;
public:
  StrPrefixMax()
  {
    static_assert(is_valid_tc_for_str_prefix_max(vec_tc), "StrPrefixMax is only valid for string types");
  }

  inline int add_one_row(RuntimeContext &agg_ctx, const int64_t batch_idx,
      const int64_t batch_size, const bool is_null, const char *data,
      const int32_t data_len, int32_t agg_col_idx, char *agg_cell) override
  {
    int ret = OB_SUCCESS;
    NotNullBitVector &not_nulls = agg_ctx.locate_notnulls_bitmap(agg_col_idx, agg_cell);
    if (!is_null) {
      const ObObjMeta &obj_meta = agg_ctx.aggr_infos_.at(agg_col_idx).param_exprs_.at(0)->obj_meta_;
      int cmp_ret = 0;
      if (not_nulls.at(agg_col_idx)) {
        int32_t agg_cell_len = *reinterpret_cast<int32_t *>(agg_cell + sizeof(char *));
        const char *agg_data = reinterpret_cast<const char *>(*reinterpret_cast<int64_t *>(agg_cell));
        int32_t equal_prefix_len = 0;
        if (OB_FAIL(prefix_data_cmp(obj_meta, agg_data, agg_cell_len, data, data_len, cmp_ret, equal_prefix_len))) {
          SQL_LOG(WARN, "cmp failed", K(ret));
        } else if (cmp_ret < 0) {
          *reinterpret_cast<int64_t *>(agg_cell) = reinterpret_cast<int64_t>(data);
          *reinterpret_cast<int32_t *>(agg_cell + sizeof(char *)) = data_len;
          if (OB_FAIL(set_tmp_var_agg_data(obj_meta, agg_ctx, agg_col_idx, agg_cell, equal_prefix_len))) {
            SQL_LOG(WARN, "set var aggregate data failed", K(ret));
          }
        }
      } else {
        *reinterpret_cast<int64_t *>(agg_cell) = reinterpret_cast<int64_t>(data);
        *reinterpret_cast<int32_t *>(agg_cell + sizeof(char *)) = data_len;
        if (OB_FAIL(set_tmp_var_agg_data(obj_meta, agg_ctx, agg_col_idx, agg_cell, 0))) {
          SQL_LOG(WARN, "set agg data failed", K(ret));
        }
      }
      not_nulls.set(agg_col_idx);
    }
    return ret;
  }

  inline int rollup_aggregation(RuntimeContext &agg_ctx, const int32_t agg_col_idx,
      AggrRowPtr group_row, AggrRowPtr rollup_row, int64_t cur_rollup_group_idx,
      int64_t max_group_cnt) override
  {
    UNUSEDx(agg_ctx, agg_col_idx, group_row,rollup_row, cur_rollup_group_idx, max_group_cnt);
    int ret = OB_NOT_SUPPORTED;
    SQL_LOG(WARN, "string prefix max not supported yet for roll up aggregation", K(ret));
    return ret;
  }

  template<typename ColumnFmt>
  inline int add_row(RuntimeContext &agg_ctx, ColumnFmt &columns, const int32_t row_num,
                    const int32_t agg_col_id, char *agg_cell, void *tmp_res, int64_t &calc_info)
  {
    UNUSED(tmp_res);
    int ret = OB_SUCCESS;
    const char *row_data = nullptr;
    int32_t row_len = 0;
    int cmp_ret = 0;
    columns.get_payload(row_num, row_data, row_len);
    int32_t agg_cell_len = *reinterpret_cast<int32_t *>(agg_cell + sizeof(char *));
    const char *agg_data = reinterpret_cast<const char *>(*reinterpret_cast<int64_t *>(agg_cell));
    const ObObjMeta &obj_meta = agg_ctx.aggr_infos_.at(agg_col_id).param_exprs_.at(0)->obj_meta_;
    bool &first_calculated = reinterpret_cast<bool &>(calc_info);
    if (first_calculated) {
      int32_t equal_prefix_len = 0;
      if (OB_FAIL(prefix_data_cmp(obj_meta, agg_data, agg_cell_len, row_data, row_len, cmp_ret, equal_prefix_len))) {
        SQL_LOG(WARN, "compare failed", K(ret));
      } else if (cmp_ret < 0) {
        // row data larger than agg data
        *reinterpret_cast<int64_t *>(agg_cell) = reinterpret_cast<int64_t>(row_data);
        *reinterpret_cast<int32_t *>(agg_cell + sizeof(char *)) = row_len;
        if (equal_prefix_len > 0) {
          if (VEC_TC_LOB == vec_tc) {
            // generate new prefix with lob format
            if (OB_FAIL(set_tmp_var_agg_data(obj_meta, agg_ctx, agg_col_id, agg_cell, equal_prefix_len))) {
              SQL_LOG(WARN, "set lob var agg data failed", K(ret));
            }
          } else {
            *reinterpret_cast<int32_t *>(agg_cell + sizeof(char *)) = equal_prefix_len;
          }
        }
      }
    } else {
      *reinterpret_cast<int64_t *>(agg_cell) = reinterpret_cast<int64_t>(row_data);
      *reinterpret_cast<int32_t *>(agg_cell + sizeof(char *)) = row_len;
      first_calculated = true;
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
      SQL_LOG(DEBUG, "add null row", K(agg_col_id), K(row_num));
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
    const int32_t agg_col_id, const char *agg_cell, const int32_t agg_cell_len)
  {
    int ret = OB_SUCCESS;
    ObEvalCtx &ctx = agg_ctx.eval_ctx_;
    int64_t output_idx = ctx.get_batch_idx();
    ColumnFmt *res_vec = static_cast<ColumnFmt *>(agg_expr.get_vector(ctx));
    const NotNullBitVector &not_nulls = agg_ctx.locate_notnulls_bitmap(agg_col_id, agg_cell);
    if (OB_LIKELY(not_nulls.at(agg_col_id))) {
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
      res_vec->set_null(output_idx);
    }
    return ret;
  }

  int collect_tmp_result(RuntimeContext &agg_ctx, const int32_t agg_col_id, char *agg_cell)
  {
    int ret = OB_SUCCESS;
    NotNullBitVector &not_nulls = agg_ctx.locate_notnulls_bitmap(agg_col_id, agg_cell);
    const ObObjMeta &obj_meta = agg_ctx.aggr_infos_.at(agg_col_id).param_exprs_.at(0)->obj_meta_;
    if (agg_ctx.win_func_agg_) {
      // do nothing
    } else if (not_nulls.at(agg_col_id)) {
      if (OB_FAIL(set_tmp_var_agg_data(obj_meta, agg_ctx, agg_col_id, agg_cell, 0))) {
        SQL_LOG(WARN, "set variable aggregate data failed", K(ret));
      }
    }
    return ret;
  }

  inline int64_t get_batch_calc_info(RuntimeContext &agg_ctx, int32_t agg_col_idx,
      char *agg_cell) override
  {
    bool first_calculated = false;
    NotNullBitVector &not_nulls = agg_ctx.locate_notnulls_bitmap(agg_col_idx, agg_cell);
    if (not_nulls.at(agg_col_idx)) {
      first_calculated = true;
    }
    return first_calculated;
  }

  TO_STRING_KV("aggregate", "str_prefix_max");

private:
  int set_tmp_var_agg_data(const ObObjMeta &obj_meta, RuntimeContext &agg_ctx,
      const int32_t agg_col_id, char *agg_cell, const ObLength prefix_len)
  {
    // generate new prefix on demand and deep copy to tmp buffer
    int ret = OB_SUCCESS;
    const bool has_lob_header = obj_meta.has_lob_header();
    char *agg_data = reinterpret_cast<char *>(*reinterpret_cast<int64_t *>(agg_cell));
    int32_t agg_data_len = *reinterpret_cast<int32_t *>(agg_cell + sizeof(char *));
    char *tmp_buf = reinterpret_cast<char *>(*reinterpret_cast<int64_t *>(agg_cell + sizeof(char *) + sizeof(int32_t)));
    int32_t &cap = *reinterpret_cast<int32_t *>(agg_cell + sizeof(int32_t) + sizeof(char *) * 2);
    const bool need_set_prefix = prefix_len != 0;
    int32_t tmp_agg_data_len = agg_data_len;
    if (need_set_prefix) {
      if (VEC_TC_LOB == vec_tc && has_lob_header) {
        tmp_agg_data_len = prefix_len + sizeof(ObLobCommon);
      } else {
        tmp_agg_data_len = prefix_len;
      }
    }

    if (cap < tmp_agg_data_len) {
      int32_t new_cap = 2 *((tmp_agg_data_len + BUF_BLOCK_SIZE - 1) / BUF_BLOCK_SIZE) * BUF_BLOCK_SIZE;
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
      if (VEC_TC_LOB == vec_tc && has_lob_header && need_set_prefix) {
        if (OB_FAIL(set_tmp_lob_prefix(obj_meta, agg_data, agg_data_len, prefix_len, tmp_buf, agg_cell))) {
          SQL_LOG(WARN, "failed to set tmp lob prefix", K(ret));
        }
      } else {
        MEMCPY(tmp_buf, agg_data, tmp_agg_data_len);
        *reinterpret_cast<int64_t *>(agg_cell) = reinterpret_cast<int64_t>(tmp_buf);
        *reinterpret_cast<int32_t *>(agg_cell + sizeof(char *)) = tmp_agg_data_len;
      }
    }
    return ret;
  }

  inline int set_tmp_lob_prefix(const ObObjMeta &obj_meta, const char *agg_data, const ObLength agg_len,
      const ObLength prefix_len, char *tmp_buf, char *agg_cell)
  {
    int ret = OB_SUCCESS;
    const char *str = nullptr;
    const ObLobCommon *lob = reinterpret_cast<const ObLobCommon *>(agg_data);
    if (agg_len != 0 && !lob->is_mem_loc_ && lob->in_row_) {
      str = lob->get_inrow_data_ptr();
    } else {
      ObString lob_data;
      common::ObArenaAllocator allocator(ObModIds::OB_LOB_READER, OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
      ObTextStringIter str_iter(ObLongTextType, obj_meta.get_collation_type(),
          ObString(agg_len, reinterpret_cast<const char *>(agg_data)), true);
      if (OB_FAIL(str_iter.init(0, NULL, &allocator))) {
        SQL_LOG(WARN, "init lob str iter failed", K(ret), K(obj_meta));
      } else if (OB_FAIL(str_iter.get_full_data(lob_data))) {
        SQL_LOG(WARN, "get left lob str iter full data failed ", K(ret), K(obj_meta), K(str_iter));
      } else {
        str = lob_data.ptr();
      }
    }

    if (OB_SUCC(ret)) {
      ObLobCommon *new_lob_data = new (tmp_buf) ObLobCommon();
      MEMCPY(new_lob_data->buffer_, str, prefix_len);
      *reinterpret_cast<int64_t *>(agg_cell) = reinterpret_cast<int64_t>(tmp_buf);
      *reinterpret_cast<int32_t *>(agg_cell + sizeof(char *)) = prefix_len + sizeof(ObLobCommon);
    }
    return ret;
  }

  inline int prefix_data_cmp(const ObObjMeta &obj_meta, const char *l_v, const ObLength l_len,
      const char *r_v, const ObLength r_len, int &cmp_ret, ObLength &equal_prefix_length)
  {
    int ret = OB_SUCCESS;
    const char *l_str = l_v;
    ObLength l_str_len = l_len;
    const char *r_str = r_v;
    ObLength r_str_len = r_len;
    const bool has_lob_header = obj_meta.has_lob_header();
    if (VEC_TC_LOB == vec_tc && has_lob_header) {
      const ObLobCommon *rlob = reinterpret_cast<const ObLobCommon *>(r_v);
      const ObLobCommon *llob = reinterpret_cast<const ObLobCommon *>(l_v);
      if (r_len != 0 && !rlob->is_mem_loc_ && rlob->in_row_ &&
          l_len != 0 && !llob->is_mem_loc_ && llob->in_row_) {
        if (OB_FAIL(prefix_str_cmp(obj_meta,
            llob->get_inrow_data_ptr(), static_cast<int32_t>(llob->get_byte_size(l_len)),
            rlob->get_inrow_data_ptr(), static_cast<int32_t>(rlob->get_byte_size(r_len)),
            cmp_ret, equal_prefix_length))) {
          SQL_LOG(WARN, "prefix string comapre failed", K(ret));
        }
      } else {
        ObString l_data;
        ObString r_data;
        common::ObArenaAllocator allocator(ObModIds::OB_LOB_READER, OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
        ObTextStringIter l_instr_iter(ObLongTextType, obj_meta.get_collation_type(),
                                      ObString(l_len, reinterpret_cast<const char *>(l_v)), true);
        ObTextStringIter r_instr_iter(ObLongTextType, obj_meta.get_collation_type(),
                                      ObString(r_len, reinterpret_cast<const char *>(r_v)), true);
        if (OB_FAIL(l_instr_iter.init(0, NULL, &allocator))) {
          SQL_LOG(WARN, "Lob: init left lob str iter failed", K(ret), K(obj_meta));
        } else if (OB_FAIL(l_instr_iter.get_full_data(l_data))) {
          SQL_LOG(WARN, "Lob: get left lob str iter full data failed ", K(ret), K(obj_meta), K(l_instr_iter));
        } else if (OB_FAIL(r_instr_iter.init(0, NULL, &allocator))) {
          SQL_LOG(WARN, "Lob: init right lob str iter failed", K(ret));
        } else if (OB_FAIL(r_instr_iter.get_full_data(r_data))) {
          SQL_LOG(WARN, "Lob: get right lob str iter full data failed ", K(ret), K(obj_meta), K(r_instr_iter));
        } else if (OB_FAIL(prefix_str_cmp(
            obj_meta, l_data.ptr(), l_data.length(), r_data.ptr(), r_data.length(), cmp_ret, equal_prefix_length))) {
          SQL_LOG(WARN, "prefix string comapre failed", K(ret));
        }
      }
    } else if (OB_FAIL(prefix_str_cmp(obj_meta, l_str, l_str_len, r_str, r_str_len, cmp_ret, equal_prefix_length))) {
      SQL_LOG(WARN, "prefix string comapre failed", K(ret));
    }
    return ret;
  }

  inline int prefix_str_cmp(const ObObjMeta &obj_meta, const char *l_str, const ObLength l_len,
      const char *r_str, const ObLength r_len, int &cmp_ret, ObLength &equal_prefix_length)
  {
    int ret = OB_SUCCESS;
    const bool end_with_space = common::is_calc_with_end_space(
        obj_meta.get_type(),
        obj_meta.get_type(),
        lib::is_oracle_mode(),
        obj_meta.get_collation_type(),
        obj_meta.get_collation_type());
    const ObCollationType &coll = obj_meta.get_collation_type();
    const int64_t l_char_num = ObCharset::strlen_char(coll, l_str, l_len);
    const int64_t r_char_num = ObCharset::strlen_char(coll, r_str, r_len);
    const bool l_shorter = l_char_num < r_char_num;

    const char *short_str = r_str;
    ObLength short_len = r_len;
    const char *long_str = l_str;
    ObLength long_len = l_len;
    if (l_shorter) {
      short_str = l_str;
      short_len = l_len;
      long_str = r_str;
      long_len = r_len;
    }

    const int64_t prefix_char_num = MIN(l_char_num, r_char_num);
    const ObLength prefix_length = ObCharset::charpos(coll, long_str, long_len, prefix_char_num);
    const int p_cmp_ret = ObCharset::strcmpsp(coll, long_str, prefix_length, short_str, short_len, end_with_space);
    if (0 != p_cmp_ret) {
      // prefix not match
      cmp_ret = l_shorter ? -p_cmp_ret : p_cmp_ret;
    } else {
      if (l_char_num == r_char_num) {
        cmp_ret = 0;
      } else {
        // prefix match, shorter string is larger
        cmp_ret = l_shorter ? 1 : -1;
        equal_prefix_length = prefix_length;
      }
    }
    return ret;
  }
};

} // namespace aggregate
} // namespace share
} // namespace oceanbase

#endif // OCEANBASE_SHARE_AGGREGATE_STR_PREFIX_MAX_H