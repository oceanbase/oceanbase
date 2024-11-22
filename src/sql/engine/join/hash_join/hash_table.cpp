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

#include "sql/engine/join/hash_join/hash_table.h"

namespace oceanbase
{
namespace sql
{

template <typename Bucket>
int GenericProber<Bucket>::equal(JoinTableCtx &ctx,
                         Bucket *bucket,
                         const int64_t batch_idx,
                         bool &is_equal) {
  int ret = OB_SUCCESS;
  is_equal = true;
  ObHJStoredRow *build_sr = bucket->get_stored_row();
  int cmp_ret = 0;
  for (int64_t i = 0; is_equal && i < ctx.build_key_proj_->count(); i++) {
    int64_t build_col_idx = ctx.build_key_proj_->at(i);
    ObExpr *probe_key = ctx.probe_keys_->at(i);
    ObIVector *vec = probe_key->get_vector(*ctx.eval_ctx_);
    const char *r_v = NULL;
    ObLength r_len = 0;
    build_sr->get_cell_payload(ctx.build_row_meta_, build_col_idx, r_v, r_len);
    vec->null_first_cmp(*probe_key, batch_idx,
                        build_sr->is_null(build_col_idx),
                        r_v, r_len, cmp_ret);
    is_equal = (cmp_ret == 0);
    LOG_DEBUG("generic probe equal", K(cmp_ret), K(is_equal));
  }

  return ret;
}

template <typename Bucket>
OB_INLINE int GenericProber<Bucket>::equal_batch(
    JoinTableCtx &ctx, const uint16_t *sel, const uint16_t sel_cnt, bool is_opt)
{
  int ret = OB_SUCCESS;
  MEMSET(ctx.cmp_ret_map_, 0, sizeof(int) * sel_cnt);
  MEMSET(ctx.cmp_ret_for_one_col_, 0, sizeof(int) * sel_cnt);

  for (int64_t i = 0; OB_SUCC(ret) && i < ctx.build_key_proj_->count(); i++) {
    int64_t build_col_idx = ctx.build_key_proj_->at(i);
    ObExpr *probe_key = ctx.probe_keys_->at(i);
    ObIVector *vec = probe_key->get_vector(*ctx.eval_ctx_);
    ObExpr *build_key = ctx.build_keys_->at(i);
    // join key type on build and probe may be different
    if (ctx.probe_cmp_funcs_.at(i) != nullptr) {
      NullSafeRowCmpFunc cmp_func = ctx.probe_cmp_funcs_.at(i);
      for (int64_t row_idx = 0; OB_SUCC(ret) && row_idx < sel_cnt; row_idx++) {
        int64_t batch_idx = sel[row_idx];
        const char *l_v = NULL;
        const char *r_v = NULL;
        ObLength l_len = 0;
        ObLength r_len = 0;
        bool l_is_null = ctx.unmatched_rows_[row_idx]->is_null(build_col_idx);
        bool r_is_null = false;
        ctx.unmatched_rows_[row_idx]->get_cell_payload(
            ctx.build_row_meta_, build_col_idx, l_v, l_len);
        vec->get_payload(batch_idx, r_is_null, r_v, r_len);
        int cmp_ret = 0;
        if (!ctx.is_ns_equal_cond_->at(i) && (l_is_null || r_is_null)) {
          cmp_ret = 1;
        } else if (OB_FAIL(cmp_func(build_key->obj_meta_,
                probe_key->obj_meta_,
                l_v,
                l_len,
                l_is_null,
                r_v,
                r_len,
                r_is_null,
                cmp_ret))) {
          LOG_WARN("failed to compare with cmp func", K(ret));
        }
        ctx.cmp_ret_map_[row_idx] |= (cmp_ret != 0);
      }
    } else {
      if (ctx.is_ns_equal_cond_->at(i)) {
        if (OB_FAIL(vec->null_first_cmp_batch_rows(*probe_key,
                sel,
                sel_cnt,
                reinterpret_cast<ObCompactRow **>(ctx.unmatched_rows_),
                build_col_idx,
                ctx.build_row_meta_,
                ctx.cmp_ret_for_one_col_))) {
          LOG_WARN("failed to compare with ns equal cond", K(ret));
        }
      } else if (is_opt &&
                 (!ctx.build_cols_have_null_->at(i) && !ctx.probe_cols_have_null_->at(i))) {
        if (OB_FAIL(vec->no_null_cmp_batch_rows(*probe_key,
                sel,
                sel_cnt,
                reinterpret_cast<ObCompactRow **>(ctx.unmatched_rows_),
                build_col_idx,
                ctx.build_row_meta_,
                ctx.cmp_ret_for_one_col_))) {
          LOG_WARN("failed to compare with no null", K(ret));
        }
      } else {
        if (OB_FAIL(vec->null_first_cmp_batch_rows(*probe_key,
                sel,
                sel_cnt,
                reinterpret_cast<ObCompactRow **>(ctx.unmatched_rows_),
                build_col_idx,
                ctx.build_row_meta_,
                ctx.cmp_ret_for_one_col_))) {
          LOG_WARN("failed to compare with have null", K(ret));
        }
        // have null and not ns equal, need to check null is not match null
        if (ctx.build_cols_have_null_->at(i)) {
          for (int64_t row_idx = 0; row_idx < sel_cnt; row_idx++) {
            ObHJStoredRow *row_ptr = ctx.unmatched_rows_[row_idx];
            if (row_ptr->is_null(build_col_idx)) {
              ctx.cmp_ret_for_one_col_[row_idx] = 1;
            }
          }
        }
        if (ctx.probe_cols_have_null_->at(i)) {
          VectorFormat format = vec->get_format();
          if (is_uniform_format(format)) {
            const uint64_t idx_mask = VEC_UNIFORM_CONST == format ? 0 : UINT64_MAX;
            const ObDatum *datums = static_cast<ObUniformBase *>(vec)->get_datums();
            for (int64_t row_idx = 0; row_idx < sel_cnt; row_idx++) {
              int64_t batch_idx = sel[row_idx];
              if (datums[batch_idx & idx_mask].is_null()) {
                ctx.cmp_ret_for_one_col_[row_idx] = 1;
              }
            }
          } else {
            ObBitVector *nulls = static_cast<ObBitmapNullVectorBase *>(vec)->get_nulls();
            for (int64_t row_idx = 0; row_idx < sel_cnt; row_idx++) {
              int64_t batch_idx = sel[row_idx];
              if (nulls->at(batch_idx)) {
                ctx.cmp_ret_for_one_col_[row_idx] = 1;
              }
            }
          }
        }
      }
      for (int64_t row_idx = 0; row_idx < sel_cnt; row_idx++) {
        ctx.cmp_ret_map_[row_idx] |= ctx.cmp_ret_for_one_col_[row_idx];
      }
    }
  }
  return ret;
}

} // end namespace sql
} // end namespace oceanbase
