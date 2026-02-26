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

#define INIT_GENERAL_CASE(func_type, vec_tc)                                                       \
  case (vec_tc): {                                                                                 \
    ret =                                                                                          \
      init_agg_func<SingleRowAggregate<func_type, vec_tc, vec_tc>>(agg_ctx, col_id, allocator, agg);    \
    if (OB_FAIL(ret)) { SQL_LOG(WARN, "init aggregate failed", K(vec_tc), K(*aggr_info.expr_)); }  \
  } break

#define INIT_GENERAL_MIN_CASE(vec_tc) INIT_GENERAL_CASE(T_FUN_MIN, vec_tc)

#define INIT_GENERAL_MAX_CASE(vec_tc) INIT_GENERAL_CASE(T_FUN_MAX, vec_tc)

#define INIT_GENERAL_COUNT_SUM_CASE(vec_tc) INIT_GENERAL_CASE(T_FUN_COUNT_SUM, vec_tc)

#define INIT_COUNT_CASE(vec_tc)                                                                    \
  case (vec_tc): {                                                                                 \
    if (lib::is_oracle_mode()) {                                                                   \
      ret = init_agg_func<SingleRowAggregate<T_FUN_COUNT, vec_tc, VEC_TC_NUMBER>>(agg_ctx, col_id,      \
                                                                                  allocator, agg); \
    } else {                                                                                       \
      ret = init_agg_func<SingleRowAggregate<T_FUN_COUNT, vec_tc, VEC_TC_INTEGER>>(                \
        agg_ctx, col_id, allocator, agg);                                                               \
    }                                                                                              \
    if (OB_FAIL(ret)) { SQL_LOG(WARN, "init aggregate failed", K(vec_tc), K(*aggr_info.expr_)); }  \
  } break

#define INIT_SUM_TO_NMB_CASE(vec_tc)                                                               \
  ret = init_agg_func<SingleRowAggregate<T_FUN_SUM, vec_tc, VEC_TC_NUMBER>>(agg_ctx, col_id, allocator, \
                                                                            agg);                  \
  if (OB_FAIL(ret)) { SQL_LOG(WARN, "init aggregate failed", K(vec_tc), K(*aggr_info.expr_)); }

#define INIT_SUM_TO_DEC_CASE(vec_tc, dec_tc)                                                       \
  ret = init_agg_func<SingleRowAggregate<T_FUN_SUM, vec_tc, dec_tc>>(agg_ctx, col_id, allocator, agg);  \
  if (OB_FAIL(ret)) {                                                                              \
    SQL_LOG(WARN, "init aggregate failed", K(vec_tc), K(dec_tc), K(*aggr_info.expr_));             \
  }
