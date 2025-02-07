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

#include "sql/engine/expr/vector_cast/vector_cast_impl.ipp"

namespace oceanbase
{
namespace sql
{

static const int COMPILATION_UNIT = 7;

#define DEF_COMPILATION_VARS(name, max_val, unit_idx)                                              \
  constexpr int name##_unit_size =                                                                 \
    max_val / COMPILATION_UNIT + (max_val % COMPILATION_UNIT == 0 ? 0 : 1);                        \
  constexpr int name##_start =                                                                     \
    (name##_unit_size * unit_idx < max_val ? name##_unit_size * unit_idx : max_val);               \
  constexpr int name##_end =                                                                       \
    (name##_start + name##_unit_size >= max_val ? max_val : name##_start + name##_unit_size);

#define DEF_COMPILE_FUNC_INIT(unit_idx)                                                                 \
  void __init_vec_cast_func##unit_idx()                                                                 \
  {                                                                                                     \
    DEF_COMPILATION_VARS(tc, ObMaxTC, unit_idx);                                                        \
    Ob2DArrayConstIniter<tc_end, ObMaxTC, VectorCastIniter, tc_start, VEC_TC_INTEGER>::init();          \
    Ob2DArrayConstIniter<tc_end, ObMaxTC, EvalArgVecCasterIniter, tc_start, VEC_TC_INTEGER>::init();    \
  }

} // end sql
} // end oceanbase