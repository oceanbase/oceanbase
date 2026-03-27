/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "sql/engine/expr/ob_expr_cmp_func.ipp"

namespace oceanbase
{
namespace sql
{

static const int COMPILATION_UNIT = 8;

#define DEF_COMPILATION_VARS(name, max_val, unit_idx)                                              \
  constexpr int name##_unit_size =                                                                 \
    max_val / COMPILATION_UNIT + (max_val % COMPILATION_UNIT == 0 ? 0 : 1);                        \
  constexpr int name##_start =                                                                     \
    (name##_unit_size * unit_idx < max_val ? name##_unit_size * unit_idx : max_val);               \
  constexpr int name##_end =                                                                       \
    (name##_start + name##_unit_size >= max_val ? max_val : name##_start + name##_unit_size);

#define DEF_COMPILE_FUNC_INIT(unit_idx)                                                            \
  void __init_expr_cmp_func##unit_idx()                                                            \
  {                                                                                                \
    DEF_COMPILATION_VARS(ty, ObMaxType, unit_idx);                                                 \
    DEF_COMPILATION_VARS(tc, ObMaxTC, unit_idx);                                                   \
    Ob2DArrayConstIniter<ty_end, ObMaxType, TypeExprCmpIniter, ty_start, 0>::init();           \
    Ob2DArrayConstIniter<tc_end, ObMaxTC, TCExprCmpIniter, tc_start, 0>::init();               \
  }

} // end sql
} // end oceanbase