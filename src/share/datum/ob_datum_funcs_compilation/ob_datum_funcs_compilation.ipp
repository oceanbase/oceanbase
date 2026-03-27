/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STR_DATUM_FUNCS_IPP
#define OCEANBASE_STR_DATUM_FUNCS_IPP

#include "share/datum/ob_datum_funcs.h"
#include "share/datum/ob_datum_cmp_func_def.h"
#include "common/object/ob_obj_funcs.h"
#include "sql/engine/ob_serializable_function.h"
#include "sql/engine/ob_bit_vector.h"
#include "share/ob_cluster_version.h"
#include "share/datum/ob_datum_funcs_impl.h"

namespace oceanbase
{
using namespace sql;
namespace common
{
static const int COMPILATION_UNIT = 8;

#define DEF_COMPILATION_VARS(name, max_val, unit_idx)                                              \
  constexpr int name##_unit_size =                                                                 \
    max_val / COMPILATION_UNIT + (max_val % COMPILATION_UNIT == 0 ? 0 : 1);                        \
  constexpr int name##_start =                                                                     \
    (name##_unit_size * unit_idx < max_val ? name##_unit_size * unit_idx : max_val);               \
  constexpr int name##_end =                                                                       \
    (name##_start + name##_unit_size >= max_val ? max_val : name##_start + name##_unit_size);

#define DEF_DATUM_FUNC_INIT(unit_idx)                                                              \
  void __init_datum_func##unit_idx()                                                               \
  {                                                                                                \
    DEF_COMPILATION_VARS(ty, ObMaxType, unit_idx);                                                 \
    DEF_COMPILATION_VARS(tc, ObMaxTC, unit_idx);                                                   \
    DEF_COMPILATION_VARS(ty_basic, ObMaxType, unit_idx);                                           \
    Ob2DArrayConstIniter<ty_end, ObMaxType, TypeCmpIniter, ty_start, 0>::init();                   \
    Ob2DArrayConstIniter<tc_end, ObMaxTC, TCCmpIniter, tc_start, 0>::init();                       \
    ObArrayConstIniter<ty_basic_end, InitBasicFuncArray, ty_basic_start>::init();                  \
  }

} // end common
} // end oceanbase
#endif // OCEANBASE_STR_DATUM_FUNCS_IPP