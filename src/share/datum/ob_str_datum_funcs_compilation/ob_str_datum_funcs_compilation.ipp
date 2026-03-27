
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

#define DEF_STR_FUNC_INIT(COLLATION, unit_idx)                                                 \
  void __init_str_func##unit_idx()                                                             \
  {                                                                                            \
    str_cmp_initer<COLLATION>::init_array();                                                   \
    str_basic_initer<COLLATION, 0>::init_array();                                              \
    str_basic_initer<COLLATION, 1>::init_array();                                              \
  }

} // end common
} // end oceanbase
#endif // OCEANBASE_STR_DATUM_FUNCS_IPP