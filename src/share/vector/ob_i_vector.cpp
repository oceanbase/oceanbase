/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "share/vector/ob_i_vector.h"
#include "sql/engine/expr/ob_expr.h"

namespace oceanbase
{
namespace common
{

int ObIVector::hash(BATCH_EVAL_HASH_ARGS) const
{
  int ret = OB_SUCCESS;
  switch (expr.get_vec_hash_algo()) {
    case VEC_HASH_ALGO_CRC:
      ret = crc_hash_v3(expr, hash_values, skip, bound, seeds, is_batch_seed);
      break;
    case VEC_HASH_ALGO_MURMUR:
    default:
      ret = murmur_hash_v3(expr, hash_values, skip, bound, seeds, is_batch_seed);
      break;
  }
  return ret;
}

int ObIVector::hash_for_one_row(EVAL_HASH_ARGS_FOR_ROW) const
{
  int ret = OB_SUCCESS;
  switch (expr.get_vec_hash_algo()) {
    case VEC_HASH_ALGO_CRC:
      ret = crc_hash_v3_for_one_row(expr, hash_value, batch_idx, batch_size, seed);
      break;
    case VEC_HASH_ALGO_MURMUR:
    default:
      ret = murmur_hash_v3_for_one_row(expr, hash_value, batch_idx, batch_size, seed);
      break;
  }
  return ret;
}

} // namespace common
} // namespace oceanbase
