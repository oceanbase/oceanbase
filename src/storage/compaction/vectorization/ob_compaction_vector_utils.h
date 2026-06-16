/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OB_STORAGE_COMPACTION_VECTORIZATION_OB_COMPACTION_VECTOR_UTILS_H_
#define OB_STORAGE_COMPACTION_VECTORIZATION_OB_COMPACTION_VECTOR_UTILS_H_

#include "share/datum/ob_datum.h"
#include "share/vector/ob_i_vector.h"
#include "sql/engine/expr/ob_expr.h"

namespace oceanbase
{
namespace compaction
{
class ObCompactionVectorUtils
{
public:
  static int new_vector(sql::VectorHeader &vector_header, VecValueTypeClass value_tc);
  static int prepare_vector(ObIVector *vector, const int64_t max_batch_size,
                            ObIAllocator &allocator);
};
} // namespace compaction
} // namespace oceanbase
#endif