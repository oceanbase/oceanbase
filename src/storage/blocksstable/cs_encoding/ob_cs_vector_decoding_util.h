/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_CS_ENCODING_OB_CS_VECTOR_DECODING_UTIL_H_
#define OCEANBASE_CS_ENCODING_OB_CS_VECTOR_DECODING_UTIL_H_

#include "src/share/vector/ob_uniform_vector.h"
#include "src/share/vector/ob_continuous_vector.h"
#include "src/share/vector/ob_discrete_vector.h"
#include "src/share/vector/ob_fixed_length_vector.h"

namespace oceanbase
{
namespace blocksstable
{
class ObCSVectorDecodingUtil final
{
public:
  static int decode_all_null_vector(
      const int32_t *row_ids,
      const int64_t row_cap,
      sql::VectorHeader &vec_header,
      const int64_t vec_offset);
};

}
}

#endif
