/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
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
