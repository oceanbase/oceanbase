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

#define USING_LOG_PREFIX STORAGE

#include "ob_cs_vector_decoding_util.h"

namespace oceanbase
{
namespace blocksstable
{

int ObCSVectorDecodingUtil::decode_all_null_vector(
    const int32_t *row_ids,
    const int64_t row_cap,
    sql::VectorHeader &vec_header,
    const int64_t vec_offset)
{
  int ret = OB_SUCCESS;
  VectorFormat vec_format = vec_header.get_format();
  ObIVector *vector = vec_header.get_vector();
  switch (vec_format) {
    case VEC_FIXED:
    case VEC_DISCRETE:
    case VEC_CONTINUOUS: {
      ObBitmapNullVectorBase *null_vec_base = static_cast<ObBitmapNullVectorBase*>(vector);
      for (int64_t i = 0; i < row_cap; i++) {
        null_vec_base->set_null(vec_offset + i);
      }
      break;
    }
    case VEC_UNIFORM: {
      ObUniformFormat<false> *uni_vec = static_cast<ObUniformFormat<false> *>(vector);
      for (int64_t i = 0; i < row_cap; i++) {
        uni_vec->set_null(vec_offset + i);
      }
      break;
    }
    default: {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected vector format", K(ret), K(vec_format));
      break;
    }
  }
  return ret;
}

}
}
