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

#ifndef OCEANBASE_CS_ENCODING_OB_INTEGER_STREAM_VECTOR_DECODER_H_
#define OCEANBASE_CS_ENCODING_OB_INTEGER_STREAM_VECTOR_DECODER_H_

#include "ob_stream_encoding_struct.h"
#include "ob_column_encoding_struct.h"
#include "src/share/vector/ob_uniform_vector.h"
#include "src/share/vector/ob_continuous_vector.h"
#include "src/share/vector/ob_discrete_vector.h"
#include "src/share/vector/ob_fixed_length_vector.h"

namespace oceanbase
{
namespace blocksstable
{

class ObVectorDecodeCtx;
class ObIntegerStreamVecDecoder final
{
public:
  static int decode_vector(
      const ObBaseColumnDecoderCtx &base_col_ctx,
      const char *data,
      const ObIntegerStreamDecoderCtx &ctx,
      const char *ref_data,
      const ObVecDecodeRefWidth ref_width,
      ObVectorDecodeCtx &vector_ctx);

private:
  template<typename VectorType>
  static int decode_vector_(
      const ObBaseColumnDecoderCtx &base_col_ctx,
      const char *data,
      const ObIntegerStreamDecoderCtx &ctx,
      const char *ref_data,
      const ObVecDecodeRefWidth ref_width,
      ObVectorDecodeCtx &vector_ctx);

};

} // namesapce blocksstable
} // namespace oceanbase

#endif // OCEANBASE_CS_ENCODING_OB_INTEGER_STREAM_VECTOR_DECODER_H_