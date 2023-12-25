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

#ifndef OCEANBASE_CS_ENCODING_OB_STRING_STREAM_VECTOR_DECODER_H_
#define OCEANBASE_CS_ENCODING_OB_STRING_STREAM_VECTOR_DECODER_H_

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
class ObStringStreamVecDecoder final
{
public:
  struct StrVecDecoderCtx final
  {
  public:
    StrVecDecoderCtx(const char *str_data,
                     const ObStringStreamDecoderCtx *str_ctx,
                     const char *offset_data,
                     const ObIntegerStreamDecoderCtx *offset_ctx,
                     const bool need_copy)
      : str_data_(str_data),
        str_ctx_(str_ctx),
        offset_data_(offset_data),
        offset_ctx_(offset_ctx),
        need_copy_(need_copy)
    {
    }

    TO_STRING_KV(KP_(str_data), KPC_(str_ctx), KP_(offset_data), KPC_(offset_ctx), K_(need_copy));

    const char *str_data_;
    const ObStringStreamDecoderCtx *str_ctx_;
    const char *offset_data_;
    const ObIntegerStreamDecoderCtx *offset_ctx_;
    const bool need_copy_;
  };

  static int decode_vector(
      const ObBaseColumnDecoderCtx &base_col_ctx,
      const StrVecDecoderCtx &vec_decode_ctx,
      const char *ref_data,
      const ObVecDecodeRefWidth ref_width,
      ObVectorDecodeCtx &vector_ctx);

private:
  template<typename VectorType>
  static int decode_vector_(
      const ObBaseColumnDecoderCtx &base_col_ctx,
      const StrVecDecoderCtx &vec_decode_ctx,
      const char *ref_data,
      const ObVecDecodeRefWidth ref_width,
      ObVectorDecodeCtx &vector_ctx);

};

} // namesapce blocksstable
} // namespace oceanbase

#endif // OCEANBASE_CS_ENCODING_OB_STRING_STREAM_VECTOR_DECODER_H_