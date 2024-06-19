/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_ENCODING_OB_INT_DICT_COLUMN_DECODER_H_
#define OCEANBASE_ENCODING_OB_INT_DICT_COLUMN_DECODER_H_

#include "ob_dict_column_decoder.h"

namespace oceanbase
{
namespace blocksstable
{

class ObIntDictColumnDecoder : public ObDictColumnDecoder
{
public:
  static const ObCSColumnHeader::Type type_ = ObCSColumnHeader::INT_DICT;
  ObIntDictColumnDecoder() {}
  virtual ~ObIntDictColumnDecoder() {}
  ObIntDictColumnDecoder(const ObDictColumnDecoder &) = delete;
  ObIntDictColumnDecoder &operator=(const ObDictColumnDecoder &) = delete;

  virtual int decode(
    const ObColumnCSDecoderCtx &ctx, const int32_t row_id, common::ObDatum &datum) const override;
  virtual int batch_decode(const ObColumnCSDecoderCtx &ctx, const int32_t *row_ids,
    const int64_t row_cap, common::ObDatum *datums) const override;
  virtual int decode_vector(const ObColumnCSDecoderCtx &ctx, ObVectorDecodeCtx &vector_ctx) const override;
  virtual int decode_and_aggregate(
    const ObColumnCSDecoderCtx &ctx,
    const int64_t row_id,
    ObStorageDatum &datum,
    storage::ObAggCell &agg_cell) const override;

  virtual ObCSColumnHeader::Type get_type() const override { return type_; }
};

}  // end namespace blocksstable
}  // end namespace oceanbase

#endif  // OCEANBASE_ENCODING_OB_INT_DICT_COLUMN_DECODER_H_
