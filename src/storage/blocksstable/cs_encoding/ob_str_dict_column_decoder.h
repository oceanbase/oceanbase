/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_ENCODING_OB_STR_DICT_COLUMN_DECODER_H_
#define OCEANBASE_ENCODING_OB_STR_DICT_COLUMN_DECODER_H_

#include "ob_dict_column_decoder.h"

namespace oceanbase
{
namespace blocksstable
{

class ObStrDictColumnDecoder : public ObDictColumnDecoder
{
public:
  static const ObCSColumnHeader::Type type_ = ObCSColumnHeader::STR_DICT;
  ObStrDictColumnDecoder() {}
  virtual ~ObStrDictColumnDecoder() {}
  ObStrDictColumnDecoder(const ObStrDictColumnDecoder &) = delete;
  ObStrDictColumnDecoder &operator=(const ObStrDictColumnDecoder &) = delete;

  virtual int decode(
    const ObColumnCSDecoderCtx &ctx, const int32_t row_id, ObStorageDatum &datum) const override;
  virtual int batch_decode(const ObColumnCSDecoderCtx &ctx, const int32_t *row_ids,
    const int64_t row_cap, common::ObDatum *datums) const override;
  virtual int decode_vector(const ObColumnCSDecoderCtx &ctx, ObVectorDecodeCtx &vector_ctx) const override;
  virtual int decode_and_aggregate(
    const ObColumnCSDecoderCtx &ctx,
    const int64_t row_id,
    ObStorageDatum &datum,
    storage::ObAggCellBase &agg_cell) const override;

  virtual ObCSColumnHeader::Type get_type() const override { return type_; }
};

}  // end namespace blocksstable
}  // end namespace oceanbase

#endif  // OCEANBASE_ENCODING_OB_STR_DICT_COLUMN_DECODER_H_
