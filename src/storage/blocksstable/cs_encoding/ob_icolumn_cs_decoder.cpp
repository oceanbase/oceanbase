/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE

#include "ob_icolumn_cs_decoder.h"
#include "ob_cs_encoding_util.h"
#include "storage/access/ob_aggregate_base.h"

namespace oceanbase
{
namespace blocksstable
{
using namespace common;

int ObIColumnCSDecoder::inner_get_null_count(
    const ObColumnCSDecoderCtx &ctx,
    const int32_t *row_ids,
    const int64_t row_cap,
    int64_t &null_count) const
{
  int ret = OB_NOT_SUPPORTED;
  return ret;
}

int ObIColumnCSDecoder::get_null_count(
    const ObColumnCSDecoderCtx &ctx,
    const int32_t *row_ids,
    const int64_t row_cap,
    int64_t &null_count) const
{
  INIT_SUCC(ret);
  if (ctx.base_ctx_.is_nop_replaced()) {
    null_count = 0;
  } else if (OB_FAIL(inner_get_null_count(ctx, row_ids, row_cap, null_count))) {
    LOG_WARN("fail to get null count", K(ret), K(ctx), K(row_ids), K(row_cap), K(null_count));
  } else if (ctx.base_ctx_.has_no_nop()) {
  } else if (!ctx.base_ctx_.has_nop_bitmap()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("should have nop bitmap", K(ret), K(ctx));
  } else {
    // has nop bitmap
    for (int64_t i = 0; i < row_cap; ++i) {
      if (ObCSDecodingUtil::test_bit(ctx.base_ctx_.nop_bitmap_, row_ids[i])) {
        --null_count;
      }
    }
  }
  return ret;
}

int ObNoneExistColumnCSDecoder::decode_vector(
    const ObColumnCSDecoderCtx &ctx,
    ObVectorDecodeCtx &vector_ctx) const
{
  UNUSED(ctx);
  int ret = OB_SUCCESS;
  if (OB_FAIL(vector_ctx.fill_from_default_datum())) {
    LOG_WARN("Failed to fill vector from default datum", K(ret), K(vector_ctx));
  } else {
    LOG_DEBUG("[NONE_EXIST_COLUMN_DECODE] decode vector", KPC(vector_ctx.default_datum_), K(lbt()));
  }
  return ret;
}

} // end of namespace blocksstable
} // end of namespace oceanbase
