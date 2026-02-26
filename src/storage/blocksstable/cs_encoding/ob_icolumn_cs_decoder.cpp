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

} // end of namespace oceanbase
} // end of namespace oceanbase
