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

#include "ob_semistruct_column_decoder.h"
#include "storage/blocksstable/cs_encoding/semistruct_encoding/ob_semistruct_encoding_util.h"

namespace oceanbase
{
namespace blocksstable
{

int ObSemiStructColumnDecoder::decode(
  const ObColumnCSDecoderCtx &ctx, const int32_t row_id, common::ObDatum &datum) const
{
  int ret = OB_SUCCESS;
  const ObSemiStructColumnDecoderCtx &semistruct_ctx = ctx.semistruct_ctx_;
  ObSemiStructDecodeHandler *handler = semistruct_ctx.handler_;
  int64_t sub_col_cnt = semistruct_ctx.semistruct_header_->column_cnt_;
  ObDatumRow& sub_row = handler->get_sub_row();
  ObString result;
  bool need_check_null = false;
  sub_row.reuse();
  if (OB_UNLIKELY(ObBaseColumnDecoderCtx::ObNullFlag::IS_NULL_REPLACED == semistruct_ctx.null_flag_
      || ObBaseColumnDecoderCtx::ObNullFlag::IS_NULL_REPLACED_REF == semistruct_ctx.null_flag_)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not support null encode", K(ret), K(semistruct_ctx.null_flag_));
  } else if (ObBaseColumnDecoderCtx::ObNullFlag::HAS_NULL_BITMAP == semistruct_ctx.null_flag_) {
    need_check_null = true;
  }

  if (OB_FAIL(ret)) {
  } else if (need_check_null && ObCSDecodingUtil::test_bit(semistruct_ctx.null_bitmap_, row_id)) {
    datum.set_null();
  } else {
    for (int i = 0; OB_SUCC(ret) && i < sub_col_cnt; ++i) {
      const ObCSColumnHeader &sub_col_header = semistruct_ctx.sub_col_headers_[i];
      ObColumnCSDecoderCtx &sub_col_ctx =  semistruct_ctx.sub_col_ctxs_[i];
      const ObIColumnCSDecoder *decoder = semistruct_ctx.sub_col_decoders_[i];
      ObStorageDatum &sub_datum = sub_row.storage_datums_[i];
      if (OB_ISNULL(decoder)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("sub column decoder is null", K(ret), K(row_id), K(i), K(sub_col_header));
      } else if (OB_FAIL(decoder->decode(sub_col_ctx, row_id, sub_datum))) {
        LOG_WARN("decode sub column fail", K(ret), K(row_id), K(i), K(sub_col_header));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(handler->serialize(sub_row, result))) {
      LOG_WARN("reassemble fail", K(ret), K(sub_row));
    } else {
      datum.set_string(result);
    }
  }
  return ret;
}

int ObSemiStructColumnDecoder::batch_decode(const ObColumnCSDecoderCtx &ctx,
    const int32_t *row_ids, const int64_t row_cap, common::ObDatum *datums) const
{
  int ret = OB_SUCCESS;
  const ObSemiStructColumnDecoderCtx &semistruct_ctx = ctx.semistruct_ctx_;
  for (int32_t i = 0; OB_SUCC(ret) && i < row_cap; ++i) {
    int32_t row_id = row_ids[i];
    if (OB_FAIL(decode(ctx, row_id, datums[i]))) {
      LOG_WARN("decode fail", K(ret), K(i), K(row_id), K(row_cap));
    }
  }
  return ret;
}

int ObSemiStructColumnDecoder::decode_vector(
    const ObColumnCSDecoderCtx &ctx, ObVectorDecodeCtx &vector_ctx) const
{
  int ret = OB_SUCCESS;
  const ObSemiStructColumnDecoderCtx &semistruct_ctx = ctx.semistruct_ctx_;
  VectorFormat vec_format = vector_ctx.vec_header_.get_format();
  ObIVector *vector = vector_ctx.vec_header_.get_vector();

  if (OB_SUCC(ret)) {
    switch (vec_format) {
      case VEC_FIXED: {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("not support vector format for semistruct", K(ret), K(vec_format));
        break;
      }
      case VEC_DISCRETE: {
        ObDiscreteFormat *disc_vec = static_cast<ObDiscreteFormat *>(vector);
        for (int64_t i = 0; OB_SUCC(ret) && i < vector_ctx.row_cap_; ++i) {
          const int32_t row_id = vector_ctx.row_ids_[i];
          const int64_t curr_vec_offset = vector_ctx.vec_offset_ + i;
          ObDatum datum;
          if (OB_FAIL(decode(ctx, row_id, datum))) {
            LOG_WARN("decode fail", K(ret), K(i), K(row_id), K(vector_ctx));
          } else if (datum.is_null()) {
            disc_vec->set_null(curr_vec_offset);
          } else {
            disc_vec->set_string(curr_vec_offset, datum.get_string());
          }
        }
        break;
      }
      case VEC_CONTINUOUS: {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("not support vector format for semistruct", K(ret), K(vec_format));
        break;
      }
      case VEC_UNIFORM: {
        ObUniformFormat<false> *uni_vec = static_cast<ObUniformFormat<false> *>(vector);
        for (int64_t i = 0; OB_SUCC(ret) && i < vector_ctx.row_cap_; ++i) {
          const int32_t row_id = vector_ctx.row_ids_[i];
          const int64_t curr_vec_offset = vector_ctx.vec_offset_ + i;
          ObDatum &datum = uni_vec->get_datum(curr_vec_offset);
          if (OB_FAIL(decode(ctx, row_id, datum))) {
            LOG_WARN("decode fail", K(ret), K(i), K(row_id), K(vector_ctx));
          } else if (datum.is_null()) {
            uni_vec->set_null(curr_vec_offset);
          }
        }
        break;
      }
      default:
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected vector format", K(ret), K(vec_format));
    }
  }
  return ret;
}

int ObSemiStructColumnDecoder::get_null_count(const ObColumnCSDecoderCtx &col_ctx,
    const int32_t *row_ids, const int64_t row_cap, int64_t &null_count) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(row_ids) || row_cap < 1) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(row_cap));
  } else {
    const ObSemiStructColumnDecoderCtx &semistruct_ctx = col_ctx.semistruct_ctx_;
    null_count = 0;
    if (semistruct_ctx.has_null_bitmap()) {
      for (int64_t i = 0; i < row_cap; ++i) {
        if (ObCSDecodingUtil::test_bit(semistruct_ctx.null_bitmap_, row_ids[i])) {
          ++null_count;
        }
      }
    } else if (semistruct_ctx.is_null_replaced()) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("semistruct ctx can not support null replaced encode", K(ret), K(semistruct_ctx));
    } else {
      null_count = 0;
    }
  }
  return ret;
}

int ObSemiStructColumnDecoder::pushdown_operator(
    const sql::ObPushdownFilterExecutor *parent,
    const ObColumnCSDecoderCtx &col_ctx,
    const sql::ObWhiteFilterExecutor &filter,
    const sql::PushdownFilterInfo &pd_filter_info,
    ObBitmap &result_bitmap) const
{
  int ret = OB_SUCCESS;
  const int64_t row_cnt = pd_filter_info.count_;
  const sql::ObPushdownWhiteFilterNode& filter_node = filter.get_filter_node();
  if (OB_UNLIKELY(row_cnt < 1 || row_cnt != result_bitmap.size())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(row_cnt), K(result_bitmap.size()), K(col_ctx));
  } else if (OB_UNLIKELY(! filter_node.is_semistruct_filter_node())) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("filter is not semistruct_filter_node, not support", K(ret), K(filter_node), K(col_ctx));
  } else {
    const sql::ObSemiStructWhiteFilterNode &semistruct_node = static_cast<const sql::ObSemiStructWhiteFilterNode &>(filter_node);
    const ObSemiStructColumnDecoderCtx &semistruct_ctx = col_ctx.semistruct_ctx_;
    ObSemiStructDecodeHandler *handler = semistruct_ctx.handler_;
    int64_t sub_col_cnt = semistruct_ctx.semistruct_header_->column_cnt_;
    bool can_pushdown = false;
    int64_t sub_col_idx = -1;
    if (OB_FAIL(handler->check_can_pushdown(semistruct_node, can_pushdown, sub_col_idx))) {
      LOG_WARN("check_can_pushdown fail", K(ret), K(semistruct_node), KPC(handler));
    } else if (OB_UNLIKELY(! can_pushdown)) {
      ret = OB_NOT_SUPPORTED;
      LOG_INFO("pushdown not support for current filter", K(semistruct_node), KPC(handler));
    } else if (OB_UNLIKELY(sub_col_idx < 0 || sub_col_idx >= sub_col_cnt)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid sub_col_idx", K(ret), K(sub_col_idx), K(semistruct_node), KPC(handler));
    } else {
      const ObIColumnCSDecoder *sub_decoder = semistruct_ctx.sub_col_decoders_[sub_col_idx];
      ObColumnCSDecoderCtx &sub_col_ctx =  semistruct_ctx.sub_col_ctxs_[sub_col_idx];
      if (OB_FAIL(sub_decoder->pushdown_operator(parent, sub_col_ctx, filter, pd_filter_info, result_bitmap))) {
        LOG_WARN("pushdown filter to sub column fail", K(ret), K(sub_col_ctx), K(semistruct_node), KPC(handler));
      }
    }
  }
  return ret;
}

}
}

