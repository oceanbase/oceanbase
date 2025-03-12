/**
 * Copyright (c) 2024 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_ENCODING_OB_NEW_COLUMN_CS_DECODER_H_
#define OCEANBASE_ENCODING_OB_NEW_COLUMN_CS_DECODER_H_

#include "storage/blocksstable/cs_encoding/ob_icolumn_cs_decoder.h"
#include "storage/blocksstable/encoding/ob_new_column_decoder.h"

namespace oceanbase
{
namespace storage
{
class ObAggCellBase;
}
namespace blocksstable
{

class ObNewColumnCSDecoder : public ObIColumnCSDecoder
{
public:
  static const ObCSColumnHeader::Type type_ = ObCSColumnHeader::MAX_TYPE;

  virtual ObCSColumnHeader::Type get_type() const override { return type_; }

  virtual bool can_vectorized() const override { return true; }

  virtual int decode(const ObColumnCSDecoderCtx &ctx, const int32_t row_id, common::ObDatum &datum) const override
  {
    UNUSEDx(row_id);
    return common_decoder_.decode(ctx.get_col_param(), ctx.get_allocator(), datum);
  }

  virtual int batch_decode(
      const ObColumnCSDecoderCtx &ctx,
      const int32_t *row_ids,
      const int64_t row_cap,
      common::ObDatum *datums) const override
  {
    UNUSEDx(ctx, row_cap, datums);
    return common_decoder_.batch_decode(ctx.get_col_param(), ctx.get_allocator(), row_cap, datums);
  }

  virtual int decode_vector(
      const ObColumnCSDecoderCtx &ctx,
      ObVectorDecodeCtx &vector_ctx) const override
  {
    UNUSED(ctx);
    return common_decoder_.decode_vector(vector_ctx);
  }

  virtual int pushdown_operator(
      const sql::ObPushdownFilterExecutor *parent,
      const ObColumnCSDecoderCtx &col_ctx,
      const sql::ObWhiteFilterExecutor &filter,
      const sql::PushdownFilterInfo &pd_filter_info,
      ObBitmap &result_bitmap) const override
  {
    UNUSEDx(parent, col_ctx, pd_filter_info);
    return common_decoder_.pushdown_operator(filter, result_bitmap);
  }

  virtual int pushdown_operator(
      const sql::ObPushdownFilterExecutor *parent,
      const ObColumnCSDecoderCtx &col_ctx,
      sql::ObBlackFilterExecutor &filter,
      sql::PushdownFilterInfo &pd_filter_info,
      ObBitmap &result_bitmap,
      bool &filter_applied) const override
  {
    UNUSEDx(parent, col_ctx);
    filter_applied = true;
    return common_decoder_.pushdown_operator(filter, pd_filter_info, result_bitmap);
  }

  virtual int get_aggregate_result(
      const ObColumnCSDecoderCtx &ctx,
      const int32_t *row_ids,
      const int64_t row_cap,
      storage::ObAggCellBase &agg_cell) const override;

  virtual int get_null_count(
      const ObColumnCSDecoderCtx &ctx,
      const int32_t *row_ids,
      const int64_t row_cap,
      int64_t &null_count) const override
  {
    UNUSED(row_ids);
    return common_decoder_.get_null_count(ctx.get_col_param(), row_cap, null_count);
  }

  virtual int read_distinct(
      const ObColumnCSDecoderCtx &ctx,
      storage::ObGroupByCellBase &group_by_cell) const override
  {
    return common_decoder_.read_distinct(ctx.get_col_param(), group_by_cell);
  }

  virtual int read_reference(
      const ObColumnCSDecoderCtx &ctx,
      const int32_t *row_ids,
      const int64_t row_cap,
      storage::ObGroupByCellBase &group_by_cell) const override
  {
    UNUSEDx(ctx, row_ids);
    return common_decoder_.read_reference(row_cap, group_by_cell);
  }

  virtual int get_distinct_count(const ObColumnCSDecoderCtx &ctx, int64_t &distinct_count) const override
  {
    UNUSED(ctx);
    return common_decoder_.get_distinct_count(distinct_count);
  }

  virtual bool is_new_column() const override { return common_decoder_.is_new_column(); }
private:
  ObNewColumnCommonDecoder common_decoder_;
};

} // end namespace blocksstable
} // end namespace oceanbase

#endif // OCEANBASE_ENCODING_OB_NEW_COLUMN_CS_DECODER_H_
