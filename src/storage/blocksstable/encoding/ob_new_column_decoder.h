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

#ifndef OCEANBASE_ENCODING_OB_NEW_COLUMN_DECODER_H_
#define OCEANBASE_ENCODING_OB_NEW_COLUMN_DECODER_H_

#include "storage/blocksstable/encoding/ob_icolumn_decoder.h"

namespace oceanbase
{
namespace storage
{
class ObGroupByCellBase;
}
namespace blocksstable
{

class ObBitStream;
class ObIRowIndex;

class ObNewColumnCommonDecoder final
{
public:
  int decode(const ObColumnParam *col_param, common::ObIAllocator *allocator, common::ObDatum &datum) const;

  int batch_decode(
      const ObColumnParam *col_param,
      common::ObIAllocator *allocator,
      const int64_t row_cap,
      common::ObDatum *datums) const;

  int decode_vector(ObVectorDecodeCtx &vector_ctx) const;

  int pushdown_operator(
      const sql::ObWhiteFilterExecutor &filter,
      ObBitmap &result_bitmap) const;

  int pushdown_operator(
      sql::ObBlackFilterExecutor &filter,
      sql::PushdownFilterInfo &pd_filter_info,
      ObBitmap &result_bitmap) const;

  int get_null_count(
      const ObColumnParam *col_param,
      const int64_t row_cap,
      int64_t &null_count) const;

  int read_distinct(
      const ObColumnParam *col_param,
      storage::ObGroupByCellBase &group_by_cell) const;

  int read_reference(
      const int64_t row_cap,
      storage::ObGroupByCellBase &group_by_cell) const;

  int get_distinct_count(int64_t &distinct_count) const { distinct_count = 1; return OB_SUCCESS; }
  bool is_new_column() const { return true; }
};

// decoder for column not exist in schema
class ObNewColumnDecoder : public ObIColumnDecoder
{
public:
  static const ObColumnHeader::Type type_ = ObColumnHeader::MAX_TYPE;

  virtual ObColumnHeader::Type get_type() const override { return type_; }

  virtual int update_pointer(const char *, const char *) override { return common::OB_SUCCESS; }

  virtual bool can_vectorized() const override { return true; }

  virtual int decode(const ObColumnDecoderCtx &ctx, common::ObDatum &datum, const int64_t row_id,
      const ObBitStream &bs, const char *data, const int64_t len) const override
  {
    UNUSEDx(row_id, bs, data, len);
    return common_decoder_.decode(ctx.col_param_, ctx.allocator_, datum);
  }

  virtual int batch_decode(
      const ObColumnDecoderCtx &ctx,
      const ObIRowIndex* row_index,
      const int32_t *row_ids,
      const char **cell_datas,
      const int64_t row_cap,
      common::ObDatum *datums) const override
  {
    UNUSEDx(row_index, row_ids, cell_datas);
    return common_decoder_.batch_decode(ctx.col_param_, ctx.allocator_, row_cap, datums);
  }

  virtual int decode_vector(
      const ObColumnDecoderCtx &decoder_ctx,
      const ObIRowIndex *row_index,
      ObVectorDecodeCtx &vector_ctx) const override
  {
    UNUSEDx(decoder_ctx, row_index, vector_ctx);
    return common_decoder_.decode_vector(vector_ctx);
  }

  virtual int pushdown_operator(
      const sql::ObPushdownFilterExecutor *parent,
      const ObColumnDecoderCtx &col_ctx,
      const sql::ObWhiteFilterExecutor &filter,
      const char* meta_data,
      const ObIRowIndex* row_index,
      const sql::PushdownFilterInfo &pd_filter_info,
      ObBitmap &result_bitmap) const override
  {
    UNUSEDx(parent, col_ctx, meta_data, row_index, pd_filter_info);
    return common_decoder_.pushdown_operator(filter, result_bitmap);
  }

  virtual int pushdown_operator(
      const sql::ObPushdownFilterExecutor *parent,
      const ObColumnDecoderCtx &col_ctx,
      sql::ObBlackFilterExecutor &filter,
      const char* meta_data,
      const ObIRowIndex* row_index,
      sql::PushdownFilterInfo &pd_filter_info,
      ObBitmap &result_bitmap,
      bool &filter_applied) const override
  {
    UNUSEDx(parent, col_ctx, meta_data, row_index);
    filter_applied = true;
    return common_decoder_.pushdown_operator(filter, pd_filter_info, result_bitmap);
  }

  virtual int get_null_count(
      const ObColumnDecoderCtx &ctx,
      const ObIRowIndex *row_index,
      const int32_t *row_ids,
      const int64_t row_cap,
      int64_t &null_count) const override
  {
    UNUSEDx(row_index, row_ids);
    return common_decoder_.get_null_count(ctx.col_param_, row_cap, null_count);
  }

  virtual int read_distinct(
      const ObColumnDecoderCtx &ctx,
      const char **cell_datas,
      storage::ObGroupByCellBase &group_by_cell)  const override
  {
    return common_decoder_.read_distinct(ctx.col_param_, group_by_cell);
  }

  virtual int read_reference(
      const ObColumnDecoderCtx &ctx,
      const int32_t *row_ids,
      const int64_t row_cap,
      storage::ObGroupByCellBase &group_by_cell) const override
  {
    UNUSEDx(ctx, row_ids);
    return common_decoder_.read_reference(row_cap, group_by_cell);
  }

  virtual int get_distinct_count(int64_t &distinct_count) const override { return common_decoder_.get_distinct_count(distinct_count); }

  virtual bool is_new_column() const override { return common_decoder_.is_new_column(); }
private:
  ObNewColumnCommonDecoder common_decoder_;
};

} // end namespace blocksstable
} // end namespace oceanbase

#endif // OCEANBASE_ENCODING_OB_NEW_COLUMN_DECODER_H_
