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

#ifndef OCEANBASE_ENCODING_OB_ICOLUMN_CS_DECODER_H_
#define OCEANBASE_ENCODING_OB_ICOLUMN_CS_DECODER_H_

#include "common/object/ob_object.h"
#include "lib/container/ob_bitmap.h"
#include "storage/blocksstable/ob_block_sstable_struct.h"
#include "storage/blocksstable/ob_data_buffer.h"
#include "storage/ob_storage_util.h"
#include "sql/engine/basic/ob_pushdown_filter.h"
#include "storage/blocksstable/ob_micro_block_header.h"
#include "storage/blocksstable/encoding/ob_icolumn_decoder.h"
#include "ob_column_encoding_struct.h"

namespace oceanbase
{
namespace storage
{
class ObAggCell;
class ObGroupByCell;
}
namespace blocksstable
{
class ObIColumnCSDecoder
{
public:
  ObIColumnCSDecoder() {}
  virtual ~ObIColumnCSDecoder() {}
  OB_INLINE void reuse() {}

  VIRTUAL_TO_STRING_KV(K(this));
  virtual int decode(const ObColumnCSDecoderCtx &ctx, const int32_t row_id, common::ObDatum &datum) const = 0;

  virtual ObCSColumnHeader::Type get_type() const = 0;

  // can_vectorized means decode data into datum column directly
  virtual bool can_vectorized() const { return true; }

  // This API should be implemented according to characteris of batch column data
  // for better utilization of CPU Pipeline/Cache and process data in batch
  // Currently only used in vectorized table scan, NOP values not supported.
  // Performance critical, only check pointer once in caller
  virtual int batch_decode(
      const ObColumnCSDecoderCtx &ctx,
      const int32_t *row_ids,
      const int64_t row_cap,
      common::ObDatum *datums) const
  {
    UNUSEDx(ctx, row_ids, row_cap, datums);
    return common::OB_NOT_SUPPORTED;
  }

  virtual int decode_vector(
      const ObColumnCSDecoderCtx &ctx,
      ObVectorDecodeCtx &vector_ctx) const
  {
    UNUSEDx(ctx, vector_ctx);
    return common::OB_NOT_SUPPORTED;
  }

  virtual int pushdown_operator(
      const sql::ObPushdownFilterExecutor *parent,
      const ObColumnCSDecoderCtx &col_ctx,
      const sql::ObWhiteFilterExecutor &filter,
      const sql::PushdownFilterInfo &pd_filter_info,
      ObBitmap &result_bitmap) const
  {
    UNUSEDx(parent, col_ctx, filter, pd_filter_info, result_bitmap);
    return common::OB_NOT_SUPPORTED;
  }
  virtual int pushdown_operator(
      const sql::ObPushdownFilterExecutor *parent,
      const ObColumnCSDecoderCtx &col_ctx,
      sql::ObBlackFilterExecutor &filter,
      sql::PushdownFilterInfo &pd_filter_info,
      ObBitmap &result_bitmap,
      bool &filter_applied) const
  {
    UNUSEDx(parent, col_ctx, filter, pd_filter_info, result_bitmap, filter_applied);
    return common::OB_NOT_SUPPORTED;
  }

  virtual int get_aggregate_result(
      const ObColumnCSDecoderCtx &ctx,
      const int32_t *row_ids,
      const int64_t row_cap,
      storage::ObAggCell &agg_cell) const
  {
    UNUSEDx(ctx, row_ids, row_cap, agg_cell);
    return common::OB_NOT_SUPPORTED;
  }

  virtual int get_null_count(
      const ObColumnCSDecoderCtx &ctx,
      const int32_t *row_ids,
      const int64_t row_cap,
      int64_t &null_count) const;

  virtual int get_distinct_count(const ObColumnCSDecoderCtx &ctx, int64_t &distinct_count) const
  {
    UNUSEDx(ctx, distinct_count);
    return OB_NOT_SUPPORTED;
  }

  virtual int read_distinct(
      const ObColumnCSDecoderCtx &ctx,
      storage::ObGroupByCell &group_by_cell)  const
  {
    UNUSEDx(ctx, group_by_cell);
    return OB_NOT_SUPPORTED;
  }

  virtual int read_reference(
      const ObColumnCSDecoderCtx &ctx,
      const int32_t *row_ids,
      const int64_t row_cap,
      storage::ObGroupByCell &group_by_cell) const
  {
    UNUSEDx(ctx, row_ids, row_cap, group_by_cell);
    return OB_NOT_SUPPORTED;
  }
};

class ObNoneExistColumnCSDecoder : public ObIColumnCSDecoder
{
public:
  static const ObCSColumnHeader::Type type_ = ObCSColumnHeader::MAX_TYPE;

  virtual int decode(const ObColumnCSDecoderCtx &ctx, const int32_t row_id, common::ObDatum &datum) const override
  {
    datum.set_ext();
    datum.no_cv(datum.extend_obj_)->set_ext(common::ObActionFlag::OP_NOP);
    return common::OB_SUCCESS;
  }
  virtual ObCSColumnHeader::Type get_type() const { return type_; }
  virtual bool can_vectorized() const override { return false; }
};


} // end namespace blocksstable
} // end namespace oceanbase

#endif
