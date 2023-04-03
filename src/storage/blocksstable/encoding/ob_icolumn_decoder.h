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

#ifndef OCEANBASE_ENCODING_OB_ICOLUMN_DECODER_H_
#define OCEANBASE_ENCODING_OB_ICOLUMN_DECODER_H_

#include "common/object/ob_object.h"
#include "storage/blocksstable/ob_block_sstable_struct.h"
#include "storage/blocksstable/ob_data_buffer.h"
#include "storage/ob_storage_util.h"
#include "sql/engine/basic/ob_pushdown_filter.h"
#include "lib/container/ob_bitmap.h"
#include "ob_bit_stream.h"
#include "ob_encoding_util.h"
#include "ob_row_index.h"
#include "storage/blocksstable/ob_micro_block_header.h"

namespace oceanbase
{
namespace blocksstable
{

class ObBitStream;
class ObIColumnDecoder;
class ObIRowIndex;

struct ObBaseDecoderCtx
{
public:
  ObBaseDecoderCtx() : obj_meta_(), micro_block_header_(NULL), col_header_(NULL) {}

  common::ObObjMeta obj_meta_;
  const ObMicroBlockHeader *micro_block_header_;
  const ObColumnHeader *col_header_;

};

struct ObColumnDecoderCtx : public ObBaseDecoderCtx
{
public:
  ObColumnDecoderCtx()
      : allocator_(NULL), ref_decoder_(NULL), ref_ctx_(NULL), col_param_(NULL)
  {
  }

  // performance critical, do not check parameters
  OB_INLINE void fill(const common::ObObjMeta &obj_meta,
      const ObMicroBlockHeader *micro_header,
      const ObColumnHeader *col_header,
      common::ObIAllocator *allocator)
  {
    obj_meta_ = obj_meta;
    micro_block_header_ = micro_header;
    col_header_ = col_header;
    allocator_ = allocator;

    cache_attributes_[ObColumnHeader::FIX_LENGTH] = col_header_->is_fix_length();
    cache_attributes_[ObColumnHeader::HAS_EXTEND_VALUE] = col_header_->has_extend_value();
    cache_attributes_[ObColumnHeader::BIT_PACKING] = col_header_->is_bit_packing();
  }
  OB_INLINE bool is_fix_length() const { return cache_attributes_[ObColumnHeader::FIX_LENGTH]; }
  OB_INLINE bool has_extend_value() const { return cache_attributes_[ObColumnHeader::HAS_EXTEND_VALUE]; }
  OB_INLINE bool is_bit_packing() const { return cache_attributes_[ObColumnHeader::BIT_PACKING]; }
  OB_INLINE void set_col_param(const share::schema::ObColumnParam *col_param)
  {
    col_param_ = col_param;
  }

  TO_STRING_KV(K_(obj_meta), K_(micro_block_header), K_(col_header), KP_(allocator),
      KP_(ref_decoder), KP_(ref_ctx));

  common::ObIAllocator *allocator_;
  const ObIColumnDecoder *ref_decoder_;
  ObColumnDecoderCtx *ref_ctx_;
  // Pointer to ColumnParam for padding in filter pushdown
  const share::schema::ObColumnParam *col_param_;
  bool cache_attributes_[ObColumnHeader::MAX_ATTRIBUTE];
};

class ObIColumnDecoder
{
public:
  ObIColumnDecoder() {}
  virtual ~ObIColumnDecoder() {}
  VIRTUAL_TO_STRING_KV(K(this));

  virtual int decode(ObColumnDecoderCtx &ctx, common::ObObj &cell, const int64_t row_id,
      const ObBitStream &bs, const char *data, const int64_t len) const = 0;

  virtual ObColumnHeader::Type get_type() const = 0;

  virtual int update_pointer(const char *old_block, const char *cur_block) = 0;

  virtual int get_ref_col_idx(int64_t &ref_col_idx) const
  {
    ref_col_idx = -1;
    return common::OB_SUCCESS;
  }
  virtual void dump_meta(const ObColumnDecoderCtx &) const {}

  // can_vectorized means decode data into datum column directly
  virtual bool can_vectorized() const { return true; }

  // This API should be implemented according to characteris of batch column data
  // for better utilization of CPU Pipeline/Cache and process data in batch
  // Currently only used in vectorized table scan, NOP values not supported.
  // Performance critical, only check pointer once in caller
  virtual int batch_decode(
      const ObColumnDecoderCtx &ctx,
      const ObIRowIndex* row_index,
      const int64_t *row_ids,
      const char **cell_datas,
      const int64_t row_cap,
      common::ObDatum *datums) const
  {
    UNUSEDx(ctx, row_index, row_ids, cell_datas, row_cap, datums);
    return common::OB_NOT_SUPPORTED;
  }

  virtual int pushdown_operator(
      const sql::ObPushdownFilterExecutor *parent,
      const ObColumnDecoderCtx &col_ctx,
      const sql::ObWhiteFilterExecutor &filter,
      const char* meta_data,
      const ObIRowIndex* row_index,
      ObBitmap &result_bitmap) const
  {
    UNUSEDx(parent, col_ctx, filter, meta_data, row_index, result_bitmap);
    return common::OB_NOT_SUPPORTED;
  }

  OB_INLINE virtual int locate_row_data(
      const ObColumnDecoderCtx &col_ctx,
      const ObIRowIndex* row_index,
      const int64_t row_id,
      const char *&row_data,
      int64_t &row_len) const;

  OB_INLINE virtual int batch_locate_row_data(
      const ObColumnDecoderCtx &col_ctx,
      const ObIRowIndex *row_index,
      const int64_t *row_ids,
      const int64_t row_cap,
      const char **row_datas,
      common::ObDatum *datums) const;

  virtual int get_is_null_bitmap_from_fixed_column(
      const ObColumnDecoderCtx &col_ctx,
      const unsigned char* col_data,
      ObBitmap &result_bitmap) const;

  virtual int get_is_null_bitmap_from_var_column(
      const ObColumnDecoderCtx &col_ctx,
      const ObIRowIndex* row_index,
      ObBitmap &result_bitmap) const;

  virtual int set_null_datums_from_fixed_column(
      const ObColumnDecoderCtx &ctx,
      const int64_t *row_ids,
      const int64_t row_cap,
      const unsigned char *col_data,
      common::ObDatum *datums) const;

  virtual int set_null_datums_from_var_column(
      const ObColumnDecoderCtx &ctx,
      const ObIRowIndex* row_index,
      const int64_t *row_ids,
      const int64_t row_cap,
      common::ObDatum *datums) const;

  virtual int get_null_count(
      const ObColumnDecoderCtx &ctx,
      const ObIRowIndex *row_index,
      const int64_t *row_ids,
      const int64_t row_cap,
      int64_t &null_count) const;

  virtual int get_null_count_from_fixed_column(
      const ObColumnDecoderCtx &ctx,
      const int64_t *row_ids,
      const int64_t row_cap,
      const unsigned char *col_data,
      int64_t &null_count) const;

  virtual int get_null_count_from_var_column(
      const ObColumnDecoderCtx &ctx,
      const ObIRowIndex* row_index,
      const int64_t *row_ids,
      const int64_t row_cap,
      int64_t &null_count) const;

protected:
  int get_null_count_from_extend_value(
    const ObColumnDecoderCtx &ctx,
    const ObIRowIndex *row_index,
    const int64_t *row_ids,
    const int64_t row_cap,
    const char *meta_data_,
    int64_t &null_count) const;

  template <typename T>
  inline void update_pointer(T *&ptr, const char *old_block, const char *cur_block)
  {
    ptr = reinterpret_cast<T *>(cur_block + (reinterpret_cast<const char *>(ptr) - old_block));
  }
};

class ObSpanColumnDecoder : public ObIColumnDecoder
{
public:
};

// decoder for column not exist in schema
class ObNoneExistColumnDecoder : public ObIColumnDecoder
{
public:
  static const ObColumnHeader::Type type_ = ObColumnHeader::MAX_TYPE;
  virtual int decode(ObColumnDecoderCtx &, common::ObObj &cell, const int64_t,
      const ObBitStream &, const char *, const int64_t) const override
  {
    cell.set_nop_value();
    return common::OB_SUCCESS;
  }

  virtual ObColumnHeader::Type get_type() const { return type_; }

  virtual int update_pointer(const char *, const char *) { return common::OB_SUCCESS; }

  virtual bool can_vectorized() const override { return false; }
};

// Read row data offset and row length from row index
OB_INLINE int ObIColumnDecoder::locate_row_data(
    const ObColumnDecoderCtx &col_ctx,
    const ObIRowIndex* row_index,
    const int64_t row_id,
    const char *&row_data,
    int64_t &row_len) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(row_index)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Null pointer of row index", K(ret));
  } else if (OB_FAIL(row_index->get(row_id, row_data, row_len))) {
    STORAGE_LOG(WARN, "Failed to get row data offset from row index", K(ret));
  }
  return ret;
}

// Batch read row data in row_datas, row_len in datums.len_
OB_INLINE int ObIColumnDecoder::batch_locate_row_data(
    const ObColumnDecoderCtx &col_ctx,
    const ObIRowIndex *row_index,
    const int64_t *row_ids,
    const int64_t row_cap,
    const char **row_datas,
    common::ObDatum *datums) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(row_ids) || OB_ISNULL(datums) || OB_ISNULL(row_datas) || OB_ISNULL(row_index)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument", K(ret),
        KP(row_ids), KP(row_datas), KP(datums), KP(row_index));
  } else if (OB_FAIL(row_index->batch_get(
      row_ids, row_cap, col_ctx.has_extend_value(),
      row_datas, datums))) {
    STORAGE_LOG(WARN, "Failed to batch get row data offset from row index", K(ret));
  }
  return ret;
}

} // end namespace blocksstable
} // end namespace oceanbase

#endif // OCEANBASE_ENCODING_OB_ICOLUMN_DECODER_H_
