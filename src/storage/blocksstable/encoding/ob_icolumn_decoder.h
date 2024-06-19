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
#include "src/share/vector/ob_uniform_vector.h"
#include "src/share/vector/ob_continuous_vector.h"
#include "src/share/vector/ob_discrete_vector.h"
#include "src/share/vector/ob_fixed_length_vector.h"

namespace oceanbase
{
namespace storage
{
class ObGroupByCell;
}
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

struct ObVectorDecodeCtx
{
  explicit ObVectorDecodeCtx(
      const char **ptr_arr,
      uint32_t *len_arr,
      const int32_t *row_ids,
      const int64_t row_cap,
      const int64_t vec_offset,
      sql::VectorHeader &vec_header)
    : ptr_arr_(ptr_arr), len_arr_(len_arr), row_ids_(row_ids), row_cap_(row_cap),
      vec_offset_(vec_offset), vec_header_(vec_header) {}

  bool is_valid() const
  {
    return nullptr != ptr_arr_ && nullptr != len_arr_ && row_cap_ > 0 && vec_offset_ >= 0;
  }

  void reset_tmp_arr()
  {
    if (nullptr != ptr_arr_) {
      MEMSET(ptr_arr_, 0, sizeof(const char *) * row_cap_);
    }
    if (nullptr != len_arr_) {
      MEMSET(len_arr_, 0, sizeof(uint32_t) * row_cap_);
    }
  }

  VectorFormat get_format() const { return vec_header_.get_format(); }
  ObIVector *get_vector() { return vec_header_.get_vector(); }

  TO_STRING_KV(KP_(ptr_arr), KP_(len_arr), KP_(row_ids), K_(row_cap), K_(vec_offset));

  const char **ptr_arr_; // tmp mem buf as pointer array
  uint32_t *len_arr_; // tmp mem buf as 4-byte array
  const int32_t *row_ids_; // projection row-ids
  const int64_t row_cap_; // batch size / array size
  const int64_t vec_offset_; // vector start projection offset
  sql::VectorHeader &vec_header_; // result
};

class ObIColumnDecoder
{
public:
  static const uint64_t BITS_PER_BLOCK = 64;
public:
  ObIColumnDecoder() {}
  virtual ~ObIColumnDecoder() {}
  VIRTUAL_TO_STRING_KV(K(this));

  virtual int decode(const ObColumnDecoderCtx &ctx, common::ObDatum &datum, const int64_t row_id,
     const ObBitStream &bs, const char *data, const int64_t len) const = 0;

  virtual int decode(const ObColumnDecoderCtx &ctx, const ObBitStream &bs, const char *data, const int64_t len,
                     sql::VectorHeader &vec_header, const int64_t vec_idx) const
  {
    UNUSEDx(ctx, bs, data, len, vec_header, vec_idx);
    return OB_NOT_SUPPORTED;
  }

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
      const int32_t *row_ids,
      const char **cell_datas,
      const int64_t row_cap,
      common::ObDatum *datums) const
  {
    UNUSEDx(ctx, row_index, row_ids, cell_datas, row_cap, datums);
    return common::OB_NOT_SUPPORTED;
  }

  /*
   * row_ids: index array of projected rows, null means all rows from idx_offset needed to be projected
   * row_cap: count of projected rows
   * vec_offset: start projection offset of vector header
   */
  virtual int decode_vector(
      const ObColumnDecoderCtx &decoder_ctx,
      const ObIRowIndex *row_index,
      ObVectorDecodeCtx &vector_ctx) const
  {
    UNUSEDx(decoder_ctx, row_index, vector_ctx);
    return common::OB_NOT_SUPPORTED;
  }

  virtual int pushdown_operator(
      const sql::ObPushdownFilterExecutor *parent,
      const ObColumnDecoderCtx &col_ctx,
      const sql::ObWhiteFilterExecutor &filter,
      const char* meta_data,
      const ObIRowIndex* row_index,
      const sql::PushdownFilterInfo &pd_filter_info,
      ObBitmap &result_bitmap) const
  {
    UNUSEDx(parent, col_ctx, filter, meta_data, row_index, pd_filter_info, result_bitmap);
    return common::OB_NOT_SUPPORTED;
  }

  virtual int pushdown_operator(
      const sql::ObPushdownFilterExecutor *parent,
      const ObColumnDecoderCtx &col_ctx,
      sql::ObBlackFilterExecutor &filter,
      const char* meta_data,
      const ObIRowIndex* row_index,
      sql::PushdownFilterInfo &pd_filter_info,
      ObBitmap &result_bitmap,
      bool &filter_applied) const
  {
    UNUSEDx(parent, col_ctx, filter, meta_data, row_index, pd_filter_info, result_bitmap, filter_applied);
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
      const int32_t *row_ids,
      const int64_t row_cap,
      const char **row_datas,
      common::ObDatum *datums) const;

  virtual int get_is_null_bitmap_from_fixed_column(
      const ObColumnDecoderCtx &col_ctx,
      const unsigned char* col_data,
      const sql::PushdownFilterInfo &pd_filter_info,
      ObBitmap &result_bitmap) const;

  virtual int get_is_null_bitmap_from_var_column(
      const ObColumnDecoderCtx &col_ctx,
      const ObIRowIndex* row_index,
      const sql::PushdownFilterInfo &pd_filter_info,
      ObBitmap &result_bitmap) const;

  virtual int set_null_datums_from_fixed_column(
      const ObColumnDecoderCtx &ctx,
      const int32_t *row_ids,
      const int64_t row_cap,
      const unsigned char *col_data,
      common::ObDatum *datums) const;

  virtual int set_null_datums_from_var_column(
      const ObColumnDecoderCtx &ctx,
      const ObIRowIndex* row_index,
      const int32_t *row_ids,
      const int64_t row_cap,
      common::ObDatum *datums) const;

  virtual int set_null_vector_from_fixed_column(
      const ObColumnDecoderCtx &ctx,
      const int32_t *row_ids,
      const int64_t row_cap,
      const int64_t vec_offset,
      const unsigned char *col_data,
      ObIVector &vector) const;

  int batch_locate_var_len_row(
      const ObColumnDecoderCtx &ctx,
      const ObIRowIndex* row_index,
      ObVectorDecodeCtx &vector_ctx,
      bool &has_null) const;

  virtual int get_null_count(
      const ObColumnDecoderCtx &ctx,
      const ObIRowIndex *row_index,
      const int32_t *row_ids,
      const int64_t row_cap,
      int64_t &null_count) const;

  virtual int get_null_count_from_fixed_column(
      const ObColumnDecoderCtx &ctx,
      const int32_t *row_ids,
      const int64_t row_cap,
      const unsigned char *col_data,
      int64_t &null_count) const;

  virtual int get_null_count_from_var_column(
      const ObColumnDecoderCtx &ctx,
      const ObIRowIndex* row_index,
      const int32_t *row_ids,
      const int64_t row_cap,
      int64_t &null_count) const;

  virtual bool fast_decode_valid(const ObColumnDecoderCtx &ctx) const
  {
    UNUSED(ctx);
    return false;
  }

  virtual int get_distinct_count(int64_t &distinct_count) const
  { UNUSED(distinct_count); return OB_NOT_SUPPORTED; }

  virtual int read_distinct(
      const ObColumnDecoderCtx &ctx,
      const char **cell_datas,
      storage::ObGroupByCell &group_by_cell)  const
  { return OB_NOT_SUPPORTED; }

  virtual int read_reference(
      const ObColumnDecoderCtx &ctx,
      const int32_t *row_ids,
      const int64_t row_cap,
      storage::ObGroupByCell &group_by_cell) const
  { return OB_NOT_SUPPORTED; }

protected:
  int get_null_count_from_extend_value(
    const ObColumnDecoderCtx &ctx,
    const ObIRowIndex *row_index,
    const int32_t *row_ids,
    const int64_t row_cap,
    const char *meta_data_,
    int64_t &null_count) const;


  template <typename Header, bool HAS_NULL>
  static int batch_locate_cell_data(
      const ObColumnDecoderCtx &ctx,
      const Header &header,
      const char **data_arr,
      uint32_t *len_arr,
      const int32_t *row_ids,
      const int64_t row_cap);

  template <typename T>
  inline void update_pointer(T *&ptr, const char *old_block, const char *cur_block)
  {
    ptr = reinterpret_cast<T *>(cur_block + (reinterpret_cast<const char *>(ptr) - old_block));
  }
};

class ObSpanColumnDecoder : public ObIColumnDecoder
{
protected:
  int decode_exception_vector(
      const ObColumnDecoderCtx &decoder_ctx,
      const int64_t ref,
      const char *exc_buf,
      const int64_t exc_buf_len,
      const int64_t vec_offset,
      sql::VectorHeader &vec_header) const;

  template <typename ValueType, ObEncodingDecodeMetodType DECODE_TYPE>
  int inner_decode_exception_vector(
      const ObColumnDecoderCtx &decoder_ctx,
      const int64_t ref,
      const char *exc_buf,
      const int64_t exc_buf_len,
      const int64_t vec_offset,
      sql::VectorHeader &vec_header) const;

  int decode_refed_range(
      const ObColumnDecoderCtx &decoder_ctx,
      const ObIRowIndex *row_index,
      const int64_t ref_start_idx,
      const int64_t ref_end_idx,
      ObVectorDecodeCtx &raw_vector_ctx) const;
};

// decoder for column not exist in schema
class ObNoneExistColumnDecoder : public ObIColumnDecoder
{
public:
  static const ObColumnHeader::Type type_ = ObColumnHeader::MAX_TYPE;

  virtual int decode(const ObColumnDecoderCtx &ctx, common::ObDatum &datum, const int64_t row_id,
      const ObBitStream &bs, const char *data, const int64_t len)const override
  {
    datum.set_ext();
    datum.no_cv(datum.extend_obj_)->set_ext(common::ObActionFlag::OP_NOP);
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
    const int32_t *row_ids,
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

template <typename Header, bool HAS_NULL>
int ObIColumnDecoder::batch_locate_cell_data(
    const ObColumnDecoderCtx &ctx,
    const Header &header,
    const char **data_arr,
    uint32_t *len_arr,
    const int32_t *row_ids,
    const int64_t row_cap)
{
  // for var-length data, nullptr == data_arr[row_id] represent for this row is null
  int ret = common::OB_SUCCESS;
  if (OB_ISNULL(data_arr) || OB_ISNULL(len_arr) || OB_ISNULL(row_ids)) {
    ret = common::OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument", K(ret), KP(row_ids), KP(data_arr), KP(len_arr));
  } else if (ctx.is_fix_length()) {
    for (int64_t i = 0; i < row_cap; ++i) {
      data_arr[i] += header.offset_;
      len_arr[i] = header.length_;
    }
  } else if (1 == ctx.micro_block_header_->var_column_count_) {
    for (int64_t i = 0; i < row_cap; ++i) {
      if (!HAS_NULL || nullptr != data_arr[i]) {
        data_arr[i] += header.offset_;
        len_arr[i] -= header.offset_;
      }
    }
  } else {
    ObIntegerArrayGenerator gen;
    if (ctx.col_header_->is_last_var_field()) {
      for (int64_t i = 0; OB_SUCC(ret) && i < row_cap; ++i) {
        if (!HAS_NULL || nullptr != data_arr[i]) {
          const uint8_t col_idx_byte = *(data_arr[i] + header.offset_);
          const char *var_data = data_arr[i] + header.offset_ + sizeof(uint8_t);
          if (OB_FAIL(gen.init(var_data - col_idx_byte, col_idx_byte))) {
            STORAGE_LOG(WARN, "init integer array generator failed", K(ret), K(header));
          } else {
            var_data += (ctx.micro_block_header_->var_column_count_ - 1) * col_idx_byte;
            const int64_t offset = 0 == header.length_ ? 0 : gen.get_array().at(header.length_);
            const int64_t datum_offset_in_row = offset + (var_data - data_arr[i]);
            // datum_offset_in_row is ensured to be included in range of int32
            len_arr[i] = len_arr[i] - static_cast<const int32_t>(datum_offset_in_row);
            data_arr[i] = var_data + offset;
          }
        }
      }
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < row_cap; ++i) {
        if (!HAS_NULL || nullptr != data_arr[i]) {
          const int8_t col_idx_byte = *(data_arr[i] + header.offset_);
          const char *var_data = data_arr[i] + header.offset_ + sizeof(uint8_t);
          if (OB_FAIL(gen.init(var_data - col_idx_byte, col_idx_byte))) {
            STORAGE_LOG(WARN, "init integer array generator failed", K(ret), K(header));
          } else {
            var_data += (ctx.micro_block_header_->var_column_count_ - 1) * col_idx_byte;
            // 0 if header.length_ == 0
            const int64_t offset = 0 == header.length_ ? 0 : gen.get_array().at(header.length_);
            len_arr[i] = gen.get_array().at(header.length_ + 1) - offset;
            data_arr[i] = var_data + offset;
          }
        }
      }
    }
  }
  return ret;
}

} // end namespace blocksstable
} // end namespace oceanbase

#endif // OCEANBASE_ENCODING_OB_ICOLUMN_DECODER_H_
