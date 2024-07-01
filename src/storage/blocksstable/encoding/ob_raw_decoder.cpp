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

#include "ob_raw_decoder.h"

#include "storage/blocksstable/ob_block_sstable_struct.h"
#include "ob_bit_stream.h"
#include "ob_integer_array.h"
#include "ob_row_index.h"
#include "ob_vector_decode_util.h"
#include "common/ob_target_specific.h"
#include "lib/hash/ob_hashset.h"
#include "sql/engine/expr/ob_expr_cmp_func.h"

namespace oceanbase
{
namespace blocksstable
{

class ObIRowIndex;
using namespace common;
const ObColumnHeader::Type ObRawDecoder::type_;

// define fast batch decode function family
template <typename T, int32_t IS_ZERO_HEADER_LEN, int32_t IS_LAST_VAR_COL>
OB_INLINE static void get_var_col_offset(
    const T *col_idxes, int64_t header_var_col_cnt, const char *&var_data,
    const int64_t header_len, const int64_t row_len, const char *row_data,
    const char *&col_data, int64_t &col_len)
{
  int64_t col_off = 0;
  var_data += sizeof(T) * (header_var_col_cnt - 1);
  if (!IS_ZERO_HEADER_LEN) {
    col_off = col_idxes[header_len - 1];
  }
  if (IS_LAST_VAR_COL) {
    col_len = row_len - col_off - (var_data - row_data);
  } else {
    col_len = col_idxes[header_len] - col_off;
  }
  col_data = var_data + col_off;
}

// ROW_IDX_BYTE : 0 - null
// STORE_CLASS: {0: ObNumberSC, 1: ObStringSC / WideIntSC, 2: ObIntSC/ObUIntSC}
template <int32_t ROW_IDX_BYTE, bool IS_ONLY_VAR_COL,
    bool IS_ZERO_HEADER_LEN, bool IS_LAST_VAR_COL, int32_t STORE_CLASS>
struct RawVarBatchDecodeFunc_T
{
  static void raw_var_batch_decode_func(
      const char *base_data, const char *row_idx_data,
      const int64_t header_off,
      const int64_t header_len,
      const int64_t header_var_col_cnt,
      const int32_t *row_ids, const int64_t row_cap,
      common::ObDatum *datums)
  {
    typedef typename ObEncodingByteLenMap<false, ROW_IDX_BYTE>::Type RowIdxType;

    const RowIdxType *row_idxes = (const RowIdxType *)row_idx_data;
    for (int64_t i = 0; i < row_cap; i++) {
      const int64_t row_id = row_ids[i];
      const int64_t off = row_idxes[row_id];
      const int64_t row_len = row_idxes[row_id + 1] - off;
      const char *row_data = base_data + off;

      const char *col_data = nullptr;
      int64_t col_len = 0;
      if (IS_ONLY_VAR_COL) {
        col_data = row_data + header_off;
        col_len = row_len - header_off;
      } else {
        const char *var_data = row_data + header_off ;
        int8_t col_idx_byte = *var_data;
        var_data += sizeof(int8_t);

        switch (col_idx_byte) {
        case 1: {
          const uint8_t *col_idxes = reinterpret_cast<const uint8_t *>(var_data);
          get_var_col_offset<uint8_t, IS_ZERO_HEADER_LEN, IS_LAST_VAR_COL>(
            col_idxes, header_var_col_cnt, var_data, header_len,
            row_len, row_data, col_data, col_len);
          break;
        }
        case 2: {
          const uint16_t *col_idxes = reinterpret_cast<const uint16_t *>(var_data);
          get_var_col_offset<uint16_t, IS_ZERO_HEADER_LEN, IS_LAST_VAR_COL>(
            col_idxes, header_var_col_cnt, var_data, header_len,
            row_len, row_data, col_data, col_len);
          break;
        }
        case 4: {
          const uint32_t *col_idxes = reinterpret_cast<const uint32_t *>(var_data);
          get_var_col_offset<uint32_t, IS_ZERO_HEADER_LEN, IS_LAST_VAR_COL>(
            col_idxes, header_var_col_cnt, var_data, header_len,
            row_len, row_data, col_data, col_len);
          break;
        }
        case 8: {
          const uint64_t *col_idxes = reinterpret_cast<const uint64_t *>(var_data);
          get_var_col_offset<uint64_t, IS_ZERO_HEADER_LEN, IS_LAST_VAR_COL>(
            col_idxes, header_var_col_cnt, var_data, header_len,
            row_len, row_data, col_data, col_len);
          break;
        }
        default: {
          LOG_ERROR_RET(OB_ERR_UNEXPECTED, "Invalid column index byte", K(col_idx_byte));
        }
        }
      }

      ObDatum &datum = datums[i];
      if (0 == STORE_CLASS) { // number
        ENCODING_ADAPT_MEMCPY(const_cast<char *>(datum.ptr_), col_data, col_len);
      } else {
        datum.ptr_ = col_data;
      }
      datum.pack_ = col_len;
    }
  }
};

// STORE_CLASS: {0: ObNumberSC, 1: ObStringSC / WideIntSC, 2: ObIntSC/ObUIntSC}
template <bool IS_SIGNED_SC, int32_t STORE_LEN_TAG, int32_t DATUM_LEN_TAG, int32_t STORE_CLASS>
struct RawFixBatchDecodeFunc_T
{
  static void raw_fix_batch_decode_func(
      const int64_t col_len,
      const char *base_data,
      const int32_t *row_ids,
      const int64_t row_cap,
      common::ObDatum *datums)
  {
    for (int64_t i = 0; i < row_cap; i++) {
      const int64_t row_id = row_ids[i];
      ObDatum &datum = datums[i];
      if (STORE_CLASS == 1) {
        datum.ptr_ = base_data + col_len * row_id;
      } else if (STORE_CLASS == 0) {
        ENCODING_ADAPT_MEMCPY(const_cast<char *>(datum.ptr_), base_data + row_id * col_len, col_len);
      }
      datum.pack_ = static_cast<int32_t>(col_len);
    }
  }
};

// decoder for UIntSC / IntSC
template <bool IS_SIGNED_SC, int32_t STORE_LEN_TAG, int32_t DATUM_LEN_TAG>
struct RawFixBatchDecodeFunc_T<IS_SIGNED_SC, STORE_LEN_TAG, DATUM_LEN_TAG, 2>
{
  static void raw_fix_batch_decode_func(
      const int64_t col_len,
      const char *base_data,
      const int32_t *row_ids,
      const int64_t row_cap,
      common::ObDatum *datums)
  {
    UNUSED(col_len);
    // TODO: @saitong.zst SIMD on Batch integer decode
    typedef typename ObEncodingTypeInference<IS_SIGNED_SC, STORE_LEN_TAG>::Type StoreType;
    typedef typename ObEncodingTypeInference<IS_SIGNED_SC, DATUM_LEN_TAG>::Type DatumType;
    const StoreType *input = reinterpret_cast<const StoreType *>(base_data);
    for (int64_t i = 0; i < row_cap; i++) {
      const int64_t row_id = row_ids[i];
      *reinterpret_cast<DatumType *>(const_cast<char *>(datums[i].ptr_)) = input[row_id];
      datums[i].pack_ = sizeof(DatumType);
    }
  }
};

// Initialize fast batch decode func family while compiling
static ObMultiDimArray_T<raw_fix_batch_decode_func, 2, 4, 4, 3> raw_fix_batch_decode_funcs;

template <int32_t IS_SIGNED_SC, int32_t STORE_LEN_TAG, int32_t DATUM_LEN_TAG, int32_t STORE_CLASS>
struct RawFixDecoderArrayInit
{
  bool operator()()
  {
    raw_fix_batch_decode_funcs[IS_SIGNED_SC][STORE_LEN_TAG][DATUM_LEN_TAG][STORE_CLASS]
        = &(RawFixBatchDecodeFunc_T<IS_SIGNED_SC, STORE_LEN_TAG, DATUM_LEN_TAG, STORE_CLASS>::raw_fix_batch_decode_func);
    return true;
  }
};

static bool raw_fix_batch_decode_funcs_inited
    = ObNDArrayIniter<RawFixDecoderArrayInit, 2, 4, 4, 3>::apply();


static ObMultiDimArray_T<raw_var_batch_decode_func, 3, 2, 2, 2, 2> var_batch_decode_funcs;

template <int32_t ROW_IDX_BYTE, int32_t IS_ONLY_VAR_COL,
    int32_t IS_ZERO_HEADER_LEN, int32_t IS_LAST_VAR_COL, int32_t STORE_CLASS>
struct RawVarDecoderArrayInit
{
  bool operator()()
  {
    var_batch_decode_funcs[ROW_IDX_BYTE][IS_ONLY_VAR_COL][IS_ZERO_HEADER_LEN][IS_LAST_VAR_COL][STORE_CLASS]
        = &(RawVarBatchDecodeFunc_T<ROW_IDX_BYTE, IS_ONLY_VAR_COL, IS_ZERO_HEADER_LEN,
                                    IS_LAST_VAR_COL, STORE_CLASS>::raw_var_batch_decode_func);
    return true;
  }
};

static bool raw_var_batch_decode_funcs_inited
    = ObNDArrayIniter<RawVarDecoderArrayInit, 3, 2, 2, 2, 2>::apply();

ObMultiDimArray_T<fix_filter_func, 2, 4, 6> raw_fix_fast_filter_funcs;

bool init_raw_fix_simd_filter_funcs();
bool init_raw_fix_neon_simd_filter_funcs();

template <int32_t IS_SIGNED, int32_t LEN_TAG, int32_t CMP_TYPE>
struct RawFixFilterArrayInit
{
  bool operator()()
  {
    raw_fix_fast_filter_funcs[IS_SIGNED][LEN_TAG][CMP_TYPE]
        = &(RawFixFilterFunc_T<IS_SIGNED, LEN_TAG, CMP_TYPE>::fix_filter_func);
    return true;
  }
};

bool init_raw_fix_fast_filter_funcs() {
  bool res = false;
  res = ObNDArrayIniter<RawFixFilterArrayInit, 2, 4, 6>::apply();
  // Dispatch simd version cmp funcs
#if defined ( __x86_64__ )
  if (is_avx512_valid()) {
    res = init_raw_fix_simd_filter_funcs();
  }
#elif defined ( __aarch64__ ) && defined ( __ARM_NEON )
  res = init_raw_fix_neon_simd_filter_funcs();
#endif
  return res;
}

bool raw_fix_fast_filter_funcs_inited = init_raw_fix_fast_filter_funcs();

int ObRawDecoder::decode(const ObColumnDecoderCtx &ctx, common::ObDatum &datum, const int64_t row_id,
    const ObBitStream &bs, const char *data, const int64_t len) const
{
  int ret = OB_SUCCESS;
  uint64_t val = STORED_NOT_EXT;
  int64_t data_offset = 0;

  const unsigned char *col_data = reinterpret_cast<const unsigned char *>(meta_data_);
  if (!is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (NULL == data || len < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(data), K(len));
  } else {
    // read extend value bit
    if (ctx.has_extend_value()) {
      if (ctx.is_fix_length() || ctx.is_bit_packing()) {
        data_offset = ctx.micro_block_header_->row_count_ * ctx.micro_block_header_->extend_value_bit_;
        if (OB_FAIL(ObBitStream::get(col_data, row_id * ctx.micro_block_header_->extend_value_bit_,
            ctx.micro_block_header_->extend_value_bit_, val))) {
          LOG_WARN("get extend value failed", K(ret), K(ctx));
        }
      } else {
        if (OB_FAIL(bs.get(ctx.col_header_->extend_value_index_,
                           ctx.micro_block_header_->extend_value_bit_,
                           val))) {
          LOG_WARN("get extend value failed", K(ret), K(bs), K(ctx));
        }
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (STORED_NOT_EXT != val) {
    set_stored_ext_value(datum, static_cast<ObStoredExtValue>(val));
  } else {
    // read bit packing value
    if (ctx.is_bit_packing()) {
      uint32_t datum_len = 0;
      val = 0;
      if (OB_FAIL(get_uint_data_datum_len(
          ObDatum::get_obj_datum_map_type(ctx.obj_meta_.get_type()),
          datum_len))) {
        LOG_WARN("Failed to get datum len for int data", K(ret));
      } else {
        datum.pack_ = datum_len;
        if (OB_FAIL(ObBitStream::get(col_data, data_offset + row_id * ctx.col_header_->length_,
            ctx.col_header_->length_, val))) {
          LOG_WARN("get bit packing value failed", K(ret), K(ctx));
        } else {
          MEMCPY(const_cast<char *>(datum.ptr_), &val, datum_len);
        }
      }
    } else {
      const char *cell_data = NULL;
      int64_t cell_len = 0;
      // get cell data offset and length
      if (ctx.is_fix_length()) {
        data_offset = (data_offset + CHAR_BIT - 1) / CHAR_BIT;
        cell_data = meta_data_ + data_offset + row_id * ctx.col_header_->length_;
        cell_len = ctx.col_header_->length_;
      } else {
        if (OB_FAIL(locate_cell_data(cell_data, cell_len, data, len,
                *ctx.micro_block_header_, *ctx.col_header_, *ctx.col_header_))) {
          LOG_WARN("locate cell data failed", K(ret), K(len), K(ctx));
        }
      }
      if (OB_FAIL(ret)){
      } else if (OB_FAIL(load_data_to_datum(
          ctx.obj_meta_.get_type(),
          cell_data,
          cell_len,
          integer_mask_,
          datum))) {
        LOG_WARN("Failed to load data to datum", K(ret), K(datum));
      }
    }
  }
  return ret;
}

int ObRawDecoder::update_pointer(const char *old_block, const char *cur_block)
{
  int ret = OB_SUCCESS;
  if (!is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_ISNULL(old_block) || OB_ISNULL(cur_block)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(old_block), KP(cur_block));
  } else {
    ObIColumnDecoder::update_pointer(meta_data_, old_block, cur_block);
  }
  return ret;
}

/**
 * Internal call, not check parameters for performance
 * Potential Optimization:
 *  1. Loop ununrolling and parallelization with SIMD
 *  2. Batch operation in ObBitStream
 */
int ObRawDecoder::batch_decode(
    const ObColumnDecoderCtx &ctx,
    const ObIRowIndex* row_index,
    const int32_t *row_ids,
    const char **cell_datas,
    const int64_t row_cap,
    common::ObDatum *datums) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited())) {
    ret = OB_NOT_INIT;
    LOG_WARN("Raw decoder not inited", K(ret));
  } else if (fast_decode_valid(ctx)) {
    if (OB_FAIL(batch_decode_fast(ctx, row_index, row_ids, row_cap, datums))) {
      LOG_WARN("Failed to decode", K(ret), K(ctx));
    }
  } else if (OB_FAIL(batch_decode_general(ctx, row_index, row_ids, cell_datas, row_cap, datums))) {
    LOG_WARN("Failed to decode ", K(ret), K(ctx));
  }
  return ret;
}

bool ObRawDecoder::fast_decode_valid(const ObColumnDecoderCtx &ctx) const
{
  bool valid = false;
  const ObColumnHeader *col_header = ctx.col_header_;
  const ObMicroBlockHeader *block_header = ctx.micro_block_header_;
  const ObObjTypeStoreClass store_class =
      get_store_class_map()[ob_obj_type_class(ctx.col_header_->get_store_obj_type())];
  if (col_header->is_fix_length()) {
    valid = !col_header->is_bit_packing()
        && !col_header->has_extend_value()
        && (store_class >= ObIntSC && store_class <= ObStringSC)
        && raw_fix_batch_decode_funcs_inited;
    if (valid && (ObIntSC == store_class || ObUIntSC == store_class)) {
      uint32_t store_size = col_header->length_;
      valid = (store_size == 1 || store_size == 2 || store_size == 4 || store_size == 8);
    }
  } else {
    uint8_t row_idx_byte = block_header->row_index_byte_;
    bool row_idx_byte_valid = (1 == row_idx_byte || 2 == row_idx_byte);
    valid = !col_header->is_bit_packing()
        && !col_header->has_extend_value()
        && row_idx_byte_valid
        && (ObNumberSC == store_class || ObDecimalIntSC == store_class || ObStringSC == store_class)
        && raw_var_batch_decode_funcs_inited;
  }
  return valid;
}

int ObRawDecoder::batch_decode_fast(
    const ObColumnDecoderCtx &ctx,
    const ObIRowIndex* row_index,
    const int32_t *row_ids,
    const int64_t row_cap,
    common::ObDatum *datums) const
{
  int ret = OB_SUCCESS;
  // Optimized decode for byte-packing data
  const ObObjType store_type = ctx.col_header_->get_store_obj_type();
  const ObObjTypeStoreClass store_class = get_store_class_map()[ob_obj_type_class(store_type)];
  if (ctx.is_fix_length()) {
    const ObObjDatumMapType map_type = ObDatum::get_obj_datum_map_type(store_type);
    // Only need store_len for UIntSC/IntSC
    uint32_t store_len = ctx.col_header_->length_ > 8 ? 0 : ctx.col_header_->length_;
    raw_fix_batch_decode_func decode_func = raw_fix_batch_decode_funcs
        [ObIntSC == store_class
            && ctx.col_header_->length_ == get_type_size_map()[store_type]]
        [get_value_len_tag_map()[store_len]]
        [get_value_len_tag_map()[get_datum_store_len(map_type)]]
        [get_store_class_tag_map()[store_class]];
    decode_func(ctx.col_header_->length_, meta_data_, row_ids, row_cap, datums);
    LOG_DEBUG("[batch_decode] Run fix raw fast batch decode", K(ret), K(store_class),
        K(store_len), K(map_type), K(ctx));
  } else {
    const ObVarRowIndex *var_row_idx = static_cast<const ObVarRowIndex *>(row_index);
    const char *base_data = var_row_idx->get_data();
    const char *row_idx_data = reinterpret_cast<const ObIntegerArray<uint64_t> *>(
                                    var_row_idx->get_index_data())->get_data();
    raw_var_batch_decode_func decode_func = var_batch_decode_funcs
                  [ctx.micro_block_header_->row_index_byte_]
                  [1 == ctx.micro_block_header_->var_column_count_]
                  [0 == ctx.col_header_->length_]
                  [ctx.col_header_->is_last_var_field()]
                  [get_store_class_tag_map()[store_class]];
    decode_func(
        base_data, row_idx_data, ctx.col_header_->offset_, ctx.col_header_->length_,
        ctx.micro_block_header_->var_column_count_, row_ids, row_cap, datums);
  }
  LOG_DEBUG("[batch_decode] Run var raw fast batch decode", K(ret), K(store_class), K(ctx));
  return ret;
}

// private call, not check parameters
int ObRawDecoder::batch_decode_general(
    const ObColumnDecoderCtx &ctx,
    const ObIRowIndex* row_index,
    const int32_t *row_ids,
    const char **cell_datas,
    const int64_t row_cap,
    common::ObDatum *datums) const
{
  int ret = OB_SUCCESS;
  const unsigned char *col_data = reinterpret_cast<const unsigned char *>(meta_data_);
  int64_t data_offset =0;

  // Set Extend(null) value if any
  if (ctx.has_extend_value()) {
    if (ctx.is_fix_length() || ctx.is_bit_packing()) {
      data_offset = ctx.micro_block_header_->row_count_
          * ctx.micro_block_header_->extend_value_bit_;
      if (OB_FAIL(set_null_datums_from_fixed_column(
          ctx, row_ids, row_cap, col_data, datums))) {
        LOG_WARN("Failed to set null datums from fixed data", K(ret), K(ctx));
      }
    } else if (OB_FAIL(set_null_datums_from_var_column(
        ctx, row_index, row_ids, row_cap, datums))) {
      LOG_WARN("Failed to set null datums from var data", K(ret), K(ctx));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (ctx.is_bit_packing()) {
    int64_t row_id = 0;
    uint32_t datum_len = 0;
    uint64_t value = 0;
    if (OB_FAIL(get_uint_data_datum_len(
        ObDatum::get_obj_datum_map_type(ctx.col_header_->get_store_obj_type()),
        datum_len))) {
      LOG_WARN("Failed to get datum len for int data", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < row_cap; ++i) {
      if (ctx.has_extend_value() && datums[i].is_null()) {
        // Do nothing
      } else {
        row_id = row_ids[i];
        datums[i].pack_ = datum_len;
        value = 0;
        if (OB_FAIL(ObBitStream::get(
            col_data,
            data_offset + row_id * ctx.col_header_->length_,
            ctx.col_header_->length_,
            value))) {
          LOG_WARN("Failed to get bitpacked value from bit stream", K(ret), K(row_id));
        } else {
            MEMCPY(const_cast<char *>(datums[i].ptr_), &value, datum_len);
        }
      }
    }
  } else {
    int64_t row_id = 0;
    if (ctx.is_fix_length()) {
      data_offset = (data_offset + CHAR_BIT - 1) / CHAR_BIT;
      for (int64_t i = 0; i < row_cap; ++i) {
        if (ctx.has_extend_value() && datums[i].is_null()) {
        } else {
          row_id = row_ids[i];
          cell_datas[i] = meta_data_ + data_offset + row_id * ctx.col_header_->length_;
          datums[i].pack_ = static_cast<uint32_t>(ctx.col_header_->length_);
        }
      }
    } else {
      if (OB_FAIL(batch_locate_row_data(ctx, row_index, row_ids, row_cap, cell_datas, datums))) {
        LOG_WARN("Failed to batch locate row data offset from row index",
            K(ret), K(ctx), K(row_cap));
      } else if (OB_FAIL(batch_locate_cell_data(cell_datas, datums, row_cap,
          *ctx.micro_block_header_, *ctx.col_header_, *ctx.col_header_))) {
        LOG_WARN("Failed to batch locate cell data for datum", K(ret), K(ctx), K(row_cap));
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(batch_load_data_to_datum(
        ctx.col_header_->get_store_obj_type(),
        cell_datas,
        row_cap,
        integer_mask_,
        datums))) {
      LOG_WARN("Failed to batch load data to datum", K(ret));
    }
  }
  return ret;
}

int ObRawDecoder::decode_vector(
    const ObColumnDecoderCtx &decoder_ctx,
    const ObIRowIndex* row_index,
    ObVectorDecodeCtx &vector_ctx) const
{
  int ret = OB_SUCCESS;
  const unsigned char *col_data = reinterpret_cast<const unsigned char *>(meta_data_);
  int64_t data_offset =0;

  if (decoder_ctx.is_bit_packing()) {
    // bit packing
    if (OB_FAIL(decode_vector_bitpacked(decoder_ctx, vector_ctx))) {
      LOG_WARN("Failed to decode bitpacked data to vector", K(ret), K(decoder_ctx), K(vector_ctx));
    }
  } else {
    if (decoder_ctx.is_fix_length()) {
      // fixed-length
      const int64_t fixed_packing_len = decoder_ctx.col_header_->length_;
      int64_t data_offset = 0;
      if (decoder_ctx.has_extend_value()) {
        data_offset = decoder_ctx.micro_block_header_->row_count_
          * decoder_ctx.micro_block_header_->extend_value_bit_;
        data_offset = (data_offset + CHAR_BIT - 1) / CHAR_BIT;
      }
      const char *fixed_buf = meta_data_ + data_offset;
      DataFixedLocator fixed_locator(vector_ctx.row_ids_, fixed_buf, fixed_packing_len, col_data);
      if (OB_FAIL(ObVecDecodeUtils::load_byte_aligned_vector<DataFixedLocator>(
          decoder_ctx.obj_meta_, decoder_ctx.col_header_->get_store_obj_type(), fixed_packing_len,
          decoder_ctx.has_extend_value(), fixed_locator, vector_ctx.row_cap_,
          vector_ctx.vec_offset_, vector_ctx.vec_header_))) {
        LOG_WARN("failed to load byte aligned data to vector", K(ret));
      }
    } else {
      // var-length
      bool has_null = false;
      if (OB_FAIL(batch_locate_var_len_row(decoder_ctx, row_index, vector_ctx, has_null))) {
        LOG_WARN("Faild to set null vector from var-length column", K(ret));
      } else if (has_null) {
        ret = ObIColumnDecoder::batch_locate_cell_data<ObColumnHeader, true>(decoder_ctx, *decoder_ctx.col_header_,
            vector_ctx.ptr_arr_, vector_ctx.len_arr_, vector_ctx.row_ids_, vector_ctx.row_cap_);
      } else {
        ret = ObIColumnDecoder::batch_locate_cell_data<ObColumnHeader, false>(decoder_ctx, *decoder_ctx.col_header_,
            vector_ctx.ptr_arr_, vector_ctx.len_arr_, vector_ctx.row_ids_, vector_ctx.row_cap_);
      }

      if (OB_FAIL(ret)) {
        LOG_WARN("failed to locate cell datas", K(ret));
      } else {
        const int64_t fixed_packing_len = 0;
        DataDiscreteLocator discrete_locator(vector_ctx.ptr_arr_, vector_ctx.len_arr_);
        if (OB_FAIL(ObVecDecodeUtils::load_byte_aligned_vector<DataDiscreteLocator>(
            decoder_ctx.obj_meta_, decoder_ctx.col_header_->get_store_obj_type(), fixed_packing_len,
            has_null, discrete_locator, vector_ctx.row_cap_,
            vector_ctx.vec_offset_, vector_ctx.vec_header_))) {
          LOG_WARN("failed to load byte aligned data to vector", K(ret));
        }
      }
    }
  }

  return ret;
}

int ObRawDecoder::decode_vector_bitpacked(
    const ObColumnDecoderCtx &decoder_ctx,
    ObVectorDecodeCtx &vector_ctx) const
{
  int ret = OB_SUCCESS;
  #define FILL_VECTOR_FUNC(vector_type, has_null) \
    if (has_null) { \
      ret = decode_vector_bitpacked<vector_type, true>(decoder_ctx, vector_ctx); \
    } else { \
      ret = decode_vector_bitpacked<vector_type, false>(decoder_ctx, vector_ctx); \
    }

  const ObObjMeta &obj_meta = decoder_ctx.obj_meta_;
  const int16_t precision = obj_meta.is_decimal_int() ? obj_meta.get_stored_precision() : PRECISION_UNKNOWN_YET;
  VecValueTypeClass vec_tc = common::get_vec_value_tc(obj_meta.get_type(), obj_meta.get_scale(), precision);
  if (VEC_UNIFORM == vector_ctx.get_format()) {
    FILL_VECTOR_FUNC(ObUniformFormat<false>, decoder_ctx.has_extend_value());
  } else if (VEC_FIXED == vector_ctx.get_format()) {
    switch (vec_tc) {
    case VEC_TC_YEAR: {
      // uint8_t
      FILL_VECTOR_FUNC(ObFixedLengthFormat<uint8_t>, decoder_ctx.has_extend_value());
      break;
    }
    case VEC_TC_DATE:
    case VEC_TC_DEC_INT32: {
      // int32_t
      FILL_VECTOR_FUNC(ObFixedLengthFormat<int32_t>, decoder_ctx.has_extend_value());
      break;
    }
    case VEC_TC_INTEGER:
    case VEC_TC_DATETIME:
    case VEC_TC_TIME:
    case VEC_TC_UNKNOWN:
    case VEC_TC_INTERVAL_YM:
    case VEC_TC_DEC_INT64: {
      // int64_t
      FILL_VECTOR_FUNC(ObFixedLengthFormat<int64_t>, decoder_ctx.has_extend_value());
      break;
    }
    case VEC_TC_UINTEGER:
    case VEC_TC_BIT:
    case VEC_TC_ENUM_SET:
    case VEC_TC_DOUBLE:
    case VEC_TC_FIXED_DOUBLE: {
      // uint64_t
      FILL_VECTOR_FUNC(ObFixedLengthFormat<uint64_t>, decoder_ctx.has_extend_value());
      break;
    }
    case VEC_TC_FLOAT: {
      // float
      FILL_VECTOR_FUNC(ObFixedLengthFormat<uint32_t>, decoder_ctx.has_extend_value());
      break;
    }
    default: {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected vector type class for fixed format", K(ret), K(vec_tc));
    }
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unnexpected vector format", K(ret), K(vector_ctx));
  }
  #undef FILL_VECTOR_FUNC
  return ret;
}

template<typename VectorType, bool HAS_NULL>
int ObRawDecoder::decode_vector_bitpacked(
    const ObColumnDecoderCtx &decoder_ctx,
    ObVectorDecodeCtx &vector_ctx) const
{
  int ret = OB_SUCCESS;
  const unsigned char *col_data = reinterpret_cast<const unsigned char *>(meta_data_);
  const int64_t bit_packing_len = decoder_ctx.col_header_->length_;
  const int64_t bs_len = decoder_ctx.col_header_->length_ * decoder_ctx.micro_block_header_->row_count_;
  bitstream_unpack unpack_func = ObBitStream::get_unpack_func(bit_packing_len);
  VectorType *vector = static_cast<VectorType *>(vector_ctx.get_vector());
  int64_t data_offset = 0;
  uint32_t vec_data_len = 0;
  if (OB_FAIL(get_uint_data_datum_len(
      ObDatum::get_obj_datum_map_type(decoder_ctx.obj_meta_.get_type()), vec_data_len))) {
    LOG_WARN("Failed to get vec data length", K(ret), K(decoder_ctx));
  } else {
    if (decoder_ctx.has_extend_value()) {
      data_offset = decoder_ctx.micro_block_header_->row_count_ * decoder_ctx.micro_block_header_->extend_value_bit_;
    }
    const sql::ObBitVector *null_bitset = sql::to_bit_vector(col_data);
    for (int64_t i = 0; i < vector_ctx.row_cap_; ++i) {
      const int64_t curr_vec_offset = vector_ctx.vec_offset_ + i;
      const int64_t row_id = vector_ctx.row_ids_[i];
      if (HAS_NULL && null_bitset->contain(row_id)) {
        vector->set_null(curr_vec_offset);
      } else {
        int64_t unpacked_val = 0;
        unpack_func(
            col_data,
            data_offset + row_id * decoder_ctx.col_header_->length_,
            decoder_ctx.col_header_->length_,
            bs_len,
            unpacked_val);
        vector->set_payload(curr_vec_offset, &unpacked_val, vec_data_len);
      }
    }
  }
  return ret;
}

/**
 * API for pushdown operator. Objects with store class as ObTextSC is not supported yet.
 * Null object is not greater than, less than or equal to any object.
 */
int ObRawDecoder::pushdown_operator(
    const sql::ObPushdownFilterExecutor *parent,
    const ObColumnDecoderCtx &col_ctx,
    const sql::ObWhiteFilterExecutor &filter,
    const char* meta_data,
    const ObIRowIndex* row_index,
    const sql::PushdownFilterInfo &pd_filter_info,
    ObBitmap &result_bitmap) const
{
  int ret = OB_SUCCESS;
  const sql::ObWhiteFilterOperatorType op_type = filter.get_op_type();
  const unsigned char *col_data = reinterpret_cast<const unsigned char *>(meta_data_);
  if (OB_UNLIKELY(!is_inited())) {
    ret = OB_NOT_INIT;
    LOG_WARN("Raw Decoder not inited", K(ret), K(filter));
  } else if (OB_UNLIKELY(NULL == meta_data || NULL == row_index)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Null pointer for data at pushdown operator", K(ret), K(meta_data));
  } else if (OB_FAIL(get_is_null_bitmap(col_ctx, col_data,
      row_index, pd_filter_info, result_bitmap))) {
    LOG_WARN("Failed to get isnull bitmap", K(ret), K(col_ctx));
  } else if (OB_UNLIKELY(op_type >= sql::WHITE_OP_MAX)){
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid op type for pushed down white filter", K(ret), K(op_type));
  }

  // Matching filter to operators
  if (OB_SUCC(ret)){
    switch (op_type) {
    case sql::WHITE_OP_NU: {
      break;
    }
    case sql::WHITE_OP_NN: {
      if (OB_FAIL(result_bitmap.bit_not())) {
        LOG_WARN("Failed to flip bits for result bitmap",
            K(ret), K(result_bitmap.size()));
      }
      break;
    }
    case sql::WHITE_OP_EQ:
    case sql::WHITE_OP_NE:
    case sql::WHITE_OP_GT:
    case sql::WHITE_OP_GE:
    case sql::WHITE_OP_LT:
    case sql::WHITE_OP_LE: {
      int32_t fix_len_tag = 0;
      bool is_signed_data = false;
      bool fast_filter_valid = false;
      if (OB_FAIL(check_fast_filter_valid(col_ctx, filter, fix_len_tag, is_signed_data, fast_filter_valid))) {
        LOG_WARN("Failed to check fast binary filter valid", K(ret));
      } else if (fast_filter_valid) {
        if (OB_FAIL(fast_binary_comparison_operator(col_ctx, col_data,
          filter, fix_len_tag, is_signed_data, pd_filter_info, result_bitmap))) {
          LOG_WARN("Failed on fast binary comparison operator", K(ret), K(col_ctx));
        }
      } else if (0 != pd_filter_info.batch_size_ && fast_decode_valid(col_ctx)) {
        if (OB_FAIL(fast_datum_comparison_operator(
            col_ctx, row_index, filter, pd_filter_info, result_bitmap))) {
          LOG_WARN("Failed on fast datum comparison operator", K(ret), K(col_ctx));
        }
      } else if (OB_FAIL(comparison_operator(parent, col_ctx, col_data, row_index,
          filter, pd_filter_info, result_bitmap))) {
        LOG_WARN("Failed on Comparison Operator", K(ret), K(col_ctx));
      }
      break;
    }
    case sql::WHITE_OP_IN: {
      if (OB_FAIL(in_operator(parent, col_ctx, col_data, row_index,
                  filter, pd_filter_info, result_bitmap))) {
        LOG_WARN("Failed on In Operator", K(ret), K(col_ctx));
      }
      break;
    }
    case sql::WHITE_OP_BT: {
      if (OB_FAIL(bt_operator(parent, col_ctx, col_data, row_index,
                  filter, pd_filter_info, result_bitmap))) {
        LOG_WARN("Failed on Between Operator", K(ret), K(col_ctx));
      }
      break;
    }
    default: {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("Not supported operation type", K(ret), K(op_type));
    }
    } // end of switch
  }
  return ret;
}

int ObRawDecoder::check_fast_filter_valid(
    const ObColumnDecoderCtx &ctx,
    const sql::ObWhiteFilterExecutor &filter,
    int32_t &fix_length_tag,
    bool &is_signed_data,
    bool &valid) const
{
  int ret = OB_SUCCESS;
  common::ObObjMeta filter_val_meta;
  if (OB_FAIL(filter.get_filter_node().get_filter_val_meta(filter_val_meta))) {
    LOG_WARN("Fail to find datum meta", K(ret), K(filter));
  } else {
    valid = !ctx.has_extend_value()
                && !ctx.is_bit_packing()
                && ctx.is_fix_length()
                && raw_fix_fast_filter_funcs_inited
                && ctx.col_header_->get_store_obj_type() == filter_val_meta.get_type();
    // vectorized binary filter on situation that filter object type different with
    // column stored type not supported yet
  }
  if (OB_SUCC(ret) && valid) {
    int64_t fix_len = ctx.col_header_->length_;
    if (fix_len != 1 && fix_len != 2 && fix_len != 4 && fix_len != 8) {
      valid = false;
    } else {
      fix_length_tag = get_value_len_tag_map()[fix_len];
    }
  }
  if (OB_SUCC(ret) && valid) {
    switch (ob_obj_type_class(ctx.col_header_->get_store_obj_type())) {
    case ObIntTC:
    case ObDateTimeTC:
    case ObDateTC:
    case ObTimeTC: {
      is_signed_data = true;
      break;
    }
    case ObUIntTC:
    case ObYearTC: {
      is_signed_data = false;
      break;
    }
    case ObDecimalIntTC: {
      is_signed_data = true;
      const int filter_precision_len = wide::ObDecimalIntConstValue::get_int_bytes_by_precision(filter_val_meta.get_stored_precision());
      const uint32_t filter_datum_len = filter.get_datums().at(0).len_;
      valid = ctx.col_header_->length_ == filter_precision_len
          && ctx.col_header_->length_ == filter_datum_len
          && filter_datum_len <= sizeof(uint64_t);
      break;
    }
    default: {
      valid = false;
    }
    }
  }
  return ret;
}

int ObRawDecoder::get_null_count(
    const ObColumnDecoderCtx &ctx,
    const ObIRowIndex *row_index,
    const int32_t *row_ids,
    const int64_t row_cap,
    int64_t &null_count) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited())) {
    ret = OB_NOT_INIT;
    LOG_WARN("Raw decoder is not inited", K(ret));
  } else if OB_FAIL(ObIColumnDecoder::get_null_count_from_extend_value(
      ctx,
      row_index,
      row_ids,
      row_cap,
      meta_data_,
      null_count)) {
    LOG_WARN("Failed to get null count", K(ctx), K(ret));
  }
  return ret;
}
int ObRawDecoder::get_is_null_bitmap(
    const ObColumnDecoderCtx &col_ctx,
    const unsigned char* col_data,
    const ObIRowIndex* row_index,
    const sql::PushdownFilterInfo &pd_filter_info,
    ObBitmap &result_bitmap) const
{
  int ret = OB_SUCCESS;
  // Get is null bitmap from encoding data
  if (OB_UNLIKELY(pd_filter_info.count_ != result_bitmap.size()
                         || NULL == col_data
                         || NULL == row_index)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Filter Pushdown Operator: invalid argument",
        K(ret), K(pd_filter_info), K(result_bitmap.size()));
  } else if (col_ctx.is_fix_length() || col_ctx.is_bit_packing()) {
    if (OB_FAIL(get_is_null_bitmap_from_fixed_column(col_ctx, col_data,
        pd_filter_info, result_bitmap))) {
      LOG_WARN("Failed to get isnull bitmap from fixed column", K(ret));
    }
  } else {
    if (OB_FAIL(get_is_null_bitmap_from_var_column(col_ctx, row_index,
        pd_filter_info, result_bitmap))) {
      LOG_WARN("Failed to get isnull bitmap from variable column", K(ret));
    }
  }
  return ret;
}

int ObRawDecoder::comparison_operator(
    const sql::ObPushdownFilterExecutor *parent,
    const ObColumnDecoderCtx &col_ctx,
    const unsigned char* col_data,
    const ObIRowIndex* row_index,
    const sql::ObWhiteFilterExecutor &filter,
    const sql::PushdownFilterInfo &pd_filter_info,
    ObBitmap &result_bitmap) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(pd_filter_info.count_ != result_bitmap.size()
                  || NULL == col_data
                  || NULL == row_index
                  || filter.get_datums().count() != 1)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Filter pushdown operator: Invalid argument",
        K(ret), K(col_ctx), K(pd_filter_info), K(result_bitmap.size()), K(filter), K(row_index));
  } else {
    ObDatumCmpFuncType type_cmp_func = filter.cmp_func_;
    ObGetFilterCmpRetFunc get_cmp_ret = get_filter_cmp_ret_func(filter.get_op_type());
    ObRawDecoderFilterCmpFunc eval(type_cmp_func, get_cmp_ret);

    if (OB_FAIL(traverse_all_data(parent, col_ctx, row_index, filter, pd_filter_info, result_bitmap, eval))) {
      LOG_WARN("Failed to traverse all data and evaluate operator", K(ret));
    }
  }
  return ret;
}

// No null value for fast comparison operator
int ObRawDecoder::fast_binary_comparison_operator(
    const ObColumnDecoderCtx &col_ctx,
    const unsigned char* col_data,
    const sql::ObWhiteFilterExecutor &filter,
    int32_t fix_len_tag,
    bool is_signed_data,
    const sql::PushdownFilterInfo &pd_filter_info,
    ObBitmap &result_bitmap) const
{
  int ret = OB_SUCCESS;
  common::ObObjMeta filter_val_meta;
  if (OB_UNLIKELY(pd_filter_info.count_ != result_bitmap.size()
                         || NULL == col_data
                         || filter.get_datums().count() != 1
                         || fix_len_tag > 3)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Filter pushdown operator: Invalid argument",
        K(ret), K(col_ctx), K(fix_len_tag), K(pd_filter_info), K(result_bitmap.size()));
  } else if (OB_FAIL(filter.get_filter_node().get_filter_val_meta(filter_val_meta))) {
    LOG_WARN("Fail to find datum meta", K(ret), K(filter));
  } else {
    const int64_t type_store_size = filter_val_meta.is_decimal_int() ?
      wide::ObDecimalIntConstValue::get_int_bytes_by_precision(filter_val_meta.get_stored_precision())
      : get_type_size_map()[col_ctx.col_header_->get_store_obj_type()];
    const int64_t node_store_size = filter_val_meta.is_decimal_int() ?
        type_store_size // for decimal int type, data_length should equal to filter precision len
        : get_type_size_map()[filter_val_meta.get_type()];
    const uint64_t node_value = get_uint64_from_buf_by_len(filter.get_datums().at(0).ptr_, node_store_size);
    const sql::ObWhiteFilterOperatorType &op_type = filter.get_op_type();
    bool exceed_stored_value_range = ~INTEGER_MASK_TABLE[col_ctx.col_header_->length_]
        & (INTEGER_MASK_TABLE[node_store_size] & node_value);
    if (exceed_stored_value_range) {
      if (sql::WHITE_OP_EQ == op_type) {
        // All false
      } else if (sql::WHITE_OP_NE == op_type) {
        // All true
        if (OB_FAIL(result_bitmap.bit_not())) {
          LOG_WARN("Failed to set result bitmap to all true", K(ret));
        }
      } else if (is_signed_data) {
        // ObIntTC, ObDateTimeTC, ObDateTC, ObTimeTC
        if ((~(INTEGER_MASK_TABLE[node_store_size] >> 1)) & (node_value & INTEGER_MASK_TABLE[node_store_size])) {
          // negative node value, node value always smaller
          if (sql::WHITE_OP_GT == op_type || sql::WHITE_OP_GE == op_type) {
            if (OB_FAIL(result_bitmap.bit_not())) {
              LOG_WARN("Failed to set result bitmap to all true", K(ret));
            }
          }
        } else {
          // positive node value, node value always larger
          if (sql::WHITE_OP_LT == op_type || sql::WHITE_OP_LE == op_type) {
            if (OB_FAIL(result_bitmap.bit_not())) {
              LOG_WARN("Failed to set result bitmap to all true", K(ret));
            }
          }
        }
      } else {
        // ObUIntTC, ObYearTC
        // Value in filter always larger
        if (sql::WHITE_OP_LT == op_type || sql::WHITE_OP_LE == op_type
            || sql::WHITE_OP_NE == op_type) {
          if (OB_FAIL(result_bitmap.bit_not())) {
            LOG_WARN("Failed to set result bitmap to all true", K(ret));
          }
        }
      }
    } else {
      raw_compare_function cmp_funtion = RawCompareFunctionFactory::instance().get_cmp_function(
                                          is_signed_data && type_store_size == col_ctx.col_header_->length_,
                                          fix_len_tag,
                                          op_type);
      if (OB_ISNULL(cmp_funtion)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Unexpected nullptr compare function", K(ret), K(is_signed_data), K(type_store_size),
                  K_(col_ctx.col_header_->length), K(fix_len_tag), K(op_type));
      } else {
        cmp_funtion(col_data, node_value, result_bitmap.get_data(), pd_filter_info.start_,
                     pd_filter_info.start_ + pd_filter_info.count_);
      }
    }
  }
  return ret;
}

int ObRawDecoder::fast_datum_comparison_operator(
    const ObColumnDecoderCtx &col_ctx,
    const ObIRowIndex* row_index,
    const sql::ObWhiteFilterExecutor &filter,
    const sql::PushdownFilterInfo &pd_filter_info,
    ObBitmap &result_bitmap) const
{
  int ret = OB_SUCCESS;
  ObDatum *datums = nullptr;
  if (OB_UNLIKELY(pd_filter_info.count_ != result_bitmap.size()
      || filter.get_datums().count() != 1)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Filter pushdown operator: Invalid argument",
        K(ret), K(col_ctx), K(pd_filter_info), K(result_bitmap.size()));
  } else if (OB_FAIL(pd_filter_info.get_col_datum(datums))) {
    LOG_WARN("Failed to get col datum for batch decode", K(ret), K(pd_filter_info));
  } else {
    ObGetFilterCmpRetFunc get_cmp_ret = get_filter_cmp_ret_func(filter.get_op_type());
    ObDatumCmpFuncType cmp_func = filter.cmp_func_;

    // OPT: remove this rowid array usage by adding a new batch decode interface？
    int32_t *row_ids = pd_filter_info.row_ids_;
    int64_t evaluated_row_cnt = 0;
    while (OB_SUCC(ret) && evaluated_row_cnt < pd_filter_info.count_) {
      //decode and evaluate one batch
      const int64_t curr_batch_size = MIN(pd_filter_info.batch_size_, (pd_filter_info.count_ - evaluated_row_cnt));
      const int64_t first_row_id = pd_filter_info.start_ + evaluated_row_cnt;
      for (int64_t i = 0; i < curr_batch_size; ++i) {
        row_ids[i] = first_row_id + i;
      }
      if (OB_FAIL(batch_decode_fast(col_ctx, row_index, row_ids, curr_batch_size, datums))) {
        LOG_WARN("Failed to batch decode", K(ret), K(col_ctx), K(evaluated_row_cnt), K(curr_batch_size));
      } else if (col_ctx.obj_meta_.is_fixed_len_char_type() && nullptr != col_ctx.col_param_
          && OB_FAIL(storage::pad_on_datums(
              col_ctx.col_param_->get_accuracy(),
              col_ctx.obj_meta_.get_collation_type(),
              *col_ctx.allocator_,
              curr_batch_size,
              datums))) {
        LOG_WARN("Failed to pad fixed char on demand", K(ret), K(col_ctx), K(curr_batch_size));
      } else {
        int cmp_res = 0;
        for (int64_t i = 0; OB_SUCC(ret) && i < curr_batch_size; ++i) {
          if (OB_FAIL(cmp_func(datums[i], filter.get_datums().at(0), cmp_res))) {
            LOG_WARN("Failed to compare datum", K(ret),
                K(i), K(datums[i]), K(filter.get_datums().at(0)));
          } else if (get_cmp_ret(cmp_res)) {
            if (OB_FAIL(result_bitmap.set(evaluated_row_cnt + i))) {
              LOG_WARN("Failed to set result bitmap", K(ret), K(evaluated_row_cnt), K(i), K(curr_batch_size));
            }
          }
        }
      }
      if (OB_SUCC(ret)) {
        evaluated_row_cnt += curr_batch_size;
      }
    }
  }
  return ret;
}

// For BETWEEN operator, the first object is left boundary and the second is right boundary
int ObRawDecoder::bt_operator(
    const sql::ObPushdownFilterExecutor *parent,
    const ObColumnDecoderCtx &col_ctx,
    const unsigned char* col_data,
    const ObIRowIndex* row_index,
    const sql::ObWhiteFilterExecutor &filter,
    const sql::PushdownFilterInfo &pd_filter_info,
    ObBitmap &result_bitmap) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(pd_filter_info.count_ != result_bitmap.size()
                         || NULL == col_data
                         || NULL == row_index
                         || filter.get_datums().count() != 2)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Filter pushdown operator: Invalid arguments",
        K(ret), K(pd_filter_info), K(result_bitmap.size()), K(filter));
  } else {
    ObDatumCmpFuncType type_cmp_func = filter.cmp_func_;
    ObRawDecoderFilterBetweenFunc eval(type_cmp_func);
    if (OB_FAIL(traverse_all_data(parent, col_ctx, row_index, filter, pd_filter_info, result_bitmap, eval))) {
      LOG_WARN("Failed to traverse all data in micro block", K(ret));
    }
  }
  return ret;
}

int ObRawDecoder::in_operator(
    const sql::ObPushdownFilterExecutor *parent,
    const ObColumnDecoderCtx &col_ctx,
    const unsigned char* col_data,
    const ObIRowIndex* row_index,
    const sql::ObWhiteFilterExecutor &filter,
    const sql::PushdownFilterInfo &pd_filter_info,
    ObBitmap &result_bitmap) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(filter.get_datums().count() == 0
             || result_bitmap.size() != pd_filter_info.count_
             || NULL == row_index)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Pushdown in operator: Invalid arguments",
        K(ret), K(pd_filter_info), K(result_bitmap.size()), K(filter));
  } else {
    ObRawDecoderFilterInFunc eval;
    if (OB_FAIL(traverse_all_data(parent, col_ctx, row_index, filter, pd_filter_info, result_bitmap, eval))) {
      LOG_WARN("Failed to traverse all data in micro block", K(ret));
    }
  }
  return ret;
}

/**
 *  Function to traverse all row data with raw encoding, regardless of column is fixed length
 *  or var lengthand run lambda function for every row element.
 */
template<typename Operator>
int ObRawDecoder::traverse_all_data(
    const sql::ObPushdownFilterExecutor *parent,
    const ObColumnDecoderCtx &col_ctx,
    const ObIRowIndex* row_index,
    const sql::ObWhiteFilterExecutor &filter,
    const sql::PushdownFilterInfo &pd_filter_info,
    ObBitmap &result_bitmap,
    Operator const &eval) const
{
  int ret = OB_SUCCESS;
  // Initialize datum
  const unsigned char *col_data = reinterpret_cast<const unsigned char *>(meta_data_);
  ObStorageDatum cur_datum;
  int64_t data_offset = 0;
  if (col_ctx.has_extend_value() && (col_ctx.is_fix_length() || col_ctx.is_bit_packing())) {
    data_offset = col_ctx.micro_block_header_->row_count_ * col_ctx.micro_block_header_->extend_value_bit_;
  }
  if (col_ctx.is_bit_packing()) {
    uint32_t datum_len = 0;
    uint64_t value = 0;
    int64_t row_id = 0;
    for (int64_t offset = 0; OB_SUCC(ret) && offset < pd_filter_info.count_; ++offset) {
      row_id = offset + pd_filter_info.start_;
      if (nullptr != parent && parent->can_skip_filter(offset)) {
        continue;
      } else if (col_ctx.has_extend_value() && result_bitmap.test(offset)) {
        // datum in this row is null
        if (OB_FAIL(result_bitmap.set(offset, false))) {
          LOG_WARN("Failed to set null value to false", K(ret), K(offset), K(pd_filter_info));
        }
      } else if (OB_FAIL(get_uint_data_datum_len(
          ObDatum::get_obj_datum_map_type(col_ctx.obj_meta_.get_type()),
          datum_len))) {
        LOG_WARN("Failed to get datum len for int data", K(ret));
      } else if (OB_FAIL(ObBitStream::get(
                  col_data,
                  data_offset + row_id * col_ctx.col_header_->length_,
                  col_ctx.col_header_->length_,
                  value))) {
        LOG_WARN("Failed to read bit packing object data from bitstream", K(ret), K(col_ctx));
      } else {
        cur_datum.pack_ = datum_len;
        cur_datum.ptr_ = (const char *)(&value);
        bool result = false;
        if (OB_FAIL(eval(cur_datum, filter, result))) {
          LOG_WARN("Failed on trying to filter the row", K(ret), K(row_id), K(cur_datum));
        } else if (result) {
          if (OB_FAIL(result_bitmap.set(offset))) {
            LOG_WARN("Failed to set result bitmap", K(ret), K(offset), K(filter));
          }
        }
      }
    }
  } else {
    int64_t cell_len = 0;
    const char *cell_data = NULL;
    data_offset = (data_offset + CHAR_BIT - 1) / CHAR_BIT;
    const char *row_data = NULL;
    int64_t row_len = 0;
    if (col_ctx.is_fix_length()) {
      cell_len = col_ctx.col_header_->length_;
    }
    int64_t row_id = 0;
    for (int64_t offset = 0; OB_SUCC(ret) && offset < pd_filter_info.count_; ++offset) {
      row_id = offset + pd_filter_info.start_;
      if (nullptr != parent && parent->can_skip_filter(offset)) {
        continue;
      } else if (col_ctx.has_extend_value() && result_bitmap.test(offset)) {
        // object in this row is null
        if (OB_FAIL(result_bitmap.set(offset, false))) {
          LOG_WARN("Failed to set null value to false", K(ret), K(offset));
       }
      } else {
        if (!col_ctx.is_fix_length()) {
          if (OB_FAIL(locate_row_data(col_ctx, row_index, row_id, row_data, row_len))) {
            LOG_WARN("Failed to read data offset from row index", K(ret), K(row_index));
          } else if (OB_FAIL(locate_cell_data(cell_data, cell_len, row_data, row_len,
                                              *col_ctx.micro_block_header_, *col_ctx.col_header_, *col_ctx.col_header_))) {
            LOG_WARN("Failed to locate cell data", K(ret), K(row_len), K(col_ctx));
          }
        } else {
          cell_data = meta_data_ + data_offset + row_id * col_ctx.col_header_->length_;
        }

        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(load_data_to_datum(col_ctx.obj_meta_.get_type(), cell_data, cell_len, integer_mask_, cur_datum))) {
          LOG_WARN("Failed to load data to object cell", K(ret), K(cell_data), K(cell_len));
        } else {
          // Padding for non-bitpacking data if required
          if (col_ctx.obj_meta_.is_fixed_len_char_type() && nullptr != col_ctx.col_param_) {
            if (OB_FAIL(storage::pad_column(col_ctx.obj_meta_, col_ctx.col_param_->get_accuracy(),
                                            *col_ctx.allocator_, cur_datum))) {
              LOG_WARN("Failed to pad column", K(ret));
            }
          }
        }
        if (OB_SUCC(ret)) {
          // Run lambda here to filter out the data according to op_type
          bool result = false;
          if (OB_FAIL(eval(cur_datum, filter, result))) {
            LOG_WARN("Failed on trying to filter the row", K(ret), K(row_id), K(cur_datum));
          } else if (result) {
            if (OB_FAIL(result_bitmap.set(offset))) {
              LOG_WARN("Failed to set result bitmap",
                  K(ret), K(offset), K(pd_filter_info), K(filter));
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObRawDecoder::ObRawDecoderFilterCmpFunc::operator()(
    const ObDatum &cur_datum,
    const sql::ObWhiteFilterExecutor &filter,
    bool &result) const
{
  int ret = OB_SUCCESS;
  int cmp_res = 0;
  if (OB_FAIL(type_cmp_func_(cur_datum, filter.get_datums().at(0), cmp_res))) {
    LOG_WARN("Failed to compare datum", K(ret), K(cur_datum), K(filter.get_datums().at(0)));
  } else {
    result = get_cmp_ret_(cmp_res);
  }
  return ret;
}

int ObRawDecoder::ObRawDecoderFilterBetweenFunc::operator()(
    const ObDatum &cur_datum,
    const sql::ObWhiteFilterExecutor &filter,
    bool &result) const
{
  int ret = OB_SUCCESS;
  int left_cmp_res = 0;
  int right_cmp_res = 0;
  if (OB_FAIL(type_cmp_func_(cur_datum, filter.get_datums().at(0), left_cmp_res))) {
    LOG_WARN("Failed to compare datum", K(ret), K(cur_datum), K(filter.get_datums().at(0)));
  } else if (left_cmp_res < 0) {
    result = false;
  } else if (OB_FAIL(type_cmp_func_(cur_datum, filter.get_datums().at(1), right_cmp_res))) {
    LOG_WARN("Failed to compare datum", K(ret), K(cur_datum), K(filter.get_datums().at(1)));
  } else {
    result = (right_cmp_res <= 0);
  }
  return ret;
}

int ObRawDecoder::ObRawDecoderFilterInFunc::operator()(
    const ObDatum &cur_datum,
    const sql::ObWhiteFilterExecutor &filter,
    bool &result) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(filter.exist_in_datum_set(cur_datum, result))) {
    LOG_WARN("Failed to check datum in hashset", K(ret), K(cur_datum));
  }
  return ret;
}

template <typename DataType>
struct RawEqualsOp
{
  static uint8_t apply(DataType a, DataType b) { return a == b; }
};

template <typename DataType>
struct RawNotEqualsOp
{
  static uint8_t apply(DataType a, DataType b) { return a != b; }
};

template <typename DataType>
struct RawGreaterOp
{
  static uint8_t apply(DataType a, DataType b) { return a > b; }
};

template <typename DataType>
struct RawLessOp
{
  static uint8_t apply(DataType a, DataType b) { return a < b; }
};

template <typename DataType>
struct RawGreaterOrEqualsOp
{
  static uint8_t apply(DataType a, DataType b) { return a >= b; }
};

template <typename DataType>
struct RawLessOrEqualsOp
{
  static uint8_t apply(DataType a, DataType b) { return a <= b; }
};

template <typename DataType, typename Op>
class RawCompareFunctionImpl {
public:
  OB_MULTITARGET_FUNCTION_AVX2_SSE42(
  OB_MULTITARGET_FUNCTION_HEADER(static void), raw_compare_function, OB_MULTITARGET_FUNCTION_BODY((
      const unsigned char* raw_data,
      const uint64_t node_value,
      uint8_t* selection,
      uint32_t from,
      uint32_t to)
  {
    // GCC will unroll the loop and use SIMD instructions.
    const DataType value = *reinterpret_cast<const DataType *>(&node_value);
    const DataType *start_pos = reinterpret_cast<const DataType *>(raw_data);
    const DataType *a_end = start_pos + to;
    const DataType * __restrict a_pos = start_pos + from;
    uint8_t * __restrict c_pos = selection;
    while (a_pos < a_end) {
      *c_pos = Op::apply(*a_pos, value);
      ++a_pos;
      ++c_pos;
    }
  }))

  OB_MULTITARGET_FUNCTION_AVX2_SSE42(
  OB_MULTITARGET_FUNCTION_HEADER(static void), raw_compare_function_with_null, OB_MULTITARGET_FUNCTION_BODY((
      const unsigned char* raw_data,
      const uint64_t node_value,
      const uint64_t null_node_value,
      uint8_t* selection,
      uint32_t from,
      uint32_t to)
  {
    const DataType value = *reinterpret_cast<const DataType *>(&node_value);
    const DataType *start_pos = reinterpret_cast<const DataType *>(raw_data);
    const DataType *a_end = start_pos + to;
    const DataType * __restrict a_pos = start_pos + from;
    uint8_t * __restrict c_pos = selection;
    while (a_pos < a_end) {
      *c_pos = (uint8_t)(*a_pos != null_node_value) & Op::apply(*a_pos, value);
      ++a_pos;
      ++c_pos;
    }
  }))
};

template <bool IS_SIGNED, int32_t LEN_TAG>
struct RawCompareFunctionProducer
{
  static raw_compare_function produce(
      const sql::ObWhiteFilterOperatorType op_type)
  {
    raw_compare_function cmp_funtion = nullptr;
    typedef typename ObEncodingTypeInference<IS_SIGNED, LEN_TAG>::Type DataType;
    const bool is_supported = is_arch_supported(ObTargetArch::AVX2);
    switch (op_type) {
      case sql::ObWhiteFilterOperatorType::WHITE_OP_EQ:
#if OB_USE_MULTITARGET_CODE
        if (is_supported) {
          cmp_funtion = RawCompareFunctionImpl<DataType, RawEqualsOp<DataType>>::raw_compare_function_avx2;
        } else {
          cmp_funtion = RawCompareFunctionImpl<DataType, RawEqualsOp<DataType>>::raw_compare_function;
        }
#else
        cmp_funtion = RawCompareFunctionImpl<DataType, RawEqualsOp<DataType>>::raw_compare_function;
#endif
        break;
      case sql::ObWhiteFilterOperatorType::WHITE_OP_LE:
#if OB_USE_MULTITARGET_CODE
        if (is_supported) {
          // cmp_funtion = RawCompareFunctionImpl<DataType, RawLessOrEqualsOp<DataType>>::raw_compare_function_sse42;
          cmp_funtion = RawCompareFunctionImpl<DataType, RawLessOrEqualsOp<DataType>>::raw_compare_function_avx2;
        } else {
          cmp_funtion = RawCompareFunctionImpl<DataType, RawLessOrEqualsOp<DataType>>::raw_compare_function;
        }
#else
        cmp_funtion = RawCompareFunctionImpl<DataType, RawLessOrEqualsOp<DataType>>::raw_compare_function;
#endif
        break;
      case sql::ObWhiteFilterOperatorType::WHITE_OP_LT:
#if OB_USE_MULTITARGET_CODE
        if (is_supported) {
          cmp_funtion = RawCompareFunctionImpl<DataType, RawLessOp<DataType>>::raw_compare_function_avx2;
        } else {
          cmp_funtion = RawCompareFunctionImpl<DataType, RawLessOp<DataType>>::raw_compare_function;
        }
#else
        cmp_funtion = RawCompareFunctionImpl<DataType, RawLessOp<DataType>>::raw_compare_function;
#endif
        break;
      case sql::ObWhiteFilterOperatorType::WHITE_OP_GE:
#if OB_USE_MULTITARGET_CODE
        if (is_supported) {
          cmp_funtion = RawCompareFunctionImpl<DataType, RawGreaterOrEqualsOp<DataType>>::raw_compare_function_avx2;
        } else {
          cmp_funtion = RawCompareFunctionImpl<DataType, RawGreaterOrEqualsOp<DataType>>::raw_compare_function;
        }
#else
        cmp_funtion = RawCompareFunctionImpl<DataType, RawGreaterOrEqualsOp<DataType>>::raw_compare_function;
#endif
        break;
      case sql::ObWhiteFilterOperatorType::WHITE_OP_GT:
#if OB_USE_MULTITARGET_CODE
        if (is_supported) {
          cmp_funtion = RawCompareFunctionImpl<DataType, RawGreaterOp<DataType>>::raw_compare_function_avx2;
        } else {
          cmp_funtion = RawCompareFunctionImpl<DataType, RawGreaterOp<DataType>>::raw_compare_function;
        }
#else
        cmp_funtion = RawCompareFunctionImpl<DataType, RawGreaterOp<DataType>>::raw_compare_function;
#endif
        break;
      case sql::ObWhiteFilterOperatorType::WHITE_OP_NE:
#if OB_USE_MULTITARGET_CODE
        if (is_supported) {
          cmp_funtion = RawCompareFunctionImpl<DataType, RawNotEqualsOp<DataType>>::raw_compare_function_avx2;
        } else {
          cmp_funtion = RawCompareFunctionImpl<DataType, RawNotEqualsOp<DataType>>::raw_compare_function;
        }
#else
        cmp_funtion = RawCompareFunctionImpl<DataType, RawNotEqualsOp<DataType>>::raw_compare_function;
#endif
        break;
      default:
        LOG_ERROR_RET(OB_ERR_UNEXPECTED, "Invalid white filter type", K(op_type));
        break;
    }
    return cmp_funtion;
  }

  static raw_compare_function_with_null produce_with_null_for_cs(
      const sql::ObWhiteFilterOperatorType op_type)
  {
    raw_compare_function_with_null cmp_funtion = nullptr;
    const bool is_supported = is_arch_supported(ObTargetArch::AVX2);
    typedef typename ObEncodingTypeInference<IS_SIGNED, LEN_TAG>::Type DataType;
    switch (op_type) {
      case sql::ObWhiteFilterOperatorType::WHITE_OP_EQ:
#if OB_USE_MULTITARGET_CODE
        if (is_supported) {
          cmp_funtion = RawCompareFunctionImpl<DataType, RawEqualsOp<DataType>>::raw_compare_function_with_null_avx2;
        } else {
          cmp_funtion = RawCompareFunctionImpl<DataType, RawEqualsOp<DataType>>::raw_compare_function_with_null;
        }
#else
        cmp_funtion = RawCompareFunctionImpl<DataType, RawEqualsOp<DataType>>::raw_compare_function_with_null;
#endif
        break;
      case sql::ObWhiteFilterOperatorType::WHITE_OP_LE:
#if OB_USE_MULTITARGET_CODE
        if (is_supported) {
          cmp_funtion = RawCompareFunctionImpl<DataType, RawLessOrEqualsOp<DataType>>::raw_compare_function_with_null_avx2;
        } else {
          cmp_funtion = RawCompareFunctionImpl<DataType, RawLessOrEqualsOp<DataType>>::raw_compare_function_with_null;
        }
#else
        cmp_funtion = RawCompareFunctionImpl<DataType, RawLessOrEqualsOp<DataType>>::raw_compare_function_with_null;
#endif
        break;
      case sql::ObWhiteFilterOperatorType::WHITE_OP_LT:
#if OB_USE_MULTITARGET_CODE
        if (is_supported) {
          cmp_funtion = RawCompareFunctionImpl<DataType, RawLessOp<DataType>>::raw_compare_function_with_null_avx2;
        } else {
          cmp_funtion = RawCompareFunctionImpl<DataType, RawLessOp<DataType>>::raw_compare_function_with_null;
        }
#else
        cmp_funtion = RawCompareFunctionImpl<DataType, RawLessOp<DataType>>::raw_compare_function_with_null;
#endif
        break;
      case sql::ObWhiteFilterOperatorType::WHITE_OP_GE:
#if OB_USE_MULTITARGET_CODE
        if (is_supported) {
          cmp_funtion = RawCompareFunctionImpl<DataType, RawGreaterOrEqualsOp<DataType>>::raw_compare_function_with_null_avx2;
        } else {
          cmp_funtion = RawCompareFunctionImpl<DataType, RawGreaterOrEqualsOp<DataType>>::raw_compare_function_with_null;
        }
#else
        cmp_funtion = RawCompareFunctionImpl<DataType, RawGreaterOrEqualsOp<DataType>>::raw_compare_function_with_null;
#endif
        break;
      case sql::ObWhiteFilterOperatorType::WHITE_OP_GT:
#if OB_USE_MULTITARGET_CODE
        if (is_supported) {
          cmp_funtion = RawCompareFunctionImpl<DataType, RawGreaterOp<DataType>>::raw_compare_function_with_null_avx2;
        } else {
          cmp_funtion = RawCompareFunctionImpl<DataType, RawGreaterOp<DataType>>::raw_compare_function_with_null;
        }
#else
        cmp_funtion = RawCompareFunctionImpl<DataType, RawGreaterOp<DataType>>::raw_compare_function_with_null;
#endif
        break;
      case sql::ObWhiteFilterOperatorType::WHITE_OP_NE:
#if OB_USE_MULTITARGET_CODE
        if (is_supported) {
          cmp_funtion = RawCompareFunctionImpl<DataType, RawNotEqualsOp<DataType>>::raw_compare_function_with_null_avx2;
        } else {
          cmp_funtion = RawCompareFunctionImpl<DataType, RawNotEqualsOp<DataType>>::raw_compare_function_with_null;
        }
#else
        cmp_funtion = RawCompareFunctionImpl<DataType, RawNotEqualsOp<DataType>>::raw_compare_function_with_null;
#endif
        break;
      default:
        LOG_ERROR_RET(OB_ERR_UNEXPECTED, "Invalid white filter type", K(op_type));
        break;
    }
    return cmp_funtion;
  }
};

RawCompareFunctionFactory::RawCompareFunctionFactory()
{
  for (uint32_t k = 0; k < OP_TYPE_CNT; ++k) {
    functions_array_[0][0][k] =
      RawCompareFunctionProducer<0, 0>::produce(static_cast<sql::ObWhiteFilterOperatorType>(k));
    functions_array_[0][1][k] =
      RawCompareFunctionProducer<0, 1>::produce(static_cast<sql::ObWhiteFilterOperatorType>(k));
    functions_array_[0][2][k] =
      RawCompareFunctionProducer<0, 2>::produce(static_cast<sql::ObWhiteFilterOperatorType>(k));
    functions_array_[0][3][k] =
      RawCompareFunctionProducer<0, 3>::produce(static_cast<sql::ObWhiteFilterOperatorType>(k));
    functions_array_[1][0][k] =
      RawCompareFunctionProducer<1, 0>::produce(static_cast<sql::ObWhiteFilterOperatorType>(k));
    functions_array_[1][1][k] =
      RawCompareFunctionProducer<1, 1>::produce(static_cast<sql::ObWhiteFilterOperatorType>(k));
    functions_array_[1][2][k] =
      RawCompareFunctionProducer<1, 2>::produce(static_cast<sql::ObWhiteFilterOperatorType>(k));
    functions_array_[1][3][k] =
      RawCompareFunctionProducer<1, 3>::produce(static_cast<sql::ObWhiteFilterOperatorType>(k));
  }

  for (uint32_t k = 0; k < OP_TYPE_CNT; ++k) {
    cs_functions_with_null_array_[0][k] = RawCompareFunctionProducer<0, 0>::produce_with_null_for_cs(
        static_cast<sql::ObWhiteFilterOperatorType>(k));
    cs_functions_with_null_array_[1][k] = RawCompareFunctionProducer<0, 1>::produce_with_null_for_cs(
        static_cast<sql::ObWhiteFilterOperatorType>(k));
    cs_functions_with_null_array_[2][k] = RawCompareFunctionProducer<0, 2>::produce_with_null_for_cs(
        static_cast<sql::ObWhiteFilterOperatorType>(k));
    cs_functions_with_null_array_[3][k] = RawCompareFunctionProducer<0, 3>::produce_with_null_for_cs(
        static_cast<sql::ObWhiteFilterOperatorType>(k));
  }
}

RawCompareFunctionFactory &RawCompareFunctionFactory::instance()
{
  static RawCompareFunctionFactory ret;
  return ret;
}

raw_compare_function RawCompareFunctionFactory::get_cmp_function(
    const bool is_signed,
    const int32_t fix_len_tag,
    const sql::ObWhiteFilterOperatorType op_type)
{
  raw_compare_function cmp_function = nullptr;
  if (OB_UNLIKELY(fix_len_tag < 0 || fix_len_tag >= FIX_LEN_TAG_CNT
                   || op_type < 0 || op_type >= OP_TYPE_CNT)) {
  } else {
    cmp_function = functions_array_[is_signed ? 1 : 0][fix_len_tag][op_type];
  }
  return cmp_function;
}

raw_compare_function_with_null RawCompareFunctionFactory::get_cs_cmp_function_with_null(
      const int32_t fix_len_tag,
      const sql::ObWhiteFilterOperatorType op_type)
{
  raw_compare_function_with_null cmp_function = nullptr;
  if (OB_UNLIKELY(fix_len_tag < 0 || fix_len_tag >= FIX_LEN_TAG_CNT)) {
  } else {
    cmp_function = cs_functions_with_null_array_[fix_len_tag][op_type];
  }
  return cmp_function;
}

template <typename DataType, typename Op>
class RawAggFunctionImpl
{
  public:
  // can use SIMD
  OB_MULTITARGET_FUNCTION_AVX2_SSE42(
  OB_MULTITARGET_FUNCTION_HEADER(static void), raw_min_max_function, OB_MULTITARGET_FUNCTION_BODY((
      const unsigned char* raw_data,
      uint32_t from,
      uint32_t to,
      uint64_t &res)
  {
    const DataType *start_pos = reinterpret_cast<const DataType *>(raw_data);
    const DataType *a_end = start_pos + to;
    const DataType * __restrict a_pos = start_pos + from;
    DataType res_value = *(start_pos + from);
    while (a_pos < a_end) {
      if (Op::apply(*a_pos, res_value)) {
        res_value = *a_pos;
      }
      ++a_pos;
    }
    res = res_value;
  }))

  // can not use SIMD
  OB_MULTITARGET_FUNCTION_AVX2_SSE42(
  OB_MULTITARGET_FUNCTION_HEADER(static void), raw_min_max_function_with_null, OB_MULTITARGET_FUNCTION_BODY((
      const unsigned char* raw_data,
      const uint64_t null_value,
      uint32_t from,
      uint32_t to,
      uint64_t &res)
  {
    const DataType *start_pos = reinterpret_cast<const DataType *>(raw_data);
    const DataType *a_end = start_pos + to;
    const DataType * __restrict a_pos = start_pos + from;
    DataType res_value = *(start_pos + from);
    while (a_pos < a_end) {
      if (*a_pos != null_value && Op::apply(*a_pos, res_value)) {
        res_value = *a_pos;
      }
      ++a_pos;
    }
    res = res_value;
  }))

  // can use SIMD
  OB_MULTITARGET_FUNCTION_AVX2_SSE42(
  OB_MULTITARGET_FUNCTION_HEADER(static void), raw_min_max_function_with_null_bitmap, OB_MULTITARGET_FUNCTION_BODY((
      const unsigned char* raw_data,
      const uint8_t *null_bitmap,
      uint32_t from,
      uint32_t to,
      uint64_t &res)
  {
    const DataType *start_pos = reinterpret_cast<const DataType *>(raw_data);
    const DataType *a_end = start_pos + to;
    const DataType * __restrict a_pos = start_pos + from;
    const uint8_t * __restrict b_pos = null_bitmap;
    DataType res_value = *(start_pos + from);
    while (a_pos < a_end) {
      if (!*b_pos && Op::apply(*a_pos, res_value)) {
        res_value = *a_pos;
      }
      ++a_pos;
      ++b_pos;
    }
    res = res_value;
  }))
};

template <bool IS_SIGNED, int32_t LEN_TAG>
struct RawAggFunctionProducer
{
  static raw_min_max_function produce_min_max(
    const uint32_t type)
  {
    raw_min_max_function min_max_function = nullptr;
    typedef typename ObEncodingTypeInference<IS_SIGNED, LEN_TAG>::Type DataType;
    const bool is_supported = is_arch_supported(ObTargetArch::AVX2);
    switch (type) {
      case 0: // min
#if OB_USE_MULTITARGET_CODE
        if (is_supported) {
          min_max_function = RawAggFunctionImpl<DataType, RawLessOp<DataType>>::raw_min_max_function_avx2;
        } else {
          min_max_function = RawAggFunctionImpl<DataType, RawLessOp<DataType>>::raw_min_max_function;
        }
#else
        min_max_function = RawAggFunctionImpl<DataType, RawLessOp<DataType>>::raw_min_max_function;
#endif
        break;
      case 1:
#if OB_USE_MULTITARGET_CODE
        if (is_supported) {
          min_max_function = RawAggFunctionImpl<DataType, RawGreaterOp<DataType>>::raw_min_max_function_avx2;
        } else {
          min_max_function = RawAggFunctionImpl<DataType, RawGreaterOp<DataType>>::raw_min_max_function;
        }
#else
        min_max_function = RawAggFunctionImpl<DataType, RawGreaterOp<DataType>>::raw_min_max_function;
#endif
        break;
      default:
        LOG_ERROR_RET(OB_ERR_UNEXPECTED, "Invalid min max type", K(type));
        break;
    }
    return min_max_function;
  }

  static raw_min_max_function_with_null produce_min_max_with_null_for_cs(
    const uint32_t type)
  {
    raw_min_max_function_with_null min_max_function = nullptr;
    typedef typename ObEncodingTypeInference<IS_SIGNED, LEN_TAG>::Type DataType;
    const bool is_supported = is_arch_supported(ObTargetArch::AVX2);
    switch (type) {
      case 0: // min
#if OB_USE_MULTITARGET_CODE
        if (is_supported) {
          min_max_function = RawAggFunctionImpl<DataType, RawLessOp<DataType>>::raw_min_max_function_with_null_avx2;
        } else {
          min_max_function = RawAggFunctionImpl<DataType, RawLessOp<DataType>>::raw_min_max_function_with_null;
        }
#else
        min_max_function = RawAggFunctionImpl<DataType, RawLessOp<DataType>>::raw_min_max_function_with_null;
#endif
        break;
      case 1:
#if OB_USE_MULTITARGET_CODE
        if (is_supported) {
          min_max_function = RawAggFunctionImpl<DataType, RawGreaterOp<DataType>>::raw_min_max_function_with_null_avx2;
        } else {
          min_max_function = RawAggFunctionImpl<DataType, RawGreaterOp<DataType>>::raw_min_max_function_with_null;
        }
#else
        min_max_function = RawAggFunctionImpl<DataType, RawGreaterOp<DataType>>::raw_min_max_function_with_null;
#endif
        break;
      default:
        LOG_ERROR_RET(OB_ERR_UNEXPECTED, "Invalid min max type", K(type));
        break;
    }
    return min_max_function;
  }

    static raw_min_max_function_with_null_bitmap produce_min_max_with_null_bitmap_for_cs(
    const uint32_t type)
  {
    raw_min_max_function_with_null_bitmap min_max_function = nullptr;
    typedef typename ObEncodingTypeInference<IS_SIGNED, LEN_TAG>::Type DataType;
    const bool is_supported = is_arch_supported(ObTargetArch::AVX2);
    switch (type) {
      case 0: // min
#if OB_USE_MULTITARGET_CODE
        if (is_supported) {
          min_max_function = RawAggFunctionImpl<DataType, RawLessOp<DataType>>::raw_min_max_function_with_null_bitmap_avx2;
        } else {
          min_max_function = RawAggFunctionImpl<DataType, RawLessOp<DataType>>::raw_min_max_function_with_null_bitmap;
        }
#else
        min_max_function = RawAggFunctionImpl<DataType, RawLessOp<DataType>>::raw_min_max_function_with_null_bitmap;
#endif
        break;
      case 1:
#if OB_USE_MULTITARGET_CODE
        if (is_supported) {
          min_max_function = RawAggFunctionImpl<DataType, RawGreaterOp<DataType>>::raw_min_max_function_with_null_bitmap_avx2;
        } else {
          min_max_function = RawAggFunctionImpl<DataType, RawGreaterOp<DataType>>::raw_min_max_function_with_null_bitmap;
        }
#else
        min_max_function = RawAggFunctionImpl<DataType, RawGreaterOp<DataType>>::raw_min_max_function_with_null_bitmap;
#endif
        break;
      default:
        LOG_ERROR_RET(OB_ERR_UNEXPECTED, "Invalid min max type", K(type));
        break;
    }
    return min_max_function;
  }
};

// For cs encoding, all value is unsigned
RawAggFunctionFactory::RawAggFunctionFactory()
{
  for (uint32_t k = 0; k < MIN_MAX_CNT; ++k) {
    min_max_functions_array_[0][k] = RawAggFunctionProducer<0, 0>::produce_min_max(k);
    min_max_functions_array_[1][k] = RawAggFunctionProducer<0, 1>::produce_min_max(k);
    min_max_functions_array_[2][k] = RawAggFunctionProducer<0, 2>::produce_min_max(k);
    min_max_functions_array_[3][k] = RawAggFunctionProducer<0, 3>::produce_min_max(k);

    cs_min_max_functions_with_null_array_[0][k] = RawAggFunctionProducer<0, 0>::produce_min_max_with_null_for_cs(k);
    cs_min_max_functions_with_null_array_[1][k] = RawAggFunctionProducer<0, 1>::produce_min_max_with_null_for_cs(k);
    cs_min_max_functions_with_null_array_[2][k] = RawAggFunctionProducer<0, 2>::produce_min_max_with_null_for_cs(k);
    cs_min_max_functions_with_null_array_[3][k] = RawAggFunctionProducer<0, 3>::produce_min_max_with_null_for_cs(k);

    cs_min_max_functions_with_null_bitmap_array_[0][k] = RawAggFunctionProducer<0, 0>::produce_min_max_with_null_bitmap_for_cs(k);
    cs_min_max_functions_with_null_bitmap_array_[1][k] = RawAggFunctionProducer<0, 1>::produce_min_max_with_null_bitmap_for_cs(k);
    cs_min_max_functions_with_null_bitmap_array_[2][k] = RawAggFunctionProducer<0, 2>::produce_min_max_with_null_bitmap_for_cs(k);
    cs_min_max_functions_with_null_bitmap_array_[3][k] = RawAggFunctionProducer<0, 3>::produce_min_max_with_null_bitmap_for_cs(k);
  }
}

RawAggFunctionFactory &RawAggFunctionFactory::instance()
{
  static RawAggFunctionFactory ret;
  return ret;
}

raw_min_max_function RawAggFunctionFactory::get_min_max_function(
    const int32_t fix_len_tag,
    const bool is_min)
{
  raw_min_max_function min_max_function = nullptr;
  if (OB_UNLIKELY(fix_len_tag < 0 || fix_len_tag >= FIX_LEN_TAG_CNT)) {
  } else {
    min_max_function = min_max_functions_array_[fix_len_tag][is_min ? 0 : 1];
  }
  return min_max_function;
}

raw_min_max_function_with_null RawAggFunctionFactory::get_cs_min_max_function_with_null(
    const int32_t fix_len_tag,
    const bool is_min)
{
  raw_min_max_function_with_null min_max_function = nullptr;
  if (OB_UNLIKELY(fix_len_tag < 0 || fix_len_tag >= FIX_LEN_TAG_CNT)) {
  } else {
    min_max_function = cs_min_max_functions_with_null_array_[fix_len_tag][is_min ? 0 : 1];
  }
  return min_max_function;
}

raw_min_max_function_with_null_bitmap RawAggFunctionFactory::get_cs_min_max_function_with_null_bitmap(
    const int32_t fix_len_tag,
    const bool is_min)
{
  raw_min_max_function_with_null_bitmap min_max_function = nullptr;
  if (OB_UNLIKELY(fix_len_tag < 0 || fix_len_tag >= FIX_LEN_TAG_CNT)) {
  } else {
    min_max_function = cs_min_max_functions_with_null_bitmap_array_[fix_len_tag][is_min ? 0 : 1];
  }
  return min_max_function;
}

} // end namespace blocksstable
} // end namespace oceanbase

