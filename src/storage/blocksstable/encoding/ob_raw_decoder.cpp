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
#include "lib/hash/ob_hashset.h"

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
// STORE_CLASS: {0: ObNumberSC, 1: ObStringSC, 2: ObIntSC/ObUIntSC}
template <int32_t ROW_IDX_BYTE, bool IS_ONLY_VAR_COL,
    bool IS_ZERO_HEADER_LEN, bool IS_LAST_VAR_COL, int32_t STORE_CLASS>
struct RawVarBatchDecodeFunc_T
{
  static void raw_var_batch_decode_func(
      const char *base_data, const char *row_idx_data,
      const int64_t header_off,
      const int64_t header_len,
      const int64_t header_var_col_cnt,
      const int64_t *row_ids, const int64_t row_cap,
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

// STORE_CLASS: {0: ObNumberSC, 1: ObStringSC, 2: ObIntSC/ObUIntSC}
template <bool IS_SIGNED_SC, int32_t STORE_LEN_TAG, int32_t DATUM_LEN_TAG, int32_t STORE_CLASS>
struct RawFixBatchDecodeFunc_T
{
  static void raw_fix_batch_decode_func(
      const int64_t col_len,
      const char *base_data,
      const int64_t *row_ids,
      const int64_t row_cap,
      common::ObDatum *datums)
  {
    for (int64_t i = 0; i < row_cap; i++) {
      const int64_t row_id = row_ids[i];
      ObDatum &datum = datums[i];
      if (STORE_CLASS == 1) {
        datum.ptr_ = base_data + col_len * row_id;
      } else if (STORE_CLASS == 0) {
        ENCODING_ADAPT_MEMCPY(
            const_cast<char *>(datum.ptr_), base_data + row_id * col_len, col_len);
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
      const int64_t *row_ids,
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

int ObRawDecoder::decode(ObColumnDecoderCtx &ctx, common::ObObj &cell, const int64_t row_id,
    const ObBitStream &bs, const char *data, const int64_t len) const
{
  UNUSEDx(row_id);
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
    set_stored_ext_value(cell, static_cast<ObStoredExtValue>(val));
  } else {
    if (cell.get_meta() != ctx.obj_meta_) {
      cell.set_meta_type(ctx.obj_meta_);
    }
    // read bit packing value
    if (ctx.is_bit_packing()) {
      val = 0;
      if (OB_FAIL(ObBitStream::get(col_data, data_offset + row_id * ctx.col_header_->length_,
          ctx.col_header_->length_, val))) {
        LOG_WARN("get bit packing value failed", K(ret), K(ctx));
      } else {
        cell.v_.uint64_ = val;
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
      // fill data
      if (OB_FAIL(load_data_to_obj_cell(ctx.obj_meta_, cell_data, cell_len, cell))) {
        LOG_WARN("Failed to load data to object cell", K(ret), K(cell));
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
    const int64_t *row_ids,
    const char **cell_datas,
    const int64_t row_cap,
    common::ObDatum *datums) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited())) {
    ret = OB_NOT_INIT;
    LOG_WARN("Raw decoder not inited", K(ret));
  } else if (fast_decode_valid(ctx)) {
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
        && (ObNumberSC == store_class || ObStringSC == store_class)
        && raw_var_batch_decode_funcs_inited;
  }
  return valid;
}

// private call, not check parameters
int ObRawDecoder::batch_decode_general(
    const ObColumnDecoderCtx &ctx,
    const ObIRowIndex* row_index,
    const int64_t *row_ids,
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
  } else if (OB_FAIL(get_is_null_bitmap(col_ctx, col_data, row_index, result_bitmap))) {
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
      if (fast_filter_valid(col_ctx, filter.get_objs().at(0).meta_.get_type(), fix_len_tag, is_signed_data)) {
        if (OB_FAIL(fast_comparison_operator(col_ctx, col_data,
          filter, fix_len_tag, is_signed_data, result_bitmap))) {
          LOG_WARN("Failed on fast comparison operator", K(ret), K(col_ctx));
        }
      } else {
        if (OB_FAIL(comparison_operator(parent, col_ctx, col_data, row_index,
                    filter, result_bitmap))) {
          LOG_WARN("Failed on Comparison Operator", K(ret), K(col_ctx));
        }
      }
      break;
    }
    case sql::WHITE_OP_IN: {
      if (OB_FAIL(in_operator(parent, col_ctx, col_data, row_index,
                  filter, result_bitmap))) {
        LOG_WARN("Failed on In Operator", K(ret), K(col_ctx));
      }
      break;
    }
    case sql::WHITE_OP_BT: {
      if (OB_FAIL(bt_operator(parent, col_ctx, col_data, row_index,
                  filter, result_bitmap))) {
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

bool ObRawDecoder::fast_filter_valid(
    const ObColumnDecoderCtx &ctx,
    const ObObjType &filter_value_type,
    int32_t &fix_length_tag,
    bool &is_signed_data) const
{
  bool valid = !ctx.has_extend_value()
              && !ctx.is_bit_packing()
              && ctx.is_fix_length()
              && raw_fix_fast_filter_funcs_inited
              && ctx.col_header_->get_store_obj_type() == filter_value_type;
  // vectorized binary filter on situation that filter object type different with
  // column stored type not supported yet
  if (valid) {
    int64_t fix_len = ctx.col_header_->length_;
    if (fix_len != 1 && fix_len != 2 && fix_len != 4 && fix_len != 8) {
      valid = false;
    } else {
      fix_length_tag = get_value_len_tag_map()[fix_len];
    }
  }
  if (valid) {
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
    default: {
      valid = false;
    }
    }
  }
  return valid;
}

int ObRawDecoder::get_null_count(
    const ObColumnDecoderCtx &ctx,
    const ObIRowIndex *row_index,
    const int64_t *row_ids,
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
    ObBitmap &result_bitmap) const
{
  int ret = OB_SUCCESS;
  // Get is null bitmap from encoding data
  if (OB_UNLIKELY(col_ctx.micro_block_header_->row_count_ != result_bitmap.size()
                         || NULL == col_data
                         || NULL == row_index)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Filter Pushdown Operator: invalid argument", K(ret), K(result_bitmap.size()));
  } else if (col_ctx.is_fix_length() || col_ctx.is_bit_packing()) {
    if (OB_FAIL(get_is_null_bitmap_from_fixed_column(col_ctx, col_data, result_bitmap))) {
      LOG_WARN("Failed to get isnull bitmap from fixed column", K(ret));
    }
  } else {
    if (OB_FAIL(get_is_null_bitmap_from_var_column(col_ctx, row_index, result_bitmap))) {
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
    ObBitmap &result_bitmap) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(col_ctx.micro_block_header_->row_count_ != result_bitmap.size()
                  || NULL == col_data
                  || NULL == row_index
                  || filter.get_objs().count() != 1)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Filter pushdown operator: Invalid argument", K(ret), K(col_ctx), K(row_index));
  } else if (OB_FAIL(traverse_all_data(parent, col_ctx, row_index, col_data,
                                       filter, result_bitmap,
                                       [](const ObObj &cur_obj,
                                          const sql::ObWhiteFilterExecutor &filter,
                                          bool &result) -> int {
                                       result = ObObjCmpFuncs::compare_oper_nullsafe(
                                           cur_obj,
                                           filter.get_objs().at(0),
                                           cur_obj.get_collation_type(),
                                           sql::ObPushdownWhiteFilterNode::WHITE_OP_TO_CMP_OP[
                                           filter.get_op_type()]);
                                       return OB_SUCCESS;
                                       }))) {
    LOG_WARN("Failed to traverse all data in micro block", K(ret));
  }
  return ret;
}

// No null value for fast comparison operator
int ObRawDecoder::fast_comparison_operator(
    const ObColumnDecoderCtx &col_ctx,
    const unsigned char* col_data,
    const sql::ObWhiteFilterExecutor &filter,
    int32_t fix_len_tag,
    bool is_signed_data,
    ObBitmap &result_bitmap) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(col_ctx.micro_block_header_->row_count_ != result_bitmap.size()
                         || NULL == col_data
                         || filter.get_objs().count() != 1
                         || fix_len_tag > 3)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Filter pushdown operator: Invalid argument", K(ret), K(col_ctx), K(fix_len_tag));
  } else {
    const int64_t type_store_size = get_type_size_map()[col_ctx.col_header_->get_store_obj_type()];
    const uint64_t node_value = filter.get_objs().at(0).v_.uint64_;
    const int64_t node_store_size = get_type_size_map()[filter.get_objs().at(0).meta_.get_type()];
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
        if ((~(INTEGER_MASK_TABLE[node_store_size] >> 1)) & node_value) {
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
      int64_t cnt = col_ctx.micro_block_header_->row_count_;
      int64_t size = sql::ObBitVector::memory_size(cnt);
      // Use BitVector to set the result of filter here because the memory of ObBitMap is not continuous
      char buf[size];
      sql::ObBitVector *bit_vec = sql::to_bit_vector(buf);
      bit_vec->reset(cnt);

      fix_filter_func fast_filter_func = raw_fix_fast_filter_funcs
          [is_signed_data && type_store_size == col_ctx.col_header_->length_]
          [fix_len_tag]
          [op_type];
      fast_filter_func(
        col_ctx.micro_block_header_->row_count_, col_data, node_value, *bit_vec);

      // convert bit vector to bitmap
      if (OB_FAIL(result_bitmap.load_blocks_from_array(reinterpret_cast<uint64_t *>(buf), cnt))) {
        LOG_WARN("Failed to load bitmap from array on stack", K(ret), KP(buf), K(cnt));
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
    ObBitmap &result_bitmap) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(col_ctx.micro_block_header_->row_count_ != result_bitmap.size()
                         || NULL == col_data
                         || NULL == row_index
                         || filter.get_objs().count() != 2)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Filter pushdown operator: Invalid arguments", K(ret), K(result_bitmap.size()));
  } else if (OB_FAIL(traverse_all_data(parent, col_ctx, row_index, col_data,
                    filter, result_bitmap,
                    [](const ObObj &cur_obj,
                      const sql::ObWhiteFilterExecutor &filter,
                      bool &result) -> int {
                      result = (cur_obj >= filter.get_objs().at(0))
                                && (cur_obj <= filter.get_objs().at(1));
                      return OB_SUCCESS;
                    }))) {
    LOG_WARN("Failed to traverse all data in micro block", K(ret));
  }
  return ret;
}

int ObRawDecoder::in_operator(
    const sql::ObPushdownFilterExecutor *parent,
    const ObColumnDecoderCtx &col_ctx,
    const unsigned char* col_data,
    const ObIRowIndex* row_index,
    const sql::ObWhiteFilterExecutor &filter,
    ObBitmap &result_bitmap) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(filter.get_objs().count() == 0
             || result_bitmap.size() != col_ctx.micro_block_header_->row_count_
             || NULL == row_index)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Pushdown in operator: Invalid arguments", K(ret), K(filter.get_objs()));
  } else if (OB_FAIL(traverse_all_data(parent, col_ctx, row_index, col_data,
                    filter, result_bitmap,
                    [](const ObObj &cur_obj,
                      const sql::ObWhiteFilterExecutor &filter,
                      bool &result) -> int {
                      int ret = OB_SUCCESS;
                      if (OB_FAIL(filter.exist_in_obj_set(cur_obj, result))) {
                        LOG_WARN("Failed to check object in hashset", K(ret), K(cur_obj));
                      }
                      return ret;
                    }))) {
    LOG_WARN("Failed to traverse all data in micro block", K(ret));
  }
  return ret;
}

/**
 *  Function to traverse all row data with raw encoding, regardless of column is fixed length
 *  or var lengthand run lambda function for every row element.
 */
int ObRawDecoder::traverse_all_data(
    const sql::ObPushdownFilterExecutor *parent,
    const ObColumnDecoderCtx &col_ctx,
    const ObIRowIndex* row_index,
    const unsigned char* col_data,
    const sql::ObWhiteFilterExecutor &filter,
    ObBitmap &result_bitmap,
    int (*lambda)(
        const ObObj &cur_obj,
        const sql::ObWhiteFilterExecutor &filter,
        bool &result)) const
{
  int ret = OB_SUCCESS;
  // Initialize object and data pointers
  ObObj cur_obj;
  cur_obj.copy_meta_type(col_ctx.obj_meta_);
  cur_obj.v_.uint64_ = 0;
  int64_t data_offset = 0;
  if (col_ctx.has_extend_value()
      && (col_ctx.is_fix_length() || col_ctx.is_bit_packing())) {
    data_offset = col_ctx.micro_block_header_->row_count_
        * col_ctx.micro_block_header_->extend_value_bit_;
  }
  bool null_value_contained = (result_bitmap.popcnt() > 0);
  if (col_ctx.is_bit_packing()) {
    uint64_t value = 0;
    for (int64_t row_id = 0;
         row_id < col_ctx.micro_block_header_->row_count_;
         ++row_id) {
      if (nullptr != parent && parent->can_skip_filter(row_id)) {
        continue;
      } else if (null_value_contained && result_bitmap.test(row_id)) {
        // object in this row is null
        if (OB_FAIL(result_bitmap.set(row_id, false))) {
          LOG_WARN("Failed to set null value to false", K(ret));
        }
      } else if (OB_FAIL(ObBitStream::get(
                  col_data,
                  data_offset + row_id * col_ctx.col_header_->length_,
                  col_ctx.col_header_->length_,
                  value))) {
        LOG_WARN("Failed to read bit packing object data from bitstream", K(ret), K(col_ctx));
      } else {
        cur_obj.v_.uint64_ = value;
        bool result = false;
        if (OB_FAIL(lambda(cur_obj, filter, result))) {
          LOG_WARN("Failed on trying to filter the row", K(ret), K(row_id), K(cur_obj));
        } else if (result) {
          if (OB_FAIL(result_bitmap.set(row_id))) {
            LOG_WARN("Failed to set result bitmap", K(ret), K(row_id), K(filter));
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
    for (int64_t row_id = 0;
        OB_SUCC(ret) && row_id < col_ctx.micro_block_header_->row_count_;
        ++row_id) {
      if (nullptr != parent && parent->can_skip_filter(row_id)) {
        continue;
      } else if (null_value_contained && result_bitmap.test(row_id)) {
        // object in this row is null
        if (OB_FAIL(result_bitmap.set(row_id, false))) {
          LOG_WARN("Failed to set null value to false", K(ret));
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
        } else if (OB_FAIL(load_data_to_obj_cell(col_ctx.obj_meta_, cell_data, cell_len, cur_obj))) {
          LOG_WARN("Failed to load data to object cell", K(ret), K(cell_data), K(cell_len));
        } else {
          // Padding for non-bitpacking data if required
          if (cur_obj.is_fixed_len_char_type() && nullptr != col_ctx.col_param_) {
            if (OB_FAIL(storage::pad_column(col_ctx.col_param_->get_accuracy(),
                                            *col_ctx.allocator_, cur_obj))) {
              LOG_WARN("Failed to pad column", K(ret));
            }
          }
        }
        if (OB_SUCC(ret)) {
          // Run lambda here to filter out the data according to op_type
          bool result = false;
          if (OB_FAIL(lambda(cur_obj, filter, result))) {
            LOG_WARN("Failed on trying to filter the row", K(ret), K(row_id), K(cur_obj));
          } else if (result) {
            if (OB_FAIL(result_bitmap.set(row_id))) {
              LOG_WARN("Failed to set result bitmap", K(ret), K(row_id), K(filter));
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObRawDecoder::load_data_to_obj_cell(
    const ObObjMeta cell_meta,
    const char *cell_data,
    int64_t cell_len,
    ObObj &load_obj) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(cell_data)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Null pointer of cell data for loading data to object cell",
             K(ret), K(cell_data), K(cell_len));
  } else {
    switch (store_class_) {
    case ObIntSC:
    case ObUIntSC: {
      load_obj.v_.uint64_ = 0;
      MEMCPY(&load_obj.v_, cell_data, cell_len);
      // Cast signed integer to int64_t manually since we use ObObj::v_int64_ for comparing.
      if (0 != integer_mask_ && (load_obj.v_.uint64_ & (integer_mask_ >> 1))) {
        load_obj.v_.uint64_ |= integer_mask_;
      }
      break;
    }
    case ObNumberSC: {
      load_obj.nmb_desc_.desc_ = *reinterpret_cast<const uint32_t*>(cell_data);
      load_obj.v_.nmb_digits_ = reinterpret_cast<uint32_t*>(
              const_cast<char*>(cell_data) + sizeof(uint32_t));
      break;
    }
    case ObStringSC:
    case ObTextSC: 
    case ObJsonSC:
    case ObGeometrySC:
    { // json and text storage class have the same behavior currently
      load_obj.val_len_ = static_cast<int32_t>(cell_len);
      load_obj.v_.string_ = cell_data;
      break;
    }
    case ObOTimestampSC:
    case ObIntervalSC: {
      ObStorageDatum tmp_datum; // TODO: remove

      ObObjDatumMapType datum_type = ObDatum::get_obj_datum_map_type(cell_meta.get_type());
      const uint32_t size = ObDatum::get_reserved_size(datum_type);
      MEMCPY(const_cast<char *>(tmp_datum.ptr_), cell_data, size);
      tmp_datum.len_ = size;
      if (OB_FAIL(tmp_datum.to_obj(load_obj, cell_meta))) {
        LOG_WARN("Failed to read datum", K(ret));
      }
      break;
    }
    default: {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("Unexpected store class", K(ret), K_(store_class));
    }
    }
  }
  return ret;
}

} // end namespace blocksstable
} // end namespace oceanbase

