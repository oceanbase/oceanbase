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

#include "ob_dict_decoder.h"
#include "storage/blocksstable/ob_block_sstable_struct.h"
#include "ob_bit_stream.h"

namespace oceanbase
{
namespace blocksstable
{
using namespace common;
const ObColumnHeader::Type ObDictDecoder::type_;

template <int32_t REF_BYTE_TAG, bool IS_SIGNED_SC, int32_t STORE_LEN_TAG,
    int32_t DATUM_LEN_TAG, int32_t STORE_CLASS>
struct DictFixBatchDecodeFunc_T
{
  static void dict_fix_batch_decode_func(
      const char *ref_data, const char *base_data,
      const int64_t fixed_len,
      const int64_t dict_cnt,
      const int64_t *row_ids, const int64_t row_cap,
      common::ObDatum *datums)
  {
    typedef typename ObEncodingTypeInference<false, REF_BYTE_TAG>::Type RefType;
    const RefType *ref_array = reinterpret_cast<const RefType *>(ref_data);
    for (int64_t i = 0; i < row_cap; i++) {
      ObDatum &datum = datums[i];
      const int64_t ref = ref_array[row_ids[i]];
      if (ref >= dict_cnt) {
        datum.set_null();
      } else {
        if (0 == STORE_CLASS) {
          ENCODING_ADAPT_MEMCPY(
              const_cast<char *>(datum.ptr_), base_data + fixed_len * ref, fixed_len);
        } else if (1 == STORE_CLASS) {
          datum.ptr_ = base_data + fixed_len * ref;
        }
        datum.pack_ = fixed_len;
      }
    }
  }
};

template <int32_t REF_BYTE_TAG, bool IS_SIGNED_SC, int32_t STORE_LEN_TAG, int32_t DATUM_LEN_TAG>
struct DictFixBatchDecodeFunc_T<REF_BYTE_TAG, IS_SIGNED_SC, STORE_LEN_TAG, DATUM_LEN_TAG, 2>
{
  static void dict_fix_batch_decode_func(
      const char *ref_data, const char *base_data,
      const int64_t fixed_len,
      const int64_t dict_cnt,
      const int64_t *row_ids, const int64_t row_cap,
      common::ObDatum *datums)
  {
    UNUSED(fixed_len);
    typedef typename ObEncodingTypeInference<false, REF_BYTE_TAG>::Type RefType;
    typedef typename ObEncodingTypeInference<IS_SIGNED_SC, STORE_LEN_TAG>::Type StoreType;
    typedef typename ObEncodingTypeInference<IS_SIGNED_SC, DATUM_LEN_TAG>::Type DatumType;
    const RefType *ref_array = reinterpret_cast<const RefType *>(ref_data);
    const StoreType *input = reinterpret_cast<const StoreType *>(base_data);
    for (int64_t i = 0; i < row_cap; i++) {
      ObDatum &datum = datums[i];
      const int64_t ref = ref_array[row_ids[i]];
      if (ref >= dict_cnt) {
        datum.set_null();
      } else {
        *reinterpret_cast<DatumType *>(const_cast<char *>(datum.ptr_)) = input[ref];
        datum.pack_ = sizeof(DatumType);
      }
    }
  }
};

template <int32_t REF_BYTE, int32_t OFF_BYTE, int32_t STORE_CLASS>
struct DictVarBatchDecodeFunc_T
{
  static void dict_var_batch_decode_func(const char *ref_data,
                              const char *off_data,
                              const char *base_data,
                              const char *base_data_end,
                              const int64_t dict_cnt,
                              const int64_t *row_ids, const int64_t row_cap,
                              common::ObDatum *datums)
  {
    typedef typename ObEncodingByteLenMap<false, REF_BYTE>::Type RefType;
    typedef typename ObEncodingByteLenMap<false, OFF_BYTE>::Type OffType;
    const RefType *ref_array = reinterpret_cast<const RefType *>(ref_data);
    const OffType *off_array = reinterpret_cast<const OffType *>(off_data);
    for (int64_t i = 0; i < row_cap; i++) {
      ObDatum &datum = datums[i];
      const int64_t ref = ref_array[row_ids[i]];
      if (ref >= dict_cnt) {
        datum.set_null();
      } else {
        int64_t offset = (0 == ref) ? 0 : off_array[ref - 1];
        int64_t data_len = (ref == dict_cnt - 1)
            ? (base_data_end - base_data) - offset
            : off_array[ref] - offset;

        if (0 == STORE_CLASS) {
          ENCODING_ADAPT_MEMCPY(const_cast<char *>(datum.ptr_), base_data + offset, data_len);
        } else {
          datum.ptr_ = base_data + offset;
        }
        datum.pack_ = data_len;
      }
    }
  }
};

// Initialize fast batch decode func family while compiling
static ObMultiDimArray_T<dict_var_batch_decode_func, 3, 3, 2> dict_var_batch_decode_funcs;

template<int32_t REF_BYTE, int32_t OFF_BYTE, int32_t STORE_CLASS>
struct DictVarDecoderArrayInit
{
  bool operator()()
  {
    dict_var_batch_decode_funcs[REF_BYTE][OFF_BYTE][STORE_CLASS]
        = &(DictVarBatchDecodeFunc_T<REF_BYTE, OFF_BYTE, STORE_CLASS>::dict_var_batch_decode_func);
    return true;
  }
};

static bool dict_var_batch_decode_funcs_inited
    = ObNDArrayIniter<DictVarDecoderArrayInit, 3, 3, 2>::apply();


static ObMultiDimArray_T<dict_fix_batch_decode_func, 2, 2, 4, 4, 3> dict_fix_batch_decode_funcs;

template <int32_t REF_BYTE_TAG, int32_t IS_SIGNED_SC, int32_t STORE_LEN_TAG,
    int32_t DATUM_LEN_TAG, int32_t STORE_CLASS>
struct DictFixDecoderArrayInit
{
  bool operator()()
  {
    dict_fix_batch_decode_funcs[REF_BYTE_TAG][IS_SIGNED_SC][STORE_LEN_TAG][DATUM_LEN_TAG][STORE_CLASS]
        = &(DictFixBatchDecodeFunc_T<REF_BYTE_TAG, IS_SIGNED_SC, STORE_LEN_TAG,
                                    DATUM_LEN_TAG, STORE_CLASS>::dict_fix_batch_decode_func);
    return true;
  }
};

static bool dict_fix_batch_decode_funcs_inited
    = ObNDArrayIniter<DictFixDecoderArrayInit, 2, 2, 4, 4, 3>::apply();

ObMultiDimArray_T<dict_cmp_ref_func, 3, 6> dict_cmp_ref_funcs;

bool init_dict_cmp_ref_simd_funcs();
bool init_dict_cmp_ref_neon_simd_funcs();

template <int32_t REF_LEN, int32_t CMP_TYPE>
struct DictCmpRefArrayInit
{
  bool operator()()
  {
    dict_cmp_ref_funcs[REF_LEN][CMP_TYPE]
        = &(DictCmpRefFunc_T<REF_LEN, CMP_TYPE>::dict_cmp_ref_func);
    return true;
  }
};

bool init_dict_cmp_ref_funcs()
{
  bool res = false;
  res = ObNDArrayIniter<DictCmpRefArrayInit, 3, 6>::apply();
  // Dispatch simd version cmp funcs
#if defined ( __x86_64__ )
  if (is_avx512_valid()) {
    res = init_dict_cmp_ref_simd_funcs();
  }
#elif defined ( __aarch64__ ) && defined ( __ARM_NEON )
    res = init_dict_cmp_ref_neon_simd_funcs();
#endif
  return res;
}

bool dict_cmp_ref_funcs_inited = init_dict_cmp_ref_funcs();

int ObDictDecoder::init(const common::ObObjType &store_obj_type, const char *meta_header)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited())) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else {
    store_class_ = get_store_class_map()[ob_obj_type_class(store_obj_type)];
    if (ObIntTC == ob_obj_type_class(store_obj_type)) {
      int64_t type_store_size = get_type_size_map()[store_obj_type];
      integer_mask_ = ~INTEGER_MASK_TABLE[type_store_size];
    } else {
      integer_mask_ = 0;
    }
    meta_header_ = reinterpret_cast<const ObDictMetaHeader *>(meta_header);

    if (!meta_header_->is_fix_length_dict()) {
      var_data_ = meta_header_->payload_ + (meta_header_->count_ - 1) * meta_header_->index_byte_;
    }
  }
  return ret;
}

int ObDictDecoder::decode(ObColumnDecoderCtx &ctx, ObObj &cell, const int64_t row_id,
    const ObBitStream &bs, const char *data, const int64_t len) const
{
  UNUSEDx(ctx, data, len, bs);
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_inited())) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(NULL == data || len < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(data), K(len));
  } else {
    int64_t ref = 0;
    const unsigned char *col_data = reinterpret_cast<unsigned char *>(
        const_cast<ObDictMetaHeader *>(meta_header_)) + ctx.col_header_->length_;

    if (cell.get_meta() != ctx.obj_meta_) {
      cell.set_meta_type(ctx.obj_meta_);
    }
    // get ref value
    if (ctx.is_bit_packing()) {
      if (OB_FAIL(ObBitStream::get(col_data, row_id * meta_header_->row_ref_size_,
          meta_header_->row_ref_size_, ref))) {
        LOG_WARN("failed to get bit packing value",
            K(ret), K(row_id), K_(*meta_header), K(ref));
      }
    } else {
      MEMCPY(&ref, col_data + row_id * meta_header_->row_ref_size_,
          meta_header_->row_ref_size_);
    }

    if (OB_FAIL(ret)) {
    } else if (decode(ctx.obj_meta_, cell, ref, ctx.col_header_->length_)) {
      LOG_WARN("failed to decode dict", K(ret), K(ref));
    }

  }

  return ret;
}

int ObDictDecoder::decode(ObObjMeta cell_meta, ObObj &cell, const int64_t ref, const int64_t meta_length) const
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_inited())) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(0 > ref) || OB_UNLIKELY(0 > meta_length)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(ref), K(meta_length));
  } else {
    const char *cell_data = NULL;
    int64_t cell_len = 0;
    const int64_t count = meta_header_->count_;

    // 1. get offset and length
    if (ref >= count || 0 == count) {
      // null or nope
    } else if (meta_header_->is_fix_length_dict()) {
      cell_data = meta_header_->payload_ + ref * meta_header_->data_size_;
      cell_len = meta_header_->data_size_;
    } else {
      const ObIntArrayFuncTable &off_array = ObIntArrayFuncTable::instance(meta_header_->index_byte_);
      int64_t offset = 0;
      if (0 != ref) { // not first, the first offset is 0
        offset = off_array.at_(meta_header_->payload_, ref - 1);
      }
      cell_data = var_data_ + offset;

      if (ref == count - 1) { // the last one
        cell_len = reinterpret_cast<const char *>(meta_header_) + meta_length - cell_data;
      } else {
        cell_len = off_array.at_(meta_header_->payload_, ref) - offset;
      }
    }

    // 2. fill data
    if (OB_SUCC(ret)) {
      if (ref >= count) {
        if (ref == count) {
          cell.set_null();
        } else if (ref == count + 1) {
          cell.set_ext(ObActionFlag::OP_NOP);
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpcted ref", K(ret), K(ref), K(count));
        }
      } else {
        if (OB_FAIL(load_data_to_obj_cell(cell_meta, cell_data, cell_len, cell))) {
          LOG_WARN("Failed to load data to object cell", K(ret));
        }
      }
    }
  }

  return ret;
}

#define DICT_UNPACK_REFS(row_ids, row_cap, col_data, datums, ref_size, unpack_type) \
  int64_t row_id = 0; \
  int64_t ref = 0; \
  int64_t bs_len = meta_header_->count_ * ref_size; \
  for (int64_t i = 0; i < row_cap; ++i) { \
    row_id = row_ids[i]; \
    ObBitStream::get<unpack_type>(col_data, row_id * row_ref_size, row_ref_size, bs_len, ref); \
    datums[i].pack_ = static_cast<uint32_t>(ref); \
  }

#define DICT_GET_NULL_COUNT(dict_count, row_ids, row_cap, col_data, ref_size, null_count, unpack_type) \
  int64_t row_id = 0; \
  int64_t ref = 0; \
  int64_t bs_len = meta_header_->count_ * ref_size; \
  for (int64_t i = 0; i < row_cap; ++i) { \
    row_id = row_ids[i]; \
    ObBitStream::get<unpack_type>(col_data, row_id * row_ref_size, row_ref_size, bs_len, ref); \
    if (ref >= dict_count) { \
      null_count++; \
    } \
  }

int ObDictDecoder::batch_get_bitpacked_refs(
    const int64_t *row_ids,
    const int64_t row_cap,
    const unsigned char *col_data,
    common::ObDatum *datums) const
{
  int ret = OB_SUCCESS;
  const uint8_t row_ref_size = meta_header_->row_ref_size_;
  if (row_ref_size < 10) {
    DICT_UNPACK_REFS(
        row_ids, row_cap, col_data, datums, row_ref_size, ObBitStream::PACKED_LEN_LESS_THAN_10)
  } else if (row_ref_size < 26) {
    DICT_UNPACK_REFS(
        row_ids, row_cap, col_data, datums, row_ref_size, ObBitStream::PACKED_LEN_LESS_THAN_26)
  } else if (row_ref_size <= 64) {
    DICT_UNPACK_REFS(
        row_ids, row_cap, col_data, datums, row_ref_size, ObBitStream::DEFAULT)
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unpack size larger than 64 bit", K(ret), K(row_ref_size));
  }
  return ret;
}

int ObDictDecoder::batch_get_null_count(
    const int64_t *row_ids,
    const int64_t row_cap,
    const unsigned char *col_data,
    int64_t &null_count) const
{
  int ret = OB_SUCCESS;
  const int64_t dict_count = meta_header_->count_;
  const uint8_t row_ref_size = meta_header_->row_ref_size_;
  if (row_ref_size < 10) {
    DICT_GET_NULL_COUNT(
        dict_count, row_ids, row_cap, col_data, row_ref_size, null_count, ObBitStream::PACKED_LEN_LESS_THAN_10)
  } else if (row_ref_size < 26) {
    DICT_GET_NULL_COUNT(
        dict_count, row_ids, row_cap, col_data, row_ref_size, null_count, ObBitStream::PACKED_LEN_LESS_THAN_26)
  } else if (row_ref_size <= 64) {
    DICT_GET_NULL_COUNT(
        dict_count, row_ids, row_cap, col_data, row_ref_size, null_count, ObBitStream::DEFAULT)
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unpack size larger than 64 bit", K(ret), K(row_ref_size));
  }
  return ret;
}

#undef DICT_UNPACK_REFS
#undef DICT_GET_NULL_COUNT

// Internal call, not check parameters for performance
int ObDictDecoder::batch_decode(
    const ObColumnDecoderCtx &ctx,
    const ObIRowIndex* row_index,
    const int64_t *row_ids,
    const char **cell_datas,
    const int64_t row_cap,
    common::ObDatum *datums) const
{
  UNUSED(row_index);
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited())) {
    ret = OB_NOT_INIT;
    LOG_WARN("Not init", K(ret));
  } else if (fast_decode_valid(ctx)) {
    const ObObjType store_obj_type = ctx.col_header_->get_store_obj_type();
    const ObObjTypeStoreClass store_class = get_store_class_map()[ob_obj_type_class(store_obj_type)];

    const char *ref_data = reinterpret_cast<char *>(
        const_cast<ObDictMetaHeader *>(meta_header_)) + ctx.col_header_->length_;
    const char *base_data = meta_header_->payload_;
    if (meta_header_->is_fix_length_dict()) {
      // Only need store_len for UIntSC/IntSC
      const int64_t store_size = meta_header_->data_size_;
      const ObObjDatumMapType map_type = ObDatum::get_obj_datum_map_type(store_obj_type);
      bool read_as_signed_data = ObIntSC == store_class
                                && store_size == get_type_size_map()[store_obj_type];
      const int64_t func_entry_store_size_idx = (store_class == ObIntSC || store_class == ObUIntSC)
                                              ? store_size : 0;
      dict_fix_batch_decode_func decode_func = dict_fix_batch_decode_funcs
                      [get_value_len_tag_map()[meta_header_->row_ref_size_]]
                      [read_as_signed_data]
                      [get_value_len_tag_map()[func_entry_store_size_idx]]
                      [get_value_len_tag_map()[get_datum_store_len(map_type)]]
                      [get_store_class_tag_map()[store_class]];
      decode_func(ref_data, base_data, store_size, meta_header_->count_, row_ids, row_cap, datums);
      LOG_DEBUG("[batch_decode] Run fix dict fast batch decode",
          K(ret), K(store_class), K(store_size), K(map_type), K(store_obj_type),
          K(read_as_signed_data), K(func_entry_store_size_idx));
    } else {
      dict_var_batch_decode_func decode_func = dict_var_batch_decode_funcs
                      [meta_header_->row_ref_size_]
                      [meta_header_->index_byte_]
                      [get_store_class_tag_map()[store_class]];
      decode_func(ref_data, base_data, var_data_,
               reinterpret_cast<const char *>(meta_header_) + ctx.col_header_->length_,
               meta_header_->count_, row_ids, row_cap, datums);
      LOG_DEBUG("[batch_decode] Run var dict fast batch decode", K(ret),
          K(meta_header_->row_ref_size_), K(store_class), K(meta_header_->index_byte_));
    }
  } else {
    // Batch read ref to datum.len_
    const unsigned char *col_data = reinterpret_cast<unsigned char *>(
        const_cast<ObDictMetaHeader *>(meta_header_)) + ctx.col_header_->length_;
    const uint8_t row_ref_size = meta_header_->row_ref_size_;
    int64_t row_id = 0;
    if (ctx.is_bit_packing()) {
      if (OB_FAIL(batch_get_bitpacked_refs(row_ids, row_cap, col_data, datums))) {
        LOG_WARN("Failed to batch unpack bitpacked value", K(ret));
      }
    } else {
      for (int64_t i = 0; i < row_cap; ++i) {
        row_id = row_ids[i];
        datums[i].pack_ = 0;
        MEMCPY(&datums[i].pack_, col_data + row_id * row_ref_size, row_ref_size);
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(batch_decode_dict(
        ctx.col_header_->get_store_obj_type(),
        cell_datas,
        row_cap,
        ctx.col_header_->length_,
        datums))) {
      LOG_WARN("Failed to batch decode referenes from dict", K(ret), K(ctx));
    }
  }
  return ret;
}

// Internal call, not check parameters for performance
// datums[i].len_ should stand for the reference to dictionary as input parameter
int ObDictDecoder::batch_decode_dict(
    const common::ObObjType &obj_type,
    const char **cell_datas,
    const int64_t row_cap,
    const int64_t meta_length,
    common::ObDatum *datums) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited())) {
    ret = OB_NOT_INIT;
    LOG_WARN("Not init", K(ret));
  } else {
    const int64_t count = meta_header_->count_;
    const char *dict_payload = meta_header_->payload_;

    // Batch locate cell_data
    if (meta_header_->is_fix_length_dict()) {
      const int64_t dict_data_size = meta_header_->data_size_;
      for (int64_t i = 0; i < row_cap; ++i) {
        uint32_t ref = datums[i].pack_;
        if (ref >= count) {
          datums[i].set_null();
        } else {
          int64_t data_offset = ref * dict_data_size;
          cell_datas[i] = dict_payload + data_offset;
          datums[i].pack_ = static_cast<uint32_t>(dict_data_size);
        }
      }
    } else {
      const ObIntArrayFuncTable &off_array
          = ObIntArrayFuncTable::instance(meta_header_->index_byte_);
      const char* end_of_col_meta = reinterpret_cast<const char *>(meta_header_) + meta_length;
      int32_t var_dict_data_len_arr[count];

      var_dict_data_len_arr[0] = off_array.at_(dict_payload, 0);
      int32_t last_dict_element_offset = 1 == count ? 0 : off_array.at_(dict_payload, 0);
      for (int64_t i = 1; i < count - 1; ++i) {
        int32_t curr_dict_element_offset = off_array.at_(dict_payload, i);
        var_dict_data_len_arr[i] = curr_dict_element_offset - last_dict_element_offset;
        last_dict_element_offset = curr_dict_element_offset;
      }
      var_dict_data_len_arr[count - 1] = end_of_col_meta - (var_data_ + last_dict_element_offset);
      for (int64_t i = 0; i < row_cap; ++i) {
        int64_t offset = 0;
        uint32_t ref = datums[i].pack_;
        if (ref < count) {
          // 0 if ref == 0
          offset = off_array.at_(dict_payload - meta_header_->index_byte_, ref)
              & (static_cast<int64_t>(ref == 0) - 1);
          cell_datas[i] = var_data_ + offset;
          datums[i].pack_ = var_dict_data_len_arr[ref];
        } else {
          datums[i].set_null();
        }
      }
    }

    // Batch load data
    if (OB_FAIL(batch_load_data_to_datum(
        obj_type,
        cell_datas,
        row_cap,
        integer_mask_,
        datums))) {
      LOG_WARN("Failed to batch load data to datum", K(ret));
    }
  }
  return ret;
}

bool ObDictDecoder::fast_decode_valid(const ObColumnDecoderCtx &ctx) const
{
  bool valid = false;
  const ObObjTypeStoreClass store_class =
      get_store_class_map()[ob_obj_type_class(ctx.col_header_->get_store_obj_type())];
  if (meta_header_->is_fix_length_dict()) {
    valid = !ctx.is_bit_packing()
          && meta_header_->row_ref_size_ <= 2
          && (store_class >= ObIntSC && store_class <= ObStringSC)
          && dict_fix_batch_decode_funcs_inited;
    if (ObIntSC == store_class || ObUIntSC == store_class) {
      uint16_t store_size = meta_header_->data_size_;
      valid = valid && (store_size == 1 || store_size == 2 || store_size == 4 || store_size == 8);
    }
  } else {
    valid = !ctx.is_bit_packing()
            && (ObNumberSC == store_class || ObStringSC == store_class)
            && meta_header_->index_byte_ <= 2
            && meta_header_->row_ref_size_ <= 2
            && dict_var_batch_decode_funcs_inited;
  }
  return valid;
}

int ObDictDecoder::get_null_count(
    const ObColumnDecoderCtx &ctx,
    const ObIRowIndex *row_index,
    const int64_t *row_ids,
    const int64_t row_cap,
    int64_t &null_count) const
{
  UNUSED(row_index);
  int ret = OB_SUCCESS;
  null_count = 0;
  if (OB_UNLIKELY(!is_inited())) {
    ret = OB_NOT_INIT;
    LOG_WARN("Dict decoder not inited", K(ret));
  } else {
    const int64_t dict_count = meta_header_->count_;
    const unsigned char *col_data = reinterpret_cast<unsigned char *>(
        const_cast<ObDictMetaHeader *>(meta_header_)) + ctx.col_header_->length_;
    const uint8_t row_ref_size = meta_header_->row_ref_size_;
    int64_t row_id = 0;
    uint64_t ref = 0;
    if (ctx.is_bit_packing()) {
      if (OB_FAIL(batch_get_null_count(row_ids, row_cap, col_data, null_count))) {
        LOG_WARN("Failed to batch unpack bitpacked value", K(ret));
      }
    } else {
      for (int64_t i = 0; i < row_cap; ++i) {
        row_id = row_ids[i];
        ref = 0;
        MEMCPY(&ref, col_data + row_id * row_ref_size, row_ref_size);
        if (ref >= dict_count) {
          null_count++;
        }
      }
    }
  }
  return ret;
}

int ObDictDecoder::update_pointer(const char *old_block, const char *cur_block)
{
  int ret = OB_SUCCESS;
  if (!is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_ISNULL(old_block) || OB_ISNULL(cur_block)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(old_block), KP(cur_block));
  } else {
    ObIColumnDecoder::update_pointer(meta_header_, old_block, cur_block);
    ObIColumnDecoder::update_pointer(var_data_, old_block, cur_block);
  }
  return ret;
}

/**
 * Filter push down operator for dict encoding.
 * Now we assume most of the micro blocks are small and most of the dictionaries are small
 * (if too large, might use another encoding), so the cost of traverse a column is less than
 * allocate an array to store the rows hit the filter conditions.
 *
 * But this might be unrealistic and could bring bottleneck to the pushdown.
 */
int ObDictDecoder::pushdown_operator(
    const sql::ObPushdownFilterExecutor *parent,
    const ObColumnDecoderCtx &col_ctx,
    const sql::ObWhiteFilterExecutor &filter,
    const char* meta_data,
    const ObIRowIndex* row_index,
    ObBitmap &result_bitmap) const
{
  UNUSEDx(meta_data, row_index);
  int ret = OB_SUCCESS;
  const sql::ObWhiteFilterOperatorType op_type = filter.get_op_type();
  if (OB_UNLIKELY(!is_inited())) {
    ret = OB_NOT_INIT;
    LOG_WARN("Dictionary decoder is not inited", K(ret));
  }  else if (OB_UNLIKELY(op_type >= sql::WHITE_OP_MAX)){
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid op type for pushed down white filter", K(ret), K(op_type));
  } else {
    const unsigned char *col_data = reinterpret_cast<const unsigned char *>(
        const_cast<ObDictMetaHeader *>(meta_header_)) + col_ctx.col_header_->length_;
    switch (op_type) {
    case sql::WHITE_OP_NU:
    case sql::WHITE_OP_NN: {
      if (OB_FAIL(nu_nn_operator(parent, col_ctx, col_data, filter, result_bitmap))) {
        LOG_WARN("Failed to run NU / NN operator", K(ret), K(col_ctx));
      }
      break;
    }
    case sql::WHITE_OP_EQ:
    case sql::WHITE_OP_NE: {
      if (OB_FAIL(eq_ne_operator(parent, col_ctx, col_data, filter, result_bitmap))) {
        LOG_WARN("Failed to run EQ / NE operator", K(ret), K(col_ctx));
      }
      break;
    }
    case sql::WHITE_OP_GT:
    case sql::WHITE_OP_GE:
    case sql::WHITE_OP_LT:
    case sql::WHITE_OP_LE: {
      if (OB_FAIL(comparison_operator(parent, col_ctx, col_data, filter, result_bitmap))) {
        LOG_WARN("Failed to run GT / LT / GE / NE operator", K(ret), K(col_ctx));
      }
      break;
    }
    case sql::WHITE_OP_IN: {
      if (OB_FAIL(in_operator(parent, col_ctx, col_data, filter, result_bitmap))) {
        LOG_WARN("Failed to run IN operator", K(ret), K(col_ctx));
      }
      break;
    }
    case sql::WHITE_OP_BT: {
      if (OB_FAIL(bt_operator(parent, col_ctx, col_data, filter, result_bitmap))) {
        LOG_WARN("Failed to run BT operator", K(ret), K(col_ctx));
      }
      break;
    }
    default: {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("Unexpected filter pushdown operation type", K(ret), K(op_type));
    }
    } // end of switch
  }
  return ret;
}

int ObDictDecoder::nu_nn_operator(
    const sql::ObPushdownFilterExecutor *parent,
    const ObColumnDecoderCtx &col_ctx,
    const unsigned char* col_data,
    const sql::ObWhiteFilterExecutor &filter,
    ObBitmap &result_bitmap) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(result_bitmap.size() != col_ctx.micro_block_header_->row_count_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument for NU / NN operator", K(ret), K(result_bitmap.size()));
  } else {
    const int64_t count = meta_header_->count_;
    if (count == 0) {
      if (sql::WHITE_OP_NU == filter.get_op_type()) {
        if (OB_FAIL(result_bitmap.bit_not())) {
          LOG_WARN("Failed to bitwise not on result_bitmap", K(ret));
        }
      }
    } else {
      if (OB_FAIL(cmp_ref_and_set_res(parent, col_ctx, count, col_data,
                                      FP_INT_OP_EQ, true, result_bitmap))) {
        LOG_WARN("Failed to compare reference and set result bitmap ",
            K(ret), K(count), K(filter));
      } else if (sql::WHITE_OP_NN == filter.get_op_type()) {
        if (OB_FAIL(result_bitmap.bit_not())) {
          LOG_WARN("Failed to bitwise not on result_bitmap", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObDictDecoder::eq_ne_operator(
    const sql::ObPushdownFilterExecutor *parent,
    const ObColumnDecoderCtx &col_ctx,
    const unsigned char* col_data,
    const sql::ObWhiteFilterExecutor &filter,
    ObBitmap &result_bitmap) const
{
  int ret = OB_SUCCESS;
  const common::ObIArray<common::ObObj> &objs = filter.get_objs();
  if (OB_UNLIKELY(result_bitmap.size() != col_ctx.micro_block_header_->row_count_
                  || objs.count() != 1
                  || filter.null_param_contained())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument for EQ / NE operator", K(ret),
             K(result_bitmap.size()), K(filter));
  } else if (meta_header_->count_ > 0) {
    bool found = false;
    const ObObj &ref_obj = objs.at(0);
    const sql::ObWhiteFilterOperatorType op_type = filter.get_op_type();
    const int64_t ref_bitset_size = meta_header_->count_ + 1;
    char ref_bitset_buf[sql::ObBitVector::memory_size(ref_bitset_size)];
    sql::ObBitVector *ref_bitset = sql::to_bit_vector(ref_bitset_buf);
    ref_bitset->init(ref_bitset_size);
    ObDictDecoderIterator begin_it = begin(&col_ctx, col_ctx.col_header_->length_);
    ObDictDecoderIterator end_it = end(&col_ctx, col_ctx.col_header_->length_);

    int64_t dict_ref = 0;
    ObDictDecoderIterator traverse_it = begin_it;
    bool is_sorted = meta_header_->is_sorted_dict();
    if (is_sorted) {
      traverse_it = std::lower_bound(begin_it, end_it, ref_obj);
      dict_ref = traverse_it - begin_it;
    }
    for (; traverse_it != end_it; ++traverse_it, ++dict_ref)
    {
      if (*traverse_it == ref_obj) {
        found = true;
        ref_bitset->set(dict_ref);
      } else if (is_sorted) {
        break;
      }
    }
    // TODO: @saitong.zst
    // SIMD optimize on sorted dictionary with only one element found
    if (found && OB_FAIL(set_res_with_bitset(parent, col_ctx, col_data, ref_bitset, result_bitmap))) {
      LOG_WARN("Failed to set result bitmap", K(ret));
    }
    if (OB_SUCC(ret) && op_type == sql::WHITE_OP_NE) {
      if (OB_FAIL(result_bitmap.bit_not())) {
        LOG_WARN("Failed to bitwise not on result_bitmap", K(ret));
      } else if (OB_FAIL(cmp_ref_and_set_res(parent, col_ctx, meta_header_->count_, col_data,
                                             FP_INT_OP_EQ, false, result_bitmap))) {
        LOG_WARN("Failed to compare reference and set result bitmap to false",
                 K(ret), K(meta_header_->count_), K(op_type));
      }
    }
  }
  return ret;
}

int ObDictDecoder::comparison_operator(
    const sql::ObPushdownFilterExecutor *parent,
    const ObColumnDecoderCtx &col_ctx,
    const unsigned char* col_data,
    const sql::ObWhiteFilterExecutor &filter,
    ObBitmap &result_bitmap) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(result_bitmap.size() != col_ctx.micro_block_header_->row_count_
                  || filter.get_objs().count() != 1
                  || filter.null_param_contained())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument for GT / LT operator", K(ret), K(col_data),
             K(result_bitmap.size()), K(filter));
  } else {
    const int64_t count = meta_header_->count_;
    if (count > 0) {
      const sql::ObWhiteFilterOperatorType op_type = filter.get_op_type();
      const ObObj &ref_obj = filter.get_objs().at(0);
      ObDictDecoderIterator begin_it = begin(&col_ctx, col_ctx.col_header_->length_);
      ObDictDecoderIterator end_it = end(&col_ctx, col_ctx.col_header_->length_);
      if (meta_header_->is_sorted_dict()) {
        // Sorted dictionary, binary search here to find boundary element
        bool fast_filter_valid = !col_ctx.is_bit_packing()
            && (meta_header_->row_ref_size_ == 2
                || (meta_header_->count_ < UINT8_MAX && meta_header_->row_ref_size_ == 1))
            && dict_cmp_ref_funcs_inited;
        switch (op_type) {
        case sql::WHITE_OP_GT: {
          int64_t bound_ref = std::upper_bound(begin_it, end_it, ref_obj) - begin_it;
          // Greater than or equal to upper bound
          if (fast_filter_valid) {
            if (OB_FAIL(fast_cmp_ref_and_set_res(col_ctx, bound_ref, col_data,
                sql::WHITE_OP_GE, result_bitmap))) {
              LOG_WARN("Failed to comapre reference and set result bitmap ",
                K(ret), K(bound_ref), K(op_type));
            }
          } else if (OB_FAIL(cmp_ref_and_set_res(parent, col_ctx, bound_ref, col_data,
                                          FP_INT_OP_GE, true, result_bitmap))) {
            LOG_WARN("Failed to comapre reference and set result bitmap ",
                K(ret), K(bound_ref), K(op_type));
          }
          break;
        }
        case sql::WHITE_OP_LT: {
          int64_t bound_ref = std::lower_bound(begin_it, end_it, ref_obj) - begin_it;
          // Less Than lower bound
          if (fast_filter_valid) {
            if (OB_FAIL(fast_cmp_ref_and_set_res(col_ctx, bound_ref, col_data,
                sql::WHITE_OP_LT, result_bitmap))) {
              LOG_WARN("Failed to compare reference and set result bitmap",
                  K(ret), K(bound_ref), K(op_type));
            }
          } else if (OB_FAIL(cmp_ref_and_set_res(parent, col_ctx, bound_ref, col_data,
                                          FP_INT_OP_LT, true, result_bitmap))) {
            LOG_WARN("Failed to comapre reference and set result bitmap ",
                K(ret), K(bound_ref), K(op_type));
          }
          break;
        }
        case sql::WHITE_OP_GE: {
          int64_t bound_ref = std::lower_bound(begin_it, end_it, ref_obj) - begin_it;
          // Greater than or equal to lower bound
          if (fast_filter_valid) {
            if (OB_FAIL(fast_cmp_ref_and_set_res(col_ctx, bound_ref, col_data,
                sql::WHITE_OP_GE, result_bitmap))) {
              LOG_WARN("Failed to compare referencee and set result bitmap",
                  K(ret), K(bound_ref), K(op_type));
            }
          } else if (OB_FAIL(cmp_ref_and_set_res(parent, col_ctx, bound_ref, col_data,
                                          FP_INT_OP_GE, true, result_bitmap))) {
            LOG_WARN("Failed to comapre reference and set result bitmap ",
                K(ret), K(bound_ref), K(op_type));
          }
          break;
        }
        case sql::WHITE_OP_LE: {
          int64_t bound_ref = std::upper_bound(begin_it, end_it, ref_obj) - begin_it;
          // Less than upper bound
          if (fast_filter_valid) {
            if (OB_FAIL(fast_cmp_ref_and_set_res(col_ctx, bound_ref, col_data,
                sql::WHITE_OP_LT, result_bitmap))) {
              LOG_WARN("Failed to compare referencee and set result bitmap",
                  K(ret), K(bound_ref), K(op_type));
            }
          } else if (OB_FAIL(cmp_ref_and_set_res(parent, col_ctx, bound_ref, col_data,
                                          FP_INT_OP_LT, true, result_bitmap))) {
            LOG_WARN("Failed to comapre reference and set result bitmap ",
                K(ret), K(bound_ref), K(op_type));
          }
          break;
        }
        default: {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("Unexpected op type for GT / LT / GE / LE pushed down operator",
              K(ret), K(op_type));
        }
        } // end of switch
      } else {
        // Unsorted dictionary, Traverse dictionary
        bool found = false;
        ObDictDecoderIterator traverse_it = begin(&col_ctx, col_ctx.col_header_->length_);
        const int64_t ref_bitset_size = meta_header_->count_ + 1;
        char ref_bitset_buf[sql::ObBitVector::memory_size(ref_bitset_size)];
        sql::ObBitVector *ref_bitset = sql::to_bit_vector(ref_bitset_buf);
        ref_bitset->init(ref_bitset_size);
        int64_t dict_ref = 0;
        while (traverse_it != end_it) {
          if (ObObjCmpFuncs::compare_oper_nullsafe(
                *traverse_it,
                ref_obj,
                ref_obj.get_collation_type(),
                sql::ObPushdownWhiteFilterNode::WHITE_OP_TO_CMP_OP[op_type])) {
            found = true;
            ref_bitset->set(dict_ref);
          }
          ++traverse_it;
          ++dict_ref;
        }
        if (found && OB_FAIL(set_res_with_bitset(parent, col_ctx, col_data, ref_bitset, result_bitmap))) {
          LOG_WARN("Failed to set result bitmap", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObDictDecoder::bt_operator(
    const sql::ObPushdownFilterExecutor *parent,
    const ObColumnDecoderCtx &col_ctx,
    const unsigned char* col_data,
    const sql::ObWhiteFilterExecutor &filter,
    ObBitmap &result_bitmap) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(NULL == col_data
                || result_bitmap.size() != col_ctx.micro_block_header_->row_count_
                || filter.get_objs().count() != 2
                || filter.get_op_type() != sql::WHITE_OP_BT
                || filter.null_param_contained())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument for BT operator", K(ret), K(col_data),
             K(result_bitmap.size()), K(filter));
  } else {
    const int64_t count = meta_header_->count_;
    if (count > 0) {
      ObDictDecoderIterator begin_it = begin(&col_ctx, col_ctx.col_header_->length_);
      ObDictDecoderIterator end_it = end(&col_ctx, col_ctx.col_header_->length_);
      const ObIArray<ObObj> &objs = filter.get_objs();
      if (meta_header_->is_sorted_dict()) {
        ObDictDecoderIterator loc;
        loc = std::lower_bound(begin_it, end_it, objs.at(0));
        int64_t left_bound_inclusive_ref = loc - begin_it;
        loc = std::upper_bound(begin_it, end_it, objs.at(1));
        int64_t right_bound_exclusive_ref = loc - begin_it;

        int64_t ref = 0;
        for (int64_t row_id = 0;
             OB_SUCC(ret) && row_id < col_ctx.micro_block_header_->row_count_;
             ++row_id) {
          if (nullptr != parent && parent->can_skip_filter(row_id)) {
            continue;
          } else if (OB_FAIL(read_ref(row_id, col_ctx.is_bit_packing(), col_data, ref))) {
            LOG_WARN("Failed to read reference for dictionary", K(ret), K(col_data), K(row_id));
          } else {
            if (left_bound_inclusive_ref <= ref && right_bound_exclusive_ref > ref && ref < count) {
              if (OB_FAIL(result_bitmap.set(row_id))) {
                LOG_WARN("Failed to set result bitmap", K(ret), K(row_id), K(filter));
              }
            }
          }
        }
      } else {
        bool found = false;
        ObDictDecoderIterator traverse_it = begin(&col_ctx, col_ctx.col_header_->length_);
        const int64_t ref_bitset_size = meta_header_->count_ + 1;
        char ref_bitset_buf[sql::ObBitVector::memory_size(ref_bitset_size)];
        sql::ObBitVector *ref_bitset = sql::to_bit_vector(ref_bitset_buf);
        ref_bitset->init(ref_bitset_size);
        int64_t dict_ref = 0;
        while (traverse_it != end_it) {
          if (*traverse_it >= objs.at(0) && *traverse_it <= objs.at(1)) {
            found = true;
            ref_bitset->set(dict_ref);
          }
          ++traverse_it;
          ++dict_ref;
        }
        if (found && OB_FAIL(set_res_with_bitset(parent, col_ctx, col_data, ref_bitset, result_bitmap))) {
          LOG_WARN("Failed to set result bitmap", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObDictDecoder::in_operator(
    const sql::ObPushdownFilterExecutor *parent,
    const ObColumnDecoderCtx &col_ctx,
    const unsigned char* col_data,
    const sql::ObWhiteFilterExecutor &filter,
    ObBitmap &result_bitmap) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(result_bitmap.size() != col_ctx.micro_block_header_->row_count_
                  || filter.get_objs().count() == 0
                  || filter.get_op_type() != sql::WHITE_OP_IN
                  || filter.null_param_contained())) {
    LOG_WARN("Invalid argument for BT operator", K(ret),
             K(col_data), K(result_bitmap.size()), K(filter));
  } else {
    const int64_t count = meta_header_->count_;
    if (count > 0) {
      bool found = false;
      ObDictDecoderIterator traverse_it = begin(&col_ctx, col_ctx.col_header_->length_);
      ObDictDecoderIterator end_it = end(&col_ctx, col_ctx.col_header_->length_);
      const int64_t ref_bitset_size = meta_header_->count_ + 1;
      char ref_bitset_buf[sql::ObBitVector::memory_size(ref_bitset_size)];
      sql::ObBitVector *ref_bitset = sql::to_bit_vector(ref_bitset_buf);
      ref_bitset->init(ref_bitset_size);
      int64_t dict_ref = 0;
      bool is_exist = false;
      while (OB_SUCC(ret) && traverse_it != end_it) {
        if (OB_FAIL(filter.exist_in_obj_set(*traverse_it, is_exist))) {
          LOG_WARN("Failed to check object in hashset", K(ret), K(*traverse_it));
        } else if (is_exist) {
          found = true;
          ref_bitset->set(dict_ref);
        }
        ++traverse_it;
        ++dict_ref;
      }
      if (found && OB_FAIL(set_res_with_bitset(parent, col_ctx, col_data, ref_bitset, result_bitmap))) {
        LOG_WARN("Failed to set result bitmap", K(ret));
      }
    }
  }
  return ret;
}

int ObDictDecoder::load_data_to_obj_cell(
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
    case ObGeometrySC: {
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

int ObDictDecoder::cmp_ref_and_set_res(
    const sql::ObPushdownFilterExecutor *parent,
    const ObColumnDecoderCtx &col_ctx,
    const int64_t dict_ref,
    const unsigned char *col_data,
    ObFPIntCmpOpType cmp_op,
    bool flag,
    ObBitmap &result_bitmap) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(col_data)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid Argument", K(ret));
  } else {
    int64_t ref = 0;
    for (int64_t row_id = 0;
        OB_SUCC(ret) && row_id < col_ctx.micro_block_header_->row_count_;
        ++row_id) {
      if (nullptr != parent && parent->can_skip_filter(row_id)) {
        continue;
      } else if (OB_FAIL(read_ref(row_id, col_ctx.is_bit_packing(), col_data, ref))) {
        LOG_WARN("Failed to read reference for dictionary", K(ret), K(col_data), K(row_id));
      } else if (fp_int_cmp<int64_t>(ref, dict_ref, cmp_op)
                 && OB_LIKELY(ref < meta_header_->count_ || FP_INT_OP_EQ == cmp_op)) {
        if (OB_FAIL(result_bitmap.set(row_id, flag))) {
          LOG_WARN("Failed to set result bitmap", K(ret), K(row_id), K(ref), K(dict_ref));
        }
      }
    }
  }
  return ret;
}

int ObDictDecoder::fast_cmp_ref_and_set_res(
    const ObColumnDecoderCtx &col_ctx,
    const int64_t dict_ref,
    const unsigned char *col_data,
    const sql::ObWhiteFilterOperatorType op_type,
    ObBitmap &result_bitmap) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(col_data) || OB_UNLIKELY(meta_header_->row_ref_size_ > 2)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret), KP(col_data), K(meta_header_->row_ref_size_));
  } else {
    int64_t cnt = col_ctx.micro_block_header_->row_count_;
    int64_t size = sql::ObBitVector::memory_size(cnt);
    char buf[size];
    sql::ObBitVector *bit_vec = sql::to_bit_vector(buf);
    bit_vec->reset(cnt);

    dict_cmp_ref_func func = dict_cmp_ref_funcs[meta_header_->row_ref_size_][op_type];
    func(col_ctx.micro_block_header_->row_count_, dict_ref,
        meta_header_->count_, col_data, *bit_vec);

    if (OB_FAIL(result_bitmap.load_blocks_from_array(reinterpret_cast<uint64_t *>(buf), cnt))) {
      LOG_WARN("Failed to load result bitmap from array", K(ret));
    }
    LOG_DEBUG("[PUSHDOWN] fast compare reference and set result bitmap",
        K(ret), K(result_bitmap.popcnt()), KPC(meta_header_), K(op_type));
  }
  return ret;
}

int ObDictDecoder::set_res_with_bitset(
    const sql::ObPushdownFilterExecutor *parent,
    const ObColumnDecoderCtx &col_ctx,
    const unsigned char *col_data,
    const sql::ObBitVector *ref_bitset,
    ObBitmap &result_bitmap) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(col_data)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid Argument", K(ret));
  } else {
    int64_t ref = 0;
    for (int64_t row_id = 0;
        OB_SUCC(ret) && row_id < col_ctx.micro_block_header_->row_count_;
        ++row_id) {
      if (nullptr != parent && parent->can_skip_filter(row_id)) {
        continue;
      } else if (OB_FAIL(read_ref(row_id, col_ctx.is_bit_packing(), col_data, ref))) {
        LOG_WARN("Failed to read reference for dictionary", K(ret), K(col_data), K(row_id));
      } else {
        if (ref_bitset->exist(ref)) {
          if (OB_FAIL(result_bitmap.set(row_id))) {
            LOG_WARN("Failed to set result bitmap", K(ret), K(row_id), K(ref));
          }
        }
      }
    }
  }
  return ret;
}

ObDictDecoderIterator ObDictDecoder::begin(
    const ObColumnDecoderCtx *ctx,
    int64_t meta_length) const
{
  return ObDictDecoderIterator(this, ctx, 0, meta_length);
}
ObDictDecoderIterator ObDictDecoder::end(const ObColumnDecoderCtx *ctx, int64_t meta_length) const
{
  return ObDictDecoderIterator(this, ctx, meta_header_->count_, meta_length);
}

} // end namespace blocksstable
} // end namespace oceanbase
