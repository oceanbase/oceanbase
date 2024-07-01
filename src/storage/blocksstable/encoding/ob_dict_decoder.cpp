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
#include "ob_raw_decoder.h"
#include "ob_vector_decode_util.h"
#include "storage/blocksstable/ob_block_sstable_struct.h"
#include "storage/access/ob_pushdown_aggregate.h"
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
      const int32_t *row_ids, const int64_t row_cap,
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
          ENCODING_ADAPT_MEMCPY(const_cast<char *>(datum.ptr_), base_data + fixed_len * ref, fixed_len);
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
      const int32_t *row_ids, const int64_t row_cap,
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
                              const int32_t *row_ids, const int64_t row_cap,
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

int ObDictDecoder::decode(const ObColumnDecoderCtx &ctx, ObDatum &datum, const int64_t row_id,
    const ObBitStream &bs, const char *data, const int64_t len) const
{
  UNUSEDx(data, len, bs);
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
    } else if (OB_FAIL(decode(ctx.obj_meta_.get_type(), datum, ref, ctx.col_header_->length_))) {
      LOG_WARN("failed to decode dict", K(ret), K(ref));
    }

  }

  return ret;
}

int ObDictDecoder::decode(const common::ObObjType &obj_type, common::ObDatum &datum, const int64_t ref, const int64_t meta_length) const
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
    if (OB_FAIL(ret)) {
    } else if (ref >= count) {
      if (ref == count) {
        datum.set_null();
      } else if (ref == count + 1) {
        datum.set_ext();
        datum.no_cv(datum.extend_obj_)->set_ext(common::ObActionFlag::OP_NOP);
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpcted ref", K(ret), K(ref), K(count));
      }
    } else {
      if (OB_FAIL(load_data_to_datum(
          obj_type,
          cell_data,
          cell_len,
          integer_mask_,
          datum))) {
        LOG_WARN("Failed to load data to datum", K(ret));
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
    const int32_t *row_ids,
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
    const int32_t *row_ids,
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
    const int32_t *row_ids,
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
            && (ObNumberSC == store_class || ObDecimalIntSC == store_class || ObStringSC == store_class)
            && meta_header_->index_byte_ <= 2
            && meta_header_->row_ref_size_ <= 2
            && dict_var_batch_decode_funcs_inited;
  }
  return valid;
}

template<bool HAS_NULL>
int ObDictDecoder::batch_decode_dict(
    const common::ObObjMeta &schema_obj_meta,
    const common::ObObjType &stored_obj_type,
    const int64_t meta_length,
    ObVectorDecodeCtx &vector_ctx) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited())) {
    ret = OB_NOT_INIT;
    LOG_WARN("Not init", K(ret));
  } else {
    int64_t fixed_packing_len = 0;
    const int64_t count = meta_header_->count_;
    const char *dict_payload = meta_header_->payload_;
    if (meta_header_->is_fix_length_dict()) {
      const uint32_t dict_data_size = meta_header_->data_size_;
      fixed_packing_len = dict_data_size;
      for (int64_t i = 0; i < vector_ctx.row_cap_; ++i) {
        const int64_t curr_vec_offset = vector_ctx.vec_offset_ + i;
        const uint32_t ref = vector_ctx.len_arr_[i];
        if (HAS_NULL && ref == count) {
          vector_ctx.ptr_arr_[i] = nullptr;
        } else {
          const int64_t data_offset = ref * dict_data_size;
          vector_ctx.ptr_arr_[i] = dict_payload + data_offset;
          vector_ctx.len_arr_[i] = dict_data_size;
        }
      }
    } else {
      const ObIntArrayFuncTable &off_array = ObIntArrayFuncTable::instance(meta_header_->index_byte_);
      const char* end_of_col_meta = reinterpret_cast<const char *>(meta_header_) + meta_length;
      const uint32_t last_dict_entry_len = reinterpret_cast<const char *>(meta_header_) + meta_length
          - (var_data_ + off_array.at_(dict_payload - meta_header_->index_byte_, count - 1));
      for (int64_t i = 0; i < vector_ctx.row_cap_; ++i) {
        const int64_t curr_vec_offset = vector_ctx.vec_offset_ + i;
        const uint32_t ref = vector_ctx.len_arr_[i];
        if (HAS_NULL && ref == count) {
          vector_ctx.ptr_arr_[i] = nullptr;
        } else {
          const int64_t data_offset = off_array.at_(dict_payload - meta_header_->index_byte_, ref)
              & (static_cast<int64_t>(ref == 0) - 1);
          vector_ctx.ptr_arr_[i] = var_data_ + data_offset;
          // can we remove this condition without modify the storage format ?
          vector_ctx.len_arr_[i] = (ref == count - 1)
              ? last_dict_entry_len
              : off_array.at_(dict_payload - meta_header_->index_byte_, ref + 1) - data_offset;
        }
      }
    }

    DataDiscreteLocator discrete_locator(vector_ctx.ptr_arr_, vector_ctx.len_arr_);
    if (OB_FAIL(ObVecDecodeUtils::load_byte_aligned_vector<DataDiscreteLocator>(
        schema_obj_meta, stored_obj_type, fixed_packing_len, HAS_NULL,
        discrete_locator, vector_ctx.row_cap_, vector_ctx.vec_offset_, vector_ctx.vec_header_))) {
      LOG_WARN("failed to load byte alighed data to vector", K(ret),
          K(schema_obj_meta), K(stored_obj_type));
    }
  }
  return ret;
}

int ObDictDecoder::decode_vector(
    const ObColumnDecoderCtx &decoder_ctx,
    const ObIRowIndex* row_index,
    ObVectorDecodeCtx &vector_ctx) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited())) {
    ret = OB_NOT_INIT;
    LOG_WARN("Not init", K(ret));
  } else if (meta_header_->is_fix_length_dict() && !decoder_ctx.is_bit_packing() && meta_header_->row_ref_size_ <= 2) {
    // TODO: more generialized fast decode
    const char *ref_buf = reinterpret_cast<char *>(
        const_cast<ObDictMetaHeader *>(meta_header_)) + decoder_ctx.col_header_->length_;
    if (1 == meta_header_->row_ref_size_) {
      ObFixedDictDataLocator_T<uint8_t> fixed_dict_locator(vector_ctx.row_ids_, meta_header_->payload_,
        meta_header_->data_size_, meta_header_->count_, ref_buf);
      if (OB_FAIL(ObVecDecodeUtils::load_byte_aligned_vector<ObFixedDictDataLocator_T<uint8_t>>(
          decoder_ctx.obj_meta_, decoder_ctx.col_header_->get_store_obj_type(), meta_header_->data_size_,
          true, fixed_dict_locator, vector_ctx.row_cap_, vector_ctx.vec_offset_, vector_ctx.vec_header_))) {
        LOG_WARN("failed to load byte aligned data to vector", K(ret));
      }
    } else if (2 == meta_header_->row_ref_size_) {
      ObFixedDictDataLocator_T<uint16_t> fixed_dict_locator(vector_ctx.row_ids_, meta_header_->payload_,
        meta_header_->data_size_, meta_header_->count_, ref_buf);
      if (OB_FAIL(ObVecDecodeUtils::load_byte_aligned_vector<ObFixedDictDataLocator_T<uint16_t>>(
          decoder_ctx.obj_meta_, decoder_ctx.col_header_->get_store_obj_type(), meta_header_->data_size_,
          true, fixed_dict_locator, vector_ctx.row_cap_, vector_ctx.vec_offset_, vector_ctx.vec_header_))) {
        LOG_WARN("failed to load byte aligned data to vector", K(ret));
      }
    }
  } else if (!meta_header_->is_fix_length_dict()
      && meta_header_->index_byte_ <= 2
      && meta_header_->row_ref_size_ <= 2
      && !decoder_ctx.is_bit_packing()) {
    const char *ref_data = reinterpret_cast<char *>(const_cast<ObDictMetaHeader *>(meta_header_)) + decoder_ctx.col_header_->length_;
    const char *off_data = meta_header_->payload_;
    const ObIntArrayFuncTable &off_array = ObIntArrayFuncTable::instance(meta_header_->index_byte_);
    const int64_t last_dict_entry_len = reinterpret_cast<const char *>(meta_header_) + decoder_ctx.col_header_->length_
        - (var_data_ + off_array.at_(meta_header_->payload_ - meta_header_->index_byte_, meta_header_->count_ - 1));
    #define VAR_DICT_DECODE_VECTOR_SPEC(ref_type, off_type) \
      ObVarDictDataLocator_T<ref_type, off_type> var_dict_locator_##ref_type##off_type( \
          vector_ctx.row_ids_, var_data_, last_dict_entry_len, meta_header_->count_, ref_data, off_data); \
      ret = ObVecDecodeUtils::load_byte_aligned_vector<ObVarDictDataLocator_T<ref_type, off_type>>( \
          decoder_ctx.obj_meta_, decoder_ctx.col_header_->get_store_obj_type(), 0, true, \
          var_dict_locator_##ref_type##off_type, vector_ctx.row_cap_, vector_ctx.vec_offset_, vector_ctx.vec_header_); \

    if (1 == meta_header_->index_byte_) {
      if (1 == meta_header_->row_ref_size_) {
        VAR_DICT_DECODE_VECTOR_SPEC(uint8_t, uint8_t);
      } else {
        VAR_DICT_DECODE_VECTOR_SPEC(uint16_t, uint8_t);
      }
    } else {
      if (1 == meta_header_->row_ref_size_) {
        VAR_DICT_DECODE_VECTOR_SPEC(uint8_t, uint16_t);
      } else {
        VAR_DICT_DECODE_VECTOR_SPEC(uint16_t, uint16_t);
      }
    }

    if (OB_FAIL(ret)) {
      LOG_WARN("failed to load var dict byte aligned data to vector", K(ret));
    }

    #undef VAR_DICT_DECODE_VECTOR_SPEC
  } else {
    // Batch read ref to datum.len_
    const unsigned char *col_data = reinterpret_cast<unsigned char *>(
        const_cast<ObDictMetaHeader *>(meta_header_)) + decoder_ctx.col_header_->length_;
    const uint8_t row_ref_size = meta_header_->row_ref_size_;
    bitstream_unpack unpack_func = ObBitStream::get_unpack_func(row_ref_size);
    int64_t row_id = 0;
    bool has_null = false;
    if (decoder_ctx.is_bit_packing()) {
      const int64_t bs_len = decoder_ctx.micro_block_header_->row_count_ * row_ref_size;
      for (int64_t i = 0; i < vector_ctx.row_cap_; ++i) {
        const int64_t row_id = vector_ctx.row_ids_[i];
        int64_t unpacked_val = 0;
        unpack_func(col_data, row_id * row_ref_size, row_ref_size, bs_len, unpacked_val);
        vector_ctx.len_arr_[i] = static_cast<uint32_t>(unpacked_val);
        has_null |= vector_ctx.len_arr_[i] == meta_header_->count_;
      }
    } else {
      for (int64_t i = 0; i < vector_ctx.row_cap_; ++i) {
        row_id = vector_ctx.row_ids_[i];
        vector_ctx.len_arr_[i] = 0;
        MEMCPY(&vector_ctx.len_arr_[i], col_data + row_id * row_ref_size, row_ref_size);
        has_null |= vector_ctx.len_arr_[i] == meta_header_->count_;
      }
    }

    if (has_null) {
      ret = batch_decode_dict<true>(decoder_ctx.obj_meta_, decoder_ctx.col_header_->get_store_obj_type(),
          decoder_ctx.col_header_->length_, vector_ctx);
    } else {
      ret = batch_decode_dict<false>(decoder_ctx.obj_meta_, decoder_ctx.col_header_->get_store_obj_type(),
          decoder_ctx.col_header_->length_, vector_ctx);
    }

    if (OB_FAIL(ret)) {
      LOG_WARN("failed to batch decode dict to vector", K(ret));
    }
  }
  return ret;
}

bool ObDictDecoder::fast_eq_ne_operator_valid(
    const int64_t dict_ref_cnt,
    const ObColumnDecoderCtx &col_ctx) const
{
  // TODO(hanling): Optimize when dict_ref_cnt is zero.
  return !col_ctx.is_bit_packing() && 1 == dict_ref_cnt;
}

bool ObDictDecoder::fast_string_equal_valid(
    const ObColumnDecoderCtx &col_ctx,
    const ObDatum &ref_datum) const
{
  return 0 == ref_datum.len_
           && col_ctx.obj_meta_.is_varying_len_char_type()
           && lib::is_mysql_mode()
           && !meta_header_->is_sorted_dict()
           && !meta_header_->is_fix_length_dict()
           && meta_header_->count_ > 0;
}

int ObDictDecoder::get_null_count(
    const ObColumnDecoderCtx &ctx,
    const ObIRowIndex *row_index,
    const int32_t *row_ids,
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
    const sql::PushdownFilterInfo &pd_filter_info,
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
      if (OB_FAIL(nu_nn_operator(parent, col_ctx, col_data,
          filter, pd_filter_info, result_bitmap))) {
        LOG_WARN("Failed to run NU / NN operator", K(ret), K(col_ctx));
      }
      break;
    }
    case sql::WHITE_OP_EQ:
    case sql::WHITE_OP_NE: {
      if (OB_FAIL(eq_ne_operator(parent, col_ctx, col_data,
          filter, pd_filter_info, result_bitmap))) {
        LOG_WARN("Failed to run EQ / NE operator", K(ret), K(col_ctx));
      }
      break;
    }
    case sql::WHITE_OP_GT:
    case sql::WHITE_OP_GE:
    case sql::WHITE_OP_LT:
    case sql::WHITE_OP_LE: {
      if (OB_FAIL(comparison_operator(parent, col_ctx, col_data,
          filter, pd_filter_info, result_bitmap))) {
        LOG_WARN("Failed to run GT / LT / GE / NE operator", K(ret), K(col_ctx));
      }
      break;
    }
    case sql::WHITE_OP_IN: {
      if (OB_FAIL(in_operator(parent, col_ctx, col_data,
          filter, pd_filter_info, result_bitmap))) {
        LOG_WARN("Failed to run IN operator", K(ret), K(col_ctx));
      }
      break;
    }
    case sql::WHITE_OP_BT: {
      if (OB_FAIL(bt_operator(parent, col_ctx, col_data,
          filter, pd_filter_info, result_bitmap))) {
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
    const sql::PushdownFilterInfo &pd_filter_info,
    ObBitmap &result_bitmap) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(result_bitmap.size() != pd_filter_info.count_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument for NU / NN operator", K(ret),
        K(result_bitmap.size()), K(pd_filter_info));
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
                                      sql::WHITE_OP_EQ, true, pd_filter_info, result_bitmap))) {
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
    const sql::PushdownFilterInfo &pd_filter_info,
    ObBitmap &result_bitmap) const
{
  int ret = OB_SUCCESS;
  const common::ObIArray<common::ObDatum> &datums = filter.get_datums();
  if (OB_UNLIKELY(result_bitmap.size() != pd_filter_info.count_
                  || datums.count() != 1
                  || filter.null_param_contained())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument for EQ / NE operator", K(ret),
             K(result_bitmap.size()), K(pd_filter_info), K(filter));
  } else if (meta_header_->count_ > 0) {
    const ObDatum &filter_datum = datums.at(0);
    const sql::ObWhiteFilterOperatorType op_type = filter.get_op_type();
    const int64_t ref_bitset_size = meta_header_->count_ + 1;
    char ref_bitset_buf[sql::ObBitVector::memory_size(ref_bitset_size)];
    sql::ObBitVector *ref_bitset = sql::to_bit_vector(ref_bitset_buf);
    ref_bitset->init(ref_bitset_size);
    int64_t dict_ref_cnt = 0;
    uint64_t cmp_value = 0;
    if (fast_string_equal_valid(col_ctx, filter_datum)) {
      if (OB_FAIL(fast_to_accquire_dict_codes(col_ctx, *ref_bitset, dict_ref_cnt, cmp_value))) {
        LOG_WARN("Failed to accquire dict codes", K(ret));
      }
    } else {
      ObDictDecoderIterator begin_it = begin(&col_ctx, col_ctx.col_header_->length_);
      ObDictDecoderIterator end_it = end(&col_ctx, col_ctx.col_header_->length_);

      ObDatumCmpFuncType cmp_func = filter.cmp_func_;
      int64_t dict_ref = 0;
      ObDictDecoderIterator traverse_it = begin_it;
      bool is_sorted = meta_header_->is_sorted_dict();
      if (is_sorted) {
        traverse_it = std::lower_bound(begin_it, end_it, filter_datum, [&cmp_func, &ret](const ObDatum &datum, const ObDatum &filter_datum)
                                        -> bool {
                                          int cmp_res = 0;
                                          if (OB_FAIL(ret)) {
                                          } else if (OB_FAIL(cmp_func(datum, filter_datum, cmp_res))) {
                                            LOG_WARN("Failed to compare datums", K(ret), K(datum), K(filter_datum));
                                          }
                                          return cmp_res < 0;});
        dict_ref = traverse_it - begin_it;
      }
      int cmp_res = 0;
      for (; OB_SUCC(ret) && traverse_it != end_it; ++traverse_it, ++dict_ref)
      {
        if (OB_FAIL(cmp_func(*traverse_it, filter_datum, cmp_res))) {
            LOG_WARN("Failed to compare datums", K(ret), K(*traverse_it), K(filter_datum));
        } else if (cmp_res == 0) {
          ++dict_ref_cnt;
          cmp_value = dict_ref;
          ref_bitset->set(dict_ref);
        } else if (is_sorted) {
          break;
        }
      }
    }
    if (OB_FAIL(ret)){
    } else if (fast_eq_ne_operator_valid(dict_ref_cnt, col_ctx)) {
      if (OB_FAIL(fast_eq_ne_operator(cmp_value, col_data, filter, pd_filter_info, result_bitmap))) {
        LOG_WARN("Failed to do fast_eq_ne_operator", K(ret), K(cmp_value), K(op_type));
      }
    } else {
      // TODO: @saitong.zst
      // SIMD optimize on sorted dictionary with only one element found
      if (0 < dict_ref_cnt && OB_FAIL(set_res_with_bitset(parent, col_ctx, col_data,
           ref_bitset, pd_filter_info, result_bitmap))) {
        LOG_WARN("Failed to set result bitmap", K(ret));
      }
      if (OB_SUCC(ret) && op_type == sql::WHITE_OP_NE) {
        if (OB_FAIL(result_bitmap.bit_not())) {
          LOG_WARN("Failed to bitwise not on result_bitmap", K(ret));
        } else if (OB_FAIL(cmp_ref_and_set_res(parent, col_ctx, meta_header_->count_, col_data,
                                                sql::WHITE_OP_EQ, false, pd_filter_info, result_bitmap))) {
          LOG_WARN("Failed to compare reference and set result bitmap to false",
                    K(ret), K(meta_header_->count_), K(op_type));
        }
      }
    }
  }
  return ret;
}

int ObDictDecoder::fast_eq_ne_operator(
    const uint64_t cmp_value,
    const unsigned char* col_data,
    const sql::ObWhiteFilterExecutor &filter,
    const sql::PushdownFilterInfo &pd_filter_info,
    ObBitmap &result_bitmap) const
{
  int ret = OB_SUCCESS;
  const uint8_t row_ref_size = meta_header_->row_ref_size_;
  if (OB_UNLIKELY(0 != (row_ref_size & (row_ref_size - 1)))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected row ref size", K(ret), K(row_ref_size));
  } else {
    const int32_t fix_len_tag = get_value_len_tag_map()[row_ref_size];
    if (sql::WHITE_OP_EQ == filter.get_op_type()) {
      raw_compare_function cmp_funtion = RawCompareFunctionFactory::instance().get_cmp_function(
                                           false,
                                           fix_len_tag,
                                           filter.get_op_type());
      if (OB_ISNULL(cmp_funtion)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Unexpected nullptr compare function", K(ret), K(fix_len_tag), K(filter.get_op_type()));
      } else {
        cmp_funtion(col_data, cmp_value, result_bitmap.get_data(), pd_filter_info.start_,
                     pd_filter_info.start_ + pd_filter_info.count_);
      }
    } else {
      raw_compare_function_with_null cmp_funtion = RawCompareFunctionFactory::instance().get_cs_cmp_function_with_null(
                                                     fix_len_tag,
                                                     filter.get_op_type());
      if (OB_ISNULL(cmp_funtion)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Unexpected nullptr compare function", K(ret), K(fix_len_tag), K(filter.get_op_type()));
      } else {
        cmp_funtion(col_data, cmp_value, meta_header_->count_, result_bitmap.get_data(),
                     pd_filter_info.start_, pd_filter_info.start_ + pd_filter_info.count_);
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
    const sql::PushdownFilterInfo &pd_filter_info,
    ObBitmap &result_bitmap) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(result_bitmap.size() != pd_filter_info.count_
                  || filter.get_datums().count() != 1
                  || filter.null_param_contained())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument for GT / LT operator", K(ret), K(col_data),
             K(result_bitmap.size()), K(pd_filter_info), K(filter));
  } else {
    const int64_t count = meta_header_->count_;
    if (count > 0) {
      const sql::ObWhiteFilterOperatorType op_type = filter.get_op_type();
      const ObDatum &filter_datum = filter.get_datums().at(0);
      ObDictDecoderIterator begin_it = begin(&col_ctx, col_ctx.col_header_->length_);
      ObDictDecoderIterator end_it = end(&col_ctx, col_ctx.col_header_->length_);
      ObGetFilterCmpRetFunc get_cmp_ret = get_filter_cmp_ret_func(op_type);
      ObDatumCmpFuncType cmp_func = filter.cmp_func_;
      if (meta_header_->is_sorted_dict()) {
        // Sorted dictionary, binary search here to find boundary element
        bool fast_filter_valid = !col_ctx.is_bit_packing()
            && (meta_header_->row_ref_size_ == 2
                || (meta_header_->count_ < UINT8_MAX && meta_header_->row_ref_size_ == 1))
            && dict_cmp_ref_funcs_inited;
        switch (op_type) {
        case sql::WHITE_OP_GT: {
          int64_t bound_ref = std::upper_bound(begin_it, end_it, filter_datum, [&cmp_func, &ret](const ObDatum &filter_datum, const ObDatum &datum)
                                                -> bool {
                                                  int cmp_res = 0;
                                                  if (OB_FAIL(ret)) {
                                                  } else if (OB_FAIL(cmp_func(datum, filter_datum, cmp_res))) {
                                                    LOG_WARN("Failed to compare datums", K(ret), K(datum), K(filter_datum));
                                                  }
                                                  return cmp_res > 0;}) - begin_it;
          // Greater than or equal to upper bound
          if (OB_FAIL(ret)) {
          } else if (fast_filter_valid) {
            if (OB_FAIL(fast_cmp_ref_and_set_res(col_ctx, bound_ref, col_data,
                sql::WHITE_OP_GE, pd_filter_info, result_bitmap))) {
              LOG_WARN("Failed to comapre reference and set result bitmap ",
                K(ret), K(bound_ref), K(op_type));
            }
          } else if (OB_FAIL(cmp_ref_and_set_res(parent, col_ctx, bound_ref, col_data,
                                          sql::WHITE_OP_GE, true, pd_filter_info, result_bitmap))) {
            LOG_WARN("Failed to comapre reference and set result bitmap ",
                K(ret), K(bound_ref), K(op_type));
          }
          break;
        }
        case sql::WHITE_OP_LT: {
          int64_t bound_ref = std::lower_bound(begin_it, end_it, filter_datum, [&cmp_func, &ret](const ObDatum &datum, const ObDatum &filter_datum)
                                                -> bool {
                                                    int cmp_res = 0;
                                                    if (OB_FAIL(ret)) {
                                                    } else if (OB_FAIL(cmp_func(datum, filter_datum, cmp_res))) {
                                                      LOG_WARN("Failed to compare datums", K(ret), K(datum), K(filter_datum));
                                                    }
                                                    return cmp_res < 0;}) - begin_it;
          // Less Than lower bound
          if (OB_FAIL(ret)) {
          } else if (fast_filter_valid) {
            if (OB_FAIL(fast_cmp_ref_and_set_res(col_ctx, bound_ref, col_data,
                sql::WHITE_OP_LT, pd_filter_info, result_bitmap))) {
              LOG_WARN("Failed to compare reference and set result bitmap",
                  K(ret), K(bound_ref), K(op_type));
            }
          } else if (OB_FAIL(cmp_ref_and_set_res(parent, col_ctx, bound_ref, col_data,
                                          sql::WHITE_OP_LT, true, pd_filter_info, result_bitmap))) {
            LOG_WARN("Failed to comapre reference and set result bitmap ",
                K(ret), K(bound_ref), K(op_type));
          }
          break;
        }
        case sql::WHITE_OP_GE: {
          int64_t bound_ref = std::lower_bound(begin_it, end_it, filter_datum, [&cmp_func, &ret](const ObDatum &datum, const ObDatum &filter_datum)
                                                -> bool {
                                                    int cmp_res = 0;
                                                    if (OB_FAIL(ret)) {
                                                    } else if (OB_FAIL(cmp_func(datum, filter_datum, cmp_res))) {
                                                      LOG_WARN("Failed to compare datums", K(ret), K(datum), K(filter_datum));
                                                    }
                                                    return cmp_res < 0;}) - begin_it;
          // Greater than or equal to lower bound
          if (OB_FAIL(ret)) {
          } else if (fast_filter_valid) {
            if (OB_FAIL(fast_cmp_ref_and_set_res(col_ctx, bound_ref, col_data,
                sql::WHITE_OP_GE, pd_filter_info, result_bitmap))) {
              LOG_WARN("Failed to compare referencee and set result bitmap",
                  K(ret), K(bound_ref), K(op_type));
            }
          } else if (OB_FAIL(cmp_ref_and_set_res(parent, col_ctx, bound_ref, col_data,
                                          sql::WHITE_OP_GE, true, pd_filter_info, result_bitmap))) {
            LOG_WARN("Failed to comapre reference and set result bitmap ",
                K(ret), K(bound_ref), K(op_type));
          }
          break;
        }
        case sql::WHITE_OP_LE: {
          int64_t bound_ref = std::upper_bound(begin_it, end_it, filter_datum, [&cmp_func, &ret](const ObDatum &filter_datum, const ObDatum &datum)
                                                -> bool {
                                                  int cmp_res = 0;
                                                  if (OB_FAIL(ret)) {
                                                  } else if (OB_FAIL(cmp_func(datum, filter_datum, cmp_res))) {
                                                    LOG_WARN("Failed to compare datums", K(ret), K(datum), K(filter_datum));
                                                  }
                                                  return cmp_res > 0;}) - begin_it;
          // Less than upper bound
          if (OB_FAIL(ret)){
          } else if (fast_filter_valid) {
            if (OB_FAIL(fast_cmp_ref_and_set_res(col_ctx, bound_ref, col_data,
                sql::WHITE_OP_LT, pd_filter_info, result_bitmap))) {
              LOG_WARN("Failed to compare referencee and set result bitmap",
                  K(ret), K(bound_ref), K(op_type));
            }
          } else if (OB_FAIL(cmp_ref_and_set_res(parent, col_ctx, bound_ref, col_data,
                                          sql::WHITE_OP_LT, true, pd_filter_info, result_bitmap))) {
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
        int cmp_res = 0;
        while (OB_SUCC(ret) && traverse_it != end_it) {
          if (OB_FAIL(cmp_func(*traverse_it, filter_datum, cmp_res))) {
            LOG_WARN("Failed to compare datums", K(ret), K(*traverse_it), K(filter_datum), K(dict_ref));
          } else if (get_cmp_ret(cmp_res)) {
            found = true;
            ref_bitset->set(dict_ref);
          }
          ++traverse_it;
          ++dict_ref;
        }
        if (OB_SUCC(ret) && found && OB_FAIL(set_res_with_bitset(parent, col_ctx, col_data,
                                                 ref_bitset, pd_filter_info, result_bitmap))) {
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
    const sql::PushdownFilterInfo &pd_filter_info,
    ObBitmap &result_bitmap) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(NULL == col_data
                || result_bitmap.size() != pd_filter_info.count_
                || filter.get_datums().count() != 2
                || filter.get_op_type() != sql::WHITE_OP_BT
                || filter.null_param_contained())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument for BT operator", K(ret), K(col_data),
             K(result_bitmap.size()), K(pd_filter_info), K(filter));
  } else {
    const int64_t count = meta_header_->count_;
    ObDatumCmpFuncType cmp_func = filter.cmp_func_;
    if (count > 0) {
      ObDictDecoderIterator begin_it = begin(&col_ctx, col_ctx.col_header_->length_);
      ObDictDecoderIterator end_it = end(&col_ctx, col_ctx.col_header_->length_);
      const common::ObIArray<common::ObDatum> &datums = filter.get_datums();
      if (meta_header_->is_sorted_dict()) {
        ObDictDecoderIterator loc;

        loc = std::lower_bound(begin_it, end_it, datums.at(0), [&cmp_func, &ret](const ObDatum &datum, const ObDatum &filter_datum)
                                -> bool {
                                  int cmp_res = 0;
                                  if (OB_FAIL(ret)) {
                                  } else if (OB_FAIL(cmp_func(datum, filter_datum, cmp_res))) {
                                    LOG_WARN("Failed to compare datums", K(ret), K(datum), K(filter_datum));
                                  }
                                  return cmp_res < 0;});
        int64_t left_bound_inclusive_ref = loc - begin_it;
        loc = std::upper_bound(begin_it, end_it, datums.at(1), [&cmp_func, &ret](const ObDatum &filter_datum, const ObDatum &datum)
                                -> bool {
                                  int cmp_res = 0;
                                  if (OB_FAIL(ret)) {
                                  } else if (OB_FAIL(cmp_func(datum, filter_datum, cmp_res))) {
                                    LOG_WARN("Failed to compare datums", K(ret), K(datum), K(filter_datum));
                                  }
                                  return cmp_res > 0;});
        int64_t right_bound_exclusive_ref = loc - begin_it;

        int64_t ref = 0;
        int64_t row_id = 0;
        for (int64_t offset = 0; OB_SUCC(ret) && offset < pd_filter_info.count_; ++offset) {
          row_id = offset + pd_filter_info.start_;
          if (nullptr != parent && parent->can_skip_filter(offset)) {
            continue;
          } else if (OB_FAIL(read_ref(row_id, col_ctx.is_bit_packing(), col_data, ref))) {
            LOG_WARN("Failed to read reference for dictionary", K(ret), K(col_data), K(row_id));
          } else {
            if (left_bound_inclusive_ref <= ref && right_bound_exclusive_ref > ref && ref < count) {
              if (OB_FAIL(result_bitmap.set(offset))) {
                LOG_WARN("Failed to set result bitmap",
                    K(ret), K(offset), K(filter));
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
        int left_cmp_res = 0;
        int right_cmp_res = 0;
        while (OB_SUCC(ret) && traverse_it != end_it) {
          if (OB_FAIL(cmp_func(*traverse_it, datums.at(0), left_cmp_res))) {
            LOG_WARN("Failed to compare datums", K(ret), K(*traverse_it), K(datums.at(0)));
          } else if (left_cmp_res < 0) {
          } else if (OB_FAIL(cmp_func(*traverse_it, datums.at(1), right_cmp_res))) {
            LOG_WARN("Failed to compare datums", K(ret), K(*traverse_it), K(datums.at(1)));
          } else if (right_cmp_res <= 0) {
            found = true;
            ref_bitset->set(dict_ref);
          }
          ++traverse_it;
          ++dict_ref;
        }
        if (OB_SUCC(ret) && found && OB_FAIL(set_res_with_bitset(parent, col_ctx, col_data,
            ref_bitset, pd_filter_info, result_bitmap))) {
          LOG_WARN("Failed to set result bitmap", K(ret));
        }
      }
    }
  }
  return ret;
}

// TODO(@wenye): optimize in operator
int ObDictDecoder::in_operator(
    const sql::ObPushdownFilterExecutor *parent,
    const ObColumnDecoderCtx &col_ctx,
    const unsigned char* col_data,
    const sql::ObWhiteFilterExecutor &filter,
    const sql::PushdownFilterInfo &pd_filter_info,
    ObBitmap &result_bitmap) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(result_bitmap.size() != pd_filter_info.count_
                  || filter.get_datums().count() == 0
                  || filter.get_op_type() != sql::WHITE_OP_IN
                  || filter.null_param_contained())) {
    LOG_WARN("Invalid argument for BT operator", K(ret),
             K(col_data), K(result_bitmap.size()), K(pd_filter_info), K(filter));
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
        if (OB_FAIL(filter.exist_in_datum_set(*traverse_it, is_exist))) {
          LOG_WARN("Failed to check object in hashset", K(ret), K(*traverse_it));
        } else if (is_exist) {
          found = true;
          ref_bitset->set(dict_ref);
        }
        ++traverse_it;
        ++dict_ref;
      }
      if (OB_FAIL(ret)) {
      } else if (found && OB_FAIL(set_res_with_bitset(parent, col_ctx, col_data,
          ref_bitset, pd_filter_info, result_bitmap))) {
        LOG_WARN("Failed to set result bitmap", K(ret));
      }
    }
  }
  return ret;
}

#define DICT_CMP_REF_SET_RES(parent, col_ctx, dict_ref, col_data, cmp_op, flag, result_bitmap, type) \
  int64_t row_id = 0; \
  ObGetFilterCmpRetFunc get_cmp_ret = get_filter_cmp_ret_func(cmp_op); \
  for (int64_t offset = 0; OB_SUCC(ret) && offset < pd_filter_info.count_; ++offset) { \
    row_id = offset + pd_filter_info.start_; \
    if (nullptr != parent && parent->can_skip_filter(offset)) { \
      continue; \
    } else if (OB_FAIL(read_ref<type>(row_id, col_data, ref))) { \
      LOG_WARN("Failed to read reference for dictionary", K(ret), K(col_data), K(row_id)); \
    } else if (get_cmp_ret(ref - dict_ref) \
        && OB_LIKELY(ref < meta_header_->count_ || sql::WHITE_OP_EQ == cmp_op)) { \
      if (OB_FAIL(result_bitmap.set(offset, flag))) { \
        LOG_WARN("Failed to set result bitmap", \
            K(ret), K(offset), K(flag), K(pd_filter_info), K(ref), K(dict_ref)); \
      } \
    } \
  }

int ObDictDecoder::cmp_ref_and_set_res(
    const sql::ObPushdownFilterExecutor *parent,
    const ObColumnDecoderCtx &col_ctx,
    const int64_t dict_ref,
    const unsigned char *col_data,
    const sql::ObWhiteFilterOperatorType cmp_op,
    bool flag,
    const sql::PushdownFilterInfo &pd_filter_info,
    ObBitmap &result_bitmap) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(col_data)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid Argument", K(ret));
  } else {
    int64_t ref = 0;
    if (col_ctx.is_bit_packing()) {
      const uint8_t row_ref_size = meta_header_->row_ref_size_;
      if (row_ref_size < 10) {
        DICT_CMP_REF_SET_RES(
            parent, col_ctx, dict_ref, col_data, cmp_op, flag, result_bitmap, PACKED_LEN_LESS_THAN_10);
      } else if(row_ref_size < 26) {
        DICT_CMP_REF_SET_RES(
            parent, col_ctx, dict_ref, col_data, cmp_op, flag, result_bitmap, PACKED_LEN_LESS_THAN_26);
      } else if(row_ref_size <=64) {
        DICT_CMP_REF_SET_RES(
            parent, col_ctx, dict_ref, col_data, cmp_op, flag, result_bitmap, DEFAULT_BIT_PACKED);
      } else {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "Unpack size larger than 64 bit", K(ret), K(row_ref_size));
      }
    } else {
      DICT_CMP_REF_SET_RES(
            parent, col_ctx, dict_ref, col_data, cmp_op, flag, result_bitmap, NOT_BIT_PACKED);
    }
  }
  return ret;
}

int ObDictDecoder::fast_cmp_ref_and_set_res(
    const ObColumnDecoderCtx &col_ctx,
    const int64_t dict_ref,
    const unsigned char *col_data,
    const sql::ObWhiteFilterOperatorType op_type,
    const sql::PushdownFilterInfo &pd_filter_info,
    ObBitmap &result_bitmap) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(col_data) || OB_UNLIKELY(meta_header_->row_ref_size_ > 2)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret), KP(col_data), K(meta_header_->row_ref_size_));
  } else {
    raw_compare_function_with_null cmp_funtion = RawCompareFunctionFactory::instance().get_cs_cmp_function_with_null(
                                                   static_cast<int32_t>(meta_header_->row_ref_size_ - 1),
                                                   op_type);
    if (OB_ISNULL(cmp_funtion)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unexpected nullptr compare function", K_(meta_header_->row_ref_size), K(op_type));
    } else {
      cmp_funtion(col_data, dict_ref, meta_header_->count_, result_bitmap.get_data(),
                   pd_filter_info.start_, pd_filter_info.start_ + pd_filter_info.count_);
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
    const sql::PushdownFilterInfo &pd_filter_info,
    ObBitmap &result_bitmap) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(col_data)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid Argument", K(ret));
  } else {
    int64_t ref = 0;
    int64_t row_id = 0;
    for (int64_t offset = 0; OB_SUCC(ret) && offset < pd_filter_info.count_; ++offset) {
      row_id = offset + pd_filter_info.start_;
      if (nullptr != parent && parent->can_skip_filter(offset)) {
        continue;
      } else if (OB_FAIL(read_ref(row_id, col_ctx.is_bit_packing(), col_data, ref))) {
        LOG_WARN("Failed to read reference for dictionary", K(ret), K(col_data), K(row_id));
      } else {
        if (ref_bitset->exist(ref)) {
          if (OB_FAIL(result_bitmap.set(offset))) {
            LOG_WARN("Failed to set result bitmap", K(ret), K(offset), K(ref));
          }
        }
      }
    }
  }
  return ret;
}

int ObDictDecoder::pushdown_operator(
    const sql::ObPushdownFilterExecutor *parent,
    const ObColumnDecoderCtx &col_ctx,
    sql::ObBlackFilterExecutor &filter,
    const char* meta_data,
    const ObIRowIndex* row_index,
    sql::PushdownFilterInfo &pd_filter_info,
    ObBitmap &result_bitmap,
    bool &filter_applied) const
{
  UNUSED(row_index);
  int ret = OB_SUCCESS;
  filter_applied = false;
  const bool enable_rich_format = filter.get_op().enable_rich_format_;
  if (OB_UNLIKELY(!is_inited())) {
    ret = OB_NOT_INIT;
    LOG_WARN("Dictionary decoder is not inited", K(ret));
  } else if (OB_UNLIKELY(result_bitmap.size() != pd_filter_info.count_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument for pushdown operator", K(ret),
             K(result_bitmap.size()), K(pd_filter_info), K(filter));
  } else if (meta_header_->count_ > pd_filter_info.count_ ||
             meta_header_->count_ > col_ctx.micro_block_header_->row_count_ * 0.8) {
  } else {
    common::ObBitmap *ref_bitmap = nullptr;
    common::ObDatum *datums = nullptr;
    ObSEArray<ObSqlDatumInfo, 16> datum_infos;

    if (OB_FAIL(pd_filter_info.init_bitmap(meta_header_->count_ + 1, ref_bitmap))) {
      LOG_WARN("Fail to init ref bitmap", K(ret), K(meta_header_->count_));
    } else if (OB_FAIL(filter.get_datums_from_column(datum_infos))) {
      LOG_WARN("Failed to get filter column datum_infos", K(ret));
    } else if (OB_UNLIKELY(1 != datum_infos.count() || !datum_infos.at(0).is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unexpected datum infos", K(ret), K(datum_infos));
    } else {
      datums = datum_infos.at(0).datum_ptr_;
    }

    for (int64_t index = 0; OB_SUCC(ret) && index <= meta_header_->count_; ) {
      int64_t upper_bound = MIN(index + pd_filter_info.batch_size_, meta_header_->count_ + 1);
      // use uniform base currently, support new format later
      //if (enable_rich_format && OB_FAIL(storage::init_exprs_vector_header(filter.get_filter_node().column_exprs_, filter.get_op().get_eval_ctx(), upper_bound - index))) {
      //  LOG_WARN("Failed to init exprs vector header", K(ret));
      //  break;
      //}
      for (int64_t dict_ref = index; dict_ref < upper_bound; dict_ref++) {
        datums[dict_ref - index].pack_ = dict_ref;
      }
      if (OB_FAIL(batch_decode_dict(
                  col_ctx.col_header_->get_store_obj_type(),
                  pd_filter_info.cell_data_ptrs_,
                  upper_bound - index,
                  col_ctx.col_header_->length_,
                  datums))) {
        LOG_WARN("Failed to batch decode referenes from dict", K(ret), K(col_ctx));
      } else if (col_ctx.obj_meta_.is_fixed_len_char_type() && nullptr != col_ctx.col_param_ &&
                OB_FAIL(storage::pad_on_datums(col_ctx.col_param_->get_accuracy(),
                                                col_ctx.obj_meta_.get_collation_type(),
                                                *col_ctx.allocator_,
                                                upper_bound - index,
                                                datums))) {
        LOG_WARN("Fail to pad on datums", K(ret), K(col_ctx), K(index), K(upper_bound));
      } else if (OB_FAIL(filter.filter_batch(nullptr, index, upper_bound, *ref_bitmap))) {
        LOG_WARN("failed to filter batch", K(ret), K(index), K(upper_bound));
      } else {
        index = upper_bound;
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(set_res_with_bitmap(parent, col_ctx, ref_bitmap,
                                      pd_filter_info, datums, result_bitmap))) {
        LOG_WARN("Fail to set bitmap", K(ret), K(pd_filter_info));
      } else {
        filter_applied = true;
      }
    }
  }
  return ret;
}

// Not used for now because if the block can not be skipped by monotonicy, the performance will descrease.
// There are two reasons for the decline in performance.
// 1. check_has_null() need to traverse all refs.
// 2. check_skip_by_monotonicity() brings redundant comparisons.
int ObDictDecoder::check_skip_block(
    const ObColumnDecoderCtx &col_ctx,
    sql::ObBlackFilterExecutor &filter,
    sql::PushdownFilterInfo &pd_filter_info,
    ObBitmap &result_bitmap,
    sql::ObBoolMask &bool_mask) const
{
  int ret = OB_SUCCESS;
  bool has_null = false;
  if (!filter.is_monotonic()) {
  } else if (meta_header_->count_ <= DICT_SKIP_THRESHOLD) {
    // Do not skip block when dict count is small.
    // Otherwise, if can not skip block by monotonicity, the performance will decrease.
  } else if (OB_FAIL(check_has_null(col_ctx, col_ctx.col_header_->length_, has_null))) {
    LOG_WARN("Failed to check has null", K(ret));
  } else if (meta_header_->is_sorted_dict()) {
    ObStorageDatum min_datum = *begin(&col_ctx, col_ctx.col_header_->length_);
    ObStorageDatum max_datum = *(end(&col_ctx, col_ctx.col_header_->length_) - 1);
    if (OB_FAIL(check_skip_by_monotonicity(filter,
                                           min_datum,
                                           max_datum,
                                           *pd_filter_info.skip_bit_,
                                           has_null,
                                           &result_bitmap,
                                           bool_mask))) {
      LOG_WARN("Failed to check can skip by monotonicity", K(ret), K(min_datum), K(max_datum), K(filter));
    }
  }
  return ret;
}

int ObDictDecoder::set_res_with_bitmap(
    const sql::ObPushdownFilterExecutor *parent,
    const ObColumnDecoderCtx &col_ctx,
    const common::ObBitmap *ref_bitmap,
    const sql::PushdownFilterInfo &pd_filter_info,
    common::ObDatum *datums,
    ObBitmap &result_bitmap) const
{
  int ret = OB_SUCCESS;
  const unsigned char *col_data = reinterpret_cast<const unsigned char *>(
      const_cast<ObDictMetaHeader *>(meta_header_)) + col_ctx.col_header_->length_;
  int64_t ref = 0;
  int64_t row_id = 0;
  for (int64_t offset = 0; OB_SUCC(ret) && offset < pd_filter_info.count_; ++offset) {
    row_id = offset + pd_filter_info.start_;
    if (nullptr != parent && parent->can_skip_filter(offset)) {
      continue;
    } else if (OB_FAIL(read_ref(row_id, col_ctx.is_bit_packing(), col_data, ref))) {
      LOG_WARN("Failed to read reference for dictionary", K(ret), K(col_data), K(row_id));
    } else {
      if (ref_bitmap->test(ref)) {
        if (OB_FAIL(result_bitmap.set(offset))) {
          LOG_WARN("Failed to set result bitmap", K(ret), K(offset), K(ref));
        }
      }
    }
  }
  return ret;
}

int ObDictDecoder::fast_to_accquire_dict_codes(
    const ObColumnDecoderCtx &col_ctx,
    sql::ObBitVector &ref_bitset,
    int64_t &dict_ref_cnt,
    uint64_t &cmp_value) const
{
  int ret = OB_SUCCESS;
  const uint32_t offset_bytes = meta_header_->index_byte_;
  switch (offset_bytes) {
    case 1:
      empty_strings_equal<0>(col_ctx.col_header_->length_, ref_bitset, dict_ref_cnt, cmp_value);
      break;
    case 2:
      empty_strings_equal<1>(col_ctx.col_header_->length_, ref_bitset, dict_ref_cnt, cmp_value);
      break;
    case 4:
      empty_strings_equal<2>(col_ctx.col_header_->length_, ref_bitset, dict_ref_cnt, cmp_value);
      break;
    case 8:
      empty_strings_equal<3>(col_ctx.col_header_->length_, ref_bitset, dict_ref_cnt, cmp_value);
      break;
    default:
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unexpected offset bytes", K(ret), K(offset_bytes));
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

int ObDictDecoder::get_distinct_count(int64_t &distinct_count) const
{
  int ret = OB_SUCCESS;
  distinct_count = meta_header_->count_;
  LOG_DEBUG("[GROUP BY PUSHDOWN]", K(meta_header_->count_));
  return ret;
}

int ObDictDecoder::read_distinct(
    const ObColumnDecoderCtx &ctx,
    const char **cell_datas,
    storage::ObGroupByCell &group_by_cell) const
{
  int ret = OB_SUCCESS;
  bool has_null = false;
  if (OB_FAIL(batch_read_distinct(ctx, cell_datas, ctx.col_header_->length_, group_by_cell))) {
    LOG_WARN("Failed to batch read distinct", K(ret));
  } else if (OB_FAIL(check_has_null(ctx, ctx.col_header_->length_, has_null))) {
    LOG_WARN("Failed to check has null", K(ret));
  } else if (has_null) {
    group_by_cell.add_distinct_null_value();
  }
  return ret;
}

int ObDictDecoder::batch_read_distinct(
    const ObColumnDecoderCtx &ctx,
    const char **cell_datas,
    const int64_t meta_length,
    storage::ObGroupByCell &group_by_cell) const
{
  int ret = OB_SUCCESS;
  const int64_t count = meta_header_->count_;
  const char *dict_payload = meta_header_->payload_;
  const common::ObObjType obj_type = ctx.col_header_->get_store_obj_type();
  common::ObDatum *datums = group_by_cell.get_group_by_col_datums_to_fill();
  group_by_cell.set_distinct_cnt(count);
  if (meta_header_->is_fix_length_dict()) {
    const int64_t dict_data_size = meta_header_->data_size_;
    const char *dict_payload = meta_header_->payload_;
    for (int64_t i = 0; i < count; ++i) {
      int64_t data_offset = i * dict_data_size;
      cell_datas[i] = dict_payload + data_offset;
      datums[i].pack_ = static_cast<uint32_t>(dict_data_size);
    }
  } else {
    const ObIntArrayFuncTable &off_array = ObIntArrayFuncTable::instance(meta_header_->index_byte_);
    const char* end_of_col_meta = reinterpret_cast<const char *>(meta_header_) + meta_length;

    cell_datas[0] = var_data_;
    int32_t last_dict_element_offset = off_array.at_(dict_payload, 0);
    datums[0].pack_ = last_dict_element_offset;
    for (int64_t i = 1; i < count - 1; ++i) {
      int32_t curr_dict_element_offset = off_array.at_(dict_payload, i);
      cell_datas[i] = var_data_ + off_array.at_(dict_payload - meta_header_->index_byte_, i);
      datums[i].pack_ = curr_dict_element_offset - last_dict_element_offset;
      last_dict_element_offset = curr_dict_element_offset;
    }
    datums[count - 1].pack_ = end_of_col_meta - (var_data_ + last_dict_element_offset);
    cell_datas[count - 1] = var_data_ + off_array.at_(dict_payload - meta_header_->index_byte_, count - 1);
  }
  if (OB_FAIL(batch_load_data_to_datum(obj_type, cell_datas, count, integer_mask_, datums))) {
    LOG_WARN("Failed to batch load data to datum", K(ret));
  }
  return ret;
}

#define LOAD_DICT_UNPACK_REFS(dict_count, row_ids, row_cap, col_data, ref_buf, ref_size, unpack_type) \
  int64_t row_id = 0; \
  int64_t ref = 0; \
  int64_t bs_len = meta_header_->count_ * ref_size; \
  for (int64_t i = 0; i < row_cap; ++i) { \
    row_id = row_ids[i]; \
    ObBitStream::get<unpack_type>(col_data, row_id * row_ref_size, row_ref_size, bs_len, ref); \
    ref_buf[i] = static_cast<uint32_t>(ref); \
  }

int ObDictDecoder::read_reference(
    const ObColumnDecoderCtx &ctx,
    const int32_t *row_ids,
    const int64_t row_cap,
    storage::ObGroupByCell &group_by_cell) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited())) {
    ret = OB_NOT_INIT;
    LOG_WARN("Not init", K(ret));
  } else {
    const int64_t count = meta_header_->count_;
    const unsigned char *col_data = reinterpret_cast<unsigned char *>(
        const_cast<ObDictMetaHeader *>(meta_header_)) + ctx.col_header_->length_;
    const uint8_t row_ref_size = meta_header_->row_ref_size_;
    uint32_t *ref_buf = group_by_cell.get_refs_buf();
    bool has_null = false;

    // Read ref
    MEMSET(ref_buf, 0, sizeof(uint32_t) * row_cap);
    if (ctx.is_bit_packing()) {
      const uint8_t row_ref_size = meta_header_->row_ref_size_;
      if (row_ref_size < 10) {
        LOAD_DICT_UNPACK_REFS(
            count, row_ids, row_cap, col_data, ref_buf, row_ref_size, ObBitStream::PACKED_LEN_LESS_THAN_10)
      } else if (row_ref_size < 26) {
        LOAD_DICT_UNPACK_REFS(
            count, row_ids, row_cap, col_data, ref_buf, row_ref_size, ObBitStream::PACKED_LEN_LESS_THAN_26)
      } else if (row_ref_size <= 64) {
        LOAD_DICT_UNPACK_REFS(
            count, row_ids, row_cap, col_data, ref_buf, row_ref_size, ObBitStream::DEFAULT)
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Unpack size larger than 64 bit", K(ret), K(row_ref_size));
      }
    } else {
      for (int64_t i = 0; i < row_cap; ++i) {
        MEMCPY(ref_buf + i, col_data + row_ids[i] * row_ref_size, row_ref_size);
      }
    }
  }
  return ret;
}

#define CHECK_NULL_REF(dict_count, row_cap, col_data, ref_size, unpack_type, has_null) \
  int64_t ref = 0; \
  int64_t bs_len = meta_header_->count_ * ref_size; \
  has_null = false; \
  for (int64_t i = 0; !has_null && i < row_cap; ++i) { \
    ObBitStream::get<unpack_type>(col_data, i * row_ref_size, row_ref_size, bs_len, ref); \
    has_null = ref >= dict_count; \
  }

int ObDictDecoder::check_has_null(const ObColumnDecoderCtx &ctx, const int64_t meta_length, bool &has_null) const
{
  int ret = OB_SUCCESS;
  has_null = false;
  const int64_t count = meta_header_->count_;
  const unsigned char *col_data = reinterpret_cast<unsigned char *>(
      const_cast<ObDictMetaHeader *>(meta_header_)) + meta_length;
  const uint8_t row_ref_size = meta_header_->row_ref_size_;
  if (ctx.is_bit_packing()) {
    const uint8_t row_ref_size = meta_header_->row_ref_size_;
    if (row_ref_size < 10) {
      CHECK_NULL_REF(
          count, ctx.micro_block_header_->row_count_, col_data, row_ref_size, ObBitStream::PACKED_LEN_LESS_THAN_10, has_null);
    } else if (row_ref_size < 26) {
      CHECK_NULL_REF(
          count, ctx.micro_block_header_->row_count_, col_data, row_ref_size, ObBitStream::PACKED_LEN_LESS_THAN_26, has_null);
    } else if (row_ref_size <= 64) {
      CHECK_NULL_REF(
          count, ctx.micro_block_header_->row_count_, col_data, row_ref_size, ObBitStream::DEFAULT, has_null);
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unpack size larger than 64 bit", K(ret), K(row_ref_size));
    }
  } else {
    uint32_t ref = 0;
    for (int64_t i = 0; !has_null && i < ctx.micro_block_header_->row_count_; ++i) {
      MEMCPY(&ref, col_data + i * row_ref_size, row_ref_size);
      has_null = ref >= count;
    }
  }
  return ret;
}

} // end namespace blocksstable
} // end namespace oceanbase
