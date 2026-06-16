/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "storage/blocksstable/cs_encoding/ob_column_encoding_struct.h"
#define USING_LOG_PREFIX STORAGE

#include "ob_integer_stream_vector_decoder.h"
#include "ob_cs_decoding_util.h"
#include "storage/blocksstable/encoding/ob_icolumn_decoder.h"

namespace oceanbase
{
using namespace common;
namespace blocksstable
{

#define DECIMAL_ASSIGN(dst_ptr, dst_len, precision_width, value)           \
  switch(precision_width) {                                                \
    case ObIntegerStream::UintWidth::UW_4_BYTE : {                         \
      *(int32_t*)dst_ptr = (int64_t)(value);                               \
      dst_len= sizeof(int32_t);                                            \
      break;                                                               \
    }                                                                      \
    case ObIntegerStream::UintWidth::UW_8_BYTE : {                         \
      *(int64_t*)dst_ptr = (int64_t)(value);                               \
      dst_len = sizeof(int64_t);                                           \
      break;                                                               \
    }                                                                      \
    case ObIntegerStream::UintWidth::UW_16_BYTE : {                        \
      *(int128_t*)dst_ptr = (int64_t)(value);                              \
      dst_len = sizeof(int128_t);                                          \
      break;                                                               \
    }                                                                      \
    case ObIntegerStream::UintWidth::UW_32_BYTE : {                        \
      *(int256_t*)dst_ptr = (int64_t)(value);                              \
      dst_len = sizeof(int256_t);                                          \
      break;                                                               \
    }                                                                      \
    case ObIntegerStream::UintWidth::UW_64_BYTE : {                        \
      *(int512_t*)dst_ptr = (int64_t)(value);                              \
      dst_len = sizeof(int512_t);                                          \
      break;                                                               \
    }                                                                      \
    default : {                                                            \
      ob_abort();                                                          \
      break;                                                               \
    }                                                                      \
  }

#define GET_SRC_VALUE(value)                                                   \
  if (ref_width_V == ObVecDecodeRefWidth::VDRW_NOT_REF) {                      \
    value = store_uint_arr[vector_ctx.row_ids_[i]] + base;                     \
  } else if (ref_width_V == ObVecDecodeRefWidth::VDRW_TEMP_UINT32_REF) {       \
    value = store_uint_arr[ref_arr[i]] + base;                                 \
  } else {                                                                     \
    value = store_uint_arr[ref_arr[vector_ctx.row_ids_[i]]] + base;            \
  }
#define HANDLE_VALUE_ASSIGN(dst_ptr, dst_len, precision_width, value)      \
  if (is_decimal_V) {                                                      \
    DECIMAL_ASSIGN(dst_ptr, dst_len, precision_width, value);              \
  } else {                                                                 \
    *(ValueType*)(dst_ptr) = value;                                        \
    dst_len = sizeof(ValueType);                                           \
  }

#define HANDLE_FIXED_VALUE_ASSIGN(dst_value, value)      \
 if (is_decimal_V) {                                     \
   dst_value =  (int64_t)(value);                        \
 } else {                                                \
   dst_value = value;                                    \
 }


#define GET_DICT_REF(ref)                                              \
  OB_ASSERT(ref_width_V != ObVecDecodeRefWidth::VDRW_NOT_REF);         \
  if (ref_width_V == ObVecDecodeRefWidth::VDRW_TEMP_UINT32_REF) {      \
    ref = ref_arr[i];                                                  \
  } else {                                                             \
    ref = ref_arr[vector_ctx.row_ids_[i]];                             \
  }


//============================ ConvertUintToVec_T ======================================//
template<typename VectorType, typename ValueType,
    int32_t store_len_V, int32_t ref_width_V, int32_t null_flag_V, int32_t is_decimal_V>
struct ConvertUintToVec_T
{
  static void process(
      const ObBaseColumnDecoderCtx &base_col_ctx,
      const char *data,
      const ObIntegerStreamDecoderCtx &ctx,
      const char *ref_data,
      ObVectorDecodeCtx &vector_ctx,
      VectorType &vector)
  {
    int ret = OB_INNER_STAT_ERROR;
    LOG_ERROR("impossible here", K(store_len_V), K(ref_width_V),
        K(null_flag_V), K(is_decimal_V), K(base_col_ctx), K(ctx));
    ::ob_abort();
  }
};

// Integer stream must use deep-copy, so it don't support decoding to
// ObUniformFormat and ObContinuousFormat, these two vector types only support shallow-copy.
// In fact, for integer stream, the sql layer only use ObFixedLengthFormat.
/**********************************************************************************************
template<typename ValueType, int32_t store_len_V,
    int32_t ref_width_V, int32_t null_flag_V, int32_t is_decimal_V>
struct ConvertUintToVec_T<ObUniformFormat<false>, ValueType,
    store_len_V, ref_width_V, null_flag_V, is_decimal_V>
{
  static void process(
      const ObBaseColumnDecoderCtx &base_col_ctx,
      const char *data,
      const ObIntegerStreamDecoderCtx &ctx,
      const char *ref_data,
      ObVectorDecodeCtx &vector_ctx,
      ObUniformFormat<false> &vector)
  {
    typedef typename ObCSEncodingStoreTypeInference<store_len_V>::Type StoreIntType;
    typedef typename ObCSEncodingStoreTypeInference<ref_width_V>::Type RefIntType;
    const StoreIntType *store_uint_arr = reinterpret_cast<const StoreIntType*>(data);
    const RefIntType *ref_arr = reinterpret_cast<const RefIntType*>(ref_data);

    int64_t ref = 0;
    const uint64_t base = ctx.meta_.is_use_base() * ctx.meta_.base_value_;
    uint64_t value = 0;
    for (int64_t i = 0; i < vector_ctx.row_cap_; i++) {
      const int64_t curr_vec_offset = vector_ctx.vec_offset_ + i;
      ObDatum &datum = vector.get_datum(curr_vec_offset);

      if (null_flag_V == ObBaseColumnDecoderCtx::ObNullFlag::HAS_NO_NULL_OR_NOP) {
        GET_SRC_VALUE(value);
        HANDLE_VALUE_ASSIGN(datum.ptr_, datum.pack_, ctx.meta_.precision_width_tag(), value);

      } else if (null_flag_V == ObBaseColumnDecoderCtx::ObNullFlag::HAS_NULL_OR_NOP_BITMAP) {
        OB_ASSERT(ref_width_V == ObVecDecodeRefWidth::VDRW_NOT_REF);
        if (ObCSDecodingUtil::test_bit(base_col_ctx.null_or_nop_bitmap_, vector_ctx.row_ids_[i])) {
          datum.set_null();
        } else {
          value = store_uint_arr[vector_ctx.row_ids_[i]] + base;
          HANDLE_VALUE_ASSIGN(datum.ptr_, datum.pack_, ctx.meta_.precision_width_tag(), value);
        }

      } else if (null_flag_V == ObBaseColumnDecoderCtx::ObNullFlag::IS_NULL_REPLACED) {
        OB_ASSERT(ref_width_V == ObVecDecodeRefWidth::VDRW_NOT_REF);
        value = store_uint_arr[vector_ctx.row_ids_[i]] + base;
        if (value == base_col_ctx.null_replaced_value_) {
          datum.set_null();
        } else {
          HANDLE_VALUE_ASSIGN(datum.ptr_, datum.pack_, ctx.meta_.precision_width_tag(), value);
        }

      } else if (null_flag_V == ObBaseColumnDecoderCtx::ObNullFlag::IS_NULL_REPLACED_REF) {
        GET_DICT_REF(ref);
        if (ref == base_col_ctx.null_replaced_ref_) {
          datum.set_null(); // must enter next loop, row_id maybe out of range of store_uint_arr
        } else {
          value = store_uint_arr[ref] + base;
          HANDLE_VALUE_ASSIGN(datum.ptr_, datum.pack_, ctx.meta_.precision_width_tag(), value);
        }

      } else {
        ob_abort();
      }
    }
  }
};

template<typename ValueType, int32_t store_len_V,
    int32_t ref_width_V, int32_t null_flag_V, int32_t is_decimal_V>
struct ConvertUintToVec_T<ObDiscreteFormat, ValueType,
    store_len_V, ref_width_V, null_flag_V, is_decimal_V>
{
  static void process(
      const ObBaseColumnDecoderCtx &base_col_ctx,
      const char *data,
      const ObIntegerStreamDecoderCtx &ctx,
      const char *ref_data,
      ObVectorDecodeCtx &vector_ctx,
      ObDiscreteFormat &vector)
  {
    typedef typename ObCSEncodingStoreTypeInference<store_len_V>::Type StoreIntType;
    typedef typename ObCSEncodingStoreTypeInference<ref_width_V>::Type RefIntType;
    const StoreIntType *store_uint_arr = reinterpret_cast<const StoreIntType*>(data);
    const RefIntType *ref_arr = reinterpret_cast<const RefIntType*>(ref_data);

    int64_t ref = 0;
    const uint64_t base = ctx.meta_.is_use_base() * ctx.meta_.base_value_;
    uint64_t value = 0;
    for (int64_t i = 0; i < vector_ctx.row_cap_; i++) {
      const int64_t curr_vec_offset = vector_ctx.vec_offset_ + i;
      char *vec_ptr = vector.get_ptrs()[curr_vec_offset];
      ObLength &vec_len =vector.get_lens()[curr_vec_offset];

      if (null_flag_V == ObBaseColumnDecoderCtx::ObNullFlag::HAS_NO_NULL_OR_NOP) {
        GET_SRC_VALUE(value);
        HANDLE_VALUE_ASSIGN(vec_ptr, vec_len, ctx.meta_.precision_width_tag(), value);

      } else if (null_flag_V == ObBaseColumnDecoderCtx::ObNullFlag::HAS_NULL_OR_NOP_BITMAP) {
        if (ObCSDecodingUtil::test_bit(base_col_ctx.null_or_nop_bitmap_, vector_ctx.row_ids_[i])) {
          vector.set_null(curr_vec_offset);
        } else {
          OB_ASSERT(ref_width_V == ObVecDecodeRefWidth::VDRW_NOT_REF);
          value = store_uint_arr[vector_ctx.row_ids_[i]] + base;
          HANDLE_VALUE_ASSIGN(vec_ptr, vec_len, ctx.meta_.precision_width_tag(), value);
        }

      } else if (null_flag_V == ObBaseColumnDecoderCtx::ObNullFlag::IS_NULL_REPLACED) {
        OB_ASSERT(ref_width_V == ObVecDecodeRefWidth::VDRW_NOT_REF);
        value = store_uint_arr[vector_ctx.row_ids_[i]] + base;
        if (value == base_col_ctx.null_replaced_value_) {
          vector.set_null(curr_vec_offset);
        } else {
          HANDLE_VALUE_ASSIGN(vec_ptr, vec_len, ctx.meta_.precision_width_tag(), value);
        }

      } else if (null_flag_V == ObBaseColumnDecoderCtx::ObNullFlag::IS_NULL_REPLACED_REF) {
        GET_DICT_REF(ref);
        if (ref == base_col_ctx.null_replaced_ref_) {
          vector.set_null(curr_vec_offset); // must enter next loop, row_id maybe out of range of store_uint_arr
        } else {
          value = store_uint_arr[ref] + base;
          HANDLE_VALUE_ASSIGN(vec_ptr, vec_len, ctx.meta_.precision_width_tag(), value);
        }

      } else {
        ob_abort();
      }
    }
  }
};

template<typename ValueType, int32_t store_len_V,
    int32_t ref_width_V, int32_t null_flag_V, int32_t is_decimal_V>
struct ConvertUintToVec_T<ObContinuousFormat, ValueType,
    store_len_V, ref_width_V, null_flag_V, is_decimal_V>
{
  static void process(
      const ObBaseColumnDecoderCtx &base_col_ctx,
      const char *data,
      const ObIntegerStreamDecoderCtx &ctx,
      const char *ref_data,
      ObVectorDecodeCtx &vector_ctx,
      ObContinuousFormat &vector)
  {
    typedef typename ObCSEncodingStoreTypeInference<store_len_V>::Type StoreIntType;
    typedef typename ObCSEncodingStoreTypeInference<ref_width_V>::Type RefIntType;
    const StoreIntType *store_uint_arr = reinterpret_cast<const StoreIntType*>(data);
    const RefIntType *ref_arr = reinterpret_cast<const RefIntType*>(ref_data);

    int64_t ref = 0;
    const uint64_t base = ctx.meta_.is_use_base() * ctx.meta_.base_value_;
    uint64_t value = 0;
    uint32_t curr_offset = 0;
    if (0 == vector_ctx.vec_offset_) {
      vector.get_offsets()[0] = 0;
    } else {
      curr_offset = vector.get_offsets()[vector_ctx.vec_offset_];
    }
    uint32_t vec_len = 0;
    for (int64_t i = 0; i < vector_ctx.row_cap_; i++) {
      const int64_t curr_vec_offset = vector_ctx.vec_offset_ + i;
      const char *vec_ptr = vector.get_data() + curr_offset;

      if (null_flag_V == ObBaseColumnDecoderCtx::ObNullFlag::HAS_NO_NULL_OR_NOP) {
        GET_SRC_VALUE(value);
        HANDLE_VALUE_ASSIGN(vec_ptr, vec_len, ctx.meta_.precision_width_tag(), value);

      } else if (null_flag_V == ObBaseColumnDecoderCtx::ObNullFlag::HAS_NULL_OR_NOP_BITMAP) {
        if (ObCSDecodingUtil::test_bit(base_col_ctx.null_or_nop_bitmap_, vector_ctx.row_ids_[i])) {
          vector.set_null(curr_vec_offset);
        } else {
          OB_ASSERT(ref_width_V == ObVecDecodeRefWidth::VDRW_NOT_REF);
          value = store_uint_arr[vector_ctx.row_ids_[i]] + base;
          HANDLE_VALUE_ASSIGN(vec_ptr, vec_len, ctx.meta_.precision_width_tag(), value);
        }

      } else if (null_flag_V == ObBaseColumnDecoderCtx::ObNullFlag::IS_NULL_REPLACED) {
        OB_ASSERT(ref_width_V == ObVecDecodeRefWidth::VDRW_NOT_REF);
        value = store_uint_arr[vector_ctx.row_ids_[i]] + base;
        if (value == base_col_ctx.null_replaced_value_) {
          vector.set_null(curr_vec_offset);
        } else {
          HANDLE_VALUE_ASSIGN(vec_ptr, vec_len, ctx.meta_.precision_width_tag(), value);
        }

      } else if (null_flag_V == ObBaseColumnDecoderCtx::ObNullFlag::IS_NULL_REPLACED_REF) {
        GET_DICT_REF(ref);
        if (ref == base_col_ctx.null_replaced_ref_) {
          vector.set_null(curr_vec_offset); // must enter next loop, row_id maybe out of range of store_uint_arr
        } else {
          value = store_uint_arr[ref] + base;
          HANDLE_VALUE_ASSIGN(vec_ptr, vec_len, ctx.meta_.precision_width_tag(), value);
        }

      } else {
        ob_abort();
      }
      curr_offset += vec_len;
      vector.get_offsets()[curr_vec_offset + 1] = curr_offset;
    }
  }
};
**********************************************************************************************************************/

namespace
{

template <typename ValueType, typename StoreIntType, int32_t is_decimal_V, bool IS_USE_BASE>
void copy(ValueType *__restrict__ dest,
          const StoreIntType *__restrict__ src,
          const uint64_t base,
          const int cnt)
{
  if constexpr (!IS_USE_BASE) {
    if constexpr (!is_decimal_V && sizeof(ValueType) == sizeof(StoreIntType)) {
      MEMCPY(dest, src, sizeof(ValueType) * cnt);
    } else {
      for (int i = 0; i < cnt; i++) {
        if constexpr (is_decimal_V) {
          dest[i] = (int64_t) src[i];
        } else {
          dest[i] = src[i];
        }
      }
    }
  } else {
    for (int i = 0; i < cnt; i++) {
      if constexpr (is_decimal_V) {
        dest[i] = (int64_t) (src[i] + base);
      } else {
        dest[i] = src[i] + base;
      }
    }
  }
}

template <typename ValueType, typename StoreIntType, typename SelectorIntType, int32_t is_decimal_V, bool IS_USE_BASE>
void gather(ValueType *__restrict__ dest,
            const StoreIntType *__restrict__ src,
            const uint64_t base,
            const SelectorIntType *__restrict__ selector,
            const int cnt)
{
  if constexpr (!IS_USE_BASE) {
    for (int i = 0; i < cnt; i++) {
      if constexpr (is_decimal_V) {
        dest[i] = (int64_t) src[selector[i]];
      } else {
        dest[i] = src[selector[i]];
      }
    }
  } else {
    for (int i = 0; i < cnt; i++) {
      if constexpr (is_decimal_V) {
        dest[i] = (int64_t) (src[selector[i]] + base);
      } else {
        dest[i] = src[selector[i]] + base;
      }
    }
  }
}

// Same as gather, but treats `null_ref` as a sentinel: when selector[i] == null_ref,
// the load index is replaced by 0 (always a legal in-range slot). This avoids 1-slot OOB
// load for IS_NULL_REPLACED_REF dict paths where null_replaced_ref_ == dict_count.
// The written value for null rows is undefined, but those rows are masked out by the nulls.
// Compiles to a single CMP + CSEL (ARM) / CMP + CMOV (x86) per iteration, which can
// be executed in parallel with the gather load and does not introduce any branch.
// Future optimization: reserve a 1-slot padding in memory so that null_ref indices can
// be handled without a per-element branch/select.
template <typename ValueType, typename StoreIntType, typename SelectorIntType, int32_t is_decimal_V, bool IS_USE_BASE
>
OB_INLINE
void gather_with_null_ref(ValueType *__restrict__ dest,
                          const StoreIntType *__restrict__ src,
                          const uint64_t base,
                          const SelectorIntType *__restrict__ selector,
                          const int cnt,
                          const SelectorIntType null_ref)
{
  for (int i = 0; i < cnt; i++) {
    const SelectorIntType r = selector[i];
    const SelectorIntType safe_r = (r == null_ref) ? static_cast<SelectorIntType>(0) : r;
    if constexpr (is_decimal_V) {
      if constexpr (IS_USE_BASE) {
        dest[i] = (int64_t) (src[safe_r] + base);
      } else {
        dest[i] = (int64_t) src[safe_r];
      }
    } else {
      if constexpr (IS_USE_BASE) {
        dest[i] = src[safe_r] + base;
      } else {
        dest[i] = src[safe_r];
      }
    }
  }
}

template <typename Vec, typename Predicate>
OB_INLINE
void mark_nulls(
    Vec &vec,
    const int64_t offset,
    const int64_t cnt,
    bool &has_null,
    bool &is_all_null,
    Predicate &&pred)
{
  vec.get_nulls()->bit_assign(offset, cnt, pred);
  const int64_t null_cnt = vec.get_nulls()->accumulate_bit_cnt(offset, offset + cnt);
  has_null = null_cnt > 0;
  is_all_null = null_cnt == cnt;
  if (has_null) {
    vec.set_has_null();
  }
}

OB_INLINE
static bool calc_is_rows_continuous(
  const int32_t *row_ids,
  const int rows_num)
{
  return (rows_num <= 1) || (row_ids[rows_num - 1] - row_ids[0] + 1 == rows_num);
}

}

template<typename ValueType, int32_t store_len_V,
    int32_t ref_width_V, int32_t null_flag_V, int32_t is_decimal_V>
struct ConvertUintToVec_T<ObFixedLengthFormat<ValueType>, ValueType,
    store_len_V, ref_width_V, null_flag_V, is_decimal_V>
{
  typedef typename ObCSEncodingStoreTypeInference<store_len_V>::Type StoreIntType;
  typedef typename ObCSEncodingStoreTypeInference<ref_width_V>::Type RefIntType;

  template <bool IS_USE_BASE>
  static void process_integer_column(
    const StoreIntType *src,
    const RefIntType *ref_arr,
    const uint64_t base,
    const char *null_bitmap,
    const int64_t null_replaced_value,
    ObFixedLengthFormat<ValueType> &vector,
    const int offset,
    const int32_t *row_ids,
    const int rows_num)
  {
    ValueType *dest = reinterpret_cast<ValueType*>(vector.get_data()) + offset;
    const bool is_rows_continuous = calc_is_rows_continuous(row_ids, rows_num);
    if constexpr (null_flag_V == ObBaseColumnDecoderCtx::ObNullFlag::HAS_NO_NULL_OR_NOP) {
      if (is_rows_continuous) {
        copy<ValueType, StoreIntType, is_decimal_V, IS_USE_BASE>(dest, src + row_ids[0], base, rows_num);
      } else {
        gather<ValueType, StoreIntType, int32_t, is_decimal_V, IS_USE_BASE>(dest, src, base, row_ids, rows_num);
      }
    } else if constexpr (null_flag_V == ObBaseColumnDecoderCtx::ObNullFlag::HAS_NULL_OR_NOP_BITMAP) {
      bool has_null = false;
      bool is_all_null = false;
      if (is_rows_continuous) {
        const int32_t start = row_ids[0];
        mark_nulls(vector, offset, rows_num, has_null, is_all_null, [&](const int idx) {
          return ObCSDecodingUtil::test_bit(null_bitmap, start + idx);
        });
        if (!is_all_null) {
          copy<ValueType, StoreIntType, is_decimal_V, IS_USE_BASE>(dest, src + row_ids[0], base, rows_num);
        }
      } else {
        mark_nulls(vector, offset, rows_num, has_null, is_all_null, [&](const int idx) {
          return ObCSDecodingUtil::test_bit(null_bitmap, row_ids[idx]);
        });
        if (!is_all_null) {
          gather<ValueType, StoreIntType, int32_t, is_decimal_V, IS_USE_BASE>(dest, src, base, row_ids, rows_num);
        }
      }
    } else if constexpr (null_flag_V == ObBaseColumnDecoderCtx::ObNullFlag::IS_NULL_REPLACED) {
      if (is_rows_continuous) {
        copy<ValueType, StoreIntType, is_decimal_V, IS_USE_BASE>(dest, src + row_ids[0], base, rows_num);
      } else {
        gather<ValueType, StoreIntType, int32_t, is_decimal_V, IS_USE_BASE>(dest, src, base, row_ids, rows_num);
      }
      const ValueType null_val = static_cast<ValueType>(null_replaced_value);
      bool has_null = false;
      bool is_all_null = false;
      mark_nulls(vector, offset, rows_num, has_null, is_all_null, [&](const int idx) {
        return dest[idx] == null_val;
      });
    } else {
      ob_abort();
    }
  }

  template <bool IS_USE_BASE>
  static void process_int_dict_column_const(
    const StoreIntType *src,
    const RefIntType *ref_arr,
    const uint64_t base,
    const int64_t null_replaced_ref,
    ObFixedLengthFormat<ValueType> &vector,
    const int offset,
    const int rows_num)
  {
    ValueType *dest = reinterpret_cast<ValueType*>(vector.get_data()) + offset;
    if constexpr (null_flag_V == ObBaseColumnDecoderCtx::ObNullFlag::HAS_NO_NULL_OR_NOP) {
      gather<ValueType, StoreIntType, RefIntType, is_decimal_V, IS_USE_BASE>(dest, src, base, ref_arr, rows_num);
    } else if constexpr (null_flag_V == ObBaseColumnDecoderCtx::ObNullFlag::IS_NULL_REPLACED_REF) {
      const RefIntType null_ref = static_cast<RefIntType>(null_replaced_ref);
      bool has_null = false;
      bool is_all_null = false;
      mark_nulls(vector, offset, rows_num, has_null, is_all_null, [&](const int idx) {
        return ref_arr[idx] == null_ref;
      });
      if (!has_null) {
        gather<ValueType, StoreIntType, RefIntType, is_decimal_V, IS_USE_BASE>(dest, src, base, ref_arr, rows_num);
      } else if (!is_all_null) {
        gather_with_null_ref<ValueType, StoreIntType, RefIntType, is_decimal_V, IS_USE_BASE>(dest, src, base, ref_arr, rows_num, null_ref);
      }
    } else {
      ob_abort();
    }
  }

  template <bool IS_USE_BASE>
  static void process_int_dict_column(
    const StoreIntType *src,
    const RefIntType *ref_arr,
    uint32_t *temp_ref_arr,
    const uint64_t base,
    const int64_t null_replaced_ref,
    ObFixedLengthFormat<ValueType> &vector,
    const int offset,
    const int32_t *row_ids,
    const int rows_num)
  {
    ValueType *dest = reinterpret_cast<ValueType*>(vector.get_data()) + offset;
    const bool is_rows_continuous = calc_is_rows_continuous(row_ids, rows_num);
    if (is_rows_continuous) {
      const int32_t start = row_ids[0];
      if constexpr (null_flag_V == ObBaseColumnDecoderCtx::ObNullFlag::HAS_NO_NULL_OR_NOP) {
        gather<ValueType, StoreIntType, RefIntType, is_decimal_V, IS_USE_BASE>(dest, src, base, ref_arr + start, rows_num);
      } else if constexpr (null_flag_V == ObBaseColumnDecoderCtx::ObNullFlag::IS_NULL_REPLACED_REF) {
        const RefIntType null_ref = static_cast<RefIntType>(null_replaced_ref);
        bool has_null = false;
        bool is_all_null = false;
        mark_nulls(vector, offset, rows_num, has_null, is_all_null, [&](const int idx) {
          return ref_arr[idx + start] == null_ref;
        });
        if (!has_null) {
          gather<ValueType, StoreIntType, RefIntType, is_decimal_V, IS_USE_BASE>(dest, src, base, ref_arr + start, rows_num);
        } else if (!is_all_null) {
          gather_with_null_ref<ValueType, StoreIntType, RefIntType, is_decimal_V, IS_USE_BASE>(dest, src, base, ref_arr + start, rows_num, null_ref);
        }
      } else {
        ob_abort();
      }
    } else {
      // ref_arr + row_ids -> temp_ref_arr
      gather<uint32_t, RefIntType, int32_t, false, false>(temp_ref_arr, ref_arr, 0/*base*/, row_ids, rows_num);
      if constexpr (null_flag_V == ObBaseColumnDecoderCtx::ObNullFlag::HAS_NO_NULL_OR_NOP) {
        gather<ValueType, StoreIntType, uint32_t, is_decimal_V, IS_USE_BASE>(dest, src, base, temp_ref_arr, rows_num);
      } else if constexpr (null_flag_V == ObBaseColumnDecoderCtx::ObNullFlag::IS_NULL_REPLACED_REF) {
        const uint32_t null_ref = static_cast<uint32_t>(null_replaced_ref);
        bool has_null = false;
        bool is_all_null = false;
        mark_nulls(vector, offset, rows_num, has_null, is_all_null, [&](const int idx) {
          return temp_ref_arr[idx] == null_ref;
        });
        if (!has_null) {
          gather<ValueType, StoreIntType, uint32_t, is_decimal_V, IS_USE_BASE>(dest, src, base, temp_ref_arr, rows_num);
        } else if (!is_all_null) {
          gather_with_null_ref<ValueType, StoreIntType, uint32_t, is_decimal_V, IS_USE_BASE>(dest, src, base, temp_ref_arr, rows_num, null_ref);
        }
      } else {
        ob_abort();
      }
    }
  }

  static void process(
      const ObBaseColumnDecoderCtx &base_col_ctx,
      const char *data,
      const ObIntegerStreamDecoderCtx &ctx,
      const char *ref_data,
      ObVectorDecodeCtx &vector_ctx,
      ObFixedLengthFormat<ValueType> &vector)
  {
    const StoreIntType *src = reinterpret_cast<const StoreIntType*>(data);
    const RefIntType *ref_arr = reinterpret_cast<const RefIntType*>(ref_data);

    // Integer Column
    if constexpr (ref_width_V == ObVecDecodeRefWidth::VDRW_NOT_REF) {
      if (ctx.meta_.is_use_base()) {
        process_integer_column<true>(src,
                                     ref_arr,
                                     ctx.meta_.base_value_,
                                     base_col_ctx.null_or_nop_bitmap_,
                                     base_col_ctx.null_replaced_value_,
                                     vector,
                                     vector_ctx.vec_offset_,
                                     vector_ctx.row_ids_,
                                     vector_ctx.row_cap_);
      } else {
        process_integer_column<false>(src,
                                      ref_arr,
                                      ctx.meta_.base_value_,
                                      base_col_ctx.null_or_nop_bitmap_,
                                      base_col_ctx.null_replaced_value_,
                                      vector,
                                      vector_ctx.vec_offset_,
                                      vector_ctx.row_ids_,
                                      vector_ctx.row_cap_);
      }
    }
    // Int Dict Column Const
    else if constexpr (ref_width_V == ObVecDecodeRefWidth::VDRW_TEMP_UINT32_REF) {
      if (ctx.meta_.is_use_base()) {
        process_int_dict_column_const<true>(src,
                                            ref_arr,
                                            ctx.meta_.base_value_,
                                            base_col_ctx.null_replaced_ref_,
                                            vector,
                                            vector_ctx.vec_offset_,
                                            vector_ctx.row_cap_);
      } else {
        process_int_dict_column_const<false>(src,
                                             ref_arr,
                                             ctx.meta_.base_value_,
                                             base_col_ctx.null_replaced_ref_,
                                             vector,
                                             vector_ctx.vec_offset_,
                                             vector_ctx.row_cap_);
        }
    }
    // Int Dict Column
    else if constexpr (ref_width_V == ObVecDecodeRefWidth::VDRW_1_BYTE ||
                       ref_width_V == ObVecDecodeRefWidth::VDRW_2_BYTE ||
                       ref_width_V == ObVecDecodeRefWidth::VDRW_4_BYTE ||
                       ref_width_V == ObVecDecodeRefWidth::VDRW_8_BYTE) {
      if (ctx.meta_.is_use_base()) {
        process_int_dict_column<true>(src,
                                      ref_arr,
                                      vector_ctx.len_arr_,
                                      ctx.meta_.base_value_,
                                      base_col_ctx.null_replaced_ref_,
                                      vector,
                                      vector_ctx.vec_offset_,
                                      vector_ctx.row_ids_,
                                      vector_ctx.row_cap_);
      } else {
        process_int_dict_column<false>(src,
                                       ref_arr,
                                       vector_ctx.len_arr_,
                                       ctx.meta_.base_value_,
                                       base_col_ctx.null_replaced_ref_,
                                       vector,
                                       vector_ctx.vec_offset_,
                                       vector_ctx.row_ids_,
                                       vector_ctx.row_cap_);
      }
    } else {
      ob_abort();
    }
  }
};

// ================================= ObIntVecDecodeDispatcher =================================//
template<typename VectorType, typename ValueType>
class ObIntVecDecodeDispatcher final
{
public:
  static ObIntVecDecodeDispatcher &instance();
  static int decode_vector(
      const ObBaseColumnDecoderCtx &base_col_ctx,
      const char *data,
      const ObIntegerStreamDecoderCtx &ctx,
      const char *ref_data,
      const ObVecDecodeRefWidth ref_width,
      ObVectorDecodeCtx &vector_ctx,
      VectorType &vector)
  {
    int ret = OB_SUCCESS;
    ConvertUnitToVec convert_func = convert_to_vec_funcs_
      [ctx.meta_.width_]               /*val_store_width_V*/
      [ref_width]
      [base_col_ctx.null_flag_]
      [ctx.meta_.is_decimal_int()];
    convert_func(base_col_ctx, data, ctx, ref_data, vector_ctx, vector);
    return ret;
  }

private:
  ObIntVecDecodeDispatcher() {
    if (!func_array_inited_) {
      func_array_inited_ = ObNDArrayIniter<ConvertFuncsInit,
          4, ObVecDecodeRefWidth::VDRW_MAX, ObBaseColumnDecoderCtx::ObNullFlag::MAX, 2>::apply();
    }
  }
  ~ObIntVecDecodeDispatcher() {}
  template<int32_t store_len_V, int32_t ref_width_V, int32_t null_flag_V, int32_t is_decimal_V>
  struct ConvertFuncsInit
  {
    bool operator()()
    {
      ObIntVecDecodeDispatcher<VectorType, ValueType>::convert_to_vec_funcs_
                               [store_len_V]
                               [ref_width_V]
                               [null_flag_V]
                               [is_decimal_V]
        = &(ConvertUintToVec_T<VectorType, ValueType, store_len_V, ref_width_V, null_flag_V, is_decimal_V>::process);
      return true;
    }
  };

  using ConvertUnitToVec = void (*)(
      const ObBaseColumnDecoderCtx &base_col_ctx,
      const char *data,
      const ObIntegerStreamDecoderCtx &ctx,
      const char *ref_data,
      ObVectorDecodeCtx &vector_ctx,
      VectorType &vector);
  DISALLOW_COPY_AND_ASSIGN(ObIntVecDecodeDispatcher);

public:

  static ObMultiDimArray_T<ConvertUnitToVec,
      4, ObVecDecodeRefWidth::VDRW_MAX, ObBaseColumnDecoderCtx::ObNullFlag::MAX, 2> convert_to_vec_funcs_;
  static bool func_array_inited_;
};

template<typename VectorType, typename ValueType>
bool ObIntVecDecodeDispatcher<VectorType, ValueType>::func_array_inited_ = false;

template<typename VectorType, typename ValueType>
ObMultiDimArray_T<typename ObIntVecDecodeDispatcher<VectorType, ValueType>::ConvertUnitToVec,
    4, ObVecDecodeRefWidth::VDRW_MAX, ObBaseColumnDecoderCtx::ObNullFlag::MAX, 2>
    ObIntVecDecodeDispatcher<VectorType, ValueType>::convert_to_vec_funcs_;

template<typename VectorType, typename ValueType>
ObIntVecDecodeDispatcher<VectorType, ValueType> &ObIntVecDecodeDispatcher<VectorType, ValueType>::instance()
{
  static ObIntVecDecodeDispatcher<VectorType, ValueType> dispatcher;
  return dispatcher;
}


//================================= ObIntegerStreamVecDecoder ===================================//

template <typename ValueType>
int ObIntegerStreamVecDecoder::decode_vector_(
    const ObBaseColumnDecoderCtx &base_col_ctx,
    const char *data,
    const ObIntegerStreamDecoderCtx &ctx,
    const char *ref_data,
    const ObVecDecodeRefWidth ref_width,
    ObVectorDecodeCtx &vector_ctx)
{
#define DISPATCH_DECODE_VECTOR(VectorType, ValueType, vector)                       \
ObIntVecDecodeDispatcher<VectorType, ValueType>::instance().decode_vector(          \
    base_col_ctx, data, ctx, ref_data, ref_width, vector_ctx, vector)

  int ret = OB_SUCCESS;
  VectorFormat vec_format = vector_ctx.vec_header_.get_format();
  ObIVector *vector = vector_ctx.vec_header_.get_vector();

  switch (vec_format) {
    case VEC_FIXED : {
      ObFixedLengthFormat<ValueType> *fix_vec = static_cast<ObFixedLengthFormat<ValueType> *>(vector);
      ret = DISPATCH_DECODE_VECTOR(ObFixedLengthFormat<ValueType>, ValueType, *fix_vec);
      break;
    }
    case VEC_CONTINUOUS:{
      // ObContinuousFormat *conti_vec = static_cast<ObContinuousFormat *>(vector);
      // ret = DISPATCH_DECODE_VECTOR(ObContinuousFormat, ValueType,  *conti_vec);
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("integer stream not support decoding to ObContinuousFormat", K(ret), K(base_col_ctx), K(ctx));
      break;
    }
    case VEC_DISCRETE:{
      //ObDiscreteFormat *disc_vec = static_cast<ObDiscreteFormat *>(vector);
      //ret = DISPATCH_DECODE_VECTOR(ObDiscreteFormat, ValueType, *disc_vec);
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("integer stream not support decoding to ObDiscreteFormat", K(ret), K(base_col_ctx), K(ctx));
      break;
    }
    case VEC_UNIFORM:{
      // ObUniformFormat<false> *uni_vec = static_cast<ObUniformFormat<false> *>(vector);
      // ret = DISPATCH_DECODE_VECTOR(ObUniformFormat<false>, ValueType, *uni_vec);
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("integer stream not support decoding to VEC_UNIFORM", K(ret), K(base_col_ctx), K(ctx));
      break;
    }
    default:
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected vector format", K(ret), K(vec_format));
    }
    return ret;
#undef DISPATCH_DECODE_VECTOR
}

int ObIntegerStreamVecDecoder::decode_vector(
    const ObBaseColumnDecoderCtx &base_col_ctx,
    const char *data,
    const ObIntegerStreamDecoderCtx &ctx,
    const char *ref_data,
    const ObVecDecodeRefWidth ref_width,
    ObVectorDecodeCtx &vector_ctx)
{
#define DECODE_VECTOR_(ValueType)                                                      \
  decode_vector_<ValueType>(base_col_ctx, data, ctx, ref_data, ref_width, vector_ctx)

  int ret = OB_SUCCESS;
  const int16_t precision = base_col_ctx.obj_meta_.is_decimal_int() ?
      base_col_ctx.obj_meta_.get_stored_precision() : PRECISION_UNKNOWN_YET;
  const VecValueTypeClass vec_tc = common::get_vec_value_tc(
      base_col_ctx.obj_meta_.get_type(), base_col_ctx.obj_meta_.get_scale(), precision);

  switch (vec_tc) {
    case VEC_TC_YEAR: {
      ret = DECODE_VECTOR_(uint8_t);
      break;
    }
    case VEC_TC_DATE:
    case VEC_TC_MYSQL_DATE:
    case VEC_TC_DEC_INT32:
    case VEC_TC_FLOAT: {
      ret = DECODE_VECTOR_(uint32_t);
      break;
    }
    case VEC_TC_INTEGER:
    case VEC_TC_DATETIME:
    case VEC_TC_MYSQL_DATETIME:
    case VEC_TC_TIME:
    case VEC_TC_UNKNOWN:
    case VEC_TC_INTERVAL_YM:
    case VEC_TC_DEC_INT64:

    case VEC_TC_UINTEGER:
    case VEC_TC_BIT:
    case VEC_TC_ENUM_SET:

    case VEC_TC_DOUBLE:
    case VEC_TC_FIXED_DOUBLE: {
      ret = DECODE_VECTOR_(uint64_t);
      break;
    }

    case VEC_TC_DEC_INT128: {
      ret = DECODE_VECTOR_(int128_t);
      break;
    }
    case VEC_TC_DEC_INT256: {
      ret = DECODE_VECTOR_(int256_t);
      break;
    }
    case VEC_TC_DEC_INT512: {
      ret = DECODE_VECTOR_(int512_t);
      break;
    }
    default: {
      ret = OB_ERR_UNEXPECTED;
      break;
    }
  }
#undef DECODE_VECTOR_

  if (OB_FAIL(ret)) {
    LOG_ERROR("unexpected vec value type for integer stream", K(ret), K(vec_tc), K(base_col_ctx), K(ctx));
  }
  return ret;
}

} // namspace blocksstable
} // namespace oceanbase