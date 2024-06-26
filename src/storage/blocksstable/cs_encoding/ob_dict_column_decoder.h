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

#ifndef OCEANBASE_ENCODING_OB_DICT_COLUMN_DECODER_H_
#define OCEANBASE_ENCODING_OB_DICT_COLUMN_DECODER_H_

#include "ob_icolumn_cs_decoder.h"
#include "ob_cs_decoding_util.h"

namespace oceanbase
{
namespace blocksstable
{

class ObDictValueIterator;

class ObDictColumnDecoder : public ObIColumnCSDecoder
{
public:
  ObDictColumnDecoder() {}
  virtual ~ObDictColumnDecoder() {}
  ObDictColumnDecoder(const ObDictColumnDecoder &) = delete;
  ObDictColumnDecoder &operator=(const ObDictColumnDecoder &) = delete;
  virtual int get_null_count(const ObColumnCSDecoderCtx &ctx, const int32_t *row_ids,
    const int64_t row_cap, int64_t &null_count) const override;

  virtual int pushdown_operator(
    const sql::ObPushdownFilterExecutor *parent,
    const ObColumnCSDecoderCtx &col_ctx,
    const sql::ObWhiteFilterExecutor &filter,
    const sql::PushdownFilterInfo &pd_filter_info,
    common::ObBitmap &result_bitmap) const override;

  virtual int pushdown_operator(
    const sql::ObPushdownFilterExecutor *parent,
    const ObColumnCSDecoderCtx &col_ctx,
    sql::ObBlackFilterExecutor &filter,
    sql::PushdownFilterInfo &pd_filter_info,
    ObBitmap &result_bitmap,
    bool &filter_applied) const override;

  virtual int get_aggregate_result(
    const ObColumnCSDecoderCtx &col_ctx,
    const int32_t *row_ids,
    const int64_t row_cap,
    storage::ObAggCell &agg_cell) const override;

  bool fast_decode_valid(const ObColumnCSDecoderCtx &ctx) const;

  virtual int get_distinct_count(const ObColumnCSDecoderCtx &ctx, int64_t &distinct_count) const override;

  virtual int read_distinct(
    const ObColumnCSDecoderCtx &ctx,
    storage::ObGroupByCell &group_by_cell) const;

  virtual int read_reference(
    const ObColumnCSDecoderCtx &ctx,
    const int32_t *row_ids,
    const int64_t row_cap,
    storage::ObGroupByCell &group_by_cell) const override;

  friend class ObDictValueIterator;

public:
  struct ObConstEncodingRefDesc
  {
    ObConstEncodingRefDesc()
      : width_size_(0), exception_cnt_(0), const_ref_(0),
        exception_row_id_buf_(nullptr), exception_ref_buf_(nullptr)
    {}
    ObConstEncodingRefDesc(const char *ref_buf, const uint32_t width_size)
      : exception_cnt_(0), const_ref_(0)
    {
      MEMCPY(&exception_cnt_, ref_buf, width_size);
      MEMCPY(&const_ref_, ref_buf + width_size, width_size);
      exception_row_id_buf_ = ref_buf + 2 * width_size;
      exception_ref_buf_ = exception_row_id_buf_ + exception_cnt_ * width_size;
      width_size_ = width_size;
    }

    uint32_t width_size_;
    uint32_t exception_cnt_;
    uint32_t const_ref_;
    const char *exception_row_id_buf_;
    const char *exception_ref_buf_;

    TO_STRING_KV(K_(width_size), K_(exception_cnt), K_(const_ref), KP_(exception_row_id_buf), KP_(exception_ref_buf))
  };

protected:
  const static int64_t MAX_STACK_BUF_SIZE = 4 << 10; // 4K
  const static int64_t CS_DICT_SKIP_THRESHOLD = 32;
  virtual int decode_and_aggregate(
    const ObColumnCSDecoderCtx &ctx,
    const int64_t row_id,
    ObStorageDatum &datum,
    storage::ObAggCell &agg_cell) const
  {
    UNUSEDx(ctx, row_id, datum, agg_cell);
    return OB_NOT_SUPPORTED;
  }

  static int check_skip_block(
    const ObDictColumnDecoderCtx &ctx,
    sql::ObBlackFilterExecutor &filter,
    sql::PushdownFilterInfo &pd_filter_info,
    ObBitmap &result_bitmap,
    sql::ObBoolMask &bool_mask);

  static int extract_ref_and_null_count_(
    const ObConstEncodingRefDesc &ref_desc,
    const int64_t dict_count,
    const int32_t *row_ids,
    const int64_t row_cap,
    common::ObDatum *datums,
    int64_t &null_count,
    uint32_t *ref_buf = nullptr);

  static int set_res_with_const_encoding_ref(
    const ObConstEncodingRefDesc &ref_desc,
    const common::ObBitmap *ref_bitmap,
    const sql::PushdownFilterInfo &pd_filter_info,
    const sql::ObPushdownFilterExecutor *parent,
    ObBitmap &result_bitmap);

  static int nu_nn_operator(
    const ObDictColumnDecoderCtx &ctx,
    const sql::ObPushdownFilterExecutor *parent,
    const sql::ObWhiteFilterExecutor &filter,
    const sql::PushdownFilterInfo &pd_filter_info,
    common::ObBitmap &result_bitmap);

  static int eq_ne_operator(
    const ObDictColumnDecoderCtx &ctx,
    const sql::ObPushdownFilterExecutor *parent,
    const sql::ObWhiteFilterExecutor &filter,
    const sql::PushdownFilterInfo &pd_filter_info,
    common::ObBitmap &result_bitmap);
  static int datum_dict_val_eq_ne_op(
    const ObDictColumnDecoderCtx &ctx,
    const sql::ObWhiteFilterExecutor &filter,
    const bool is_sorted,
    const uint64_t dict_val_cnt,
    sql::ObBitVector *ref_bitset,
    int64_t &matched_ref_cnt,
    uint64_t &matched_ref_val);
  static int fast_eq_ne_operator(const uint64_t ori_cmp_value,
    const bool has_null,
    const uint8_t ref_width_tag,
    const char *ref_buf,
    const uint64_t dict_val_cnt,
    const sql::ObWhiteFilterExecutor &filter,
    const sql::PushdownFilterInfo &pd_filter_info,
    common::ObBitmap &result_bitmap);

  static bool is_empty_varying_string(const ObDictColumnDecoderCtx &ctx, const ObDatum &filter_datum);

  static int fast_handle_empty_varying_string(
    const ObDictColumnDecoderCtx &ctx,
    const int64_t distinct_val_cnt, sql::ObBitVector *ref_bitset,
    int64_t &matched_ref_cnt, uint64_t &matched_ref_val);

  static int comparison_operator(
    const ObDictColumnDecoderCtx &ctx,
    const sql::ObPushdownFilterExecutor *parent,
    const sql::ObWhiteFilterExecutor &filter,
    const sql::PushdownFilterInfo &pd_filter_info,
    common::ObBitmap &result_bitmap);

  static int integer_dict_val_cmp_op(
    const ObDictColumnDecoderCtx &ctx,
    const sql::ObWhiteFilterOperatorType &op_type,
    const bool is_col_signed,
    const uint64_t filter_val,
    const int64_t filter_val_size,
    const bool is_filter_signed,
    const uint64_t dict_val_cnt,
    sql::ObBitVector *ref_bitset,
    int64_t &matched_ref_cnt);
  static int datum_dict_val_cmp_op(
    const ObDictColumnDecoderCtx &ctx,
    const sql::ObWhiteFilterExecutor &filter,
    const uint64_t dict_val_cnt,
    sql::ObBitVector *ref_bitset,
    int64_t &matched_ref_cnt);
  static int in_operator(
    const ObDictColumnDecoderCtx &ctx,
    const sql::ObPushdownFilterExecutor *parent,
    const sql::ObWhiteFilterExecutor &filter,
    const sql::PushdownFilterInfo &pd_filter_info,
    common::ObBitmap &result_bitmap);

  static int datum_dict_val_in_op(
    const ObDictColumnDecoderCtx &ctx,
    const sql::ObWhiteFilterExecutor &filter,
    const bool is_sorted_dict,
    sql::ObBitVector *ref_bitset,
    bool &matched_ref_exist,
    const bool is_const_result_set = false);

  static int in_operator_merge_search(
    const ObDictColumnDecoderCtx &ctx,
    const sql::ObWhiteFilterExecutor &filter,
    sql::ObBitVector *ref_bitset,
    int64_t &matched_ref_cnt,
    const bool is_const_result_set);

  static int in_operator_binary_search_dict(
    const ObDictColumnDecoderCtx &ctx,
    const sql::ObWhiteFilterExecutor &filter,
    sql::ObBitVector *ref_bitset,
    int64_t &matched_ref_cnt,
    const bool is_const_result_set);

  static int in_operator_binary_search(
    const ObDictColumnDecoderCtx &ctx,
    const sql::ObWhiteFilterExecutor &filter,
    sql::ObBitVector *ref_bitset,
    int64_t &matched_ref_cnt,
    const bool is_const_result_set);

  static int in_operator_hash_search(
    const ObDictColumnDecoderCtx &ctx,
    const sql::ObWhiteFilterExecutor &filter,
    sql::ObBitVector *ref_bitset,
    int64_t &matched_ref_cnt,
    const bool is_const_result_set);

  static int bt_operator(
    const ObDictColumnDecoderCtx &ctx,
    const sql::ObPushdownFilterExecutor *parent,
    const sql::ObWhiteFilterExecutor &filter,
    const sql::PushdownFilterInfo &pd_filter_info,
    common::ObBitmap &result_bitmap);

  static int integer_dict_val_bt_op(
    const ObDictColumnDecoderCtx &ctx,
    const uint64_t dict_val_cnt,
    sql::ObBitVector *ref_bitset,
    const sql::ObWhiteFilterExecutor &filter,
    const sql::PushdownFilterInfo &pd_filter_info,
    int64_t &matched_ref_cnt);

  static int datum_dict_val_bt_op(
    const ObDictColumnDecoderCtx &ctx,
    const sql::ObWhiteFilterExecutor &filter,
    const ObDatum &left_ref_datum,
    const ObDatum &right_ref_datum,
    const uint64_t dict_val_cnt,
    sql::ObBitVector *ref_bitset,
    int64_t &matched_ref_cnt);

  static int sorted_comparison_for_ref(
    const ObDictColumnDecoderCtx &ctx,
    const sql::ObWhiteFilterExecutor &filter,
    const sql::ObPushdownFilterExecutor *parent,
    const sql::PushdownFilterInfo &pd_filter_info,
    common::ObBitmap &result_bitmap);

  static int sorted_between_for_ref(
    const ObDictColumnDecoderCtx &ctx,
    const uint64_t dict_val_cnt,
    const sql::ObWhiteFilterExecutor &filter,
    const sql::ObPushdownFilterExecutor *parent,
    const sql::PushdownFilterInfo &pd_filter_info,
    common::ObBitmap &result_bitmap);

  static int fast_cmp_ref_and_set_result(
    const int64_t dict_ref_val,
    const int64_t null_replaced_val,
    const char *dict_ref_buf,
    const uint8_t ref_width_tag,
    const sql::ObWhiteFilterOperatorType op_type,
    const sql::PushdownFilterInfo &pd_filter_info,
    ObBitmap &result_bitmap);

  static int traverse_datum_dict_agg(
    const ObDictColumnDecoderCtx &ctx,
    storage::ObAggCell &agg_cell);

  static int cmp_ref_and_set_result(const uint32_t ref_width_size, const char *ref_buf,
    const int64_t dict_ref, const bool has_null, const uint64_t null_replaced_val,
    const sql::ObWhiteFilterOperatorType &op_type, const int64_t row_start,
    const int64_t row_cnt, const sql::ObPushdownFilterExecutor *parent, ObBitmap &result_bitmap);

  static int integer_dict_val_cmp_func(const uint32_t dict_val_width_size, const int64_t datums_cnt,
    const uint64_t *datums_val, const int64_t dict_val_cnt, const char *buf,
    const sql::ObWhiteFilterOperatorType &op_type,
    int64_t &matched_ref_cnt, sql::ObBitVector *ref_bitset);

  // if the row is null, set it as false in @result_bitmap; if the row's ref is set in @ref_bitset,
  // set it as @flag in @result_bitmap, default is true.
  static int set_bitmap_with_bitset(const uint32_t ref_width_size, const char *ref_buf, sql::ObBitVector *ref_bitset,
    const int64_t row_start, const int64_t row_cnt, const bool has_null, const uint64_t null_replaced_val,
    const sql::ObPushdownFilterExecutor *parent, ObBitmap &result_bitmap, const bool flag = true);
  // mainly used for dict_const decoder
  static int set_bitmap_with_bitset_const(const uint32_t ref_width_size, const char *exception_row_id_buf,
    const char *exception_ref_buf, sql::ObBitVector *ref_bitset, const int64_t exception_cnt,
    const int64_t row_start, const int64_t row_cnt, const bool has_null, const uint64_t null_replaced_val,
    ObBitmap &result_bitmap, const bool flag = true);
  // if the row's ref is not set in @ref_bitset and the row is not null, set it in @result_bitmap.
  static int set_bitmap_with_bitset_conversely(const uint32_t ref_width_size, const char *ref_buf,
    sql::ObBitVector *ref_bitset, const int64_t row_start, const int64_t row_cnt,
    const bool has_null, const uint64_t null_replaced_val, const sql::ObPushdownFilterExecutor *parent,
    ObBitmap &result_bitmap);

  static int set_res_with_bitmap(const ObDictEncodingMeta &dict_meta, const char *ref_buf,
    const uint32_t ref_width_size, const common::ObBitmap *ref_bitmap,
    const sql::PushdownFilterInfo &pd_filter_info, common::ObDatum *datums,
    const sql::ObPushdownFilterExecutor *parent,
    ObBitmap &result_bitmap);

  // support white filter for const decoder
  static int do_const_only_operator(const ObDictColumnDecoderCtx &ctx,
    const ObConstEncodingRefDesc &const_ref_desc, const sql::ObWhiteFilterExecutor &filter,
    const sql::PushdownFilterInfo &pd_filter_info, common::ObBitmap &result_bitmap);
  static int nu_nn_const_operator(const ObDictColumnDecoderCtx &ctx, const ObConstEncodingRefDesc &const_ref_desc,
    const sql::ObPushdownFilterExecutor *parent, const sql::ObWhiteFilterExecutor &filter,
    const sql::PushdownFilterInfo &pd_filter_info, common::ObBitmap &result_bitmap);

  static int comparison_const_operator(const ObDictColumnDecoderCtx &ctx, const ObConstEncodingRefDesc &const_ref_desc,
    const sql::ObPushdownFilterExecutor *parent, const sql::ObWhiteFilterExecutor &filter,
    const sql::PushdownFilterInfo &pd_filter_info, common::ObBitmap &result_bitmap);

  static int bt_const_operator(const ObDictColumnDecoderCtx &ctx, const ObConstEncodingRefDesc &const_ref_desc,
    const sql::ObPushdownFilterExecutor *parent, const sql::ObWhiteFilterExecutor &filter,
    const sql::PushdownFilterInfo &pd_filter_info, common::ObBitmap &result_bitmap);

  static int in_const_operator(const ObDictColumnDecoderCtx &ctx, const ObConstEncodingRefDesc &const_ref_desc,
    const sql::ObPushdownFilterExecutor *parent, const sql::ObWhiteFilterExecutor &filter,
    const sql::PushdownFilterInfo &pd_filter_info, common::ObBitmap &result_bitmap);

  template <uint8_t WIDTH_TAG>
  static OB_INLINE void check_empty_varying_string(const ObDictColumnDecoderCtx &ctx,
    const int64_t dict_val_cnt, sql::ObBitVector *ref_bitset,
    int64_t &matched_ref_cnt, uint64_t &matched_ref_val);
  static OB_INLINE bool is_string_space_padded(const char *str, const uint64_t str_len);
};

template <int32_t REF_WIDTH_TAG, int32_t CMP_TYPE>
struct ObCSFilterDictCmpRefFunction
{
  static void dict_cmp_ref_func(
      const int64_t dict_ref_val,
      const int64_t null_val,
      const char *dict_ref_buf,
      const sql::PushdownFilterInfo &pd_filter_info,
      sql::ObBitVector &result) {
    typedef typename ObCSEncodingStoreTypeInference<REF_WIDTH_TAG>::Type RefType;
    const RefType *ref_arr = reinterpret_cast<const RefType *>(dict_ref_buf);
    const RefType casted_dict_ref = *reinterpret_cast<const RefType *>(&dict_ref_val);
    const RefType casted_null_val = *reinterpret_cast<const RefType *>(&null_val);
    RefType cur_ref = 0;
    int64_t row_id = 0;
    switch (CMP_TYPE) {
      case sql::WHITE_OP_LT:
      case sql::WHITE_OP_LE:
      case sql::WHITE_OP_EQ: {
        for (int64_t idx = 0; idx < pd_filter_info.count_; ++idx) {
          row_id = idx + pd_filter_info.start_;
          if (value_cmp_t<RefType, CMP_TYPE>(ref_arr[row_id], casted_dict_ref)) {
            result.set(idx);
          }
        }
        break;
      }
      case sql::WHITE_OP_GT:
      case sql::WHITE_OP_GE:
      case sql::WHITE_OP_NE: {
        for (int64_t idx = 0; idx < pd_filter_info.count_; ++idx) {
          row_id = idx + pd_filter_info.start_;
          if (value_cmp_t<RefType, sql::WHITE_OP_GE>(ref_arr[row_id], casted_null_val)) {
            // if greater than 'casted_null_val', means null value. We should promise that
            // 'null_val' is the largest dict ref value.
          } else if (value_cmp_t<RefType, CMP_TYPE>(ref_arr[row_id], casted_dict_ref)) {
            result.set(idx);
          }
        }
        break;
      }
      default:
        break;
    }
  }
};

extern ObMultiDimArray_T<cs_dict_fast_cmp_function, 4, 6> cs_dict_fast_cmp_funcs;
extern bool cs_dict_fast_cmp_funcs_inited;

template <uint8_t WIDTH_TAG>
OB_INLINE void ObDictColumnDecoder::check_empty_varying_string(
    const ObDictColumnDecoderCtx &ctx,
    const int64_t dict_val_cnt,
    sql::ObBitVector *ref_bitset,
    int64_t &matched_ref_cnt,
    uint64_t &matched_ref_val)
{
  typedef typename ObCSEncodingStoreTypeInference<WIDTH_TAG>::Type DataType;
  if (OB_NOT_NULL(ref_bitset) && OB_NOT_NULL(ctx.offset_data_) && OB_NOT_NULL(ctx.str_data_)) {
    const DataType *offsets = reinterpret_cast<const DataType *>(ctx.offset_data_);
    const char *dict_val_data = nullptr;
    int64_t dict_val_len = 0;
    for (int64_t i = 0; i < dict_val_cnt; ++i) {
      if (0 == i) {
        dict_val_len = offsets[0];
        dict_val_data = ctx.str_data_;
      } else {
        dict_val_len = offsets[i] - offsets[i - 1];
        dict_val_data = ctx.str_data_ + offsets[i - 1];
      }
      if (is_string_space_padded(dict_val_data, dict_val_len)) {
        matched_ref_val = i;
        ref_bitset->set(i);
        ++matched_ref_cnt;
      }
    }
  }
}

OB_INLINE bool ObDictColumnDecoder::is_string_space_padded(const char *str, const uint64_t str_len)
{
  bool bool_ret = true;
  for (uint64_t i = 0; i < str_len; ++i) {
    if (' ' != str[i]) {
      bool_ret = false;
      break;
    }
  }
  return bool_ret;
}
//=============================== ObDictValueIterator =======================================//

class ObDictValueIterator
{
public:
  typedef ObStorageDatum value_type;
  typedef int64_t difference_type;
  typedef ObStorageDatum *pointer;
  typedef ObStorageDatum &reference;
  typedef std::random_access_iterator_tag iterator_category;
public:
  ObDictValueIterator()
    : ctx_(nullptr), index_(0), cell_(),
      decode_by_ref_func_(nullptr) {}
  explicit ObDictValueIterator(const ObDictColumnDecoderCtx *ctx, int64_t index)
      : ctx_(ctx), index_(index), cell_()
  {
    build_decode_by_ref_func_();
  }
  explicit ObDictValueIterator(const ObDictColumnDecoderCtx *ctx, int64_t index, ObStorageDatum& cell)
  {
    ctx_ = ctx;
    index_ = index;
    cell_ = cell;
    build_decode_by_ref_func_();
  }
  OB_INLINE value_type &operator*()
  {
    cell_.pack_ = (uint32_t)index_;
    decode_by_ref_func_(*ctx_, cell_);
    return cell_;
  }
  OB_INLINE value_type *operator->()
  {
    cell_.pack_ = (uint32_t)index_;
    decode_by_ref_func_(*ctx_, cell_);
    return &cell_;
  }
  OB_INLINE ObDictValueIterator operator--(int)
  {
    return ObDictValueIterator(ctx_, index_--, cell_);
  }
  OB_INLINE ObDictValueIterator operator--()
  {
    index_--;
    return *this;
  }
  OB_INLINE ObDictValueIterator operator++(int)
  {
    return ObDictValueIterator(ctx_, index_++, cell_);
  }
  OB_INLINE ObDictValueIterator &operator++()
  {
    index_++;
    return *this;
  }
  OB_INLINE ObDictValueIterator &operator+(int64_t offset)
  {
    index_ += offset;
    return *this;
  }
  OB_INLINE ObDictValueIterator &operator+=(int64_t offset)
  {
    index_ += offset;
    return *this;
  }
  OB_INLINE difference_type operator-(const ObDictValueIterator &rhs)
  {
    return index_ - rhs.index_;
  }
  OB_INLINE ObDictValueIterator &operator-(int64_t offset)
  {
    index_ -= offset;
    return *this;
  }
  OB_INLINE bool operator==(const ObDictValueIterator &rhs) const
  {
    return (this->index_ == rhs.index_);
  }
  OB_INLINE bool operator!=(const ObDictValueIterator &rhs)
  {
    return (this->index_ != rhs.index_);
  }
  OB_INLINE bool operator<(const ObDictValueIterator &rhs)
  {
    return (this->index_ < rhs.index_);
  }
  OB_INLINE bool operator<=(const ObDictValueIterator &rhs)
  {
    return (this->index_ <= rhs.index_);
  }
private:
  using DecodeByRefsFunc =  void(*)(const ObDictColumnDecoderCtx &ctx, value_type &datum);
  static void decode_integer_by_ref_(const ObDictColumnDecoderCtx &ctx, value_type &datum);
  template<bool is_fixed_len_V, bool need_padding_V>
  static void decode_string_by_ref_(const ObDictColumnDecoderCtx &ctx, value_type &datum);
  void build_decode_by_ref_func_();

private:
  const ObDictColumnDecoderCtx *ctx_;
  int64_t index_;
  value_type cell_;
  DecodeByRefsFunc decode_by_ref_func_;
};

#define DO_GET_CONST_ENCODING_REF(width_tag_T, ref_data, row_id, ref)                                                        \
  typedef typename ObCSEncodingStoreTypeInference<width_tag_T>::Type RefStoreIntType;                                        \
  ObDictColumnDecoder::ObConstEncodingRefDesc ref_desc(ref_data, sizeof(RefStoreIntType));                                   \
  const RefStoreIntType *exception_row_id_arr = reinterpret_cast<const RefStoreIntType *>(ref_desc.exception_row_id_buf_);   \
  const RefStoreIntType *exception_ref_arr = reinterpret_cast<const RefStoreIntType *>(ref_desc.exception_ref_buf_);         \
  if (0 == ref_desc.exception_cnt_) {                                                                                        \
    ref = ref_desc.const_ref_;                                                                                               \
  } else {                                                                                                                   \
    const RefStoreIntType *it = std::lower_bound(                                                                            \
        exception_row_id_arr, exception_row_id_arr + ref_desc.exception_cnt_, row_id);                                       \
    if (it == exception_row_id_arr + ref_desc.exception_cnt_ || row_id != *it) {                                             \
      ref = ref_desc.const_ref_;                                                                                             \
    } else {                                                                                                                 \
      ref = exception_ref_arr[it - exception_row_id_arr];                                                                    \
    }                                                                                                                        \
  }

#define GET_CONST_ENCODING_REF(width_tag, ref_data, row_id, ref)                                  \
  switch (width_tag) {                                                                            \
    case ObIntegerStream::UintWidth::UW_1_BYTE: {                                                 \
      DO_GET_CONST_ENCODING_REF(ObIntegerStream::UintWidth::UW_1_BYTE, ref_data, row_id, ref);    \
      break;                                                                                      \
    }                                                                                             \
    case ObIntegerStream::UintWidth::UW_2_BYTE: {                                                 \
      DO_GET_CONST_ENCODING_REF(ObIntegerStream::UintWidth::UW_2_BYTE, ref_data, row_id, ref);    \
      break;                                                                                      \
    }                                                                                             \
    case ObIntegerStream::UintWidth::UW_4_BYTE: {                                                 \
      DO_GET_CONST_ENCODING_REF(ObIntegerStream::UintWidth::UW_4_BYTE, ref_data, row_id, ref);    \
      break;                                                                                      \
    }                                                                                             \
    case ObIntegerStream::UintWidth::UW_8_BYTE: {                                                 \
      DO_GET_CONST_ENCODING_REF(ObIntegerStream::UintWidth::UW_8_BYTE, ref_data, row_id, ref);    \
      break;                                                                                      \
    }                                                                                             \
    default: {                                                                                    \
      LOG_ERROR("illegal ref width", K(ret), K(dict_ctx));                                        \
      ob_abort();                                                                                 \
    }                                                                                             \
  }

#define DO_GET_REF_FROM_REF_ARRAY(width_tag_T, ref_data, row_id, ref)               \
  typedef typename ObCSEncodingStoreTypeInference<width_tag_T>::Type RefIntType;    \
  const RefIntType *ref_arr = reinterpret_cast<const RefIntType *>(ref_data);       \
  ref = ref_arr[row_id];

#define GET_REF_FROM_REF_ARRAY(width_tag, ref_data, row_id, ref)                                 \
  switch (width_tag) {                                                                           \
    case ObIntegerStream::UintWidth::UW_1_BYTE: {                                                \
      DO_GET_REF_FROM_REF_ARRAY(ObIntegerStream::UintWidth::UW_1_BYTE, ref_data, row_id, ref);   \
      break;                                                                                     \
    }                                                                                            \
    case ObIntegerStream::UintWidth::UW_2_BYTE: {                                                \
      DO_GET_REF_FROM_REF_ARRAY(ObIntegerStream::UintWidth::UW_2_BYTE, ref_data, row_id, ref);   \
      break;                                                                                     \
    }                                                                                            \
    case ObIntegerStream::UintWidth::UW_4_BYTE: {                                                \
      DO_GET_REF_FROM_REF_ARRAY(ObIntegerStream::UintWidth::UW_4_BYTE, ref_data, row_id, ref);   \
      break;                                                                                     \
    }                                                                                            \
    case ObIntegerStream::UintWidth::UW_8_BYTE: {                                                \
      DO_GET_REF_FROM_REF_ARRAY(ObIntegerStream::UintWidth::UW_8_BYTE, ref_data, row_id, ref);   \
      break;                                                                                     \
    }                                                                                            \
    default: {                                                                                   \
      LOG_ERROR("illegal ref width", K(ret), K(dict_ctx));                                       \
      ob_abort();                                                                                \
    }                                                                                            \
  }

}  // end namespace blocksstable
}  // end namespace oceanbase

#endif  // OCEANBASE_ENCODING_OB_DICT_COLUMN_DECODER_H_
