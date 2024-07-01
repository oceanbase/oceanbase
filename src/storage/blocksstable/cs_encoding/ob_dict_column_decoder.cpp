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

#include "ob_dict_column_decoder.h"
#include "ob_integer_stream_decoder.h"
#include "ob_string_stream_decoder.h"
#include "ob_cs_encoding_util.h"
#include "storage/blocksstable/encoding/ob_raw_decoder.h"
#include "storage/access/ob_pushdown_aggregate.h"

namespace oceanbase
{
namespace blocksstable
{
//---------------------------------------- cs_dict_fast_cmp_funcs -----------------------------------/
ObMultiDimArray_T<cs_dict_fast_cmp_function, 4, 6> cs_dict_fast_cmp_funcs;
bool init_cs_dict_fast_cmp_simd_func();
bool init_cs_dict_fast_cmp_neon_simd_func();


template <int32_t REF_WIDTH_TAG, int32_t CMP_TYPE>
struct CSDictFastCmpArrayInit
{
  bool operator()()
  {
    cs_dict_fast_cmp_funcs[REF_WIDTH_TAG][CMP_TYPE]
        = &(ObCSFilterDictCmpRefFunction<REF_WIDTH_TAG, CMP_TYPE>::dict_cmp_ref_func);
    return true;
  }
};

bool init_cs_dict_fast_cmp_func()
{
  bool bool_ret = false;
  bool_ret = ObNDArrayIniter<CSDictFastCmpArrayInit, 4, 6>::apply();
#if defined ( __x86_64__ )
  if (is_avx512_valid()) {
    bool_ret = init_cs_dict_fast_cmp_simd_func();
  }
#elif defined ( __aarch64__ ) && defined ( __ARM_NEON )
  bool_ret = init_cs_dict_fast_cmp_neon_simd_func();
#endif
  return bool_ret;
}
bool cs_dict_fast_cmp_funcs_inited = init_cs_dict_fast_cmp_func();


//===================================== ObDictValueIterator ==========================================//
void ObDictValueIterator::decode_integer_by_ref_(const ObDictColumnDecoderCtx &ctx, value_type &datum)
{
  ConvertUnitToDatumFunc convert_func = convert_uint_to_datum_funcs
      [ctx.int_ctx_->meta_.width_/*val_store_width_V*/]
      [ObRefStoreWidthV::REF_IN_DATUMS/*ref_store_width_V*/]
      [get_width_tag_map()[ctx.datum_len_]]  /*datum_width_V*/
      [ctx.null_flag_]
      [ctx.int_ctx_->meta_.is_decimal_int()];
  convert_func(ctx, ctx.int_data_, *ctx.int_ctx_, ctx.ref_data_, nullptr, 1, &datum);

}

template<>
void ObDictValueIterator::decode_string_by_ref_<true/*is_fixed_len_V*/, true/*need_padding_V*/>(
     const ObDictColumnDecoderCtx &ctx, value_type &datum)
{
  ConvertStringToDatumFunc convert_func = convert_string_to_datum_funcs
      [FIX_STRING_OFFSET_WIDTH_V]
      [ObRefStoreWidthV::REF_IN_DATUMS]
      [ctx.null_flag_]
      [ctx.need_copy_];
  convert_func(ctx, ctx.str_data_, *ctx.str_ctx_, nullptr, nullptr, nullptr, 1, &datum);
  OB_ASSERT(OB_SUCCESS == storage::pad_column(ctx.col_param_->get_meta_type(), ctx.col_param_->get_accuracy(), *(ctx.allocator_), datum));
}

template<>
void ObDictValueIterator::decode_string_by_ref_<true/*is_fixed_len_V*/, false/*need_padding_V*/>(
     const ObDictColumnDecoderCtx &ctx, value_type &datum)
{
  ConvertStringToDatumFunc convert_func = convert_string_to_datum_funcs
      [FIX_STRING_OFFSET_WIDTH_V]
      [ObRefStoreWidthV::REF_IN_DATUMS]
      [ctx.null_flag_]
      [ctx.need_copy_];
  convert_func(ctx, ctx.str_data_, *ctx.str_ctx_, nullptr, nullptr, nullptr, 1, &datum);
}
template<>
void ObDictValueIterator::decode_string_by_ref_<false/*is_fixed_len_V*/, true/*need_padding_V*/>(
     const ObDictColumnDecoderCtx &ctx, value_type &datum)
{
  ConvertStringToDatumFunc convert_func = convert_string_to_datum_funcs
      [ctx.offset_ctx_->meta_.width_]
      [ObRefStoreWidthV::REF_IN_DATUMS]
      [ctx.null_flag_]
      [ctx.need_copy_];
  convert_func(ctx, ctx.str_data_, *ctx.str_ctx_, ctx.offset_data_, nullptr, nullptr, 1, &datum);
  OB_ASSERT(OB_SUCCESS == storage::pad_column(ctx.col_param_->get_meta_type(),
      ctx.col_param_->get_accuracy(), *(ctx.allocator_), datum));
}
template<>
void ObDictValueIterator::decode_string_by_ref_<false/*is_fixed_len_V*/, false/*need_padding_V*/>(
     const ObDictColumnDecoderCtx &ctx, value_type &datum)
{
  ConvertStringToDatumFunc convert_func = convert_string_to_datum_funcs
      [ctx.offset_ctx_->meta_.width_]
      [ObRefStoreWidthV::REF_IN_DATUMS]
      [ctx.null_flag_]
      [ctx.need_copy_];
  convert_func(ctx, ctx.str_data_, *ctx.str_ctx_, ctx.offset_data_, nullptr, nullptr, 1, &datum);
}

void ObDictValueIterator::build_decode_by_ref_func_()
{
  if (ctx_->col_header_->is_integer_dict()) {
    decode_by_ref_func_ = decode_integer_by_ref_;
  } else {
    bool need_padding = (nullptr != ctx_->col_param_ && ctx_->col_param_->get_meta_type().is_fixed_len_char_type());
    if (ctx_->str_ctx_->meta_.is_fixed_len_string()) {
      if (need_padding) {
        decode_by_ref_func_ = decode_string_by_ref_<true, true>;
      } else {
        decode_by_ref_func_ = decode_string_by_ref_<true, false>;
      }
    } else {
      if (need_padding) {
        decode_by_ref_func_ = decode_string_by_ref_<false, true>;
      } else {
        decode_by_ref_func_ = decode_string_by_ref_<false, false>;
      }
    }
  }
}


//========================================== filter ===================================================//
#define BUILD_REF_BITSET(ctx, bitset_size, ref_bitset) \
  const int64_t MAX_COUNT_FOR_STACK = MAX_STACK_BUF_SIZE * CHAR_BIT; \
  char ref_bitset_buf_stack[sql::ObBitVector::memory_size(bitset_size % MAX_COUNT_FOR_STACK)]; \
  char *ref_bitset_buf = nullptr; \
  if (bitset_size >= MAX_COUNT_FOR_STACK) { \
    ref_bitset_buf = static_cast<char *>(ctx.allocator_->alloc(sql::ObBitVector::memory_size(bitset_size))); \
    ref_bitset = sql::to_bit_vector(ref_bitset_buf); \
  } else { \
    ref_bitset = sql::to_bit_vector(ref_bitset_buf_stack); \
  } \
  if (OB_ISNULL(ref_bitset)) { \
    ret = OB_ERR_UNEXPECTED; \
    LOG_WARN("ref_bitset should not be null", KR(ret), K(bitset_size)); \
  } else { \
    ref_bitset->init(bitset_size); \
  } \

int ObDictColumnDecoder::get_null_count(
    const ObColumnCSDecoderCtx &col_ctx,
    const int32_t *row_ids,
    const int64_t row_cap,
    int64_t &null_count) const
{
  int ret = OB_SUCCESS;
  null_count = 0;

  if (OB_ISNULL(row_ids) || OB_UNLIKELY(row_cap < 1)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(row_cap), K(col_ctx));
  } else  {
    const ObDictColumnDecoderCtx &ctx = col_ctx.dict_ctx_;
    if (OB_UNLIKELY(0 == ctx.dict_meta_->distinct_val_cnt_)) {
      null_count = row_cap;
    } else if (!ctx.dict_meta_->has_null()) {
      null_count = 0;
    } else {
      const uint32_t width_size = ctx.ref_ctx_->meta_.get_uint_width_size();
      if (ctx.dict_meta_->is_const_encoding_ref()) {
        ObConstEncodingRefDesc ref_desc(ctx.ref_data_, width_size);
        if (OB_UNLIKELY(0 == ref_desc.exception_cnt_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("const encoding can not be all null", KR(ret), K(ref_desc));
        } else if OB_FAIL(extract_ref_and_null_count_(ref_desc,
            ctx.dict_meta_->distinct_val_cnt_, row_ids, row_cap, nullptr, null_count)) {
          LOG_WARN("Failed to extrace null count", K(ret));
        }
      } else {
        uint64_t cur_ref = 0;
        for (int64_t i = 0; i < row_cap; ++i) {
          ENCODING_ADAPT_MEMCPY(&cur_ref, ctx.ref_data_ + row_ids[i] * width_size, width_size);
          if (cur_ref == ctx.dict_meta_->distinct_val_cnt_) {
            ++null_count;
          }
        }
      }
    }
  }
  return ret;
}

int ObDictColumnDecoder::extract_ref_and_null_count_(
    const ObConstEncodingRefDesc &ref_desc,
    const int64_t dict_count,
    const int32_t *row_ids,
    const int64_t row_cap,
    common::ObDatum *datums,
    int64_t &null_count,
    uint32_t *ref_buf)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(0 == ref_desc.exception_cnt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("0 == ref_desc.exception_cnt_ should be handled elsewhere, can't reach here", K(ret));
  } else {
    const ObIntArrayFuncTable &funcs = ObIntArrayFuncTable::instance(ref_desc.width_size_);
    int64_t row_id = 0;
    int64_t except_table_pos = 0;
    int64_t next_except_row_id = funcs.at_(ref_desc.exception_row_id_buf_, except_table_pos);

    bool monotonic_inc = true;
    if (row_cap > 1) {
      monotonic_inc = row_ids[1] > row_ids[0];
    }
    int64_t step = monotonic_inc ? 1 : -1;
    int64_t trav_idx = monotonic_inc ? 0 : row_cap - 1;
    int64_t trav_cnt = 0;
    uint32_t ref;
    while (trav_cnt < row_cap) {
      row_id = row_ids[trav_idx];
      uint32_t *curr_ref = nullptr != ref_buf ? &ref_buf[trav_idx] : nullptr == datums ? &ref : &datums[trav_idx].pack_;
      if (except_table_pos == ref_desc.exception_cnt_ || row_id < next_except_row_id) {
        *curr_ref = ref_desc.const_ref_;
      } else if (row_id == next_except_row_id) {
        *curr_ref = funcs.at_(ref_desc.exception_ref_buf_, except_table_pos);
        ++except_table_pos;
        if (except_table_pos < ref_desc.exception_cnt_) {
          next_except_row_id = funcs.at_(ref_desc.exception_row_id_buf_, except_table_pos);
        }
      } else {
        except_table_pos = funcs.lower_bound_(
            ref_desc.exception_row_id_buf_, 0, ref_desc.exception_cnt_, row_id);
        next_except_row_id = funcs.at_(ref_desc.exception_row_id_buf_, except_table_pos);
        if (except_table_pos == ref_desc.exception_cnt_ || row_id != next_except_row_id) {
          *curr_ref= ref_desc.const_ref_;
        } else {
          *curr_ref = funcs.at_(ref_desc.exception_ref_buf_, except_table_pos);
          ++except_table_pos;
          if (except_table_pos < ref_desc.exception_cnt_) {
            next_except_row_id = funcs.at_(ref_desc.exception_row_id_buf_, except_table_pos);
          }
        }
      }
      if (*curr_ref >= dict_count) {
        null_count++;
      }
      ++trav_cnt;
      trav_idx += step;
    }
  }
  return ret;
}

int ObDictColumnDecoder::set_res_with_const_encoding_ref(
    const ObConstEncodingRefDesc &ref_desc,
    const common::ObBitmap *ref_bitmap,
    const sql::PushdownFilterInfo &pd_filter_info,
    const sql::ObPushdownFilterExecutor *parent,
    ObBitmap &result_bitmap)
{
  int ret = OB_SUCCESS;
  int64_t row_id = 0;
  const bool const_ref_bit = ref_bitmap->test(ref_desc.const_ref_);

  if (0 == ref_desc.exception_cnt_) {
    for (int64_t offset = 0;  OB_SUCC(ret) && offset < pd_filter_info.count_; offset++) {
      row_id = offset + pd_filter_info.start_;
      if (const_ref_bit && OB_FAIL(result_bitmap.set(offset))) {
        LOG_WARN("failed to set result bitmap", K(ret), K(offset), K(offset));
      }
    }
  } else {
    const ObIntArrayFuncTable &funcs = ObIntArrayFuncTable::instance(ref_desc.width_size_);
    int64_t except_table_pos = 0;
    int64_t next_except_row_id = funcs.at_(ref_desc.exception_row_id_buf_, except_table_pos);
    uint32_t curr_ref = 0;
    for (int64_t offset = 0;  OB_SUCC(ret) && offset < pd_filter_info.count_; offset++) {
      row_id = offset + pd_filter_info.start_;
      if (except_table_pos == ref_desc.exception_cnt_ || row_id < next_except_row_id) {
        // curr_ref = ref_desc.const_ref_;
        if (const_ref_bit && OB_FAIL(result_bitmap.set(offset))) {
          LOG_WARN("failed to set result bitmap", K(ret), K(offset), K(offset));
        }
      } else if (row_id == next_except_row_id) {
        curr_ref = funcs.at_(ref_desc.exception_ref_buf_, except_table_pos);
        if (ref_bitmap->test(curr_ref) && OB_FAIL(result_bitmap.set(offset))) {
          LOG_WARN("fail to set result bitmap", KR(ret), K(offset), K(curr_ref));
        }
        ++except_table_pos;
        if (except_table_pos < ref_desc.exception_cnt_) {
          next_except_row_id = funcs.at_(ref_desc.exception_row_id_buf_, except_table_pos);
        }
      } else {
        except_table_pos = funcs.lower_bound_(
            ref_desc.exception_row_id_buf_, 0, ref_desc.exception_cnt_, row_id);
        next_except_row_id = funcs.at_(ref_desc.exception_row_id_buf_, except_table_pos);
        if (except_table_pos == ref_desc.exception_cnt_ || row_id != next_except_row_id) {
          // curr_ref= ref_desc.const_ref_;
          if (const_ref_bit && OB_FAIL(result_bitmap.set(offset))) {
            LOG_WARN("failed to set result bitmap", K(ret), K(offset), K(offset));
          }
        } else {
          curr_ref = funcs.at_(ref_desc.exception_ref_buf_, except_table_pos);
          if (ref_bitmap->test(curr_ref) && OB_FAIL(result_bitmap.set(offset))) {
            LOG_WARN("fail to set result bitmap", KR(ret), K(offset), K(curr_ref));
          }
          ++except_table_pos;
          if (except_table_pos < ref_desc.exception_cnt_) {
            next_except_row_id = funcs.at_(ref_desc.exception_row_id_buf_, except_table_pos);
          }
        }
      }
    }
  }

  return ret;
}

int ObDictColumnDecoder::pushdown_operator(
    const sql::ObPushdownFilterExecutor *parent,
    const ObColumnCSDecoderCtx &col_ctx,
    sql::ObBlackFilterExecutor &filter,
    sql::PushdownFilterInfo &pd_filter_info,
    ObBitmap &result_bitmap,
    bool &filter_applied) const
{
  int ret = OB_SUCCESS;
  filter_applied = false;
  const bool enable_rich_format = filter.get_op().enable_rich_format_;
  if (OB_UNLIKELY(result_bitmap.size() != pd_filter_info.count_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(result_bitmap.size()), K(pd_filter_info));
  } else {
    const ObDictColumnDecoderCtx &ctx = col_ctx.dict_ctx_;
    const uint64_t dict_val_cnt = ctx.dict_meta_->distinct_val_cnt_;
    uint64_t effective_rows = pd_filter_info.count_;
    if (parent != nullptr && parent->need_check_row_filter()) {
      if (parent->is_logic_and_node()) {
        effective_rows = parent->get_result()->popcnt();
      } else {
        effective_rows = pd_filter_info.count_ - parent->get_result()->popcnt();
      }
    }
    if (effective_rows > 1.2 * dict_val_cnt) {
      common::ObBitmap *ref_bitmap = nullptr;
      common::ObDatum *datums = nullptr;
      ObSEArray<ObSqlDatumInfo, 16> datum_infos;
      const bool has_null = ctx.dict_meta_->has_null();
      const int64_t distinct_ref_cnt = (has_null ? (dict_val_cnt + 1) : dict_val_cnt);
      if (OB_FAIL(pd_filter_info.init_bitmap(distinct_ref_cnt, ref_bitmap))) {
        LOG_WARN("fail to init bitmap", KR(ret), K(has_null), K(distinct_ref_cnt));
      } else if (OB_FAIL(filter.get_datums_from_column(datum_infos))) {
        LOG_WARN("fail to get filter column datum_infos", KR(ret));
      } else if (OB_UNLIKELY(1 != datum_infos.count() || !datum_infos.at(0).is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected datum infos", KR(ret), K(datum_infos));
      } else {
        datums = datum_infos.at(0).datum_ptr_;
      }

      if (OB_SUCC(ret)) {
        if (0 == dict_val_cnt) {  // empty dict, all datum is null
          // use uniform base currently, support new format later
          //if (enable_rich_format && OB_FAIL(storage::init_exprs_vector_header(filter.get_filter_node().column_exprs_, filter.get_op().get_eval_ctx(), 1))) {
          //  LOG_WARN("Failed to init exprs vector header", K(ret));
          if (FALSE_IT(datums[0].set_null())) {
          } else if (OB_FAIL(filter.filter_batch(nullptr, 0, 1, *ref_bitmap))) {
            LOG_WARN("fail to filter batch", KR(ret), K(pd_filter_info));
          } else {
            if (ref_bitmap->test(0)) {
              result_bitmap.reuse(true);
            }
            filter_applied = true;
          }
          LOG_TRACE("dict black filter pushdown", K(ret), K(ctx), K(filter_applied), K(pd_filter_info));
        } else {
          sql::ObBoolMask bool_mask;
          if (OB_FAIL(check_skip_block(ctx, filter, pd_filter_info, result_bitmap, bool_mask))) {
            LOG_WARN("Failed to check whether hit shortcut", KR(ret), K(ctx), K(filter), K(pd_filter_info));
          } else if (!bool_mask.is_uncertain()) {
            filter_applied = true;
            LOG_DEBUG("skip block in dict black filter pushdown", K(result_bitmap.popcnt()));
          } else {
            for (int64_t index = 0; OB_SUCC(ret) && (index < distinct_ref_cnt); ) {
              int64_t upper_bound = MIN(index + pd_filter_info.batch_size_, distinct_ref_cnt);
              const int64_t cur_ref_cnt = upper_bound - index;
              // use uniform base currently, support new format later
              //if (enable_rich_format && OB_FAIL(storage::init_exprs_vector_header(filter.get_filter_node().column_exprs_, filter.get_op().get_eval_ctx(), cur_ref_cnt))) {
              //  LOG_WARN("Failed to init exprs vector header", K(ret));
              //  break;
              //}
              for (int64_t dict_ref = index; dict_ref < upper_bound; dict_ref++) {
                datums[dict_ref - index].pack_ = dict_ref;
              }
              if (col_ctx.is_int_dict_type()) {
                ConvertUnitToDatumFunc convert_func = convert_uint_to_datum_funcs
                    [ctx.int_ctx_->meta_.width_/*val_store_width_V*/]
                    [ObRefStoreWidthV::REF_IN_DATUMS/*ref_store_width_V*/]
                    [get_width_tag_map()[ctx.datum_len_]/*datum_width_V*/]
                    [ctx.null_flag_]
                    [ctx.int_ctx_->meta_.is_decimal_int()];
                convert_func(ctx, ctx.int_data_, *ctx.int_ctx_, ctx.ref_data_, nullptr, cur_ref_cnt, datums);
              } else {
                const uint32_t offset_width = ctx.str_ctx_->meta_.is_fixed_len_string() ?
                    FIX_STRING_OFFSET_WIDTH_V : ctx.offset_ctx_->meta_.width_;
                ConvertStringToDatumFunc convert_func = convert_string_to_datum_funcs
                    [offset_width]
                    [ObRefStoreWidthV::REF_IN_DATUMS]
                    [ctx.null_flag_]
                    [ctx.need_copy_];
                convert_func(ctx, ctx.str_data_, *ctx.str_ctx_,
                    ctx.offset_data_, ctx.ref_data_, nullptr, cur_ref_cnt, datums);
              }
              if (ctx.obj_meta_.is_fixed_len_char_type() && (nullptr != ctx.col_param_)) {
                if (OB_FAIL(storage::pad_on_datums(ctx.col_param_->get_accuracy(),
                    ctx.obj_meta_.get_collation_type(), *ctx.allocator_, cur_ref_cnt, datums))) {
                  LOG_WARN("fail to pad on datums", KR(ret), K(ctx), K(index), K(upper_bound));
                }
              }
              if (FAILEDx(filter.filter_batch(nullptr, index, upper_bound, *ref_bitmap))) {
                LOG_WARN("fail to filter batch", KR(ret), K(index), K(upper_bound));
              } else {
                index = upper_bound;
              }
            }

            if (OB_SUCC(ret)) {
              const uint32_t ref_width_size = ctx.ref_ctx_->meta_.get_uint_width_size();
              if (OB_FAIL(set_res_with_bitmap(*ctx.dict_meta_, ctx.ref_data_,
                  ref_width_size, ref_bitmap, pd_filter_info, datums, parent, result_bitmap))) {
                LOG_WARN("fail to set result with bitmap", KR(ret), K(ref_width_size), K(pd_filter_info));
              } else {
                filter_applied = true;
              }
            }
          }
          LOG_TRACE("dict black filter pushdown", K(ret), K(ctx),
                K(filter_applied), K(pd_filter_info), K(result_bitmap.popcnt()));
        }
      }
    }
  }

  return ret;
}

int ObDictColumnDecoder::check_skip_block(
    const ObDictColumnDecoderCtx &ctx,
    sql::ObBlackFilterExecutor &filter,
    sql::PushdownFilterInfo &pd_filter_info,
    ObBitmap &result_bitmap,
    sql::ObBoolMask &bool_mask)
{
  int ret = OB_SUCCESS;
  const uint64_t dict_val_cnt = ctx.dict_meta_->distinct_val_cnt_;
  const bool has_null = ctx.dict_meta_->has_null();
  if (!filter.is_monotonic()) {
  } else if (dict_val_cnt <= CS_DICT_SKIP_THRESHOLD) {
    // Do not skip block when dict count is small.
    // Otherwise, if can not skip block by monotonicity, the performance will decrease.
  } else if (ctx.dict_meta_->is_sorted()) {
    ObStorageDatum min_datum = *ObDictValueIterator(&ctx, 0);
    ObStorageDatum max_datum = *(ObDictValueIterator(&ctx, dict_val_cnt) - 1);
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

int ObDictColumnDecoder::pushdown_operator(
    const sql::ObPushdownFilterExecutor *parent,
    const ObColumnCSDecoderCtx &col_ctx,
    const sql::ObWhiteFilterExecutor &filter,
    const sql::PushdownFilterInfo &pd_filter_info,
    ObBitmap &result_bitmap) const
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(result_bitmap.size() != pd_filter_info.count_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), "result_map size", result_bitmap.size(), K(pd_filter_info));
  } else {
    const ObDictColumnDecoderCtx &ctx = col_ctx.dict_ctx_;
    const bool is_const_encoding = ctx.dict_meta_->is_const_encoding_ref();
    const sql::ObWhiteFilterOperatorType op_type = filter.get_op_type();
    if (is_const_encoding) {
      const uint32_t ref_width_size = ctx.ref_ctx_->meta_.get_uint_width_size();
      ObConstEncodingRefDesc const_ref_desc(ctx.ref_data_, ref_width_size);
      if (0 == const_ref_desc.exception_cnt_) {
        if (OB_FAIL(do_const_only_operator(ctx, const_ref_desc, filter, pd_filter_info, result_bitmap))) {
          LOG_WARN("fail to check const only operator", KR(ret));
        }
      } else {
        switch(op_type) {
          case sql::WHITE_OP_NU:
          case sql::WHITE_OP_NN: {
            if (OB_FAIL(nu_nn_const_operator(ctx, const_ref_desc, parent, filter, pd_filter_info, result_bitmap))) {
              LOG_WARN("fail to handle nu_nn const operator", KR(ret), K(const_ref_desc));
            }
            break;
          }
          case sql::WHITE_OP_EQ:
          case sql::WHITE_OP_NE:
          case sql::WHITE_OP_GT:
          case sql::WHITE_OP_GE:
          case sql::WHITE_OP_LT:
          case sql::WHITE_OP_LE: {
            if (OB_FAIL(comparison_const_operator(ctx, const_ref_desc, parent, filter, pd_filter_info, result_bitmap))) {
              LOG_WARN("fail to handle comparison const operator", KR(ret), K(const_ref_desc));
            }
            break;
          }
          case sql::WHITE_OP_IN: {
            if (OB_FAIL(in_const_operator(ctx, const_ref_desc, parent, filter, pd_filter_info, result_bitmap))) {
              LOG_WARN("fail to handle in const operator", KR(ret), K(const_ref_desc));
            }
            break;
          }
          case sql::WHITE_OP_BT: {
            if (OB_FAIL(bt_const_operator(ctx, const_ref_desc, parent, filter, pd_filter_info, result_bitmap))) {
              LOG_WARN("fail to handle bt const operator", KR(ret), K(const_ref_desc));
            }
            break;
          }
          default: {
            ret = OB_NOT_SUPPORTED;
            LOG_WARN("unexpected operation type", KR(ret), K(op_type));
          }
        }
      }
    } else {
      switch(op_type) {
        case sql::WHITE_OP_NU:
        case sql::WHITE_OP_NN: {
          if (OB_FAIL(nu_nn_operator(ctx, parent, filter, pd_filter_info, result_bitmap))) {
            LOG_WARN("fail to handle nu_nn operator", KR(ret));
          }
          break;
        }
        case sql::WHITE_OP_EQ:
        case sql::WHITE_OP_NE: {
          if (OB_FAIL(eq_ne_operator(ctx, parent, filter, pd_filter_info, result_bitmap))) {
            LOG_WARN("fail to handle eq_ne operator", KR(ret));
          }
          break;
        }
        case sql::WHITE_OP_GT:
        case sql::WHITE_OP_GE:
        case sql::WHITE_OP_LT:
        case sql::WHITE_OP_LE: {
          if (OB_FAIL(comparison_operator(ctx, parent, filter, pd_filter_info, result_bitmap))) {
            LOG_WARN("fail to handle comparison operator", KR(ret));
          }
          break;
        }
        case sql::WHITE_OP_IN: {
          if (OB_FAIL(in_operator(ctx, parent, filter, pd_filter_info, result_bitmap))) {
            LOG_WARN("fail to handle in operator", KR(ret));
          }
          break;
        }
        case sql::WHITE_OP_BT: {
          if (OB_FAIL(bt_operator(ctx, parent, filter, pd_filter_info, result_bitmap))) {
            LOG_WARN("fail to handle bt operator", KR(ret));
          }
          break;
        }
        default: {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("unexpected operation type", KR(ret), K(op_type));
        }
      }
    }
    LOG_TRACE("dict white filter pushdown", K(ret), K(is_const_encoding),
        K(ctx), K(op_type), K(pd_filter_info), K(result_bitmap.popcnt()));
  }
  return ret;
}

int ObDictColumnDecoder::nu_nn_operator(
    const ObDictColumnDecoderCtx &ctx,
    const sql::ObPushdownFilterExecutor *parent,
    const sql::ObWhiteFilterExecutor &filter,
    const sql::PushdownFilterInfo &pd_filter_info,
    ObBitmap &result_bitmap)
{
  int ret = OB_SUCCESS;
  int64_t datums_cnt = 0;
  const uint32_t distinct_val_cnt = ctx.dict_meta_->distinct_val_cnt_;
  if (0 == distinct_val_cnt) {
    // all datum is null
    if ((sql::WHITE_OP_NU == filter.get_op_type()) && OB_FAIL(result_bitmap.bit_not())) {
      LOG_WARN("fail to execute bit_not", KR(ret));
    }
  } else {
    if (ctx.dict_meta_->has_null()) {
      // change 'is nullptr' to 'dict_ref == distinct_val_cnt', like integer_stream decoder equal operator
      const uint64_t filter_val = distinct_val_cnt;

      const int64_t row_start = pd_filter_info.start_;
      const int64_t row_cnt = pd_filter_info.count_;
      const uint32_t ref_width_size = ctx.ref_ctx_->meta_.get_uint_width_size();
      // filter_val must be in the range of ref value.
      if (OB_FAIL(ObCSFilterFunctionFactory::instance().integer_compare_tranverse(ctx.ref_data_, ref_width_size,
          filter_val, row_start, row_cnt, false, sql::WHITE_OP_EQ, parent, result_bitmap))) {
        LOG_WARN("fail to handle integer bt tranverse", KR(ret), K(ctx), K(filter_val), K(ref_width_size));
      }
    }

    if (OB_SUCC(ret) && (sql::WHITE_OP_NN == filter.get_op_type())) {
      if (OB_FAIL(result_bitmap.bit_not())) {
        LOG_WARN("fail to execute bit_not", KR(ret));
      }
    }
  }
  return ret;
}

int ObDictColumnDecoder::eq_ne_operator(
    const ObDictColumnDecoderCtx &ctx,
    const sql::ObPushdownFilterExecutor *parent,
    const sql::ObWhiteFilterExecutor &filter,
    const sql::PushdownFilterInfo &pd_filter_info,
    ObBitmap &result_bitmap)
{
  int ret = OB_SUCCESS;
  int64_t datums_cnt = 0;
  const uint64_t dict_val_cnt = ctx.dict_meta_->distinct_val_cnt_;
  if (OB_UNLIKELY((datums_cnt = filter.get_datums().count()) != 1)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(datums_cnt));
  } else if (0 == dict_val_cnt) {
    // all datum is null
  } else {
    const sql::ObWhiteFilterOperatorType op_type = filter.get_op_type();
    const ObDatum &filter_datum = filter.get_datums().at(0);
    const bool has_null = ctx.dict_meta_->has_null();
    sql::ObBitVector *ref_bitset = nullptr;
    BUILD_REF_BITSET(ctx, (dict_val_cnt + 1), ref_bitset);
    if (OB_SUCC(ret)) {
      int64_t matched_ref_cnt = 0;
      uint64_t matched_ref_val = 0;
      const bool is_integer_dict = ctx.col_header_->is_integer_dict();

      if (!is_integer_dict && is_empty_varying_string(ctx, filter_datum)) {
        if (OB_FAIL(fast_handle_empty_varying_string(ctx, dict_val_cnt, ref_bitset, matched_ref_cnt, matched_ref_val))) {
          LOG_WARN("fail to fast handle empty varying string", KR(ret), K(dict_val_cnt));
        }
      } else {
        bool is_sorted = ctx.dict_meta_->is_sorted();
        if (is_sorted || !is_integer_dict) {
          if (OB_FAIL(datum_dict_val_eq_ne_op(ctx, filter, is_sorted, dict_val_cnt, ref_bitset, matched_ref_cnt, matched_ref_val))) {
	          LOG_WARN("fail to exe datum dict val eq ne op", K(ret));
	        }
        } else {
          const ObObjType col_type = ctx.col_header_->get_store_obj_type();
          const sql::ObPushdownWhiteFilterNode &filter_node = filter.get_filter_node();
          bool is_col_signed = false;
          bool is_filter_signed = false;
          uint64_t filter_val = 0;
          int64_t filter_val_size = 0;
          common::ObObjMeta filter_val_meta;
          if (OB_FAIL(filter.get_filter_node().get_filter_val_meta(filter_val_meta))) {
            LOG_WARN("fail to get filter meta", K(ret));
          } else {
            if (ObCSDecodingUtil::can_convert_to_integer(col_type, is_col_signed)) {
              if (ObCSDecodingUtil::check_datum_not_over_8bytes(filter_val_meta.get_type(),
                    filter_datum, is_filter_signed, filter_val, filter_val_size)) {
                if (OB_FAIL(integer_dict_val_cmp_op(ctx, op_type, is_col_signed, filter_val,
                    filter_val_size, is_filter_signed, dict_val_cnt, ref_bitset, matched_ref_cnt))) {
                  LOG_WARN("fail to exe integer_dict_val_cmp_op", KR(ret), K(op_type), K(filter_datum));
                }
              } else { // over 8bytes, all datum not requal
                matched_ref_cnt = 0;
              }
            } else if (OB_FAIL(datum_dict_val_eq_ne_op(ctx, filter, is_sorted, dict_val_cnt, ref_bitset, matched_ref_cnt, matched_ref_val))) {
              LOG_WARN("fail to exe datum dict val eq ne op", K(ret));
            }
          }
        }
      }

      if (OB_SUCC(ret)) {
        if (1 == matched_ref_cnt) {
          if (OB_FAIL(fast_eq_ne_operator(matched_ref_val, has_null, ctx.ref_ctx_->meta_.width_, ctx.ref_data_,
              dict_val_cnt, filter, pd_filter_info, result_bitmap))) {
            LOG_WARN("fail to exexute fast eq_ne operator", KR(ret), K(matched_ref_val), K(has_null), K(ctx));
          }
        } else {
          const uint32_t ref_width_size = ctx.ref_ctx_->meta_.get_uint_width_size();
          if (sql::WHITE_OP_EQ == op_type) {
            if ((matched_ref_cnt > 0) && OB_FAIL(set_bitmap_with_bitset(ref_width_size, ctx.ref_data_,
                ref_bitset, pd_filter_info.start_, pd_filter_info.count_, false, 0, parent, result_bitmap))) {
              LOG_WARN("fail to set result bitmap", KR(ret), K(ref_width_size), K(matched_ref_cnt), K(pd_filter_info));
            }
          } else {
            // if exists null, we need to filter the null row
            if (OB_FAIL(set_bitmap_with_bitset_conversely(ref_width_size, ctx.ref_data_, ref_bitset,
                pd_filter_info.start_, pd_filter_info.count_, has_null, dict_val_cnt, parent, result_bitmap))) {
              LOG_WARN("fail to set result bitmap conversely", KR(ret), K(ref_width_size), K(matched_ref_cnt),
                K(has_null), K(dict_val_cnt), K(pd_filter_info));
            }
          }
        }
      }

    }
  }
  return ret;
}

int ObDictColumnDecoder::datum_dict_val_eq_ne_op(
    const ObDictColumnDecoderCtx &ctx,
    const sql::ObWhiteFilterExecutor &filter,
    const bool is_sorted,
    const uint64_t dict_val_cnt,
    sql::ObBitVector *ref_bitset,
    int64_t &matched_ref_cnt,
    uint64_t &matched_ref_val)
{
  int ret = OB_SUCCESS;
  ObCmpFunc cmp_func;
  cmp_func.cmp_func_ = filter.cmp_func_;
  const ObDatum &filter_datum = filter.get_datums().at(0);
  ObDictValueIterator begin_it = ObDictValueIterator(&ctx, 0);
  ObDictValueIterator end_it = ObDictValueIterator(&ctx, dict_val_cnt);
  ObDictValueIterator tranverse_it = begin_it;
  int64_t dict_ref = 0;

  if (OB_LIKELY(is_sorted)) {
    tranverse_it = std::lower_bound(begin_it, end_it, filter_datum,
		                    [&cmp_func, &ret](const ObDatum &datum, const ObDatum &filter_datum)
                                    -> bool {
                                      int cmp_ret = 0;
                                      if (OB_FAIL(ret)) {
                                      } else if (OB_FAIL(cmp_func.cmp_func_(datum, filter_datum, cmp_ret))) {
                                        LOG_WARN("failed to compare datums", K(ret), K(datum), K(filter_datum));
                                      }
                                      return cmp_ret < 0;});
    dict_ref = tranverse_it - begin_it;
  }
  for (; tranverse_it != end_it && OB_SUCC(ret); ++tranverse_it, ++dict_ref) {
    int cmp_ret = 0;
    if (OB_FAIL(cmp_func.cmp_func_(*tranverse_it, filter_datum, cmp_ret))) {
      LOG_WARN("failed to compare datums", K(ret), K(*tranverse_it), K(filter_datum));
    } else if (0 != cmp_ret) {
      if (is_sorted) {
        break;
      }
    } else {
      ++matched_ref_cnt;
      matched_ref_val = dict_ref;
      ref_bitset->set(dict_ref);
    }
  }
  return ret;
}

int ObDictColumnDecoder::fast_eq_ne_operator(
    const uint64_t cmp_value,
    const bool has_null,
    const uint8_t ref_width_tag,
    const char *ref_buf,
    const uint64_t dict_val_cnt,
    const sql::ObWhiteFilterExecutor &filter,
    const sql::PushdownFilterInfo &pd_filter_info,
    ObBitmap &result_bitmap)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ref_buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret));
  } else {
    const sql::ObWhiteFilterOperatorType op_type = filter.get_op_type();
    if (sql::WHITE_OP_EQ == op_type || !has_null) {
      raw_compare_function cmp_func = RawCompareFunctionFactory::instance().get_cmp_function(
                                      false, ref_width_tag, op_type);
      if (OB_ISNULL(cmp_func)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected nullptr compare function", KR(ret), K(ref_width_tag), K(op_type));
      } else {
        cmp_func(reinterpret_cast<const unsigned char *>(ref_buf), cmp_value, result_bitmap.get_data(),
                 pd_filter_info.start_, pd_filter_info.start_ + pd_filter_info.count_);
      }
    } else {
      raw_compare_function_with_null cmp_func = RawCompareFunctionFactory::instance().get_cs_cmp_function_with_null(
                                                ref_width_tag, op_type);
      if (OB_ISNULL(cmp_func)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected nullptr compare function", KR(ret), K(ref_width_tag), K(op_type));
      } else {
        cmp_func(reinterpret_cast<const unsigned char *>(ref_buf), cmp_value, dict_val_cnt, result_bitmap.get_data(),
                 pd_filter_info.start_, pd_filter_info.start_ + pd_filter_info.count_);
      }
    }
  }
  return ret;
}

bool ObDictColumnDecoder::is_empty_varying_string(const ObDictColumnDecoderCtx &ctx, const ObDatum &filter_datum)

{
  bool bool_ret = false;
  bool_ret = (0 == filter_datum.len_
              && ctx.obj_meta_.is_varying_len_char_type()
              && lib::is_mysql_mode()
              && !ctx.dict_meta_->is_sorted()
              && !ctx.col_header_->is_fixed_length()
              && ctx.dict_meta_->distinct_val_cnt_ > 0);
  return bool_ret;
}

int ObDictColumnDecoder::fast_handle_empty_varying_string(
    const ObDictColumnDecoderCtx &ctx,
    const int64_t dict_val_cnt,
    sql::ObBitVector *ref_bitset,
    int64_t &matched_ref_cnt,
    uint64_t &matched_ref_val)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(ctx.offset_ctx_)) {
    const uint8_t idx_width_tag = ctx.offset_ctx_->meta_.get_width_tag();
    switch (idx_width_tag) {
      case ObIntegerStream::UintWidth::UW_1_BYTE: {
        check_empty_varying_string<ObIntegerStream::UintWidth::UW_1_BYTE>(ctx, dict_val_cnt, ref_bitset, matched_ref_cnt, matched_ref_val);
        break;
      }
      case ObIntegerStream::UintWidth::UW_2_BYTE: {
        check_empty_varying_string<ObIntegerStream::UintWidth::UW_2_BYTE>(ctx, dict_val_cnt, ref_bitset, matched_ref_cnt, matched_ref_val);
        break;
      }
      case ObIntegerStream::UintWidth::UW_4_BYTE: {
        check_empty_varying_string<ObIntegerStream::UintWidth::UW_4_BYTE>(ctx, dict_val_cnt, ref_bitset, matched_ref_cnt, matched_ref_val);
        break;
      }
      case ObIntegerStream::UintWidth::UW_8_BYTE: {
        check_empty_varying_string<ObIntegerStream::UintWidth::UW_4_BYTE>(ctx, dict_val_cnt, ref_bitset, matched_ref_cnt, matched_ref_val);
        break;
      }
      default:
        ret = OB_NOT_SUPPORTED;
    }
  }

  return ret;
}

int ObDictColumnDecoder::comparison_operator(
    const ObDictColumnDecoderCtx &ctx,
    const sql::ObPushdownFilterExecutor *parent,
    const sql::ObWhiteFilterExecutor &filter,
    const sql::PushdownFilterInfo &pd_filter_info,
    ObBitmap &result_bitmap)
{
  int ret = OB_SUCCESS;
  int64_t datums_cnt = 0;
  if (OB_UNLIKELY((datums_cnt = filter.get_datums().count()) != 1)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(datums_cnt));
  } else {
    const sql::ObWhiteFilterOperatorType op_type = filter.get_op_type();
    const ObDatum &filter_datum = filter.get_datums().at(0);
    const uint64_t dict_val_cnt = ctx.dict_meta_->distinct_val_cnt_;
    if (0 == dict_val_cnt) {
      // all datum is null
    } else {
      sql::ObBitVector *ref_bitset = nullptr;
      int64_t matched_ref_cnt = 0;
      BUILD_REF_BITSET(ctx, (dict_val_cnt + 1), ref_bitset);

      if (OB_FAIL(ret)) {
      } else if (ctx.col_header_->is_integer_dict()) {
        const ObObjType col_type = ctx.col_header_->get_store_obj_type();
        const bool is_use_base = ctx.int_ctx_->meta_.is_use_base();
        const uint64_t dict_val_base = is_use_base * ctx.int_ctx_->meta_.base_value_;

        if (ctx.dict_meta_->is_sorted()) {
          if (OB_FAIL(sorted_comparison_for_ref(ctx, filter, parent, pd_filter_info, result_bitmap))) {
            LOG_WARN("fail to exe sorted_comparison_for_ref", KR(ret), K(ctx), K(filter_datum), K(pd_filter_info));
          }
        } else {
          const ObObjType col_type = ctx.col_header_->get_store_obj_type();
          common::ObObjMeta filter_val_meta;
          bool is_col_signed = false;
          bool is_filter_signed = false;
          uint64_t filter_val = 0;
          int64_t filter_val_size = 0;
          if (OB_FAIL(filter.get_filter_node().get_filter_val_meta(filter_val_meta))) {
            LOG_WARN("fail to get filter meta", K(ret));
          } else {
            if (ObCSDecodingUtil::can_convert_to_integer(col_type, is_col_signed) &&
                ObCSDecodingUtil::check_datum_not_over_8bytes(filter_val_meta.get_type(),
                    filter_datum, is_filter_signed, filter_val, filter_val_size)) {
              if (OB_FAIL(integer_dict_val_cmp_op(ctx, op_type, is_col_signed, filter_val,
                  filter_val_size, is_filter_signed, dict_val_cnt, ref_bitset, matched_ref_cnt))) {
                LOG_WARN("fail to exe integer_dict_val_cmp_op", KR(ret), K(op_type), K(filter_datum));
              }
            } else if (OB_FAIL(datum_dict_val_cmp_op(ctx, filter, dict_val_cnt, ref_bitset, matched_ref_cnt))) {
              LOG_WARN("fail to cmp dict datum", K(ret), K(ctx), K(filter), K(dict_val_cnt));
            }
          }
        }
      } else {
        if (ctx.dict_meta_->is_sorted()) {
          if (OB_FAIL(sorted_comparison_for_ref(ctx, filter, parent, pd_filter_info, result_bitmap))) {
            LOG_WARN("fail to execute sorted_comparison_for_ref", KR(ret), K(op_type), K(filter_datum), K(pd_filter_info));
          }
        } else if (OB_FAIL(datum_dict_val_cmp_op(ctx, filter, dict_val_cnt, ref_bitset, matched_ref_cnt))) {
          LOG_WARN("fail to cmp dict datum", K(ret), K(ctx), K(filter), K(dict_val_cnt));
        }
      }

      if (OB_SUCC(ret) && (matched_ref_cnt > 0)) {
        const uint32_t ref_width_size = ctx.ref_ctx_->meta_.get_uint_width_size();
        if (OB_FAIL(set_bitmap_with_bitset(ref_width_size, ctx.ref_data_, ref_bitset, pd_filter_info.start_,
            pd_filter_info.count_, false/*has_null*/, 0, parent, result_bitmap))) {
          LOG_WARN("fail to set result bitmap", KR(ret), K(ref_width_size), K(pd_filter_info));
        }
      }
    }
  }
  return ret;
}

int ObDictColumnDecoder::integer_dict_val_cmp_op(
    const ObDictColumnDecoderCtx &ctx,
    const sql::ObWhiteFilterOperatorType &op_type,
    const bool is_col_signed,
    const uint64_t filter_val,
    const int64_t filter_val_size,
    const bool is_filter_signed,
    const uint64_t dict_val_cnt,
    sql::ObBitVector *ref_bitset,
    int64_t &matched_ref_cnt)
{
  int ret = OB_SUCCESS;
  const ObIntegerStreamMeta &stream_meta = ctx.int_ctx_->meta_;
  const uint64_t dict_val_base = stream_meta.is_use_base() * stream_meta.base_value();
  uint64_t filter_val_base_diff = 0;
  bool is_less_than_base = ObCSDecodingUtil::is_less_than_with_diff(
      filter_val, filter_val_size, is_filter_signed, dict_val_base, 8, is_col_signed, filter_val_base_diff);

  if (is_less_than_base) {
    // if filter_val less than base, it must be less than all row value
    if (sql::WHITE_OP_NE == op_type || sql::WHITE_OP_GT == op_type || sql::WHITE_OP_GE == op_type) {
      // the bitmap is ALL_TRUE
      matched_ref_cnt = dict_val_cnt;
      ref_bitset->set_all(matched_ref_cnt);
    } else if (sql::WHITE_OP_EQ == op_type || sql::WHITE_OP_LT == op_type || sql::WHITE_OP_LE == op_type) {
      // the bitmap is ALL_FALSE
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected operator type", KR(ret), K(op_type));
    }
  } else {
    const uint32_t store_width_size = stream_meta.get_uint_width_size();
    const bool is_exceed_range = ~INTEGER_MASK_TABLE[store_width_size] & filter_val_base_diff;
    if (is_exceed_range) {
      // filter_val larger than RANGE_MAX_VALUE, it must larger than all row value
      if (sql::WHITE_OP_EQ == op_type || sql::WHITE_OP_GT == op_type || sql::WHITE_OP_GE == op_type) {
        // the bitmap is ALL_FALSE
      } else if (sql::WHITE_OP_NE == op_type || sql::WHITE_OP_LT == op_type || sql::WHITE_OP_LE == op_type) {
        // the bitmap is ALL_TRUE
        matched_ref_cnt = dict_val_cnt;
        ref_bitset->set_all(matched_ref_cnt);
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected operator type", KR(ret), K(op_type));
      }
    } else if (OB_FAIL(integer_dict_val_cmp_func(store_width_size, 1, &filter_val_base_diff,
        dict_val_cnt, ctx.int_data_, op_type, matched_ref_cnt, ref_bitset))) {
      LOG_WARN("fail to execute integer type dict_val cmp", KR(ret), K(filter_val), K(op_type));
    }
  }
  return ret;
}

int ObDictColumnDecoder::datum_dict_val_cmp_op(
    const ObDictColumnDecoderCtx &ctx,
    const sql::ObWhiteFilterExecutor &filter,
    const uint64_t dict_val_cnt,
    sql::ObBitVector *ref_bitset,
    int64_t &matched_ref_cnt)
{
  int ret = OB_SUCCESS;
  const common::ObCmpOp &cmp_op = sql::ObPushdownWhiteFilterNode::WHITE_OP_TO_CMP_OP[filter.get_op_type()];
  const ObDatum &filter_datum = filter.get_datums().at(0);

  int64_t dict_ref = 0;
  ObDictValueIterator begin_it = ObDictValueIterator(&ctx, 0);
  ObDictValueIterator end_it = ObDictValueIterator(&ctx, dict_val_cnt);
  bool cmp_ret = false;
  while (OB_SUCC(ret) && begin_it != end_it) {
    if (OB_FAIL(compare_datum(*begin_it, filter_datum, filter.cmp_func_, cmp_op, cmp_ret))) {
        LOG_WARN("Failed to compare datum", K(ret), K(*begin_it), K(filter_datum), K(cmp_op));
    } else if (cmp_ret) {
      ref_bitset->set(dict_ref);
      ++matched_ref_cnt;
    }
    ++begin_it;
    ++dict_ref;
  }
  return ret;
}

int ObDictColumnDecoder::sorted_comparison_for_ref(
    const ObDictColumnDecoderCtx &ctx,
    const sql::ObWhiteFilterExecutor &filter,
    const sql::ObPushdownFilterExecutor *parent,
    const sql::PushdownFilterInfo &pd_filter_info,
    ObBitmap &result_bitmap)
{
  int ret = OB_SUCCESS;
  const bool has_null = ctx.dict_meta_->has_null();
  const uint64_t dict_val_cnt = ctx.dict_meta_->distinct_val_cnt_;
  const uint8_t ref_width_tag = ctx.ref_ctx_->meta_.width_;
  const uint32_t ref_width_size = ctx.ref_ctx_->meta_.get_uint_width_size();
  ObCmpFunc cmp_func;
  cmp_func.cmp_func_ = filter.cmp_func_;
  const sql::ObWhiteFilterOperatorType op_type = filter.get_op_type();
  const ObDatum &filter_datum = filter.get_datums().at(0);
  ObDictValueIterator begin_it = ObDictValueIterator(&ctx, 0);
  ObDictValueIterator end_it = ObDictValueIterator(&ctx, dict_val_cnt);

  bool can_fast_cmp = cs_dict_fast_cmp_funcs_inited;

  int64_t bound_ref = -1;
  switch (op_type) {
    case sql::WHITE_OP_LT:
    case sql::WHITE_OP_GE: {
      bound_ref = std::lower_bound(begin_it, end_it, filter_datum,
		  [&cmp_func, &ret](const ObDatum &datum, const ObDatum &filter_datum)
                  -> bool {
                    int cmp_ret = 0;
                    if (OB_FAIL(ret)) {
                    } else if (OB_FAIL(cmp_func.cmp_func_(datum, filter_datum, cmp_ret))) {
                      LOG_WARN("failed to compare datums", K(ret), K(datum), K(filter_datum));
                    }
                    return cmp_ret < 0;}) - begin_it;
      break;
    }
    case sql::WHITE_OP_LE:
    case sql::WHITE_OP_GT: {
      bound_ref = std::upper_bound(begin_it, end_it, filter_datum,
                  [&cmp_func, &ret](const ObDatum &filter_datum, const ObDatum &datum)
                  -> bool {
		    int cmp_ret = 0;
		    if (OB_FAIL(ret)) {
		    } else if (OB_FAIL(cmp_func.cmp_func_(datum, filter_datum, cmp_ret))) {
		      LOG_WARN("failed to compare datums", K(ret), K(datum), K(filter_datum));
		    }
		    return cmp_ret > 0;}) - begin_it;
      break;
    }
    default: {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected type for dict string", KR(ret), K(op_type));
      break;
    }
  }


  // NOTICE: for LE, we use upper_bound, the result should not contain this bound, thus convert to LT;
  //         for GT, we use upper_bound, the result shoud contain this bound, thus convert to GE
  sql::ObWhiteFilterOperatorType cmp_op_type = (op_type == sql::WHITE_OP_LE || op_type == sql::WHITE_OP_LT) ?
                                                sql::WHITE_OP_LT : sql::WHITE_OP_GE;
  if(OB_FAIL(ret)) {
  } else if (can_fast_cmp) {
    if (OB_FAIL(fast_cmp_ref_and_set_result(bound_ref, dict_val_cnt, ctx.ref_data_, ref_width_tag, cmp_op_type,
        pd_filter_info, result_bitmap))) {
      LOG_WARN("fail to fast_cmp_ref_and_set_result", KR(ret), K(ref_width_tag), K(bound_ref), K(op_type),
        K(cmp_op_type));
    }
  } else {
    if (OB_FAIL(cmp_ref_and_set_result(ref_width_size, ctx.ref_data_, bound_ref, has_null, dict_val_cnt,
        cmp_op_type, pd_filter_info.start_, pd_filter_info.count_, parent, result_bitmap))) {
      LOG_WARN("fail to cmp_ref_and_set_result", KR(ret), K(ref_width_size), K(bound_ref), K(op_type),
        K(cmp_op_type), K(has_null), K(pd_filter_info));
    }
  }
  return ret;
}

int ObDictColumnDecoder::in_operator(
    const ObDictColumnDecoderCtx &ctx,
    const sql::ObPushdownFilterExecutor *parent,
    const sql::ObWhiteFilterExecutor &filter,
    const sql::PushdownFilterInfo &pd_filter_info,
    common::ObBitmap &result_bitmap)
{
  int ret = OB_SUCCESS;
  int64_t datums_cnt = 0;
  const uint64_t dict_val_cnt = ctx.dict_meta_->distinct_val_cnt_;
  const bool is_sorted_dict = ctx.dict_meta_->is_sorted();
  if (OB_UNLIKELY((datums_cnt = filter.get_datums().count()) < 1)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(datums_cnt));
  } else if (0 == dict_val_cnt) {
    // all datum is null
  } else {
    const sql::ObWhiteFilterOperatorType op_type = filter.get_op_type();
    sql::ObBitVector *ref_bitset = nullptr;
    bool matched_ref_exist = false;
    BUILD_REF_BITSET(ctx, (dict_val_cnt + 1), ref_bitset);

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(datum_dict_val_in_op(ctx, filter, is_sorted_dict, ref_bitset, matched_ref_exist))) {
      LOG_WARN("Failed to exe datum_dict_val_in_op", KR(ret), K(dict_val_cnt));
    } else if (matched_ref_exist) {
      const uint32_t ref_width_size = ctx.ref_ctx_->meta_.get_uint_width_size();
      if (OB_FAIL(set_bitmap_with_bitset(ref_width_size, ctx.ref_data_, ref_bitset,
          pd_filter_info.start_, pd_filter_info.count_, false/*has_null*/, 0, parent, result_bitmap))) {
        LOG_WARN("fail to set result bitmap", KR(ret), K(ref_width_size), K(pd_filter_info));
      }
    }
  }
  return ret;
}

int ObDictColumnDecoder::datum_dict_val_in_op(
    const ObDictColumnDecoderCtx &ctx,
    const sql::ObWhiteFilterExecutor &filter,
    const bool is_sorted_dict,
    sql::ObBitVector *ref_bitset,
    bool &matched_ref_exist,
    const bool is_const_result_set)
{
  int ret = OB_SUCCESS;
  const uint64_t dict_val_cnt = ctx.dict_meta_->distinct_val_cnt_;
  ObDictValueIterator min_it = ObDictValueIterator(&ctx, 0);
  ObDictValueIterator max_it = ObDictValueIterator(&ctx, dict_val_cnt > 0 ? dict_val_cnt - 1 : 0);

  bool hit_shortcut = false;
  int cmp_ret = 0;
  if (filter.null_param_contained()) {
    hit_shortcut = true;
  } else if (is_sorted_dict) {
    if (OB_FAIL(filter.cmp_func_(*min_it, filter.get_max_param(), cmp_ret))) {
      LOG_WARN("Failed to compare min dict value and max param", KR(ret), K(*min_it), K(filter.get_max_param()));
    } else if (cmp_ret > 0) {
      hit_shortcut = true;
    } else if (OB_FAIL(filter.cmp_func_(*max_it, filter.get_min_param(), cmp_ret))) {
      LOG_WARN("Failed to compare max dict value and min param", KR(ret), K(*max_it), K(filter.get_min_param()));
    } else if (cmp_ret < 0) {
      hit_shortcut = true;
    }
  }

  if (OB_FAIL(ret)) {
  } else if (hit_shortcut) {
    if (is_const_result_set) {
      matched_ref_exist = true;
      ref_bitset->bit_not(dict_val_cnt + 1); // all 1
    }
    LOG_DEBUG("Hit shortcut to judge IN filter");
  } else {
    const ObFilterInCmpType cmp_type = get_filter_in_cmp_type(dict_val_cnt, filter.get_datums().count(), is_sorted_dict);
    int64_t matched_ref_cnt = 0;
    switch (cmp_type) {
      case ObFilterInCmpType::MERGE_SEARCH: {
        if (OB_FAIL(in_operator_merge_search(ctx, filter, ref_bitset,
                                             matched_ref_cnt,
                                             is_const_result_set))) {
          LOG_WARN("Failed to merge search in IN operator", KR(ret));
        }
        break;
      }
      case ObFilterInCmpType::BINARY_SEARCH_DICT: {
        if (OB_FAIL(in_operator_binary_search_dict(ctx, filter, ref_bitset,
                                                   matched_ref_cnt,
                                                   is_const_result_set))) {
          LOG_WARN("Failed to binary search dict in IN operator", KR(ret));
        }
        break;
      }
      case ObFilterInCmpType::BINARY_SEARCH: {
        if (OB_FAIL(in_operator_binary_search(ctx, filter, ref_bitset,
                                              matched_ref_cnt,
                                              is_const_result_set))) {
          LOG_WARN("Failed to binary search in IN operator", KR(ret));
        }
        break;
      }
      case ObFilterInCmpType::HASH_SEARCH: {
        if (OB_FAIL(in_operator_hash_search(ctx, filter, ref_bitset,
                                            matched_ref_cnt,
                                            is_const_result_set))) {
          LOG_WARN("Failed to hash search in IN operator", KR(ret));
        }
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Unexpected compare type", K(ret), K(cmp_type));
      }
    }
    if (OB_SUCC(ret)) {
      if (is_const_result_set) {
        matched_ref_exist = matched_ref_cnt < dict_val_cnt;
        ref_bitset->bit_not(dict_val_cnt + 1);
      } else {
        matched_ref_exist = matched_ref_cnt > 0;
      }
    }
  }
  return ret;
}

int ObDictColumnDecoder::in_operator_merge_search(
    const ObDictColumnDecoderCtx &ctx,
    const sql::ObWhiteFilterExecutor &filter,
    sql::ObBitVector *ref_bitset,
    int64_t &matched_ref_cnt,
    const bool is_const_result_set)
{
  int ret = OB_SUCCESS;
  const uint64_t dict_val_cnt = ctx.dict_meta_->distinct_val_cnt_;
  ObDictValueIterator begin_it = ObDictValueIterator(&ctx, 0);
  ObDictValueIterator end_it = ObDictValueIterator(&ctx, dict_val_cnt);
  ObDictValueIterator trav_it = begin_it;
  const ObFixedArray<ObDatum, ObIAllocator> *params
          = static_cast<const ObFixedArray<ObDatum, ObIAllocator> *>(&(filter.get_datums()));
  array::Iterator<ObFixedArrayImpl<ObDatum, ObIAllocator>, const ObDatum> param_it = params->begin();
  int cmp_ret = 0;
  bool equal = false;
  ObDatumComparator cmp(filter.cmp_func_, ret, equal);
  ObDatumComparator cmp_rev(filter.cmp_func_rev_, ret, equal);
  while (OB_SUCC(ret) && trav_it != end_it && param_it != params->end()) {
    const ObDatum dict_datum = *trav_it;
    const ObDatum param_datum = *param_it;
    if (equal) {
      cmp_ret = 0;
    } else if (OB_FAIL(filter.cmp_func_(dict_datum, param_datum, cmp_ret))) {
      LOG_WARN("Failed to compare dict and param datum", K(ret));
    }
    if (OB_SUCC(ret)) {
      equal = false;
      if (cmp_ret == 0) {
        ++matched_ref_cnt;
        ref_bitset->set(trav_it - begin_it);
        // The values in the dictionary and array are unique.
        ++trav_it;
        ++param_it;
      } else if (cmp_ret > 0) {
        ++param_it;
        param_it = std::lower_bound(param_it, params->end(), dict_datum, cmp_rev);
      } else {
        ++trav_it;
        trav_it = std::lower_bound(trav_it, end_it, param_datum, cmp);
      }
      if (OB_FAIL(ret)) {
        LOG_WARN("Failed to find next compare positions", K(ret));
      }
    }
  }
  return ret;
}

int ObDictColumnDecoder::in_operator_binary_search_dict(
    const ObDictColumnDecoderCtx &ctx,
    const sql::ObWhiteFilterExecutor &filter,
    sql::ObBitVector *ref_bitset,
    int64_t &matched_ref_cnt,
    const bool is_const_result_set)
{
  int ret = OB_SUCCESS;
  const uint64_t dict_val_cnt = ctx.dict_meta_->distinct_val_cnt_;
  ObDictValueIterator begin_it = ObDictValueIterator(&ctx, 0);
  ObDictValueIterator end_it = ObDictValueIterator(&ctx, dict_val_cnt);
  ObDictValueIterator trav_it = begin_it;
  const ObFixedArray<ObDatum, ObIAllocator> *params
          = static_cast<const ObFixedArray<ObDatum, ObIAllocator> *>(&(filter.get_datums()));
  array::Iterator<ObFixedArrayImpl<ObDatum, ObIAllocator>, const ObDatum> param_it = params->begin();
  bool is_exist = false;
  ObDatumComparator cmp(filter.cmp_func_, ret, is_exist);
  while (OB_SUCC(ret) && trav_it != end_it && param_it != params->end()) {
    const ObDatum param_datum = *param_it;
    is_exist = false;
    trav_it = std::lower_bound(trav_it, end_it, param_datum, cmp);
    if (OB_FAIL(ret)) {
      LOG_WARN("Failed to get lower_bound in dict", K(ret), K(param_datum));
    } else if (trav_it == end_it) {
    } else if (is_exist) {
      ++matched_ref_cnt;
      ref_bitset->set(trav_it - begin_it);
      ++trav_it;
    }
    ++param_it;
  }
  return ret;
}

int ObDictColumnDecoder::in_operator_binary_search(
    const ObDictColumnDecoderCtx &ctx,
    const sql::ObWhiteFilterExecutor &filter,
    sql::ObBitVector *ref_bitset,
    int64_t &matched_ref_cnt,
    const bool is_const_result_set)
{
  int ret = OB_SUCCESS;
  const uint64_t dict_val_cnt = ctx.dict_meta_->distinct_val_cnt_;
  ObDictValueIterator begin_it = ObDictValueIterator(&ctx, 0);
  ObDictValueIterator end_it = ObDictValueIterator(&ctx, dict_val_cnt);
  ObDictValueIterator trav_it = begin_it;
  const ObFixedArray<ObDatum, ObIAllocator> *params
          = static_cast<const ObFixedArray<ObDatum, ObIAllocator> *>(&(filter.get_datums()));
  array::Iterator<ObFixedArrayImpl<ObDatum, ObIAllocator>, const ObDatum> param_it = params->begin();
  bool is_exist = false;
  while (OB_SUCC(ret) && trav_it != end_it && param_it != params->end()) {
    const ObDatum dict_datum = *trav_it;
    if (OB_FAIL(filter.exist_in_datum_array(dict_datum, is_exist, param_it - params->begin()))) {
      LOG_WARN("Failed to check dict datum in param array", K(ret), K(dict_datum));
    } else if (is_exist) {
      ++matched_ref_cnt;
      ref_bitset->set(trav_it - begin_it);
      ++param_it;
    }
    ++trav_it;
  }
  return ret;
}

int ObDictColumnDecoder::in_operator_hash_search(
    const ObDictColumnDecoderCtx &ctx,
    const sql::ObWhiteFilterExecutor &filter,
    sql::ObBitVector *ref_bitset,
    int64_t &matched_ref_cnt,
    const bool is_const_result_set)
{
  int ret = OB_SUCCESS;
  const uint64_t dict_val_cnt = ctx.dict_meta_->distinct_val_cnt_;
  ObDictValueIterator begin_it = ObDictValueIterator(&ctx, 0);
  ObDictValueIterator end_it = ObDictValueIterator(&ctx, dict_val_cnt);
  ObDictValueIterator trav_it = begin_it;
  const ObFixedArray<ObDatum, ObIAllocator> *params
          = static_cast<const ObFixedArray<ObDatum, ObIAllocator> *>(&(filter.get_datums()));
  array::Iterator<ObFixedArrayImpl<ObDatum, ObIAllocator>, const ObDatum> param_it = params->begin();
  bool is_exist = false;
  while (OB_SUCC(ret) && trav_it != end_it && param_it != params->end()) {
    const ObDatum dict_datum = *trav_it;
    if (OB_FAIL(filter.exist_in_datum_set(dict_datum, is_exist))) {
      LOG_WARN("Failed to check dict datum in param set", K(ret), K(dict_datum));
    } else if (is_exist) {
      ++matched_ref_cnt;
      ref_bitset->set(trav_it - begin_it);
    }
    ++trav_it;
  }
  return ret;
}

int ObDictColumnDecoder::bt_operator(
    const ObDictColumnDecoderCtx &ctx,
    const sql::ObPushdownFilterExecutor *parent,
    const sql::ObWhiteFilterExecutor &filter,
    const sql::PushdownFilterInfo &pd_filter_info,
    common::ObBitmap &result_bitmap)
{
  int ret = OB_SUCCESS;
  const uint64_t dict_val_cnt = ctx.dict_meta_->distinct_val_cnt_;
  int64_t datums_cnt = 0;
  if (OB_UNLIKELY((datums_cnt = filter.get_datums().count()) != 2)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(datums_cnt), K(ctx));
  } else if (0 == dict_val_cnt) {
    // all datum is null
  } else {
    const sql::ObWhiteFilterOperatorType op_type = filter.get_op_type();
    const bool has_null = ctx.dict_meta_->has_null();
    const ObDatum &left_ref_datum = filter.get_datums().at(0);
    const ObDatum &right_ref_datum = filter.get_datums().at(1);

    sql::ObBitVector *ref_bitset = nullptr;
    BUILD_REF_BITSET(ctx, (dict_val_cnt + 1), ref_bitset);

    int64_t matched_ref_cnt = 0;
    if (OB_FAIL(ret)) {
    } else if (ctx.col_header_->is_integer_dict()) {
      if (ctx.dict_meta_->is_sorted()) {
        if (OB_FAIL(sorted_between_for_ref(ctx, dict_val_cnt, filter, parent, pd_filter_info, result_bitmap))) {
          LOG_WARN("fail to exe sorted_between_for_ref", KR(ret), K(dict_val_cnt), K(pd_filter_info));
        }
      } else {
        bool is_signed_data = false;
        if (ObCSDecodingUtil::can_convert_to_integer(ctx.col_header_->get_store_obj_type(), is_signed_data)) {
          if (OB_FAIL(integer_dict_val_bt_op(ctx, dict_val_cnt, ref_bitset, filter, pd_filter_info, matched_ref_cnt))) {
            LOG_WARN("fail to exe integer_dict_val_bt_op", KR(ret), K(dict_val_cnt), K(pd_filter_info));
          }
        } else {
          if (OB_FAIL(datum_dict_val_bt_op(ctx, filter, left_ref_datum, right_ref_datum, dict_val_cnt, ref_bitset, matched_ref_cnt))) {
	          LOG_WARN("fail to exe datum_dict_val_bt_op", KR(ret), K(ctx), K(filter), K(dict_val_cnt));
	        }
        }
      }
    } else {
      if (ctx.dict_meta_->is_sorted()) {
        if (OB_FAIL(sorted_between_for_ref(ctx, dict_val_cnt, filter, parent, pd_filter_info, result_bitmap))) {
          LOG_WARN("fail to exe sorted_between_for_ref", KR(ret), K(dict_val_cnt), K(pd_filter_info));
        }
      } else {
        if (OB_FAIL(datum_dict_val_bt_op(ctx, filter, left_ref_datum, right_ref_datum, dict_val_cnt, ref_bitset, matched_ref_cnt))) {
	        LOG_WARN("fail to exe datum_dict_val_bt_op", KR(ret), K(ctx), K(filter), K(dict_val_cnt));
	      }
      }
    }

    if(OB_SUCC(ret) && (matched_ref_cnt > 0)) {
      const uint32_t ref_width_size = ctx.ref_ctx_->meta_.get_uint_width_size();
      if (OB_FAIL(set_bitmap_with_bitset(ref_width_size, ctx.ref_data_, ref_bitset, pd_filter_info.start_,
          pd_filter_info.count_, false, 0, parent, result_bitmap))) {
        LOG_WARN("fail to set result bitmap", KR(ret), K(ref_width_size), K(pd_filter_info));
      }
    }
  }
  return ret;
}

int ObDictColumnDecoder::integer_dict_val_bt_op(
    const ObDictColumnDecoderCtx &ctx,
    const uint64_t dict_val_cnt,
    sql::ObBitVector *ref_bitset,
    const sql::ObWhiteFilterExecutor &filter,
    const sql::PushdownFilterInfo &pd_filter_info,
    int64_t &matched_ref_cnt)
{
  int ret = OB_SUCCESS;
  const ObObjType col_type = ctx.col_header_->get_store_obj_type();
  const bool is_col_signed = ObCSDecodingUtil::is_signed_object_type(col_type);
  const ObIntegerStreamMeta &stream_meta = ctx.int_ctx_->meta_;
  const uint64_t dict_val_base = stream_meta.is_use_base() * stream_meta.base_value();

  const sql::ObPushdownWhiteFilterNode &filter_node = filter.get_filter_node();
  ObObjType left_filter_type = filter_node.get_filter_arg_obj_type(0);
  ObObjType right_filter_type = filter_node.get_filter_arg_obj_type(1);
  bool is_left_signed = false;
  bool is_right_signed = false;
  int64_t left_filter_size = 0;
  int64_t right_filter_size = 0;
  uint64_t left_filter_val = 0;
  uint64_t right_filter_val = 0;
  // if filter dutum over 8bytes, it will be cast to the border value(int64_min/int64_max)
  ObCSDecodingUtil::check_datum_not_over_8bytes(left_filter_type, filter.get_datums().at(0), is_left_signed, left_filter_val, left_filter_size);
  ObCSDecodingUtil::check_datum_not_over_8bytes(right_filter_type, filter.get_datums().at(1), is_right_signed, right_filter_val, right_filter_size);
  // 1. left_filter > right_filter, the bt range is invalid.
  // 2. right_filter < dict_val_base, no value will between this range, cuz dict_val_base is minimum
  bool is_param_invalid = ObCSDecodingUtil::is_less_than(right_filter_val, right_filter_size, is_right_signed,
                                                         left_filter_val, left_filter_size, is_left_signed);
  uint64_t right_filter_base_diff = 0;
  bool is_right_less_than_base = ObCSDecodingUtil::is_less_than_with_diff(right_filter_val, right_filter_size,
                                  is_right_signed, dict_val_base, 8, is_col_signed, right_filter_base_diff);
  if (is_param_invalid || is_right_less_than_base) {
    // skip
  } else {
    const uint32_t store_width_size = ctx.ref_ctx_->meta_.get_uint_width_size();
    const int64_t row_cnt = pd_filter_info.count_;
    const int64_t row_start = pd_filter_info.start_;

    bool need_tranverse = true;
    uint64_t left_filter_base_diff = 0;
    bool is_left_less_than_base = ObCSDecodingUtil::is_less_than_with_diff(left_filter_val, left_filter_size,
                                  is_left_signed, dict_val_base, 8, is_col_signed, left_filter_base_diff);
    if (is_left_less_than_base) {
      left_filter_base_diff = 0; // ori_left < base, let 'left = 0'
    } else if (~INTEGER_MASK_TABLE[store_width_size] & left_filter_base_diff) {
      need_tranverse = false; // left > MAX_RANGE, no value will match
    }

    if (need_tranverse) {
      if (~INTEGER_MASK_TABLE[store_width_size] & right_filter_base_diff) {
        right_filter_val = INTEGER_MASK_TABLE[store_width_size]; // right > MAX_RANGE, right = MAX_RANGE.
      }
      uint64_t filter_vals[] = {left_filter_base_diff, right_filter_base_diff};

      if (OB_FAIL(integer_dict_val_cmp_func(store_width_size, 2, filter_vals, dict_val_cnt,
          ctx.int_data_, sql::WHITE_OP_BT, matched_ref_cnt, ref_bitset))) {
        LOG_WARN("fail to execute integer type dict_val cmp", KR(ret), K(left_filter_base_diff), K(right_filter_base_diff));
      }
    }
  }
  return ret;
}

int ObDictColumnDecoder::sorted_between_for_ref(
    const ObDictColumnDecoderCtx &ctx,
    const uint64_t dict_val_cnt,
    const sql::ObWhiteFilterExecutor &filter,
    const sql::ObPushdownFilterExecutor *parent,
    const sql::PushdownFilterInfo &pd_filter_info,
    common::ObBitmap &result_bitmap)
{
  int ret = OB_SUCCESS;
  ObCmpFunc cmp_func;
  cmp_func.cmp_func_ = filter.cmp_func_;
  ObDictValueIterator begin_it = ObDictValueIterator(&ctx, 0);
  ObDictValueIterator end_it = ObDictValueIterator(&ctx, dict_val_cnt);

  int64_t left_ref_inclusive = std::lower_bound(begin_it, end_it, filter.get_datums().at(0),
                  [&cmp_func, &ret](const ObDatum &datum, const ObDatum &filter_datum)
                  -> bool {
                  int cmp_ret = 0;
                  if (OB_FAIL(ret)) {
                  } else if (OB_FAIL(cmp_func.cmp_func_(datum, filter_datum, cmp_ret))) {
                  LOG_WARN("failed to comapre datums", K(ret), K(datum), K(filter_datum));
                  }
                  return cmp_ret  < 0;}) - begin_it;
  int64_t right_ref_exclusive = std::upper_bound(begin_it, end_it, filter.get_datums().at(1),
                  [&cmp_func, &ret](const ObDatum &filter_datum, const ObDatum &datum)
                  -> bool {
                  int cmp_ret = 0;
                  if (OB_FAIL(ret)) {
                  } else if (OB_FAIL(cmp_func.cmp_func_(datum, filter_datum, cmp_ret))) {
                  LOG_WARN("failed to compare datums", K(ret), K(datum), K(filter_datum));
                  }
                  return cmp_ret > 0;}) - begin_it;

  const uint32_t ref_width_size = ctx.ref_ctx_->meta_.get_uint_width_size();
  const int64_t row_cnt = pd_filter_info.count_;
  const int64_t row_start = pd_filter_info.start_;
  int64_t refs_val[2] = {left_ref_inclusive, right_ref_exclusive};
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ObCSFilterFunctionFactory::instance().dict_ref_sort_bt_tranverse(ctx.ref_data_, dict_val_cnt, refs_val,
          row_start, row_cnt, parent, ref_width_size, result_bitmap))) {
    LOG_WARN("fail to exe dict_ref_sort_bt_tranverse", KR(ret), K(ref_width_size), K(dict_val_cnt), K(left_ref_inclusive),
        K(right_ref_exclusive));
  }


  return ret;
}

int ObDictColumnDecoder::datum_dict_val_bt_op(
    const ObDictColumnDecoderCtx &ctx,
    const sql::ObWhiteFilterExecutor &filter,
    const ObDatum &left_ref_datum,
    const ObDatum &right_ref_datum,
    const uint64_t dict_val_cnt,
    sql::ObBitVector *ref_bitset,
    int64_t &matched_ref_cnt)
{
  int ret = OB_SUCCESS;
  ObDatumCmpFuncType cmp_func = filter.cmp_func_;

  int64_t dict_ref = 0;
  ObDictValueIterator begin_it = ObDictValueIterator(&ctx, 0);
  ObDictValueIterator end_it = ObDictValueIterator(&ctx, dict_val_cnt);
  while (OB_SUCC(ret) && begin_it != end_it) {
    int left_cmp_ret = 0;
    int right_cmp_ret = 0;
    if (OB_FAIL(cmp_func(*begin_it, left_ref_datum, left_cmp_ret))) {
      LOG_WARN("fail to compare datums", K(ret), K(*begin_it), K(left_ref_datum));
    } else if (left_cmp_ret < 0) {
      // skip
    } else if (OB_FAIL(cmp_func(*begin_it, right_ref_datum, right_cmp_ret))) {
      LOG_WARN("fail to compare datums", K(ret), K(*begin_it), K(right_ref_datum));
    } else if (right_cmp_ret > 0) {
      //skip
    } else {
      ref_bitset->set(dict_ref);
      ++matched_ref_cnt;
    }
    ++begin_it;
    ++dict_ref;
  }
  return ret;
}

//const encoding filter
int ObDictColumnDecoder::do_const_only_operator(
    const ObDictColumnDecoderCtx &ctx,
    const ObConstEncodingRefDesc &const_ref_desc,
    const sql::ObWhiteFilterExecutor &filter,
    const sql::PushdownFilterInfo &pd_filter_info,
    ObBitmap &result_bitmap)
{
  int ret = OB_SUCCESS;
  ObDictValueIterator dict_iter = ObDictValueIterator(&ctx, 0);
  ObStorageDatum &const_datum = *dict_iter;
  const common::ObIArray<common::ObDatum> &ref_datums = filter.get_datums();
  ObDatumCmpFuncType cmp_func = filter.cmp_func_;

  switch (filter.get_op_type()) {
    case sql::WHITE_OP_NU: {
      if (1 == const_ref_desc.const_ref_) {
        if (OB_FAIL(result_bitmap.bit_not())) {
          LOG_WARN("fail to execute bit_not", KR(ret));
        }
      }
      break;
    }
    case sql::WHITE_OP_NN: {
      if (0 == const_ref_desc.const_ref_) {
        if (OB_FAIL(result_bitmap.bit_not())) {
          LOG_WARN("fail to execute bit_not", KR(ret));
        }
      }
      break;
    }
    case sql::WHITE_OP_EQ:
    case sql::WHITE_OP_NE:
    case sql::WHITE_OP_GT:
    case sql::WHITE_OP_LT:
    case sql::WHITE_OP_GE:
    case sql::WHITE_OP_LE: {
      bool cmp_ret = false;
      if (OB_UNLIKELY(ref_datums.count() != 1 || filter.null_param_contained())) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid argument", KR(ret), K(filter), K(ref_datums.count()));
      } else if (1 == const_ref_desc.const_ref_) {
        // all null, skip
      } else if (OB_FAIL(compare_datum(const_datum, ref_datums.at(0), cmp_func,
        sql::ObPushdownWhiteFilterNode::WHITE_OP_TO_CMP_OP[filter.get_op_type()], cmp_ret))) {
          LOG_WARN("Failed to compare datum", K(ret), K(const_datum), K(ref_datums.at(0)),
              K(sql::ObPushdownWhiteFilterNode::WHITE_OP_TO_CMP_OP[filter.get_op_type()]));
      } else if (cmp_ret) {
        if (OB_FAIL(result_bitmap.bit_not())) {
          LOG_WARN("fail to execute bit_not", KR(ret));
        }
      }
      break;
    }
    case sql::WHITE_OP_BT: {
      int left_cmp_ret = 0;
      int right_cmp_ret = 0;
      if (OB_UNLIKELY(ref_datums.count() != 2)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid argument", KR(ret), K(ref_datums));
      } else if (1 == const_ref_desc.const_ref_) {
      } else if (OB_FAIL(cmp_func(const_datum, ref_datums.at(0), left_cmp_ret))) {
        LOG_WARN("failed to compare datums", K(ret), K(const_datum), K(ref_datums.at(0)));
      } else if (left_cmp_ret < 0) {
        // skip
      } else if (OB_FAIL(cmp_func(const_datum, ref_datums.at(1), right_cmp_ret))) {
        LOG_WARN("failed to compare datums", K(ret), K(const_datum), K(ref_datums.at(1)));
      } else if (right_cmp_ret > 0) {
        // skip
      } else if (OB_FAIL(result_bitmap.bit_not())) {
        LOG_WARN("fail to execute bit_not", KR(ret));
      }
      break;
    }
    case sql::WHITE_OP_IN: {
      if (OB_UNLIKELY(ref_datums.count() == 0)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid argument", KR(ret), K(ref_datums));
      } else if (1 == const_ref_desc.const_ref_) {
      } else {
        bool is_existed = false;
        if (OB_FAIL(filter.exist_in_datum_set(const_datum, is_existed))) {
          LOG_WARN("fail to check object in hashset", KR(ret), K(const_datum));
        } else if (is_existed) {
          if (OB_FAIL(result_bitmap.bit_not())) {
            LOG_WARN("fail to execute bit_not", KR(ret));
          }
        }
      }
      break;
    }
    default: {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("not support this type", KR(ret), "op_type", filter.get_op_type());
      break;
    }
  }

  return ret;
}

int ObDictColumnDecoder::nu_nn_const_operator(
    const ObDictColumnDecoderCtx &ctx,
    const ObConstEncodingRefDesc &const_ref_desc,
    const sql::ObPushdownFilterExecutor *parent,
    const sql::ObWhiteFilterExecutor &filter,
    const sql::PushdownFilterInfo &pd_filter_info,
    common::ObBitmap &result_bitmap)
{
  int ret = OB_SUCCESS;
  const int64_t dict_val_cnt = ctx.dict_meta_->distinct_val_cnt_;
  const int64_t ref_exception_cnt = const_ref_desc.exception_cnt_;
  const uint32_t ref_width_size = const_ref_desc.width_size_;
  const ObIntArrayFuncTable &const_exception_buf = ObIntArrayFuncTable::instance(ref_width_size);
  int64_t row_id = 0;
  // handle NU first
  if (const_ref_desc.const_ref_ == dict_val_cnt) {
    // 'const_val' is null, exception values are not null, bit not first
    if (OB_FAIL(result_bitmap.bit_not())) {
      LOG_WARN("fail to execute bit_not", KR(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && (i < ref_exception_cnt); ++i) {
      row_id = const_exception_buf.at_(const_ref_desc.exception_row_id_buf_, i);
      if ((row_id >= pd_filter_info.start_)
          && (row_id < pd_filter_info.start_ + pd_filter_info.count_)
          && OB_FAIL(result_bitmap.set(row_id - pd_filter_info.start_, false))) {
        LOG_WARN("fail to set result bitmap", KR(ret), K(i), K(row_id), K(pd_filter_info));
      }
    }
  } else {
    int64_t ref = 0;
    for (int64_t i = 0; OB_SUCC(ret) && (i < ref_exception_cnt); ++i) {
      ref = const_exception_buf.at_(const_ref_desc.exception_ref_buf_, i);
      if (ref == dict_val_cnt) {
        row_id = const_exception_buf.at_(const_ref_desc.exception_row_id_buf_, i);
        if ((row_id >= pd_filter_info.start_)
            && (row_id < pd_filter_info.start_ + pd_filter_info.count_)
            && OB_FAIL(result_bitmap.set(row_id - pd_filter_info.start_))) {
          LOG_WARN("fail to set result bitmap", KR(ret), K(i), K(row_id), K(ref), K(pd_filter_info));
        }
      }
    }
  }

  // if NN, just bit not
  if (OB_SUCC(ret) && (sql::WHITE_OP_NN == filter.get_op_type())) {
    if (OB_FAIL(result_bitmap.bit_not())) {
      LOG_WARN("fail to execute bit_not", KR(ret));
    }
  }
  return ret;
}

int ObDictColumnDecoder::comparison_const_operator(
    const ObDictColumnDecoderCtx &ctx,
    const ObConstEncodingRefDesc &const_ref_desc,
    const sql::ObPushdownFilterExecutor *parent,
    const sql::ObWhiteFilterExecutor &filter,
    const sql::PushdownFilterInfo &pd_filter_info,
    common::ObBitmap &result_bitmap)
{
  int ret = OB_SUCCESS;
  const int64_t dict_val_cnt = ctx.dict_meta_->distinct_val_cnt_;
  const bool has_null = ctx.dict_meta_->has_null();
  const ObDatum &filter_datum = filter.get_datums().at(0);
  const sql::ObWhiteFilterOperatorType op_type = filter.get_op_type();
  ObDatumCmpFuncType cmp_func = filter.cmp_func_;

  bool is_const_result_set = false;
  bool cmp_ret = false;
  if (const_ref_desc.const_ref_ == dict_val_cnt) {
    // skip null
  } else {
    ObDictValueIterator dict_iter = ObDictValueIterator(&ctx, 0);
    ObStorageDatum &const_datum = *(dict_iter + const_ref_desc.const_ref_);
    if (OB_FAIL(compare_datum(const_datum, filter_datum, cmp_func,
               sql::ObPushdownWhiteFilterNode::WHITE_OP_TO_CMP_OP[op_type], cmp_ret))) {
      LOG_WARN("Failed to compare datum", K(ret), K(const_datum), K(filter_datum),
          K(sql::ObPushdownWhiteFilterNode::WHITE_OP_TO_CMP_OP[op_type]));
    } else if (cmp_ret) {
      if (OB_FAIL(result_bitmap.bit_not())) {
        LOG_WARN("fail to execute bit_not", KR(ret));
      } else {
        is_const_result_set = true;
      }
    }
  }

  if (OB_SUCC(ret)) {
    const int64_t ref_bitset_size = dict_val_cnt + 1;
    sql::ObBitVector *ref_bitset = nullptr;
    BUILD_REF_BITSET(ctx, (dict_val_cnt + 1), ref_bitset);

    bool exist_matched_ref = false;
    ObDictValueIterator mv_iter = ObDictValueIterator(&ctx, 0);
    ObDictValueIterator end_iter = ObDictValueIterator(&ctx, dict_val_cnt);
    int64_t tmp_idx = 0;
    // TODO, douglou, decode all dict once or batch
    while (OB_SUCC(ret) && (mv_iter != end_iter)) {
      // why use '!is_const_result_set'?
      // Cuz if we set 'is_const_result_set' as true, we should use ref_bitset to record the
      // unmatched ref; if we don't set, we should use ref_bitset to record the matched ref.
      if (OB_FAIL(compare_datum(*mv_iter, filter_datum, cmp_func,
          sql::ObPushdownWhiteFilterNode::WHITE_OP_TO_CMP_OP[op_type], cmp_ret))) {
        LOG_WARN("Failed to compare datum", K(ret), K(*mv_iter), K(filter_datum),
            K(sql::ObPushdownWhiteFilterNode::WHITE_OP_TO_CMP_OP[op_type]));
      } else if ((!is_const_result_set) == cmp_ret){
        exist_matched_ref = true;
        ref_bitset->set(tmp_idx);
      }
      ++tmp_idx;
      ++mv_iter;
    }

    const bool need_filter_null = is_const_result_set && has_null;
    if (OB_FAIL(ret)) {
    } else if ((exist_matched_ref || need_filter_null) && OB_FAIL(set_bitmap_with_bitset_const(
      const_ref_desc.width_size_, const_ref_desc.exception_row_id_buf_, const_ref_desc.exception_ref_buf_,
      ref_bitset, const_ref_desc.exception_cnt_, pd_filter_info.start_, pd_filter_info.count_,
      need_filter_null, dict_val_cnt, result_bitmap, !is_const_result_set))) {
      LOG_WARN("fail to set bitmap with bitset", KR(ret), K(const_ref_desc), K(exist_matched_ref), K(need_filter_null),
        K(dict_val_cnt), K(pd_filter_info));
    }
  }

  return ret;
}

int ObDictColumnDecoder::bt_const_operator(
    const ObDictColumnDecoderCtx &ctx,
    const ObConstEncodingRefDesc &const_ref_desc,
    const sql::ObPushdownFilterExecutor *parent,
    const sql::ObWhiteFilterExecutor &filter,
    const sql::PushdownFilterInfo &pd_filter_info,
    common::ObBitmap &result_bitmap)
{
  int ret = OB_SUCCESS;
  const int64_t dict_val_cnt = ctx.dict_meta_->distinct_val_cnt_;
  const bool has_null = ctx.dict_meta_->has_null();
  const ObDatum &left_ref_datum = filter.get_datums().at(0);
  const ObDatum &right_ref_datum = filter.get_datums().at(1);
  ObDatumCmpFuncType cmp_func = filter.cmp_func_;


  bool is_const_result_set = false;
  if (const_ref_desc.const_ref_ == dict_val_cnt) {
    // skip null
  } else {
    ObDictValueIterator dict_iter = ObDictValueIterator(&ctx, 0);
    ObStorageDatum &const_datum = *(dict_iter + const_ref_desc.const_ref_);
    int left_cmp_ret = 0;
    int right_cmp_ret = 0;
    if (OB_FAIL(cmp_func(const_datum, left_ref_datum, left_cmp_ret))) {
      LOG_WARN("failed to compare datums", K(ret), K(const_datum), K(left_ref_datum));
    } else if (left_cmp_ret < 0) {
      // skip
    } else if (OB_FAIL(cmp_func(const_datum, right_ref_datum, right_cmp_ret))) {
      LOG_WARN("failed to compare datums", K(ret), K(const_datum), K(right_ref_datum));
    } else if (right_cmp_ret > 0) {
      // skip
    } else if (OB_FAIL(result_bitmap.bit_not())) {
      LOG_WARN("fail to execute bit_not", KR(ret));
    } else {
      is_const_result_set = true;
    }
  }

  if (OB_SUCC(ret)) {
    sql::ObBitVector *ref_bitset = nullptr;
    BUILD_REF_BITSET(ctx, (dict_val_cnt + 1), ref_bitset);

    bool exist_matched_ref = false;
    ObDictValueIterator mv_iter = ObDictValueIterator(&ctx, 0);
    ObDictValueIterator end_iter = ObDictValueIterator(&ctx, dict_val_cnt);
    int64_t tmp_idx = 0;
    while (OB_SUCC(ret) && (mv_iter != end_iter)) {
      ObStorageDatum &mv_datum = *mv_iter;
      int left_cmp_ret = 0;
      int right_cmp_ret = 0;
      if (OB_FAIL(cmp_func(mv_datum, left_ref_datum, left_cmp_ret))) {
        LOG_WARN("failed to compare datums", K(ret), K(mv_datum), K(left_ref_datum));
      } else if (OB_FAIL(cmp_func(mv_datum, right_ref_datum, right_cmp_ret))) {
        LOG_WARN("failed to compare datums", K(ret), K(mv_datum), K(right_ref_datum));
      } else if ((!is_const_result_set) == (left_cmp_ret >= 0 && right_cmp_ret <= 0)) {
        exist_matched_ref = true;
        ref_bitset->set(tmp_idx);
      }
      ++tmp_idx;
      ++mv_iter;
    }

    const bool need_filter_null = is_const_result_set && has_null;
    if (OB_FAIL(ret)) {
    } else if ((exist_matched_ref || need_filter_null) && OB_FAIL(set_bitmap_with_bitset_const(
      const_ref_desc.width_size_, const_ref_desc.exception_row_id_buf_, const_ref_desc.exception_ref_buf_,
      ref_bitset, const_ref_desc.exception_cnt_, pd_filter_info.start_, pd_filter_info.count_,
      need_filter_null, dict_val_cnt, result_bitmap, !is_const_result_set))) {
      LOG_WARN("fail to set bitmap with bitset", KR(ret), K(const_ref_desc), K(exist_matched_ref), K(need_filter_null),
        K(dict_val_cnt), K(pd_filter_info));
    }
  }

  return ret;
}

int ObDictColumnDecoder::in_const_operator(
    const ObDictColumnDecoderCtx &ctx,
    const ObConstEncodingRefDesc &const_ref_desc,
    const sql::ObPushdownFilterExecutor *parent,
    const sql::ObWhiteFilterExecutor &filter,
    const sql::PushdownFilterInfo &pd_filter_info,
    common::ObBitmap &result_bitmap)
{
  int ret = OB_SUCCESS;
  const int64_t dict_val_cnt = ctx.dict_meta_->distinct_val_cnt_;
  const bool has_null = ctx.dict_meta_->has_null();
  const bool is_sorted_dict = ctx.dict_meta_->is_sorted();

  bool is_const_result_set = false;
  if (const_ref_desc.const_ref_ == dict_val_cnt) {
    // skip null
  } else {
    ObDictValueIterator dict_iter = ObDictValueIterator(&ctx, 0);
    ObStorageDatum &const_datum = *(dict_iter + const_ref_desc.const_ref_);
    if (OB_FAIL(filter.exist_in_datum_set(const_datum, is_const_result_set))) {
      LOG_WARN("fail to check whether const value is in set", KR(ret), K(const_datum));
    } else if (is_const_result_set) {
      if (OB_FAIL(result_bitmap.bit_not())) {
        LOG_WARN("fail to execute bit_not", KR(ret));
      }
    }
  }

  const bool need_filter_null = is_const_result_set && has_null;
  bool matched_ref_exist = false;
  if (OB_SUCC(ret)) {
    sql::ObBitVector *ref_bitset = nullptr;
    BUILD_REF_BITSET(ctx, (dict_val_cnt + 1), ref_bitset);
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(datum_dict_val_in_op(ctx, filter, is_sorted_dict, ref_bitset, matched_ref_exist, is_const_result_set))) {
      LOG_WARN("Failed to to exe datum_dict_val_in_op", KR(ret));
    } else if ((matched_ref_exist || need_filter_null) && OB_FAIL(set_bitmap_with_bitset_const(
      const_ref_desc.width_size_, const_ref_desc.exception_row_id_buf_, const_ref_desc.exception_ref_buf_,
      ref_bitset, const_ref_desc.exception_cnt_, pd_filter_info.start_, pd_filter_info.count_,
      need_filter_null, dict_val_cnt, result_bitmap, !is_const_result_set))) {
      LOG_WARN("fail to set bitmap with bitset", KR(ret), K(const_ref_desc), K(matched_ref_exist),
          K(need_filter_null), K(dict_val_cnt), K(pd_filter_info));
    }
  }

  return ret;
}

int ObDictColumnDecoder::integer_dict_val_cmp_func(
    const uint32_t val_width_size,
    const int64_t datums_cnt,
    const uint64_t *datums_val,
    const int64_t dict_val_cnt,
    const char *dict_buf,
    const sql::ObWhiteFilterOperatorType &op_type,
    int64_t &matched_ref_cnt,
    sql::ObBitVector *ref_bitset)
{
  int ret = OB_SUCCESS;
  matched_ref_cnt = 0;
  if (OB_UNLIKELY((dict_val_cnt < 1)) || OB_ISNULL(datums_val)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(dict_val_cnt), KP(datums_val));
  } else {
    switch (op_type) {
      case sql::WHITE_OP_BT: {
        ObCSFilterFunctionFactory::instance().dict_val_bt_tranverse(dict_buf, val_width_size,
          datums_val, dict_val_cnt, matched_ref_cnt, ref_bitset);
        break;
      }
      case sql::WHITE_OP_EQ:
      case sql::WHITE_OP_NE:
      case sql::WHITE_OP_LT:
      case sql::WHITE_OP_LE:
      case sql::WHITE_OP_GT:
      case sql::WHITE_OP_GE: {
        ObCSFilterFunctionFactory::instance().dict_val_compare_tranverse(dict_buf, val_width_size,
          *datums_val, dict_val_cnt, op_type, matched_ref_cnt, ref_bitset);
        break;
      }
      default:
        ret = OB_NOT_SUPPORTED;
    }

  }
  return ret;
}

int ObDictColumnDecoder::fast_cmp_ref_and_set_result(
    const int64_t dict_ref_val,
    const int64_t null_replaced_val,
    const char *dict_ref_buf,
    const uint8_t ref_width_tag,
    const sql::ObWhiteFilterOperatorType op_type,
    const sql::PushdownFilterInfo &pd_filter_info,
    ObBitmap &result_bitmap)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(dict_ref_buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret), KP(dict_ref_buf));
  } else {
    raw_compare_function_with_null cmp_funtion = RawCompareFunctionFactory::instance().get_cs_cmp_function_with_null(
                                                 ref_width_tag, op_type);
    if (OB_ISNULL(cmp_funtion)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected nullptr compare function", KR(ret), K(ref_width_tag), K(op_type));
    } else {
      cmp_funtion(reinterpret_cast<const unsigned char *>(dict_ref_buf), dict_ref_val, null_replaced_val, result_bitmap.get_data(),
                  pd_filter_info.start_, pd_filter_info.start_ + pd_filter_info.count_);
    }
  }
  return ret;
}

int ObDictColumnDecoder::cmp_ref_and_set_result(
    const uint32_t ref_width_size,
    const char *ref_buf,
    const int64_t dict_ref,
    const bool has_null,
    const uint64_t null_replaced_val,
    const sql::ObWhiteFilterOperatorType &op_type,
    const int64_t row_start,
    const int64_t row_cnt,
    const sql::ObPushdownFilterExecutor *parent,
    ObBitmap &result_bitmap)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY((ref_width_size < 1) || (ref_width_size > 8) || (dict_ref < 0))
      || OB_ISNULL(ref_buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(ref_width_size), K(dict_ref), KP(ref_buf));
  } else {
    ObGetFilterCmpRetFunc get_cmp_ret = get_filter_cmp_ret_func(op_type);
    auto dict_ref_cmp = [&] (const uint64_t cur_val, const uint64_t dict_ref)
    {
      return cur_val == dict_ref ? 0
          : cur_val < dict_ref ? -1 : 1;
    };
    for (int64_t i = 0; OB_SUCC(ret) && (i < row_cnt); ++i) {
      const int64_t row_id = i + row_start;
      if ((nullptr != parent) && parent->can_skip_filter(i)) {
        // skip
      } else {
        uint64_t cur_val = 0;
        ENCODING_ADAPT_MEMCPY(&cur_val, ref_buf + row_id * ref_width_size, ref_width_size);
        bool matched = get_cmp_ret(dict_ref_cmp(cur_val, dict_ref));
        if (has_null) {
          matched = matched && (cur_val != null_replaced_val);
        }
        if (matched) {
          if (OB_FAIL(result_bitmap.set(i))) {
            LOG_WARN("fail to set bitmap", KR(ret), K(i), K(row_id));
          }
        }
      }
    }
  }
  return ret;
}

int ObDictColumnDecoder::set_bitmap_with_bitset(
    const uint32_t ref_width_size,
    const char *ref_buf,
    sql::ObBitVector *ref_bitset,
    const int64_t row_start,
    const int64_t row_cnt,
    const bool has_null,
    const uint64_t null_replaced_val,
    const sql::ObPushdownFilterExecutor *parent,
    ObBitmap &result_bitmap,
    const bool flag)
{
  int ret = OB_SUCCESS;
  // NOTICE: for performance, not check param
  uint64_t cur_ref = 0;
  for (int64_t i = 0; OB_SUCC(ret) && (i < row_cnt); ++i) {
    const int64_t row_id = i + row_start;
    if ((nullptr != parent) && parent->can_skip_filter(i)) {
      // skip
    } else {
      ENCODING_ADAPT_MEMCPY(&cur_ref, ref_buf + row_id * ref_width_size, ref_width_size);
      if (has_null && (cur_ref == null_replaced_val)) {
        if (OB_FAIL(result_bitmap.set(i, false))) {
          LOG_WARN("fail to set bitmap", KR(ret), K(i), K(row_id));
        }
      } else if (ref_bitset->exist(cur_ref)) {
        if (OB_FAIL(result_bitmap.set(i, flag))) {
          LOG_WARN("fail to set bitmap", KR(ret), K(i), K(row_id), K(flag));
        }
      }
    }
  }
  return ret;
}

int ObDictColumnDecoder::set_bitmap_with_bitset_const(
    const uint32_t ref_width_size,
    const char *exception_row_id_buf,
    const char *exception_ref_buf,
    sql::ObBitVector *ref_bitset,
    const int64_t exception_cnt,
    const int64_t row_start,
    const int64_t row_cnt,
    const bool has_null,
    const uint64_t null_replaced_val,
    ObBitmap &result_bitmap,
    const bool flag)
{
  int ret = OB_SUCCESS;
  // NOTICE: for performance, not check param
  uint64_t cur_row_id = 0;
  uint64_t cur_ref = 0;
  const ObIntArrayFuncTable &const_exception_buf = ObIntArrayFuncTable::instance(ref_width_size);
  const int64_t row_end = row_start + row_cnt;
  for (int64_t i = 0; OB_SUCC(ret) && (i < exception_cnt); ++i) {
    cur_row_id = const_exception_buf.at_(exception_row_id_buf, i);
    if ((cur_row_id >= row_start) && (cur_row_id < row_end)) {
      cur_ref = const_exception_buf.at_(exception_ref_buf, i);
      if (has_null && (cur_ref == null_replaced_val)) {
        if (OB_FAIL(result_bitmap.set(cur_row_id - row_start, false))) {
          LOG_WARN("fail to set bitmap", KR(ret), K(row_start), K(cur_row_id));
        }
      } else if (ref_bitset->exist(cur_ref)) {
        if (OB_FAIL(result_bitmap.set(cur_row_id - row_start, flag))) {
          LOG_WARN("fail to set bitmap", KR(ret), K(row_start), K(cur_row_id), K(flag));
        }
      }
    }
  }
  return ret;
}

int ObDictColumnDecoder::set_bitmap_with_bitset_conversely(
    const uint32_t ref_width_size,
    const char *ref_buf,
    sql::ObBitVector *ref_bitset,
    const int64_t row_start,
    const int64_t row_cnt,
    const bool has_null,
    const uint64_t null_replaced_val,
    const sql::ObPushdownFilterExecutor *parent,
    ObBitmap &result_bitmap)
{
  int ret = OB_SUCCESS;
  // NOTICE: for performance, not check param
  for (int64_t i = 0; OB_SUCC(ret) && (i < row_cnt); ++i) {
    const int64_t row_id = i + row_start;
    if ((nullptr != parent) && parent->can_skip_filter(i)) {
      // skip
    } else {
      uint64_t cur_val = 0;
      ENCODING_ADAPT_MEMCPY(&cur_val, ref_buf + row_id * ref_width_size, ref_width_size);
      bool need_set = !ref_bitset->exist(cur_val);
      if (has_null && need_set) {
        need_set = (cur_val != null_replaced_val);
      }
      if (need_set && OB_FAIL(result_bitmap.set(i))) {
        LOG_WARN("fail to set bitmap", KR(ret), K(i), K(row_id));
      }
    }
  }
  return ret;
}

int ObDictColumnDecoder::set_res_with_bitmap(
    const ObDictEncodingMeta &dict_meta,
    const char *ref_buf,
    const uint32_t ref_width_size,
    const common::ObBitmap *ref_bitmap,
    const sql::PushdownFilterInfo &pd_filter_info,
    common::ObDatum *datums,
    const sql::ObPushdownFilterExecutor *parent,
    ObBitmap &result_bitmap)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ref_buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(ref_buf));
  } else if (dict_meta.is_const_encoding_ref()) {
    ObConstEncodingRefDesc ref_desc(ref_buf, ref_width_size);
    if (OB_FAIL(set_res_with_const_encoding_ref(ref_desc, ref_bitmap, pd_filter_info, parent, result_bitmap))) {
      LOG_WARN("fail to set_res_with_const_encoding_ref", K(ret), K(ref_desc), K(pd_filter_info));
    }
  } else {
    if (OB_FAIL(ObCSFilterFunctionFactory::instance().dict_tranverse_ref(ref_buf, ref_width_size, pd_filter_info.start_,
        pd_filter_info.count_, ref_bitmap, parent, result_bitmap))) {
      LOG_WARN("fail to tranverse ref", KR(ret), K(ref_width_size), K(pd_filter_info));
    }
  }
  return ret;
}

int ObDictColumnDecoder::get_distinct_count(const ObColumnCSDecoderCtx &ctx, int64_t &distinct_count) const
{
  int ret = OB_SUCCESS;
  const ObDictColumnDecoderCtx &dict_ctx = ctx.dict_ctx_;
  distinct_count = dict_ctx.dict_meta_->distinct_val_cnt_;
  return ret;
}

int ObDictColumnDecoder::read_distinct(
    const ObColumnCSDecoderCtx &ctx,
    storage::ObGroupByCell &group_by_cell) const
{
  int ret = OB_SUCCESS;
  const ObDictColumnDecoderCtx &dict_ctx = ctx.dict_ctx_;
  const ObDictEncodingMeta *dict_meta = dict_ctx.dict_meta_;
  const int64_t dict_val_cnt = dict_meta->distinct_val_cnt_;
  common::ObDatum *datums = group_by_cell.get_group_by_col_datums_to_fill();
  if (OB_UNLIKELY(0 == dict_val_cnt)) { // empty dict, all datum is null
    datums[0].set_null();
    group_by_cell.set_distinct_cnt(1);
  } else {
    group_by_cell.set_distinct_cnt(dict_val_cnt);
    ObDictValueIterator begin_it = ObDictValueIterator(&dict_ctx, 0);
    ObDictValueIterator end_it = ObDictValueIterator(&dict_ctx, dict_val_cnt);
    int64_t dict_ref = 0;
    while (OB_SUCC(ret) && (begin_it != end_it)) {
      ObObj cur_obj;
      bool is_exist = false;
      if (OB_FAIL(datums[dict_ref].from_storage_datum(*begin_it, group_by_cell.get_obj_datum_map_type()))) {
        LOG_WARN("Failed to read datum", K(ret));
      } else {
        ++begin_it;
        ++dict_ref;
      }
    }
    if (OB_SUCC(ret) && dict_meta->has_null()) {
      group_by_cell.add_distinct_null_value();
    }
  }
  return ret;
}

int ObDictColumnDecoder::read_reference(
    const ObColumnCSDecoderCtx &ctx,
    const int32_t *row_ids,
    const int64_t row_cap,
    storage::ObGroupByCell &group_by_cell) const
{
  int ret = OB_SUCCESS;
  const ObDictColumnDecoderCtx &dict_ctx = ctx.dict_ctx_;
  const ObDictEncodingMeta *dict_meta = dict_ctx.dict_meta_;
  const int64_t dict_val_cnt = dict_meta->distinct_val_cnt_;

  uint32_t *group_by_ref_buf = group_by_cell.get_refs_buf();
  MEMSET(group_by_ref_buf, 0, sizeof(uint32_t) * row_cap);
  if (OB_UNLIKELY(0 == dict_val_cnt)) { // empty dict, all datum is null
  } else {
    const uint32_t width_size = dict_ctx.ref_ctx_->meta_.get_uint_width_size();
    if (dict_meta->is_const_encoding_ref()) {
      int64_t unused_null_cnt = 0;
      ObConstEncodingRefDesc ref_desc(dict_ctx.ref_data_, width_size);
      if (0 == ref_desc.exception_cnt_) {
      } else if OB_FAIL(extract_ref_and_null_count_(ref_desc,
          dict_val_cnt, row_ids, row_cap, nullptr, unused_null_cnt, group_by_ref_buf)) {
        LOG_WARN("Failed to extrace null count", K(ret));
      }
    } else {
      for (int64_t i = 0; i < row_cap; ++i) {
        ENCODING_ADAPT_MEMCPY(group_by_ref_buf + i, dict_ctx.ref_data_ + row_ids[i] * width_size, width_size);
      }
    }
  }
  return ret;
}

int ObDictColumnDecoder::get_aggregate_result(
    const ObColumnCSDecoderCtx &col_ctx,
    const int32_t *row_ids,
    const int64_t row_cap,
    storage::ObAggCell &agg_cell) const
{
  int ret = OB_SUCCESS;
  const ObDictColumnDecoderCtx &dict_ctx = col_ctx.dict_ctx_;
  const bool is_const_encoding = dict_ctx.dict_meta_->is_const_encoding_ref();
  const uint64_t dict_val_cnt = dict_ctx.dict_meta_->distinct_val_cnt_;
  if (OB_UNLIKELY(nullptr == row_ids || row_cap <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_DEBUG("Invalid arguments to get aggregate result", KR(ret), KP(row_ids), K(row_cap));
  } else {
    bool all_null = false;
    if (dict_val_cnt == 0) {
      // skip if all null
    } else if (row_cap == dict_ctx.micro_block_header_->row_count_) { // cover whole microblock
      if (dict_ctx.dict_meta_->is_sorted() && (agg_cell.is_min_agg() || agg_cell.is_max_agg())) { // int dict must be sorted
        ObDictValueIterator res_iter = ObDictValueIterator(&dict_ctx, agg_cell.is_min_agg() ? 0 : dict_val_cnt - 1);
        if (OB_FAIL(agg_cell.eval(*res_iter))) {
          LOG_WARN("Failed to eval agg cell", KR(ret), K(*res_iter), K(agg_cell));
        }
      } else if (OB_FAIL(traverse_datum_dict_agg(dict_ctx, agg_cell))) {
        LOG_WARN("Failed to traverse datum dict to aggregate", KR(ret), K(dict_ctx));
      }
    } else { // cover partial microblock
      int64_t row_id = 0;
      ObStorageDatum storage_datum;
      if (OB_FAIL(agg_cell.reserve_bitmap(dict_val_cnt))) {
        LOG_WARN("Failed to reserve memory for bitmap", KR(ret), K(row_cap));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < row_cap; ++i) {
          row_id = row_ids[i];
          if (OB_FAIL(decode_and_aggregate(col_ctx, row_id, storage_datum, agg_cell))) {
            LOG_WARN("Failed to decode", KR(ret), K(row_id));
          }
        }
      }
    }
  }
  return ret;
}

int ObDictColumnDecoder::traverse_datum_dict_agg(
    const ObDictColumnDecoderCtx &ctx,
    storage::ObAggCell &agg_cell)
{
  int ret = OB_SUCCESS;
  const uint64_t dict_val_cnt = ctx.dict_meta_->distinct_val_cnt_;
  ObDictValueIterator mv_iter = ObDictValueIterator(&ctx, 0);
  ObDictValueIterator end_iter = ObDictValueIterator(&ctx, dict_val_cnt);
  while (OB_SUCC(ret) && mv_iter != end_iter) {
    if (OB_FAIL(agg_cell.eval(*mv_iter))) {
      LOG_WARN("Failed to eval agg cell", KR(ret), K(*mv_iter), K(agg_cell));
    }
    ++mv_iter;
  }
  return ret;
}

}  // namespace blocksstable
}  // namespace oceanbase
