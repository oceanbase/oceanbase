
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

#include "ob_int_dict_column_decoder.h"
#include "ob_integer_stream_vector_decoder.h"
#include "ob_cs_vector_decoding_util.h"
#include "storage/access/ob_pushdown_aggregate.h"

namespace oceanbase
{
namespace blocksstable
{
int ObIntDictColumnDecoder::decode(
  const ObColumnCSDecoderCtx &ctx, const int32_t row_id, common::ObDatum &datum) const
{
  int ret = OB_SUCCESS;
  const ObDictColumnDecoderCtx &dict_ctx = ctx.dict_ctx_;
  const uint64_t distinct_cnt = dict_ctx.dict_meta_->distinct_val_cnt_;
  if (OB_UNLIKELY(0 == distinct_cnt)) {
    datum.set_null();  // empty dict, all datum is null
  } else {
    if (dict_ctx.dict_meta_->is_const_encoding_ref()) {
      GET_CONST_ENCODING_REF(dict_ctx.ref_ctx_->meta_.width_, dict_ctx.ref_data_, row_id, datum.pack_);
    } else {
      GET_REF_FROM_REF_ARRAY(dict_ctx.ref_ctx_->meta_.width_, dict_ctx.ref_data_, row_id, datum.pack_);
    }
    if (datum.pack_ == distinct_cnt) {
      datum.set_null();
    } else {
      ConvertUnitToDatumFunc convert_func = convert_uint_to_datum_funcs
          [dict_ctx.int_ctx_->meta_.width_]               /*val_store_width_V*/
          [ObRefStoreWidthV::REF_IN_DATUMS]               /*ref_store_width_V*/
          [get_width_tag_map()[dict_ctx.datum_len_]]      /*datum_width_V*/
          [ObBaseColumnDecoderCtx::ObNullFlag::HAS_NO_NULL] /*null has been processed, so here set HAS_NO_NULL*/
          [dict_ctx.int_ctx_->meta_.is_decimal_int()];
      convert_func(dict_ctx, dict_ctx.int_data_,
          *dict_ctx.int_ctx_, nullptr/*ref_data*/, nullptr/*row_ids*/, 1, &datum);
    }
  }
  return ret;
}

int ObIntDictColumnDecoder::decode_and_aggregate(
    const ObColumnCSDecoderCtx &ctx,
    const int64_t row_id,
    ObStorageDatum &datum,
    storage::ObAggCellBase &agg_cell) const
{
  int ret = OB_SUCCESS;
  const ObDictColumnDecoderCtx &dict_ctx = ctx.dict_ctx_;
  const uint64_t distinct_cnt = dict_ctx.dict_meta_->distinct_val_cnt_;
  if (OB_UNLIKELY(0 == distinct_cnt)) {
    datum.set_null();  // empty dict, all datum is null
  } else {
    if (dict_ctx.dict_meta_->is_const_encoding_ref()) {
      GET_CONST_ENCODING_REF(dict_ctx.ref_ctx_->meta_.width_, dict_ctx.ref_data_, row_id, datum.pack_);
    } else {
      GET_REF_FROM_REF_ARRAY(dict_ctx.ref_ctx_->meta_.width_, dict_ctx.ref_data_, row_id, datum.pack_);
    }
    ObBitmap &bitmap = agg_cell.get_bitmap();
    if (datum.pack_ == distinct_cnt) {
      datum.set_null();
    } else if (bitmap.test(datum.pack_)) {
      // has been evaluated.
    } else if (OB_FAIL(bitmap.set(datum.pack_))) {
      LOG_WARN("Failed to set bitmap", KR(ret), K(datum.pack_));
    } else {
      ConvertUnitToDatumFunc convert_func = convert_uint_to_datum_funcs
          [dict_ctx.int_ctx_->meta_.width_]               /*val_store_width_V*/
          [ObRefStoreWidthV::REF_IN_DATUMS]               /*ref_store_width_V*/
          [get_width_tag_map()[dict_ctx.datum_len_]]      /*datum_width_V*/
          [ObBaseColumnDecoderCtx::ObNullFlag::HAS_NO_NULL] /*null has been processed, so here set HAS_NO_NULL*/
          [dict_ctx.int_ctx_->meta_.is_decimal_int()];
      convert_func(dict_ctx, dict_ctx.int_data_,
          *dict_ctx.int_ctx_, nullptr/*ref_data*/, nullptr/*row_ids*/, 1, &datum);
      if (!datum.is_null() && OB_FAIL(agg_cell.eval(datum))) {
        LOG_WARN("Failed to eval agg cell", KR(ret), K(datum), K(agg_cell));
      }
    }
  }
  return ret;
}

int ObIntDictColumnDecoder::batch_decode(const ObColumnCSDecoderCtx &ctx, const int32_t *row_ids,
  const int64_t row_cap, common::ObDatum *datums) const
{
  int ret = OB_SUCCESS;
  const ObDictColumnDecoderCtx &dict_ctx = ctx.dict_ctx_;
  if (OB_UNLIKELY(0 == dict_ctx.dict_meta_->distinct_val_cnt_)) { // empty dict, all datum is null
    for (int64_t i = 0; i < row_cap; i++) {
      datums[i].set_null();
    }
  } else {
    if (dict_ctx.dict_meta_->is_const_encoding_ref()) {
      const uint64_t width_size = dict_ctx.ref_ctx_->meta_.get_uint_width_size();
      ObConstEncodingRefDesc ref_desc(dict_ctx.ref_data_, width_size);
      if (0 == ref_desc.exception_cnt_) {
        datums[0].pack_ = ref_desc.const_ref_;
        ConvertUnitToDatumFunc convert_func = convert_uint_to_datum_funcs
            [dict_ctx.int_ctx_->meta_.width_]            /*val_store_width_V*/
            [ObRefStoreWidthV::REF_IN_DATUMS]            /*ref_store_width_V*/
            [get_width_tag_map()[dict_ctx.datum_len_]]   /*datum_width_V*/
            [dict_ctx.null_flag_]
            [dict_ctx.int_ctx_->meta_.is_decimal_int()];
        convert_func(dict_ctx, dict_ctx.int_data_,
            *dict_ctx.int_ctx_, dict_ctx.ref_data_, nullptr, 1, datums);
        for (int64_t i = 1; i < row_cap; ++i) {
          datums[i].pack_ = datums[0].pack_;
          ENCODING_ADAPT_MEMCPY(const_cast<char *>(datums[i].ptr_), datums[0].ptr_, datums[0].len_);
        }
      } else {
        int64_t unused_null_cnt = 0;
        if (OB_FAIL(extract_ref_and_null_count_(
            ref_desc, dict_ctx.dict_meta_->distinct_val_cnt_, row_ids, row_cap, datums, unused_null_cnt))) {
          LOG_WARN("fail to extract_ref_and_null_count_", K(ret), K(dict_ctx));
        } else {
          ConvertUnitToDatumFunc convert_func = convert_uint_to_datum_funcs
              [dict_ctx.int_ctx_->meta_.width_]           /*val_store_width_V*/
              [ObRefStoreWidthV::REF_IN_DATUMS]           /*ref_store_width_V*/
              [get_width_tag_map()[dict_ctx.datum_len_]]  /*datum_width_V*/
              [dict_ctx.null_flag_]
              [dict_ctx.int_ctx_->meta_.is_decimal_int()];
          convert_func(dict_ctx, dict_ctx.int_data_,
              *dict_ctx.int_ctx_, dict_ctx.ref_data_, row_ids, row_cap, datums);

        }
      }
    } else {  // not const encoding ref
      ConvertUnitToDatumFunc convert_func = convert_uint_to_datum_funcs
          [dict_ctx.int_ctx_->meta_.width_]           /*val_store_width_V*/
          [dict_ctx.ref_ctx_->meta_.width_]           /*ref_store_width_V*/
          [get_width_tag_map()[dict_ctx.datum_len_]]  /*datum_width_V*/
          [dict_ctx.null_flag_]
          [dict_ctx.int_ctx_->meta_.is_decimal_int()];
      convert_func(dict_ctx, dict_ctx.int_data_,
          *dict_ctx.int_ctx_, dict_ctx.ref_data_, row_ids, row_cap, datums);
    }
  }

  return ret;
}

int ObIntDictColumnDecoder::decode_vector(
    const ObColumnCSDecoderCtx &ctx,
    ObVectorDecodeCtx &vector_ctx) const
{
  int ret = OB_SUCCESS;
  const ObDictColumnDecoderCtx &dict_ctx = ctx.dict_ctx_;
  if (OB_UNLIKELY(0 == dict_ctx.dict_meta_->distinct_val_cnt_)) { // empty dict, all datum is null
    if (OB_FAIL(ObCSVectorDecodingUtil::decode_all_null_vector(
         vector_ctx.row_ids_, vector_ctx.row_cap_, vector_ctx.vec_header_, vector_ctx.vec_offset_))) {
      LOG_WARN("fail to decode_all_null_vector", K(ret));
    }
  } else {
     const char *ref_arr = nullptr;
     ObVecDecodeRefWidth ref_width = ObVecDecodeRefWidth::VDRW_MAX;

    if (dict_ctx.dict_meta_->is_const_encoding_ref()) {
      uint32_t *temp_ref_arr = vector_ctx.len_arr_;
      ref_arr =  (char*)temp_ref_arr;
      ref_width = ObVecDecodeRefWidth::VDRW_TEMP_UINT32_REF;
      const uint64_t width_size = dict_ctx.ref_ctx_->meta_.get_uint_width_size();
      ObConstEncodingRefDesc ref_desc(dict_ctx.ref_data_, width_size);
      int64_t unused_null_cnt = 0;
      if (0 == ref_desc.exception_cnt_) {
        for (int64_t i = 0; i < vector_ctx.row_cap_; ++i) {
          temp_ref_arr[i] = ref_desc.const_ref_;
        }
      } else if (OB_FAIL(extract_ref_and_null_count_(ref_desc, dict_ctx.dict_meta_->distinct_val_cnt_,
          vector_ctx.row_ids_, vector_ctx.row_cap_, nullptr, unused_null_cnt, temp_ref_arr))) {
        LOG_WARN("fail to extract_ref_and_null_count_", K(ret), K(dict_ctx));
      }
    } else {  // not const encoding ref
      ref_arr = dict_ctx.ref_data_;
      ref_width = static_cast<ObVecDecodeRefWidth>(dict_ctx.ref_ctx_->meta_.get_width_tag());
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(ObIntegerStreamVecDecoder::decode_vector(
          dict_ctx, dict_ctx.int_data_, *dict_ctx.int_ctx_, ref_arr, ref_width, vector_ctx))) {
        LOG_WARN("fail to decode_vector", K(ret), K(dict_ctx), K(vector_ctx));
      }
    }
  }
  return ret;
}

}  // namespace blocksstable
}  // namespace oceanbase
