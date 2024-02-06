
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

#include "ob_str_dict_column_decoder.h"
#include "ob_string_stream_decoder.h"

namespace oceanbase
{
namespace blocksstable
{
int ObStrDictColumnDecoder::decode(
  const ObColumnCSDecoderCtx &ctx, const int64_t row_id, common::ObDatum &datum) const
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
      const uint8_t offset_width = dict_ctx.str_ctx_->meta_.is_fixed_len_string() ?
          FIX_STRING_OFFSET_WIDTH_V : dict_ctx.offset_ctx_->meta_.width_;
      ConvertStringToDatumFunc convert_func = convert_string_to_datum_funcs
          [offset_width]
          [ObRefStoreWidthV::REF_IN_DATUMS]
          [ObBaseColumnDecoderCtx::ObNullFlag::HAS_NO_NULL]  /*null has been processed, so here set HAS_NO_NULL*/
          [dict_ctx.need_copy_];
      convert_func(dict_ctx, dict_ctx.str_data_, *dict_ctx.str_ctx_,
          dict_ctx.offset_data_, nullptr/*ref_data*/, nullptr/*row_ids*/, 1, &datum);
    }
  }
  return ret;
}

int ObStrDictColumnDecoder::batch_decode(const ObColumnCSDecoderCtx &ctx, const int64_t *row_ids,
  const int64_t row_cap, common::ObDatum *datums) const
{
  int ret = OB_SUCCESS;
  const ObDictColumnDecoderCtx &dict_ctx = ctx.dict_ctx_;
  if (OB_UNLIKELY(0 == dict_ctx.dict_meta_->distinct_val_cnt_)) { // empty dict, all datum is null
    for (int64_t i = 0; i < row_cap; i++) {
      datums[i].set_null();
    }
  } else {
    const uint8_t offset_width = dict_ctx.str_ctx_->meta_.is_fixed_len_string() ?
        FIX_STRING_OFFSET_WIDTH_V : dict_ctx.offset_ctx_->meta_.width_;
    if (dict_ctx.dict_meta_->is_const_encoding_ref()) {
      const uint64_t width_size = dict_ctx.ref_ctx_->meta_.get_uint_width_size();
      ObConstEncodingRefDesc ref_desc(dict_ctx.ref_data_, width_size);
      if (0 == ref_desc.exception_cnt_) {
        datums[0].pack_ = ref_desc.const_ref_;
        ConvertStringToDatumFunc convert_func = convert_string_to_datum_funcs
            [offset_width]
            [ObRefStoreWidthV::REF_IN_DATUMS]
            [dict_ctx.null_flag_]
            [dict_ctx.need_copy_];
        convert_func(dict_ctx, dict_ctx.str_data_, *dict_ctx.str_ctx_,
            dict_ctx.offset_data_, dict_ctx.ref_data_, nullptr, 1, datums);
        if (dict_ctx.need_copy_) {
          for (int64_t i = 1; i < row_cap; ++i) {
            datums[i].pack_ = datums[0].pack_;
            ENCODING_ADAPT_MEMCPY(const_cast<char *>(datums[i].ptr_), datums[0].ptr_, datums[0].len_);
          }
        } else {
          for (int64_t i = 1; i < row_cap; ++i) {
            datums[i].pack_ = datums[0].pack_;
            datums[i].ptr_ = datums[0].ptr_;
          }
        }
      } else {
        int64_t unused_null_cnt = 0;
        if (OB_FAIL(extract_ref_and_null_count_(
            ref_desc, dict_ctx.dict_meta_->distinct_val_cnt_, row_ids, row_cap, datums, unused_null_cnt))) {
          LOG_WARN("fail to extract_ref_and_null_count_", K(ret), K(dict_ctx));
        } else {
          ConvertStringToDatumFunc convert_func = convert_string_to_datum_funcs
              [offset_width]
              [ObRefStoreWidthV::REF_IN_DATUMS]
              [dict_ctx.null_flag_]
              [dict_ctx.need_copy_];
          convert_func(dict_ctx, dict_ctx.str_data_, *dict_ctx.str_ctx_,
              dict_ctx.offset_data_, dict_ctx.ref_data_, row_ids, row_cap, datums);
        }
      }
    } else { // not const encoding ref
      ConvertStringToDatumFunc convert_func = convert_string_to_datum_funcs
          [offset_width]
          [dict_ctx.ref_ctx_->meta_.width_]
          [dict_ctx.null_flag_]
          [dict_ctx.need_copy_];
      convert_func(dict_ctx, dict_ctx.str_data_, *dict_ctx.str_ctx_,
          dict_ctx.offset_data_, dict_ctx.ref_data_, row_ids, row_cap, datums);

    }
  }

  return ret;
}

}  // namespace blocksstable
}  // namespace oceanbase
