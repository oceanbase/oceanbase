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

#include "ob_cs_micro_block_transformer.h"
#include "lib/compress/ob_compressor_pool.h"
#include "ob_int_dict_column_encoder.h"
#include "ob_str_dict_column_encoder.h"
#include "ob_cs_encoding_util.h"
#include "ob_cs_decoding_util.h"
#include "storage/blocksstable/ob_sstable_printer.h"
#include "ob_string_stream_decoder.h"
#include "ob_integer_stream_decoder.h"

namespace oceanbase
{
namespace blocksstable
{

int64_t ObMicroBlockTransformDesc::calc_serialize_size(
  const int32_t col_cnt, const int32_t stream_count) const
{
  int64_t size = 0;
  size += sizeof(*column_meta_pos_arr_) * col_cnt;
  size += sizeof(*column_first_stream_decoding_ctx_offset_arr_) * col_cnt;
  size += sizeof(*column_first_stream_idx_arr_) * col_cnt;
  size += sizeof(*stream_desc_attr_arr_) * stream_count;
  size += sizeof(*stream_data_pos_arr_) * stream_count;
  return size;
}

int ObMicroBlockTransformDesc::deserialize(
  const int32_t col_cnt, const int32_t stream_count, char *buf, const int64_t len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  int64_t tmp_pos = pos;


  column_meta_pos_arr_ = reinterpret_cast<Pos *>(buf + tmp_pos);
  tmp_pos += sizeof(*column_meta_pos_arr_) * col_cnt;

  column_first_stream_decoding_ctx_offset_arr_ = reinterpret_cast<uint32_t *>(buf + tmp_pos);
  tmp_pos += sizeof(*column_first_stream_decoding_ctx_offset_arr_) * col_cnt;

  column_first_stream_idx_arr_ = reinterpret_cast<uint16_t *>(buf + tmp_pos);
  tmp_pos += sizeof(*column_first_stream_idx_arr_) * col_cnt;

  stream_desc_attr_arr_ = reinterpret_cast<uint8_t *>(buf + tmp_pos);
  tmp_pos += sizeof(*stream_desc_attr_arr_) * stream_count;

  stream_data_pos_arr_ = reinterpret_cast<Pos *>(buf + tmp_pos);
  tmp_pos += sizeof(*stream_data_pos_arr_) * stream_count;

  if (OB_UNLIKELY(tmp_pos > len)) {
    ret = OB_BUF_NOT_ENOUGH;
    LOG_WARN(
      "fail to deserialize", K(ret), K(len), K(pos), K(tmp_pos), K(col_cnt), K(stream_count));
  } else {
    pos = tmp_pos;
  }

  return ret;

}
int64_t ObMicroBlockTransformDesc::to_string(const uint32_t col_cnt,
                                             const uint32_t stream_cnt,
                                             char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV("column_meta_pos_arr",
       ObArrayWrap<Pos>(column_meta_pos_arr_, col_cnt),
       "column_first_stream_decoding_ctx_offset_arr",
       ObArrayWrap<uint32_t>(column_first_stream_decoding_ctx_offset_arr_, col_cnt),
       "column_first_stream_idx_arr",
       ObArrayWrap<uint16_t>(column_first_stream_idx_arr_, col_cnt),
       "stream_desc_attr_arr",
       ObArrayWrap<uint8_t>(stream_desc_attr_arr_, stream_cnt),
       "stream_data_pos_arr",
       ObArrayWrap<Pos>(stream_data_pos_arr_, stream_cnt));
  J_OBJ_END();

  return pos;
}

//========================= ObCSMicroBlockTransformer =================================//
ObCSMicroBlockTransformer::ObCSMicroBlockTransformer()
  : is_inited_(false), is_part_tranform_(false),
    has_string_need_project_(false),
    compressor_(nullptr), header_(nullptr),
    payload_buf_(nullptr), all_column_header_(nullptr), column_headers_(nullptr),
    column_meta_begin_offset_(0), stream_offsets_arr_(nullptr), original_desc_(),
    orig_desc_buf_(nullptr), orig_desc_buf_size_(0), stream_decoding_ctx_buf_(nullptr),
    stream_decoding_ctx_buf_size_(0), stream_row_cnt_arr_(nullptr), stream_meta_len_arr_(nullptr),
    all_string_data_offset_(0), all_string_uncompress_len_(0),
    allocator_("BlkTransformer")
{
}

// NOTE: ObMicroBlockHeader and payload may be not continuous
int ObCSMicroBlockTransformer::init(const ObMicroBlockHeader *header,
  const char *payload_buf, const int64_t payload_len,
  const bool is_part_tranform, const int32_t *store_ids, const int32_t store_ids_cnt)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObCSMicroBlockTransformer has inited", K(ret));
  } else if (OB_FAIL(ObCompressorPool::get_instance().get_compressor(
      static_cast<ObCompressorType>(header->compressor_type_), compressor_))) {
    LOG_WARN("fail to get compressor", K(ret), KPC(header));
  } else {
    header_ = header;
    payload_buf_ = payload_buf;
    all_column_header_ = reinterpret_cast<const ObAllColumnHeader *>(payload_buf);
    column_headers_ = reinterpret_cast<const ObCSColumnHeader *>(payload_buf_ + sizeof(ObAllColumnHeader));
    column_meta_begin_offset_ = sizeof(ObAllColumnHeader) + sizeof(ObCSColumnHeader) * header->column_count_;
    is_part_tranform_ = is_part_tranform;
  }

  if (OB_FAIL(ret)) {
  } else if (OB_UNLIKELY(!all_column_header_->is_valid() || all_column_header_->is_full_transformed())) {
    ret = OB_INVALID_DATA;
    LOG_WARN("invalid all column header", K(ret), KPC(all_column_header_));
  } else if (OB_FAIL(decode_stream_offsets_(payload_len))) {
    LOG_WARN("fail to decode_stream_offsets", K(ret));
  } else if (OB_FAIL(build_all_string_data_desc_(payload_len))) {
    LOG_WARN("fail to build_all_string_data_desc_", K(ret));
  } else if (OB_FAIL(build_original_transform_desc_(store_ids, store_ids_cnt))) {
    LOG_WARN("fail to build transform desc", K(ret));
  } else if (OB_FAIL(build_stream_decoder_ctx_())) {
    LOG_WARN("fail to build stream decoding ctx", K(ret));
  } else {
    is_inited_ = true;
  }
  LOG_DEBUG("finish init ObCSMicroBlockTransformer", K(ret), KPC(this));
  return ret;
}

int ObCSMicroBlockTransformer::decode_stream_offsets_(const int64_t payload_len)
{
  int ret = OB_SUCCESS;

  if (0 == all_column_header_->stream_count_) {
    // has no stream, do nothing
  } else {
    OB_ASSERT(all_column_header_->stream_offsets_length_ != 0);

    const uint32_t col_count = header_->column_count_;
    const uint32_t stream_count = all_column_header_->stream_count_;
    const char *stream_offsets_buf =
      payload_buf_ + payload_len - all_column_header_->stream_offsets_length_;
    ObIntegerStreamDecoderCtx ctx;
    uint16_t offset_stream_meta_len = 0;
    ObStreamData data(stream_offsets_buf, all_column_header_->stream_offsets_length_);
    if (stream_count <= STREAM_OFFSETS_CACHED_COUNT) {
      stream_offsets_arr_ = stream_offsets_cached_arr_;
    } else if (OB_ISNULL(stream_offsets_arr_ = (uint32_t *)allocator_.alloc(
                           stream_count * sizeof(*stream_offsets_arr_)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc stream_offsets_arr memory", K(ret), K(stream_count));
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(ObIntegerStreamDecoder::build_decoder_ctx(
        data, stream_count, (ObCompressorType)header_->compressor_type_, ctx, offset_stream_meta_len))) {
      LOG_WARN("fail to decode header for stream offsets stream", K(ret), K(data), KPC_(all_column_header));
    } else if (ctx.meta_.is_use_base()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("stream offsets encoding must has no base", K(ctx));
    } else {
      ObStreamData offset_stream_data;
      offset_stream_data.buf_ = stream_offsets_buf + offset_stream_meta_len;
      offset_stream_data.len_ = all_column_header_->stream_offsets_length_ - offset_stream_meta_len;
      const uint32_t width_size = ctx.meta_.get_uint_width_size();
      if (width_size > sizeof(uint32_t)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("width_size of stream offsets is unexpected", K(ctx), K(width_size));
      } else if (OB_FAIL(ObIntegerStreamDecoder::transform_to_raw_array(offset_stream_data, ctx, (char*)stream_offsets_arr_, allocator_))) {
        LOG_WARN("fail to decode stream offset to array", K(ret), K(ctx), K(offset_stream_data));
      } else if (width_size != sizeof(uint32_t)) {
        uint32_t tmp = 0;
        // copy from end to start to make memory safe
        for (int32_t i = stream_count - 1; i >= 0; --i) {
          memcpy(&tmp, (char*)stream_offsets_arr_ + i * width_size, width_size);
          stream_offsets_arr_[i] = tmp;
        }
      }
      LOG_DEBUG("decode stream offsets", K(ret), K(col_count), K(stream_count),
        K(payload_len), K_(column_meta_begin_offset), K(data), K(offset_stream_data),
        K(offset_stream_meta_len), K(ctx),
        "stream_offsets_arr",
        ObArrayWrap<uint32_t>(stream_offsets_arr_, stream_count));
    }
  }

  return ret;
}

int ObCSMicroBlockTransformer::build_all_string_data_desc_(const int64_t payload_len)
{
  int ret = OB_SUCCESS;
  uint32_t end_offset = payload_len - all_column_header_->stream_offsets_length_;
  all_string_data_offset_ = end_offset - all_column_header_->all_string_data_length_;
  return ret;
}

int ObCSMicroBlockTransformer::build_original_transform_desc_(
  const int32_t *store_ids, const int32_t store_ids_cnt)
{
  int ret = OB_SUCCESS;

  const uint32_t col_count = header_->column_count_;
  const uint32_t stream_count = all_column_header_->stream_count_;
  orig_desc_buf_size_ = original_desc_.calc_serialize_size(col_count, stream_count);
  // alloc stream_row_cnt_arr_ and stream_meta_len_arr_ with desc_buf
  int32_t stream_row_cnt_arr_size = sizeof(*stream_row_cnt_arr_) * stream_count;
  int32_t stream_meta_len_arr_size = sizeof(*stream_meta_len_arr_) * stream_count;
  int64_t pos = 0;

  if (OB_ISNULL(orig_desc_buf_ = (char *)allocator_.alloc(
                  orig_desc_buf_size_ + stream_row_cnt_arr_size + stream_meta_len_arr_size))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory", K(ret), K(orig_desc_buf_size_), K(stream_row_cnt_arr_size),
      K(stream_meta_len_arr_size));
  } else if (OB_FAIL(original_desc_.deserialize(
               col_count, stream_count, orig_desc_buf_, orig_desc_buf_size_, pos))) {
    LOG_WARN("fail to deserialize desc", K(ret), K(col_count), K(stream_count),
        KP(orig_desc_buf_), K(orig_desc_buf_size_));
  } else {
    // reseted stream_desc_attr means is_string_stream and not_need_part_tranform and
    // not_in_part_transform_buf
    if (stream_count != 0) {
      original_desc_.reset_stream_desc_attr(stream_count);
      stream_row_cnt_arr_ = reinterpret_cast<uint32_t *>(orig_desc_buf_ + orig_desc_buf_size_);
      stream_meta_len_arr_ =
      reinterpret_cast<uint16_t *>(orig_desc_buf_ + orig_desc_buf_size_ + stream_row_cnt_arr_size);
      MEMSET(stream_row_cnt_arr_, 0, stream_row_cnt_arr_size);
    }
    // stream offsets is relative to micro block header and offset in column_meta_begin_offset_
    // is relative to payload buf, so here need to plus micro block header size
    uint32_t first_stream_begin_offset = column_meta_begin_offset_ + header_->header_size_;
    int32_t stream_idx = -1;
    uint32_t pre_streams_len = 0;
    for (uint32_t i = 0; OB_SUCC(ret) && i < col_count; i++) {
      const ObCSColumnHeader &column_header = column_headers_[i];
        LOG_DEBUG("build_original_transform_desc for one column", K(i), "stream_start_idx", stream_idx,
          K(pre_streams_len), K(first_stream_begin_offset), K(column_header));
      if (OB_UNLIKELY(!column_header.is_valid())) {
        ret = OB_INVALID_DATA;
        LOG_WARN("invalid column header", K(ret), K(column_header), K(i));
      } else if (ObCSColumnHeader::Type::INTEGER == column_header.type_) {
        stream_idx = stream_idx + 1;
        original_desc_.column_first_stream_idx_arr_[i] = stream_idx;
        original_desc_.column_meta_pos_arr_[i].offset_ = column_meta_begin_offset_ + pre_streams_len;
        original_desc_.column_meta_pos_arr_[i].len_ = column_header.has_null_bitmap() ?
          ObCSEncodingUtil::get_bitmap_byte_size(header_->row_count_) : 0;
        original_desc_.set_is_integer_stream(stream_idx);
        stream_row_cnt_arr_[stream_idx] = header_->row_count_;
        pre_streams_len = stream_offsets_arr_[stream_idx] - first_stream_begin_offset;

      } else if (ObCSColumnHeader::Type::STRING == column_header.type_) {
        stream_idx = stream_idx + 1;
        original_desc_.column_first_stream_idx_arr_[i] = stream_idx;
        original_desc_.column_meta_pos_arr_[i].offset_ = column_meta_begin_offset_ + pre_streams_len;
        original_desc_.column_meta_pos_arr_[i].len_ = column_header.has_null_bitmap()
          ? ObCSEncodingUtil::get_bitmap_byte_size(header_->row_count_)
          : 0;
        // bytes stream has nothing to set, keep default value
        if (!column_header.is_fixed_length()) {
          stream_idx = stream_idx + 1;
          original_desc_.set_is_integer_stream(stream_idx);
          stream_row_cnt_arr_[stream_idx] = header_->row_count_;
        }
        pre_streams_len = stream_offsets_arr_[stream_idx] - first_stream_begin_offset;

      } else if (ObCSColumnHeader::Type::INT_DICT == column_header.type_) {
        original_desc_.column_meta_pos_arr_[i].offset_ = column_meta_begin_offset_ + pre_streams_len;
        // must has no null bitmap for dict encoding
        original_desc_.column_meta_pos_arr_[i].len_ = sizeof(ObDictEncodingMeta);
        const ObDictEncodingMeta *dict_meta = reinterpret_cast<const ObDictEncodingMeta *>(
          payload_buf_ + original_desc_.column_meta_pos_arr_[i].offset_);
        if (0 == dict_meta->distinct_val_cnt_) {
          // this column has no stream, column_first_stream_idx is same with next column's first_stream_idx
          // or the stream count if this column is the last column.
          original_desc_.column_first_stream_idx_arr_[i] = stream_idx + 1;
          pre_streams_len += sizeof(ObDictEncodingMeta);
        } else {
          // integer dict stream + dict ref stream
          stream_idx = stream_idx + 1;
          original_desc_.column_first_stream_idx_arr_[i] = stream_idx;
          original_desc_.set_is_integer_stream(stream_idx);
          stream_row_cnt_arr_[stream_idx] = dict_meta->distinct_val_cnt_;

          stream_idx = stream_idx + 1;
          original_desc_.set_is_integer_stream(stream_idx);
          stream_row_cnt_arr_[stream_idx] = dict_meta->ref_row_cnt_;
          pre_streams_len = stream_offsets_arr_[stream_idx] - first_stream_begin_offset;
        }
      } else if (ObCSColumnHeader::Type::STR_DICT == column_header.type_) {
        original_desc_.column_meta_pos_arr_[i].offset_ = column_meta_begin_offset_ + pre_streams_len;
        // must has no null bitmap for dict encoding
        original_desc_.column_meta_pos_arr_[i].len_ = sizeof(ObDictEncodingMeta);
        const ObDictEncodingMeta *dict_meta = reinterpret_cast<const ObDictEncodingMeta *>(
          payload_buf_ + original_desc_.column_meta_pos_arr_[i].offset_);
        if (0 == dict_meta->distinct_val_cnt_) {
          // this column has no stream, column_first_stream_idx is same with next column's first_stream_idx
          // or the stream count if this column is the last column.
          original_desc_.column_first_stream_idx_arr_[i] = stream_idx + 1;
          pre_streams_len += sizeof(ObDictEncodingMeta);
        } else {
          // is string dict, bytes stream has nothing to set, keep default value
          stream_idx = stream_idx + 1;
          original_desc_.column_first_stream_idx_arr_[i] = stream_idx;
          if (column_header.is_fixed_length()) {  // fix length string dict
            // dict ref stream
            stream_idx = stream_idx + 1;
            original_desc_.set_is_integer_stream(stream_idx);
            stream_row_cnt_arr_[stream_idx] = dict_meta->ref_row_cnt_;
          } else {  // variable length string dict
            // string offset array stream + dict ref stream
            stream_idx = stream_idx + 1;
            original_desc_.set_is_integer_stream(stream_idx);
            stream_row_cnt_arr_[stream_idx] = dict_meta->distinct_val_cnt_;

            stream_idx = stream_idx + 1;
            original_desc_.set_is_integer_stream(stream_idx);
            stream_row_cnt_arr_[stream_idx] = dict_meta->ref_row_cnt_;
          }
          pre_streams_len = stream_offsets_arr_[stream_idx] - first_stream_begin_offset;
        }
      }
    }

    // build  original_desc_.stream_data_pos_arr_
    int64_t pre_stream_offset = first_stream_begin_offset;
    for (uint32_t i = 0; OB_SUCC(ret) && i < col_count; i++) {
      uint16_t col_end_stream_idx = 0;
      uint16_t col_first_stream_idx = original_desc_.column_first_stream_idx_arr_[i];
      if (i != col_count - 1) {  // not last column
        col_end_stream_idx = original_desc_.column_first_stream_idx_arr_[i + 1];
      } else {
        col_end_stream_idx = stream_count;
      }
      if (col_end_stream_idx - col_first_stream_idx == 0) {
        // this column has no stream, only has column meta
        pre_stream_offset += original_desc_.column_meta_pos_arr_[i].len_;
      } else {
        // Stream offsets is relative to micro block header and offset in stream_data_pos_arr_
        // is relative to payload buf, so here need to minus micro block header size.
        // Column first stream offset include the column meta, so need to handle sepcially.
        original_desc_.stream_data_pos_arr_[col_first_stream_idx].offset_ =
          pre_stream_offset - header_->header_size_ + original_desc_.column_meta_pos_arr_[i].len_;
        original_desc_.stream_data_pos_arr_[col_first_stream_idx].len_ =
          stream_offsets_arr_[col_first_stream_idx] - pre_stream_offset - original_desc_.column_meta_pos_arr_[i].len_;
        pre_stream_offset = stream_offsets_arr_[col_first_stream_idx];
        int64_t stream_idx = col_first_stream_idx + 1;
        while (stream_idx < col_end_stream_idx) {
          original_desc_.stream_data_pos_arr_[stream_idx].offset_ = pre_stream_offset - header_->header_size_;
          original_desc_.stream_data_pos_arr_[stream_idx].len_ = stream_offsets_arr_[stream_idx] - pre_stream_offset;
          pre_stream_offset = stream_offsets_arr_[stream_idx];
          stream_idx++;
        }
      }
    }

    if (is_part_tranform_) {
      if (nullptr == store_ids) { // part tranform all columns
        for (int64_t stream_idx = 0; stream_idx < stream_count; stream_idx++) {
          original_desc_.set_need_project(stream_idx);
        }
      } else {
        uint16_t col_first_stream_idx = 0;
        uint16_t col_end_stream_idx = 0;
        for (int64_t i = 0; i < store_ids_cnt; i++) {
          const int64_t &col_idx = store_ids[i];
          if (col_idx >= col_count || col_idx < 0) {
            // skip non exist column
          } else {
            col_first_stream_idx = original_desc_.column_first_stream_idx_arr_[col_idx];
            if (col_idx != col_count - 1) {  // not last column
              col_end_stream_idx = original_desc_.column_first_stream_idx_arr_[col_idx + 1];
            } else {
              col_end_stream_idx = stream_count;
            }
            while (col_first_stream_idx < col_end_stream_idx) {
              original_desc_.set_need_project(col_first_stream_idx);
              col_first_stream_idx++;
            }
          }
        }
      }
    }

    LOG_DEBUG("finish build_original_transform_desc", K(ret), K_(is_part_tranform),
        "store_ids", ObArrayWrap<int32_t>(store_ids, store_ids_cnt),
        "original_desc", ObMicroBlockTransformDescPrinter(col_count, stream_count, original_desc_));
  }

  return ret;
}

int ObCSMicroBlockTransformer::build_stream_decoder_ctx_()
{
  int ret = OB_SUCCESS;
  stream_decoding_ctx_buf_size_ = 0;

  if (0 == all_column_header_->stream_count_) {
    // do nothing
  } else {
    int32_t string_stream_cnt = 0;
    for (int32_t stream_idx = 0; stream_idx < all_column_header_->stream_count_; stream_idx++) {
      if (original_desc_.is_integer_stream(stream_idx)) {
        if (is_part_tranform_ && !original_desc_.is_need_project(stream_idx)) {
          // skip
        } else {
          stream_decoding_ctx_buf_size_ += sizeof(ObIntegerStreamDecoderCtx);
        }
      } else {  // is string stream
        string_stream_cnt++;
        if (original_desc_.is_need_project(stream_idx)) {
          if (!has_string_need_project_) {
            has_string_need_project_ = true;
          }
        }
      }
    }
    if (!is_part_tranform_ || has_string_need_project_) {
      stream_decoding_ctx_buf_size_ += string_stream_cnt * sizeof(ObStringStreamDecoderCtx);
    }

    if (stream_decoding_ctx_buf_size_ > 0 && OB_ISNULL(
          stream_decoding_ctx_buf_ = (char *)allocator_.alloc(stream_decoding_ctx_buf_size_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc memory", K(ret), K(stream_decoding_ctx_buf_size_));
    }
    char *buf = stream_decoding_ctx_buf_;

    for (int32_t stream_idx = 0; OB_SUCC(ret) && stream_idx < all_column_header_->stream_count_;
         stream_idx++) {
      if (original_desc_.is_integer_stream(stream_idx)) {
        if (is_part_tranform_ && !original_desc_.is_need_project(stream_idx)) {
          // skip
        } else {
          ObIntegerStreamDecoderCtx *ctx = new(buf) ObIntegerStreamDecoderCtx();
          ObStreamData int_data(payload_buf_ + original_desc_.stream_data_pos_arr_[stream_idx].offset_,
                              original_desc_.stream_data_pos_arr_[stream_idx].len_);
          if (OB_FAIL(ObIntegerStreamDecoder::build_decoder_ctx(int_data, stream_row_cnt_arr_[stream_idx],
              (ObCompressorType)header_->compressor_type_, *ctx, stream_meta_len_arr_[stream_idx]))) {
            LOG_WARN("fail to build decoding ctx", K(ret), K(stream_row_cnt_arr_[stream_idx]));
          } else {
            buf += sizeof(ObIntegerStreamDecoderCtx);
            LOG_DEBUG("build integer stream decoding ctx", K(stream_idx),
                K(stream_row_cnt_arr_[stream_idx]), K(*ctx), K(sizeof(ObIntegerStreamDecoderCtx)));
          }
        }
      } else {  // is string stream
        if (is_part_tranform_ && !has_string_need_project_) {
          // skip
        } else {
          ObStringStreamDecoderCtx *ctx = new(buf) ObStringStreamDecoderCtx();
          ObStreamData str_data(payload_buf_ + original_desc_.stream_data_pos_arr_[stream_idx].offset_,
                              original_desc_.stream_data_pos_arr_[stream_idx].len_);
          if (OB_FAIL(ObStringStreamDecoder::build_decoder_ctx(str_data, *ctx, stream_meta_len_arr_[stream_idx]))) {
            LOG_WARN("fail to build decoding ctx", K(ret), K(stream_idx));
          } else {
            // update the offset to the offset in all_string_data
            original_desc_.stream_data_pos_arr_[stream_idx].offset_ = all_string_uncompress_len_;
            all_string_uncompress_len_ += ctx->meta_.uncompressed_len_;
            buf += sizeof(ObStringStreamDecoderCtx);
            LOG_DEBUG("build string stream decoding ctx", K(stream_idx), K(*ctx), K(sizeof(ObStringStreamDecoderCtx)));
          }
        }
      }
    }
  }

  return ret;
}

// full transform format:
// micro_block_header + all_column_header + column_header * col_cnt +
// transform_desc_buf +
// stream_decoding_ctx * stream_cnt +
// column_meta * col_cnt +
// stream_data(need transform two kind of stream, compressed string stream and
//             non-raw integer stream which need to be decoded to raw array)
int ObCSMicroBlockTransformer::calc_full_transform_size(int64_t &size) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(is_part_tranform_)) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("must be not part tranform", K(ret), K(is_part_tranform_));
  } else {
    // <1> micro_block_header + all_column_header + column_header * col_cnt
    size = header_->header_size_ + column_meta_begin_offset_;
    // <2> transform_desc_buf size
    size +=
      original_desc_.calc_serialize_size(header_->column_count_, all_column_header_->stream_count_);
    // <3> stream_decoding_ctx * stream_cnt
    size += stream_decoding_ctx_buf_size_;
    // <4> column_meta * col_cnt
    for (int32_t col_idx = 0; col_idx < header_->column_count_; col_idx++) {
      size += original_desc_.column_meta_pos_arr_[col_idx].len_;
    }
    // <5> stream_data(not include stream meta)
    const char *buf = stream_decoding_ctx_buf_;
    uint32_t store_size = 0;
    for (int32_t stream_idx = 0;
      OB_SUCC(ret) && stream_idx < all_column_header_->stream_count_; stream_idx++) {
      if (original_desc_.is_integer_stream(stream_idx)) {
        const ObIntegerStreamDecoderCtx *ctx = reinterpret_cast<const ObIntegerStreamDecoderCtx *>(buf);
        store_size = ctx->meta_.get_uint_width_size();
        if (ctx->meta_.is_raw_encoding()) {
          uint32_t real_stream_data_len =
              original_desc_.stream_data_pos_arr_[stream_idx].len_ - stream_meta_len_arr_[stream_idx];
          size += real_stream_data_len;
        } else { // the stream is not original raw and will be decoded to raw when full_transform
          size += ctx->count_ * store_size;
        }
        buf += sizeof(ObIntegerStreamDecoderCtx);
      } else {  // is string stream
        const ObStringStreamDecoderCtx *ctx = reinterpret_cast<const ObStringStreamDecoderCtx *>(buf);
        size += ctx->meta_.uncompressed_len_;
        buf += sizeof(ObStringStreamDecoderCtx);
      }
    }
  }

  return ret;
}
int ObCSMicroBlockTransformer::full_transform(char *buf, const int64_t buf_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  int64_t tmp_pos = pos;
  ObMicroBlockHeader *copied_micro_header = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_FREE;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(is_part_tranform_)) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("must be not part tranform", K(ret), K(is_part_tranform_));
  // <1> micro_block_header
  } else if (OB_FAIL(header_->deep_copy(buf, buf_len, tmp_pos, copied_micro_header))) {
    LOG_WARN("fail to serialize micro block header", K(ret));
  }

  const int32_t col_cnt = header_->column_count_;
  const int32_t stream_cnt = all_column_header_->stream_count_;
  uint32_t all_string_data_len = all_column_header_->all_string_data_length_;
  ObAllColumnHeader *dst_all_col_header = reinterpret_cast<ObAllColumnHeader*>(buf + tmp_pos);
  // <2> all_column_header + column_header * col_cnt
  if (OB_SUCC(ret)) {
    if (OB_UNLIKELY(tmp_pos + column_meta_begin_offset_ > buf_len)) {
      ret = OB_BUF_NOT_ENOUGH;
      LOG_WARN("buf not enough", K(ret), K(tmp_pos), K(column_meta_begin_offset_), K(buf_len));
    } else {
      MEMCPY(buf + tmp_pos, payload_buf_, column_meta_begin_offset_);
      tmp_pos += column_meta_begin_offset_;
      // Indicates that buf has been transformed
      dst_all_col_header->set_full_transformed();
    }
  }
  // <3> transform_desc_buf
  ObMicroBlockTransformDesc dst_desc;
  char *dst_desc_buf = buf + tmp_pos;
  if (OB_SUCC(ret)) {
    int64_t dst_desc_buf_pos = 0;
    if (OB_FAIL(dst_desc.deserialize(
          col_cnt, stream_cnt, dst_desc_buf, orig_desc_buf_size_, dst_desc_buf_pos))) {
      LOG_WARN("fail to deserialize ObMicroBlockTransformDesc", K(ret), K(orig_desc_buf_size_));
    } else if (OB_UNLIKELY(tmp_pos + orig_desc_buf_size_ > buf_len)) {
      ret = OB_BUF_NOT_ENOUGH;
      LOG_WARN("buf not enough", K(ret), K(tmp_pos), K(buf_len), K(orig_desc_buf_size_));
    } else {
      MEMCPY(dst_desc_buf, orig_desc_buf_, orig_desc_buf_size_);
      tmp_pos += orig_desc_buf_size_;
    }
  }
  // <4> stream_decoding_ctx * stream_cnt
  const uint32_t dst_stream_decoding_ctx_buf_offset = tmp_pos;
  if (OB_SUCC(ret) && stream_decoding_ctx_buf_size_ != 0) {
    if (OB_UNLIKELY(tmp_pos + stream_decoding_ctx_buf_size_ > buf_len)) {
      ret = OB_BUF_NOT_ENOUGH;
      LOG_WARN("buf not enough", K(ret), K(tmp_pos), K(buf_len), K(stream_decoding_ctx_buf_size_));
    } else {
      MEMCPY(buf + tmp_pos, stream_decoding_ctx_buf_, stream_decoding_ctx_buf_size_);
      tmp_pos += stream_decoding_ctx_buf_size_;
    }
  }
  // <5> column_meta * col_cnt
  // 1. copy column data and update dst column_meta_pos_arr
  // 2. update dst_desc.col_first_stream_decoding_ctx_offset_arr_
  uint16_t col_first_stream_idx = 0;
  uint16_t col_end_stream_idx = 0;
  uint32_t decoding_ctx_buf_offset = dst_stream_decoding_ctx_buf_offset;
  for (int32_t col_idx = 0; OB_SUCC(ret) && col_idx < col_cnt; col_idx++) {
    ObMicroBlockTransformDesc::Pos &meta_pos = dst_desc.column_meta_pos_arr_[col_idx];
    if (OB_UNLIKELY(tmp_pos + meta_pos.len_ > buf_len)) {
      ret = OB_BUF_NOT_ENOUGH;
      LOG_WARN("buf not enough", K(ret), K(tmp_pos), K(buf_len), K(col_idx), K(meta_pos));
    } else {
      MEMCPY(buf + tmp_pos, payload_buf_ + meta_pos.offset_, meta_pos.len_);
      meta_pos.offset_ = tmp_pos;  // update to the column meta offset of dst buf
      tmp_pos += meta_pos.len_;
      // if has no stream, column_first_stream_decoding_ctx_offset_arr_ is meaningless
      dst_desc.column_first_stream_decoding_ctx_offset_arr_[col_idx] = decoding_ctx_buf_offset;

      col_first_stream_idx = original_desc_.column_first_stream_idx_arr_[col_idx];
      if (col_idx != col_cnt - 1) {
        col_end_stream_idx = original_desc_.column_first_stream_idx_arr_[col_idx + 1];
      } else {
        col_end_stream_idx = stream_cnt;
      }
      while (col_first_stream_idx < col_end_stream_idx) {
        if (original_desc_.is_integer_stream(col_first_stream_idx)) {
          decoding_ctx_buf_offset += sizeof(ObIntegerStreamDecoderCtx);
        } else {
          decoding_ctx_buf_offset += sizeof(ObStringStreamDecoderCtx);
        }
        col_first_stream_idx++;
      }
    }
  }

  // <6> stream_data(decompress string stream and decode non-raw integer stream)
  // copy stream data(not include stream meta) and update dst stream_data_pos_arr
  char *ctx_buf = buf + dst_stream_decoding_ctx_buf_offset;
  int64_t dst_all_string_data_start_pos = 0;
  if (OB_FAIL(ret)) {
  } else if (OB_UNLIKELY(tmp_pos + all_string_uncompress_len_ > buf_len)) {
    ret = OB_BUF_NOT_ENOUGH;
    LOG_WARN( "buf not enough", K(ret), K(tmp_pos), K(buf_len), K(all_string_uncompress_len_));
  } else if (all_column_header_->is_all_string_compressed()) {
    int64_t real_decomp_size = 0;
    if (OB_FAIL(compressor_->decompress(payload_buf_ + all_string_data_offset_,
        all_string_data_len, buf + tmp_pos, all_string_uncompress_len_, real_decomp_size))) {
      LOG_WARN("fail to decompress all string data", K(ret),
          K(all_string_data_offset_), K(all_string_data_len), K(all_string_uncompress_len_));
    } else if (OB_UNLIKELY(real_decomp_size != all_string_uncompress_len_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("size mismatch", KR(ret), K(all_string_uncompress_len_), K(real_decomp_size));
    } else {
      dst_all_string_data_start_pos = tmp_pos;
      tmp_pos += all_string_uncompress_len_;
    }
  } else if (0 != all_string_uncompress_len_) {
    if (OB_UNLIKELY(all_string_uncompress_len_ != all_string_data_len)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("all string data len mismatch", K(ret), K(all_string_uncompress_len_), K(all_string_data_len));
    } else {
      MEMCPY(buf + tmp_pos, payload_buf_ + all_string_data_offset_, all_string_data_len);
      dst_all_string_data_start_pos = tmp_pos;
      tmp_pos += all_string_uncompress_len_;
    }
  }

  for (int32_t stream_idx = 0; OB_SUCC(ret) && stream_idx < all_column_header_->stream_count_;
       stream_idx++) {
    if (original_desc_.is_integer_stream(stream_idx)) {
      ObStreamData orig_stream_data(
          payload_buf_ + original_desc_.stream_data_pos_arr_[stream_idx].offset_ + stream_meta_len_arr_[stream_idx],
          original_desc_.stream_data_pos_arr_[stream_idx].len_ - stream_meta_len_arr_[stream_idx]);
      ObIntegerStreamDecoderCtx *ctx = reinterpret_cast<ObIntegerStreamDecoderCtx *>(ctx_buf);
      if (ctx->meta_.is_raw_encoding()) {
        if (OB_UNLIKELY(tmp_pos + orig_stream_data.len_ > buf_len)) {
          ret = OB_BUF_NOT_ENOUGH;
          LOG_WARN( "buf not enough", K(ret), K(tmp_pos), K(buf_len), K(stream_idx), K(orig_stream_data));
        } else {
          MEMCPY(buf + tmp_pos, orig_stream_data.buf_, orig_stream_data.len_);
          dst_desc.stream_data_pos_arr_[stream_idx].offset_ = tmp_pos;
          dst_desc.stream_data_pos_arr_[stream_idx].len_ = orig_stream_data.len_;
          tmp_pos += orig_stream_data.len_;
        }
      } else { // the stream is not original raw and should be decoded to raw array
        const uint32_t arr_size = ctx->meta_.get_uint_width_size() * ctx->count_;
        if (OB_UNLIKELY(tmp_pos + arr_size > buf_len)) {
          ret = OB_BUF_NOT_ENOUGH;
          LOG_WARN( "buf not enough", K(ret), K(pos), K(buf_len), K(arr_size), KPC(ctx));
        } else if (OB_FAIL(ObIntegerStreamDecoder::transform_to_raw_array(orig_stream_data, *ctx, buf + tmp_pos, allocator_))) {
          LOG_WARN("fail to transform_to_raw_array", K(ret), K(orig_stream_data), KPC(ctx), K(tmp_pos));
        } else {
          dst_desc.stream_data_pos_arr_[stream_idx].offset_ = tmp_pos;
          dst_desc.stream_data_pos_arr_[stream_idx].len_ = arr_size;
          tmp_pos += arr_size;
        }
      }
      ctx_buf += sizeof(ObIntegerStreamDecoderCtx);
    } else {  // is string stream
      ObStringStreamDecoderCtx *ctx = reinterpret_cast<ObStringStreamDecoderCtx *>(ctx_buf);
      dst_desc.stream_data_pos_arr_[stream_idx].offset_ =
          dst_all_string_data_start_pos + original_desc_.stream_data_pos_arr_[stream_idx].offset_;
      dst_desc.stream_data_pos_arr_[stream_idx].len_ = ctx->meta_.uncompressed_len_;
      ctx_buf += sizeof(ObStringStreamDecoderCtx);
    }
  }
  if (OB_SUCC(ret)) {
    pos = tmp_pos;
  }

  LOG_DEBUG("finish full transform", K(ret), K(pos),
    "dst_desc", ObMicroBlockTransformDescPrinter(col_cnt, stream_cnt, dst_desc));

  return ret;
}

// part transform format:
// transform_desc_buf +
// stream_decoding_ctx * stream_cnt +
// stream_data(need transform two kind of stream, compressed string stream and
//             non-raw integer stream which need to be decoded to raw array)
int ObCSMicroBlockTransformer::calc_part_transform_size(int64_t &size) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!is_part_tranform_)) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("must be part tranform", K(ret), K(is_part_tranform_));
  } else {
    // <1> transform_desc_buf size
    size =
      original_desc_.calc_serialize_size(header_->column_count_, all_column_header_->stream_count_);
    // <2> stream_decoding_ctx * stream_cnt
    size += stream_decoding_ctx_buf_size_;
    // <3> stream_data(not include stream header) * compressed_stream_cnt
    // need copy stream data if has column_level_compression
    const char *buf = stream_decoding_ctx_buf_;
    if (has_string_need_project_ && all_column_header_->is_all_string_compressed()) {
      size += all_string_uncompress_len_;
    }
    for (int32_t stream_idx = 0;
        OB_SUCC(ret) && stream_idx < all_column_header_->stream_count_; stream_idx++) {
      if (original_desc_.is_integer_stream(stream_idx)) {
        if (!original_desc_.is_need_project(stream_idx)) {
          // skip
        } else {
          const ObIntegerStreamDecoderCtx *ctx = reinterpret_cast<const ObIntegerStreamDecoderCtx *>(buf);
          const uint32_t store_size = ctx->meta_.get_uint_width_size();
          if (ctx->meta_.is_raw_encoding()) {
            // original stream is raw, no need transform to part_transform_buf_
          } else { // the stream is not original raw and will be decoded to raw when part_transform
            size += ctx->count_ * store_size;
          }
          buf += sizeof(ObIntegerStreamDecoderCtx);
        }
      } else {  // is string stream
        if (has_string_need_project_) {
          buf += sizeof(ObStringStreamDecoderCtx);
        }
      }
    }
  }
  return ret;
}
int ObCSMicroBlockTransformer::part_transform(char *buf, const int64_t buf_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  int64_t tmp_pos = pos;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!is_part_tranform_)) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("must be part tranform", K(ret), K(is_part_tranform_));
  }
  const int32_t col_cnt = header_->column_count_;
  const int32_t stream_cnt = all_column_header_->stream_count_;
  uint32_t all_string_data_len = all_column_header_->all_string_data_length_;
  // <1> copy transform_desc_buf
  ObMicroBlockTransformDesc dst_desc;
  char *dst_desc_buf = buf + tmp_pos;
  if (OB_SUCC(ret)) {
    int64_t dst_desc_buf_pos = 0;
    if (OB_FAIL(dst_desc.deserialize(
          col_cnt, stream_cnt, dst_desc_buf, orig_desc_buf_size_, dst_desc_buf_pos))) {
      LOG_WARN("fail to deserialize ObMicroBlockTransformDesc", K(ret), K(orig_desc_buf_size_));
    } else if (OB_UNLIKELY(tmp_pos + orig_desc_buf_size_ > buf_len)) {
      ret = OB_BUF_NOT_ENOUGH;
      LOG_WARN("buf not enough", K(ret), K(tmp_pos), K(buf_len), K(orig_desc_buf_size_));
    } else {
      MEMCPY(dst_desc_buf, orig_desc_buf_, orig_desc_buf_size_);
      tmp_pos += orig_desc_buf_size_;
    }
  }
  // <2> copy stream_decoding_ctx
  const uint32_t dst_stream_decoding_ctx_buf_offset = tmp_pos;
  if (OB_SUCC(ret) && stream_decoding_ctx_buf_size_ != 0) {
    if (OB_UNLIKELY(tmp_pos + stream_decoding_ctx_buf_size_ > buf_len)) {
      ret = OB_BUF_NOT_ENOUGH;
      LOG_WARN("buf not enough", K(ret), K(tmp_pos), K(buf_len), K(stream_decoding_ctx_buf_size_));
    } else {
      MEMCPY(buf + tmp_pos, stream_decoding_ctx_buf_, stream_decoding_ctx_buf_size_);
      tmp_pos += stream_decoding_ctx_buf_size_;
    }
  }

  // <3> update dst_desc.col_first_stream_decoding_ctx_offset_arr_ and dst_desc.column_meta_pos_arr_
  if (OB_SUCC(ret)) {
    uint16_t col_first_stream_idx = 0;
    uint16_t col_end_stream_idx = 0;
    uint32_t decoding_ctx_buf_offset = dst_stream_decoding_ctx_buf_offset;
    for (int32_t col_idx = 0; col_idx < col_cnt; col_idx++) {
      // if column has no stream or is not projected, column_first_stream_decoding_ctx_offset_arr_ is meaningless.
      dst_desc.column_first_stream_decoding_ctx_offset_arr_[col_idx] = decoding_ctx_buf_offset;

      col_first_stream_idx = original_desc_.column_first_stream_idx_arr_[col_idx];
      if (col_idx != col_cnt - 1) {
        col_end_stream_idx = original_desc_.column_first_stream_idx_arr_[col_idx + 1];
      } else {
        col_end_stream_idx = stream_cnt;
      }
      while (col_first_stream_idx < col_end_stream_idx) {
        if (original_desc_.is_integer_stream(col_first_stream_idx)) {
          if (!original_desc_.is_need_project(col_first_stream_idx)) {
            // skip
          } else {
            decoding_ctx_buf_offset += sizeof(ObIntegerStreamDecoderCtx);
          }
        } else { // is string stream
          if (!has_string_need_project_) {
            // skip
          } else {
            decoding_ctx_buf_offset += sizeof(ObStringStreamDecoderCtx);
          }
        }
        col_first_stream_idx++;
      }
      dst_desc.column_meta_pos_arr_[col_idx].offset_ += header_->header_size_;
    }
  }

  // <4> stream_data(decompress string stream and decode non-raw integer stream)
  //     and adjust stream_data_pos_arr_ to make the offset relative to the header
  if (OB_SUCC(ret)) {
    char *ctx_buf = buf + dst_stream_decoding_ctx_buf_offset;
    int64_t dst_all_string_data_start_pos = 0;
    if (!has_string_need_project_) {
      // no need transform all string data, do nothing
    } else if (!all_column_header_->is_all_string_compressed()) {
      // just check, do nothing
      if (OB_UNLIKELY(all_string_uncompress_len_ != all_string_data_len)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("all string data len mismatch", K(ret), K(all_string_uncompress_len_), K(all_string_data_len));
      }
    } else if (OB_UNLIKELY(tmp_pos + all_string_uncompress_len_ > buf_len)) {
      ret = OB_BUF_NOT_ENOUGH;
      LOG_WARN( "buf not enough", K(ret), K(tmp_pos), K(buf_len), K(all_string_uncompress_len_));
    } else {
      int64_t real_decomp_size = 0;
      if (OB_FAIL(compressor_->decompress(payload_buf_ + all_string_data_offset_,
          all_string_data_len, buf + tmp_pos, all_string_uncompress_len_, real_decomp_size))) {
        LOG_WARN("fail to decompress all string data", K(ret));
      } else if (OB_UNLIKELY(real_decomp_size != all_string_uncompress_len_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("size mismatch", KR(ret), K(all_string_uncompress_len_), K(real_decomp_size));
      } else {
        dst_all_string_data_start_pos = tmp_pos;
        tmp_pos += all_string_uncompress_len_;
      }
    }

    for (int32_t stream_idx = 0; OB_SUCC(ret) && stream_idx < all_column_header_->stream_count_;
         stream_idx++) {
      if (original_desc_.is_integer_stream(stream_idx)) {
        if (!original_desc_.is_need_project(stream_idx)) {
          // skip
        } else {
          ObIntegerStreamDecoderCtx *ctx = reinterpret_cast<ObIntegerStreamDecoderCtx *>(ctx_buf);
          ObStreamData orig_stream_data(payload_buf_ +
              original_desc_.stream_data_pos_arr_[stream_idx].offset_ + stream_meta_len_arr_[stream_idx],
              original_desc_.stream_data_pos_arr_[stream_idx].len_ - stream_meta_len_arr_[stream_idx]);
          if (ctx->meta_.is_raw_encoding()) {
            // original stream is raw, no need copy, just remove the stream meta len in dst_desc.stream_data_pos_arr_
            dst_desc.stream_data_pos_arr_[stream_idx].offset_ =
                original_desc_.stream_data_pos_arr_[stream_idx].offset_ +
                stream_meta_len_arr_[stream_idx] + header_->header_size_;
            dst_desc.stream_data_pos_arr_[stream_idx].len_ = orig_stream_data.len_;
          } else { // the stream is not original raw and should be decoded to raw array
            const uint32_t arr_size = ctx->meta_.get_uint_width_size() * ctx->count_;
            if (tmp_pos + arr_size > buf_len) {
              ret = OB_BUF_NOT_ENOUGH;
              LOG_WARN( "buf not enough", K(ret), K(pos), K(buf_len), K(arr_size), KPC(ctx));
            } else if (OB_FAIL(ObIntegerStreamDecoder::transform_to_raw_array(orig_stream_data, *ctx, buf + tmp_pos, allocator_))) {
              LOG_WARN("fail to tranform_to_raw_array", K(ret), K(orig_stream_data), KPC(ctx), K(tmp_pos));
            } else {
              dst_desc.set_stream_in_part_transfrom_buf(stream_idx);
              dst_desc.stream_data_pos_arr_[stream_idx].offset_ = tmp_pos;
              dst_desc.stream_data_pos_arr_[stream_idx].len_ = arr_size;
              tmp_pos += arr_size;
            }
          }
          ctx_buf += sizeof(ObIntegerStreamDecoderCtx);
        }
      } else {  // is string stream
        if (!has_string_need_project_) {
          // skip
        } else {
          ObStringStreamDecoderCtx *ctx = reinterpret_cast<ObStringStreamDecoderCtx *>(ctx_buf);
          if (all_column_header_->is_all_string_compressed()) {
            dst_desc.set_stream_in_part_transfrom_buf(stream_idx);
            dst_desc.stream_data_pos_arr_[stream_idx].offset_ =
                dst_all_string_data_start_pos + original_desc_.stream_data_pos_arr_[stream_idx].offset_;
            dst_desc.stream_data_pos_arr_[stream_idx].len_ = ctx->meta_.uncompressed_len_;
          } else {
            dst_desc.stream_data_pos_arr_[stream_idx].offset_ = all_string_data_offset_ +
                original_desc_.stream_data_pos_arr_[stream_idx].offset_ + header_->header_size_;
            dst_desc.stream_data_pos_arr_[stream_idx].len_ = ctx->meta_.uncompressed_len_;
          }
          ctx_buf += sizeof(ObStringStreamDecoderCtx);
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    pos = tmp_pos;
  }

  LOG_DEBUG("finish part transform", K(ret), K(pos),
    "dst_desc", ObMicroBlockTransformDescPrinter(col_cnt, stream_cnt, dst_desc));

  return ret;
}

int64_t ObCSMicroBlockTransformer::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(is_inited), K_(is_part_tranform), K_(has_string_need_project),
      KP_(compressor), K_(all_string_data_offset), K_(all_string_uncompress_len),
      KPC_(header), KP_(payload_buf), KPC_(all_column_header),
      K_(column_meta_begin_offset), K_(orig_desc_buf_size), K_(stream_decoding_ctx_buf_size));
  J_COMMA();

  if (header_ != nullptr && all_column_header_ != nullptr) {
    J_KV("column_headers",
         ObArrayWrap<ObCSColumnHeader>(column_headers_, header_->column_count_),
         "stream_offsets_arr",
         ObArrayWrap<uint32_t>(stream_offsets_arr_, all_column_header_->stream_count_),
         "stream_row_cnt_arr",
         ObArrayWrap<uint32_t>(stream_row_cnt_arr_, all_column_header_->stream_count_),
         "stream_meta_len_arr",
         ObArrayWrap<uint16_t>(stream_meta_len_arr_, all_column_header_->stream_count_));
    J_COMMA();
  }
  if (header_ != nullptr && all_column_header_ != nullptr) {
    J_KV("original_desc", ObMicroBlockTransformDescPrinter(
        header_->column_count_, all_column_header_->stream_count_, original_desc_));
  } else {
    J_KV("original_desc", ObMicroBlockTransformDescPrinter(0, 0, original_desc_));
  }
  J_OBJ_END();
  return pos;
}

int ObCSMicroBlockTransformer::full_transform_block_data(const ObMicroBlockHeader &header,
                                                         const char *payload_buf,
                                                         const int64_t payload_size,
                                                         const char *&dst_block_buf,
                                                         int64_t &dst_buf_size,
                                                         ObIAllocator *allocator)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  ObCSMicroBlockTransformer transformer;
  if (OB_FAIL(transformer.init(&header, payload_buf, payload_size))) {
    LOG_WARN("fail to init cs micro block transformer", K(ret), K(header));
  } else if (OB_FAIL(transformer.calc_full_transform_size(dst_buf_size))) {
    LOG_WARN("fail to calc transformed size", K(ret), K(transformer));
  } else if (OB_ISNULL(dst_block_buf = static_cast<const char *>(allocator->alloc(dst_buf_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc", K(ret), K(dst_buf_size));
  } else if (OB_FAIL(transformer.full_transform(const_cast<char *&>(dst_block_buf), dst_buf_size, pos))) {
    LOG_WARN("fail to transfrom cs encoding mirco blcok", K(ret));
  } else if (OB_UNLIKELY(pos != dst_buf_size)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("pos should equal to buf_size", K(ret), K(pos), K(dst_buf_size));
  }

  return ret;
}

int ObCSMicroBlockTransformer::dump_cs_encoding_info(char *hex_print_buf, const int64_t hex_buf_size)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    ObSSTablePrinter::print_cs_encoding_all_column_header(*all_column_header_);
    const uint32_t col_cnt = header_->column_count_;
    const uint32_t stream_cnt = all_column_header_->stream_count_;
    uint16_t col_first_stream_idx = 0;
    uint16_t col_end_stream_idx = 0;
    for (int64_t i = 0; i < col_cnt; i++) {
      ObSSTablePrinter::print_cs_encoding_column_header(column_headers_[i], i);
      ObMicroBlockTransformDesc::Pos &meta_pos = original_desc_.column_meta_pos_arr_[i];
      const char *meta_buf = payload_buf_ + meta_pos.offset_;
      const int64_t meta_len = meta_pos.len_;
      ObSSTablePrinter::print_cs_encoding_column_meta(
          meta_buf, meta_len, (ObCSColumnHeader::Type)column_headers_[i].type_, i, hex_print_buf, hex_buf_size);

      col_first_stream_idx = original_desc_.column_first_stream_idx_arr_[i];
      if (i != col_cnt - 1) {
        col_end_stream_idx = original_desc_.column_first_stream_idx_arr_[i + 1];
      } else {
        col_end_stream_idx = stream_cnt;
      }
      int32_t decoding_ctx_offset = 0;
      while (col_first_stream_idx < col_end_stream_idx) {
        if (original_desc_.is_integer_stream(col_first_stream_idx)) {
          ObIntegerStreamDecoderCtx *ctx = reinterpret_cast<ObIntegerStreamDecoderCtx *>(stream_decoding_ctx_buf_ + decoding_ctx_offset);
          decoding_ctx_offset += sizeof(ObIntegerStreamDecoderCtx);
          ObSSTablePrinter::print_integer_stream_decoder_ctx(col_first_stream_idx, *ctx, hex_print_buf, hex_buf_size);
        } else { // is string stream
          ObStringStreamDecoderCtx *ctx = reinterpret_cast<ObStringStreamDecoderCtx *>(stream_decoding_ctx_buf_ + decoding_ctx_offset);
          decoding_ctx_offset += sizeof(ObStringStreamDecoderCtx);
          ObSSTablePrinter::print_string_stream_decoder_ctx(col_first_stream_idx, *ctx, hex_print_buf, hex_buf_size);
        }
        col_first_stream_idx++;
      }
    }
    ObSSTablePrinter::print_cs_encoding_orig_stream_data(
        stream_cnt, original_desc_, payload_buf_, all_string_data_offset_, all_column_header_->all_string_data_length_);
  }
  return ret;
}


// =============================ObCSMicroBlockTransformHelper ===========================//
ObCSMicroBlockTransformHelper::ObCSMicroBlockTransformHelper()
  : is_inited_(false), allocator_(nullptr), block_data_(), header_(nullptr),
    all_col_header_(nullptr), col_headers_(nullptr), transform_desc_(),
    part_transform_buf_(nullptr), part_transform_buf_size_(0)
{
}
void ObCSMicroBlockTransformHelper::reset()
{
  is_inited_ = false;
  allocator_ = nullptr;
  header_ = nullptr;
  all_col_header_ = nullptr;
  col_headers_ = nullptr;
  transform_desc_.reset();
  part_transform_buf_ = nullptr;
  part_transform_buf_size_ = 0;
}

int ObCSMicroBlockTransformHelper::init(common::ObIAllocator *allocator,
  const ObMicroBlockData &block_data, const int32_t *store_ids, const int32_t store_ids_cnt)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObCSMicroBlockTransformHelper has inited", K(ret));
  } else if (OB_ISNULL(allocator) || OB_UNLIKELY(!block_data.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(allocator), K(block_data));
  } else {
    allocator_ = allocator;
    block_data_ = block_data;
    header_ = reinterpret_cast<const ObMicroBlockHeader *>(block_data_.get_buf());
    all_col_header_ =
      reinterpret_cast<const ObAllColumnHeader *>(block_data_.get_buf() + header_->header_size_);
    col_headers_ = reinterpret_cast<const ObCSColumnHeader *>(
      block_data_.get_buf() + header_->header_size_ + sizeof(ObAllColumnHeader));
    if (OB_UNLIKELY(!header_->is_valid())) {
      ret = OB_INVALID_DATA;
      LOG_WARN("invalid micro block header", K(ret), KPC(header_));
    } else if (OB_FAIL(build_tranform_desc_(store_ids, store_ids_cnt))) {
      LOG_WARN("fail to build transform desc", K(ret), KP(store_ids), K(store_ids_cnt));
    } else {
      is_inited_ = true;
      LOG_DEBUG("finish init ObCSMicroBlockTransformHelper", KPC(all_col_header_),
        "transform_desc", ObMicroBlockTransformDescPrinter(
          header_->column_count_, all_col_header_->stream_count_, transform_desc_));
    }
  }

  return ret;
}

int ObCSMicroBlockTransformHelper::build_tranform_desc_(
    const int32_t *store_ids, const int32_t store_ids_cnt)
{
  int ret = OB_SUCCESS;
  if (all_col_header_->is_full_transformed()) {
    // the column data has been full transformed, ignore store_ids and store_ids_cnt
    int64_t pos = header_->header_size_ + sizeof(ObAllColumnHeader) +
      sizeof(ObCSColumnHeader) * header_->column_count_;
    if (OB_FAIL(transform_desc_.deserialize(header_->column_count_, all_col_header_->stream_count_,
          const_cast<char *>(block_data_.get_buf()), block_data_.get_buf_size(), pos))) {
      LOG_WARN("failc to deserialize ObMicroBlockTransformDesc", K(ret));
    }
  } else {
    ObCSMicroBlockTransformer transformer;
    int64_t part_transform_size = 0;
    if (OB_FAIL(transformer.init(header_,
                                 block_data_.get_buf() + header_->header_size_,
                                 block_data_.get_buf_size() - header_->header_size_,
                                 true/*is_part_transform*/, store_ids, store_ids_cnt))) {
      LOG_WARN("fail to init ObCSMicroBlockTransformer", K(ret), K_(block_data), K(transformer));
    } else if (OB_FAIL(transformer.calc_part_transform_size(part_transform_size))) {
      LOG_WARN("fail to calc_part_transform_size", K(ret), K(transformer));
    }

    if (OB_SUCC(ret)) {
      if (OB_ISNULL(part_transform_buf_ = (char *)allocator_->alloc(part_transform_size))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc part_transform_buf_ memory", K(ret));
      } else {
        part_transform_buf_size_ = part_transform_size;
      }
    }

    if (OB_SUCC(ret)) {
      int64_t pos = 0;
      if (OB_FAIL(transformer.part_transform(part_transform_buf_, part_transform_size, pos))) {
        LOG_WARN("fail to part transform", K(ret), K(transformer));
      } else if (OB_UNLIKELY(pos != part_transform_size)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("pos must be equal to part_transform_size",
            K(ret), K(pos), K(part_transform_size), K(transformer));
      }
    }
    if (OB_SUCC(ret)) {
      int64_t pos = 0;
      if (OB_FAIL(transform_desc_.deserialize(header_->column_count_,
            all_col_header_->stream_count_, part_transform_buf_, part_transform_size, pos))) {
        LOG_WARN("fail to deserialize ObMicroBlockTransformDesc", K(ret), K(transformer));
      }
    }
  }

  return ret;
}

int ObCSMicroBlockTransformHelper::build_column_decoder_ctx(
    const ObObjMeta &obj_meta, const int32_t col_idx, ObColumnCSDecoderCtx &decoder_ctx)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObCSMicroBlockTransformHelper not init", K(ret));
  } else {
    const ObCSColumnHeader &col_header = col_headers_[col_idx];
    const int32_t col_first_stream_idx = transform_desc_.column_first_stream_idx_arr_[col_idx];
    int32_t col_end_stream_idx = 0;
    const int32_t col_cnt =  header_->column_count_;
    const int32_t stream_cnt = all_col_header_->stream_count_;
    if (col_idx == col_cnt - 1) {  // is last column
      col_end_stream_idx = stream_cnt;
    } else {
      col_end_stream_idx = transform_desc_.column_first_stream_idx_arr_[col_idx + 1];
    }
    decoder_ctx.reset();
    decoder_ctx.type_ = static_cast<ObCSColumnHeader::Type>(col_header.type_);
    switch(col_header.type_) {
      case ObCSColumnHeader::Type::INTEGER : {
        if (OB_FAIL(build_integer_column_decoder_ctx_(obj_meta, col_first_stream_idx,
            col_end_stream_idx, col_idx, decoder_ctx.integer_ctx_))) {
          LOG_WARN("fail to build_integer_decoder_ctx", K(ret), K(col_first_stream_idx),
              K(col_end_stream_idx), K(col_idx),
              "transform_desc", ObMicroBlockTransformDescPrinter(col_cnt, stream_cnt, transform_desc_));
        }
        break;
      }
      case ObCSColumnHeader::Type::STRING : {
        if (OB_FAIL(build_string_column_decoder_ctx_(obj_meta, col_first_stream_idx,
            col_end_stream_idx, col_idx, decoder_ctx.string_ctx_))) {
          LOG_WARN("fail to build_integer_decoder_ctx",
              K(ret), K(col_first_stream_idx), K(col_end_stream_idx), K(col_idx),
              "transform_desc", ObMicroBlockTransformDescPrinter(col_cnt, stream_cnt, transform_desc_));
        }
        break;
      }
      case ObCSColumnHeader::Type::INT_DICT : {
        if (OB_FAIL(build_integer_dict_decoder_ctx_(obj_meta, col_first_stream_idx,
            col_end_stream_idx, col_idx, decoder_ctx.dict_ctx_))) {
          LOG_WARN("fail to build_integer_dict_decoder_ctx",
              K(ret), K(col_first_stream_idx), K(col_end_stream_idx), K(col_idx),
              "transform_desc", ObMicroBlockTransformDescPrinter(col_cnt, stream_cnt, transform_desc_));
        }
        break;
      }
      case ObCSColumnHeader::Type::STR_DICT : {
        if (OB_FAIL(build_string_dict_decoder_ctx_(obj_meta, col_first_stream_idx,
            col_end_stream_idx, col_idx, decoder_ctx.dict_ctx_))) {
          LOG_WARN("fail to build_string_dict_decoder_ctx",
              K(ret), K(col_first_stream_idx), K(col_end_stream_idx), K(col_idx),
              "transform_desc", ObMicroBlockTransformDescPrinter(col_cnt, stream_cnt, transform_desc_));
        }
        break;
      }
      default : {
        ret = OB_INNER_STAT_ERROR;
        LOG_WARN("unknow column encoding type", K(ret), K(col_header));
      }
    }
  }

  return ret;
}

// For full transfrom, stream_data and decoding_ctx in the same block data buf,
// For part transform, stream_data which is no need to decompress is in the
// block data buf, otherwise the stream data is decompressed to part_transform_buf_,
// decoding_ctx is always in the part_transform_buf_.
// More details can refer to ObCSMicroBlockTransformer::part_transform
#define GET_STREAM_BUF(stream_idx)                                                      \
  if (all_col_header_->is_full_transformed()) {                                         \
    buf = block_data_.get_buf();                                                        \
    ctx_buf = buf;                                                                      \
  } else if (OB_UNLIKELY(!transform_desc_.is_need_project(stream_idx))) {               \
    ret = OB_INNER_STAT_ERROR;                                                          \
    LOG_WARN("this stream must be part transformed", K(ret), K(stream_idx));            \
  } else if (transform_desc_.is_stream_in_part_transfrom_buf(stream_idx)) {             \
    buf = part_transform_buf_;                                                          \
    ctx_buf = part_transform_buf_;                                                      \
  } else {                                                                              \
    buf = block_data_.get_buf();                                                        \
    ctx_buf = part_transform_buf_;                                                      \
  }

int ObCSMicroBlockTransformHelper::build_integer_column_decoder_ctx_(
                                  const ObObjMeta &obj_meta,
                                  const int32_t col_first_stream_idx,
                                  const int32_t col_end_stream_idx,
                                  const int32_t col_idx,
                                  ObIntegerColumnDecoderCtx &ctx)
{
  int ret = OB_SUCCESS;
  const char *buf = nullptr;
  const char *ctx_buf = nullptr;
  ctx.datum_len_ = sizeof(uint64_t); // for ObDecimalIntType, datum_len is not used but must be a legal value
  const common::ObObjType obj_type = static_cast<common::ObObjType>(col_headers_[col_idx].obj_type_);

  if (OB_UNLIKELY(col_end_stream_idx - col_first_stream_idx != 1)) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("integer must has one stream", K(ret), K(col_first_stream_idx), K(col_end_stream_idx));
  } else if (obj_type != ObDecimalIntType &&
      OB_FAIL(get_uint_data_datum_len(ObDatum::get_obj_datum_map_type(obj_type), ctx.datum_len_))) {
    LOG_WARN("fail to get datum len for obj type", K(ret), K(col_headers_[col_idx]), K(col_idx));
  } else {
    GET_STREAM_BUF(col_first_stream_idx);
    if (OB_SUCC(ret)) {
      ctx.ctx_ = reinterpret_cast<const ObIntegerStreamDecoderCtx *>(
        ctx_buf + transform_desc_.column_first_stream_decoding_ctx_offset_arr_[col_idx]);
      ctx.data_ = buf + transform_desc_.stream_data_pos_arr_[col_first_stream_idx].offset_;
      ctx.obj_meta_ = obj_meta;
      ctx.micro_block_header_ = get_micro_block_header();
      ctx.col_header_  = &get_column_header(col_idx);
      ctx.allocator_ = allocator_;
      if (ctx.col_header_->has_null_bitmap()) {
        ctx.null_flag_ = ObBaseColumnDecoderCtx::HAS_NULL_BITMAP;
        ctx.null_bitmap_ = get_column_meta(col_idx);
      } else if (ctx.ctx_->meta_.is_use_null_replace_value()) {
        ctx.null_flag_ = ObBaseColumnDecoderCtx::IS_NULL_REPLACED;
        ctx.null_replaced_value_ = ctx.ctx_->meta_.null_replaced_value_;
      } else {
        ctx.null_flag_ = ObBaseColumnDecoderCtx::HAS_NO_NULL;
        ctx.null_desc_ = nullptr;
      }
      LOG_TRACE("build_integer_column_decoder_ctx", K(col_first_stream_idx), K(col_end_stream_idx), K(col_idx), K(ctx));
    }
  }
  return ret;
}

int ObCSMicroBlockTransformHelper::build_string_column_decoder_ctx_(
                                    const ObObjMeta &obj_meta,
                                    const int32_t col_first_stream_idx,
                                    const int32_t col_end_stream_idx,
                                    const int32_t col_idx,
                                    ObStringColumnDecoderCtx &ctx)
{
  int ret = OB_SUCCESS;
  const char *buf = nullptr;
  const char *ctx_buf = nullptr;
  const int32_t col_stream_cnt = col_end_stream_idx - col_first_stream_idx;
  const ObCSColumnHeader &col_header = col_headers_[col_idx];
  if (OB_UNLIKELY(col_stream_cnt <= 0 || col_stream_cnt > 2)) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("stream count is illegal", K(ret), K(col_first_stream_idx), K(col_end_stream_idx));
  } else if (OB_UNLIKELY(col_header.is_fixed_length() && col_end_stream_idx - col_first_stream_idx != 1)) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("fixed length string must has one stream", K(ret),
        K(col_first_stream_idx), K(col_end_stream_idx), K(col_header));
  } else {
    GET_STREAM_BUF(col_first_stream_idx);
    if (OB_SUCC(ret)) {
      const ObObjTypeStoreClass store_class =
        get_store_class_map()[ob_obj_type_class(static_cast<common::ObObjType>(col_headers_[col_idx].obj_type_))];
      ctx.need_copy_ = ObCSEncodingUtil::is_store_class_need_copy(store_class);
      ctx.str_ctx_ = reinterpret_cast<const ObStringStreamDecoderCtx *>(ctx_buf +
        transform_desc_.column_first_stream_decoding_ctx_offset_arr_[col_idx]);
      ctx.str_data_ = buf + transform_desc_.stream_data_pos_arr_[col_first_stream_idx].offset_;
      ctx.obj_meta_ = obj_meta;
      ctx.micro_block_header_ = get_micro_block_header();
      ctx.col_header_  = &col_header;
      ctx.allocator_ = allocator_;
      if (col_header.has_null_bitmap()) {
        ctx.null_flag_ = ObBaseColumnDecoderCtx::HAS_NULL_BITMAP;
        ctx.null_bitmap_ = get_column_meta(col_idx);
      } else if (ctx.str_ctx_->meta_.is_use_zero_len_as_null()) {
        ctx.null_flag_ = ObBaseColumnDecoderCtx::IS_NULL_REPLACED;
      } else {
        ctx.null_flag_ = ObBaseColumnDecoderCtx::HAS_NO_NULL;
      }
      if (col_stream_cnt == 2) {
        const int64_t col_second_stream_idx = col_first_stream_idx + 1;
        GET_STREAM_BUF(col_second_stream_idx);
        if (OB_SUCC(ret)) {
          ctx.offset_ctx_ = reinterpret_cast<const ObIntegerStreamDecoderCtx *>(ctx_buf +
            transform_desc_.column_first_stream_decoding_ctx_offset_arr_[col_idx] +
            sizeof(ObStringStreamDecoderCtx));
          ctx.offset_data_ = buf + transform_desc_.stream_data_pos_arr_[col_second_stream_idx].offset_;
        }
      }
      LOG_TRACE("build_string_column_decoder_ctx", K(col_first_stream_idx), K(col_end_stream_idx), K(col_idx), K(ctx));
    }
  }

  return ret;
}
int ObCSMicroBlockTransformHelper::build_integer_dict_decoder_ctx_(const ObObjMeta &obj_meta,
                                                                   const int32_t col_first_stream_idx,
                                                                   const int32_t col_end_stream_idx,
                                                                   const int32_t col_idx,
                                                                   ObDictColumnDecoderCtx &ctx)
{
  int ret = OB_SUCCESS;
  const char *buf = nullptr;
  const char *ctx_buf = nullptr;
  const int32_t col_stream_cnt = col_end_stream_idx - col_first_stream_idx;
  ctx.dict_meta_ = reinterpret_cast<const ObDictEncodingMeta*>(get_column_meta(col_idx));
  ctx.obj_meta_ = obj_meta;
  ctx.micro_block_header_ = get_micro_block_header();
  ctx.col_header_  = &col_headers_[col_idx];
  ctx.allocator_ = allocator_;
  if (ctx.dict_meta_->has_null()) {
    ctx.null_flag_ = ObBaseColumnDecoderCtx::IS_NULL_REPLACED_REF;
    ctx.null_replaced_ref_ = ctx.dict_meta_->distinct_val_cnt_;
  } else {
    ctx.null_flag_ = ObBaseColumnDecoderCtx::HAS_NO_NULL;
  }
  if (col_stream_cnt == 0) {  // empty dict, has no stream
    // set nothing
  } else if (col_stream_cnt == 2) {
    GET_STREAM_BUF(col_first_stream_idx);
    if (OB_SUCC(ret)) {
      ctx.int_ctx_ = reinterpret_cast<const ObIntegerStreamDecoderCtx *>(ctx_buf +
          transform_desc_.column_first_stream_decoding_ctx_offset_arr_[col_idx]);
      ctx.int_data_ = buf + transform_desc_.stream_data_pos_arr_[col_first_stream_idx].offset_;
      const int32_t col_second_stream_idx = col_first_stream_idx + 1;
      GET_STREAM_BUF(col_second_stream_idx);
      if (OB_SUCC(ret)) {
        ctx.datum_len_ = sizeof(uint64_t); // for ObDecimalIntType, datum_len is not used but must be a legal value
        const common::ObObjType obj_type = static_cast<common::ObObjType>(col_headers_[col_idx].obj_type_);
        if (obj_type != ObDecimalIntType &&
            OB_FAIL(get_uint_data_datum_len(ObDatum::get_obj_datum_map_type(obj_type), ctx.datum_len_))) {
          LOG_WARN("fail to get datum len for obj type", K(ret), K(col_headers_[col_idx]), K(col_idx));
        } else {
          ctx.ref_ctx_ = reinterpret_cast<const ObIntegerStreamDecoderCtx *>(ctx_buf +
              transform_desc_.column_first_stream_decoding_ctx_offset_arr_[col_idx] +
              sizeof(ObIntegerStreamDecoderCtx));
          ctx.ref_data_ = buf + transform_desc_.stream_data_pos_arr_[col_second_stream_idx].offset_;

          LOG_TRACE("build_integer_dict_decoder_ctx",
              K(col_first_stream_idx), K(col_end_stream_idx), K(col_idx), K(ctx));
        }
      }
    }
  } else {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("integer dict must zero or two stream", K(ret), K(col_first_stream_idx),
      K(col_end_stream_idx));
  }
  return ret;
}


int ObCSMicroBlockTransformHelper::build_string_dict_decoder_ctx_(const ObObjMeta &obj_meta,
                                                                 const int32_t col_first_stream_idx,
                                                                 const int32_t col_end_stream_idx,
                                                                 const int32_t col_idx,
                                                                 ObDictColumnDecoderCtx &ctx)
{
  int ret = OB_SUCCESS;
  const char *buf = nullptr;
  const char *ctx_buf = nullptr;
  const int32_t col_stream_cnt = col_end_stream_idx - col_first_stream_idx;
  const ObCSColumnHeader &col_header = col_headers_[col_idx];
  if (OB_UNLIKELY(col_stream_cnt == 2 && !col_header.is_fixed_length())) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("dict string has two stream, must be fixed length", K(ret),
        K(col_first_stream_idx), K(col_end_stream_idx), K(col_header));
  } else {
    ctx.dict_meta_ = reinterpret_cast<const ObDictEncodingMeta*>(get_column_meta(col_idx));
    ctx.obj_meta_ = obj_meta;
    ctx.micro_block_header_ = get_micro_block_header();
    ctx.col_header_  =  &col_headers_[col_idx];
    ctx.allocator_ = allocator_;
    if (ctx.dict_meta_->has_null()) {
      ctx.null_flag_ = ObBaseColumnDecoderCtx::IS_NULL_REPLACED_REF;
      ctx.null_replaced_ref_ = ctx.dict_meta_->distinct_val_cnt_;
    } else {
      ctx.null_flag_ = ObBaseColumnDecoderCtx::HAS_NO_NULL;
    }
    if (col_stream_cnt == 0) {  // empty dict, has no stream
      // set nothing
    } else if (col_stream_cnt == 2) {
      GET_STREAM_BUF(col_first_stream_idx);
      if (OB_SUCC(ret)) {
        // fix length string dict, byte_stream + ref_stream
        const ObObjTypeStoreClass store_class =
            get_store_class_map()[ob_obj_type_class(static_cast<common::ObObjType>(ctx.col_header_->obj_type_))];
        ctx.need_copy_ = ObCSEncodingUtil::is_store_class_need_copy(store_class);
        ctx.str_ctx_ = reinterpret_cast<const ObStringStreamDecoderCtx *>(ctx_buf +
            transform_desc_.column_first_stream_decoding_ctx_offset_arr_[col_idx]);
        ctx.str_data_ = buf + transform_desc_.stream_data_pos_arr_[col_first_stream_idx].offset_;

        const int64_t col_second_stream_idx = col_first_stream_idx + 1;
        GET_STREAM_BUF(col_second_stream_idx);
        if (OB_SUCC(ret)) {
          ctx.ref_ctx_ = reinterpret_cast<const ObIntegerStreamDecoderCtx *>(ctx_buf +
              transform_desc_.column_first_stream_decoding_ctx_offset_arr_[col_idx] +
              sizeof(ObStringStreamDecoderCtx));
          ctx.ref_data_ = buf + transform_desc_.stream_data_pos_arr_[col_second_stream_idx].offset_;

          LOG_TRACE("build_string_dict_decoder_ctx",
              K(col_first_stream_idx), K(col_end_stream_idx), K(col_idx), K(ctx));
        }
      }
    } else if (col_stream_cnt == 3) {
      // var length string dict, bytes_stream + bytes_offset_stream + ref_stream
      GET_STREAM_BUF(col_first_stream_idx);
      if (OB_SUCC(ret)) {
        const ObObjTypeStoreClass store_class =
            get_store_class_map()[ob_obj_type_class(static_cast<common::ObObjType>(ctx.col_header_->obj_type_))];
        ctx.need_copy_ = ObCSEncodingUtil::is_store_class_need_copy(store_class);
        ctx.str_ctx_ = reinterpret_cast<const ObStringStreamDecoderCtx *>(ctx_buf +
            transform_desc_.column_first_stream_decoding_ctx_offset_arr_[col_idx]);
        ctx.str_data_ = buf + transform_desc_.stream_data_pos_arr_[col_first_stream_idx].offset_;

        const int32_t col_second_stream_idx = col_first_stream_idx + 1;
        GET_STREAM_BUF(col_second_stream_idx);
        if (OB_SUCC(ret)) {
          ctx.int_ctx_ = reinterpret_cast<const ObIntegerStreamDecoderCtx *>(ctx_buf +
            transform_desc_.column_first_stream_decoding_ctx_offset_arr_[col_idx] +
            sizeof(ObStringStreamDecoderCtx));
          ctx.offset_data_ = buf + transform_desc_.stream_data_pos_arr_[col_second_stream_idx].offset_;

          const int32_t col_third_stream_idx = col_first_stream_idx + 2;
          GET_STREAM_BUF(col_third_stream_idx);
          if (OB_SUCC(ret)) {
            ctx.ref_ctx_ = reinterpret_cast<const ObIntegerStreamDecoderCtx *>(ctx_buf +
                transform_desc_.column_first_stream_decoding_ctx_offset_arr_[col_idx] +
                sizeof(ObStringStreamDecoderCtx) + sizeof(ObIntegerStreamDecoderCtx));
            ctx.ref_data_ = buf + transform_desc_.stream_data_pos_arr_[col_third_stream_idx].offset_;

            LOG_TRACE("build_string_dict_decoder_ctx",
                K(col_first_stream_idx), K(col_end_stream_idx), K(col_idx), K(ctx));
          }
        }
      }
    } else {
      ret = OB_INNER_STAT_ERROR;
      LOG_WARN("stream count is illegal", K(ret), K(col_first_stream_idx), K(col_end_stream_idx));
    }
  }

  return ret;
}


}  // namespace blocksstable
}  // namespace oceanbase
