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

#ifndef OCEANBASE_ENCODING_OB_CS_MICRO_BLOCK_TRANSFORMER_H_
#define OCEANBASE_ENCODING_OB_CS_MICRO_BLOCK_TRANSFORMER_H_

#include "lib/compress/ob_compressor.h"
#include "ob_column_encoding_struct.h"
#include "ob_icolumn_cs_decoder.h"
#include "ob_stream_encoding_struct.h"
#include "storage/blocksstable/ob_micro_block_header.h"
#include "storage/blocksstable/ob_imicro_block_reader.h"

namespace oceanbase
{
namespace blocksstable
{
//================================= ObMicroBlockTransformDesc ==================================//
struct ObMicroBlockTransformDesc
{
  ObMicroBlockTransformDesc()
  {
    reset();
  }
  int64_t calc_serialize_size(const int32_t col_cnt, const int32_t stream_count) const;
  int deserialize(
    const int32_t col_cnt, const int32_t stream_count, char *buf, const int64_t len, int64_t &pos);
  void reset()
  {
    MEMSET(this, 0, sizeof(*this));
  }

  enum StreamDescAttr : uint8_t
  {
    IS_INTEGER = 0x01,
    NEED_PROJECT = 0x02,
    IN_PART_TRANSFORM_BUF = 0x04,
  };
  OB_INLINE bool is_integer_stream(const int32_t stream_idx) const
  {
    return stream_desc_attr_arr_[stream_idx] & IS_INTEGER;
  }
  OB_INLINE void set_is_integer_stream(const int32_t stream_idx)
  {
    stream_desc_attr_arr_[stream_idx] |= IS_INTEGER;
  }
  OB_INLINE bool is_need_project(const int32_t stream_idx) const
  {
    return stream_desc_attr_arr_[stream_idx] & NEED_PROJECT;
  }
  OB_INLINE void set_need_project(const int32_t stream_idx)
  {
    stream_desc_attr_arr_[stream_idx] |= NEED_PROJECT;
  }
  OB_INLINE bool is_stream_in_part_transfrom_buf(const int32_t stream_idx) const
  {
    return stream_desc_attr_arr_[stream_idx] & IN_PART_TRANSFORM_BUF;
  }
  OB_INLINE void set_stream_in_part_transfrom_buf(const int32_t stream_idx)
  {
    stream_desc_attr_arr_[stream_idx] |= IN_PART_TRANSFORM_BUF;
  }

  OB_INLINE void reset_stream_desc_attr(const int32_t stream_count)
  {
    MEMSET(stream_desc_attr_arr_, 0, sizeof(*stream_desc_attr_arr_) * stream_count);
  }

  int64_t to_string(const uint32_t col_cnt, const uint32_t stream_cnt, char *buf, const int64_t buf_len) const;

  struct Pos
  {
    uint32_t offset_;
    uint32_t len_;

    TO_STRING_KV(K_(offset), K_(len));
  };

  // For original desc, column meta offset is relative to payload buf.
  // For transformed desc, column meta offset is relative to header buf.
  Pos *column_meta_pos_arr_;
  uint32_t *column_first_stream_decoding_ctx_offset_arr_;
  uint16_t *column_first_stream_idx_arr_;
  uint8_t *stream_desc_attr_arr_;

  // For original desc, stream data offset is relative to the payload buf,
  // include stream meta, after transform the stream meta is removed.
  // For full transformed desc, stream data offset is relative to the buf allocated from kvcahe.
  // For part transformed format, the offset of stream data without compression is
  // relative to the header buf, the offset of stream data which has decompressed
  // is relative to the buf allocated by the caller.
  Pos *stream_data_pos_arr_;
};

//=============================== ObMicroBlockTransformDescPrinter ==============================//

struct ObMicroBlockTransformDescPrinter
{
  ObMicroBlockTransformDescPrinter(const uint32_t col_cnt,
                                   const uint32_t stream_cnt,
                                   const ObMicroBlockTransformDesc &desc)
      : col_cnt_(col_cnt), stream_cnt_(stream_cnt), desc_(desc)
  {
  }
  int64_t to_string(char *buf, const int64_t buf_len) const
  {
    return desc_.to_string(col_cnt_, stream_cnt_, buf, buf_len);
  }

  uint32_t col_cnt_;
  uint32_t stream_cnt_;
  const ObMicroBlockTransformDesc &desc_;

};

//====================================ObCSMicroBlockTransformer=================================//


class ObCSMicroBlockTransformer final
{
public:
  ObCSMicroBlockTransformer();
  ~ObCSMicroBlockTransformer() {}
  ObCSMicroBlockTransformer(const ObCSMicroBlockTransformer &) = delete;
  ObCSMicroBlockTransformer &operator=(const ObCSMicroBlockTransformer &) = delete;

  int init(const ObMicroBlockHeader *header, const char *payload_buf, const int64_t payload_len,
      const bool is_part_tranform = false, const int32_t *store_ids = nullptr, const int32_t store_ids_cnt = 0);
  int full_transform(char *buf, const int64_t buf_len, int64_t &pos);
  int part_transform(char *buf, const int64_t buf_len, int64_t &pos);

  int calc_full_transform_size(int64_t &size) const;
  int calc_part_transform_size(int64_t &size) const;

  int64_t to_string(char *buf, const int64_t buf_len) const;

  static int full_transform_block_data(const ObMicroBlockHeader &header,
                                       const char *payload_buf,
                                       const int64_t payload_size,
                                       const char *&dst_block_buf,
                                       int64_t &dst_buf_size,
                                       ObIAllocator *allocator);

  int dump_cs_encoding_info(char *hex_print_buf, const int64_t buf_size);

private:
  int decode_stream_offsets_(const int64_t payload_len);
  int build_all_string_data_desc_(const int64_t payload_len);
  int build_original_transform_desc_(const int32_t *store_ids, const int32_t store_ids_cnt);
  int build_stream_decoder_ctx_();

private:
  static const int32_t STREAM_OFFSETS_CACHED_COUNT = 128;
  bool is_inited_;
  bool is_part_tranform_;
  bool has_string_need_project_;
  common::ObCompressor *compressor_;
  const ObMicroBlockHeader *header_;
  const char *payload_buf_;
  const ObAllColumnHeader *all_column_header_;
  const ObCSColumnHeader *column_headers_;
  uint32_t column_meta_begin_offset_;  // this offset is relative to the payload_buf_
  uint32_t stream_offsets_cached_arr_[STREAM_OFFSETS_CACHED_COUNT];
  uint32_t *stream_offsets_arr_;  // this offset is relative to the micro block header

  ObMicroBlockTransformDesc original_desc_;
  char *orig_desc_buf_;
  int32_t orig_desc_buf_size_;

  char *stream_decoding_ctx_buf_;
  int32_t stream_decoding_ctx_buf_size_;

  // string stream decoding don't need row count,
  // here only need record row count for integer stream.
  // normally, the row count is equal to the record in the micro block header,
  // except for dict encoding where the distinct_value_count and const_ref_count need to be record
  uint32_t *stream_row_cnt_arr_;
  uint16_t *stream_meta_len_arr_;

  uint32_t all_string_data_offset_; // this offset is relative to payload_buf
  uint32_t all_string_uncompress_len_;
  common::ObArenaAllocator allocator_;
};

//==============================ObCSMicroBlockTransformHelper=================================//

class ObCSMicroBlockTransformHelper
{
public:
  ObCSMicroBlockTransformHelper();
  ~ObCSMicroBlockTransformHelper() {}
  ObCSMicroBlockTransformHelper(const ObCSMicroBlockTransformHelper &) = delete;
  ObCSMicroBlockTransformHelper &operator=(const ObCSMicroBlockTransformHelper &) = delete;

  void reset();
  //@store_ids: represent the column ids array which need be part transformed.
  //            for full transformed data, this param is ignored.
  //            for not full transformed data, only the column ids which need to be projected are passed.
  //@store_ids_cnt: the column ids array size
  int init(common::ObIAllocator *allocator, const ObMicroBlockData &block_data,
      const int32_t *store_ids, const int32_t store_ids_cnt);

  int build_column_decoder_ctx(const ObObjMeta &obj_meta, const int32_t col_idx, ObColumnCSDecoderCtx &ctx);

  bool is_inited() const
  {
    return is_inited_;
  }
  const ObMicroBlockHeader *get_micro_block_header() const
  {
    return header_;
  }
  const ObAllColumnHeader *get_all_column_header()
  {
    return all_col_header_;
  }
  const ObCSColumnHeader &get_column_header(const int32_t col_idx) const
  {
    return col_headers_[col_idx];
  }
  const char *get_column_meta(const int32_t col_idx)
  {
    return block_data_.get_buf() + transform_desc_.column_meta_pos_arr_[col_idx].offset_;
  }


private:
  int build_tranform_desc_(const int32_t *store_ids, const int32_t store_ids_cnt);
  int build_integer_column_decoder_ctx_(const ObObjMeta &obj_meta,
                                        const int32_t col_first_stream_idx,
                                        const int32_t col_end_stream_idx,
                                        const int32_t col_idx,
                                        ObIntegerColumnDecoderCtx &ctx);
  int build_string_column_decoder_ctx_(const ObObjMeta &obj_meta,
                                       const int32_t col_first_stream_idx,
                                       const int32_t col_end_stream_idx,
                                       const int32_t col_idx,
                                       ObStringColumnDecoderCtx &ctx);
  int build_integer_dict_decoder_ctx_(const ObObjMeta &obj_meta,
                                      const int32_t col_first_stream_idx,
                                      const int32_t col_end_stream_idx,
                                      const int32_t col_idx,
                                      ObDictColumnDecoderCtx &ctx);
  int build_string_dict_decoder_ctx_(const ObObjMeta &obj_meta,
                                     const int32_t col_first_stream_idx,
                                     const int32_t col_end_stream_idx,
                                     const int32_t col_idx,
                                     ObDictColumnDecoderCtx &ctx);

private:
  bool is_inited_;
  common::ObIAllocator *allocator_;
  ObMicroBlockData block_data_;
  const ObMicroBlockHeader *header_;
  const ObAllColumnHeader *all_col_header_;
  const ObCSColumnHeader *col_headers_;
  ObMicroBlockTransformDesc transform_desc_;
  char *part_transform_buf_;
  int32_t part_transform_buf_size_;
};

}  // namespace blocksstable
}  // namespace oceanbase

#endif
