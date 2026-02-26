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

#ifndef OCEANBASE_ENCODING_OB_STRING_PREFIX_DECODER_H_
#define OCEANBASE_ENCODING_OB_STRING_PREFIX_DECODER_H_

#include "ob_icolumn_decoder.h"
#include "ob_encoding_util.h"
#include "storage/blocksstable/ob_data_buffer.h"
#include "ob_string_prefix_encoder.h"

namespace oceanbase
{
namespace blocksstable
{

struct ObColumnHeader;
struct ObStringPrefixMetaHeader;
struct ObStringPrefixCellHeader;

class ObStringPrefixDecoder : public ObIColumnDecoder
{
public:
  static const ObColumnHeader::Type type_ = ObColumnHeader::STRING_PREFIX;
  ObStringPrefixDecoder() : meta_header_(NULL), meta_data_(NULL)
  {}
  virtual ~ObStringPrefixDecoder();

  OB_INLINE int init(
           const ObMicroBlockHeader &micro_block_header,
           const ObColumnHeader &column_header,
           const char *meta);

  virtual int decode(const ObColumnDecoderCtx &ctx, common::ObDatum &datum, const int64_t row_id,
      const ObBitStream &bs, const char *data, const int64_t len) const override;

  virtual int update_pointer(const char *old_block, const char *cur_block) override;

  void reset() { this->~ObStringPrefixDecoder(); new (this) ObStringPrefixDecoder(); }
  OB_INLINE void reuse();
  virtual ObColumnHeader::Type get_type() const { return type_; }
  bool is_inited() const { return NULL != meta_header_; }

  virtual int batch_decode(
      const ObColumnDecoderCtx &ctx,
      const ObIRowIndex* row_index,
      const int32_t *row_ids,
      const char **cell_datas,
      const int64_t row_cap,
      common::ObDatum *datums) const override;
  virtual int decode_vector(
      const ObColumnDecoderCtx &decoder_ctx,
      const ObIRowIndex *row_index,
      ObVectorDecodeCtx &vector_ctx) const override;
  virtual int get_null_count(
      const ObColumnDecoderCtx &ctx,
      const ObIRowIndex *row_index,
      const int32_t *row_ids,
      const int64_t row_cap,
      int64_t &null_count) const override;
private:
  template <typename VectorType, bool HAS_NULL, bool HEX_PACKED>
  int fill_vector(
      const ObColumnDecoderCtx &ctx,
      const ObIRowIndex *row_index,
      ObVectorDecodeCtx &vector_ctx) const;
private:
  const ObStringPrefixMetaHeader *meta_header_;
  const char *meta_data_;
};

OB_INLINE int ObStringPrefixDecoder::init(
    const ObMicroBlockHeader &micro_block_header,
    const ObColumnHeader &column_header,
    const char *meta)
{
  UNUSEDx(micro_block_header, column_header);
  // performance critical, don't check params, already checked upper layer
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(is_inited())) {
    ret = common::OB_INIT_TWICE;
    STORAGE_LOG(WARN, "init twice", K(ret));
  } else {
    meta += column_header.offset_;
    meta_header_ = reinterpret_cast<const ObStringPrefixMetaHeader *>(meta);
    meta_data_ = meta + sizeof(ObStringPrefixMetaHeader);
    STORAGE_LOG(DEBUG, "debug", K(meta_header_->is_hex_packing()),
        K(meta_header_->hex_char_array_size_));
    meta_data_ += meta_header_->is_hex_packing() ? meta_header_->hex_char_array_size_ : 0;
  }
  return ret;
}

OB_INLINE void ObStringPrefixDecoder::reuse()
{
  meta_header_ = NULL;
  /*
  obj_meta_.reset();
  micro_block_header_ = NULL;
  column_header_ = NULL;
  meta_header_ = NULL;
  meta_data_ = NULL;
  prefix_index_byte_ = 0;
  */
}


} // end namespace blocksstable
} // end namespace oceanbase

#endif // OCEANBASE_ENCODING_OB_STRING_PREFIX_DECODER_H_
