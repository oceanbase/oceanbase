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

#ifndef OCEANBASE_ENCODING_OB_STRING_DIFF_DECODER_H_
#define OCEANBASE_ENCODING_OB_STRING_DIFF_DECODER_H_

#include "ob_icolumn_decoder.h"
#include "ob_encoding_util.h"
#include "storage/blocksstable/ob_data_buffer.h"
#include "ob_string_diff_encoder.h"

namespace oceanbase
{
namespace blocksstable
{

struct ObColumnHeader;
struct ObStringDiffHeader;

class ObStringDiffDecoder : public ObIColumnDecoder
{
public:
  static const ObColumnHeader::Type type_ = ObColumnHeader::STRING_DIFF;

  ObStringDiffDecoder();
  virtual ~ObStringDiffDecoder();

  OB_INLINE int init(
      const ObMicroBlockHeader &micro_block_header,
      const ObColumnHeader &column_header,
      const char *meta);

  virtual int decode(const ObColumnDecoderCtx &ctx, common::ObDatum &datum, const int64_t row_id,
      const ObBitStream &bs, const char *data, const int64_t len) const override;

  virtual int update_pointer(const char *old_block, const char *cur_block) override;

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

  void reset() { this->~ObStringDiffDecoder(); new (this) ObStringDiffDecoder(); }
  OB_INLINE void reuse();
  virtual ObColumnHeader::Type get_type() const { return type_; }

  bool is_inited() const { return NULL != header_; }
private:
  template <typename VectorType, bool HAS_NULL, bool HEX_PACKED>
  int decode_vector_from_fixed_data(
      const ObColumnDecoderCtx &decoder_ctx,
      const ObIRowIndex *row_index,
      ObVectorDecodeCtx &vector_ctx) const;

  template <typename VectorType, bool HAS_NULL, bool HEX_PACKED>
  int decode_vector_from_var_len_data(
      const ObColumnDecoderCtx &decoder_ctx,
      const ObIRowIndex *row_index,
      ObVectorDecodeCtx &vector_ctx) const;
private:
  const ObStringDiffHeader *header_;
};

OB_INLINE int ObStringDiffDecoder::init(
    const ObMicroBlockHeader &micro_block_header,
    const ObColumnHeader &column_header,
    const char *meta)
{
  UNUSEDx(micro_block_header, column_header);
  // performance critical, don't check params, already checked upper layer
  int ret = common::OB_SUCCESS;
  if (is_inited()) {
    ret = common::OB_INIT_TWICE;
    STORAGE_LOG(WARN, "init twice", K(ret));
  } else {
    meta += column_header.offset_;
    header_ = reinterpret_cast<const ObStringDiffHeader *>(meta);
  }
  return ret;
}

OB_INLINE void ObStringDiffDecoder::reuse()
{
  header_ = NULL;
  /*
  obj_meta_.reset();
  micro_block_header_ = NULL;
  col_header_ = NULL;
  header_ = NULL;
  */
}
} // end namespace blocksstable
} // end namespace oceanbase
#endif // OCEANBASE_ENCODING_OB_STRING_DIFF_DECODER_H_
