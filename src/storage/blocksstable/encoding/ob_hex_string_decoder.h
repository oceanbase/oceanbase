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

#ifndef OCEANBASE_ENCODING_OB_HEX_STRING_DECODER_H_
#define OCEANBASE_ENCODING_OB_HEX_STRING_DECODER_H_

#include "ob_icolumn_decoder.h"
#include "ob_encoding_util.h"
#include "storage/blocksstable/ob_data_buffer.h"
#include "ob_hex_string_encoder.h"
#include "ob_bit_stream.h"

namespace oceanbase
{
namespace blocksstable
{

struct ObColumnHeader;
struct ObHexStringHeader;

class ObHexStringDecoder : public ObIColumnDecoder
{
public:
  static const ObColumnHeader::Type type_ = ObColumnHeader::HEX_PACKING;
  ObHexStringDecoder();
  ~ObHexStringDecoder();

  OB_INLINE int init(
      const ObMicroBlockHeader &micro_block_header,
      const ObColumnHeader &column_header,
      const char *meta);

  virtual int decode(const ObColumnDecoderCtx &ctx, common::ObDatum &datum, const int64_t row_id,
      const ObBitStream &bs, const char *data, const int64_t len) const override;

  virtual int update_pointer(const char *old_block, const char *cur_block) override;

  void reset() { this->~ObHexStringDecoder(); new (this) ObHexStringDecoder(); }
  OB_INLINE void reuse();
  virtual ObColumnHeader::Type get_type() const override { return type_; }

  bool is_inited() const { return NULL != header_; }

  virtual int batch_decode(
      const ObColumnDecoderCtx &ctx,
      const ObIRowIndex* row_index,
      const int32_t *row_ids,
      const char **cell_datas,
      const int64_t row_cap,
      common::ObDatum *datums) const override;

  virtual int decode_vector(
      const ObColumnDecoderCtx &decoder_ctx,
      const ObIRowIndex* row_index,
      ObVectorDecodeCtx &vector_ctx) const override;

  virtual int pushdown_operator(
      const sql::ObPushdownFilterExecutor *parent,
      const ObColumnDecoderCtx &col_ctx,
      const sql::ObWhiteFilterExecutor &filter_node,
      const char* meta_data,
      const ObIRowIndex* row_index,
      const sql::PushdownFilterInfo &pd_filter_info,
      ObBitmap &result_bitmap) const override;
private:
  template <typename VectorType>
  int inner_decode_vector(
      const ObColumnDecoderCtx &decoder_ctx,
      const ObIRowIndex* row_index,
      ObVectorDecodeCtx &vector_ctx) const;
private:
  const ObHexStringHeader *header_;
};

OB_INLINE int ObHexStringDecoder::init(
    const ObMicroBlockHeader &micro_block_header,
    const ObColumnHeader &column_header,
    const char *meta)
{
  // performance critical, don't check params, already checked upper layer
  UNUSEDx(micro_block_header);
  int ret = common::OB_SUCCESS;
  if (is_inited()) {
    ret = common::OB_INIT_TWICE;
    STORAGE_LOG(WARN, "init twice", K(ret));
  } else {

    meta += column_header.offset_;
    header_ = reinterpret_cast<const ObHexStringHeader *>(meta);
  }
  return ret;
}

OB_INLINE void ObHexStringDecoder::reuse()
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

#endif // OCEANBASE_ENCODING_OB_HEX_STRING_DECODER_H_
