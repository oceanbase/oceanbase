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

#ifndef OCEANBASE_ENCODING_OB_INTER_COLUMN_SUBSTR_DECODER_H_
#define OCEANBASE_ENCODING_OB_INTER_COLUMN_SUBSTR_DECODER_H_

#include "ob_icolumn_decoder.h"
#include "ob_encoding_util.h"
#include "ob_integer_array.h"
#include "ob_encoding_bitset.h"
#include "ob_inter_column_substring_encoder.h"

namespace oceanbase
{
namespace blocksstable
{

struct ObColumnHeader;
struct ObInterColSubStrMetaHeader;

class ObInterColSubStrDecoder : public ObSpanColumnDecoder
{
public:
  static const ObColumnHeader::Type type_ = ObColumnHeader::COLUMN_SUBSTR;
  ObInterColSubStrDecoder();
  virtual ~ObInterColSubStrDecoder();

  OB_INLINE int init(
                  const ObMicroBlockHeader &micro_block_header,
                  const ObColumnHeader &column_header,
                  const char *meta);
  virtual int decode(ObColumnDecoderCtx &ctx, common::ObObj &cell, const int64_t row_id,
      const ObBitStream &bs, const char *data, const int64_t len) const override;

  virtual int update_pointer(const char *old_block, const char *cur_block) override;

  virtual int get_ref_col_idx(int64_t &ref_col_idx) const override;

  void reset() { this->~ObInterColSubStrDecoder(); new (this) ObInterColSubStrDecoder(); }
  OB_INLINE void reuse();
  virtual ObColumnHeader::Type get_type() const override { return type_; }

  bool is_inited() const { return NULL != meta_header_; }

  virtual bool can_vectorized() const override { return false; }

protected:
  inline bool has_exc(const ObColumnDecoderCtx &ctx) const
  { return ctx.col_header_->length_ > sizeof(ObInterColSubStrMetaHeader); }

private:
  const ObInterColSubStrMetaHeader *meta_header_;
};

OB_INLINE int ObInterColSubStrDecoder::init(
    const ObMicroBlockHeader &micro_block_header,
    const ObColumnHeader &column_header,
    const char *meta)
{
  int ret = common::OB_SUCCESS;
  UNUSEDx(micro_block_header, column_header);
  // performance critical, don't check params, already checked upper layer
  if (OB_UNLIKELY(is_inited())) {
    ret = common::OB_INIT_TWICE;
    STORAGE_LOG(WARN, "init twice", K(ret));
  } else {
    meta += column_header.offset_;
    meta_header_ = reinterpret_cast<const ObInterColSubStrMetaHeader *>(meta);
    STORAGE_LOG(DEBUG, "decoder meta", K(*meta_header_));
  }
  return ret;
}

OB_INLINE void ObInterColSubStrDecoder::reuse()
{
  meta_header_ = NULL;
  /*
  ref_decoder_ = NULL;
  obj_meta_.reset();
  micro_block_header_ = NULL;
  column_header_ = NULL;
  meta_header_ = NULL;
  meta_data_ = NULL;
  //len_ = 0;
  //count_ = 0;
  opt_.reset();
  meta_reader_.reuse();
  */
}


} // end namespace blocksstable
} // end namespace oceanbase

#endif // OCEANBASE_ENCODING_OB_INTER_COLUMN_SUBSTR_DECODER_H_
