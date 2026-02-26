/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#pragma once
#include "ob_icolumn_decoder.h"
#include "ob_encoding_util.h"
#include "ob_integer_array.h"
#include "ob_dict_decoder.h"

namespace oceanbase
{
namespace observer
{
namespace table_load_backup
{
class ObColumnHeader;

struct ObRLEMetaHeader
{
  uint16_t count_;
  // the offset of dict
  uint16_t offset_;
  union {
    struct {
      uint8_t row_id_byte_:2;
      uint8_t ref_byte_:2;
    };
    uint8_t attr_;
  };
  char payload_[0];

  ObRLEMetaHeader() { reset(); }
  void reset() { memset(this, 0, sizeof(*this)); }

  TO_STRING_KV(K_(count), K_(offset), K_(row_id_byte), K_(ref_byte));
}__attribute__((packed));

class ObRLEDecoder : public ObIColumnDecoder
{
public:
  static const ObColumnHeader::Type type_ = ObColumnHeader::RLE;

  ObRLEDecoder() : meta_header_(NULL),
                   ref_offset_(0), dict_decoder_()
  {
  }
  virtual ~ObRLEDecoder() {}

  OB_INLINE int init(const common::ObObjMeta &obj_meta,
      const ObMicroBlockHeaderV2 &micro_block_header,
      const ObColumnHeader &column_header,
      const char *block_data);

  virtual int decode(ObColumnDecoderCtx &ctx, common::ObObj &cell, const int64_t row_id,
      const ObBitStream &bs, const char *data, const int64_t len) const override;

  virtual int batch_decode(
      const ObColumnDecoderCtx &ctx,
      const ObIRowIndex* row_index,
      const int64_t *row_ids,
      const char **cell_datas,
      const int64_t row_cap,
      common::ObDatum *datums) const override;

  virtual int get_null_count(
      const ObColumnDecoderCtx &ctx,
      const ObIRowIndex *row_index,
      const int64_t *row_ids,
      const int64_t row_cap,
      int64_t &null_count) const override;

  virtual int update_pointer(const char *old_block, const char *cur_block) override;

  void reset() { this->~ObRLEDecoder(); new (this) ObRLEDecoder(); }
  OB_INLINE void reuse();
  virtual ObColumnHeader::Type get_type() const override { return type_; }

  bool is_inited() const { return NULL != meta_header_; }

  virtual int pushdown_operator(
      const ObPushdownFilterExecutor *parent,
      const ObColumnDecoderCtx &col_ctx,
      const ObWhiteFilterExecutor &filter,
      const char* meta_data,
      const ObIRowIndex* row_index,
      ObBitmap &result_bitmap) const override;

private:
  int nu_nn_operator(
      const ObPushdownFilterExecutor *parent,
      const ObColumnDecoderCtx &col_ctx,
      const ObWhiteFilterExecutor &filter,
      ObBitmap &result_bitmap) const;

  int eq_ne_operator(
      const ObPushdownFilterExecutor *parent,
      const ObColumnDecoderCtx &col_ctx,
      const ObWhiteFilterExecutor &filter,
      ObBitmap &result_bitmap) const;

  int comparison_operator(
      const ObPushdownFilterExecutor *parent,
      const ObColumnDecoderCtx &col_ctx,
      const ObWhiteFilterExecutor &filter,
      ObBitmap &result_bitmap) const;

  int bt_operator(
      const ObPushdownFilterExecutor *parent,
      const ObColumnDecoderCtx &col_ctx,
      const ObWhiteFilterExecutor &filter,
      ObBitmap &result_bitmap) const;

  int in_operator(
      const ObPushdownFilterExecutor *parent,
      const ObColumnDecoderCtx &col_ctx,
      const ObWhiteFilterExecutor &filter,
      ObBitmap &result_bitmap) const;

  int cmp_ref_and_set_res(
      const ObPushdownFilterExecutor *parent,
      const ObColumnDecoderCtx &col_ctx,
      const int64_t dict_ref,
      const ObFPIntCmpOpType cmp_op,
      bool flag,
      ObBitmap &result_bitmap) const;

  int set_res_with_bitset(
      const ObPushdownFilterExecutor *parent,
      const ObColumnDecoderCtx &col_ctx,
      const sql::ObBitVector *ref_bitset,
      ObBitmap &result_bitmap) const;

  int extract_ref_and_null_count(
      const int64_t *row_ids,
      const int64_t row_cap,
      common::ObDatum *datums,
      int64_t &null_count) const;

private:
  const ObRLEMetaHeader *meta_header_;
  uint16_t ref_offset_;
  ObDictDecoder dict_decoder_;
};

OB_INLINE int ObRLEDecoder::init(const common::ObObjMeta &obj_meta,
    const ObMicroBlockHeaderV2 &micro_block_header,
    const ObColumnHeader &column_header,
    const char *block_data)
{
  UNUSEDx(micro_block_header, column_header);
  // performance critical, don't check params
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(is_inited())) {
    ret = common::OB_INIT_TWICE;
    STORAGE_LOG(WARN, "init twice", K(ret));
  } else {
    meta_header_ = reinterpret_cast<const ObRLEMetaHeader *>
        (block_data + column_header.offset_);
    const char *dict_meta_header = block_data + column_header.offset_
        + meta_header_->offset_;

    if (OB_FAIL(dict_decoder_.init(obj_meta, dict_meta_header))) {
      STORAGE_LOG(WARN, "failed to init dict decoder", K(ret), KP(dict_meta_header));
      meta_header_ = NULL;
    } else {
      ref_offset_ = static_cast<int16_t>(meta_header_->count_ * meta_header_->row_id_byte_);
    }
  }
  return ret;
}

OB_INLINE void ObRLEDecoder::reuse()
{
  meta_header_ = NULL;
  /*
  obj_meta_.reset();
  micro_block_header_ = NULL;
  column_header_ = NULL;
  meta_header_ = NULL;
  // row_id_array_
  // ref_array_
  */
  dict_decoder_.reuse();
}

} // table_load_backup
} // namespace observer
} // namespace oceanbase
