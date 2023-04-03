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

#ifndef OCEANBASE_ENCODING_OB_CONST_DECODER_H_
#define OCEANBASE_ENCODING_OB_CONST_DECODER_H_

#include "ob_icolumn_decoder.h"
#include "ob_encoding_util.h"
#include "ob_integer_array.h"
#include "ob_dict_decoder.h"
#include "ob_const_encoder.h"

namespace oceanbase
{
namespace blocksstable
{

struct ObColumnHeader;
struct ObConstMetaHeader;

class ObConstDecoder : public ObIColumnDecoder
{
public:
  static const ObColumnHeader::Type type_ = ObColumnHeader::CONST;
  ObConstDecoder() : meta_header_(NULL), dict_decoder_()
  {
  }
  virtual ~ObConstDecoder() {}

  OB_INLINE int init(
      const ObMicroBlockHeader &micro_block_header,
      const ObColumnHeader &column_header,
      const char *block_data);
  virtual int decode(ObColumnDecoderCtx &ctx, common::ObObj &cell, const int64_t row_id,
      const ObBitStream &bs, const char *data, const int64_t len) const override;
  virtual int update_pointer(const char *old_block, const char *cur_block) override;

  void reset() { this->~ObConstDecoder(); new (this) ObConstDecoder(); }
  OB_INLINE void reuse();
  virtual ObColumnHeader::Type get_type() const override { return type_; }
  bool is_inited() const { return NULL != meta_header_; }

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

  virtual int pushdown_operator(
      const sql::ObPushdownFilterExecutor *parent,
      const ObColumnDecoderCtx &col_ctx,
      const sql::ObWhiteFilterExecutor &filter,
      const char* meta_data,
      const ObIRowIndex* row_index,
      ObBitmap &result_bitmap) const override;
protected:
  int decode_without_dict(const ObColumnDecoderCtx &ctx, common::ObObj &cell) const;
  int batch_decode_without_dict(
      const ObColumnDecoderCtx &ctx,
      const int64_t row_cap,
      common::ObDatum *datums) const;

private:
  int const_only_operator(
      const ObColumnDecoderCtx &col_ctx,
      const sql::ObWhiteFilterExecutor &filter,
      ObBitmap &result_bitmap) const;

  int nu_nn_operator(
      const ObColumnDecoderCtx &col_ctx,
      const sql::ObWhiteFilterExecutor &filter,
      ObBitmap &result_bitmap) const;

  int comparison_operator(
      const ObColumnDecoderCtx &col_ctx,
      const sql::ObWhiteFilterExecutor &filter,
      ObBitmap &result_bitmap) const;

  int bt_operator(
      const ObColumnDecoderCtx &col_ctx,
      const sql::ObWhiteFilterExecutor &filter,
      ObBitmap &result_bitmap) const;

  int in_operator(
      const ObColumnDecoderCtx &col_ctx,
      const sql::ObWhiteFilterExecutor &filter,
      ObBitmap &result_bitmap) const;

  int traverse_refs_and_set_res(
      const ObIntArrayFuncTable &row_ids,
      const int64_t dict_ref,
      const bool flag,
      ObBitmap &result_bitmap) const;

  int set_res_with_bitset(
      const ObIntArrayFuncTable &row_ids,
      const sql::ObBitVector *ref_bitset,
      const bool flag,
      ObBitmap &result_bitmap) const;

  int extract_ref_and_null_count(
      const int64_t *row_ids,
      const int64_t row_cap,
      common::ObDatum *datums,
      int64_t &null_count) const;

private:
  const ObConstMetaHeader *meta_header_;
  ObDictDecoder dict_decoder_;
};

OB_INLINE int ObConstDecoder::init(
    const ObMicroBlockHeader &micro_block_header,
    const ObColumnHeader &column_header,
    const char *meta_data)
{
  UNUSEDx(micro_block_header);
  // performance critical, don't check params
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(is_inited())) {
    ret = common::OB_INIT_TWICE;
    STORAGE_LOG(WARN, "init twice", K(ret));
  } else {
    meta_data += column_header.offset_;
    meta_header_ = reinterpret_cast<const ObConstMetaHeader *>(meta_data);
    const char *dict_data = meta_data + meta_header_->offset_;
    if (meta_header_->count_ > 0) {
      if (OB_FAIL(dict_decoder_.init(column_header.get_store_obj_type(), dict_data))) {
        STORAGE_LOG(WARN, "failed to init dict decoder", K(ret), KP(dict_data));
        meta_header_ = NULL;
      }
    }
  }
  return ret;
}

OB_INLINE void ObConstDecoder::reuse()
{
  meta_header_ = NULL;
  /*
  obj_meta_.reset();
  micro_block_header_ = NULL;
  column_header_ = NULL;
  meta_header_ = NULL;
  // row_id_array_
  // ref_array_
  count_ = 0;
  integer_mask_ = 0;
  data_buf_ = NULL;
  single_len_ = 0;
  sc_ = ObExtendSC;
  */
  dict_decoder_.reuse();
}


} // end namespace blocksstable
} // end namespace oceanbase

#endif // OCEANBASE_ENCODING_OB_CONST_DECODER_H_
