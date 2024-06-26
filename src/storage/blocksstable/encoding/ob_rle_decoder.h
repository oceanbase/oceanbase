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

#ifndef OCEANBASE_ENCODING_OB_RLE_DECODER_H_
#define OCEANBASE_ENCODING_OB_RLE_DECODER_H_

#include "ob_icolumn_decoder.h"
#include "ob_encoding_util.h"
#include "ob_integer_array.h"
#include "ob_dict_decoder.h"
#include "ob_rle_encoder.h"

namespace oceanbase
{
namespace blocksstable
{

struct ObColumnHeader;
struct ObRLEMetaHeader;

class ObRLEDecoder : public ObIColumnDecoder
{
public:
  static const ObColumnHeader::Type type_ = ObColumnHeader::RLE;

  ObRLEDecoder() : meta_header_(NULL),
                   ref_offset_(0), dict_decoder_()
  {
  }
  virtual ~ObRLEDecoder() {}

  OB_INLINE int init(
      const ObMicroBlockHeader &micro_block_header,
      const ObColumnHeader &column_header,
      const char *block_data);

  virtual int decode(const ObColumnDecoderCtx &ctx, common::ObDatum &datum, const int64_t row_id,
      const ObBitStream &bs, const char *data, const int64_t len) const override;

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

  virtual int update_pointer(const char *old_block, const char *cur_block) override;

  void reset() { this->~ObRLEDecoder(); new (this) ObRLEDecoder(); }
  OB_INLINE void reuse();
  virtual ObColumnHeader::Type get_type() const override { return type_; }

  bool is_inited() const { return NULL != meta_header_; }

  virtual int pushdown_operator(
      const sql::ObPushdownFilterExecutor *parent,
      const ObColumnDecoderCtx &col_ctx,
      const sql::ObWhiteFilterExecutor &filter,
      const char* meta_data,
      const ObIRowIndex* row_index,
      const sql::PushdownFilterInfo &pd_filter_info,
      ObBitmap &result_bitmap) const override;

private:
  int nu_nn_operator(
      const sql::ObPushdownFilterExecutor *parent,
      const ObColumnDecoderCtx &col_ctx,
      const sql::ObWhiteFilterExecutor &filter,
      const sql::PushdownFilterInfo &pd_filter_info,
      ObBitmap &result_bitmap) const;

  int eq_ne_operator(
      const sql::ObPushdownFilterExecutor *parent,
      const ObColumnDecoderCtx &col_ctx,
      const sql::ObWhiteFilterExecutor &filter,
      const sql::PushdownFilterInfo &pd_filter_info,
      ObBitmap &result_bitmap) const;

  int comparison_operator(
      const sql::ObPushdownFilterExecutor *parent,
      const ObColumnDecoderCtx &col_ctx,
      const sql::ObWhiteFilterExecutor &filter,
      const sql::PushdownFilterInfo &pd_filter_info,
      ObBitmap &result_bitmap) const;

  int bt_operator(
      const sql::ObPushdownFilterExecutor *parent,
      const ObColumnDecoderCtx &col_ctx,
      const sql::ObWhiteFilterExecutor &filter,
      const sql::PushdownFilterInfo &pd_filter_info,
      ObBitmap &result_bitmap) const;

  int in_operator(
      const sql::ObPushdownFilterExecutor *parent,
      const ObColumnDecoderCtx &col_ctx,
      const sql::ObWhiteFilterExecutor &filter,
      const sql::PushdownFilterInfo &pd_filter_info,
      ObBitmap &result_bitmap) const;

  int cmp_ref_and_set_res(
      const sql::ObPushdownFilterExecutor *parent,
      const ObColumnDecoderCtx &col_ctx,
      const int64_t dict_ref,
      const sql::ObWhiteFilterOperatorType cmp_op,
      bool flag,
      const sql::PushdownFilterInfo &pd_filter_info,
      ObBitmap &result_bitmap) const;

  int set_res_with_bitset(
      const sql::ObPushdownFilterExecutor *parent,
      const ObColumnDecoderCtx &col_ctx,
      const sql::ObBitVector *ref_bitset,
      const sql::PushdownFilterInfo &pd_filter_info,
      ObBitmap &result_bitmap) const;

  template <typename T>
  int extract_ref_and_null_count(
      const int32_t *row_ids,
      const int64_t row_cap,
      T ref_buf,
      int64_t &null_count) const;

  template <typename T>
  void load_ref_to_buf(T ref_buf, const int64_t trav_idx, const uint32_t ref) const;

  virtual int get_distinct_count(int64_t &distinct_count) const override;

  virtual int read_distinct(
      const ObColumnDecoderCtx &ctx,
      const char **cell_datas,
      storage::ObGroupByCell &group_by_cell) const override;

  virtual int read_reference(
      const ObColumnDecoderCtx &ctx,
      const int32_t *row_ids,
      const int64_t row_cap,
      storage::ObGroupByCell &group_by_cell) const override;

  bool has_null_value() const;

private:
  const ObRLEMetaHeader *meta_header_;
  uint16_t ref_offset_;
  ObDictDecoder dict_decoder_;
};

OB_INLINE int ObRLEDecoder::init(
    const ObMicroBlockHeader &micro_block_header,
    const ObColumnHeader &column_header,
    const char *block_data)
{
  UNUSEDx(micro_block_header);
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

    if (OB_FAIL(dict_decoder_.init(column_header.get_store_obj_type(), dict_meta_header))) {
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
} // end namespace blocksstable
} // end namespace oceanbase

#endif // OCEANBASE_ENCODING_OB_RLE_DECODER_H_
