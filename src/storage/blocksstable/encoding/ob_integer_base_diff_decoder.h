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

#ifndef OCEANBASE_ENCODING_OB_INTEGER_BASE_DIFF_DECODER_H_
#define OCEANBASE_ENCODING_OB_INTEGER_BASE_DIFF_DECODER_H_


#include "ob_icolumn_decoder.h"
#include "ob_encoding_util.h"
#include "ob_integer_base_diff_encoder.h"
#include "ob_bit_stream.h"

namespace oceanbase
{
namespace blocksstable
{

struct ObColumnHeader;
struct ObIntegerBaseDiffHeader;

class ObIntegerBaseDiffDecoder : public ObIColumnDecoder
{
public:
  static const ObColumnHeader::Type type_ = ObColumnHeader::INTEGER_BASE_DIFF;
  ObIntegerBaseDiffDecoder() : header_(NULL), base_(0)
  {}
  virtual ~ObIntegerBaseDiffDecoder() {}

  OB_INLINE int init(
      const ObMicroBlockHeader &micro_block_header,
      const ObColumnHeader &column_header,
      const char *meta);

  virtual int decode(const ObColumnDecoderCtx &ctx, common::ObDatum &datum, const int64_t row_id,
      const ObBitStream &bs, const char *data, const int64_t len) const override;

  virtual int update_pointer(const char *old_block, const char *cur_block) override;

  void reset() { this->~ObIntegerBaseDiffDecoder(); new (this) ObIntegerBaseDiffDecoder(); }
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
      const ObColumnDecoderCtx &ctx,
      const ObIRowIndex* row_index,
      ObVectorDecodeCtx &vector_ctx) const override;
  virtual int pushdown_operator(
      const sql::ObPushdownFilterExecutor *parent,
      const ObColumnDecoderCtx &col_ctx,
      const sql::ObWhiteFilterExecutor &filter,
      const char* meta_data,
      const ObIRowIndex* row_index,
      const sql::PushdownFilterInfo &pd_filter_info,
      ObBitmap &result_bitmap) const override;

  virtual int get_null_count(
      const ObColumnDecoderCtx &ctx,
      const ObIRowIndex *row_index,
      const int32_t *row_ids,
      const int64_t row_cap,
      int64_t &null_count) const override;
private:
  int batch_get_bitpacked_values(
      const ObColumnDecoderCtx &ctx,
      const int32_t *row_ids,
      const int64_t row_cap,
      const int64_t datum_len,
      const int64_t data_offset,
      common::ObDatum *datums) const;

  template <typename T>
  inline int get_delta(const ObObjType &obj_type, const common::ObDatum &datum, uint64_t &delta) const
  {
    UNUSEDx(datum, delta);
    int ret = common::OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "Undefined delta calculation", K(ret));
    return ret;
  }

  int comparison_operator(
      const sql::ObPushdownFilterExecutor *parent,
      const ObColumnDecoderCtx &col_ctx,
      const unsigned char* col_data,
      const sql::ObWhiteFilterExecutor &filter,
      const sql::PushdownFilterInfo &pd_filter_info,
      ObBitmap &result_bitmap) const;

  int bt_operator(
      const sql::ObPushdownFilterExecutor *parent,
      const ObColumnDecoderCtx &col_ctx,
      const unsigned char* col_data,
      const sql::ObWhiteFilterExecutor &filter,
      const sql::PushdownFilterInfo &pd_filter_info,
      ObBitmap &result_bitmap) const;

  int in_operator(
      const sql::ObPushdownFilterExecutor *parent,
      const ObColumnDecoderCtx &col_ctx,
      const unsigned char* col_data,
      const sql::ObWhiteFilterExecutor &filter,
      const sql::PushdownFilterInfo &pd_filter_info,
      ObBitmap &result_bitmap) const;

  int traverse_all_data(
      const sql::ObPushdownFilterExecutor *parent,
      const ObColumnDecoderCtx &col_ctx,
      const unsigned char* col_data,
      const sql::ObWhiteFilterExecutor &filter,
      const sql::PushdownFilterInfo &pd_filter_info,
      ObBitmap &result_bitmap,
      int (*lambda)(
          const ObDatum &cur_datum,
          const sql::ObWhiteFilterExecutor &filter,
          bool &result)) const;

  template<typename VectorType, bool HAS_NULL>
  int inner_decode_vector(const ObColumnDecoderCtx &decoder_ctx, ObVectorDecodeCtx &vector_ctx) const;
private:
  const ObIntegerBaseDiffHeader *header_;
  uint64_t base_;
};

OB_INLINE int ObIntegerBaseDiffDecoder::init(
                                             const ObMicroBlockHeader &micro_block_header,
                                             const ObColumnHeader &column_header, const char *meta)
{
  UNUSED(micro_block_header);
  int ret = common::OB_SUCCESS;
  // performance critical, don't check params
  if (is_inited()) {
    ret = common::OB_INIT_TWICE;
    STORAGE_LOG(WARN, "init twice", K(ret));
  } else {
    const int64_t store_size = get_type_size_map()[column_header.get_store_obj_type()];
    ObObjTypeStoreClass sc = get_store_class_map()[ob_obj_type_class(column_header.get_store_obj_type())];
    if (ObIntSC != sc && ObUIntSC != sc) {
      ret = common::OB_INNER_STAT_ERROR;
      STORAGE_LOG(WARN, "not supported store class", K(ret), K(column_header), K(sc));
    } else {
      meta += column_header.offset_;
      header_ = reinterpret_cast<const ObIntegerBaseDiffHeader *>(meta);
      base_ = 0;
      MEMCPY(&base_, meta + sizeof(*header_), store_size);
      // manual cast to int64_t
      uint64_t mask = ~INTEGER_MASK_TABLE[store_size];
      if (ObIntSC == sc && 0 != mask && (base_ & (mask >> 1))) {
        base_ |= mask;
      }
    }
  }
  return ret;
}

OB_INLINE void ObIntegerBaseDiffDecoder::reuse()
{
  header_ = NULL;
  /*
  obj_meta_.reset();
  micro_block_header_ = NULL;
  col_header_ = NULL;
  header_ = NULL;
  base_ = 0;
  */
}
} // end namespace blocksstable
} // end namespace oceanbase

#endif // OCEANBASE_ENCODING_OB_INTEGER_BASE_DIFF_DECODER_H_
