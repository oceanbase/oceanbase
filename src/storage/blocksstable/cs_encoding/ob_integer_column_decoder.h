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

#ifndef OCEANBASE_ENCODING_OB_INTEGER_COLUMN_DECODER_H_
#define OCEANBASE_ENCODING_OB_INTEGER_COLUMN_DECODER_H_

#include "ob_icolumn_cs_decoder.h"

namespace oceanbase
{
namespace blocksstable
{

class ObIntegerColumnDecoder : public ObIColumnCSDecoder
{
public:
  static const ObCSColumnHeader::Type type_ = ObCSColumnHeader::INTEGER;
  ObIntegerColumnDecoder() {}
  virtual ~ObIntegerColumnDecoder() {}
  ObIntegerColumnDecoder(const ObIntegerColumnDecoder&) = delete;
  ObIntegerColumnDecoder &operator=(const ObIntegerColumnDecoder&) = delete;

  virtual int decode(const ObColumnCSDecoderCtx &ctx,
      const int32_t row_id, common::ObDatum &datum) const override;
  virtual int batch_decode(const ObColumnCSDecoderCtx &ctx, const int32_t *row_ids,
      const int64_t row_cap, common::ObDatum *datums) const override;
  virtual int decode_vector(const ObColumnCSDecoderCtx &ctx, ObVectorDecodeCtx &vector_ctx) const override;

  virtual int get_null_count(const ObColumnCSDecoderCtx &ctx,
     const int32_t *row_ids, const int64_t row_cap, int64_t &null_count) const override;

  virtual ObCSColumnHeader::Type get_type() const override { return type_; }

  virtual int pushdown_operator(
      const sql::ObPushdownFilterExecutor *parent,
      const ObColumnCSDecoderCtx &col_ctx,
      const sql::ObWhiteFilterExecutor &filter,
      const sql::PushdownFilterInfo &pd_filter_info,
      common::ObBitmap &result_bitmap) const override;

  virtual int get_aggregate_result(
      const ObColumnCSDecoderCtx &ctx,
      const int32_t *row_ids,
      const int64_t row_cap,
      storage::ObAggCell &agg_cell) const override;

private:
  static int nu_nn_operator(const ObIntegerColumnDecoderCtx &ctx,
                            const sql::ObPushdownFilterExecutor *parent,
                            const sql::ObWhiteFilterExecutor &filter,
                            const sql::PushdownFilterInfo &pd_filter_info,
                            common::ObBitmap &result_bitmap);

  static int comparison_operator(const ObIntegerColumnDecoderCtx &ctx,
                                 const sql::ObPushdownFilterExecutor *parent,
                                 const sql::ObWhiteFilterExecutor &filter,
                                 const sql::PushdownFilterInfo &pd_filter_info,
                                 common::ObBitmap &result_bitmap);

  static int tranverse_integer_comparison_op(const ObIntegerColumnDecoderCtx &ctx,
                                             const sql::ObWhiteFilterOperatorType &ori_op_type,
                                             const bool is_col_signed,
                                             const uint64_t filter_val,
                                             const int64_t filter_val_size,
                                             const bool is_filter_signed,
                                             const int64_t row_start,
                                             const int64_t row_cnt,
                                             const sql::ObPushdownFilterExecutor *parent,
                                             common::ObBitmap &result_bitmap);

  static int between_operator(const ObIntegerColumnDecoderCtx &ctx,
                              const sql::ObPushdownFilterExecutor *parent,
                              const sql::ObWhiteFilterExecutor &filter,
                              const sql::PushdownFilterInfo &pd_filter_info,
                              common::ObBitmap &result_bitmap);
  static int tranverse_integer_between_op(const ObIntegerColumnDecoderCtx &ctx,
                                          const sql::ObPushdownFilterExecutor *parent,
                                          const sql::ObWhiteFilterExecutor &filter,
                                          const sql::PushdownFilterInfo &pd_filter_info,
                                          ObBitmap &result_bitmap);

  static int in_operator(const ObIntegerColumnDecoderCtx &ctx,
                         const sql::ObPushdownFilterExecutor *parent,
                         const sql::ObWhiteFilterExecutor &filter,
                         const sql::PushdownFilterInfo &pd_filter_info,
                         common::ObBitmap &result_bitmap);

  static int tranverse_integer_in_op(const ObIntegerColumnDecoderCtx &ctx,
                                     bool *filter_vals_valid,
                                     uint64_t *filter_vals,
                                     const sql::ObPushdownFilterExecutor *parent,
                                     const sql::ObWhiteFilterExecutor &filter,
                                     const sql::PushdownFilterInfo &pd_filter_info,
                                     common::ObBitmap &result_bitmap);

  template<typename Operator>
  static int tranverse_datum_all_op(const ObIntegerColumnDecoderCtx &ctx,
                                    const sql::PushdownFilterInfo &pd_filter_info,
                                    common::ObBitmap &result_bitmap,
                                    Operator const &eval);

  static int traverse_integer_in_agg(const ObIntegerColumnDecoderCtx &ctx,
                                     const bool is_col_signed,
                                     const int64_t row_start,
                                     const int64_t row_count,
                                     storage::ObAggCell &agg_cell);

};

}  // end namespace blocksstable
}  // end namespace oceanbase

#endif  // OCEANBASE_ENCODING_OB_INTEGER_COLUMN_DECODER_H_
