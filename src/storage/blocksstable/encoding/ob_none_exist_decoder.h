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

#ifndef OCEANBASE_ENCODING_OB_NONE_EXIST_DECODER_H_
#define OCEANBASE_ENCODING_OB_NONE_EXIST_DECODER_H_

#include "ob_icolumn_decoder.h"

namespace oceanbase
{
namespace blocksstable
{
// decoder for column not exist in schema
class ObNoneExistColumnDecoder : public ObIColumnDecoder
{
public:
  static const ObColumnHeader::Type type_ = ObColumnHeader::MAX_TYPE;

  virtual int get_null_count(
    const ObColumnDecoderCtx &ctx,
    const ObIRowIndex *row_index,
    const int64_t *row_ids,
    const int64_t row_cap,
    int64_t &null_count) const;

    virtual int get_distinct_count(int64_t &distinct_count) const;

  virtual int read_distinct(
      const ObColumnDecoderCtx &ctx,
      const char **cell_datas,
      storage::ObGroupByCell &group_by_cell)  const;

  virtual int read_reference(
      const ObColumnDecoderCtx &ctx,
      const int64_t *row_ids,
      const int64_t row_cap,
      storage::ObGroupByCell &group_by_cell) const;


  virtual int decode(const ObColumnDecoderCtx &ctx, common::ObDatum &datum, const int64_t row_id,
      const ObBitStream &bs, const char *data, const int64_t len)const override;

  virtual int decode_vector(
    const ObColumnDecoderCtx &decoder_ctx,
    const ObIRowIndex* row_index,
    ObVectorDecodeCtx &vector_ctx) const override;

  virtual int pushdown_operator(
    const sql::ObPushdownFilterExecutor *parent,
    const ObColumnDecoderCtx &col_ctx,
    const sql::ObWhiteFilterExecutor &filter,
    const char* meta_data,
    const ObIRowIndex* row_index,
    const sql::PushdownFilterInfo &pd_filter_info,
    ObBitmap &result_bitmap) const;

  virtual int pushdown_operator(
    const sql::ObPushdownFilterExecutor *parent,
    const ObColumnDecoderCtx &col_ctx,
    sql::ObBlackFilterExecutor &filter,
    const char* meta_data,
    const ObIRowIndex* row_index,
    sql::PushdownFilterInfo &pd_filter_info,
    ObBitmap &result_bitmap,
    bool &filter_applied) const;

  virtual ObColumnHeader::Type get_type() const { return type_; }

  virtual int update_pointer(const char *, const char *) { return common::OB_SUCCESS; }

  virtual bool can_vectorized() const override { return false; }
};


}
}

#endif // OCEANBASE_ENCODING_OB_NONE_EXIST_DECODER_H_
