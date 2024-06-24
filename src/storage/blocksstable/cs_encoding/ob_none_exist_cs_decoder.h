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

#ifndef OCEANBASE_ENCODING_OB_NONE_EXIST_CS_DECODER_H_
#define OCEANBASE_ENCODING_OB_NONE_EXIST_CS_DECODER_H_

#include "ob_icolumn_cs_decoder.h"

namespace oceanbase
{
namespace blocksstable
{
// decoder for column not exist in schema

class ObNoneExistColumnCSDecoder : public ObIColumnCSDecoder
{
public:
  static const ObCSColumnHeader::Type type_ = ObCSColumnHeader::MAX_TYPE;

  virtual int get_null_count(
    const ObColumnCSDecoderCtx &ctx,
    const ObIRowIndex *row_index,
    const int64_t *row_ids,
    const int64_t row_cap,
    int64_t &null_count) const;

  virtual int get_distinct_count(int64_t &distinct_count) const;

  virtual int read_distinct(
    const ObColumnCSDecoderCtx &ctx,
    const char **cell_datas,
    storage::ObGroupByCell &group_by_cell)  const;

  virtual int read_reference(
    const ObColumnCSDecoderCtx &ctx,
    const int64_t *row_ids,
    const int64_t row_cap,
    storage::ObGroupByCell &group_by_cell) const;

  virtual int decode(const ObColumnCSDecoderCtx &ctx, const int32_t row_id, common::ObDatum &datum) const;

  virtual int decode_vector(
    const ObColumnCSDecoderCtx &decoder_ctx,
    ObVectorDecodeCtx &vector_ctx) const;

  virtual int pushdown_operator(
    const sql::ObPushdownFilterExecutor *parent,
    const ObColumnCSDecoderCtx &col_ctx,
    const sql::ObWhiteFilterExecutor &filter,
    const sql::PushdownFilterInfo &pd_filter_info,
    ObBitmap &result_bitmap) const;

  virtual int pushdown_operator(
    const sql::ObPushdownFilterExecutor *parent,
    const ObColumnCSDecoderCtx &col_ctx,
    sql::ObBlackFilterExecutor &filter,
    sql::PushdownFilterInfo &pd_filter_info,
    ObBitmap &result_bitmap,
    bool &filter_applied) const;

  virtual ObCSColumnHeader::Type get_type() const { return type_; }

  virtual int update_pointer(const char *, const char *) { return common::OB_SUCCESS; }

  virtual bool can_vectorized() const override { return true; }
};


}
}

#endif // OCEANBASE_ENCODING_OB_NONE_EXIST_CS_DECODER_H_
