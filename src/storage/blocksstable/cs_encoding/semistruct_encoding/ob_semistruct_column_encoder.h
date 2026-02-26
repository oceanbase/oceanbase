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

#ifndef OCEANBASE_ENCODING_OB_SEMISTRUCT_COLUMN_ENCODER_H_
#define OCEANBASE_ENCODING_OB_SEMISTRUCT_COLUMN_ENCODER_H_

#include "storage/blocksstable/cs_encoding/ob_icolumn_cs_encoder.h"

namespace oceanbase
{
namespace blocksstable
{

class ObSemiStructColumnEncodeCtx;
class ObSemiStructEncodeHeader;
class ObSemiStructColumnEncoder : public ObIColumnCSEncoder
{
public:
  static const ObCSColumnHeader::Type type_ = ObCSColumnHeader::SEMISTRUCT;

  ObSemiStructColumnEncoder();
  virtual ~ObSemiStructColumnEncoder();

  ObSemiStructColumnEncoder(const ObSemiStructColumnEncoder&) = delete;
  ObSemiStructColumnEncoder &operator=(const ObSemiStructColumnEncoder&) = delete;

  int init(const ObColumnCSEncodingCtx &ctx, const int64_t column_index, const int64_t row_count) override;
  void reuse() override;
  int store_column(ObMicroBufferWriter &buf_writer) override;
  int64_t estimate_store_size() const override;
  ObCSColumnHeader::Type get_type() const override { return type_; }
  int get_identifier_and_stream_types(
      ObColumnEncodingIdentifier &identifier, const ObIntegerStream::EncodingType *&types) const override;
  int get_maximal_encoding_store_size(int64_t &size) const override;
  int get_string_data_len(uint32_t &len) const override;
  int store_column_meta(ObMicroBufferWriter &buf_writer);

  INHERIT_TO_STRING_KV("ICSColumnEncoder", ObIColumnCSEncoder, KP_(semistruct_ctx));

private:
  int do_init_();
  int64_t base_header_size_() const;
  int reserve_header_(ObMicroBufferWriter &buf_writer);
  int reserve_sub_col_headers_(ObMicroBufferWriter &buf_writer);
  int serialize_sub_schema_(ObMicroBufferWriter &buf_writer);
  int update_previous_info_after_encoding_(const int32_t col_idx, ObIColumnCSEncoder &e);

private:
  ObSemiStructColumnEncodeCtx *semistruct_ctx_;
  ObSemiStructEncodeHeader *semistruct_header_;
  ObCSColumnHeader *sub_col_headers_;

};

}  // end namespace blocksstable
}  // end namespace oceanbase

#endif  // OCEANBASE_ENCODING_OB_SEMISTRUCT_COLUMN_ENCODER_H_
