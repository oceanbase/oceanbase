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

#ifndef OCEANBASE_ENCODING_OB_STRING_COLUMN_ENCODER_H_
#define OCEANBASE_ENCODING_OB_STRING_COLUMN_ENCODER_H_

#include "ob_icolumn_cs_encoder.h"
#include "ob_string_stream_encoder.h"

namespace oceanbase
{
namespace blocksstable
{

class ObStringColumnEncoder : public ObIColumnCSEncoder
{
public:
  static const ObCSColumnHeader::Type type_ = ObCSColumnHeader::STRING;
  ObStringColumnEncoder();
  virtual ~ObStringColumnEncoder();

  ObStringColumnEncoder(const ObStringColumnEncoder&) = delete;
  ObStringColumnEncoder &operator=(const ObStringColumnEncoder&) = delete;

  int init(
    const ObColumnCSEncodingCtx &ctx, const int64_t column_index, const int64_t row_count) override;
  void reuse() override;
  int store_column(ObMicroBufferWriter &buf_writer) override;
  int64_t estimate_store_size() const override;
  ObCSColumnHeader::Type get_type() const override { return type_; }
  int get_identifier_and_stream_types(
      ObColumnEncodingIdentifier &identifier, const ObIntegerStream::EncodingType *&types) const override;
  int get_maximal_encoding_store_size(int64_t &size) const override;
  int get_string_data_len(uint32_t &len) const override;

  INHERIT_TO_STRING_KV("ICSColumnEncoder", ObIColumnCSEncoder, K_(enc_ctx));

private:
  int do_init_();
  int store_column_meta_(ObMicroBufferWriter &buf_writer);

private:
  ObStringStreamEncoderCtx enc_ctx_;
  ObStringStreamEncoder string_stream_encoder_;
  common::ObCompressor *compressor_;
};

}  // end namespace blocksstable
}  // end namespace oceanbase

#endif  // OCEANBASE_ENCODING_OB_INTEGER_COLUMN_ENCODER_H_
