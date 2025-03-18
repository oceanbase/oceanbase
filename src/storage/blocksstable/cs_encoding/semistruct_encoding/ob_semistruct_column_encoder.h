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

class ObSemiStructColumnEncoder : public ObIColumnCSEncoder
{
public:
  static const ObCSColumnHeader::Type type_ = ObCSColumnHeader::SEMISTRUCT;

  ObSemiStructColumnEncoder() {}
  virtual ~ObSemiStructColumnEncoder() {}

  ObSemiStructColumnEncoder(const ObSemiStructColumnEncoder&) = delete;
  ObSemiStructColumnEncoder &operator=(const ObSemiStructColumnEncoder&) = delete;

  int init(const ObColumnCSEncodingCtx &ctx, const int64_t column_index, const int64_t row_count) override { return OB_NOT_SUPPORTED; }
  int store_column(ObMicroBufferWriter &buf_writer) override { return OB_NOT_SUPPORTED; }
  int64_t estimate_store_size() const override { return 0; }
  ObCSColumnHeader::Type get_type() const override { return type_; }
  int get_identifier_and_stream_types(
      ObColumnEncodingIdentifier &identifier, const ObIntegerStream::EncodingType *&types) const override { return OB_NOT_SUPPORTED; }
  int get_maximal_encoding_store_size(int64_t &size) const override { return OB_NOT_SUPPORTED; }
  int get_string_data_len(uint32_t &len) const override { return OB_NOT_SUPPORTED; }

};

}  // end namespace blocksstable
}  // end namespace oceanbase

#endif  // OCEANBASE_ENCODING_OB_SEMISTRUCT_COLUMN_ENCODER_H_