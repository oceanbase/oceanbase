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

#ifndef OCEANBASE_ENCODING_OB_INT_DICT_COLUMN_ENCODER_H_
#define OCEANBASE_ENCODING_OB_INT_DICT_COLUMN_ENCODER_H_

#include "ob_dict_column_encoder.h"

namespace oceanbase
{
namespace blocksstable
{
class ObIntDictColumnEncoder : public ObDictColumnEncoder
{
public:
  static const ObCSColumnHeader::Type type_ = ObCSColumnHeader::INT_DICT;
  ObIntDictColumnEncoder()
    : ObDictColumnEncoder(),
      integer_dict_enc_ctx_(),
      dict_integer_range_(0),
      precision_width_size_(-1),
      is_monotonic_inc_integer_dict_(false)  { }
  virtual ~ObIntDictColumnEncoder() {}

  int init(
    const ObColumnCSEncodingCtx &ctx, const int64_t column_index, const int64_t row_count) override;
  void reuse() override;
  int store_column(ObMicroBufferWriter &buf_writer) override;
  int64_t estimate_store_size() const override;
  ObCSColumnHeader::Type get_type() const override { return type_; }
  int get_maximal_encoding_store_size(int64_t &size) const override;
  int get_string_data_len(uint32_t &len) const override;

  INHERIT_TO_STRING_KV("ObDictColumnEncoder", ObDictColumnEncoder,
     K_(integer_dict_enc_ctx), K_(dict_integer_range), K_(is_monotonic_inc_integer_dict));

private:
  int build_integer_dict_encoder_ctx_();
  int store_dict_(ObMicroBufferWriter &buf_writer);
  int sort_dict_();
  ObIntegerStreamEncoderCtx integer_dict_enc_ctx_;
  uint64_t dict_integer_range_;
  int64_t  precision_width_size_;
  bool is_monotonic_inc_integer_dict_;


};

}  // end namespace blocksstable
}  // end namespace oceanbase

#endif  // OCEANBASE_ENCODING_OB_INT_DICT_COLUMN_ENCODER_H_
