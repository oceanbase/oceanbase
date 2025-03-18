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

#ifndef OCEANBASE_ENCODING_OB_SEMISTRUCT_COLUMN_DECODER_H_
#define OCEANBASE_ENCODING_OB_SEMISTRUCT_COLUMN_DECODER_H_

#include "storage/blocksstable/cs_encoding/ob_icolumn_cs_decoder.h"

namespace oceanbase
{
namespace blocksstable
{
class ObSemiStructColumnDecoder : public ObIColumnCSDecoder
{
public:
  static const ObCSColumnHeader::Type type_ = ObCSColumnHeader::SEMISTRUCT;
  ObSemiStructColumnDecoder() {}
  virtual ~ObSemiStructColumnDecoder() {}

  ObSemiStructColumnDecoder(const ObSemiStructColumnDecoder&) = delete;
  ObSemiStructColumnDecoder &operator=(const ObSemiStructColumnDecoder&) = delete;

  virtual int decode(const ObColumnCSDecoderCtx &ctx,
    const int32_t row_id, common::ObDatum &datum) const override { return OB_NOT_SUPPORTED; }
  virtual ObCSColumnHeader::Type get_type() const override { return type_; }

};

}  // end namespace blocksstable
}  // end namespace oceanbase

#endif  // OCEANBASE_ENCODING_OB_SEMISTRUCT_COLUMN_DECODER_H_