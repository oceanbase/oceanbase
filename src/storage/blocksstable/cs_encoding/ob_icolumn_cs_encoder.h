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

#ifndef OCEANBASE_ENCODING_OB_ICOLUMN_CS_ENCODER_H_
#define OCEANBASE_ENCODING_OB_ICOLUMN_CS_ENCODER_H_

#include "storage/blocksstable/ob_data_buffer.h"
#include "ob_column_encoding_struct.h"
#include "storage/blocksstable/ob_imicro_block_writer.h"

namespace oceanbase
{
namespace blocksstable
{


class ObIColumnCSEncoder
{
public:
  ObIColumnCSEncoder();
  virtual ~ObIColumnCSEncoder() {}
  ObCSColumnHeader &get_column_header() { return column_header_; }
  virtual int store_column(ObMicroBufferWriter &buf_writer) = 0;
  virtual ObCSColumnHeader::Type get_type() const = 0;
  virtual int init(const ObColumnCSEncodingCtx &ctx, const int64_t column_index, const int64_t row_count);
  virtual void reuse();
  // Used to choose the best column encoding algorithm which has the minimal store size
  virtual int64_t estimate_store_size() const = 0;
  virtual int get_identifier_and_stream_types(
      ObColumnEncodingIdentifier &identifier_, const ObIntegerStream::EncodingType *&types) const = 0;
  //Only for encoding pre-allocated space
  virtual int get_maximal_encoding_store_size(int64_t &size) const = 0;
  virtual int get_string_data_len(uint32_t &len) const = 0;

  int get_stream_offsets(ObIArray<uint32_t> &offsets) const;

  VIRTUAL_TO_STRING_KV(K_(column_index), K_(column_type), K_(store_class),
    KPC_(ctx), K_(row_count), K_(column_header), K_(is_force_raw), K_(stream_offsets));

protected:
  int store_null_bitamp(ObMicroBufferWriter &buf_writer);


protected:
  bool is_inited_;
  common::ObObjMeta column_type_;
  ObObjTypeStoreClass store_class_;
  const ObColumnCSEncodingCtx *ctx_;
  int64_t column_index_;
  int64_t row_count_;
  ObCSColumnHeader column_header_;
  bool is_force_raw_;
  ObSEArray<uint32_t, 4> stream_offsets_;
  ObIntegerStream::EncodingType int_stream_encoding_types_[ObCSColumnHeader::MAX_INT_STREAM_COUNT_OF_COLUMN];
  int32_t int_stream_count_;

};



} // end namespace blocksstable
} // end namespace oceanbase

#endif // OCEANBASE_ENCODING_OB_ICOLUMN_ENCODER_H_
