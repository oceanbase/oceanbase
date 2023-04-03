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

#ifndef OCEANBASE_ENCODING_OB_RLE_ENCODER_H_
#define OCEANBASE_ENCODING_OB_RLE_ENCODER_H_

#include "ob_icolumn_encoder.h"
#include "ob_encoding_util.h"
#include "ob_dict_encoder.h"

namespace oceanbase
{
namespace blocksstable
{

class ObEncodingHashTable;

struct ObRLEMetaHeader
{
  static constexpr uint8_t OB_RLE_META_HEADER_V1 = 0;
  uint8_t version_;
  union {
    struct {
      uint8_t row_id_byte_:3;
      uint8_t ref_byte_:3;
      uint8_t reserved_:2;
    };
    uint8_t attr_;
  };
  uint32_t count_;
  // the offset of dict
  uint32_t offset_;
  char payload_[0];

  ObRLEMetaHeader() { reset(); }
  void reset() { memset(this, 0, sizeof(*this)); }

  TO_STRING_KV(K_(version), K_(row_id_byte), K_(ref_byte), K_(reserved), K_(count), K_(offset));
}__attribute__((packed));

class ObRLEEncoder : public ObIColumnEncoder
{
public:
  static constexpr int64_t MAX_STORED_BYTES_PER_ROW = 4 + 4; // row id and dict ref
  static constexpr int64_t MAX_DICT_OFFSET = UINT32_MAX;
  static constexpr int64_t MAX_DICT_COUNT =
      (MAX_DICT_OFFSET - sizeof(ObRLEMetaHeader)) / MAX_STORED_BYTES_PER_ROW;
  static const ObColumnHeader::Type type_ = ObColumnHeader::RLE;

  ObRLEEncoder();
  virtual ~ObRLEEncoder();

  virtual int init(
      const ObColumnEncodingCtx &ctx_,
      const int64_t column_index,
      const ObConstDatumRowArray &rows) override;
  virtual void reuse() override;

  virtual int store_meta(ObBufferWriter &buf_writer) override;
  virtual int store_data(const int64_t row_id, ObBitStream &bs,
                         char *buf, const int64_t len) override;
  virtual int get_row_checksum(int64_t &checksum) const override;
  virtual int traverse(bool &suitable) override;
  virtual int64_t calc_size() const override;
  virtual ObColumnHeader::Type get_type() const override { return type_; }
  virtual int store_fix_data(ObBufferWriter &buf_writer) override;
private:
  int64_t count_;
  int64_t row_id_byte_;
  int64_t ref_byte_;
  ObRLEMetaHeader *rle_meta_header_;
  ObEncodingHashTable *ht_;
  ObDictEncoder dict_encoder_;

  DISALLOW_COPY_AND_ASSIGN(ObRLEEncoder);
};

} // end namespace blocksstable
} // end namespace oceanbase

#endif // OCEANBASE_ENCODING_OB_RLE_ENCODER_H_
