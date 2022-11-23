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

#ifndef OCEANBASE_ENCODING_OB_CONST_ENCODER_H_
#define OCEANBASE_ENCODING_OB_CONST_ENCODER_H_

#include "ob_icolumn_encoder.h"
#include "ob_encoding_util.h"
#include "ob_dict_encoder.h"

namespace oceanbase
{
namespace blocksstable
{

class ObEncodingHashTable;
struct ObEncodingHashNodeList;

struct ObConstMetaHeader
{
  static constexpr uint8_t OB_CONST_META_HEADER_V1 = 0;
  uint8_t version_;
  // count of the except rows
  uint8_t count_;
  // the offset of dict
  uint8_t const_ref_;
  union {
    struct {
      uint8_t row_id_byte_:3;
    };
    uint8_t attr_;
  };
  uint16_t offset_;
  char payload_[0];

  ObConstMetaHeader() { reset(); }
  void reset() { memset(this, 0, sizeof(*this)); }

  TO_STRING_KV(K_(version), K_(count), K_(const_ref), K_(row_id_byte), K_(attr), K_(offset));
}__attribute__((packed));

class ObConstEncoder : public ObIColumnEncoder
{
public:
  static const int64_t MAX_EXCEPTION_SIZE = 32; // Maybe need tune up for pure columnar store
  static const int64_t MAX_EXCEPTION_PCT = 10;
  static const ObColumnHeader::Type type_ = ObColumnHeader::CONST;

  ObConstEncoder();
  virtual ~ObConstEncoder();

  virtual int init(const ObColumnEncodingCtx &ctx_,
                   const int64_t column_index,
                   const ObConstDatumRowArray &rows) override;
  virtual int store_meta(ObBufferWriter &buf_writer) override;
  virtual int store_data(const int64_t row_id, ObBitStream &bs,
                         char *buf, const int64_t len) override;
  virtual int get_row_checksum(int64_t &checksum) const override;
  virtual int traverse(bool &suitable) override;
  virtual int64_t calc_size() const override;
  virtual ObColumnHeader::Type get_type() const override { return type_; }

  virtual void reuse() override ;
  virtual int store_fix_data(ObBufferWriter &buf_writer) override;
private:
  int store_meta_without_dict(ObBufferWriter &buf_writer);
  int get_cell_len(const common::ObDatum &datum, int64_t &length) const;
  int store_value(const common::ObDatum &datum, char *buf);

private:
  ObObjTypeStoreClass sc_;
  int64_t count_;
  int64_t row_id_byte_;
  ObConstMetaHeader *const_meta_header_;
  ObEncodingHashNode *const_list_header_;
  ObEncodingHashTable *ht_;
  ObDictEncoder dict_encoder_;

  DISALLOW_COPY_AND_ASSIGN(ObConstEncoder);
};

} // end namespace blocksstable
} // end namespace oceanbase

#endif // OCEANBASE_ENCODING_OB_CONST_ENCODER_H_
