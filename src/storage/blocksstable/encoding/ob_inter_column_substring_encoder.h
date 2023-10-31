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

#ifndef OCEANBASE_ENCODING_OB_INTER_COLUMN_SUBSTR_ENCODER_H_
#define OCEANBASE_ENCODING_OB_INTER_COLUMN_SUBSTR_ENCODER_H_

#include "lib/allocator/ob_allocator.h"
#include "ob_encoding_hash_util.h"
#include "common/object/ob_object.h"
#include "ob_encoding_bitset.h"

namespace oceanbase
{
namespace blocksstable
{

struct ObInterColSubStrMetaHeader
{
  static constexpr uint8_t OB_INTER_COL_SUB_STR_META_HEADER_V1 = 0;
  static const uint64_t START_POS_BYTE_MASK = (0x1UL << 2) - 1;
  static const uint64_t VAL_LEN_BYTE_MASK = (0x1UL << 2) - 1;

  uint8_t version_;
  union
  {
    uint8_t attr_;
    struct
    {
      uint8_t start_pos_byte_     : 2;
      uint8_t val_len_byte_       : 2;
      uint8_t is_same_start_pos_  : 1;
      uint8_t is_fix_length_      : 1;
    };
  };
  uint16_t start_pos_; // same start pos
  uint16_t length_; // fix length of substr
  uint16_t ref_col_idx_;
  char payload_[0];

  ObInterColSubStrMetaHeader() { reset(); }
  void reset() { MEMSET(this, 0, sizeof(*this)); }

  inline void set_same_start_pos() { is_same_start_pos_ = 1; }
  inline void set_fix_length() { is_fix_length_ = 1; }
  inline bool is_same_start_pos() const { return is_same_start_pos_; }
  inline bool is_fix_length() const { return is_fix_length_; }
  inline void set_start_pos_byte(const int64_t start_pos_byte) { start_pos_byte_ = start_pos_byte & START_POS_BYTE_MASK; }
  inline void set_val_len_byte(const int64_t val_len_byte) { val_len_byte_ = val_len_byte & VAL_LEN_BYTE_MASK; }

  TO_STRING_KV(K_(start_pos), K_(length), K_(ref_col_idx), K_(start_pos_byte),
      K_(val_len_byte), K_(is_same_start_pos), K_(is_fix_length));
}__attribute__((packed));

class ObInterColSubStrEncoder : public ObSpanColumnEncoder
{
public:
  static const ObColumnHeader::Type type_ = ObColumnHeader::COLUMN_SUBSTR;
  static const int64_t EXCEPTION_START_POS = -2;
  static const int64_t EXT_START_POS = -1;

  ObInterColSubStrEncoder();
  virtual ~ObInterColSubStrEncoder();

  virtual int init(
      const ObColumnEncodingCtx &ctx,
      const int64_t column_index,
      const ObConstDatumRowArray &rows) override;
  virtual int get_encoding_store_meta_need_space(int64_t &need_size) const override;
  virtual int store_meta(ObBufferWriter &buf_writer) override;
  virtual int store_data(const int64_t row_id, ObBitStream &bs,
      char *buf, const int64_t len) override
  {
    UNUSEDx(row_id, bs, buf, len);
    return common::OB_NOT_SUPPORTED;
  }

  virtual int get_row_checksum(int64_t &checksum) const override;
  virtual int traverse(bool &suitable) override;
  virtual int64_t calc_size() const override;
  virtual ObColumnHeader::Type get_type() const override { return type_; }
  virtual int set_ref_col_idx(const int64_t ref_col_idx,
      const ObColumnEncodingCtx &ref_ctx) override;
  virtual int64_t get_ref_col_idx() const override;
  virtual void reuse() override;
  virtual int store_fix_data(ObBufferWriter &buf_writer) override;

private:
  // return:
  //   EXCEPTION_START_POS for exception
  //   EXT_START_POS for same extend type
  //   non-negative integer for a substring
  int64_t is_substring(const common::ObDatum &cell, const common::ObDatum &refed_cell);
  struct ColumnDataSetter;

private:
  common::ObArray<int64_t> start_pos_array_;
  common::ObArray<int64_t> exc_row_ids_;
  int64_t ref_col_idx_;
  const ObColumnEncodingCtx *ref_ctx_;
  int64_t same_start_pos_;
  int64_t fix_data_size_; // only for un-exception data
  int64_t start_pos_byte_;
  int64_t val_len_byte_;
  ObStringBitMapMetaWriter meta_writer_;

  DISALLOW_COPY_AND_ASSIGN(ObInterColSubStrEncoder);
};

} // end namespace blocksstable
} // end namespace oceanbase

#endif // OCEANBASE_ENCODING_OB_INTER_COLUMN_SUBSTR_ENCODER_H_
