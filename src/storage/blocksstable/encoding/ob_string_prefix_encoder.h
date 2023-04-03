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

#ifndef OCEANBASE_ENCODING_OB_STRING_PREFIX_ENCODER_H_
#define OCEANBASE_ENCODING_OB_STRING_PREFIX_ENCODER_H_

#include "lib/allocator/ob_allocator.h"
#include "ob_encoding_hash_util.h"
#include "ob_icolumn_encoder.h"
#include "common/object/ob_object.h"
#include "ob_hex_string_encoder.h"

namespace oceanbase
{
namespace blocksstable
{

class ObMultiPrefixTree;

struct ObStringPrefixCellHeader
{
  static const uint64_t REF_ODD_MASK = (0x1UL << 4) - 1;
  // assume prefix count is less than 16
  union
  {
    uint8_t ref_odd_;
    struct
    {
      uint8_t ref_ : 4;
      uint8_t odd_ : 4;
    };
  };
  // length of common str with prefix
  uint16_t len_;

  inline void set_odd(const int64_t odd)
  {
    odd_ = odd & REF_ODD_MASK;
  }

  inline void set_ref(const int64_t ref)
  {
    ref_ = ref & REF_ODD_MASK;
  }

  inline int64_t get_ref() const
  {
    return ref_;
  }

  inline int64_t get_odd() const
  {
    return odd_;
  }


  void reset() { MEMSET(this, 0, sizeof(*this)); }

  TO_STRING_KV(K_(ref), K_(odd), K_(len));

}__attribute__((packed));

struct ObStringPrefixMetaHeader
{
  static constexpr uint8_t OB_STRING_PREFIX_META_HEADER_V1 = 0;
  static const uint64_t PREFIX_INDEX_BYTE_MASK = (0x1UL << 2) - 1;
  static const uint64_t HEX_CHAR_ARRAY_SIZE_MASK = (0x1UL << 5) - 1;

  uint8_t version_;
  uint8_t count_; // the count of prefixes
  uint32_t offset_; // consider as var in row
  uint32_t length_;
  uint32_t max_string_size_;
  union
  {
    uint8_t size_byte_;
    struct
    {
      uint8_t prefix_index_byte_ : 2;
      uint8_t hex_char_array_size_ : 5;
    };
  };
  unsigned char hex_char_array_[0];

  ObStringPrefixMetaHeader() { reset(); }
  inline void reset() { memset(this, 0, sizeof(*this)); }
  bool is_hex_packing() const { return 0 < hex_char_array_size_; }

  inline void set_prefix_index_byte(const int64_t prefix_index_byte)
  {
    prefix_index_byte_ = prefix_index_byte & PREFIX_INDEX_BYTE_MASK;
  }

  inline void set_hex_char_array_size(const int64_t hex_char_array_size)
  {
    hex_char_array_size_ = hex_char_array_size & HEX_CHAR_ARRAY_SIZE_MASK;
  }

  TO_STRING_KV(K_(count), K_(prefix_index_byte),
      K_(offset), K_(length), K_(max_string_size), K_(hex_char_array_size));

}__attribute__((packed));

class ObStringPrefixEncoder : public ObIColumnEncoder
{
public:
  static const ObColumnHeader::Type type_ = ObColumnHeader::STRING_PREFIX;
  ObStringPrefixEncoder();
  virtual ~ObStringPrefixEncoder();

  virtual int init(
      const ObColumnEncodingCtx &ctx,
      const int64_t column_index,
      const ObConstDatumRowArray &rows) override;
  virtual void reuse() override;
  virtual int set_data_pos(const int64_t offset, const int64_t length) override;
  virtual int get_var_length(const int64_t row_id, int64_t &length) override;
  virtual int store_meta(ObBufferWriter &buf_writer) override;
  virtual int store_data(const int64_t row_id, ObBitStream &bs,
      char *buf, const int64_t len) override;
  virtual int traverse(bool &suitable) override;
  virtual int64_t calc_size() const override;
  virtual ObColumnHeader::Type get_type() const override { return type_; }
  virtual int store_fix_data(ObBufferWriter &buf_writer) override;
private:
  ObStringPrefixMetaHeader *meta_header_;
  int64_t prefix_count_;
  int64_t prefix_index_byte_;
  int64_t prefix_length_;
  ObMultiPrefixTree *prefix_tree_;
  ObHexStringMap hex_string_map_;
  mutable int64_t calc_size_;
  const ObColumnEncodingCtx *col_ctx_;

  DISALLOW_COPY_AND_ASSIGN(ObStringPrefixEncoder);
};


} // end namespace blocksstable
} // end namespace oceanbase

#endif // OCEANBASE_ENCODING_OB_STRING_PREFIX_ENCODER_H_
