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

#ifndef OCEANBASE_ENCODING_OB_HEX_STRING_ENCODER_H_
#define OCEANBASE_ENCODING_OB_HEX_STRING_ENCODER_H_

#include "ob_icolumn_encoder.h"
#include "ob_encoding_util.h"

namespace oceanbase
{
namespace blocksstable
{

struct ObHexStringMap
{
  uint8_t size_;
  uint8_t map_[1 << CHAR_BIT];

  ObHexStringMap();

  OB_INLINE void mark(const unsigned char c)
  {
    if (can_packing() && 0 == map_[c]) {
      map_[c] = ++size_;
    }
  }
  OB_INLINE uint8_t index(const unsigned char c) const { return map_[c]; }
  bool can_packing() const { return size_ <= (1 << (CHAR_BIT / 2)); }
  void build_index(unsigned char *char_array);
};

class ObHexStringPacker
{
public:
  ObHexStringPacker(const ObHexStringMap &map, unsigned char *data)
      : map_(map), data_(data), pos_(0)
  {
  }

  OB_INLINE void pack(unsigned char c);
  OB_INLINE void pack(const unsigned char *str, const int64_t len);

private:
  const ObHexStringMap &map_;
  unsigned char *data_;
  uint64_t pos_;
};

class ObHexStringUnpacker
{
public:
  ObHexStringUnpacker(const unsigned char *map, const unsigned char *data)
      : map_(map), data_(data), pos_(0)
  {
  }

  OB_INLINE unsigned char unpack();
  OB_INLINE void unpack(unsigned char &c) { c = unpack(); }
  OB_INLINE void unpack(unsigned char *str, const int64_t len);

private:
  const unsigned char *map_;
  const unsigned char *data_;
  uint64_t pos_;
};

OB_INLINE void ObHexStringPacker::pack(unsigned char c)
{
  data_[pos_ / 2] = static_cast<unsigned char>(
      data_[pos_ / 2] | (map_.index(c) << (((pos_  + 1) % 2) * (CHAR_BIT / 2))));
  ++pos_;
}

OB_INLINE void ObHexStringPacker::pack(const unsigned char *str, const int64_t len)
{
  // performance critical, don't check param
  const unsigned char *p = str;
  const unsigned char *end = str + len;
  if (pos_ % 2 != 0 && len > 0) {
    pack(str[0]);
    p++;
  }
  p++;
  unsigned char *d = data_ + pos_ / 2;
  for (; p < end; p += 2) {
    *(d++) = static_cast<unsigned char >(
        map_.index(*(p - 1)) << (CHAR_BIT / 2) | map_.index(*p));
  }
  pos_ = (d - data_) * 2;
  if (p == end) {
    pack(*(p - 1));
  }
}


OB_INLINE unsigned char ObHexStringUnpacker::unpack()
{
  const uint8_t idx = (data_[pos_ / 2] >> ((pos_ + 1) % 2 * (CHAR_BIT / 2))) & 0xF;
  ++pos_;
  return map_[idx];
}

OB_INLINE void ObHexStringUnpacker::unpack(unsigned char *str, const int64_t len)
{
  // performance critical, don't check param
  unsigned char *p = str;
  const unsigned char *end = str + len;
  if (pos_ % 2 != 0 && len > 0) {
    unpack(str[0]);
    p++;
  }
  p++;
  const unsigned char *d = data_ + pos_ / 2;
  for (; p < end; p += 2) {
    *(p - 1) = map_[*d >> (CHAR_BIT / 2)];
    *p = map_[*d & 0xF];
    d++;
  }
  pos_ = (d - data_) * 2;
  if (p == end) {
    unpack(*(p - 1));
  }
}

struct ObHexStringHeader
{
  void reset() { memset(this, 0, sizeof(*this)); }
  static constexpr uint8_t OB_HEX_STRING_HEADER_V1 = 0;
  uint8_t version_;
  uint32_t offset_;
  uint32_t length_;
  uint32_t max_string_size_;
  unsigned char hex_char_array_[0];

  TO_STRING_KV(K_(offset), K_(length), K_(max_string_size));
} __attribute__((packed));

struct ObVarHexCellHeader
{
  uint8_t odd_;
} __attribute__((packed));

class ObHexStringEncoder : public ObIColumnEncoder
{
public:
  static const ObColumnHeader::Type type_ = ObColumnHeader::HEX_PACKING;
  ObHexStringEncoder();
  virtual ~ObHexStringEncoder() {}

  virtual int init(
      const ObColumnEncodingCtx &ctx,
      const int64_t column_index,
      const ObConstDatumRowArray &rows) override;

  virtual int set_data_pos(const int64_t offset, const int64_t length) override;
  virtual int get_var_length(const int64_t row_id, int64_t &length) override;
  virtual int store_meta(ObBufferWriter &buf_writer) override;
  virtual int store_data(
      const int64_t row_id, ObBitStream &bs, char *buf, const int64_t len) override;

  virtual int traverse(bool &suitable) override;
  virtual int64_t calc_size() const override;
  virtual ObColumnHeader::Type get_type() const override { return type_; }

  virtual void reuse() override;
  virtual int store_fix_data(ObBufferWriter &buf_writer) override;

private:
  struct ColumnStoreFiller;

private:
  int64_t min_string_size_;
  int64_t max_string_size_;
  int64_t sum_size_;
  int64_t null_cnt_;
  int64_t nope_cnt_;
  ObHexStringHeader *header_;
  ObHexStringMap hex_string_map_;
};

} // end namespace blocksstable
} // end namespace oceanbase
#endif // OCEANBASE_ENCODING_OB_HEX_STRING_ENCODER_H_
