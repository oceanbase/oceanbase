/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#pragma once
#include "ob_icolumn_decoder.h"
#include "ob_encoding_util.h"
#include "storage/blocksstable/ob_data_buffer.h"
#include "ob_bit_stream.h"

namespace oceanbase
{
namespace observer
{
namespace table_load_backup
{
class ObColumnHeader;

struct ObHexStringHeader
{
public:
  uint16_t offset_;
  uint16_t length_;
  uint16_t max_string_size_;
  unsigned char hex_char_array_[0];

  TO_STRING_KV(K_(offset), K_(length));
} __attribute__((packed));

struct ObVarHexCellHeader
{
  uint8_t odd_;
} __attribute__((packed));

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

class ObHexStringDecoder : public ObIColumnDecoder
{
public:
  static const ObColumnHeader::Type type_ = ObColumnHeader::HEX_PACKING;
  ObHexStringDecoder();
  ~ObHexStringDecoder();

  OB_INLINE int init(const common::ObObjMeta &obj_meta,
      const ObMicroBlockHeaderV2 &micro_block_header,
      const ObColumnHeader &column_header,
      const char *meta);

  virtual int decode(ObColumnDecoderCtx &ctx, common::ObObj &cell, const int64_t row_id,
      const ObBitStream &bs, const char *data, const int64_t len) const override;

  virtual int update_pointer(const char *old_block, const char *cur_block) override;

  void reset() { this->~ObHexStringDecoder(); new (this) ObHexStringDecoder(); }
  OB_INLINE void reuse();
  virtual ObColumnHeader::Type get_type() const override { return type_; }

  bool is_inited() const { return NULL != header_; }

  virtual int batch_decode(
      const ObColumnDecoderCtx &ctx,
      const ObIRowIndex* row_index,
      const int64_t *row_ids,
      const char **cell_datas,
      const int64_t row_cap,
      common::ObDatum *datums) const override;

  virtual int pushdown_operator(
      const ObPushdownFilterExecutor *parent,
      const ObColumnDecoderCtx &col_ctx,
      const ObWhiteFilterExecutor &filter_node,
      const char* meta_data,
      const ObIRowIndex* row_index,
      ObBitmap &result_bitmap) const override;
private:
  const ObHexStringHeader *header_;
};

OB_INLINE int ObHexStringDecoder::init(const common::ObObjMeta &obj_meta,
    const ObMicroBlockHeaderV2 &micro_block_header,
    const ObColumnHeader &column_header,
    const char *meta)
{
  // performance critical, don't check params, already checked upper layer
  UNUSEDx(obj_meta, micro_block_header);
  int ret = common::OB_SUCCESS;
  if (is_inited()) {
    ret = common::OB_INIT_TWICE;
    STORAGE_LOG(WARN, "init twice", K(ret));
  } else {

    meta += column_header.offset_;
    header_ = reinterpret_cast<const ObHexStringHeader *>(meta);
  }
  return ret;
}

OB_INLINE void ObHexStringDecoder::reuse()
{
  header_ = NULL;
  /*
  obj_meta_.reset();
  micro_block_header_ = NULL;
  col_header_ = NULL;
  header_ = NULL;
  */
}

} // table_load_backup
} // namespace observer
} // namespace oceanbase
