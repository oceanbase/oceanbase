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

namespace oceanbase
{
namespace observer
{
namespace table_load_backup
{

class ObColumnHeader;

struct ObStringPrefixMetaHeader
{
  static const uint64_t PREFIX_INDEX_BYTE_MASK = (0x1UL << 2) - 1;
  static const uint64_t HEX_CHAR_ARRAY_SIZE_MASK = (0x1UL << 5) - 1;

  uint16_t offset_; // consider as var in row
  uint16_t length_;
  uint16_t max_string_size_;
  uint8_t count_; // the count of prefixes
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

class ObStringPrefixDecoder : public ObIColumnDecoder
{
public:
  static const ObColumnHeader::Type type_ = ObColumnHeader::STRING_PREFIX;
  ObStringPrefixDecoder() : meta_header_(NULL), meta_data_(NULL)
  {}
  virtual ~ObStringPrefixDecoder();

  OB_INLINE int init(const common::ObObjMeta &obj_meta,
           const ObMicroBlockHeaderV2 &micro_block_header,
           const ObColumnHeader &column_header,
           const char *meta);
  virtual int decode(ObColumnDecoderCtx &ctx_, common::ObObj &cell, const int64_t row_id,
      const ObBitStream &bs, const char *data, const int64_t len) const override;

  virtual int update_pointer(const char *old_block, const char *cur_block) override;

  void reset() { this->~ObStringPrefixDecoder(); new (this) ObStringPrefixDecoder(); }
  OB_INLINE void reuse();
  virtual ObColumnHeader::Type get_type() const { return type_; }
  bool is_inited() const { return NULL != meta_header_; }

  virtual int batch_decode(
      const ObColumnDecoderCtx &ctx,
      const ObIRowIndex* row_index,
      const int64_t *row_ids,
      const char **cell_datas,
      const int64_t row_cap,
      common::ObDatum *datums) const override;
  virtual int get_null_count(
      const ObColumnDecoderCtx &ctx,
      const ObIRowIndex *row_index,
      const int64_t *row_ids,
      const int64_t row_cap,
      int64_t &null_count) const override;
private:
  const ObStringPrefixMetaHeader *meta_header_;
  const char *meta_data_;
};

OB_INLINE int ObStringPrefixDecoder::init(const common::ObObjMeta &obj_meta,
    const ObMicroBlockHeaderV2 &micro_block_header,
    const ObColumnHeader &column_header,
    const char *meta)
{
  UNUSEDx(obj_meta, micro_block_header, column_header);
  // performance critical, don't check params, already checked upper layer
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(is_inited())) {
    ret = common::OB_INIT_TWICE;
    STORAGE_LOG(WARN, "init twice", K(ret));
  } else {
    meta += column_header.offset_;
    meta_header_ = reinterpret_cast<const ObStringPrefixMetaHeader *>(meta);
    meta_data_ = meta + sizeof(ObStringPrefixMetaHeader);
    STORAGE_LOG(DEBUG, "debug", K(meta_header_->is_hex_packing()),
        K(meta_header_->hex_char_array_size_));
    meta_data_ += meta_header_->is_hex_packing() ? meta_header_->hex_char_array_size_ : 0;
  }
  return ret;
}

OB_INLINE void ObStringPrefixDecoder::reuse()
{
  meta_header_ = NULL;
  /*
  obj_meta_.reset();
  micro_block_header_ = NULL;
  column_header_ = NULL;
  meta_header_ = NULL;
  meta_data_ = NULL;
  prefix_index_byte_ = 0;
  */
}

} // table_load_backup
} // namespace observer
} // namespace oceanbase
