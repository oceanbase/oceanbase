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
#include "ob_integer_array.h"
#include "ob_encoding_bitset.h"

namespace oceanbase
{
namespace observer
{
namespace table_load_backup
{

class ObColumnHeader;

struct ObInterColSubStrMetaHeader
{
  static const uint64_t START_POS_BYTE_MASK = (0x1UL << 2) - 1;
  static const uint64_t VAL_LEN_BYTE_MASK = (0x1UL << 2) - 1;

  uint16_t start_pos_; // same start pos
  uint16_t length_; // fix length of substr
  uint8_t ref_col_idx_;
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

class ObInterColSubStrDecoder : public ObSpanColumnDecoder
{
public:
  static const ObColumnHeader::Type type_ = ObColumnHeader::COLUMN_SUBSTR;
  ObInterColSubStrDecoder();
  virtual ~ObInterColSubStrDecoder();

  OB_INLINE int init(const common::ObObjMeta &obj_meta,
                  const ObMicroBlockHeaderV2 &micro_block_header,
                  const ObColumnHeader &column_header,
                  const char *meta);
  virtual int decode(ObColumnDecoderCtx &ctx, common::ObObj &cell, const int64_t row_id,
      const ObBitStream &bs, const char *data, const int64_t len) const override;

  virtual int update_pointer(const char *old_block, const char *cur_block) override;

  virtual int get_ref_col_idx(int64_t &ref_col_idx) const override;

  void reset() { this->~ObInterColSubStrDecoder(); new (this) ObInterColSubStrDecoder(); }
  OB_INLINE void reuse();
  virtual ObColumnHeader::Type get_type() const override { return type_; }

  bool is_inited() const { return NULL != meta_header_; }

  virtual bool can_vectorized() const override { return false; }

protected:
  inline bool has_exc(const ObColumnDecoderCtx &ctx) const
  { return ctx.col_header_->length_ > sizeof(ObInterColSubStrMetaHeader); }

private:
  const ObInterColSubStrMetaHeader *meta_header_;
};

OB_INLINE int ObInterColSubStrDecoder::init(const common::ObObjMeta &obj_meta,
    const ObMicroBlockHeaderV2 &micro_block_header,
    const ObColumnHeader &column_header,
    const char *meta)
{
  int ret = common::OB_SUCCESS;
  UNUSEDx(obj_meta, micro_block_header, column_header);
  // performance critical, don't check params, already checked upper layer
  if (OB_UNLIKELY(is_inited())) {
    ret = common::OB_INIT_TWICE;
    STORAGE_LOG(WARN, "init twice", K(ret));
  } else {
    meta += column_header.offset_;
    meta_header_ = reinterpret_cast<const ObInterColSubStrMetaHeader *>(meta);
    STORAGE_LOG(DEBUG, "decoder meta", K(*meta_header_));
  }
  return ret;
}

OB_INLINE void ObInterColSubStrDecoder::reuse()
{
  meta_header_ = NULL;
  /*
  ref_decoder_ = NULL;
  obj_meta_.reset();
  micro_block_header_ = NULL;
  column_header_ = NULL;
  meta_header_ = NULL;
  meta_data_ = NULL;
  //len_ = 0;
  //count_ = 0;
  opt_.reset();
  meta_reader_.reuse();
  */
}

} // table_load_backup
} // namespace observer
} // namespace oceanbase
