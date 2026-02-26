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

struct ObStringDiffHeader
{
  struct DiffDesc
  {
    uint8_t diff_:1;
    uint8_t count_:7;
    TO_STRING_KV(K_(count), K_(diff));
  };

  struct LeftToRight
  {
    template <typename L, typename R>
    void operator()(L l, R r, int64_t n)
    {
      MEMCPY(r, l, n);
    }
  };

  struct RightToLeft
  {
    template <typename L, typename R>
    void operator()(L l, R r, int64_t n)
    {
      MEMCPY(l, r, n);
    }
  };

  template <typename T>
  struct LogicTrue
  {
    bool operator()(T t) { return !!t; }
  };

  uint16_t offset_;
  uint16_t length_;
  uint16_t string_size_;
  uint8_t diff_desc_cnt_;
  uint8_t hex_char_array_size_;
  DiffDesc diff_descs_[0];

  char *common_data()
  {
    return reinterpret_cast<char *>(&diff_descs_[diff_desc_cnt_]) + hex_char_array_size_;
  }
  const char *common_data() const
  {
    return reinterpret_cast<const char *>(&diff_descs_[diff_desc_cnt_]) + hex_char_array_size_;
  }
  const unsigned char *hex_char_array() const
  {
    return reinterpret_cast<const unsigned char *>(&diff_descs_[diff_desc_cnt_]);
  }
  unsigned char *hex_char_array()
  {
    return reinterpret_cast<unsigned char *>(&diff_descs_[diff_desc_cnt_]);
  }
  bool is_hex_packing() const
  {
    return hex_char_array_size_ > 0;
  }

  template <typename Copy, typename IsDiff, typename PartStr, typename FullStr>
  OB_INLINE void copy_string(Copy copy, IsDiff is_diff, PartStr p, FullStr f) const;

  template <typename HexStringOperator, typename OperatorFunc, typename FullStr>
  OB_INLINE void copy_hex_string(HexStringOperator &opt, OperatorFunc func,
      FullStr data) const;

  TO_STRING_KV(K_(offset), K_(length), K_(string_size), K_(diff_desc_cnt), K_(hex_char_array_size));
} __attribute__((packed));

template <typename Copy, typename IsDiff, typename PartStr, typename FullStr>
OB_INLINE void ObStringDiffHeader::copy_string(Copy copy, IsDiff is_diff,
    PartStr p, FullStr f) const
{
  int64_t ppos = 0;
  int64_t fpos = 0;
  for (int64_t i = 0; i < diff_desc_cnt_; ++i) {
    if (is_diff(diff_descs_[i].diff_)) {
      copy(p + ppos, f + fpos, diff_descs_[i].count_);
      ppos += diff_descs_[i].count_;
    }
    fpos += diff_descs_[i].count_;
  }
}

template <typename HexStringOperator, typename OperatorFunc, typename FullStr>
OB_INLINE void ObStringDiffHeader::copy_hex_string(HexStringOperator &opt, OperatorFunc func,
    FullStr data) const
{
  int64_t pos = 0;
  for (int64_t i = 0; i < diff_desc_cnt_; ++i) {
    if (0 != diff_descs_[i].diff_) {
      for (int64_t k = 0; k < diff_descs_[i].count_; ++k) {
        (opt.*func)(data[pos++]);
      }
    } else {
      pos += diff_descs_[i].count_;
    }
  }
}

class ObStringDiffDecoder : public ObIColumnDecoder
{
public:
  static const ObColumnHeader::Type type_ = ObColumnHeader::STRING_DIFF;

  ObStringDiffDecoder();
  virtual ~ObStringDiffDecoder();

  OB_INLINE int init(const common::ObObjMeta &obj_metan,
      const ObMicroBlockHeaderV2 &micro_block_header,
      const ObColumnHeader &column_header,
      const char *meta);

  virtual int decode(ObColumnDecoderCtx &ctx, common::ObObj &cell, const int64_t row_id,
      const ObBitStream &bs, const char *data, const int64_t len) const override;

  virtual int update_pointer(const char *old_block, const char *cur_block) override;

  virtual int batch_decode(
      const ObColumnDecoderCtx &ctx,
      const ObIRowIndex* row_index,
      const int64_t *row_ids,
      const char **cell_datas,
      const int64_t row_cap,
      common::ObDatum *datums) const override;

  void reset() { this->~ObStringDiffDecoder(); new (this) ObStringDiffDecoder(); }
  OB_INLINE void reuse();
  virtual ObColumnHeader::Type get_type() const { return type_; }

  bool is_inited() const { return NULL != header_; }
private:
  const ObStringDiffHeader *header_;
};

OB_INLINE int ObStringDiffDecoder::init(const common::ObObjMeta &obj_meta,
    const ObMicroBlockHeaderV2 &micro_block_header,
    const ObColumnHeader &column_header,
    const char *meta)
{
  UNUSEDx(obj_meta, micro_block_header, column_header);
  // performance critical, don't check params, already checked upper layer
  int ret = common::OB_SUCCESS;
  if (is_inited()) {
    ret = common::OB_INIT_TWICE;
    STORAGE_LOG(WARN, "init twice", K(ret));
  } else {
    meta += column_header.offset_;
    header_ = reinterpret_cast<const ObStringDiffHeader *>(meta);
  }
  return ret;
}

OB_INLINE void ObStringDiffDecoder::reuse()
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
