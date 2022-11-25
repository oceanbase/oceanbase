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

#ifndef OCEANBASE_ENCODING_OB_STRING_DIFF_ENCODER_H_
#define OCEANBASE_ENCODING_OB_STRING_DIFF_ENCODER_H_

#include "lib/container/ob_se_array.h"
#include "ob_icolumn_encoder.h"
#include "ob_hex_string_encoder.h"
#include "ob_encoding_util.h"
#include "lib/allocator/page_arena.h"

namespace oceanbase
{
namespace blocksstable
{

struct ObStringDiffHeader
{
  // encoding not supported for string larger than 65535
  static constexpr uint8_t OB_STRING_DIFF_HEADER_V1 = 0;
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
  uint8_t version_;
  uint8_t hex_char_array_size_;
  uint16_t string_size_;
  uint32_t offset_;
  uint32_t length_; // maybe 2 bytes is enough
  uint8_t diff_desc_cnt_;
  DiffDesc diff_descs_[0];
  ObStringDiffHeader() { reset(); }
  void reset()
  {
    memset(this, 0, sizeof(*this));
  }
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

  TO_STRING_KV(K_(version), K_(hex_char_array_size), K_(string_size),
      K_(offset), K_(length), K_(diff_desc_cnt));
} __attribute__((packed));

// fix length string diff encoder
// FIXME bin.lb: support exceptions?
class ObStringDiffEncoder : public ObIColumnEncoder
{
public:
  static const ObColumnHeader::Type type_ = ObColumnHeader::STRING_DIFF;
  const static int64_t DEFAULT_DIFF_DESC_CNT= 8;

  ObStringDiffEncoder();
  virtual ~ObStringDiffEncoder() {}

  virtual int init(
      const ObColumnEncodingCtx &ctx,
      const int64_t column_index,
      const ObConstDatumRowArray &rows) override;
  virtual void reuse() override;

  virtual int set_data_pos(const int64_t offset, const int64_t length) override;
  virtual int get_var_length(const int64_t row_id, int64_t &length) override;
  virtual int store_meta(ObBufferWriter &buf_writer) override;
  virtual int store_data(
      const int64_t row_id, ObBitStream &bs, char *buf, const int64_t len) override;

  virtual int traverse(bool &suitable) override;
  virtual int64_t calc_size() const override;
  virtual ObColumnHeader::Type get_type() const override { return type_; }
  virtual int store_fix_data(ObBufferWriter &buf_writer) override;
private:
  int traverse_cell(
      char *&data,
      bool *&diff,
      bool &suitable,
      const common::ObDatum &datum,
      const int64_t row_id);

  struct ColumnStoreFiller;

private:
  int64_t string_size_;
  int64_t common_size_;
  int64_t row_store_size_;
  int64_t null_cnt_;
  int64_t nope_cnt_;
  const char *first_string_; // first cell data
  ObStringDiffHeader *header_;
  common::ObSEArray<ObStringDiffHeader::DiffDesc, DEFAULT_DIFF_DESC_CNT> diff_descs_;
  ObHexStringMap hex_string_map_;
  int64_t last_change_diff_row_id_;
  common::ObArenaAllocator allocator_;
};

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

} // end namespace blocksstable
} // end namespace oceanbase

#endif // OCEANBASE_ENCODING_OB_STRING_DIFF_ENCODER_H_
