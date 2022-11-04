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

#ifndef OCEANBASE_ENCODING_OB_ICOLUMN_ENCODER_H_
#define OCEANBASE_ENCODING_OB_ICOLUMN_ENCODER_H_

#include "lib/container/ob_array.h"
#include "storage/ob_i_store.h"
#include "storage/blocksstable/ob_block_sstable_struct.h"
#include "storage/blocksstable/ob_data_buffer.h"
#include "ob_encoding_util.h"
#include "ob_bit_stream.h"

DEFINE_HAS_MEMBER(INCLUDE_EXT_CELL);

namespace oceanbase
{
namespace blocksstable
{

typedef common::ObArray<ObConstDatumRow> ObConstDatumRowArray;
/*
typedef ObPodFix2dArray<
    storage::ObStoreRow, 64 << 10, common::OB_MALLOC_MIDDLE_BLOCK_SIZE> ObStoreRowArray;
*/

class ObBitStream;

class ObIColumnEncoder
{
public:
  const int64_t DEF_VAR_INDEX_BYTE = 2;

  struct EmptyGetter
  {
    int operator()(const int64_t row_id, const common::ObDatum &datum, uint64_t &v)
    {
      UNUSEDx(row_id, datum, v);
      return common::OB_NOT_SUPPORTED;
    }
  };

  struct EncoderDesc
  {
    bool need_data_store_; // need store data (column store/row store) outside meta
    bool is_var_data_; // is store var length data in row store
    bool has_null_;
    bool has_nope_;
    bool need_extend_value_bit_store_;
    int64_t bit_packing_length_;
    int64_t fix_data_length_;

    // same with set_data_pos(), used in micro block encoder
    int64_t row_offset_;
    int64_t row_length_;

    EncoderDesc() { reuse(); }
    void reuse() { MEMSET(this, 0, sizeof(*this)); }
    TO_STRING_KV(K_(need_data_store), K_(is_var_data),
        K_(has_null), K_(has_nope), K_(need_extend_value_bit_store),
        K_(bit_packing_length), K_(fix_data_length), K_(row_offset), K_(row_length));
  };

  ObIColumnEncoder();
  virtual ~ObIColumnEncoder();

  EncoderDesc &get_desc() { return desc_; }
  ObColumnHeader &get_column_header() { return column_header_; }
  ObObjType get_obj_type() { return column_type_.get_type(); }
  // var data store only
  virtual int set_data_pos(const int64_t offset, const int64_t length)
  {
    UNUSEDx(offset, length);
    return common::OB_NOT_SUPPORTED;
  }

  virtual int get_var_length(const int64_t row_id, int64_t &length)
  {
    UNUSEDx(row_id, length);
    return common::OB_NOT_SUPPORTED;
  }

  virtual int get_row_checksum(int64_t &checksum) const;
  virtual int store_meta(ObBufferWriter &buf_writer) = 0;
  // Store data to row.
  // %buf may be NULL for bit packing column.
  // copy data to %buf for fix/var length column.
  virtual int store_data(
      const int64_t row_id, ObBitStream &bs, char *buf, const int64_t len) = 0;

  virtual int traverse(bool &suitable) = 0;
  virtual int64_t calc_size() const = 0;
  virtual ObColumnHeader::Type get_type() const = 0;

  VIRTUAL_TO_STRING_KV(K_(column_index), K_(column_type), K_(desc),
      K_(column_header), K_(extend_value_bit));

  virtual int init(
      const ObColumnEncodingCtx &ctx,
      const int64_t column_index,
      const ObConstDatumRowArray &rows);

  virtual void reuse();

  void set_extend_value_bit(const int64_t v) { extend_value_bit_ = v; }

  inline int is_valid_fix_encoder() const
  {
    return !desc_.is_var_data_ && desc_.need_data_store_
           && (0 <= desc_.fix_data_length_ || 0 <= desc_.bit_packing_length_)
           && (0 == desc_.fix_data_length_ || 0 == desc_.bit_packing_length_);
  }

  inline void calc_fix_data_size(const int64_t row_cnt,
                                int64_t &bits_size,
                                int64_t &fix_data_size)
  {
    if (desc_.need_extend_value_bit_store_) {
      bits_size += extend_value_bit_ * row_cnt;
    }
    bits_size += desc_.bit_packing_length_ * row_cnt;
    bits_size = (bits_size + CHAR_BIT - 1) / CHAR_BIT;
    fix_data_size = bits_size + desc_.fix_data_length_ * row_cnt;
  }

  virtual int store_fix_data(ObBufferWriter &buf_writer) = 0;

  template <typename Getter>
  int store_fix_bits(
      char *buf,
      const ObColDatums *col_datums,
      const int64_t bits_size,
      Getter &getter);
  template <typename BitPackingValueGetter, typename FixDataSetter>
  int fill_column_store(
      ObBufferWriter &writer,
      const ObColDatums &datums,
      BitPackingValueGetter &getter,
      const FixDataSetter &setter);

protected:
  bool is_inited_;
  common::ObObjMeta column_type_;
  const ObColumnEncodingCtx *ctx_;
  int64_t column_index_;
  int64_t extend_value_bit_;
  const ObConstDatumRowArray *rows_;
  EncoderDesc desc_;
  ObColumnHeader column_header_;
};

class ObSpanColumnEncoder : public ObIColumnEncoder
{
public:
  static const int64_t MAX_EXC_CNT;
  static const int64_t EXC_THRESHOLD_PCT;

  virtual int set_ref_col_idx(const int64_t ref_col_idx,
      const ObColumnEncodingCtx &ref_ctx_) = 0;
  virtual int64_t get_ref_col_idx() const = 0;
};

template <typename Getter>
int ObIColumnEncoder::store_fix_bits(
    char *buf,
    const ObColDatums *col_datums,
    const int64_t bits_size,
    Getter &getter)
{
  int ret = common::OB_SUCCESS;
  if (OB_ISNULL(buf)
      || OB_ISNULL(col_datums)
      || OB_UNLIKELY(0 > bits_size)) {
    ret = common::OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), KP(buf), KP(col_datums), K(bits_size));
  } else if (0 < bits_size) {
    ObBitStream bs;
    int64_t pos = 0;
    const int64_t row_cnt = col_datums->count();

    if (desc_.need_extend_value_bit_store_) {
      // store extend bits
      for (int64_t row_id = 0; OB_SUCC(ret) && row_id < row_cnt; ++row_id)  {
        const common::ObDatum &datum = col_datums->at(row_id);
        const ObStoredExtValue ext_val = get_stored_ext_value(datum);
        ObBitStream::memory_safe_set(reinterpret_cast<unsigned char *>(buf),
            pos, static_cast<uint64_t>(ext_val));
        pos += extend_value_bit_;
      }
    }
    if (OB_SUCC(ret) && (0 < desc_.bit_packing_length_)) {
      for (int64_t row_id = 0; OB_SUCC(ret) && row_id < row_cnt; ++row_id)  {
        const common::ObDatum &datum = col_datums->at(row_id);
        if (HAS_MEMBER(Getter, INCLUDE_EXT_CELL)
            || STORED_NOT_EXT == get_stored_ext_value(datum)) {
          uint64_t v = 0;
          if (OB_FAIL(getter(row_id, datum, v))) {
            STORAGE_LOG(WARN, "get value failed", K(ret), K(datum), K(v));
          } else {
            ObBitStream::memory_safe_set(reinterpret_cast<unsigned char *>(buf),
                pos, desc_.bit_packing_length_, v);
          }
        }
        pos += desc_.bit_packing_length_;
      }
    }
  }
  return ret;
}

template <typename BitPackingValueGetter, typename FixDataSetter>
int ObIColumnEncoder::fill_column_store(
    ObBufferWriter &writer,
    const ObColDatums &datums,
    BitPackingValueGetter &getter,
    const FixDataSetter &setter)
{
  int ret = common::OB_SUCCESS;
  if (!is_valid_fix_encoder()) {
    // do nothing for var store
    ret = common::OB_SUCCESS;
  } else {
    int64_t fix_data_size  = 0;
    int64_t bit_data_size = 0;
    char *buf = writer.current();
    calc_fix_data_size(datums.count(), bit_data_size, fix_data_size);
    // extra 8 bytes for memory safe bit setting
    if (OB_FAIL(writer.advance_zero(fix_data_size + sizeof(uint64_t)))) {
      STORAGE_LOG(WARN, "buffer advance failed", K(ret), K(fix_data_size));
    } else {
      if (bit_data_size > 0) {
        if (OB_FAIL(store_fix_bits(buf, &datums, bit_data_size, getter))) {
          STORAGE_LOG(WARN, "store fix bit data failed", K(ret));
        }
      }
    }

    if (OB_SUCC(ret) && desc_.fix_data_length_ > 0) {
      buf += bit_data_size;
      for (int64_t row_id = 0; OB_SUCC(ret) && row_id < datums.count(); ++row_id) {
        const common::ObDatum &datum = datums.at(row_id);
        if (HAS_MEMBER(FixDataSetter, INCLUDE_EXT_CELL)
            || STORED_NOT_EXT == get_stored_ext_value(datum)) {
          if (OB_FAIL(setter(row_id, datum, buf, desc_.fix_data_length_))) {
            STORAGE_LOG(WARN, "set data failed", K(ret), K(row_id), K(datum));
          }
        }
        buf += desc_.fix_data_length_;
      }
    }

    if (OB_SUCC(ret)) {
      // revert extra bytes
      if (OB_FAIL(writer.backward(sizeof(uint64_t)))) {
        STORAGE_LOG(WARN, "backward buffer failed", K(ret));
      }
    }
  }
  return ret;
}

} // end namespace blocksstable
} // end namespace oceanbase

#endif // OCEANBASE_ENCODING_OB_ICOLUMN_ENCODER_H_
