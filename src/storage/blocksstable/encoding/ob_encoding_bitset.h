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

#ifndef OCEANBASE_ENCODING_OB_ENCODING_BITSET_H_
#define OCEANBASE_ENCODING_OB_ENCODING_BITSET_H_

#include "share/ob_define.h"
#include <limits.h>
#include "ob_bit_stream.h"
#include "common/object/ob_object.h"
#include "ob_icolumn_encoder.h"
#include "ob_integer_array.h"
#include "ob_encoding_util.h"
#include "src/share/vector/ob_fixed_length_vector.h"

namespace oceanbase
{
namespace common
{
class ObOTimestampData;
}
namespace blocksstable
{
class BitSet
{
public:
  static const int64_t BYTE_PER_WORD = sizeof(uint64_t);
  static const int64_t BITS_PER_WORD = BYTE_PER_WORD * CHAR_BIT;

  BitSet() : words_(NULL), words_num_(0) {}

  inline int init(uint64_t *words, const int64_t bits_num)
  {
    int ret = common::OB_SUCCESS;
    if (NULL == words || 0 > bits_num) {
      ret = common::OB_INVALID_ARGUMENT;
      STORAGE_LOG(WARN, "invalid argument", K(ret), KP(words), K(bits_num));
    } else {
      words_ = words;
      words_num_ = get_words_num(bits_num);
    }
    return ret;
  }

  inline bool is_init() const { return (NULL != words_); }
  inline void reset() { MEMSET(words_, 0, words_num_ * BYTE_PER_WORD); }
  inline int64_t size() const { return words_num_; }
  inline void set(const int64_t pos) { unchecked_set(pos); }
  inline bool get(const int64_t pos) const { return unchecked_get(pos); }
  inline int64_t get_ref(const int64_t pos) const { return count_before(pos); }
  inline int64_t count() const { return count_before(BITS_PER_WORD * words_num_ - 1); }

  // performance critical, do not check parameters
  inline static bool get(const uint64_t *words, const int64_t pos)
  {
    return (words[which_word(pos)] & mark_bit(pos)) != static_cast<uint64_t>(0);
  }

  // performance critical, do not check parameters
  inline static int64_t get_ref(const uint64_t *words, const int64_t pos)
  {
    return get(words, pos) ? count_before(words, pos) : -1;
  }

  inline static int64_t get_words_num(const int64_t nb)
  {
    return (nb < 1) ? 0 : (nb + BITS_PER_WORD - 1) / BITS_PER_WORD;
  }
  void update_pointer(const int64_t offset)
  {
    if (NULL != words_) {
      words_ = reinterpret_cast<uint64_t *>(reinterpret_cast<char *>(words_) + offset);
    }
  }

private:
  inline static int64_t which_word(const int64_t pos)
  {
    return pos / BITS_PER_WORD;
  }

  inline static int64_t which_bit(const int64_t pos)
  {
    return pos % BITS_PER_WORD;
  }

  inline static uint64_t mark_bit(const int64_t pos)
  {
    return (static_cast<uint64_t>(1)) << which_bit(pos);
  }

  inline uint64_t &get_word(const int64_t pos) const
  {
    return words_[which_word(pos)];
  }

  inline void unchecked_set(const int64_t pos)
  {
    get_word(pos) |= mark_bit(pos);
  }

  inline bool unchecked_get(const int64_t pos) const
  {
    return ((get_word(pos) & mark_bit(pos)) != static_cast<uint64_t>(0));
  }

  inline int64_t count_before(const int64_t pos) const
  {
    int64_t ret = 0;
    if (BITS_PER_WORD > pos) {
      ret = popcnt64(words_[0], pos);
    } else {
      int64_t wc_before = which_word(pos);
      for (int64_t i = 0; i < wc_before; ++i) {
        ret += __builtin_popcountl(words_[i]);
      }
      ret += popcnt64(words_[wc_before], pos - wc_before * BITS_PER_WORD);
    }
    return ret;
  }

  inline static int64_t count_before(const uint64_t *words, const int64_t pos)
  {
    int64_t ret = 0;
    if (BITS_PER_WORD > pos) {
      ret = popcnt64(words[0], pos);
    } else {
      int64_t wc_before = which_word(pos);
      for (int64_t i = 0; i < wc_before; ++i) {
        ret += __builtin_popcountl(words[i]);
      }
      ret += popcnt64(words[wc_before], pos - wc_before * BITS_PER_WORD);
    }
    return ret;
  }

  inline static int64_t popcnt64(const uint64_t word, const int64_t pos)
  {
    return (0 == pos) ? 0 : __builtin_popcountl(word << (BITS_PER_WORD - pos));
  }

private:
  uint64_t *words_;
  int64_t words_num_;

  DISALLOW_COPY_AND_ASSIGN(BitSet);
};

static const int64_t EXT_VALUE_BITS = 2;
OB_INLINE static int64_t get_ext_size(const int64_t count)
{
  return (count * EXT_VALUE_BITS + CHAR_BIT - 1) / CHAR_BIT;
}

struct ObBitMapMetaHeader
{
  uint8_t ext_offset_;
  uint8_t index_offset_;
  uint8_t data_offset_; // exc data
  union
  {
    uint8_t bit_packing_len_;
    uint8_t fix_data_cnt_; // fix data cnt
    uint8_t index_byte_; // var data index byte
  };

  inline int64_t get_var_cnt() const
  {
    return (data_offset_ - index_offset_) / index_byte_ + 1;
  }

  inline bool has_ext_val() const
  {
    return (index_offset_ - ext_offset_) > 0;
  }

  inline bool is_var_exc() const
  {
    return (data_offset_ - index_offset_) > 0;
  }

  inline int64_t get_fix_data_size(const int64_t len) const
  {
    return len / fix_data_cnt_;
  }

  TO_STRING_KV(K_(ext_offset), K_(index_offset), K_(data_offset),
      K_(bit_packing_len), K_(fix_data_cnt), K_(index_byte));

}__attribute__((packed));

class ObBitMapMetaBaseWriter
{
public:
  ObBitMapMetaBaseWriter() { reset(); }

  int init(
      const common::ObIArray<int64_t> *exc_row_ids,
      const ObColDatums *col_datums,
      const common::ObObjMeta type);
  void reset() { MEMSET(this, 0, sizeof(*this)); }
  int64_t size() const;
  inline bool is_bit_packing() const { return bit_packing_; }

protected:
  common::ObObjMeta type_;
  const ObColDatums *col_datums_;
  const common::ObIArray<int64_t> *exc_row_ids_;
  BitSet bitset_;
  ObBitStream ext_bs_;
  ObIntegerArrayGenerator index_gen_;
  ObBitStream packing_bs_;
  ObBitMapMetaHeader meta_;
  int64_t exc_total_size_;
  int64_t exc_fix_size_;
  int64_t index_byte_;
  uint64_t max_integer_;
  bool bit_packing_;
  bool has_ext_val_;  // whether has extend value in exception
  bool is_inited_;
  bool var_store_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObBitMapMetaBaseWriter);
};

template <ObObjTypeStoreClass StoreClass>
class ObBitMapMetaWriter : public ObBitMapMetaBaseWriter
{
public:
  ObBitMapMetaWriter() {}
  int traverse_exc(bool &suitable);
  int write(char *buf);

private:
  OB_INLINE void process_cell(const common::ObDatum &datum);
  OB_INLINE void fill_meta_header(bool &suitable);
  OB_INLINE void fill_param();
  int write_bit_packing_data(char *buf);
  int write_fix_data(char *buf);
  int write_var_data(char *buf);
  OB_INLINE static void write_cell(
      char *buf,
      int64_t &offset,
      const common::ObDatum &datum,
      const int64_t len);

private:
  DISALLOW_COPY_AND_ASSIGN(ObBitMapMetaWriter);
};

template <ObObjTypeStoreClass StoreClass>
int ObBitMapMetaWriter<StoreClass>::traverse_exc(bool &suitable)
{
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = common::OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
  } else {
    exc_total_size_ = 0;
    var_store_ = false;
    exc_fix_size_ = -1;
    max_integer_ = 0;

    for (int64_t i = 0; i < exc_row_ids_->count(); ++i) {
      const int64_t row_id = exc_row_ids_->at(i);
      const common::ObDatum &datum = col_datums_->at(row_id);
      if (datum.is_null() || datum.is_nop()) {
        has_ext_val_ = true;
      } else {
        process_cell(datum);
      }
    }

    fill_param();
    fill_meta_header(suitable);
  }
  return ret;
}

template <ObObjTypeStoreClass StoreClass>
OB_INLINE void ObBitMapMetaWriter<StoreClass>::fill_param()
{
  if (0 > exc_fix_size_) {
    if (exc_total_size_ <= UINT8_MAX) {
      index_byte_ = 1;
    } else if (exc_total_size_ <= UINT16_MAX) {
      index_byte_ = 2;
    } else if (exc_total_size_ <= UINT32_MAX) {
      index_byte_ = 4;
    } else {
      index_byte_ = 8;
    }
  } else {
    exc_total_size_ = exc_fix_size_ * exc_row_ids_->count();
  }
}

template <>
OB_INLINE void ObBitMapMetaWriter<ObIntSC>::fill_param()
{
  exc_fix_size_ = get_packing_size(bit_packing_, max_integer_);
  exc_total_size_ = exc_fix_size_ * exc_row_ids_->count();
  exc_total_size_ = bit_packing_ ? (exc_total_size_ + CHAR_BIT - 1) / CHAR_BIT : exc_total_size_;
}

template <>
OB_INLINE void ObBitMapMetaWriter<ObUIntSC>::fill_param()
{
  exc_fix_size_ = get_packing_size(bit_packing_, max_integer_);
  exc_total_size_ = exc_fix_size_ * exc_row_ids_->count();
  exc_total_size_ = bit_packing_ ? (exc_total_size_ + CHAR_BIT - 1) / CHAR_BIT : exc_total_size_;
}

template <ObObjTypeStoreClass StoreClass>
OB_INLINE void ObBitMapMetaWriter<StoreClass>::fill_meta_header(bool &suitable)
{
  const int64_t bs_exc_cnt = has_ext_val_ ? exc_row_ids_->count() : 0;
  const int64_t ext_len = BitSet::get_words_num(col_datums_->count()) * BitSet::BYTE_PER_WORD;
  const int64_t bs_len = get_ext_size(bs_exc_cnt);
  const int64_t index_len = exc_fix_size_ < 0 ? (exc_row_ids_->count() - 1) * index_byte_ : 0;

  if (UINT8_MAX < (ext_len + bs_len + index_len)) {
    suitable = false;
  } else {
    meta_.ext_offset_ = static_cast<uint8_t>(ext_len);
    meta_.index_offset_ = static_cast<uint8_t>(ext_len + bs_len);
    meta_.data_offset_ = static_cast<uint8_t>(ext_len + bs_len + index_len);
    if (bit_packing_) {
      meta_.bit_packing_len_ = static_cast<uint8_t>(exc_fix_size_);
    } else if (0 > exc_fix_size_) {
      meta_.index_byte_ = static_cast<uint8_t>(index_byte_);
    } else {
      meta_.fix_data_cnt_ = static_cast<uint8_t>(exc_row_ids_->count());
    }
  }
}

template <ObObjTypeStoreClass StoreClass>
OB_INLINE void ObBitMapMetaWriter<StoreClass>::process_cell(const common::ObDatum &datum)
{
  const int64_t len = datum.len_;
  exc_total_size_ += len;
  if (!var_store_) {
    if (exc_fix_size_ < 0) {
      exc_fix_size_ = len;
    } else if (len != exc_fix_size_) {
      exc_fix_size_ = -1;
      var_store_ = true;
    }
  }
}

template <>
OB_INLINE void ObBitMapMetaWriter<ObIntSC>::process_cell(const common::ObDatum &datum)
{
  const uint64_t integer_mask = INTEGER_MASK_TABLE[get_type_size_map()[type_.get_type()]];
  const uint64_t v = datum.get_uint64() & integer_mask;
  if (v > max_integer_) {
    max_integer_ = v;
  }
}

template <>
OB_INLINE void ObBitMapMetaWriter<ObUIntSC>::process_cell(const common::ObDatum &datum)
{
  const uint64_t integer_mask = INTEGER_MASK_TABLE[get_type_size_map()[type_.get_type()]];
  const uint64_t v = datum.get_uint64() & integer_mask;
  if (v > max_integer_) {
    max_integer_ = v;
  }
}


template <ObObjTypeStoreClass StoreClass>
int ObBitMapMetaWriter<StoreClass>::write(char *buf)
{
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = common::OB_NOT_INIT;
    STORAGE_LOG(WARN, "not init", K(ret));
  } else if (OB_ISNULL(buf)) {
    ret = common::OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), KP(buf));
  } else {
    const int64_t row_cnt = col_datums_->count();
    const int64_t bs_exc_cnt = has_ext_val_ ? exc_row_ids_->count() : 0;

    // initalize
    const int64_t bs_len = get_ext_size(bs_exc_cnt);
    MEMCPY(buf, reinterpret_cast<char *>(&meta_), sizeof(meta_));
    buf += sizeof(meta_);

    if (OB_FAIL(bitset_.init(reinterpret_cast<uint64_t *>(buf), row_cnt))) {
      STORAGE_LOG(WARN, "failed to init bitset", K(ret), KP(buf), K(row_cnt));
    } else if (OB_FAIL(ext_bs_.init(reinterpret_cast<unsigned char *>(buf + meta_.ext_offset_), bs_len))) {
      STORAGE_LOG(WARN, "failed to init bit stream", K(ret), KP(buf), K(bs_len));
    }

    // write exception data
    if (OB_SUCC(ret)) {
      if (bit_packing_) { // bit packing exc
        if (OB_FAIL(write_bit_packing_data(buf + meta_.data_offset_))) {
          STORAGE_LOG(WARN, "write bit packing data failed", K(ret));
        }
      } else if (exc_fix_size_ < 0) { // var exc
        if (OB_FAIL(index_gen_.init(buf + meta_.index_offset_, index_byte_))) {
          STORAGE_LOG(WARN, "init index gen failed", K(ret), KP(buf), K_(index_byte));
        } else if (OB_FAIL(write_var_data(buf + meta_.data_offset_))) {
          STORAGE_LOG(WARN, "write var data failed", K(ret));
        }
      } else { // fix exc
        if (OB_FAIL(write_fix_data(buf + meta_.data_offset_))) {
          STORAGE_LOG(WARN, "write bit packing data failed", K(ret));
        }
      }
    }
  }
  return ret;
}

template <ObObjTypeStoreClass StoreClass>
int ObBitMapMetaWriter<StoreClass>::write_bit_packing_data(char *buf)
{
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = common::OB_NOT_INIT;
    STORAGE_LOG(WARN, "not init", K(ret));
  } else if (OB_ISNULL(buf)) {
    ret = common::OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), KP(buf));
  } else {
    int64_t offset = 0;
    int64_t byte_offset = 0;
    int64_t bit_off_in_byte = 0;
    bool overflow = false;
    const uint64_t mask = ObBitStream::get_mask(exc_fix_size_);

    for (int64_t ref = 0; OB_SUCC(ret) && ref < exc_row_ids_->count(); ++ref) {
      const int64_t rid = exc_row_ids_->at(ref);
      const common::ObDatum &datum = col_datums_->at(rid);
      const int64_t ext_val = has_ext_val_ ? get_stored_ext_value(datum) : STORED_NOT_EXT;
      if (STORED_NOT_EXT != ext_val) {
        // ext also need space
        offset += exc_fix_size_;
      } else {
        byte_offset = offset / CHAR_BIT;
        bit_off_in_byte = offset % CHAR_BIT;
        overflow = (bit_off_in_byte + exc_fix_size_) > 64;
        ObBitStream::memory_safe_set(reinterpret_cast<unsigned char *>(buf) + byte_offset,
            bit_off_in_byte, overflow, datum.get_uint64() & mask);
        offset += exc_fix_size_;
      }

      // set exc bit
      bitset_.set(rid);
      if (has_ext_val_) {
        if (OB_FAIL(ext_bs_.set(ref * EXT_VALUE_BITS, EXT_VALUE_BITS, ext_val))) {
          STORAGE_LOG(WARN, "set ext bs failed", K(ret), K(ref), K(ext_val));
        }
      }
    }
  }
  return ret;
}

template <ObObjTypeStoreClass StoreClass>
int ObBitMapMetaWriter<StoreClass>::write_var_data(char *buf)
{
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = common::OB_NOT_INIT;
    STORAGE_LOG(WARN, "not init", K(ret));
  } else if (OB_ISNULL(buf)) {
    ret = common::OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), KP(buf));
  } else {
    int64_t offset = 0;
    for (int64_t ref = 0; OB_SUCC(ret) && ref < exc_row_ids_->count(); ++ref) {
      const int64_t rid = exc_row_ids_->at(ref);
      const common::ObDatum &datum = col_datums_->at(rid);
      const int64_t ext_val = has_ext_val_ ? get_stored_ext_value(datum) : STORED_NOT_EXT;
      if (ref > 0) {
        index_gen_.get_array().set(ref - 1, offset);
      }
      if (STORED_NOT_EXT != ext_val) {
        // ext does not need space
      } else {
        write_cell(buf, offset, datum, datum.len_);
      }

      // set exc bit
      bitset_.set(rid);
      if (has_ext_val_) {
        if (OB_FAIL(ext_bs_.set(ref * EXT_VALUE_BITS, EXT_VALUE_BITS, ext_val))) {
          STORAGE_LOG(WARN, "set ext bs failed", K(ret), K(ref), K(ext_val));
        }
      }
    }
  }
  return ret;
}

template <ObObjTypeStoreClass StoreClass>
int ObBitMapMetaWriter<StoreClass>::write_fix_data(char *buf)
{
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = common::OB_NOT_INIT;
    STORAGE_LOG(WARN, "not init", K(ret));
  } else if (OB_ISNULL(buf)) {
    ret = common::OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), KP(buf));
  } else {
    int64_t offset = 0;
    for (int64_t ref = 0; OB_SUCC(ret) && ref < exc_row_ids_->count(); ++ref) {
      const int64_t rid = exc_row_ids_->at(ref);
      const common::ObDatum &datum = col_datums_->at(rid);
      const int64_t ext_val = has_ext_val_ ? get_stored_ext_value(datum) : STORED_NOT_EXT;
      if (STORED_NOT_EXT != ext_val) {
        // ext also need space
        offset += exc_fix_size_;
      } else {
        write_cell(buf, offset, datum, exc_fix_size_);
      }

      // set exc bit
      bitset_.set(rid);
      if (has_ext_val_) {
        if (OB_FAIL(ext_bs_.set(ref * EXT_VALUE_BITS, EXT_VALUE_BITS, ext_val))) {
          STORAGE_LOG(WARN, "set ext bs failed", K(ret), K(ref), K(ext_val));
        }
      }
    }
  }
  return ret;
}

template<ObObjTypeStoreClass StoreClass>
OB_INLINE void ObBitMapMetaWriter<StoreClass>::write_cell(
      char *buf,
      int64_t &offset,
      const common::ObDatum &datum,
      const int64_t len)
{
  MEMCPY(buf + offset, datum.ptr_, datum.len_);
  offset += len;
}

template <ObObjTypeStoreClass StoreClass>
class ObBitMapMetaReader
{
public:
  static int read(const char *buf, const int64_t row_count,
      const bool bit_packing, const int64_t row_id, const int64_t len,
      int64_t &ref, common::ObDatum &datum, const common::ObObjType &obj_type);

  OB_INLINE static int read_exc_cell(const char *buf, const ObBitMapMetaHeader *meta,
    const bool bit_packing, const int64_t ref, const int64_t len,
    common::ObDatum &datum, const uint64_t integer_mask, const common::ObObjType &obj_type);
};


template <ObObjTypeStoreClass StoreClass>
int ObBitMapMetaReader<StoreClass>::read(const char *buf, const int64_t row_count,
    const bool bit_packing, const int64_t row_id, const int64_t len,
    int64_t &ref, common::ObDatum &datum, const common::ObObjType &obj_type)
{
  int ret = common::OB_SUCCESS;
  if (OB_ISNULL(buf)
      || OB_UNLIKELY(row_count <= 0)
      || OB_UNLIKELY(row_id < 0)
      || OB_UNLIKELY(len <= 0)) {
    ret = common::OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), KP(buf), K(row_count), K(row_id), K(len));
  } else {
    const ObBitMapMetaHeader *meta = reinterpret_cast<const ObBitMapMetaHeader *>(buf);
    buf += sizeof(ObBitMapMetaHeader);

    // get ref
    ref = BitSet::get_ref(reinterpret_cast<uint64_t *>(const_cast<char *>(buf)), row_id);
    if (-1 == ref) {
      // not exception cell, handled by caller
    } else {
      // read ext
      int64_t ext_val = STORED_NOT_EXT;
      if (meta->has_ext_val()) { // has extend value
        if (OB_FAIL(ObBitStream::get(reinterpret_cast<unsigned char *>(
                  const_cast<char *>(buf + meta->ext_offset_)),
                ref * EXT_VALUE_BITS, EXT_VALUE_BITS, ext_val))) {
          STORAGE_LOG(WARN, "bitstream get failed", K(ret));
        }
      }
      // read data
      uint64_t integer_mask = 0;
      if (common::ObIntTC == ob_obj_type_class(obj_type)) {
        integer_mask = ~INTEGER_MASK_TABLE[get_type_size_map()[obj_type]];
      }
      if (OB_FAIL(ret)) {
      } else if (STORED_NOT_EXT != ext_val) {
        set_stored_ext_value(datum, static_cast<ObStoredExtValue>(ext_val));
      } else if (OB_FAIL(read_exc_cell(buf, meta, bit_packing, ref,
              len - sizeof(ObBitMapMetaHeader) - meta->data_offset_, datum, integer_mask, obj_type))) {
        STORAGE_LOG(WARN, "read exc cell failed", K(ret));
      }
    }
  }
  return ret;
}

template <ObObjTypeStoreClass StoreClass>
OB_INLINE int ObBitMapMetaReader<StoreClass>::read_exc_cell(const char *buf,
    const ObBitMapMetaHeader *meta, const bool bit_packing, const int64_t ref,
    const int64_t len, common::ObDatum &datum, const uint64_t integer_mask, const common::ObObjType &obj_type)
{
  int ret = common::OB_SUCCESS;
  uint32_t datum_len = 0;
  uint64_t v = 0;
  if (OB_FAIL(get_uint_data_datum_len(
      common::ObDatum::get_obj_datum_map_type(obj_type),
      datum_len))){
    STORAGE_LOG(WARN, "Failed to get datum len for int data", K(ret));
  } else if (bit_packing) {
    if (OB_FAIL(ObBitStream::get(reinterpret_cast<unsigned char *>
        (const_cast<char *>(buf + meta->data_offset_)),
        ref * meta->bit_packing_len_, meta->bit_packing_len_, v))) {
      STORAGE_LOG(WARN, "bs get failed", K(ret), K(ref), K(*meta));
    } else {
      datum.pack_ = datum_len;
      MEMCPY(const_cast<char *>(datum.ptr_), &v, datum_len);
    }
  } else {
    const int64_t cell_len = meta->get_fix_data_size(len);
    MEMCPY(&v, buf + meta->data_offset_ + ref * cell_len, cell_len);
    if (0 != integer_mask && (v & (integer_mask >> 1))) {
      v |= integer_mask;
    }
    datum.pack_ = datum_len;
    MEMCPY(const_cast<char *>(datum.ptr_), &v, datum_len);
  }
  return ret;
}

template <>
OB_INLINE int ObBitMapMetaReader<ObNumberSC>::read_exc_cell(const char *buf,
    const ObBitMapMetaHeader *meta, const bool bit_packing, const int64_t ref,
    const int64_t len, common::ObDatum &datum, const uint64_t integer_mask, const common::ObObjType &obj_type)
{
  int ret = common::OB_SUCCESS;
  UNUSEDx(bit_packing, integer_mask, obj_type);
  // get offset and length
  int64_t offset = 0;
  if (!meta->is_var_exc()) {
    offset = ref * meta->get_fix_data_size(len);
  } else {
    ObIntegerArrayGenerator index_gen;
    if (OB_FAIL(index_gen.init(buf + meta->index_offset_, meta->index_byte_))) {
      STORAGE_LOG(WARN, "init index gen failed", K(ret));
    } else {
      if (0 != ref) {
        offset = index_gen.get_array().at(ref - 1);
      }
    }
  }
  MEMCPY(const_cast<char *>(datum.ptr_), buf + meta->data_offset_ + offset, sizeof(ObNumberDesc));
  const uint8_t num_len = datum.num_->desc_.len_;
  datum.pack_ = sizeof(ObNumberDesc) + num_len * sizeof(uint32_t);
  if (OB_LIKELY(1 == num_len)) {
    MEMCPY(const_cast<char *>(datum.ptr_) + sizeof(ObNumberDesc),
        buf + meta->data_offset_ + offset + sizeof(ObNumberDesc), sizeof(uint32_t));
  } else {
    MEMCPY(const_cast<char *>(datum.ptr_) + sizeof(ObNumberDesc),
        buf + meta->data_offset_ + offset + sizeof(ObNumberDesc), num_len * sizeof(uint32_t));
  }
  return ret;
}

template <>
OB_INLINE int ObBitMapMetaReader<ObDecimalIntSC>::read_exc_cell(const char *buf,
    const ObBitMapMetaHeader *meta, const bool bit_packing, const int64_t ref,
    const int64_t len, common::ObDatum &datum, const uint64_t integer_mask, const common::ObObjType &obj_type)
{
  int ret = common::OB_SUCCESS;
  UNUSEDx(bit_packing, integer_mask, obj_type);
  int64_t offset = 0;
  int64_t cell_len = 0;
  if (!meta->is_var_exc()) {
    cell_len = meta->get_fix_data_size(len);
    offset = ref * cell_len;
  } else {
    const int64_t exc_cnt = meta->get_var_cnt();
    ObIntegerArrayGenerator index_gen;
    if (OB_FAIL(index_gen.init(buf + meta->index_offset_, meta->index_byte_))) {
      STORAGE_LOG(WARN, "init index gen failed", K(ret));
    } else {
      if (0 != ref) {
        offset = index_gen.get_array().at(ref - 1);
      }
      if (ref == exc_cnt - 1) {
        cell_len = len - offset;
      } else {
        cell_len = index_gen.get_array().at(ref) - offset;
      }
    }
  }
  datum.pack_ = static_cast<int32_t>(cell_len);
  MEMCPY(const_cast<char *>(datum.ptr_), buf + meta->data_offset_ + offset, cell_len);
  return ret;
}

template <>
OB_INLINE int ObBitMapMetaReader<ObStringSC>::read_exc_cell(const char *buf,
    const ObBitMapMetaHeader *meta, const bool bit_packing, const int64_t ref,
    const int64_t len, common::ObDatum &datum, const uint64_t integer_mask, const common::ObObjType &obj_type)
{
  int ret = common::OB_SUCCESS;
  UNUSEDx(bit_packing, integer_mask, obj_type);
  // get offset and length
  int64_t offset = 0;
  int64_t cell_len = 0;
  if (!meta->is_var_exc()) {
    cell_len = meta->get_fix_data_size(len);
    offset = ref * cell_len;
  } else {
    const int64_t exc_cnt = meta->get_var_cnt();
    ObIntegerArrayGenerator index_gen;
    if (OB_FAIL(index_gen.init(buf + meta->index_offset_, meta->index_byte_))) {
      STORAGE_LOG(WARN, "init index gen failed", K(ret));
    } else {
      if (0 != ref) {
        offset = index_gen.get_array().at(ref - 1);
      }
      if (ref == exc_cnt - 1) { // last one
        cell_len = len - offset;
      } else {
        cell_len = index_gen.get_array().at(ref) - offset;
      }
    }
  }
  datum.pack_ = static_cast<int32_t>(cell_len);
  datum.ptr_ = buf + meta->data_offset_ + offset;
  return ret;
}

template <>
OB_INLINE int ObBitMapMetaReader<ObOTimestampSC>::read_exc_cell(const char *buf,
    const ObBitMapMetaHeader *meta, const bool bit_packing, const int64_t ref,
    const int64_t len, common::ObDatum &datum, const uint64_t integer_mask, const common::ObObjType &obj_type)
{
  int ret = common::OB_SUCCESS;
  UNUSEDx(bit_packing, integer_mask);
  // get offset and length
  int64_t offset = 0;
  if (!meta->is_var_exc()) {
    offset = ref * meta->get_fix_data_size(len);
  } else {
    ObIntegerArrayGenerator index_gen;
    if (OB_FAIL(index_gen.init(buf + meta->index_offset_, meta->index_byte_))) {
      STORAGE_LOG(WARN, "init index gen failed", K(ret));
    } else {
      if (0 != ref) {
        offset = index_gen.get_array().at(ref - 1);
      }
    }
  }

  if (OB_SUCC(ret)) {
    ObObjDatumMapType datum_type = ObDatum::get_obj_datum_map_type(obj_type);
    const uint32_t size = ObDatum::get_reserved_size(datum_type);
    MEMCPY(const_cast<char *>(datum.ptr_), buf + meta->data_offset_ + offset, size);
    datum.pack_ = size;
  }
  return ret;
}

template <>
OB_INLINE int ObBitMapMetaReader<ObIntervalSC>::read_exc_cell(const char *buf,
    const ObBitMapMetaHeader *meta, const bool bit_packing, const int64_t ref,
    const int64_t len, common::ObDatum &datum, const uint64_t integer_mask, const common::ObObjType &obj_type)
{
  int ret = common::OB_SUCCESS;
  UNUSEDx(bit_packing, integer_mask);
  // get offset and length
  int64_t offset = 0;
  if (!meta->is_var_exc()) {
    offset = ref * meta->get_fix_data_size(len);
  } else {
    ObIntegerArrayGenerator index_gen;
    if (OB_FAIL(index_gen.init(buf + meta->index_offset_, meta->index_byte_))) {
      STORAGE_LOG(WARN, "init index gen failed", K(ret));
    } else {
      if (0 != ref) {
        offset = index_gen.get_array().at(ref - 1);
      }
    }
  }

  if (OB_SUCC(ret)) {
    ObObjDatumMapType datum_type = ObDatum::get_obj_datum_map_type(obj_type);
    const uint32_t size = ObDatum::get_reserved_size(datum_type);
    MEMCPY(const_cast<char *>(datum.ptr_), buf + meta->data_offset_ + offset, size);
    datum.pack_ = size;
  }
  return ret;
}


template<typename VectorType, ObEncodingDecodeMetodType DECODE_TYPE>
struct ObBitMapExcValReadCellFunc
{
  static inline int read_exc_cell(const ObBitMapMetaHeader *meta, const char *buf, const int64_t ref,
      const bool bitpacked, const int64_t len, const int64_t vec_offset,
      const common::ObObjType &store_type, VectorType &vector)
  {
    int ret = OB_SUCCESS;
    int64_t offset = 0;
    uint32_t cell_len = 0;
    if (!meta->is_var_exc()) {
      cell_len = meta->get_fix_data_size(len);
      offset = ref * cell_len;
    } else {
      const int64_t exc_cnt = meta->get_var_cnt();
      ObIntegerArrayGenerator index_gen;
      if (OB_FAIL(index_gen.init(buf + meta->index_offset_, meta->index_byte_))) {
        STORAGE_LOG(WARN, "init index gen failed", K(ret));
      } else {
        if (0 != ref) {
          offset = index_gen.get_array().at(ref - 1);
        }
        if (ref == exc_cnt - 1) { // last one
          cell_len = len - offset;
        } else {
          cell_len = index_gen.get_array().at(ref) - offset;
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (ObEncodingDecodeMetodType::D_DEEP_COPY == DECODE_TYPE) {
        vector.set_payload(vec_offset, buf + meta->data_offset_ + offset, cell_len);
      } else if (ObEncodingDecodeMetodType::D_SHALLOW_COPY == DECODE_TYPE) {
        vector.set_payload_shallow(vec_offset, buf + meta->data_offset_ + offset, cell_len);
      } else {
        ret = OB_NOT_SUPPORTED;
        STORAGE_LOG(WARN, "not supported decode type", K(ret), K(store_type));
      }
    }
    return ret;
  }
};

template<typename VectorType>
struct ObBitMapExcValReadCellFunc<VectorType, ObEncodingDecodeMetodType::D_INTEGER>
{
  static inline int read_exc_cell(const ObBitMapMetaHeader *meta, const char *buf, const int64_t ref,
      const bool bitpacked, const int64_t len, const int64_t vec_offset,
      const common::ObObjType &store_type, VectorType &vector)
  {
    int ret = OB_SUCCESS;
    uint32_t datum_len = 0;
    uint64_t unpacked_val = 0;
    if (OB_FAIL(get_uint_data_datum_len(common::ObDatum::get_obj_datum_map_type(store_type), datum_len))){
      STORAGE_LOG(WARN, "Failed to get datum len for int data", K(ret));
    } else if (bitpacked) {
      if (OB_FAIL(ObBitStream::get(
          reinterpret_cast<unsigned char *>(const_cast<char *>(buf + meta->data_offset_)),
          ref * meta->bit_packing_len_, meta->bit_packing_len_, unpacked_val))) {
        STORAGE_LOG(WARN, "failed to unpack data", K(ret), K(ref), KPC(meta));
      }
    } else {
      const int64_t cell_len = meta->get_fix_data_size(len);
      MEMCPY(&unpacked_val, buf + meta->data_offset_ + ref * cell_len, cell_len);
      if (common::ObIntTC == ob_obj_type_class(store_type)) {
        uint64_t integer_mask = ~INTEGER_MASK_TABLE[get_type_size_map()[store_type]];
        if (0 != integer_mask && (unpacked_val & (integer_mask >> 1))) {
          unpacked_val |= integer_mask;
        }
      }
    }

    if (OB_SUCC(ret)) {
      vector.set_payload(vec_offset, &unpacked_val, datum_len);
    }
    return ret;
  }
};

template<typename VectorType, ObEncodingDecodeMetodType DECODE_TYPE>
struct ObBitMapExcValDecodeFunc
{
  static int decode(const char *buf, const int64_t ref, const bool bitpacked, const int64_t len,
      const int64_t vec_offset, const common::ObObjType &store_type, VectorType &vector)
  {
    int ret = OB_SUCCESS;
    if (OB_ISNULL(buf) || OB_UNLIKELY(-1 == ref || len <= 0)) {
      ret = OB_INVALID_ARGUMENT;
      STORAGE_LOG(WARN, "Invalid argument", K(ret), KP(buf), K(bitpacked), K(len), K(ref), K(store_type));
    } else {
      const ObBitMapMetaHeader *meta = reinterpret_cast<const ObBitMapMetaHeader *>(buf);
      const char *store_data = buf + sizeof(ObBitMapMetaHeader);
      const int64_t store_data_len = len - sizeof(ObBitMapMetaHeader) - meta->data_offset_;
      int64_t ext_val = STORED_NOT_EXT;
      if (meta->has_ext_val()) { // has extend value
        if (OB_FAIL(ObBitStream::get(reinterpret_cast<unsigned char *>(
                  const_cast<char *>(store_data + meta->ext_offset_)),
                ref * EXT_VALUE_BITS, EXT_VALUE_BITS, ext_val))) {
          STORAGE_LOG(WARN, "Bitstream get failed", K(ret));
        }
      }

      if (OB_FAIL(ret)) {
      } else if (STORED_NOT_EXT != ext_val) {
        // nop not supported for vector yet
        if (STORED_NULL == ext_val) {
          vector.set_null(vec_offset);
        } else {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(WARN, "unexpected extended value", K(ret), K(ext_val));
        }
      } else {
        ret = ObBitMapExcValReadCellFunc<VectorType, DECODE_TYPE>::read_exc_cell(
            meta, store_data, ref, bitpacked, store_data_len, vec_offset, store_type, vector);
        if (OB_FAIL(ret)) {
          STORAGE_LOG(WARN, "Failed to read exception cell", K(ret), KPC(meta), K(store_type));
        }
      }
    }
  return ret;
  }
};

template<ObEncodingDecodeMetodType DECODE_TYPE>
struct ObBitMapExcValDecodeFunc<ObFixedLengthFormat<char[0]>, DECODE_TYPE>
{
  static int decode(const char *buf, const int64_t ref, const bool bitpacked, const int64_t len,
      const int64_t vec_offset, const common::ObObjType &store_type, ObFixedLengthFormat<char[0]> &vector)
  {
    int ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "unexpected var-length data for fixed-length vector", K(ret));
    return ret;
  }
};

typedef ObBitMapMetaWriter<ObUIntSC> ObIntBitMapMetaWriter;
typedef ObBitMapMetaWriter<ObNumberSC> ObNumberBitMapMetaWriter;
typedef ObBitMapMetaWriter<ObStringSC> ObStringBitMapMetaWriter;
typedef ObBitMapMetaWriter<ObOTimestampSC> ObOTimestampBitMapMetaWriter;
typedef ObBitMapMetaWriter<ObIntervalSC> ObIntervalBitMapMetaWriter;

} // end namespace blocksstable
} // end namespace oceanbase

#endif // OCEANBASE_ENCODING_OB_ENCODING_BITSET_H_
