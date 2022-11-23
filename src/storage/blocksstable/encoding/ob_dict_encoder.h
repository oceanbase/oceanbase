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

#ifndef OCEANBASE_ENCODING_OB_DICT_ENCODER_H_
#define OCEANBASE_ENCODING_OB_DICT_ENCODER_H_

#include "ob_icolumn_encoder.h"
#include "lib/allocator/ob_pooled_allocator.h"
#include "lib/container/ob_array_iterator.h"
#include "ob_encoding_util.h"
#include "ob_bit_stream.h"
#include "ob_encoding_hash_util.h"

namespace oceanbase
{
namespace blocksstable
{

class ObEncodingHashTable;
struct ObEncodingHashNodeList;

struct ObDictMetaHeader
{
  static constexpr uint8_t OB_DICT_META_HEADER_V1 = 0;
  enum DictAttribute
  {
    FIX_LENGTH = 0x1,
    IS_SORTED = 0x2
  };

  uint8_t version_;
  uint8_t row_ref_size_;
  uint32_t count_;
  union {
    uint16_t data_size_;
    uint16_t index_byte_;
  };
  uint8_t attr_;
  char payload_[0];

  ObDictMetaHeader() { memset(this, 0, sizeof(*this)); }
  inline void reset() { memset(this, 0, sizeof(*this)); }
  inline bool is_fix_length_dict() const { return attr_ & FIX_LENGTH; }
  inline void set_fix_length_attr() { attr_ |= FIX_LENGTH; }
  inline void set_sorted_attr() { attr_ |= IS_SORTED; }
  inline bool is_sorted_dict() const { return attr_ & IS_SORTED; }

  TO_STRING_KV(K_(version), K_(count), K_(data_size), K_(index_byte), K_(row_ref_size), K_(attr));
}__attribute__((packed));

class ObDictEncoder : public ObIColumnEncoder
{
public:
  static const ObColumnHeader::Type type_ = ObColumnHeader::DICT;
  typedef common::ObArray<int64_t> RowIdArray;

  struct DictCmp
  {
    explicit DictCmp(int &ret, const ObCmpFunc &cmp_func) : ret_(ret), cmp_func_(cmp_func)
    {
    }

    bool operator()(const ObEncodingHashNodeList &left, const ObEncodingHashNodeList &right);
  private:
    int &ret_;
    const ObCmpFunc &cmp_func_;
  };

  ObDictEncoder();
  virtual ~ObDictEncoder();

  virtual int init(
      const ObColumnEncodingCtx &ctx,
      const int64_t column_index,
      const ObConstDatumRowArray &rows) override;
  virtual void reuse() override;
  virtual int store_meta(ObBufferWriter &buf_writer);
  virtual int store_data(const int64_t row_id, ObBitStream &bs,
      char *buf, const int64_t len) override
  {
    UNUSEDx(row_id, bs, buf, len);
    return common::OB_NOT_SUPPORTED;
  }
  virtual int get_row_checksum(int64_t &checksum) const override;

  virtual int traverse(bool &suitable) override;
  // should be invoked after traverse
  virtual int64_t calc_size() const override
  {
    int64_t size = calc_meta_size();
    if (desc_.bit_packing_length_ > 0) { // row
      size += (rows_->count() * desc_.bit_packing_length_ + CHAR_BIT - 1) / CHAR_BIT;
    } else {
      size += rows_->count() * desc_.fix_data_length_;
    }
    return size;
  }

  inline int64_t calc_meta_size() const
  {
    int64_t size = sizeof(ObDictMetaHeader);
    if (store_var_dict()) { // dict
      // we do not store the index for the first
      // element, since it is always 0
      size += dict_index_byte_ * (count_ - 1) + var_data_size_;
    } else {
      size += dict_fix_data_size_ * count_;
    }
    return size;
  }

  virtual ObColumnHeader::Type get_type() const override { return type_; }
  virtual int store_fix_data(ObBufferWriter &buf_writer) override;
private:
  int build_dict();
  int store_dict(const common::ObDatum &datum, char *buf, int64_t &len);
  inline int64_t number_store_size(const common::ObObj &obj)
  {
    return sizeof(obj.nmb_desc_) + obj.nmb_desc_.len_ * sizeof(obj.v_.nmb_digits_[0]);
  }
  bool store_var_dict() const { return 0 > dict_fix_data_size_ || UINT16_MAX < dict_fix_data_size_; }

  struct ColumnStoreFiller;
private:
  ObObjTypeStoreClass store_class_;
  int64_t type_store_size_;
  int64_t dict_fix_data_size_;
  // size of all var values
  int64_t var_data_size_;
  // index byte for var dict
  int64_t dict_index_byte_;
  uint64_t max_integer_;
  ObDictMetaHeader *dict_meta_header_;
  // count of the distinct cells
  int64_t count_;
  bool need_sort_;
  ObEncodingHashTable *ht_;

  DISALLOW_COPY_AND_ASSIGN(ObDictEncoder);
};

} // end namespace blocksstable
} // end namespace oceanbase

#endif // OCEANBASE_ENCODING_OB_DICT_ENCODER_H_
