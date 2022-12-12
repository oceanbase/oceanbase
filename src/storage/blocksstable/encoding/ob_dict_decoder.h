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

#ifndef OCEANBASE_ENCODING_OB_DICT_DECODER_H_
#define OCEANBASE_ENCODING_OB_DICT_DECODER_H_

#include "ob_icolumn_decoder.h"
#include "ob_encoding_query_util.h"
#include "ob_integer_array.h"
#include "ob_dict_encoder.h"
#include "sql/engine/ob_bit_vector.h"

namespace oceanbase
{
namespace blocksstable
{

struct ObColumnHeader;
struct ObDictMetaHeader;
class ObDictDecoderIterator;

typedef void (*dict_var_batch_decode_func)(
                const char *ref_data,
                const char *off_data,
                const char *base_data,
                const char *base_data_end,
                const int64_t dict_cnt,
                const int64_t *row_ids, const int64_t row_cap,
                common::ObDatum *datums);

typedef void (*dict_fix_batch_decode_func)(
                  const char *ref_data, const char *base_data,
                  const int64_t fixed_len,
                  const int64_t dict_cnt,
                  const int64_t *row_ids, const int64_t row_cap,
                  common::ObDatum *datums);

typedef void (*dict_cmp_ref_func)(
                  const int64_t row_cnt,
                  const int64_t dict_ref,
                  const int64_t dict_cnt,
                  const unsigned char *col_data,
                  sql::ObBitVector &result);

class ObDictDecoder : public ObIColumnDecoder
{
public:
  static const ObColumnHeader::Type type_ = ObColumnHeader::DICT;
  ObDictDecoder() : store_class_(ObExtendSC),
                    integer_mask_(0), meta_header_(NULL)
  {}
  virtual ~ObDictDecoder() {}

  OB_INLINE int init(
           const ObMicroBlockHeader &micro_block_header,
           const ObColumnHeader &column_header,
           const char *meta);
  int init(const common::ObObjType &store_obj_type, const char *meta_header);
  virtual int decode(ObColumnDecoderCtx &ctx, common::ObObj &cell, const int64_t row_id,
      const ObBitStream &bs, const char *data, const int64_t len) const override;

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

  virtual int update_pointer(const char *old_block, const char *cur_block) override;

  int decode(common::ObObjMeta cell_meta, common::ObObj &cell, const int64_t ref, const int64_t meta_legnth) const;

  int batch_decode_dict(
      const common::ObObjType &obj_type,
      const char **cell_datas,
      const int64_t row_cap,
      const int64_t meta_length,
      common::ObDatum *datums) const;

  void reset() { this->~ObDictDecoder(); new (this) ObDictDecoder(); }
  OB_INLINE void reuse();
  virtual ObColumnHeader::Type get_type() const override { return type_; }
  bool is_inited() const { return NULL != meta_header_; }

  virtual int pushdown_operator(
      const sql::ObPushdownFilterExecutor *parent,
      const ObColumnDecoderCtx &col_ctx,
      const sql::ObWhiteFilterExecutor &filter,
      const char* meta_data,
      const ObIRowIndex* row_index,
      ObBitmap &result_bitmap) const override;

  OB_INLINE const ObDictMetaHeader* get_dict_header() const { return meta_header_; }
public:
  ObDictDecoderIterator begin(const ObColumnDecoderCtx *ctx, int64_t meta_length) const;
  ObDictDecoderIterator end(const ObColumnDecoderCtx *ctx, int64_t meta_length) const;

private:
  bool fast_decode_valid(const ObColumnDecoderCtx &ctx) const;

  // unpacked refs should be stores in datums.pack_
  int batch_get_bitpacked_refs(
      const int64_t *row_ids,
      const int64_t row_cap,
      const unsigned char *col_data,
      common::ObDatum *datums) const;

  int batch_get_null_count(
    const int64_t *row_ids,
    const int64_t row_cap,
    const unsigned char *col_data,
    int64_t &null_count) const;

  int nu_nn_operator(
      const sql::ObPushdownFilterExecutor *parent,
      const ObColumnDecoderCtx &col_ctx,
      const unsigned char* col_data,
      const sql::ObWhiteFilterExecutor &filter,
      ObBitmap &result_bitmap) const;

  int eq_ne_operator(
      const sql::ObPushdownFilterExecutor *parent,
      const ObColumnDecoderCtx &col_ctx,
      const unsigned char* col_data,
      const sql::ObWhiteFilterExecutor &filter,
      ObBitmap &result_bitmap) const;

  int comparison_operator(
      const sql::ObPushdownFilterExecutor *parent,
      const ObColumnDecoderCtx &col_ctx,
      const unsigned char* col_data,
      const sql::ObWhiteFilterExecutor &filter,
      ObBitmap &result_bitmap) const;

  int bt_operator(
      const sql::ObPushdownFilterExecutor *parent,
      const ObColumnDecoderCtx &col_ctx,
      const unsigned char* col_data,
      const sql::ObWhiteFilterExecutor &filter,
      ObBitmap &result_bitmap) const;

  int in_operator(
      const sql::ObPushdownFilterExecutor *parent,
      const ObColumnDecoderCtx &col_ctx,
      const unsigned char* col_data,
      const sql::ObWhiteFilterExecutor &filter,
      ObBitmap &result_bitmap) const;

  int load_data_to_obj_cell(const ObObjMeta cell_meta, const char *cell_data, int64_t cell_len, ObObj &load_obj) const;

  int cmp_ref_and_set_res(
      const sql::ObPushdownFilterExecutor *parent,
      const ObColumnDecoderCtx &col_ctx,
      const int64_t dict_ref,
      const unsigned char *col_data,
      ObFPIntCmpOpType cmp_op,
      bool flag,
      ObBitmap &result_bitmap) const;

  int fast_cmp_ref_and_set_res(
      const ObColumnDecoderCtx &col_ctx,
      const int64_t dict_ref,
      const unsigned char *col_data,
      const sql::ObWhiteFilterOperatorType op_type,
      ObBitmap &result_bitmap) const;

  int set_res_with_bitset(
      const sql::ObPushdownFilterExecutor *parent,
      const ObColumnDecoderCtx &col_ctx,
      const unsigned char *col_data,
      const sql::ObBitVector *ref_bitset,
      ObBitmap &result_bitmap) const;

  OB_INLINE int read_ref(
      const int64_t row_id,
      const bool is_bit_packing,
      const unsigned char *col_data,
      int64_t &ref) const;

private:
  ObObjTypeStoreClass store_class_;
  uint64_t integer_mask_;
  const ObDictMetaHeader *meta_header_;
  const char *var_data_;
};

OB_INLINE int ObDictDecoder::init(
    const ObMicroBlockHeader &micro_block_header,
    const ObColumnHeader &column_header,
    const char *meta)
{
  UNUSEDx(micro_block_header);
  int ret = common::OB_SUCCESS;
  // performance critical, don't check params, already checked upper layer
  if (OB_UNLIKELY(is_inited())) {
    ret = common::OB_INIT_TWICE;
    STORAGE_LOG(WARN, "init twice", K(ret));
  } else {
    const common::ObObjTypeClass type_class = ob_obj_type_class(column_header.get_store_obj_type());
    store_class_ = get_store_class_map()[type_class];
    if (common::ObIntTC == type_class) {
      int64_t type_store_size = get_type_size_map()[column_header.get_store_obj_type()];
      integer_mask_ = ~INTEGER_MASK_TABLE[type_store_size];
    } else {
      integer_mask_ = 0;
    }

    // better to put code that access same data together
    meta += column_header.offset_;
    meta_header_ = reinterpret_cast<const ObDictMetaHeader *>(meta);

    if (!meta_header_->is_fix_length_dict()) {
      // the index of the first var (is 0) was not stored
      var_data_ = meta_header_->payload_ + (meta_header_->count_ - 1) * meta_header_->index_byte_;
    }
  }

  return ret;
}

OB_INLINE void ObDictDecoder::reuse()
{
  meta_header_ = NULL;
  /*
  obj_meta_.reset();
  store_class_ = ObExtendSC;
  integer_mask_ = 0;
  micro_block_header_ = NULL;
  column_header_ = NULL;
  meta_header_ = NULL;
  meta_data_ = NULL;
  var_data_ = NULL;
  */
}

OB_INLINE int ObDictDecoder::read_ref(
    const int64_t row_id,
    const bool is_bit_packing,
    const unsigned char *col_data,
    int64_t &ref) const
{
  // Not check inited ot parameters for performance
  int ret = OB_SUCCESS;
  ref = 0;
  if (is_bit_packing) {
    if (OB_FAIL(ObBitStream::get(col_data, row_id * meta_header_->row_ref_size_,
          meta_header_->row_ref_size_, ref))) {
      STORAGE_LOG(WARN, "Failed to get bit packing value",
          K(ret), K(row_id), K_(*meta_header), K(ref));
    }
  } else {
    MEMCPY(&ref, col_data + row_id * meta_header_->row_ref_size_,
        meta_header_->row_ref_size_);
  }
  return ret;
}

/**
 *  Iterator to traverse the dictionary for DICT / CONST / RLE encoding
 */
class ObDictDecoderIterator
{
public:
  typedef ObObj value_type;
  typedef int64_t difference_type;
  typedef ObObj *pointer;
  typedef ObObj &reference;
  typedef std::random_access_iterator_tag iterator_category;
public:
  ObDictDecoderIterator() : decoder_(nullptr), ctx_(nullptr),
                            index_(0), meta_length_(0), cell_() {}
  explicit ObDictDecoderIterator(
      const ObDictDecoder *decoder,
      const ObColumnDecoderCtx *ctx,
      int64_t index,
      int64_t meta_length)
      : decoder_(decoder), ctx_(ctx), index_(index), meta_length_(meta_length), cell_() {}
  explicit ObDictDecoderIterator(
      const ObDictDecoder *decoder,
      const ObColumnDecoderCtx *ctx,
      int64_t index,
      int64_t meta_length,
      ObObj& cell)
  {
    decoder_ = decoder;
    ctx_ = ctx;
    index_ = index;
    meta_length_ = meta_length;
    cell_ = cell;
  }
  inline value_type &operator*()
  {
    OB_ASSERT(nullptr != decoder_);
    cell_.set_meta_type(ctx_->obj_meta_);
    OB_ASSERT(OB_SUCCESS == decoder_->decode(ctx_->obj_meta_, cell_, index_, meta_length_));
    if (cell_.is_fixed_len_char_type() && nullptr != ctx_->col_param_) {
      OB_ASSERT(OB_SUCCESS == storage::pad_column(ctx_->col_param_->get_accuracy(),
                                                  *(ctx_->allocator_), cell_));
    }
    return cell_;
  }
  inline value_type *operator->()
  {
    OB_ASSERT(nullptr != decoder_);
    cell_.set_meta_type(ctx_->obj_meta_);
    OB_ASSERT(OB_SUCCESS == decoder_->decode(ctx_->obj_meta_, cell_, index_, meta_length_));
    if (cell_.is_fixed_len_char_type() && nullptr != ctx_->col_param_) {
      OB_ASSERT(OB_SUCCESS == storage::pad_column(ctx_->col_param_->get_accuracy(),
                                                  *(ctx_->allocator_), cell_));
    }
    return &cell_;
  }
  inline ObDictDecoderIterator operator--(int)
  {
    OB_ASSERT(nullptr != decoder_);
    return ObDictDecoderIterator(decoder_, ctx_, index_--, meta_length_, cell_);
  }
  inline ObDictDecoderIterator operator--()
  {
    OB_ASSERT(nullptr != decoder_);
    index_--;
    return *this;
  }
  inline ObDictDecoderIterator operator++(int)
  {
    OB_ASSERT(nullptr != decoder_);
    return ObDictDecoderIterator(decoder_, ctx_, index_++, meta_length_, cell_);
  }
  inline ObDictDecoderIterator &operator++()
  {
    OB_ASSERT(nullptr != decoder_);
    index_++;
    return *this;
  }
  inline ObDictDecoderIterator &operator+(int64_t offset)
  {
    OB_ASSERT(nullptr != decoder_);
    index_ += offset;
    return *this;
  }
  inline ObDictDecoderIterator &operator+=(int64_t offset)
  {
    OB_ASSERT(nullptr != decoder_);
    index_ += offset;
    return *this;
  }
  inline difference_type operator-(const ObDictDecoderIterator &rhs)
  {
    return index_ - rhs.index_;
  }
  inline ObDictDecoderIterator &operator-(int64_t offset)
  {
    OB_ASSERT(nullptr != decoder_);
    index_ -= offset;
    return *this;
  }
  inline bool operator==(const ObDictDecoderIterator &rhs) const
  {
    return (this->index_ == rhs.index_);
  }
  inline bool operator!=(const ObDictDecoderIterator &rhs)
  {
    return (this->index_ != rhs.index_);
  }
  inline bool operator<(const ObDictDecoderIterator &rhs)
  {
    return (this->index_ < rhs.index_);
  }
  inline bool operator<=(const ObDictDecoderIterator &rhs)
  {
    return (this->index_ <= rhs.index_);
  }
private:
  const ObDictDecoder *decoder_;
  const ObColumnDecoderCtx *ctx_;
  int64_t index_;
  int64_t meta_length_;
  value_type cell_;
};

template <int32_t REF_LEN, int32_t CMP_TYPE>
struct DictCmpRefFunc_T
{
  static void dict_cmp_ref_func(
      const int64_t row_cnt,
      const int64_t dict_ref,
      const int64_t dict_cnt,
      const unsigned char *col_data,
      sql::ObBitVector &result)
  {
    typedef typename ObEncodingByteLenMap<false, REF_LEN>::Type RefType;
    const RefType *ref_arr = reinterpret_cast<const RefType *>(col_data);
    const RefType casted_dict_ref = *reinterpret_cast<const RefType *>(&dict_ref);
    const RefType casted_dict_cnt = *reinterpret_cast<const RefType *>(&dict_cnt);
    RefType ref = 0;
    if (CMP_TYPE <= sql::WHITE_OP_LT) {
      // equal, less than, less than or equal to
      for (int64_t row_id = 0; row_id < row_cnt; ++row_id) {
        if (value_cmp_t<RefType, CMP_TYPE>(ref_arr[row_id], casted_dict_ref)) {
          result.set(row_id);
        }
      }
    } else {
      for (int64_t row_id = 0; row_id < row_cnt; ++row_id) {
        if (value_cmp_t<RefType, sql::WHITE_OP_GE>(ref_arr[row_id], casted_dict_cnt)) {
          // null value
        } else if (value_cmp_t<RefType, CMP_TYPE>(ref_arr[row_id], casted_dict_ref)) {
          result.set(row_id);
        }
      }
    }
  }
};

extern ObMultiDimArray_T<dict_cmp_ref_func, 3, 6> dict_cmp_ref_funcs;
extern bool dict_cmp_ref_funcs_inited;

} // end namespace blocksstable
} // end namespace oceanbase


#endif // OCEANBASE_ENCODING_OB_DICT_DECODER_H_
