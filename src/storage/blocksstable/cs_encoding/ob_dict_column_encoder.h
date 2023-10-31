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

#ifndef OCEANBASE_ENCODING_OB_DICT_COLUMN_ENCODER_H_
#define OCEANBASE_ENCODING_OB_DICT_COLUMN_ENCODER_H_

#include "ob_icolumn_cs_encoder.h"
#include "ob_stream_encoding_struct.h"
#include "storage/blocksstable/encoding/ob_encoding_hash_util.h"
#include "ob_integer_stream_encoder.h"

namespace oceanbase
{
namespace blocksstable
{
class ObDictColumnEncoder : public ObIColumnCSEncoder
{
public:
  static const int64_t MAX_EXCEPTION_COUNT = 64;
  static const int32_t MAX_CONST_EXCEPTION_PCT = 10;

  ObDictColumnEncoder();
  virtual ~ObDictColumnEncoder();

  struct DictCmp
  {
    explicit DictCmp(int &ret, const ObCmpFunc &cmp_func) : ret_(ret), cmp_func_(cmp_func) { }
    bool operator()(const ObEncodingHashNodeList &left, const ObEncodingHashNodeList &right);

  private:
    int &ret_;
    const ObCmpFunc &cmp_func_;
  };
  void reuse() override;
  int get_identifier_and_stream_types(
      ObColumnEncodingIdentifier &identifier, const ObIntegerStream::EncodingType *&types) const override;

  INHERIT_TO_STRING_KV("ICSColumnEncoder", ObIColumnCSEncoder, K_(dict_encoding_meta), K_(ref_enc_ctx), K_(max_ref));

protected:
  enum IdentifierFlag
  {
    IS_CONST_REF = 0x1,
  };

  int build_ref_encoder_ctx_();
  int try_const_encoding_ref_();
  int do_sort_dict_();
  int store_dict_encoding_meta_(ObMicroBufferWriter &buf_writer);
  int store_dict_ref_(ObMicroBufferWriter &buf_writer);
  template <typename T>
  int do_store_dict_ref_(ObMicroBufferWriter &buf_writer);

protected:
  ObDictEncodingMeta dict_encoding_meta_;
  ObIntegerStreamEncoderCtx ref_enc_ctx_;
  int64_t max_ref_;
  int64_t ref_stream_max_value_;
  int32_t int_stream_idx_;
  ObEncodingHashNode *const_list_header_;
  uint16_t ref_exception_cnt_; // total non-const ref
  common::ObCompressor *compressor_;
};

// for const ref, the ref array format:
// ref_exception_cnt + const_ref + [exception row ids] + [exception refs]
template <typename T>
int ObDictColumnEncoder::do_store_dict_ref_(ObMicroBufferWriter &buf_writer)
{
  int ret = OB_SUCCESS;
  T *ref_arr = nullptr;
  int64_t ref_arr_size = sizeof(T) * dict_encoding_meta_.ref_row_cnt_;
  if (OB_ISNULL(ref_arr = static_cast<T*>(ctx_->allocator_->alloc(ref_arr_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "fail to alloc", K(ret), K(ref_arr_size), K_(dict_encoding_meta));
  } else if (dict_encoding_meta_.is_const_encoding_ref()) {
    const ObEncodingHashNode *node_list = ctx_->ht_->get_node_list();
    ref_arr[0] = ref_exception_cnt_;
    ref_arr[1] = const_list_header_->dict_ref_;
    if (dict_encoding_meta_.ref_row_cnt_ > 2) {
      T *row_id_arr = ref_arr + 2;
      T *ref_exception_arr = ref_arr + 2 + ref_exception_cnt_;
      int64_t idx = 0;
      for (int64_t row_id = 0; OB_SUCC(ret) && row_id < row_count_; ++row_id) {
        const ObEncodingHashNode &node = node_list[row_id];
        if (const_list_header_->dict_ref_ != node.dict_ref_) {
          if (OB_UNLIKELY(idx >= ref_exception_cnt_)) {
            ret = OB_ERR_UNEXPECTED;
            STORAGE_LOG(WARN, "unexpected idx", K(ret), K(idx), K_(ref_exception_cnt));
          } else {
            row_id_arr[idx] = row_id;
            ref_exception_arr[idx] = node.dict_ref_;
            ++idx;
          }
        }
      }
    }
  } else {
    for (int64_t i = 0; i < row_count_; i++) {
      ref_arr[i] = ctx_->ht_->get_node_list()[i].dict_ref_;
    }
  }
  ObIntegerStreamEncoder integer_encoder;
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(integer_encoder.encode(ref_enc_ctx_, ref_arr, dict_encoding_meta_.ref_row_cnt_, buf_writer))) {
    STORAGE_LOG(WARN, "fail to store ref integer stream", K(ret), K(ref_enc_ctx_), K_(dict_encoding_meta));
  } else if (OB_FAIL(stream_offsets_.push_back((uint32_t)buf_writer.length()))) {
    STORAGE_LOG(WARN, "fail to push back ref stream offset", K(ret));
  } else {
    int_stream_encoding_types_[int_stream_idx_] = ref_enc_ctx_.meta_.get_encoding_type();
    int_stream_idx_++;
  }

  return ret;
}

}  // end namespace blocksstable
}  // end namespace oceanbase

#endif  // OCEANBASE_ENCODING_OB_DICT_COLUMN_ENCODER_H_
