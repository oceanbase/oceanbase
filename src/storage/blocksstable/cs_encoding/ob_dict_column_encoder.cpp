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

#define USING_LOG_PREFIX STORAGE

#include "ob_dict_column_encoder.h"
#include "ob_cs_encoding_util.h"
#include "ob_string_stream_encoder.h"
#include "ob_integer_stream_encoder.h"
#include "ob_column_datum_iter.h"
#include "storage/blocksstable/ob_imicro_block_writer.h"

namespace oceanbase
{
namespace blocksstable
{

using namespace common;

bool ObDictColumnEncoder::DictCmp::operator()(
    const ObEncodingHashNodeList &lhs,
    const ObEncodingHashNodeList &rhs)
{
  bool res = false;
  int &ret = ret_;
  int cmp_ret = 0;
  if (OB_FAIL(ret)) {
  } else if (OB_UNLIKELY(nullptr == lhs.header_ || nullptr == rhs.header_)) {
    ret_ = OB_INVALID_ARGUMENT;
    LOG_WARN_RET(ret_, "invalid argument", KP(lhs.header_), KP(rhs.header_));
  } else if (OB_FAIL(cmp_func_.cmp_func_(*lhs.header_->datum_, *rhs.header_->datum_, cmp_ret))) {
    LOG_WARN("failed to compare datums", K(ret), K(*lhs.header_->datum_), K(*rhs.header_->datum_));
  } else {
    res = cmp_ret < 0;
  }
  return res;
}

ObDictColumnEncoder::ObDictColumnEncoder()
  : dict_encoding_meta_(),
    ref_enc_ctx_(),
    max_ref_(0),
    ref_stream_max_value_(0),
    int_stream_idx_(0),
    const_list_header_(nullptr),
    ref_exception_cnt_(0)
{
}

ObDictColumnEncoder::~ObDictColumnEncoder() {}


void ObDictColumnEncoder::reuse()
{
  ObIColumnCSEncoder::reuse();
  dict_encoding_meta_.reuse();
  ref_enc_ctx_.reset();
  max_ref_ = 0;
  int_stream_idx_ = 0;
  const_list_header_ = nullptr;
  ref_exception_cnt_ = 0;
}

int ObDictColumnEncoder::get_identifier_and_stream_types(
    ObColumnEncodingIdentifier &identifier, const ObIntegerStream::EncodingType *&types) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    int32_t flags = 0;
    if (dict_encoding_meta_.is_const_encoding_ref()) {
      flags |= IdentifierFlag::IS_CONST_REF;
    }
    identifier.set(get_type(), int_stream_count_, flags);
    types = int_stream_encoding_types_;
  }
  return ret;
}

int ObDictColumnEncoder::build_ref_encoder_ctx_()
{
  int ret = OB_SUCCESS;

  if (row_count_ == ctx_->null_cnt_) { // has no dict value
    dict_encoding_meta_.ref_row_cnt_ = 0;
    if (dict_encoding_meta_.distinct_val_cnt_ != 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected dict count", K(ret), KPC_(ctx), K_(dict_encoding_meta));
    }
  } else {
    bool is_replace_null = false;
    uint64_t null_replaced_value = 0;

    max_ref_ = dict_encoding_meta_.distinct_val_cnt_ - 1;
    if (ctx_->null_cnt_ > 0) {
      max_ref_ = dict_encoding_meta_.distinct_val_cnt_;
    }

    uint64_t range = 0;
    if (is_force_raw_) {
      if (OB_FAIL(ref_enc_ctx_.build_unsigned_stream_meta(
          0, max_ref_, is_replace_null, null_replaced_value, true, range))) {
        LOG_WARN("fail to build_unsigned_stream_meta", K(ret));
      }
    } else {
      if (OB_FAIL(try_const_encoding_ref_())) {
        LOG_WARN("fail to try_use_const_ref", K(ret));
      } else if (OB_FAIL(ref_enc_ctx_.build_unsigned_stream_meta(0, ref_stream_max_value_,
          is_replace_null, null_replaced_value, false, range))) {
        LOG_WARN("fail to build_unsigned_stream_meta", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      int32_t ref_stream_idx = 0;
      if (column_header_.is_integer_dict()) {
        ref_stream_idx = 1;
      } else if (!column_header_.is_fixed_length()) {
        ref_stream_idx = 1;
      }
      ++int_stream_count_;
      if (OB_FAIL(ref_enc_ctx_.build_stream_encoder_info(
          false/*has_null*/,
          false/*not monotonic*/,
          &ctx_->encoding_ctx_->cs_encoding_opt_,
          ctx_->encoding_ctx_->previous_cs_encoding_.get_column_encoding(column_index_),
          ref_stream_idx, ctx_->encoding_ctx_->compressor_type_, ctx_->allocator_))) {
        LOG_WARN("fail to build_stream_encoder_info", K(ret));
      }
    }
  }

  return ret;
}

int ObDictColumnEncoder::try_const_encoding_ref_()
{
  STATIC_ASSERT(MAX_EXCEPTION_COUNT < std::numeric_limits<__typeof__(ref_exception_cnt_)>::max(),
      "MAX_EXCEPTION_COUNT is too large");
  int ret = OB_SUCCESS;
  int64_t max_const_cnt = 0;

  FOREACH(l, *ctx_->ht_) { // choose the const value
    if (l->size_ > max_const_cnt) {
      max_const_cnt = l->size_;
      const_list_header_ = l->header_;
    }
  }
  if (ctx_->null_cnt_ > 0) {
    if (ctx_->null_cnt_ > max_const_cnt) {
      max_const_cnt = ctx_->null_cnt_;
      const_list_header_ = ctx_->ht_->get_null_list().header_;
    }
  }
  const int64_t exception_cnt = row_count_ - max_const_cnt;
  if (OB_UNLIKELY(exception_cnt < 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected count", K(ret), K(exception_cnt), K_(row_count), K(max_const_cnt));
  } else if (0 == exception_cnt) {
    ref_exception_cnt_ = 0;
    dict_encoding_meta_.set_const_encoding_ref(exception_cnt);
    ref_stream_max_value_ = MAX(exception_cnt, const_list_header_->dict_ref_);
  } else if (exception_cnt <= MAX_EXCEPTION_COUNT &&
      exception_cnt < row_count_ * MAX_CONST_EXCEPTION_PCT / 100) {
    dict_encoding_meta_.set_const_encoding_ref(exception_cnt);
    const ObEncodingHashNode *node_list = ctx_->ht_->get_node_list();
    uint32_t exception_max_row_id = 0;
    for (int64_t i = ctx_->ht_->get_node_cnt() - 1; i >= 0; --i) {
      if (const_list_header_->dict_ref_ != node_list[i].dict_ref_) {
        exception_max_row_id = i;
        break;
      }
    }
    ref_stream_max_value_ = MAX3(exception_cnt, exception_max_row_id, max_ref_);
    ref_exception_cnt_ = exception_cnt;
  } else {
    ref_stream_max_value_ = max_ref_; // don't use const ref
  }

  return ret;
}

int ObDictColumnEncoder::do_sort_dict_()
{
  int ret = OB_SUCCESS;
  sql::ObExprBasicFuncs *basic_funcs = ObDatumFuncs::get_basic_func(
      column_type_.get_type(), column_type_.get_collation_type());
  ObCmpFunc cmp_func;
  cmp_func.cmp_func_ = lib::is_oracle_mode()
      ? basic_funcs->null_last_cmp_ : basic_funcs->null_first_cmp_;
  lib::ob_sort(ctx_->ht_->begin(), ctx_->ht_->end(), DictCmp(ret, cmp_func));
  // calc new dict_ref if dict is sorted
  int64_t i = 0;
  FOREACH(l, *ctx_->ht_) {
    FOREACH(n, *l) {
      n->dict_ref_ = i;
    }
    ++i;
  }
  dict_encoding_meta_.attrs_ |= ObDictEncodingMeta::Attribute::IS_SORTED;

  return ret;
}

int ObDictColumnEncoder::store_dict_encoding_meta_(ObMicroBufferWriter &buf_writer)
{
  int ret = OB_SUCCESS;
  ObDictEncodingMeta *dict_encoding_meta = reinterpret_cast<ObDictEncodingMeta*>(buf_writer.current());
  if (OB_FAIL(buf_writer.advance(sizeof(ObDictEncodingMeta)))) {
    LOG_WARN("buffer advance failed", K(ret), K(sizeof(ObDictEncodingMeta)));
  } else {
    *dict_encoding_meta = dict_encoding_meta_;
    LOG_DEBUG("store dict meta", KPC(dict_encoding_meta), K(buf_writer.length()), K(sizeof(ObDictEncodingMeta)));
  }

  return ret;
}

int ObDictColumnEncoder::store_dict_ref_(ObMicroBufferWriter &buf_writer)
{
  int ret = OB_SUCCESS;
  int64_t node_cnt = ctx_->ht_->get_node_cnt();
  if (OB_UNLIKELY(node_cnt != row_count_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "dict node count must equal to row_count", K(ret), K(node_cnt), K_(row_count));
  } else if (0 == dict_encoding_meta_.distinct_val_cnt_) {
    // has no dict value, means all datums are null, so don't need to store ref
  } else {
    const int64_t width_size = ref_enc_ctx_.meta_.get_uint_width_size();
    switch(width_size) {
    case 1 : {
      if (OB_FAIL(do_store_dict_ref_<uint8_t>(buf_writer))) {
        LOG_WARN("fail to do_store_dict_ref_", K(ret), K_(ref_enc_ctx));
      }
      break;
    }
    case 2 : {
      if (OB_FAIL(do_store_dict_ref_<uint16_t>(buf_writer))) {
        LOG_WARN("fail to do_store_dict_ref_", K(ret), K_(ref_enc_ctx));
      }
      break;
    }
    case 4 : {
      if (OB_FAIL(do_store_dict_ref_<uint32_t>(buf_writer))) {
        LOG_WARN("fail to do_store_dict_ref_", K(ret), K_(ref_enc_ctx));
      }
      break;
    }
    case 8 : {
      if (OB_FAIL(do_store_dict_ref_<uint64_t>(buf_writer))) {
        LOG_WARN("fail to do_store_dict_ref_", K(ret), K_(ref_enc_ctx));
      }
      break;
    }
    default:
      ret = OB_INVALID_ARGUMENT;
      STORAGE_LOG(WARN, "uint byte width size not invalid", K(ret), K(width_size));
      break;
    }
  }

  return ret;
}

}  // end namespace blocksstable
}  // end namespace oceanbase
