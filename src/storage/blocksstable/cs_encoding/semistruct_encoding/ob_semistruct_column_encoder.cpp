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

#include "ob_semistruct_column_encoder.h"
#include "storage/blocksstable/cs_encoding/ob_cs_encoding_util.h"
#include "storage/blocksstable/cs_encoding/semistruct_encoding/ob_semistruct_encoding_util.h"

namespace oceanbase
{
namespace blocksstable
{

using namespace common;

ObSemiStructColumnEncoder::ObSemiStructColumnEncoder():
  semistruct_ctx_(nullptr), semistruct_header_(nullptr), sub_col_headers_(nullptr)
{
}

ObSemiStructColumnEncoder::~ObSemiStructColumnEncoder() {}

int ObSemiStructColumnEncoder::init(const ObColumnCSEncodingCtx &ctx, const int64_t column_index, const int64_t row_count)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_FAIL(ObIColumnCSEncoder::init(ctx, column_index, row_count))) {
    LOG_WARN("init base column encoder failed", K(ret), K(ctx), K(column_index), K(row_count));
  } else {
    column_header_.type_ = type_;
    if (OB_FAIL(do_init_())) {
      LOG_WARN("fail to pre_handle", K(ret));
    }
  }
  return ret;
}

int ObSemiStructColumnEncoder::do_init_()
{
  int ret = OB_SUCCESS;
  uint32_t uncompress_len = ctx_->var_data_size_;
  if (ctx_->null_or_nop_cnt_ > 0) {
    column_header_.set_has_null_or_nop_bitmap();
  }
  if (ctx_->nop_cnt_ > 0) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("nop is not supported in semistruct json column", K(ret));
  } else if (OB_ISNULL(semistruct_ctx_ = ctx_->semistruct_ctx_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("semistruct_ctx is null", K(ret), KPC(ctx_));
  } else if (OB_FAIL(semistruct_ctx_->init_sub_column_encoders())) {
    LOG_WARN("init sub column encoders fail", K(ret), KPC(semistruct_ctx_));
  }
  return ret;
}

void ObSemiStructColumnEncoder::reuse()
{
  ObIColumnCSEncoder::reuse();
  semistruct_ctx_ = nullptr;
  semistruct_header_ = nullptr;
  sub_col_headers_ = nullptr;
  // sub column encoder will freed in ObSemiStructColumnEncodeCtx::reuse
  // that will be called in ObMicroBlockCSEncoder::reuse, so here donot sub column encoder reuse
}

int ObSemiStructColumnEncoder::store_column(ObMicroBufferWriter &buf_writer)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_ISNULL(semistruct_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("semistruct_ctx is null", K(ret));
  } else if (ctx_->has_stored_meta_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid state", K(ret), KPC(ctx_));
  } else if (OB_FAIL(store_column_meta(buf_writer))) {
    LOG_WARN("fail to store column meta", K(ret));
  } else {
    int64_t sub_col_count = semistruct_ctx_->get_store_column_count();
    for (int64_t i = 0; OB_SUCC(ret) && i < sub_col_count; ++i) {
      ObIColumnCSEncoder *sub_encoder = nullptr;
      if (OB_ISNULL(sub_encoder = semistruct_ctx_->sub_encoders_.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("sub column encoder is null", K(ret), K(i), KPC(semistruct_ctx_));
      } else if (OB_FAIL(sub_encoder->store_column(buf_writer))) {
        LOG_WARN("store sub column fail", K(ret), K(i), KPC(semistruct_ctx_));
      } else if (OB_FAIL(sub_encoder->get_stream_offsets(stream_offsets_))) {
        LOG_WARN("fail to get sub column stream offsets", K(ret), K(i), KPC(semistruct_ctx_));
      } else if (OB_FAIL(update_previous_info_after_encoding_(i, *sub_encoder))) {
        LOG_WARN("update_previous_info_after_encoding fail", K(ret), K(i), KPC(semistruct_ctx_));
      } else {
        sub_col_headers_[i] = sub_encoder->get_column_header();
      }
    }
    if (OB_SUCC(ret)) {
      semistruct_header_->stream_cnt_ += stream_offsets_.count();
    }
  }
  return ret;
}

int ObSemiStructColumnEncoder::store_column_meta(ObMicroBufferWriter &buf_writer)
{
  int ret = OB_SUCCESS;
  int64_t start_offset = buf_writer.length();
  if (OB_FAIL(reserve_header_(buf_writer))) {
    LOG_WARN("reserve_header fail", K(ret), K(buf_writer));
  } else if (OB_FAIL(reserve_sub_col_headers_(buf_writer))) {
    LOG_WARN("reserve_sub_col_headers fail", K(ret), K(buf_writer));
  } else if (OB_FAIL(serialize_sub_schema_(buf_writer))) {
    LOG_WARN("serialize sub schema fail", K(ret));
  } else if (OB_FAIL(store_null_bitamp(buf_writer))) {
    LOG_WARN("fail to store null bitmap", K(ret));
  } else {
    int64_t sub_col_count = semistruct_ctx_->get_store_column_count();
    for (int64_t i = 0; OB_SUCC(ret) && i < sub_col_count; ++i) {
      ObIColumnCSEncoder *sub_encoder = nullptr;
      if (OB_ISNULL(sub_encoder = semistruct_ctx_->sub_encoders_.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("sub column encoder is null", K(ret), K(i), KPC(semistruct_ctx_));
      } else if (OB_FAIL(sub_encoder->store_column_meta(buf_writer))) {
        LOG_WARN("store sub column meta fail", K(ret), K(i), KPC(semistruct_ctx_));
      } else {
        semistruct_ctx_->sub_col_ctxs_.at(i).has_stored_meta_ = true;
      }
    }
    if (OB_SUCC(ret)) {
      semistruct_header_->header_len_ = buf_writer.length() - start_offset;
    }
  }
  return ret;
}

int64_t ObSemiStructColumnEncoder::estimate_store_size() const
{
  int64_t size = INT64_MAX;
  if (IS_NOT_INIT) {
  } else {
    size = base_header_size_();
    int64_t sub_col_count = semistruct_ctx_->get_store_column_count();
    int64_t sub_col_store_size = 0;
    for (int64_t i = 0; i < sub_col_count; ++i) {
      sub_col_store_size += semistruct_ctx_->sub_encoders_.at(i)->estimate_store_size();
    }
    size += sub_col_store_size;
    LOG_TRACE("store size info", K(sub_col_count), K(size), K(sub_col_store_size));
  }
  return size;
}

int ObSemiStructColumnEncoder::get_identifier_and_stream_types(
    ObColumnEncodingIdentifier &identifier, const ObIntegerStream::EncodingType *&types) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    identifier.set(type_, int_stream_count_, 0);
    types = int_stream_encoding_types_;
  }
  return ret;
}

int ObSemiStructColumnEncoder::get_maximal_encoding_store_size(int64_t &size) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    size = base_header_size_();
    int64_t sub_col_count = semistruct_ctx_->get_store_column_count();
    for (int64_t i = 0; OB_SUCC(ret) && i < sub_col_count; ++i) {
      int64_t sub_col_store_size = 0;
      if (OB_FAIL(semistruct_ctx_->sub_encoders_.at(i)->get_maximal_encoding_store_size(sub_col_store_size))) {
        LOG_WARN("get sub column maximal_encoding_store_size fail", K(ret), K(i), KPC(semistruct_ctx_));
      } else {
        size += sub_col_store_size;
      }
    }
    if (OB_SUCC(ret)) {
      size = std::min(size, ObCSEncodingUtil::MAX_COLUMN_ENCODING_STORE_SIZE);
    }
  }
  return ret;
}

int ObSemiStructColumnEncoder::get_string_data_len(uint32_t &len) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    len = 0;
    int64_t sub_col_count = semistruct_ctx_->get_store_column_count();
    for (int64_t i = 0; OB_SUCC(ret) && i < sub_col_count; ++i) {
      uint32_t sub_col_string_len = 0;
      if (OB_FAIL(semistruct_ctx_->sub_encoders_.at(i)->get_string_data_len(sub_col_string_len))) {
        LOG_WARN("get sub column string data len fail", K(ret), K(i), KPC(semistruct_ctx_));
      } else {
        len += sub_col_string_len;
      }
    }
  }
  return ret;
}

int64_t ObSemiStructColumnEncoder::base_header_size_() const
{
  int64_t size = 0;
  int64_t sub_col_count = semistruct_ctx_->get_store_column_count();
  size += sizeof(ObSemiStructEncodeHeader);
  size += sizeof(ObCSColumnHeader) * sub_col_count;
  size += semistruct_ctx_->get_sub_schema_serialize_size();
  if (column_header_.has_null_or_nop_bitmap()) {
    size += ObCSEncodingUtil::get_bitmap_byte_size(row_count_);
  }
  return size;
}

int ObSemiStructColumnEncoder::reserve_header_(ObMicroBufferWriter &buf_writer)
{
  int ret = OB_SUCCESS;
  int64_t header_size = sizeof(ObSemiStructEncodeHeader);
  if (OB_ISNULL(semistruct_header_ = new (buf_writer.current()) ObSemiStructEncodeHeader())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("current of buffer is null", K(ret), K(buf_writer));
  } else if (OB_FAIL(buf_writer.write_nop(header_size, true))) {
    LOG_WARN("data buffer fail to advance headers size", K(ret), K(header_size));
  } else {
    semistruct_header_->type_ = ObSemiStructEncodeHeader::Type::JSON;
    semistruct_header_->column_cnt_ = semistruct_ctx_->get_store_column_count();
  }
  return ret;
}

int ObSemiStructColumnEncoder::reserve_sub_col_headers_(ObMicroBufferWriter &buf_writer)
{
  int ret = OB_SUCCESS;
  int64_t sub_col_count = semistruct_ctx_->get_store_column_count();
  int64_t sub_col_header_size = sizeof(ObCSColumnHeader) * sub_col_count;
  if (OB_ISNULL(sub_col_headers_ = reinterpret_cast<ObCSColumnHeader*>(buf_writer.current()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("current of buffer is null", K(ret), K(buf_writer));
  } else if (OB_FAIL(buf_writer.write_nop(sub_col_header_size, true))) {
    LOG_WARN("data buffer fail to advance sub column headers size", K(ret), K(sub_col_header_size));
  } else {
    for (int i = 0; i < sub_col_count; ++i) {
      sub_col_headers_[i].reuse();
    }
  }
  return ret;
}

int ObSemiStructColumnEncoder::serialize_sub_schema_(ObMicroBufferWriter &buf_writer)
{
  int ret = OB_SUCCESS;
  int64_t start_offset = buf_writer.length();
  if (OB_FAIL(semistruct_ctx_->serialize_sub_schema(buf_writer))) {
    LOG_WARN("serialize sub schema fail", K(ret));
  } else {
    semistruct_header_->schema_len_ = buf_writer.length() - start_offset;
    LOG_TRACE("serialize sub column succese", KPC(semistruct_header_), K(semistruct_ctx_->sub_schema_));
  }
  return ret;
}

int ObSemiStructColumnEncoder::update_previous_info_after_encoding_(const int32_t col_idx, ObIColumnCSEncoder &e)
{
  int ret = OB_SUCCESS;
  const ObIntegerStream::EncodingType *stream_types = nullptr;
  ObColumnEncodingIdentifier identifier;
  if (OB_FAIL(e.get_identifier_and_stream_types(identifier, stream_types))) {
    LOG_WARN("get_identifier_and_stream_types fail", K(ret), K(col_idx));
  } else if (OB_FAIL(semistruct_ctx_->previous_cs_encoding_.update_stream_detect_info(col_idx, identifier,
      stream_types, ctx_->encoding_ctx_->major_working_cluster_version_))) {
    LOG_WARN("update_column_detect_info fail", K(ret), K(col_idx), K(identifier));
  }
  return ret;
}

}  // end namespace blocksstable
}  // end namespace oceanbase
