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

#include "ob_micro_block_cs_decoder.h"
#include "storage/access/ob_pushdown_aggregate.h"
#include "storage/access/ob_pushdown_aggregate_vec.h"

namespace oceanbase
{
using namespace lib;
using namespace common;
using namespace storage;
namespace blocksstable
{
struct ObBlockCachedCSDecoderHeader
{
  struct Col
  {
    uint16_t offset_;
    int16_t ref_col_idx_;
  };
  uint16_t count_;
  uint16_t col_count_;
  Col col_[0];

  const ObIColumnCSDecoder &at(const int64_t idx) const
  {
    return *reinterpret_cast<const ObIColumnCSDecoder *>(
      (reinterpret_cast<const char *>(&col_[count_]) + col_[idx].offset_));
  }
} __attribute__((packed));

struct ObDecoderArrayAllocator
{
  ObDecoderArrayAllocator(char *buf) : buf_(buf), offset_(0) {}
  template <typename T>
  int alloc(T *&t)
  {
    int ret = OB_SUCCESS;
    if (NULL == buf_) {
      LOG_WARN("not init", K(ret));
    } else {
      t = new (buf_ + offset_) T();
      offset_ += sizeof(T);
    }
    return ret;
  }

  template <typename T>
  void free(const T *)
  {
    // do nothing
  }

private:
  char *buf_;
  int64_t offset_;
};

ObNoneExistColumnCSDecoder ObMicroBlockCSDecoder::none_exist_column_decoder_;
ObColumnCSDecoderCtx ObMicroBlockCSDecoder::none_exist_column_decoder_ctx_;
ObNewColumnCSDecoder ObMicroBlockCSDecoder::new_column_decoder_;

// performance critical, do not check parameters
int ObColumnCSDecoder::decode(const int64_t row_id, ObStorageDatum &datum)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(decoder_->decode(*ctx_, row_id, datum))) {
    LOG_WARN("decode fail", K(ret), K(row_id), KPC_(ctx));
  }
  return ret;
}

int ObColumnCSDecoder::batch_decode(
  const int32_t *row_ids, const int64_t row_cap, common::ObDatum *datums)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == row_ids || nullptr == datums || 0 >= row_cap)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid null row_iter or datums", K(ret), KP(row_ids), KP(datums), K(row_cap));
  } else if (OB_UNLIKELY(!decoder_->can_vectorized())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpect column batch_decode not supported.", K(ret), K(decoder_->get_type()));
  } else if (OB_FAIL(decoder_->batch_decode(*ctx_, row_ids, row_cap, datums))) {
    LOG_WARN("Failed to batch decode data to datum in column decoder", K(ret), K(*ctx_));
  }

  LOG_DEBUG("[Batch decode] Batch decoded datums: ", K(ret), K(row_cap), K(*ctx_),
    K(ObArrayWrap<int32_t>(row_ids, row_cap)), K(ObArrayWrap<ObDatum>(datums, row_cap)));
  return ret;
}

int ObColumnCSDecoder::decode_vector(ObVectorDecodeCtx &vector_ctx)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!vector_ctx.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid parameters", K(ret), K(vector_ctx));
  } else if (OB_UNLIKELY(!decoder_->can_vectorized())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpect column batch_decode not supported.", K(ret), K(decoder_->get_type()));
  } else if (OB_FAIL(decoder_->decode_vector(*ctx_, vector_ctx))) {
    LOG_WARN("failed to decocde vector in column decoder", K(ret), K(*ctx_), K(vector_ctx));
  }
  return ret;
}

int ObColumnCSDecoder::get_row_count(
    const int32_t *row_ids, const int64_t row_cap, const bool contains_null, int64_t &count)
{
  int ret = OB_SUCCESS;
  int64_t null_count = 0;
  if (contains_null) {
    count = row_cap;
  } else if (OB_FAIL(decoder_->get_null_count(*ctx_, row_ids, row_cap, null_count))) {
    LOG_WARN("Failed to get null count from column decoder", K(ret), K(*ctx_));
  } else {
    count = row_cap - null_count;
  }
  return ret;
}

// performance critical, do not check parameters
int ObColumnCSDecoder::quick_compare(const ObStorageDatum &left,
  const ObStorageDatumCmpFunc &cmp_func, const int64_t row_id, int32_t &cmp_ret)
{
  int ret = OB_SUCCESS;
  ObStorageDatum right_datum;
  cmp_ret = 0;
  if (OB_FAIL(decoder_->decode(*ctx_, row_id, right_datum))) {
    LOG_WARN("decode cell fail", K(ret), K(row_id));
  } else if (OB_FAIL(cmp_func.compare(left, right_datum, cmp_ret))) {
    STORAGE_LOG(WARN, "fail to compare datums", K(ret), K(left), K(right_datum));
  }
  return ret;
}

typedef int (*new_decoder_func)(char *buf, const ObIColumnCSDecoder *&decoder);

static new_decoder_func new_decoder_funcs_[ObCSColumnHeader::MAX_TYPE] =
{
    new_decoder_with_allocated_buf<ObIntegerColumnDecoder>,
    new_decoder_with_allocated_buf<ObStringColumnDecoder>,
    new_decoder_with_allocated_buf<ObIntDictColumnDecoder>,
    new_decoder_with_allocated_buf<ObStrDictColumnDecoder>,
    new_decoder_with_allocated_buf<ObSemiStructColumnDecoder>,
};
template <class Decoder>
int new_decoder_with_allocated_buf(char *buf, const ObIColumnCSDecoder *&decoder)
{
  int ret = OB_SUCCESS;
  Decoder *d = nullptr;
  if (OB_ISNULL(d = new(buf) Decoder())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null", K(ret), KP(buf));
  } else {
    decoder = d;
  }
  return ret;
}

//////////////////////////ObIEncodeBlockGetReader/////////////////////////
ObNoneExistColumnCSDecoder ObICSEncodeBlockReader::none_exist_column_decoder_;
ObColumnCSDecoderCtx ObICSEncodeBlockReader::none_exist_column_decoder_ctx_;

ObICSEncodeBlockReader::ObICSEncodeBlockReader()
  : block_addr_(), request_cnt_(0), cached_decoder_(NULL), decoders_(nullptr),
    transform_helper_(), column_count_(0), default_decoders_(),
    ctxs_(NULL),
    decoder_allocator_(SET_IGNORE_MEM_VERSION(ObMemAttr(MTL_ID(), common::ObModIds::OB_DECODER_CTX)), OB_MALLOC_NORMAL_BLOCK_SIZE),
    buf_allocator_(SET_IGNORE_MEM_VERSION(ObMemAttr(MTL_ID(), "IENB_CSREADER")), OB_MALLOC_NORMAL_BLOCK_SIZE),
    allocated_decoders_buf_(nullptr),
    allocated_decoders_buf_size_(0),
    store_id_array_(NULL), default_store_ids_(),
    default_column_types_(), semistruct_decode_ctx_()
{
}

ObICSEncodeBlockReader::~ObICSEncodeBlockReader()
{
  semistruct_decode_ctx_.reset();
}

void ObICSEncodeBlockReader::reuse()
{
  cached_decoder_ = nullptr;
  request_cnt_ = 0;
  decoders_ = nullptr;
  ctxs_ = nullptr;
  store_id_array_ = nullptr;
  column_type_array_ = nullptr;
  transform_helper_.reset();
  column_count_ = 0;
  decoder_allocator_.reuse();
  semistruct_decode_ctx_.reuse();
}

void ObICSEncodeBlockReader::reset()
{
  semistruct_decode_ctx_.reset();
  cached_decoder_ = NULL;
  request_cnt_ = 0;
  decoders_ = nullptr;
  ctxs_ = NULL;
  store_id_array_ = NULL;
  column_type_array_ = nullptr;
  transform_helper_.reset();
  column_count_ = 0;
  decoder_allocator_.reset();
  buf_allocator_.reset();
  allocated_decoders_buf_ = nullptr;
  allocated_decoders_buf_size_ = 0;
}

int ObICSEncodeBlockReader::prepare(const int64_t column_cnt)
{
  int ret = OB_SUCCESS;
  if (column_cnt > DEFAULT_DECODER_CNT) {
    const int64_t store_ids_size = ALIGN_UP(sizeof(store_id_array_[0]) * column_cnt, 8);
    const int64_t column_types_size = ALIGN_UP(sizeof(ObObjMeta) * column_cnt, 8);
    const int64_t col_decoder_size = sizeof(ObColumnCSDecoder) * column_cnt;
    char *buf = nullptr;
    if (nullptr
        == (buf = reinterpret_cast<char*>(decoder_allocator_.alloc(
          store_ids_size + column_types_size + col_decoder_size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc memory for store ids fail", K(ret), K(column_cnt));
    } else {
      store_id_array_ = reinterpret_cast<int32_t *>(buf);
      column_type_array_ = reinterpret_cast<ObObjMeta *>(buf + store_ids_size);
      decoders_ = reinterpret_cast<ObColumnCSDecoder *>(buf + store_ids_size + column_types_size);
    }
  } else {
    store_id_array_ = &default_store_ids_[0];
    column_type_array_ = &default_column_types_[0];
    decoders_ = &default_decoders_[0];
  }
  return ret;
}

int ObICSEncodeBlockReader::do_init(const ObMicroBlockData &block_data, const int64_t request_cnt)
{
  int ret = OB_SUCCESS;
  column_count_ = transform_helper_.get_micro_block_header()->column_count_;
  request_cnt_ = request_cnt;
  cached_decoder_ =
      reinterpret_cast<const ObBlockCachedCSDecoderHeader *>(block_data.get_extra_buf());

  if (OB_FAIL(init_decoders())) {
    LOG_WARN("init decoders failed", K(ret));
  }

  if (OB_FAIL(ret)) {
    reset();
  }
  return ret;
}

int ObICSEncodeBlockReader::init_decoders()
{
  int ret = OB_SUCCESS;
  int64_t decoders_buf_pos = 0;
  if (OB_UNLIKELY(NULL == store_id_array_ || NULL == column_type_array_ ||
      nullptr == decoders_ || !transform_helper_.is_inited())) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("invalid inner stat", K(ret), KP_(store_id_array),
        KP(decoders_), K(transform_helper_.is_inited()));
  } else if (OB_FAIL(alloc_decoders_buf(decoders_buf_pos))) {
    LOG_WARN("fail to alloc decoders buf", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < request_cnt_; ++i) {
      if (OB_FAIL(add_decoder(store_id_array_[i], column_type_array_[i], decoders_buf_pos, decoders_[i]))) {
        LOG_WARN("add_decoder failed", K(ret), "request_idx", i, K(decoders_buf_pos));
      }
    }
  }
  return ret;
}

int ObICSEncodeBlockReader::add_decoder(const int64_t store_idx,
                                        const ObObjMeta &obj_meta,
                                        int64_t &decoders_buf_pos,
                                        ObColumnCSDecoder &dest)
{
  int ret = OB_SUCCESS;
  if (store_idx < 0 || store_idx >= column_count_) {  // non exist column
    dest.decoder_ = &none_exist_column_decoder_;
    dest.ctx_ = &none_exist_column_decoder_ctx_;
  } else {
    const ObIColumnCSDecoder *decoder = nullptr;
    if (OB_FAIL(acquire(store_idx, decoders_buf_pos, decoder))) {
      LOG_WARN("acquire decoder failed", K(ret), K(store_idx));
    } else if (OB_FAIL(transform_helper_.build_column_decoder_ctx(obj_meta, store_idx, ctxs_[store_idx]))) {
      LOG_WARN("fail to build column decoder ctx", K(ret), K(store_idx));
    } else if (decoder->get_type() == ObCSColumnHeader::SEMISTRUCT && OB_FAIL(init_semistruct_decoder(decoder, ctxs_[store_idx]))) {
      LOG_WARN("init semistruct decoder fail", K(ret), K(store_idx), K(obj_meta));
    } else {
      dest.decoder_ = decoder;
      dest.ctx_ = &ctxs_[store_idx];
    }
  }

  return ret;
}

int ObICSEncodeBlockReader::init_semistruct_decoder(const ObIColumnCSDecoder *decoder, ObColumnCSDecoderCtx &decoder_ctx)
{
  int ret = OB_SUCCESS;
  ObSemiStructColumnDecoderCtx &semistruct_ctx = decoder_ctx.semistruct_ctx_;
  if (OB_FAIL(semistruct_decode_ctx_.build_semistruct_ctx(semistruct_ctx))){
    LOG_WARN("build semistruct handler fail", K(ret), K(decoder_ctx));
  }
  return ret;
}

// called before inited
// performance critical, do not check parameters
int ObICSEncodeBlockReader::acquire(const int64_t store_idx,
                                    int64_t &decoders_buf_pos,
                                    const ObIColumnCSDecoder *&decoder)
{
  int ret = OB_SUCCESS;
  const ObCSColumnHeader &col_header = transform_helper_.get_column_header(store_idx);
  if (NULL != cached_decoder_ && store_idx < cached_decoder_->count_) {
    decoder = &cached_decoder_->at(store_idx);
  } else {
    const int64_t decoder_size = cs_decoder_sizes[col_header.type_];
    if (OB_UNLIKELY(decoders_buf_pos + decoder_size > allocated_decoders_buf_size_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("decoders buf not enough", K(ret), K(decoders_buf_pos),
          K(decoder_size), K(allocated_decoders_buf_size_), K(store_idx));
    } else if (OB_FAIL(new_decoder_funcs_[col_header.type_](
        allocated_decoders_buf_ + decoders_buf_pos, decoder))) {
      LOG_WARN("fail to new decoder", K(ret), K(store_idx), K(col_header.type_));
    } else {
      decoders_buf_pos += decoder_size;
    }
  }
  return ret;
}

int ObICSEncodeBlockReader::alloc_decoders_buf(int64_t &decoders_buf_pos)
{
  int ret = OB_SUCCESS;
  int64_t size = 0;
  int64_t decoder_ctx_cnt = MAX(request_cnt_, column_count_);
  size += sizeof(ObColumnCSDecoderCtx) * decoder_ctx_cnt;
  decoders_buf_pos = 0;

  for (int64_t i = 0; i < request_cnt_; ++i) {
    const int64_t store_idx = store_id_array_[i];
    const ObCSColumnHeader &col_header = transform_helper_.get_column_header(store_idx);
    if (store_idx < 0 || store_idx >= column_count_) {
      // non exist column
    } else if (NULL != cached_decoder_ && store_idx < cached_decoder_->count_) {
     // cached decoder
    } else {
      size += cs_decoder_sizes[col_header.type_];
    }
  }
  if (size > allocated_decoders_buf_size_) {
    if (allocated_decoders_buf_size_ > 0) {
      allocated_decoders_buf_ = nullptr;
      allocated_decoders_buf_size_ = 0;
      buf_allocator_.reuse();
    }
    if (OB_ISNULL(allocated_decoders_buf_ = (char*)buf_allocator_.alloc(size))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc", K(ret), K(size));
    } else {
      allocated_decoders_buf_size_ = size;
    }
  }
  if (OB_SUCC(ret)) {
    ctxs_ = reinterpret_cast<ObColumnCSDecoderCtx*>(allocated_decoders_buf_);
    decoders_buf_pos += sizeof(ObColumnCSDecoderCtx) * decoder_ctx_cnt;
  }
  return ret;
}

// ==============================ObCSEncodeBlockReader==============================//
void ObCSEncodeBlockGetReader::reuse()
{
  ObICSEncodeBlockReader::reuse();
  read_info_ = nullptr;
}

int ObCSEncodeBlockGetReader::init(
  const ObMicroBlockAddr &block_addr,
  const ObMicroBlockData &block_data,
  const ObITableReadInfo &read_info)
{
  int ret = OB_SUCCESS;
  const int64_t request_cnt = read_info.get_request_count();
  if (OB_UNLIKELY(!block_data.is_valid()) || request_cnt <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("argument is invalid", K(ret), K(block_data), K(request_cnt));
  } else if (OB_FAIL(prepare(request_cnt))) {
    LOG_WARN("prepare fail", K(ret), K(request_cnt));
  } else if (typeid(ObRowkeyReadInfo) == typeid(read_info)) {
    for (int64_t i  = 0; OB_SUCC(ret) && i < request_cnt; ++i) {
      store_id_array_[i] = i;
    }
  } else {
    const ObColumnIndexArray &cols_index = read_info.get_columns_index();
    const ObColDescIArray &cols_desc = read_info.get_columns_desc();
    for (int64_t idx = 0; idx < request_cnt; idx++) {
      store_id_array_[idx] = cols_index.at(idx);
      column_type_array_[idx] = cols_desc.at(idx).col_type_;
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(transform_helper_.init(&decoder_allocator_, block_data, store_id_array_, request_cnt))) {
    LOG_WARN("fail to init transform helper", K(ret));
  } else if (typeid(ObRowkeyReadInfo) == typeid(read_info)) {
    ObObjMeta col_type;
    const int64_t col_cnt = MIN(request_cnt, transform_helper_.get_micro_block_header()->column_count_);
    for (int64_t i  = 0; OB_SUCC(ret) && i < col_cnt; ++i) {
      col_type.set_type(static_cast<ObObjType>(transform_helper_.get_column_header(i).obj_type_));
      column_type_array_[i] = col_type;
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(do_init(block_data, request_cnt))) {
      LOG_WARN("failed to do init", K(ret), K(block_data), K(request_cnt));
    } else {
      // TODO: fenggu.yh, fix the logic of reuse block addr
      // block_addr_ = block_addr;
      read_info_ = &read_info;
    }
  }

  return ret;
}

int ObCSEncodeBlockGetReader::init_if_need(const ObMicroBlockAddr &block_addr,
                                          const ObMicroBlockData &block_data,
                                          const ObITableReadInfo &read_info)
{
  int ret = OB_SUCCESS;
  // TODO: fenggu.yh, fix the logic of reuse read info
  reuse();
  if (OB_FAIL(init(block_addr, block_data, read_info))) {
    LOG_WARN("failed to do inner init", K(ret), K(block_data), K(read_info));
  }
  return ret;
}

int ObCSEncodeBlockGetReader::get_row(const ObMicroBlockAddr &block_addr,
                                      const ObMicroBlockData &block_data,
                                      const ObDatumRowkey &rowkey,
                                      const ObITableReadInfo &read_info,
                                      ObDatumRow &row)
{
  int ret = OB_SUCCESS;
  bool found = false;
  int64_t row_id = -1;

  if (OB_FAIL(init_if_need(block_addr, block_data, read_info))) {
    LOG_WARN("failed to do inner init", K(ret), K(block_data), K(read_info));
  } else if (OB_FAIL(locate_row(rowkey, read_info.get_datum_utils(), row_id, found))) {
    LOG_WARN("failed to locate row", K(ret), K(rowkey));
  } else {
    row.row_flag_.reset();
    row.mvcc_row_flag_.reset();
    if (found) {
      if (OB_FAIL(get_all_columns(row_id, row))) {
        LOG_WARN("failed to get left columns", K(ret), K(rowkey), K(row_id));
      } else {
        row.count_ = request_cnt_;
        row.row_flag_.set_flag(ObDmlFlag::DF_INSERT);
      }
    } else {
      // not found
      row.row_flag_.set_flag(ObDmlFlag::DF_NOT_EXIST);
      ret = OB_BEYOND_THE_RANGE;
    }
  }
  LOG_DEBUG("ObCSEncodeBlockGetReader get_row", K(ret), K(found), K(row_id), K(read_info));
  return ret;
}

int ObCSEncodeBlockGetReader::get_all_columns(const int64_t row_id, ObDatumRow &row)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(row.get_capacity() < request_cnt_)) {
    ret = OB_BUF_NOT_ENOUGH;
    LOG_WARN("datum buf is not enough", K(ret), "expect_obj_count", request_cnt_,
             "actual_datum_capacity", row.get_capacity());
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < request_cnt_; ++i) {
      row.storage_datums_[i].reuse();
      if (OB_FAIL(decoders_[i].decode(row_id, row.storage_datums_[i]))) {
        LOG_WARN("decode cell failed", K(ret), K(row_id));
      }
    }
  }
  return ret;
}

int ObCSEncodeBlockGetReader::locate_row(const ObDatumRowkey &rowkey,
  const ObStorageDatumUtils &datum_utils, int64_t &row_id, bool &found)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(rowkey.get_datum_cnt() > datum_utils.get_rowkey_count())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument to locate row", K(ret), K(rowkey), K(datum_utils));
  } else {
    found = false;
    row_id = -1;
    // reader_
    const int64_t rowkey_cnt = rowkey.get_datum_cnt();
    const ObStorageDatum *datums = rowkey.datums_;
    // binary search
    int32_t high = transform_helper_.get_micro_block_header()->row_count_ - 1;
    int32_t low = 0;
    int32_t middle = 0;
    int32_t cmp_result = 0;

    while (OB_SUCC(ret) && low <= high) {
      middle = (low + high) >> 1;
      cmp_result = 0;
      for (int64_t i = 0; OB_SUCC(ret) && 0 == cmp_result && i < rowkey_cnt; ++i) {
        if (OB_FAIL(decoders_[i].quick_compare(
                    datums[i], datum_utils.get_cmp_funcs().at(i), middle, cmp_result))) {
          LOG_WARN("decode and compare cell failed", K(ret), K(middle), K(i), K(datums[i]));
        }
      }

      if (OB_SUCC(ret)) {
        if (cmp_result < 0) {
          high = middle - 1;
        } else if (cmp_result > 0) {
          low = middle + 1;
        } else {
          found = true;
          row_id = middle;
          break;
        }
      }
    }
  }
  return ret;
}

int ObCSEncodeBlockGetReader::init(const ObMicroBlockData &block_data,
                                   const int64_t schema_rowkey_cnt,
                                   const ObColDescIArray &cols_desc,
                                   const int64_t request_cnt)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!block_data.is_valid()) || request_cnt <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("argument is invalid", K(ret), K(block_data), K(request_cnt));
  } else if (OB_FAIL(prepare(MAX(schema_rowkey_cnt, request_cnt)))) {
    LOG_WARN("prepare fail", K(ret), K(request_cnt), K(schema_rowkey_cnt));
  } else {
    int64_t idx = 0;
    for (idx = 0; idx < schema_rowkey_cnt; idx++) {
      store_id_array_[idx] = idx;
      column_type_array_[idx] = cols_desc.at(idx).col_type_;
    }
    for (; idx < request_cnt; ++idx) {
      store_id_array_[idx] = idx;
      column_type_array_[idx] = cols_desc.at(idx).col_type_;
    }
    if (OB_FAIL(transform_helper_.init(&decoder_allocator_, block_data, store_id_array_, request_cnt))) {
      LOG_WARN("fail to init transform helper", K(ret));
    } else if (OB_FAIL(do_init(block_data, request_cnt))) {
      LOG_WARN("failed to do init", K(ret), K(block_data), K(request_cnt));
    }
  }
  return ret;
}

int ObCSEncodeBlockGetReader::exist_row(const ObMicroBlockData &block_data,
  const ObDatumRowkey &rowkey, const ObITableReadInfo &read_info, bool &exist, bool &found)
{
  int ret = OB_SUCCESS;
  found = false;
  exist = false;
  const char *row_data = NULL;
  int64_t row_id = -1;
  const int64_t rowkey_cnt = rowkey.get_datum_cnt();

  reuse();
  if (OB_UNLIKELY(!read_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret), K(read_info));
  } else if (OB_FAIL(init(block_data,
                          read_info.get_schema_rowkey_count(),
                          read_info.get_columns_desc(),
                          rowkey_cnt))) {
    LOG_WARN("failed to do inner init", K(ret), K(block_data), K(read_info));
  } else if (OB_FAIL(locate_row(rowkey, read_info.get_datum_utils(), row_id, found))) {
    LOG_WARN("failed to locate row", K(ret), K(rowkey));
  } else if (found) {
    exist = true;
  }
  return ret;
}

int ObCSEncodeBlockGetReader::get_row(
    const ObMicroBlockAddr &block_addr,
    const ObMicroBlockData &block_data,
    const ObITableReadInfo &read_info,
    const uint32_t row_idx,
    ObDatumRow &row)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(init_if_need(block_addr, block_data, read_info))) {
    LOG_WARN("failed to do inner init", K(ret), K(block_addr), K(block_data), K(read_info));
  } else if (OB_UNLIKELY(row_idx >= transform_helper_.get_micro_block_header()->row_count_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected row index", K(ret), K(row_idx), K(transform_helper_.get_micro_block_header()->row_count_));
  } else {
    row.row_flag_.reset();
    row.mvcc_row_flag_.reset();
    if (OB_FAIL(get_all_columns(row_idx, row))) {
      LOG_WARN("Failed to get left columns", K(ret), K(row_idx));
    } else {
      row.count_ = request_cnt_;
      row.row_flag_.set_flag(ObDmlFlag::DF_INSERT);
    }
  }
  LOG_DEBUG("ObCSEncodeBlockGetReader get_row", K(ret), K(row_idx), K(read_info));
  return ret;
}

int ObCSEncodeBlockGetReader::get_row_id(
    const ObMicroBlockAddr &block_addr,
    const ObMicroBlockData &block_data,
    const ObDatumRowkey &rowkey,
    const ObITableReadInfo &read_info,
    int64_t &row_id)
{
  int ret = OB_SUCCESS;
  bool found = false;

  if (OB_FAIL(init_if_need(block_addr, block_data, read_info))) {
    LOG_WARN("failed to do inner init", K(ret), K(block_addr), K(block_data), K(read_info));
  } else if (OB_FAIL(locate_row(rowkey, read_info.get_datum_utils(), row_id, found))) {
    LOG_WARN("failed to locate row", K(ret), K(rowkey));
  } else if (!found) {
    ret = OB_BEYOND_THE_RANGE;
  }
  return ret;
}

//=============================ObMicroBlockCSDecoder===================================//
ObMicroBlockCSDecoder::ObMicroBlockCSDecoder()
  : request_cnt_(0), cached_decoder_(nullptr), decoders_(nullptr),
    transform_helper_(), column_count_(0), ctxs_(nullptr),
    decoder_allocator_(ObModIds::OB_DECODER_CTX),
    transform_allocator_(SET_IGNORE_MEM_VERSION(ObMemAttr(MTL_ID(), "MICB_TRANSFORM")), OB_MALLOC_NORMAL_BLOCK_SIZE),
    buf_allocator_(SET_IGNORE_MEM_VERSION(ObMemAttr(MTL_ID(), "MICB_CSDECODER")), OB_MALLOC_NORMAL_BLOCK_SIZE),
    allocated_decoders_buf_(nullptr),
    allocated_decoders_buf_size_(0),
    semistruct_decode_ctx_()
{
  reader_type_ = CSDecoder;
}

ObMicroBlockCSDecoder::~ObMicroBlockCSDecoder()
{
  reset();
}

template <typename Allocator>
int ObMicroBlockCSDecoder::acquire(
  Allocator &allocator, const ObCSColumnHeader::Type type, const ObIColumnCSDecoder *&decoder)
{
  int ret = OB_SUCCESS;
  decoder = NULL;
  switch (type) {
  case ObCSColumnHeader::INTEGER: {
    ObIntegerColumnDecoder *d = NULL;
    if (OB_FAIL(allocator.alloc(d))) {
      LOG_WARN("alloc failed", K(ret));
    } else {
      decoder = d;
    }
    break;
  }
  case ObCSColumnHeader::STRING: {
    ObStringColumnDecoder *d = NULL;
    if (OB_FAIL(allocator.alloc(d))) {
      LOG_WARN("alloc failed", K(ret));
    } else {
      decoder = d;
    }
    break;
  }
  case ObCSColumnHeader::INT_DICT: {
    ObIntDictColumnDecoder *d = NULL;
    if (OB_FAIL(allocator.alloc(d))) {
      LOG_WARN("alloc failed", K(ret));
    } else {
      decoder = d;
    }
    break;
  }
  case ObCSColumnHeader::STR_DICT: {
    ObStrDictColumnDecoder *d = NULL;
    if (OB_FAIL(allocator.alloc(d))) {
      LOG_WARN("alloc failed", K(ret));
    } else {
      decoder = d;
    }
    break;
  }
  case ObCSColumnHeader::SEMISTRUCT: {
    ObSemiStructColumnDecoder *d = NULL;
    if (OB_FAIL(allocator.alloc(d))) {
      LOG_WARN("alloc failed", K(ret));
    } else {
      decoder = d;
    }
    break;
  }
  default:
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("unsupported encoding type", K(ret), K(type));
  }

  return ret;
}

// called before inited
// performance critical, do not check parameters
int ObMicroBlockCSDecoder::acquire(
    const int64_t store_idx, int64_t &decoders_buf_pos, const ObIColumnCSDecoder *&decoder)
{
  int ret = OB_SUCCESS;
  decoder = nullptr;
  if (NULL != cached_decoder_ && store_idx < cached_decoder_->count_) {
    decoder = &cached_decoder_->at(store_idx);
  } else {
    const ObCSColumnHeader &col_header = transform_helper_.get_column_header(store_idx);
    const int64_t decoder_size = cs_decoder_sizes[col_header.type_];
    if (OB_UNLIKELY(decoders_buf_pos + decoder_size > allocated_decoders_buf_size_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("decoders buf not enough", K(ret), K(decoders_buf_pos),
          K(decoder_size), K(allocated_decoders_buf_size_));
    } else if (OB_FAIL(new_decoder_funcs_[col_header.type_](
        allocated_decoders_buf_ + decoders_buf_pos, decoder))) {
      LOG_WARN("fail to new decoder", K(ret), K(store_idx), K(col_header));
    } else {
      decoders_buf_pos += decoder_size;
    }
  }
  return ret;
}

int ObMicroBlockCSDecoder::alloc_decoders_buf(const bool by_read_info, int64_t &decoders_buf_pos)
{
  int ret = OB_SUCCESS;
  int64_t size = 0;
  decoders_buf_pos = 0;

  if (OB_UNLIKELY(by_read_info && read_info_ == nullptr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null read_info", K(ret));
  } else {
    size += request_cnt_ * sizeof(ObColumnCSDecoder); // for decoders_

    int64_t decoder_ctx_cnt = nullptr == read_info_ ? column_count_ : MAX(column_count_,
        read_info_->get_schema_column_count() + storage::ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt());

    size += sizeof(ObColumnCSDecoderCtx) * decoder_ctx_cnt;  // for decoder ctxs


    const ObColumnIndexArray *cols_index = by_read_info ? &(read_info_->get_columns_index()) : nullptr;
    int64_t decoders_cnt = by_read_info ? request_cnt_ : column_count_;
    for (int64_t i = 0; i < decoders_cnt; ++i) {
      const int64_t store_idx = by_read_info ? cols_index->at(i) : i;
      const ObCSColumnHeader &col_header = transform_helper_.get_column_header(store_idx);

      if (store_idx < 0 || store_idx >= column_count_) {
        // non exist column
      } else if (NULL != cached_decoder_ && store_idx < cached_decoder_->count_) {
        // in cached_decoder_
      } else {
        size += cs_decoder_sizes[col_header.type_];
      }
    }
    if (size > allocated_decoders_buf_size_) {
      if (allocated_decoders_buf_size_ > 0) {
        allocated_decoders_buf_ = nullptr;
        allocated_decoders_buf_size_ = 0;
        buf_allocator_.reuse();
      }
      if (OB_ISNULL(allocated_decoders_buf_ = (char*)buf_allocator_.alloc(size))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc", K(ret), K(size));
      } else {
        allocated_decoders_buf_size_ = size;
      }
    }
    if (OB_SUCC(ret)) {
      decoders_ = reinterpret_cast<ObColumnCSDecoder *>(allocated_decoders_buf_);
      decoders_buf_pos += request_cnt_ * sizeof(ObColumnCSDecoder);

      ctxs_ = reinterpret_cast<ObColumnCSDecoderCtx*>(allocated_decoders_buf_ + decoders_buf_pos);
      decoders_buf_pos += sizeof(ObColumnCSDecoderCtx) * decoder_ctx_cnt;
    }
  }
  return ret;
}

void ObMicroBlockCSDecoder::inner_reset()
{
  cached_decoder_ = nullptr;
  ctxs_ = nullptr;
  column_count_ = 0;
  transform_helper_.reset();
  ObIMicroBlockReader::reset();
  decoder_allocator_.reuse();
  transform_allocator_.reuse();
  semistruct_decode_ctx_.reuse();
}

void ObMicroBlockCSDecoder::reset()
{
  inner_reset();
  request_cnt_ = 0;
  buf_allocator_.reset();
  decoder_allocator_.reset();
  transform_allocator_.reset();
  allocated_decoders_buf_ = nullptr;
  allocated_decoders_buf_size_ = 0;
  semistruct_decode_ctx_.reset();
}

int ObMicroBlockCSDecoder::init_decoders()
{
  int ret = OB_SUCCESS;
  int64_t decoders_buf_pos = 0;

  if (OB_ISNULL(read_info_) || typeid(ObRowkeyReadInfo) == typeid(*read_info_)) {
    ObObjMeta col_type;
    if (OB_UNLIKELY(column_count_ < request_cnt_ && nullptr == read_info_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("for empty read info, request cnt is invalid", KR(ret), KP(read_info_), K(request_cnt_));
    } else if (OB_FAIL(alloc_decoders_buf(false/*by_read_info*/, decoders_buf_pos))) {
      LOG_WARN("fail to alloc decoders buf", K(ret));
    } else {
      int i = 0;
      for ( ; OB_SUCC(ret) && i < column_count_; ++i) {
        col_type.set_type(static_cast<ObObjType>(transform_helper_.get_column_header(i).obj_type_));
        if (OB_FAIL(add_decoder(i, col_type, nullptr, decoders_buf_pos, decoders_[i]))) {
          LOG_WARN("add_decoder failed", K(ret), K(i), K(col_type));
        }
      }
      for ( ; OB_SUCC(ret) && i < request_cnt_; ++i) { // add nop decoder for not-exist col
        decoders_[i].decoder_ = &none_exist_column_decoder_;
        decoders_[i].ctx_ = &none_exist_column_decoder_ctx_;
      }
    }
  } else if (OB_FAIL(alloc_decoders_buf(true/*by_read_info*/, decoders_buf_pos))) {
    LOG_WARN("fail to alloc decoders buf", K(ret));
  } else {
    const ObColumnIndexArray &cols_index = read_info_->get_columns_index();
    const ObColDescIArray &cols_desc = read_info_->get_columns_desc();
    if (typeid(ObCGRowkeyReadInfo) == typeid(*read_info_) || typeid(ObCGReadInfo) == typeid(*read_info_) || read_info_->get_columns()->count() < 1) {
      FOREACH_ADD_DECODER(nullptr)
    } else {
      const ObColumnParamIArray *cols_param = read_info_->get_columns();
      FOREACH_ADD_DECODER(cols_param->at(i))
    }
  }

  return ret;
}

int ObMicroBlockCSDecoder::add_decoder(
    const int64_t store_idx,
    const ObObjMeta &obj_meta,
    const ObColumnParam *col_param,
    int64_t &decoders_buf_pos,
    ObColumnCSDecoder &dest)
{
  int ret = OB_SUCCESS;
  if (store_idx < 0) {
    dest.decoder_ = &none_exist_column_decoder_;
    dest.ctx_ = &none_exist_column_decoder_ctx_;
  } else if (store_idx >= column_count_) {  // non exist column
    dest.decoder_ = &new_column_decoder_;
    dest.ctx_ = &ctxs_[store_idx];
    dest.ctx_->fill_for_new_column(col_param, &decoder_allocator_.get_inner_allocator());
  } else {
    const ObIColumnCSDecoder *decoder = NULL;
    if (OB_FAIL(acquire(store_idx, decoders_buf_pos, decoder))) {
      LOG_WARN("acquire decoder failed", K(ret), K(store_idx));
    } else if (OB_FAIL(transform_helper_.build_column_decoder_ctx(obj_meta, store_idx, ctxs_[store_idx]))) {
      LOG_WARN("fail to build column decoder ctx", K(ret), K(store_idx));
    } else if (decoder->get_type() == ObCSColumnHeader::SEMISTRUCT && OB_FAIL(init_semistruct_decoder(decoder, ctxs_[store_idx]))) {
      LOG_WARN("init semistruct decoder fail", K(ret), K(store_idx), K(obj_meta));
    } else {
      dest.decoder_ = decoder;
      dest.ctx_ = &ctxs_[store_idx];
    }
  }

  return ret;
}

int ObMicroBlockCSDecoder::init_semistruct_decoder(const ObIColumnCSDecoder *decoder, ObColumnCSDecoderCtx &decoder_ctx)
{
  int ret = OB_SUCCESS;
  ObSemiStructColumnDecoderCtx &semistruct_ctx = decoder_ctx.semistruct_ctx_;
  if (OB_FAIL(semistruct_decode_ctx_.build_semistruct_ctx(semistruct_ctx))){
    LOG_WARN("build semistruct handler fail", K(ret), K(decoder_ctx));
  }
  return ret;
}

int ObMicroBlockCSDecoder::init(
  const ObMicroBlockData &block_data, const ObITableReadInfo &read_info)
{
  int ret = OB_SUCCESS;
  // can be init twice
  if (OB_UNLIKELY(!block_data.is_valid() || !read_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(block_data), K(read_info));
  } else {
    if (OB_SUCC(ret)) {
      if (is_inited_) {
        inner_reset();
      }
    }
    read_info_ = &read_info;
    datum_utils_ = &(read_info_->get_datum_utils());
    request_cnt_ = read_info.get_request_count();

    if (OB_FAIL(do_init(block_data))) {
      LOG_WARN("do init failed", K(ret));
    }

    LOG_DEBUG("init ObMicroBlockCSDecoder", K(ret), K(block_data), K(read_info));
  }

  return ret;
}

int ObMicroBlockCSDecoder::init(
    const ObMicroBlockData &block_data,
    const ObStorageDatumUtils *datum_utils)
{
  int ret = OB_SUCCESS;
  // can be init twice
  if (OB_UNLIKELY(block_data.get_buf_size() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(block_data));
  } else {
    if (is_inited_) {
      inner_reset();
    }
    read_info_ = nullptr;
    datum_utils_ = datum_utils;
    if (OB_FAIL(do_init(block_data))) {
      LOG_WARN("do init failed", K(ret));
    }
  }
  return ret;
}

int ObMicroBlockCSDecoder::do_init(const ObMicroBlockData &block_data)
{
  int ret = OB_SUCCESS;
  const int32_t *data = nullptr;
  int32_t count = 0;
  if (nullptr != read_info_) {
    const ObColumnIndexArray &cols_index = read_info_->get_columns_index();
    if (!cols_index.rowkey_mode_) {
      data = cols_index.array_.get_data();
      count = cols_index.array_.count();
    }
  }
  if (OB_FAIL(transform_helper_.init(&transform_allocator_, block_data, data, count))) {
    LOG_WARN("fail to init transform helper", K(ret));
  } else {
    column_count_ = transform_helper_.get_micro_block_header()->column_count_;
    if (nullptr == read_info_) {
      request_cnt_ = column_count_;
    } else {
      request_cnt_ = read_info_->get_request_count();
    }
    row_count_ = transform_helper_.get_micro_block_header()->row_count_;
    if (block_data.type_ == ObMicroBlockData::Type::DATA_BLOCK &&
      NULL != block_data.get_extra_buf() && block_data.get_extra_size() > 0) {
      cached_decoder_ =
        reinterpret_cast<const ObBlockCachedCSDecoderHeader *>(block_data.get_extra_buf());
    }

    if (OB_FAIL(init_decoders())) {
      LOG_WARN("init decoders failed", K(ret));
    } else {
      is_inited_ = true;
    }
  }

  if (OB_FAIL(ret)) {
    reset();
  }
  return ret;
}

int ObMicroBlockCSDecoder::decode_cells(
  const uint64_t row_id, const int64_t col_begin, const int64_t col_end, ObStorageDatum *datums)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(col_begin < 0 || col_begin > col_end || col_end > request_cnt_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(col_begin), K(col_end), K_(request_cnt));
  } else {
    for (int64_t i = col_begin; OB_SUCC(ret) && i < col_end; ++i) {
      datums[i].reuse();
      if (OB_FAIL(decoders_[i].decode(row_id, datums[i]))) {
        LOG_WARN("decode cell failed", K(ret), K(row_id), K(i));
      }
    }
  }
  return ret;
}

int ObMicroBlockCSDecoder::get_row(const int64_t index, ObDatumRow &row)
{
  int ret = OB_SUCCESS;
  decoder_allocator_.reuse();
  if (OB_FAIL(get_row_impl(index, row))) {
    LOG_WARN("fail to get row", K(ret), K(index));
  }
  return ret;
}

OB_INLINE int ObMicroBlockCSDecoder::get_row_impl(int64_t index, ObDatumRow &row)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(index >= row_count_ || !row.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument to get row", K(ret), K(index), K_(row_count), K(row));
  } else if (row.get_capacity() < request_cnt_) {
    ret = OB_BUF_NOT_ENOUGH;
    LOG_WARN("obj buf is not enough", K(ret), "expect_obj_count", request_cnt_, K(row));
  } else if (OB_FAIL(decode_cells(index, 0, request_cnt_, row.storage_datums_))) {
    LOG_WARN("decode cells failed", K(ret), K(index), K_(request_cnt));
  } else {
    row.row_flag_.set_flag(ObDmlFlag::DF_INSERT);
    row.count_ = request_cnt_;
    row.mvcc_row_flag_.reset();
    row.fast_filter_skipped_ = false;
  }
  return ret;
}

const ObRowHeader ObMicroBlockCSDecoder::init_major_store_row_header()
{
  ObRowHeader rh;
  rh.set_row_flag(ObDmlFlag::DF_INSERT);
  return rh;
}
int ObMicroBlockCSDecoder::compare_rowkey(
  const ObDatumRowkey &rowkey, const int64_t index, int32_t &compare_result)
{
  int ret = OB_SUCCESS;
  compare_result = 0;
  if (OB_UNLIKELY(index >= row_count_ || nullptr == datum_utils_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(index), K_(row_count), KPC_(datum_utils));
  } else {
    const ObStorageDatumUtils &datum_utils = *datum_utils_;
    int64_t compare_column_count = rowkey.get_datum_cnt();
    if (OB_UNLIKELY(datum_utils.get_rowkey_count() < compare_column_count)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "Unexpected datum utils to compare rowkey", K(ret), K(compare_column_count),
        K(datum_utils));
    } else {
      ObStorageDatum store_datum;
      for (int64_t i = 0; OB_SUCC(ret) && i < compare_column_count && 0 == compare_result; ++i) {
        // before calling decode, datum ptr should point to the local buffer
        store_datum.reuse();
        if (OB_FAIL((decoders_ + i)->decode(index, store_datum))) {
          LOG_WARN("fail to decode datum", K(ret), K(index), K(i));
        } else if (OB_FAIL(datum_utils.get_cmp_funcs().at(i).compare(
                     store_datum, rowkey.datums_[i], compare_result))) {
          STORAGE_LOG(WARN, "Failed to compare datums", K(ret), K(i), K(store_datum), K(rowkey));
        }
      }
    }
  }
  return ret;
}
int ObMicroBlockCSDecoder::compare_rowkey(const ObDatumRange &range,
                                          const int64_t index,
                                          int32_t &start_key_compare_result,
                                          int32_t &end_key_compare_result)
{
  int ret = OB_SUCCESS;
  start_key_compare_result = 0;
  end_key_compare_result = 0;
  const ObDatumRowkey &start_rowkey = range.get_start_key();
  const ObDatumRowkey &end_rowkey = range.get_end_key();
  if (OB_UNLIKELY(index >= row_count_ || nullptr == datum_utils_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(index), K_(row_count), KPC_(datum_utils));
  } else {
    const ObStorageDatumUtils &datum_utils = *datum_utils_;
    int64_t compare_column_count = start_rowkey.get_datum_cnt();
    if (OB_UNLIKELY(datum_utils.get_rowkey_count() < compare_column_count)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "Unexpected datum utils to compare rowkey", K(ret), K(compare_column_count),
        K(datum_utils));
    } else {
      ObStorageDatum store_datum;
      for (int64_t i = 0; OB_SUCC(ret) && i < compare_column_count && 0 == start_key_compare_result;
           ++i) {
        // before calling decode, datum ptr should point to the local buffer
        store_datum.reuse();
        if (OB_FAIL((decoders_ + i)->decode(index, store_datum))) {
          LOG_WARN("fail to decode datum", K(ret), K(index), K(i));
        } else if (OB_FAIL(datum_utils.get_cmp_funcs().at(i).compare(
                     store_datum, start_rowkey.datums_[i], start_key_compare_result))) {
          STORAGE_LOG(
            WARN, "Failed to compare datums", K(ret), K(i), K(store_datum), K(start_rowkey));
        } else {
          if (start_key_compare_result >= 0 && 0 == end_key_compare_result) {
            if (OB_FAIL(datum_utils.get_cmp_funcs().at(i).compare(
                  store_datum, end_rowkey.datums_[i], end_key_compare_result))) {
              STORAGE_LOG(
                WARN, "Failed to compare datums", K(ret), K(i), K(store_datum), K(end_rowkey));
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObMicroBlockCSDecoder::get_decoder_cache_size(
  const char *block, const int64_t block_size, int64_t &size)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == block || block_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(block), K(block_size));
  } else {
    const ObMicroBlockHeader *header = reinterpret_cast<const ObMicroBlockHeader *>(block);
    const ObCSColumnHeader *col_header = reinterpret_cast<const ObCSColumnHeader *>(
      block + header->header_size_ + sizeof(ObAllColumnHeader));
    size = sizeof(ObBlockCachedCSDecoderHeader);
    const int64_t offset_size = sizeof(ObBlockCachedCSDecoderHeader::Col);
    for (int64_t i = 0; size < MAX_CACHED_DECODER_BUF_SIZE && i < header->column_count_; i++) {
      if (size + offset_size + cs_decoder_sizes[col_header[i].type_] >
        MAX_CACHED_DECODER_BUF_SIZE) {
        break;
      }
      size += offset_size + cs_decoder_sizes[col_header[i].type_];
    }
  }
  return ret;
}

int ObMicroBlockCSDecoder::cache_decoders(char *buf,
    const int64_t size, const char *block, const int64_t block_size)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == buf || size <= sizeof(ObBlockCachedCSDecoderHeader) ||
        nullptr == block || block_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN(
      "invalid argument", K(ret), KP(buf), K(size), KP(block), K(block_size));
  } else {
    const ObMicroBlockHeader *header = reinterpret_cast<const ObMicroBlockHeader *>(block);
    block += header->header_size_ + sizeof(ObAllColumnHeader);
    const ObCSColumnHeader *col_header = reinterpret_cast<const ObCSColumnHeader *>(block);
    MEMSET(buf, 0, size);
    ObBlockCachedCSDecoderHeader *h = reinterpret_cast<ObBlockCachedCSDecoderHeader *>(buf);
    h->col_count_ = header->column_count_;
    int64_t used = sizeof(*h);
    int64_t offset = 0;
    for (int64_t i = 0; used < size; i++) {
      h->col_[i].offset_ = static_cast<uint16_t>(offset);
      const int64_t decoder_size = cs_decoder_sizes[col_header[i].type_];
      used += sizeof(h->col_[0]) + decoder_size;
      offset += decoder_size;
      h->count_++;
    }
    LOG_DEBUG("cache decoders", K(size), K(header->column_count_), "cached_col_cnt", h->count_);

    ObDecoderArrayAllocator allocator(reinterpret_cast<char *>(&h->col_[h->count_]));
    const ObIColumnCSDecoder *d = nullptr;
    for (int64_t i = 0; OB_SUCC(ret) && i < h->count_; ++i) {
      if (OB_FAIL(acquire(allocator, (ObCSColumnHeader::Type)col_header[i].type_, d))) {
        LOG_WARN("acquire allocator failed", K(ret), "col_header", col_header[i]);
      }
    }

  }
  return ret;
}

int ObMicroBlockCSDecoder::get_row_header(const int64_t row_idx, const ObRowHeader *&row_header)
{
  UNUSEDx(row_idx);
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    row_header = &get_major_store_row_header();
  }
  return ret;
}

int ObMicroBlockCSDecoder::get_row_count(int64_t &row_count)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    row_count = row_count_;
  }
  return ret;
}

int ObMicroBlockCSDecoder::get_multi_version_info(
    const int64_t row_idx,
    const int64_t schema_rowkey_cnt,
    const ObRowHeader *&row_header,
    int64_t &trans_version,
    int64_t &sql_sequence)
{
  UNUSEDx(row_idx, schema_rowkey_cnt, row_header, trans_version, sql_sequence);
  return OB_NOT_SUPPORTED;
}

int ObMicroBlockCSDecoder::filter_pushdown_filter(
    const sql::ObPushdownFilterExecutor *parent,
    sql::ObPushdownFilterExecutor *filter,
    const sql::PushdownFilterInfo &pd_filter_info,
    common::ObBitmap &result_bitmap)
{
  int ret = OB_SUCCESS;
  if (filter->is_filter_black_node()) {
    sql::ObBlackFilterExecutor *black_filter = static_cast<sql::ObBlackFilterExecutor *>(filter);
    // TODO 暂时不考虑vectorize
    if (OB_FAIL(filter_pushdown_filter(parent, black_filter, pd_filter_info, result_bitmap))) {
      LOG_WARN("fail to filter pushdown black filter", KR(ret));
    }
  } else if (filter->is_filter_white_node()) {
    sql::ObWhiteFilterExecutor *white_filter = static_cast<sql::ObWhiteFilterExecutor *>(filter);
    if (OB_FAIL(filter_pushdown_filter(parent, white_filter, pd_filter_info, result_bitmap))) {
      LOG_WARN("fail to filter pushdown white filter", KR(ret));
    }
  } else if (filter->is_filter_node()) { // TODO @donglou.zl 直接下压白盒子树
    sql::ObPushdownFilterExecutor **children = filter->get_childs();
    for (int64_t i = 0; OB_SUCC(ret) && (i < filter->get_child_count()); ++i) {
      if (OB_ISNULL(children[i])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Unexpected null child filter", K(ret));
      } else if (OB_FAIL(filter_pushdown_filter(filter, children[i], pd_filter_info, result_bitmap))) {
        LOG_WARN("Failed to filter micro block", K(ret), K(i), KP(children[i]));
      }
    }
  }
  return ret;
}

int ObMicroBlockCSDecoder::filter_pushdown_filter(
    const sql::ObPushdownFilterExecutor *parent,
    sql::ObPhysicalFilterExecutor &filter,
    const sql::PushdownFilterInfo &pd_filter_info,
    common::ObBitmap &result_bitmap)
{
  int ret = OB_SUCCESS;
  ObStorageDatum *datum_buf = pd_filter_info.datum_buf_;
  const int64_t col_capacity = pd_filter_info.col_capacity_;
  const storage::ObTableIterParam *param = pd_filter_info.param_;
  storage::ObTableAccessContext *context = pd_filter_info.context_;
  const bool has_lob_out_row = param->has_lob_column_out() && transform_helper_.get_micro_block_header()->has_lob_out_row();
  if (OB_UNLIKELY(pd_filter_info.start_ < 0 ||
                  pd_filter_info.start_ + pd_filter_info.count_ > row_count_ ||
                  (has_lob_out_row && nullptr == context->lob_locator_helper_))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument",
             K(ret), K(row_count_), K(pd_filter_info.start_), K(pd_filter_info.count_),
             K(has_lob_out_row), KP(context->lob_locator_helper_));
  } else if (OB_FAIL(validate_filter_info(pd_filter_info, filter, datum_buf, col_capacity,
      transform_helper_.get_micro_block_header()))) {
    LOG_WARN("Failed to validate filter info", K(ret));
  } else {
    const int64_t col_count = filter.get_col_count();
    const int64_t trans_col_idx = transform_helper_.get_micro_block_header()->rowkey_column_count_ > 0 ?
        read_info_->get_schema_rowkey_count() : INT32_MIN;
    const ObColumnIndexArray &cols_index = read_info_->get_columns_index();
    const common::ObIArray<int32_t> &col_offsets = filter.get_col_offsets(pd_filter_info.is_pd_to_cg_);
    const sql::ColumnParamFixedArray &col_params = filter.get_col_params();
    decoder_allocator_.reuse();
    int64_t row_idx = 0;
    bool need_reuse_lob_locator = false;
    for (int64_t offset = 0; OB_SUCC(ret) && offset < pd_filter_info.count_; ++offset) {
      row_idx = offset + pd_filter_info.start_;
      if (nullptr != parent && parent->can_skip_filter(offset)) {
        // skip
      } else {
        if (0 < col_count) {
          for (int64_t i = 0; OB_SUCC(ret) && i < col_count; i++) {
            ObStorageDatum &datum = datum_buf[i];
            datum.reuse();
            if (OB_FAIL(decoders_[col_offsets.at(i)].decode(row_idx, datum))) {
              LOG_WARN("decode cell failed", K(ret), K(row_idx), K(i), K(datum));
            } else if (OB_UNLIKELY(trans_col_idx == cols_index.at(col_offsets.at(i)))) {
              if (OB_FAIL(storage::reverse_trans_version_val(datum))) {
                LOG_WARN("Failed to reverse trans version val", K(ret));
              }
            } else if (nullptr == col_params.at(i) || datum.is_null()) {
            } else if (need_padding(filter.is_padding_mode(), col_params.at(i)->get_meta_type())) {
              if (OB_FAIL(storage::pad_column(col_params.at(i)->get_meta_type(),
                                              col_params.at(i)->get_accuracy(), decoder_allocator_.get_inner_allocator(), datum))) {
                LOG_WARN("Failed to pad column", K(ret), K(i), K(datum));
              }
            } else if (has_lob_out_row && col_params.at(i)->get_meta_type().is_lob_storage() && !datum.get_lob_data().in_row_) {
              if (OB_FAIL(context->lob_locator_helper_->fill_lob_locator_v2(datum, *col_params.at(i), *param, *context))) {
                LOG_WARN("Failed to fill lob loactor", K(ret), K(datum), KPC(context), KPC(param));
              } else {
                need_reuse_lob_locator = true;
              }
            }
          }
        }
        bool filtered = false;
        if (OB_SUCC(ret)) {
          if (OB_FAIL(filter.filter(datum_buf, col_count, *pd_filter_info.skip_bit_, filtered))) {
            LOG_WARN("Failed to filter row with black filter", K(ret), "datum_buf",
              common::ObArrayWrap<ObStorageDatum>(datum_buf, col_count));
          } else if (!filtered) {
            if (OB_FAIL(result_bitmap.set(offset))) {
              LOG_WARN("Failed to set result bitmap", K(ret), K(offset));
            }
          }
          if (need_reuse_lob_locator) {
            context->lob_locator_helper_->reuse();
            need_reuse_lob_locator = false;
          }
        }
      }
    }
    LOG_TRACE("[PUSHDOWN] micro block black pushdown filter row", K(ret), K(pd_filter_info),
              K(col_offsets), K(result_bitmap.popcnt()), K(result_bitmap),
              KPC(transform_helper_.get_micro_block_header()), K(filter));
  }
  return ret;
}

int ObMicroBlockCSDecoder::filter_pushdown_filter(
    const sql::ObPushdownFilterExecutor *parent,
    sql::ObWhiteFilterExecutor &filter,
    const sql::PushdownFilterInfo &pd_filter_info,
    ObBitmap &result_bitmap)
{
  int ret = OB_SUCCESS;
  int32_t col_offset = 0;
  ObColumnCSDecoder *col_cs_decoder = nullptr;
  ObStorageDatum *datum_buf = pd_filter_info.datum_buf_;
  const int64_t col_capacity = pd_filter_info.col_capacity_;
  const common::ObIArray<int32_t> &col_offsets = filter.get_col_offsets(pd_filter_info.is_pd_to_cg_);
  const sql::ColumnParamFixedArray &col_params = filter.get_col_params();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("Micro block decoder not inited", K(ret));
  } else if (OB_UNLIKELY(pd_filter_info.start_ < 0
      || pd_filter_info.start_ + pd_filter_info.count_ > row_count_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument",
        K(ret), K(row_count_), K(pd_filter_info.start_), K(pd_filter_info.count_));
  } else if (1 != filter.get_col_count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected col_ids count: not 1", KR(ret), "col_cnt", filter.get_col_count(), K(filter));
  } else if (OB_UNLIKELY(sql::WHITE_OP_MAX <= filter.get_op_type() || !result_bitmap.is_inited())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid op type or bitmap", KR(ret), K(result_bitmap.is_inited()), K(filter));
  } else if (OB_ISNULL(datum_buf) && (0 < col_capacity)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected null datum buf", KR(ret), K(datum_buf), K(col_capacity));
  } else if (FALSE_IT(col_offset = col_offsets.at(0))) {
  } else if (OB_UNLIKELY((col_offset < 0))) {
    ret = OB_INDEX_OUT_OF_RANGE;
    LOG_WARN("Filter column offset out of range", KR(ret), K_(column_count), K(col_offset));
  } else if (OB_ISNULL(col_cs_decoder = decoders_ + col_offset)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Null pointer of column decoder", KR(ret));
  } else if (OB_ISNULL(col_cs_decoder->ctx_) || OB_ISNULL(col_cs_decoder->decoder_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Null pointer of decoder ctx or decoder", KR(ret), KP(col_cs_decoder->ctx_));
  } else {
    const sql::ObWhiteFilterOperatorType op_type = filter.get_op_type();
    col_cs_decoder->ctx_->get_base_ctx().set_col_param(col_params.at(0));
    if (filter.null_param_contained()
        && (op_type != sql::WHITE_OP_NU)
        && (op_type != sql::WHITE_OP_NN)) {
    } else if (filter.is_semistruct_filter_node() && col_cs_decoder->decoder_->get_type() != ObCSColumnHeader::SEMISTRUCT) {
      sql::ObPhysicalFilterExecutor &phy_filter = filter;
      if (OB_FAIL(filter_pushdown_filter(parent, phy_filter, pd_filter_info, result_bitmap))) {
        LOG_WARN("Failed to filter micro block", K(ret), K(filter));
      }
    } else if (!transform_helper_.get_micro_block_header()->all_lob_in_row_) {
      // In the column store scenario, the pushdown row range is split by row store.
      // This means that the row store has determined that there is no out_row lob,
      // but the column store microblock is large and may contain out_row lob. In this case,
      // it is safe to use retrograde white filter, because the out_row lob will not be accessed,
      // so the comparison function which is illegal for out_row lob will not be used at the storage layer.
      if (OB_FAIL(filter_pushdown_retro(parent, filter, pd_filter_info, col_offset,
            col_params.at(0), datum_buf[0], result_bitmap))) {
        LOG_WARN("[PUSHDOWN] Retrograde path failed.", KR(ret), K(filter), KPC(col_cs_decoder->ctx_));
      }
    } else if (OB_FAIL(col_cs_decoder->decoder_->pushdown_operator(parent,
               *col_cs_decoder->ctx_, filter, pd_filter_info, result_bitmap))) {
      if (OB_LIKELY(ret == OB_NOT_SUPPORTED)) {
        LOG_TRACE(
          "[PUSHDOWN] Column specific operator failed, switch to retrograde filter pushdown",
          KR(ret), K(filter));
        // reuse result bitmap as null objs set
        result_bitmap.reuse();
        if (OB_FAIL(filter_pushdown_retro(parent, filter, pd_filter_info, col_offset,
              col_params.at(0), datum_buf[0], result_bitmap))) {
          LOG_WARN("Retrograde path failed.", KR(ret), K(filter), KPC(col_cs_decoder->ctx_));
        }
      }
    }
  }

  LOG_TRACE("[PUSHDOWN] white pushdown filter row", KR(ret), "need_padding", nullptr != col_params.at(0),
            K(pd_filter_info), K(col_offset), K(result_bitmap.popcnt()), K(result_bitmap),
            KPC(transform_helper_.get_micro_block_header()), K(filter));
  return ret;
}

/**
 * Retrograde path for filter pushdown. Scan the microblock by row and set @result_bitmap.
 * This path do not use any meta_data from microblock but ensure the pushdown logic
 *    is safe from unsupported type.
 */
int ObMicroBlockCSDecoder::filter_pushdown_retro(const sql::ObPushdownFilterExecutor *parent,
  sql::ObWhiteFilterExecutor &filter, const sql::PushdownFilterInfo &pd_filter_info,
  const int32_t col_offset, const share::schema::ObColumnParam *col_param,
  ObStorageDatum &decoded_datum, common::ObBitmap &result_bitmap)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("Micro Block decoder not inited", K(ret));
  } else if (OB_UNLIKELY(0 > col_offset)) {
    ret = OB_INDEX_OUT_OF_RANGE;
    LOG_WARN("Filter column id out of range", K(ret), K(col_offset), K(column_count_));
  } else if (OB_UNLIKELY(sql::WHITE_OP_MAX <= filter.get_op_type() || !result_bitmap.is_inited())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN(
      "Invalid operator type of Filter node", K(ret), K(filter), K(result_bitmap.is_inited()));
  } else if (filter.is_semistruct_filter_node()) {
    sql::ObPhysicalFilterExecutor &phy_filter = filter;
    if (OB_FAIL(filter_pushdown_filter(parent, phy_filter, pd_filter_info, result_bitmap))) {
      LOG_WARN("Failed to filter micro block", K(ret), K(filter));
    }
  } else {
    const sql::ObWhiteFilterOperatorType op_type = filter.get_op_type();
    decoder_allocator_.reuse();
    int64_t row_id = 0;
    for (int64_t offset = 0; OB_SUCC(ret) && offset < pd_filter_info.count_; ++offset) {
      row_id = offset + pd_filter_info.start_;
      if (nullptr != parent && parent->can_skip_filter(offset)) {
        // skip
      } else {
        const ObObjMeta &obj_meta = read_info_->get_columns_desc().at(col_offset).col_type_;
        decoded_datum.reuse();
        if (OB_FAIL(decoders_[col_offset].decode(row_id, decoded_datum))) {
          LOG_WARN("decode cell failed", K(ret), K(row_id));
        } else if (need_padding(filter.is_padding_mode(), obj_meta) &&
          OB_FAIL(storage::pad_column(
            obj_meta, col_param->get_accuracy(), decoder_allocator_.get_inner_allocator(), decoded_datum))) {
          LOG_WARN("Failed to pad column", K(ret), K(col_offset), K(row_id));
        }

        bool filtered = false;
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(filter_white_filter(filter, decoded_datum, filtered))) {
          LOG_WARN("Failed to filter row with white filter", K(ret), K(row_id), K(decoded_datum));
        } else if (!filtered && OB_FAIL(result_bitmap.set(offset))) {
          LOG_WARN("Failed to set result bitmap", K(ret), K(offset));
        }
      }
    }
  }
  LOG_TRACE("[PUSHDOWN] Retrograde when pushdown filter", K(ret), KP(col_param), K(pd_filter_info),
            K(col_offset), K(result_bitmap.popcnt()), K(result_bitmap),
            KPC(transform_helper_.get_micro_block_header()), K(filter));
  return ret;
}

int ObMicroBlockCSDecoder::filter_black_filter_batch(
    const sql::ObPushdownFilterExecutor *parent,
    sql::ObBlackFilterExecutor &filter,
    sql::PushdownFilterInfo &pd_filter_info,
    common::ObBitmap &result_bitmap,
    bool &filter_applied)
{
  int ret = OB_SUCCESS;
  filter_applied = false;
  if (OB_UNLIKELY(pd_filter_info.start_ < 0
                  || pd_filter_info.start_ + pd_filter_info.count_ > row_count_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K_(row_count), K(pd_filter_info.start_), K(pd_filter_info.count_));
  } else {
    const common::ObIArray<int32_t> &col_offsets = filter.get_col_offsets(pd_filter_info.is_pd_to_cg_);
    const sql::ColumnParamFixedArray &col_params = filter.get_col_params();
    const uint8_t column_encoding_type = decoders_[col_offsets.at(0)].ctx_->type_;
    if (1 != col_offsets.count() || ((ObCSColumnHeader::INT_DICT != column_encoding_type)
        && (ObCSColumnHeader::STR_DICT != column_encoding_type))) {
    } else {
      ObColumnCSDecoder* cs_column_decoder = decoders_ + col_offsets.at(0);
      cs_column_decoder->ctx_->get_base_ctx().set_col_param(col_params.at(0));
      if (OB_FAIL(cs_column_decoder->decoder_->pushdown_operator(parent, *cs_column_decoder->ctx_,
          filter, pd_filter_info, result_bitmap, filter_applied))) {
        LOG_WARN("fail to pushdown operator", KR(ret), K(filter));
      }
    }
  }
  return ret;
}

int ObMicroBlockCSDecoder::filter_pushdown_truncate_filter(
    const sql::ObPushdownFilterExecutor *parent,
    sql::ObPushdownFilterExecutor &filter,
    const sql::PushdownFilterInfo &pd_filter_info,
    common::ObBitmap &result_bitmap)
{
  int ret = OB_SUCCESS;
  bool filter_applied = false;
  storage::ObTableAccessContext *context = pd_filter_info.context_;
  if (OB_UNLIKELY(pd_filter_info.start_ < 0 ||
                  pd_filter_info.start_ + pd_filter_info.count_ > row_count_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret), K(row_count_), K(pd_filter_info.start_), K(pd_filter_info.count_));
  } else if (OB_UNLIKELY(!filter.is_truncate_filter_node())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected truncate filter type", K(ret), K(filter));
  } else if (OB_LIKELY(filter.is_filter_white_node())) {
    int64_t col_offset = 0;
    ObColumnCSDecoder *col_cs_decoder = nullptr;
    ObTruncateWhiteFilterExecutor *truncate_executor = static_cast<ObTruncateWhiteFilterExecutor*>(&filter);
    const common::ObIArray<int32_t> &col_idxs = truncate_executor->get_col_idxs();
    ObTruncateWhiteFilterExecutor::FilterBatchGuard filter_guard(*truncate_executor);
    if (OB_UNLIKELY(1 != col_idxs.count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected col idx count for white filter", K(ret), K(col_idxs));
    } else if (FALSE_IT(col_offset = col_idxs.at(0))) {
    } else if (OB_UNLIKELY(col_offset < 0 || col_offset >= column_count_)) {
      ret = OB_INDEX_OUT_OF_RANGE;
      LOG_WARN("column offset out of range", K(ret), K(column_count_), K(col_offset));
    } else if (FALSE_IT(col_cs_decoder = decoders_ + col_offset)) {
    } else if (OB_ISNULL(col_cs_decoder->ctx_) || OB_ISNULL(col_cs_decoder->decoder_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Null pointer of decoder ctx or decoder", KR(ret), KP(col_cs_decoder->ctx_));
    } else if (OB_FAIL(col_cs_decoder->decoder_->pushdown_operator(parent, *col_cs_decoder->ctx_, *truncate_executor,
        pd_filter_info, result_bitmap))) {
      if (OB_LIKELY(ret == OB_NOT_SUPPORTED)) {
        LOG_TRACE("[PUSHDOWN] Column specific operator failed, switch to retrograde filter pushdown", K(ret), K(filter));
        // reuse result bitmap as null objs set
        result_bitmap.reuse();
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("failed to pushdown truncate operator", K(ret), KPC(truncate_executor));
      }
    } else {
      filter_applied = true;
      if (OB_UNLIKELY(truncate_executor->need_flip())) {
        result_bitmap.bit_not();
      }
    }
    LOG_TRACE("[TRUNCATE INFO] micro block black pushdown filter row", K(ret), K(pd_filter_info),
              K(result_bitmap.popcnt()), K(result_bitmap), KPC(transform_helper_.get_micro_block_header()),
              KPC(truncate_executor), K(filter));
  }
  if (OB_SUCC(ret) && !filter_applied) {
    ObITruncateFilterExecutor *truncate_executor = nullptr;
    if (filter.is_filter_black_node()) {
      truncate_executor = static_cast<ObTruncateBlackFilterExecutor*>(&filter);
    } else {
      truncate_executor = static_cast<ObTruncateWhiteFilterExecutor*>(&filter);
    }
    ObStorageDatum *datum_buf = truncate_executor->get_tmp_datum_buffer();
    const common::ObIArray<int32_t> &col_idxs = truncate_executor->get_col_idxs();
    const int64_t col_count = col_idxs.count();
    decoder_allocator_.reuse();
    int64_t row_idx = 0;
    if (OB_UNLIKELY(col_count <= 0 || nullptr == datum_buf)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected col count", K(ret), K(col_count), KP(datum_buf));
    }
    for (int64_t offset = 0; OB_SUCC(ret) && offset < pd_filter_info.count_; ++offset) {
      row_idx = offset + pd_filter_info.start_;
      if (nullptr != parent && parent->can_skip_filter(offset)) {
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < col_count; i++) {
          ObStorageDatum &datum = datum_buf[i];
          datum.reuse();
          if (OB_FAIL(decoders_[col_idxs.at(i)].decode(row_idx, datum))) {
            LOG_WARN("decode cell failed", K(ret), K(row_idx), K(i), K(datum));
          } else if (OB_UNLIKELY(transform_helper_.get_micro_block_header()->is_trans_version_column_idx(col_idxs.at(i)))) {
            if (OB_FAIL(storage::reverse_trans_version_val(datum))) {
              LOG_WARN("Failed to reverse trans version val", K(ret));
            }
          }
        }
        if (OB_SUCC(ret)) {
          bool filtered = false;
          if (OB_FAIL(truncate_executor->filter(datum_buf, col_count, filtered))) {
            LOG_WARN("Failed to filter row with black filter", K(ret), K(row_idx));
          } else if (!filtered) {
            if (OB_FAIL(result_bitmap.set(offset))) {
              LOG_WARN("Failed to set result bitmap", K(ret), K(offset));
            }
          }
        }
      }
    }
    LOG_TRACE("[TRUNCATE INFO] micro block black pushdown filter row", K(ret), K(pd_filter_info),
              K(result_bitmap.popcnt()), K(result_bitmap), K(transform_helper_.get_micro_block_header()),
              KPC(truncate_executor), K(filter));
  }
  return ret;
}

int ObMicroBlockCSDecoder::get_rows(
    const common::ObIArray<int32_t> &cols,
    const common::ObIArray<const share::schema::ObColumnParam *> &col_params,
    const bool is_padding_mode,
    const int32_t *row_ids,
    const char **cell_datas,
    const int64_t row_cap,
    common::ObIArray<ObSqlDatumInfo> &datum_infos,
    const int64_t datum_offset)
{
  int ret = OB_SUCCESS;
  UNUSED(cell_datas);
  decoder_allocator_.reuse();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(nullptr == row_ids || cols.count() != datum_infos.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(row_ids), KP(cell_datas),
             K(cols.count()), K(datum_infos.count()));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < cols.count(); i++) {
      int32_t col_id = cols.at(i);
      common::ObDatum *col_datums = datum_infos.at(i).datum_ptr_ + datum_offset;
      if (OB_FAIL(get_col_datums(col_id, row_ids, row_cap, col_datums))) {
         LOG_WARN("fail to get col datums", K(ret), K(i), K(col_id), K(row_cap));
      } else if (nullptr != col_params.at(i) && need_padding(is_padding_mode, col_params.at(i)->get_meta_type())) {
        // need padding
        if (OB_FAIL(storage::pad_on_datums(
                    col_params.at(i)->get_accuracy(),
                    col_params.at(i)->get_meta_type().get_collation_type(),
                    decoder_allocator_.get_inner_allocator(),
                    row_cap,
                    col_datums))) {
          LOG_WARN("fail to pad on datums", K(ret), K(i), K(row_cap));
        }
      }
    }
  }
  return ret;
}

int ObMicroBlockCSDecoder::get_row_count(int32_t col_id, const int32_t *row_ids,
  const int64_t row_cap, const bool contains_null, const share::schema::ObColumnParam *col_param, int64_t &count)
{
  UNUSED(col_param);
  int ret = OB_SUCCESS;
  decoder_allocator_.reuse();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(decoders_[col_id].get_row_count(row_ids, row_cap, contains_null, count))) {
    LOG_WARN("fail to get datums from decoder", K(ret), K(col_id), K(row_cap), "row_ids",
      common::ObArrayWrap<const int32_t>(row_ids, row_cap));
  }
  return ret;
}

int ObMicroBlockCSDecoder::get_column_datum(
    const ObTableIterParam &iter_param,
    const ObTableAccessContext &context,
    const share::schema::ObColumnParam &col_param,
    const int32_t col_offset,
    const int64_t row_index,
    ObStorageDatum &datum)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObMicroBlockDecoder is not init", K(ret));
  } else if (OB_UNLIKELY(col_offset >= column_count_ || row_index >= row_count_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument to get row", K(ret), K(col_offset), K(row_index));
  } else  {
    datum.reuse();
    if (OB_FAIL(decoders_[col_offset].decode(row_index, datum))) {
      LOG_WARN("Decode cell failed", K(ret));
    } else if (col_param.get_meta_type().is_lob_storage() && !datum.is_null()
               && !datum.is_nop() && !datum.get_lob_data().in_row_) {
      if (OB_FAIL(context.lob_locator_helper_->fill_lob_locator_v2(datum, col_param, iter_param, context))) {
        LOG_WARN("Failed to fill lob loactor", K(ret), K(datum), K(context), K(iter_param));
      }
    }
  }

  return ret;
}

bool ObMicroBlockCSDecoder::can_pushdown_decoder(
    const share::schema::ObColumnParam &col_param,
    const int32_t col_offset,
    const int32_t *row_ids,
    const int64_t row_cap,
    const storage::ObAggCellBase &agg_cell)
{
  bool bret = false;
  const ObPDAggType agg_type = agg_cell.get_type();
  ObColumnCSDecoder *column_decoder = decoders_ + col_offset;
  if (OB_UNLIKELY(column_decoder->decoder_->is_new_column())) {
    bret = (PD_MIN == agg_type || PD_MAX == agg_type || PD_HLL == agg_type || PD_STR_PREFIX_MIN == agg_type || PD_STR_PREFIX_MAX == agg_type);
  } else if (FALSE_IT(bret = !((col_param.get_meta_type().is_lob_storage() && has_lob_out_row())))) {
  } else if (bret) {
    bool can_convert = false;
    bool is_consecutive = false;
    switch (column_decoder->ctx_->type_) {
      case ObCSColumnHeader::INTEGER : {
        const ObIntegerColumnDecoderCtx &integer_ctx = column_decoder->ctx_->integer_ctx_;
        bool is_col_signed = false;
        const ObObjType store_col_type = integer_ctx.col_header_->get_store_obj_type();
        is_consecutive = (nullptr == row_ids || row_cap == std::abs(row_ids[0] - row_ids[row_cap - 1]) + 1);
        can_convert = ObCSDecodingUtil::can_convert_to_integer(store_col_type, is_col_signed);
        bret = ((agg_cell.is_min_agg() || agg_cell.is_max_agg() || PD_STR_PREFIX_MIN == agg_type || PD_STR_PREFIX_MAX == agg_type) && is_consecutive && can_convert);
        break;
      }
      case ObCSColumnHeader::INT_DICT:
      case ObCSColumnHeader::STR_DICT: {
        bret = (PD_MIN == agg_type || PD_MAX == agg_type || PD_HLL == agg_type
            || PD_STR_PREFIX_MIN == agg_type || PD_STR_PREFIX_MAX == agg_type);
        break;
      }
      default: {
        bret = false;
      }
    }
    LOG_DEBUG("can pushdown decoder", K(bret), K(column_decoder->ctx_->type_),
      KP(row_ids), K(is_consecutive), K(row_cap), K(can_convert), K(agg_cell));
  }
  return bret;
}

int ObMicroBlockCSDecoder::get_aggregate_result(
    const ObTableIterParam &iter_param,
    const ObTableAccessContext &context,
    const int32_t col_offset,
    const share::schema::ObColumnParam &col_param,
    const int32_t *row_ids,
    const int64_t row_cap,
    storage::ObAggDatumBuf &agg_datum_buf,
    ObAggCell &agg_cell)
{
  int ret = OB_SUCCESS;
  decoder_allocator_.reuse();
  ObColumnCSDecoder *column_decoder = nullptr;
  if (OB_UNLIKELY(nullptr == row_ids || row_cap <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid arguments to get aggregate result", K(ret), KP(row_ids), K(row_cap));
  } else if (OB_ISNULL(column_decoder = decoders_ + col_offset)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Column decoder is null", K(ret), K(col_offset));
  } else {
    ObDatum *datum_buf = agg_datum_buf.get_datums();
    const bool can_pushdown = can_pushdown_decoder(col_param, col_offset, row_ids, row_cap, agg_cell);
    if (can_pushdown) {
      column_decoder->ctx_->is_padding_mode_ = agg_cell.is_padding_mode();
      ObPushdownRowIdCtx pd_row_id_ctx(row_ids, row_cap);
      if (OB_FAIL(column_decoder->decoder_->get_aggregate_result(
                  *column_decoder->ctx_,
                  pd_row_id_ctx,
                  agg_cell))) {
        LOG_WARN("Failed to get aggregate result", K(ret), K(col_offset));
      }
    } else {
      if (OB_FAIL(get_col_datums(col_offset, row_ids, row_cap, datum_buf))) {
        LOG_WARN("fail to get col datums", KR(ret), K(col_offset), K(row_cap));
      } else if (agg_cell.need_padding() && OB_FAIL(storage::pad_on_datums(
                                              col_param.get_accuracy(),
                                              col_param.get_meta_type().get_collation_type(),
                                              decoder_allocator_.get_inner_allocator(),
                                              row_cap,
                                              datum_buf))) {
        LOG_WARN("fail to pad on datums", K(ret), K(row_cap));
      } else if (col_param.get_meta_type().is_lob_storage() && has_lob_out_row() &&
                OB_FAIL(fill_datums_lob_locator(iter_param, context, col_param, row_cap, datum_buf))) {
        LOG_WARN("Fail to fill lob locator", K(ret));
      } else if (OB_FAIL(agg_cell.eval_batch(datum_buf, row_cap))) {  // TODO @donglou.zl can pushdown into decoder.
        LOG_WARN("Failed to eval batch", K(ret));
      }
    }
    LOG_DEBUG("get_aggregate_result", K(ret), K(can_pushdown), K(agg_cell));
  }
  return ret;
}

int ObMicroBlockCSDecoder::get_aggregate_result(
    const int32_t col_offset,
    const ObPushdownRowIdCtx &pd_row_id_ctx,
    ObAggCellVec &agg_cell)
{
  int ret = OB_SUCCESS;
  decoder_allocator_.reuse();
  ObColumnCSDecoder *column_decoder = nullptr;
  if (OB_UNLIKELY(!pd_row_id_ctx.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid arguments to get aggregate result", K(ret), K(pd_row_id_ctx));
  } else if (OB_ISNULL(column_decoder = decoders_ + col_offset)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Column decoder is null", K(ret), K(col_offset));
  } else if (FALSE_IT(column_decoder->ctx_->is_padding_mode_ = agg_cell.is_padding_mode())) {
  } else if (OB_FAIL(column_decoder->decoder_->get_aggregate_result(
                                                *column_decoder->ctx_,
                                                pd_row_id_ctx,
                                                agg_cell))) {
    LOG_WARN("Failed to get aggregate result", K(ret), K(col_offset));
  }
  return ret;
}

int ObMicroBlockCSDecoder::get_col_datums(
    int32_t col_id,
    const int32_t *row_ids,
    const int64_t row_cap,
    common::ObDatum *col_datums)
{
  int ret = OB_SUCCESS;
  if (!decoders_[col_id].decoder_->can_vectorized()) {
    // normal path
    int64_t row_id = common::OB_INVALID_INDEX;
    int64_t col_idx = nullptr == read_info_ || typeid(ObRowkeyReadInfo) == typeid(*read_info_) ? col_id : read_info_->get_columns_index().at(col_id);
    const ObObjType obj_type = static_cast<ObObjType>(transform_helper_.get_column_header(col_idx).obj_type_);
    const ObObjDatumMapType map_type = ObDatum::get_obj_datum_map_type(obj_type);
    for (int64_t idx = 0; OB_SUCC(ret) && (idx < row_cap); idx++) {
      row_id = row_ids[idx];
      ObStorageDatum storage_datum;
      if (OB_FAIL(decoders_[col_id].decode(row_id, storage_datum))) {
        LOG_WARN("fail to decode cell", KR(ret), K(idx), K(row_id));
      } else if (OB_FAIL(col_datums[idx].from_storage_datum(storage_datum, map_type))) {
        LOG_WARN("from storage datum fail", K(ret), K(idx), K(row_id), K(storage_datum));
      }
    }
  } else if (OB_FAIL(decoders_[col_id].batch_decode(row_ids, row_cap, col_datums))) {
    LOG_WARN("fail to get datums from decoder", K(ret), K(col_id), K(row_cap),
             "row_ids", common::ObArrayWrap<const int32_t>(row_ids, row_cap));
  }
  if (OB_SUCC(ret)) {
    const ObColumnIndexArray &cols_index = read_info_->get_columns_index();
    if (OB_UNLIKELY(transform_helper_.get_micro_block_header()->is_trans_version_column_idx(cols_index.at(col_id)) &&
        OB_FAIL(storage::reverse_trans_version_val(col_datums, row_cap)))) {
      LOG_WARN("Failed to reverse trans version val", K(ret));
    }
  }
  return ret;
}

int ObMicroBlockCSDecoder::get_distinct_count(const int32_t group_by_col, int64_t &distinct_cnt) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObMicroBlockDecoder is not init", K(ret));
  } else {
    ret = decoders_[group_by_col].get_distinct_count(distinct_cnt);
  }
  return ret;
}

int ObMicroBlockCSDecoder::read_distinct(
    const int32_t group_by_col,
    const char **cell_datas,
    const bool is_padding_mode,
    storage::ObGroupByCellBase &group_by_cell) const
{
  UNUSED(cell_datas);
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObMicroBlockDecoder is not init", K(ret));
  } else {
    decoders_[group_by_col].ctx_->is_padding_mode_ = is_padding_mode;
    if (OB_FAIL(decoders_[group_by_col].read_distinct(group_by_cell))) {
      LOG_WARN("Failed to read distinct", K(ret));
    }
  }
  LOG_DEBUG("[GROUP BY PUSHDOWN]", K(ret), K(group_by_cell));
  return ret;
}

int ObMicroBlockCSDecoder::read_reference(
    const int32_t group_by_col,
    const int32_t *row_ids,
    const int64_t row_cap,
    storage::ObGroupByCellBase &group_by_cell) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObMicroBlockDecoder is not init", K(ret));
  } else if (OB_FAIL(decoders_[group_by_col].read_reference(row_ids, row_cap, group_by_cell))) {
    LOG_WARN("Failed to read reference", K(ret));
  } else {
    group_by_cell.set_ref_cnt(row_cap);
  }
  LOG_DEBUG("[GROUP BY PUSHDOWN]", K(ret), K(group_by_cell));
  return ret;
}

int ObMicroBlockCSDecoder::get_group_by_aggregate_result(
    const ObTableIterParam &iter_param,
    const ObTableAccessContext &context,
    const int32_t *row_ids,
    const char **cell_datas,
    const int64_t row_cap,
    storage::ObGroupByCell &group_by_cell)
{
  UNUSED(cell_datas);
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObMicroBlockCSDecoder is not init", K(ret));
  } else {
    const int32_t group_by_col = group_by_cell.get_group_by_col_offset();
    int32_t last_agg_col_offset = INT32_MIN;
    for (int64_t i = 0; OB_SUCC(ret) && i < group_by_cell.get_agg_cells().count(); ++i) {
      storage::ObAggCell *agg_cell =  group_by_cell.get_agg_cells().at(i);
      const int32_t agg_col_offset = agg_cell->get_col_offset();
      common::ObDatum *col_datums = agg_cell->get_col_datums();
      common::ObDatum *group_by_col_datums = group_by_cell.get_group_by_col_datums();
      const share::schema::ObColumnParam *col_param = agg_cell->get_col_param();
      bool need_get_col_datum = (0 == i || agg_col_offset != last_agg_col_offset) && agg_cell->need_access_data();
      if (agg_col_offset == group_by_col) {
        // agg on group by column
        if (OB_FAIL(group_by_cell.eval_batch(group_by_col_datums, row_cap, i, true))) {
          LOG_WARN("Failed to eval batch", K(ret), K(i));
        }
      } else {
        if (need_get_col_datum) {
          if (OB_FAIL(get_col_datums(agg_col_offset, row_ids, row_cap, col_datums))) {
            LOG_WARN("Failed to get col datums", K(ret), K(i), K(agg_col_offset), K(row_cap));
          } else if (agg_cell->need_padding() && OB_FAIL(storage::pad_on_datums(
                        col_param->get_accuracy(),
                        col_param->get_meta_type().get_collation_type(),
                        decoder_allocator_.get_inner_allocator(),
                        row_cap,
                        col_datums))) {
            LOG_WARN("Failed to pad col datums", K(ret), K(i), K(agg_col_offset), K(row_cap), KPC(col_param), KPC(col_datums));
          } else if (iter_param.has_lob_column_out() && has_lob_out_row()
                    && nullptr != col_param && col_param->get_meta_type().is_lob_storage()
                    && OB_FAIL(fill_datums_lob_locator(iter_param, context, *col_param, row_cap, col_datums, false))) {
            LOG_WARN("Failed to fill lob locator", K(ret), K(i), K(row_cap), K(has_lob_out_row()), KPC(col_param), K(iter_param), KPC(col_datums));
          }
        }
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(group_by_cell.eval_batch(col_datums, row_cap, i))) {
          LOG_WARN("Failed to eval batch", K(ret), K(i));
        } else if (need_get_col_datum) {
          last_agg_col_offset = agg_col_offset;
        }
      }
      LOG_DEBUG("[GROUP BY PUSHDOWN]", K(ret), K(i), K(group_by_col), K(agg_col_offset), K(group_by_cell), K(need_get_col_datum));
    }
  }
  return ret;
}

int ObMicroBlockCSDecoder::get_group_by_aggregate_result(
    const ObTableIterParam &iter_param,
    const ObTableAccessContext &context,
    const int32_t *row_ids,
    const char **cell_datas,
    const int64_t row_cap,
    const int64_t vec_offset,
    const common::ObIArray<blocksstable::ObStorageDatum> &default_datums,
    uint32_t *len_array,
    sql::ObEvalCtx &eval_ctx,
    storage::ObGroupByCellVec &group_by_cell,
    const bool enable_rich_format)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObMicroBlockDecoder is not init", K(ret));
  } else {
    const int32_t group_by_col = group_by_cell.get_group_by_col_offset();
    int32_t last_agg_col_offset = INT32_MIN;
    for (int64_t i = 0; OB_SUCC(ret) && i < group_by_cell.get_agg_cells().count(); ++i) {
      storage::ObAggCellVec *agg_cell =  group_by_cell.get_sorted_cell(i);
      const int32_t agg_col_offset = agg_cell->get_col_offset();
      bool need_get_col = (0 == i || agg_col_offset != last_agg_col_offset) && agg_cell->need_get_row_ids();
      if (agg_col_offset == group_by_col) {
        // agg on group by column
        ObDatum *datums = static_cast<ObStorageDatum *>(group_by_cell.get_group_by_col_datums());
        if (OB_FAIL(group_by_cell.eval_batch(datums, row_cap, i, true))) {
          LOG_WARN("Failed to eval batch", K(ret), K(i));
        }
      } else {
        sql::ObExpr &expr = *(agg_cell->get_project_expr());
        const bool need_padding = agg_cell->need_padding();
        const share::schema::ObColumnParam *col_param = agg_cell->get_col_param();
        if (need_get_col) {
          if (enable_rich_format) {
            if (0 == vec_offset) {
              const VectorFormat format = need_padding ? VectorFormat::VEC_DISCRETE : expr.get_default_res_format();
              if (OB_FAIL(storage::init_expr_vector_header(expr, eval_ctx, eval_ctx.max_batch_size_, format))) {
                LOG_WARN("Failed to init vector", K(ret));
              }
            }
            if (OB_SUCC(ret)) {
              ObVectorDecodeCtx vector_decode_ctx(
                  cell_datas, len_array, row_ids, row_cap, vec_offset, expr.get_vector_header(eval_ctx));
              if (decoders_[agg_col_offset].decoder_->is_new_column() && OB_FAIL(agg_cell->get_def_datum(vector_decode_ctx.default_datum_))) {
                LOG_WARN("Failed to get default datum for new column", K(ret), K(vector_decode_ctx));
              } else if (OB_FAIL(get_col_data(agg_col_offset, vector_decode_ctx))) {
                LOG_WARN("Failed to get col datums", K(ret), K(i), K(agg_col_offset), K(vector_decode_ctx));
              } else if (need_padding && OB_FAIL(storage::pad_on_rich_format_columns(
                  col_param->get_accuracy(),
                  col_param->get_meta_type().get_collation_type(),
                  row_cap,
                  vec_offset,
                  decoder_allocator_.get_inner_allocator(),
                  expr,
                  eval_ctx))) {
                LOG_WARN("Failed pad on rich format columns", K(ret), K(expr));
            } else if (iter_param.has_lob_column_out() && has_lob_out_row()
                       && nullptr != col_param && col_param->get_meta_type().is_lob_storage()
                       && OB_FAIL(fill_exprs_lob_locator(iter_param, context, *col_param, expr, eval_ctx, vec_offset, row_cap))) {
              LOG_WARN("Failed to fill lob locator", K(ret), K(i), K(vec_offset), K(row_cap), K(has_lob_out_row()), KPC(col_param), K(iter_param));
              }
            }
          } else {
            common::ObDatum *col_datums = expr.locate_batch_datums(eval_ctx);
            if (OB_ISNULL(col_datums)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("Unexpected null col datums", K(ret), K(expr));
            } else if (OB_FAIL(get_col_datums(agg_col_offset, row_ids, row_cap, col_datums))) {
              LOG_WARN("Failed to get col datums", K(ret), K(i), K(agg_col_offset), K(row_cap));
            } else if (need_padding && OB_FAIL(storage::pad_on_datums(
                col_param->get_accuracy(),
                col_param->get_meta_type().get_collation_type(),
                decoder_allocator_.get_inner_allocator(),
                row_cap,
                col_datums))) {
              LOG_WARN("Failed pad on datums", K(ret), K(row_cap), KPC(col_datums), KPC(col_param));
            } else if (iter_param.has_lob_column_out() && has_lob_out_row()
                       && nullptr != col_param && col_param->get_meta_type().is_lob_storage()
                       && OB_FAIL(fill_datums_lob_locator(iter_param, context, *col_param, row_cap, col_datums, false))) {
              LOG_WARN("Failed to fill lob locator", K(ret), K(i), K(row_cap), K(has_lob_out_row()), KPC(col_param), K(iter_param), KPC(col_datums));
            }
          }
        }
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(group_by_cell.eval_batch(nullptr, row_cap, i, false))) {
          LOG_WARN("Failed to eval batch", K(ret), K(i));
        } else if (need_get_col) {
          last_agg_col_offset = agg_col_offset;
        }
      }
      LOG_DEBUG("[GROUP BY PUSHDOWN]", K(ret), K(i), K(group_by_col), K(agg_col_offset), K(group_by_cell), K(need_get_col));
    }
  }
  return ret;
}

int ObMicroBlockCSDecoder::get_rows(
    const common::ObIArray<int32_t> &cols,
    const common::ObIArray<const share::schema::ObColumnParam *> &col_params,
    const common::ObIArray<blocksstable::ObStorageDatum> *default_datums,
    const bool is_padding_mode,
    const int32_t *row_ids,
    const int64_t row_cap,
    const char **cell_datas,
    const int64_t vec_offset,
    uint32_t *len_array,
    sql::ObEvalCtx &eval_ctx,
    sql::ObExprPtrIArray &exprs,
    const bool need_init_vector)
{
  int ret = OB_SUCCESS;
  decoder_allocator_.reuse();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(nullptr == row_ids || cols.count() != exprs.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(row_ids), KP(cell_datas),
             K(cols.count()), K(exprs.count()));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < cols.count(); i++) {
      const int32_t col_id = cols.at(i);
      sql::ObExpr &expr = *(exprs.at(i));
      const bool is_need_padding = nullptr != col_params.at(i) && need_padding(is_padding_mode, col_params.at(i)->get_meta_type());
      if (0 == vec_offset) {
        if (need_init_vector) {
          const VectorFormat format = (is_need_padding) ? VectorFormat::VEC_DISCRETE : expr.get_default_res_format();
          if (OB_FAIL(storage::init_expr_vector_header(expr, eval_ctx, eval_ctx.max_batch_size_, format))) {
            LOG_WARN("Failed to init vector", K(ret));
          }
        } else {
          expr.set_all_not_null(eval_ctx, eval_ctx.max_batch_size_);
        }
      }
      if (OB_SUCC(ret)) {
        ObVectorDecodeCtx vector_decode_ctx(
          cell_datas, len_array, row_ids, row_cap, vec_offset, expr.get_vector_header(eval_ctx));
        if (OB_NOT_NULL(default_datums)) {
          vector_decode_ctx.set_default_datum(default_datums->at(i));
        }
        if (OB_FAIL(get_col_data(col_id, vector_decode_ctx))) {
          LOG_WARN("Failed to get col datums", K(ret), K(i), K(col_id), K(vector_decode_ctx));
        } else if (is_need_padding && OB_FAIL(storage::pad_on_rich_format_columns(
            col_params.at(i)->get_accuracy(),
            col_params.at(i)->get_meta_type().get_collation_type(),
            row_cap,
            vec_offset,
            decoder_allocator_.get_inner_allocator(),
            expr,
            eval_ctx))) {
          LOG_WARN("Failed pad on rich format columns", K(ret), K(expr));
        }
      }
    }
  }
  return ret;
}

int ObMicroBlockCSDecoder::get_col_data(const int32_t col_id, ObVectorDecodeCtx &vector_ctx)
{
  int ret = OB_SUCCESS;
  const ObColumnIndexArray &cols_index = read_info_->get_columns_index();
  if (OB_FAIL(decoders_[col_id].decode_vector(vector_ctx))) {
    LOG_WARN("fail to get datums from decoder", K(ret),  K(column_count_), K(col_id), K(vector_ctx));
  } else if (OB_UNLIKELY(transform_helper_.get_micro_block_header()->is_trans_version_column_idx(cols_index.at(col_id))) &&
             OB_FAIL(storage::reverse_trans_version_val(vector_ctx.get_vector(), vector_ctx.row_cap_))) {
     LOG_WARN("Failed to reverse trans version val", K(ret));
  }
  return ret;
}

int ObSemiStructDecodeCtx::build_semistruct_ctx(ObSemiStructColumnDecoderCtx &semistruct_ctx)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(build_decode_handler(semistruct_ctx))) {
    LOG_WARN("build_decode_handler fail", K(ret));
  } else if (OB_FAIL(build_sub_col_decoder(semistruct_ctx))) {
     LOG_WARN("build_sub_col_decoder fail", K(ret));
  }
  return ret;
}

int ObSemiStructDecodeCtx::build_decode_handler(ObSemiStructColumnDecoderCtx &semistruct_ctx)
{
  int ret = OB_SUCCESS;
  ObSemiStructDecodeHandler* handler = nullptr;
  if (OB_ISNULL(handler = OB_NEWx(ObSemiStructDecodeHandler, &allocator_, allocator_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc semsitruct decode handler fail", K(ret), "size", sizeof(ObSemiStructDecodeHandler));
  } else if (OB_FAIL(handlers_.push_back(handler))) {
    LOG_WARN("push back handler fail", K(ret));
  } else if (OB_FAIL(handler->init(allocator_, semistruct_ctx.sub_schema_data_ptr_, semistruct_ctx.semistruct_header_->schema_len_))) {
    LOG_WARN("deserialize sub schema fail", K(ret), KPC(semistruct_ctx.semistruct_header_), K(semistruct_ctx));
  } else {
    semistruct_ctx.handler_ = handler;
  }
  return ret;
}

int ObSemiStructDecodeCtx::build_sub_col_decoder(ObSemiStructColumnDecoderCtx &semistruct_ctx)
{
  int ret = OB_SUCCESS;
  int64_t sub_column_cnt = semistruct_ctx.semistruct_header_->column_cnt_;
  char *allocated_decoders_buf = nullptr;
  int64_t allocated_decoders_buf_size = 0;
  int64_t decoders_buf_pos = 0;
  if (OB_ISNULL(semistruct_ctx.sub_col_decoders_ = reinterpret_cast<const ObIColumnCSDecoder**>(allocator_.alloc(sizeof(ObIColumnCSDecoder*) * sub_column_cnt)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc sub column decoder array fail", K(ret), "size", (sizeof(ObIColumnCSDecoder*) * sub_column_cnt), K(sub_column_cnt));
  } else {
    for (int64_t i = 0; i < sub_column_cnt; ++i) {
      const ObCSColumnHeader &sub_col_header = semistruct_ctx.sub_col_headers_[i];
      allocated_decoders_buf_size += cs_decoder_sizes[sub_col_header.type_];
    }
    if (OB_ISNULL(allocated_decoders_buf = (char*)allocator_.alloc(allocated_decoders_buf_size))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc", K(ret), K(allocated_decoders_buf_size));
    }
  }
  for (int i = 0; OB_SUCC(ret) && i < sub_column_cnt; ++i) {
    const ObCSColumnHeader &sub_col_header = semistruct_ctx.sub_col_headers_[i];
    const ObIColumnCSDecoder *decoder = nullptr;
    if (OB_FAIL(new_decoder_funcs_[sub_col_header.type_](
        allocated_decoders_buf + decoders_buf_pos, decoder))) {
      LOG_WARN("fail to new decoder", K(ret), K(sub_col_header), K(decoders_buf_pos), KP(allocated_decoders_buf));
    } else {
      semistruct_ctx.sub_col_decoders_[i] = decoder;
      decoders_buf_pos += cs_decoder_sizes[sub_col_header.type_];
    }
  }
  return ret;
}
void ObSemiStructDecodeCtx::reset()
{
  for (int i = 0; i < handlers_.count(); ++i) {
    ObSemiStructDecodeHandler* handler = handlers_.at(i);
    if (OB_NOT_NULL(handler)) {
      handler->reset();
    }
  }
  handlers_.reset();
  allocator_.reset();
  reserve_memory_ = false;
}

void ObSemiStructDecodeCtx::reuse()
{
  for (int i = 0; i < handlers_.count(); ++i) {
    ObSemiStructDecodeHandler* handler = handlers_.at(i);
    if (OB_NOT_NULL(handler)) {
      handler->reset();
    }
  }
  handlers_.reuse();
  if (! reserve_memory_) allocator_.reuse();
}

}  // namespace blocksstable
}  // namespace oceanbase
