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
#include "ob_micro_block_decoder.h"
#include "storage/access/ob_pushdown_aggregate.h"
#include "storage/access/ob_pushdown_aggregate_vec.h"

namespace oceanbase
{
using namespace lib;
using namespace common;
using namespace storage;
namespace blocksstable
{
struct ObBlockCachedDecoderHeader
{
  struct Col {
    uint16_t offset_;
    int16_t ref_col_idx_;
  };
  uint16_t count_;
  uint16_t col_count_;
  Col col_[0];

  const ObIColumnDecoder &at(const int64_t idx) const
  {
    return *reinterpret_cast<const ObIColumnDecoder *>(
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

ObNoneExistColumnDecoder ObMicroBlockDecoder::none_exist_column_decoder_;
ObColumnDecoderCtx ObMicroBlockDecoder::none_exist_column_decoder_ctx_;
ObNewColumnDecoder ObMicroBlockDecoder::new_column_decoder_;

// performance critical, do not check parameters
int ObColumnDecoder::decode(common::ObDatum &datum, const int64_t row_id,
    const ObBitStream &bs, const char *data, const int64_t len)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(decoder_->decode(*ctx_, datum, row_id, bs, data, len))) {
    LOG_WARN("decode fail", K(ret), KPC(ctx_), K(row_id), K(len), KP(data), K(bs));
  }
  return ret;
}

int ObColumnDecoder::batch_decode(
    const ObIRowIndex *row_index,
    const int32_t *row_ids,
    const char **cell_datas,
    const int64_t row_cap,
    common::ObDatum *datums)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == row_index
                  || nullptr == row_ids
                  || nullptr == cell_datas
                  || nullptr == datums
                  || 0 >= row_cap)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid null row_iter or datums", K(ret), KP(row_index),
             KP(row_ids), KP(cell_datas), KP(datums), K(row_cap));
  } else if (OB_UNLIKELY(!decoder_->can_vectorized())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpect column batch_decdoe not supported.", K(ret), K(decoder_->get_type()));
  } else if (OB_FAIL(decoder_->batch_decode(
              *ctx_, row_index, row_ids, cell_datas, row_cap, datums))) {
    LOG_WARN("Failed to batch decode data to datum in column decoder", K(ret), K(*ctx_));
  } else if (OB_UNLIKELY(ctx_->is_trans_version_col()) &&
             OB_FAIL(storage::reverse_trans_version_val(datums, row_cap))) {
    LOG_WARN("Failed to reverse trans version val", K(ret));
  }

  LOG_DEBUG("[Batch decode] Batch decoded datums: ",
      K(ret), K(row_cap), K(*ctx_), K(ObArrayWrap<int32_t>(row_ids, row_cap)),
      K(ObArrayWrap<ObDatum>(datums, row_cap)));
  return ret;
}

int ObColumnDecoder::decode_vector(
    const ObIRowIndex* row_index,
    ObVectorDecodeCtx &vector_ctx)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == row_index || !vector_ctx.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid null parameter", K(ret), KP(row_index), K(vector_ctx));
  } else if (OB_FAIL(decoder_->decode_vector(*ctx_, row_index, vector_ctx))) {
    LOG_WARN("Failed to batch decode data to vector format in column decoder", K(ret), K(*ctx_));
  } else if (OB_UNLIKELY(ctx_->is_trans_version_col()) &&
             OB_FAIL(storage::reverse_trans_version_val(vector_ctx.get_vector(), vector_ctx.row_cap_))) {
     LOG_WARN("Failed to reverse trans version val", K(ret));
  }
  LOG_DEBUG("[Vector decode] Batch decoded datums: ", K(ret), K(*ctx_), K(vector_ctx));
  return ret;
}

int ObColumnDecoder::get_row_count(
    const ObIRowIndex *row_index,
    const int32_t *row_ids,
    const int64_t row_cap,
    const bool contains_null,
    int64_t &count)
{
  int ret = OB_SUCCESS;
  int64_t null_count = 0;
  if (contains_null) {
    count = row_cap;
  } else if (OB_FAIL(decoder_->get_null_count(*ctx_, row_index, row_ids, row_cap, null_count))) {
    LOG_WARN("Failed to get null count from column decoder", K(ret), K(*ctx_));
  } else {
    count = row_cap - null_count;
  }
  return ret;
}

// performance critical, do not check parameters
int ObColumnDecoder::quick_compare(const ObStorageDatum &left, const ObStorageDatumCmpFunc &cmp_func, const int64_t row_id,
    const ObBitStream &bs, const char *data, const int64_t len, int32_t &cmp_ret)
{
  int ret = OB_SUCCESS;
  ObStorageDatum right_datum;
  cmp_ret = 0;
  if (OB_FAIL(decoder_->decode(*ctx_, right_datum, row_id, bs, data, len))) {
    LOG_WARN("decode cell fail", K(ret), K(row_id), K(len), KP(data), K(bs));
  } else if (OB_FAIL(cmp_func.compare(left, right_datum, cmp_ret))) {
    STORAGE_LOG(WARN, "Failed to compare datums", K(ret), K(left), K(right_datum));
  }
  return ret;
}

typedef int (*new_decoder_func)(char *buf,
                                const ObMicroBlockHeader &header,
                                const ObColumnHeader &col_header,
                                const char *meta_data,
                                const ObIColumnDecoder *&decoder);

static new_decoder_func new_decoder_funcs_[ObColumnHeader::MAX_TYPE] =
{
    new_decoder_with_allocated_buf<ObRawDecoder>,
    new_decoder_with_allocated_buf<ObDictDecoder>,
    new_decoder_with_allocated_buf<ObRLEDecoder>,
    new_decoder_with_allocated_buf<ObConstDecoder>,
    new_decoder_with_allocated_buf<ObIntegerBaseDiffDecoder>,
    new_decoder_with_allocated_buf<ObStringDiffDecoder>,
    new_decoder_with_allocated_buf<ObHexStringDecoder>,
    new_decoder_with_allocated_buf<ObStringPrefixDecoder>,
    new_decoder_with_allocated_buf<ObColumnEqualDecoder>,
    new_decoder_with_allocated_buf<ObInterColSubStrDecoder>
};


template <class Decoder>
int new_decoder_with_allocated_buf(char *buf,
                                   const ObMicroBlockHeader &header,
                                   const ObColumnHeader &col_header,
                                   const char *meta_data,
                                   const ObIColumnDecoder *&decoder)
{
  int ret = OB_SUCCESS;
  Decoder *d = nullptr;
  if (OB_UNLIKELY(!col_header.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid column header", K(ret), K(header), K(col_header));
  } else if (OB_ISNULL(d = new(buf) Decoder())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null", K(ret), KP(buf));
  } else if (OB_FAIL(d->init(header, col_header, meta_data))) {
    LOG_WARN("init decoder failed", K(ret));
  } else {
    decoder = d;
  }
  return ret;
}

//////////////////////////ObIEncodeBlockGetReader/////////////////////////
ObNoneExistColumnDecoder ObIEncodeBlockReader::none_exist_column_decoder_;
ObColumnDecoderCtx ObIEncodeBlockReader::none_exist_column_decoder_ctx_;
ObIEncodeBlockReader::ObIEncodeBlockReader()
  : block_addr_(), request_cnt_(0),
    header_(NULL), col_header_(NULL), cached_decoder_(NULL), meta_data_(NULL), row_data_(NULL),
    var_row_index_(), fix_row_index_(), row_index_(&var_row_index_),
    decoders_(nullptr),
    default_decoders_(),
    ctxs_(NULL),
    decoder_allocator_(SET_IGNORE_MEM_VERSION(ObMemAttr(MTL_ID(), common::ObModIds::OB_DECODER_CTX)), OB_MALLOC_NORMAL_BLOCK_SIZE),
    buf_allocator_(SET_IGNORE_MEM_VERSION(ObMemAttr(MTL_ID(), "OB_IENB_READER")), OB_MALLOC_NORMAL_BLOCK_SIZE),
    allocated_decoders_buf_(nullptr),
    allocated_decoders_buf_size_(0),
    store_id_array_(NULL), column_type_array_(NULL),
    default_store_ids_(), default_column_types_()
{}

ObIEncodeBlockReader::~ObIEncodeBlockReader()
{
}

void ObIEncodeBlockReader::reuse()
{
  cached_decoder_ = nullptr;
  header_ = nullptr;
  request_cnt_ = 0;
  decoders_ = nullptr;
  col_header_ = nullptr;
  meta_data_ = nullptr;
  row_data_ = nullptr;
  ctxs_ = nullptr;
  store_id_array_ = nullptr;
  column_type_array_ = nullptr;
  decoder_allocator_.reuse();
}

void ObIEncodeBlockReader::reset()
{
  cached_decoder_ = NULL;
  header_ = NULL;
  request_cnt_ = 0;
  decoders_ = nullptr;
  col_header_ = NULL;
  meta_data_ = NULL;
  row_data_ = NULL;
  ctxs_ = NULL;
  store_id_array_ = NULL;
  column_type_array_ = NULL;
  decoder_allocator_.reset();
  buf_allocator_.reset();
  allocated_decoders_buf_ = nullptr;
  allocated_decoders_buf_size_ = 0;
}

int ObIEncodeBlockReader::prepare(const int64_t column_cnt)
{
  int ret = OB_SUCCESS;
  if (column_cnt > DEFAULT_DECODER_CNT) {
    const int64_t store_ids_size = sizeof(store_id_array_[0]) * column_cnt;
    const int64_t column_types_size = ALIGN_UP(sizeof(ObObjMeta) * column_cnt, 8);
    const int64_t col_decoder_size = sizeof(ObColumnDecoder) * column_cnt;
    char *buf = nullptr;
    if (nullptr
        == (buf = reinterpret_cast<char*>(decoder_allocator_.alloc(
          store_ids_size + column_types_size + col_decoder_size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc memory for store ids fail", K(ret), K(column_cnt));
    } else {
      store_id_array_ = reinterpret_cast<int64_t *>(buf);
      column_type_array_ = reinterpret_cast<ObObjMeta *>(buf + store_ids_size);
      decoders_ =  reinterpret_cast<ObColumnDecoder *>(buf + store_ids_size + column_types_size);
    }
  } else {
    store_id_array_ = &default_store_ids_[0];
    column_type_array_ = &default_column_types_[0];
    decoders_ = &default_decoders_[0];
  }
  return ret;
}

int ObIEncodeBlockReader::do_init(
    const ObMicroBlockData &block_data,
    const int64_t request_cnt)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!block_data.is_valid()) || request_cnt <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("argument is invalid", K(ret), K(block_data), K(request_cnt));
  } else {
    request_cnt_ = request_cnt;
    row_data_ = block_data.get_buf() + header_->row_data_offset_;
    const int64_t row_data_len = block_data.get_buf_size() - header_->row_data_offset_;

    if (NULL != block_data.get_extra_buf() && block_data.get_extra_size() > 0) {
      cached_decoder_ = reinterpret_cast<const ObBlockCachedDecoderHeader *>(
          block_data.get_extra_buf());
    }

    if (header_->row_index_byte_ > 0) {
      if (OB_FAIL(var_row_index_.init(row_data_, row_data_len,
          header_->row_count_, header_->row_index_byte_))) {
        LOG_WARN("init var row index failed", K(ret), KP_(row_data), K(row_data_len), K_(header));
      }
      row_index_ = &var_row_index_;
    } else {
      if (OB_FAIL(fix_row_index_.init(row_data_, row_data_len, header_->row_count_))) {
        LOG_WARN("init fix row index failed", K(ret), KP_(row_data), K(row_data_len), K_(header));
      }
      row_index_ = &fix_row_index_;
    }

    if (OB_SUCC(ret)) {
      if (OB_UNLIKELY(!header_->is_valid())) {
        ret = OB_INNER_STAT_ERROR;
        LOG_ERROR("invalid micro_block_header", K(ret), "header", *header_);
      } else if (OB_FAIL(init_decoders())) {
        LOG_WARN("init decoders failed", K(ret));
      }
    }
  }
  if (OB_FAIL(ret)) {
    reset();
  }
  return ret;
}

int ObIEncodeBlockReader::init_decoders()
{
  int ret = OB_SUCCESS;
  int64_t decoders_buf_pos = 0;

  if (OB_UNLIKELY(NULL == store_id_array_ || NULL == column_type_array_
      || nullptr == decoders_ || NULL == header_ || NULL == col_header_)) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("header should be set while init decoders", K(ret), KP_(store_id_array),
        KP_(column_type_array), KP(decoders_), KP_(header), KP_(col_header));
  } else if (OB_FAIL(alloc_decoders_buf(decoders_buf_pos))) {
    LOG_WARN("fail to alloc decoders buf", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < request_cnt_; ++i) {
      if (OB_FAIL(add_decoder(store_id_array_[i], column_type_array_[i], decoders_buf_pos, decoders_[i]))) {
        LOG_WARN("add_decoder failed", K(ret), "request_idx", i);
      }
    }
  }
  return ret;
}

int ObIEncodeBlockReader::get_micro_metas(const ObMicroBlockHeader *&header,
    const ObColumnHeader *&col_header, const char *&meta_data,
    const char *block, const int64_t block_size)
{
  int ret = OB_SUCCESS;
  if (NULL == block || block_size <= 0) {

    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KP(block), K(block_size));
  } else {
    header = reinterpret_cast<const ObMicroBlockHeader *>(block);
    block += header->header_size_;
    col_header = reinterpret_cast<const ObColumnHeader *>(block);
    meta_data = block + sizeof(*col_header) * header->column_count_;
    if (meta_data - block > block_size
        || header->row_data_offset_ > block_size) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("block data buffer not enough",
          K(ret), KP(block), K(block_size), "meta data offset", meta_data - block, K(*header));
    }
  }
  return ret;
}

int ObIEncodeBlockReader::add_decoder(
    const int64_t store_idx, const ObObjMeta &obj_meta,
    int64_t &decoders_buf_pos, ObColumnDecoder &dest)
{
  int ret = OB_SUCCESS;
  if (store_idx >= 0 && !obj_meta.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(store_idx), K(obj_meta));
  } else {
    if (store_idx < 0 || store_idx >= header_->column_count_) { // non exist column
      dest.decoder_ = &none_exist_column_decoder_;
      dest.ctx_ = &none_exist_column_decoder_ctx_;
    } else {
      const ObIColumnDecoder *decoder = nullptr;
      const ObIColumnDecoder *ref_decoder = nullptr;
      if (OB_FAIL(acquire(store_idx, decoders_buf_pos, decoder))) {
        LOG_WARN("acquire decoder failed", K(ret), K(obj_meta), K(store_idx));
      } else {
        dest.decoder_ = decoder;
        dest.ctx_ = &ctxs_[store_idx];
        dest.ctx_->fill(obj_meta, header_, &col_header_[store_idx], &decoder_allocator_, store_idx);

        int64_t ref_col_idx = -1;
        if (NULL != cached_decoder_ && cached_decoder_->count_ > store_idx) {
          ref_col_idx = cached_decoder_->col_[store_idx].ref_col_idx_;
        } else {
          if (OB_FAIL(decoder->get_ref_col_idx(ref_col_idx))) {
            LOG_WARN("get context param failed", K(ret));
          }
        }
        if (OB_SUCC(ret) && ref_col_idx >= 0) {
          if (OB_FAIL(acquire(ref_col_idx, decoders_buf_pos, ref_decoder))) {
            LOG_WARN("acquire decoder failed", K(ret), K(obj_meta), K(ref_col_idx));
          } else {
            dest.ctx_->ref_decoder_ = ref_decoder;
            dest.ctx_->ref_ctx_ = &ctxs_[ref_col_idx];
            dest.ctx_->ref_ctx_->fill(obj_meta, header_, &col_header_[ref_col_idx], &decoder_allocator_, ref_col_idx);
          }
        }
      }
    }
  }
  return ret;
}

// called before inited
// performance critical, do not check parameters
int ObIEncodeBlockReader::acquire(const int64_t store_idx,
                                  int64_t &decoders_buf_pos,
                                  const ObIColumnDecoder *&decoder)
{
  int ret = OB_SUCCESS;
  if (NULL != cached_decoder_ && store_idx < cached_decoder_->count_) {
    decoder = &cached_decoder_->at(store_idx);
  } else {
    const int64_t decoder_size = decoder_sizes[col_header_[store_idx].type_];
    if (OB_UNLIKELY(decoders_buf_pos + decoder_size > allocated_decoders_buf_size_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("decoders buf not enough", K(ret), K(decoders_buf_pos),
          K(decoder_size), K(allocated_decoders_buf_size_), K(store_idx));
    } else if (OB_FAIL(new_decoder_funcs_[col_header_[store_idx].type_](
        allocated_decoders_buf_ + decoders_buf_pos,
        *header_, col_header_[store_idx], meta_data_, decoder))) {
      LOG_WARN("fail to new decoder", K(ret), K(store_idx), K(col_header_[store_idx]));
    } else {
      decoders_buf_pos += decoder_size;
    }
  }
  return ret;
}

int ObIEncodeBlockReader::alloc_decoders_buf(int64_t &decoders_buf_pos)
{
  int ret = OB_SUCCESS;
  int64_t size = 0;
  const int64_t decoder_ctx_cnt = MAX(request_cnt_, header_->column_count_);
  size += sizeof(ObColumnDecoderCtx) * decoder_ctx_cnt;

  for (int64_t i = 0; i < request_cnt_; ++i) {
    const int64_t store_idx = store_id_array_[i];
    if (store_idx < 0 || store_idx >= header_->column_count_) {
      // non exist column
    } else if (NULL != cached_decoder_ && store_idx < cached_decoder_->count_) {
     // ref decoder may not in cached_decoder_
     int64_t ref_col_idx = cached_decoder_->col_[store_idx].ref_col_idx_;
     if (ref_col_idx >= 0 && ref_col_idx >= cached_decoder_->count_) {
       size += MAX_DECODER_SIZE;
     }
    } else {
      size += decoder_sizes[col_header_[store_idx].type_];
      if (col_header_[store_idx].type_ == ObColumnHeader::COLUMN_EQUAL ||
          col_header_[store_idx].type_ == ObColumnHeader::COLUMN_SUBSTR) {
        // for decoder which has ref deocoder, we don't know the exact type of ref decoder,
        // so use the maximum size.
        size += MAX_DECODER_SIZE;
      }
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
    ctxs_ = reinterpret_cast<ObColumnDecoderCtx*>(allocated_decoders_buf_);
    decoders_buf_pos = sizeof(ObColumnDecoderCtx) * decoder_ctx_cnt;
  }

  return ret;
}

int ObIEncodeBlockReader::setup_row(const uint64_t row_id, int64_t &row_len, const char *&row_data)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == header_ || row_id >= header_->row_count_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(row_id), KPC_(header));
  } else if (OB_FAIL(row_index_->get(row_id, row_data, row_len))) {
    LOG_WARN("get row data failed", K(ret), K(row_id));
  }
  return ret;
}

//////////////////////////////// ObEncodeBlockReaderV2 /////////////////////////////////
void ObEncodeBlockGetReader::reuse()
{
  ObIEncodeBlockReader::reuse();
  read_info_ = nullptr;
}

int ObEncodeBlockGetReader::init_by_read_info(
    const ObMicroBlockAddr &block_addr,
    const ObMicroBlockData &block_data,
    const ObITableReadInfo &read_info)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!block_data.is_valid() ||
                  !read_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("argument is invalid", K(ret), K(block_data), K(read_info));
  } else {
    const int64_t request_cnt = read_info.get_request_count();
    if (OB_FAIL(prepare(request_cnt))) {
      LOG_WARN("prepare fail", K(ret), K(request_cnt));
    } else if (OB_FAIL(get_micro_metas(header_, col_header_, meta_data_,
        block_data.get_buf(), block_data.get_buf_size()))) {
      LOG_WARN("get micro block meta failed", K(ret), K(block_data), KPC(header_));
    } else if (typeid(ObRowkeyReadInfo) == typeid(read_info)) {
      ObObjMeta col_type;
      int64_t i = 0;
      for (; i < header_->column_count_; ++i) {
        col_type.set_type(static_cast<ObObjType>(col_header_[i].obj_type_));
        store_id_array_[i] = i;
        column_type_array_[i] = col_type;
      }
      for (; i < request_cnt; ++i) {
        col_type.set_type(ObNullType);
        store_id_array_[i] = i;
        column_type_array_[i] = col_type;
      }
    } else {
      const ObColDescIArray &cols_desc = read_info.get_columns_desc();
      const ObColumnIndexArray &cols_index = read_info.get_columns_index();
      for (int64_t idx = 0; idx < request_cnt; idx++) {
        store_id_array_[idx] = cols_index.at(idx);
        column_type_array_[idx] = cols_desc.at(idx).col_type_;
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(do_init(block_data, request_cnt))) {
      LOG_WARN("failed to do init", K(ret), K(block_data), K(request_cnt));
    } else {
      row_count_ = header_->row_count_;
      original_data_length_ = header_->original_length_;
      read_info_ = &read_info;
      // TODO: fenggu.yh, fix the logic of reuse block addr
      // block_addr_ = block_addr;
    }
  }
  return ret;
}

int ObEncodeBlockGetReader::init_if_need(const ObMicroBlockAddr &block_addr,
                                         const ObMicroBlockData &block_data,
                                         const ObITableReadInfo &read_info)
{
  int ret = OB_SUCCESS;
  // TODO: fenggu.yh, fix the logic of reuse read info
  reuse();
  if (OB_FAIL(init_by_read_info(block_addr, block_data, read_info))) {
    LOG_WARN("failed to do inner init", K(ret), K(block_addr), K(block_data), K(read_info));
  }
  return ret;
}

int ObEncodeBlockGetReader::get_row(
    const ObMicroBlockAddr &block_addr,
    const ObMicroBlockData &block_data,
    const ObDatumRowkey &rowkey,
    const ObITableReadInfo &read_info,
    ObDatumRow &row)
{
  int ret = OB_SUCCESS;
  bool found = false;
  const char *row_data = NULL;
  int64_t row_len = 0;
  int64_t row_id = -1;

  if (OB_FAIL(init_if_need(block_addr, block_data, read_info))) {
    LOG_WARN("failed to do inner init", K(ret), K(block_addr), K(block_data), K(read_info));
  } else if (OB_FAIL(locate_row(rowkey, read_info.get_datum_utils(), row_data, row_len, row_id, found))) {
    LOG_WARN("failed to locate row", K(ret), K(rowkey), K(block_addr), K(block_data));
  } else {
    row.row_flag_.reset();
    row.mvcc_row_flag_.reset();
    if (found) {
      if (OB_FAIL(get_all_columns(row_data, row_len, row_id, row))) {
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
  return ret;
}

int ObEncodeBlockGetReader::get_all_columns(
    const char *row_data,
    const int64_t row_len,
    const int64_t row_id,
    ObDatumRow &row)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(row.get_capacity() < request_cnt_)) {
    ret = OB_BUF_NOT_ENOUGH;
    LOG_WARN("datum buf is not enough", K(ret), "expect_obj_count", request_cnt_,
             "actual_datum_capacity", row.get_capacity());
  } else {
    ObBitStream bs(reinterpret_cast<unsigned char *>(const_cast<char *>(row_data)), row_len);
    for (int64_t i = 0; OB_SUCC(ret) && i < request_cnt_; ++i) {
      row.storage_datums_[i].reuse();
      if (OB_FAIL(decoders_[i].decode(row.storage_datums_[i], row_id, bs, row_data, row_len))) {
        LOG_WARN("decode cell failed", K(ret), K(row_id), K(i), KP(row_data), K(row_len));
      }
    }
  }
  return ret;
}

int ObEncodeBlockGetReader::locate_row(
    const ObDatumRowkey &rowkey,
    const ObStorageDatumUtils &datum_utils,
    const char *&row_data,
    int64_t &row_len,
    int64_t &row_id,
    bool &found)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(rowkey.get_datum_cnt() > datum_utils.get_rowkey_count())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument to locate row", K(ret), K(rowkey), K(datum_utils));
  } else {
    found = false;
    row_data = NULL;
    row_len = 0;
    row_id = -1;
    //reader_
    const int64_t rowkey_cnt = rowkey.get_datum_cnt();
    const ObStorageDatum *datums = rowkey.datums_;
    //binary search
    int32_t high = header_->row_count_ - 1;
    int32_t low = 0;
    int32_t middle = 0;
    int32_t cmp_result = 0;

    while (OB_SUCC(ret) && low <= high) {
      middle = (low + high) >> 1;
      cmp_result = 0;
      if (OB_FAIL(setup_row(middle, row_len, row_data))) {
        LOG_WARN("failed to setup row", K(ret));
      } else {
        ObBitStream bs(reinterpret_cast<unsigned char *>(const_cast<char *>(row_data)), row_len);
        for (int64_t i = 0; OB_SUCC(ret) && 0 == cmp_result && i < rowkey_cnt; ++i) {
          if (OB_FAIL(decoders_[i].quick_compare(datums[i], datum_utils.get_cmp_funcs().at(i),
                                                 middle, bs, row_data, row_len, cmp_result))) {
            LOG_WARN("decode and compare cell failed", K(ret), K(middle), K(i), KP(row_data),
                K(row_len), K(datums[i]));
          } else {
            LOG_DEBUG("encode get reader quick compare", K(decoders_[i]), K(middle), K(row_len));
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
  }

  return ret;
}

int ObEncodeBlockGetReader::init_by_columns_desc(
    const ObMicroBlockData &block_data,
    const int64_t schema_rowkey_cnt,
    const ObColDescIArray &cols_desc,
    const int64_t request_cnt)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(prepare(MAX(schema_rowkey_cnt, request_cnt)))) {
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
    if (OB_FAIL(get_micro_metas(header_, col_header_, meta_data_,
        block_data.get_buf(), block_data.get_buf_size()))) {
      LOG_WARN("get micro block meta failed", K(ret), K(block_data), KPC(header_));
    } else if (OB_FAIL(do_init(block_data, request_cnt))) {
      LOG_WARN("failed to do init", K(ret), K(block_data), K(request_cnt));
    }
  }
  return ret;
}

int ObEncodeBlockGetReader::exist_row(
    const ObMicroBlockData &block_data,
    const ObDatumRowkey &rowkey,
    const ObITableReadInfo &read_info,
    bool &exist,
    bool &found)
{
  int ret = OB_SUCCESS;
  found = false;
  exist = false;
  const char *row_data = NULL;
  int64_t row_len = 0;
  int64_t row_id = -1;
  const int64_t rowkey_cnt = rowkey.get_datum_cnt();

  reuse();
  if (OB_UNLIKELY(!read_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret), K(read_info));
  } else if (OB_FAIL(init_by_columns_desc(
              block_data,
              read_info.get_schema_rowkey_count(),
              read_info.get_columns_desc(),
              rowkey_cnt))) {
    LOG_WARN("failed to do inner init", K(ret), K(block_data), K(read_info));
  } else if (OB_FAIL(locate_row(rowkey, read_info.get_datum_utils(), row_data, row_len, row_id, found))) {
    LOG_WARN("failed to locate row", K(ret), K(rowkey));
  } else if (found) {
    exist = true;
  }
  return ret;
}

int ObEncodeBlockGetReader::get_row(
    const ObMicroBlockAddr &block_addr,
    const ObMicroBlockData &block_data,
    const ObITableReadInfo &read_info,
    const uint32_t row_idx,
    ObDatumRow &row)
{
  int ret = OB_SUCCESS;
  const char *row_data = NULL;
  int64_t row_len = 0;

  if (OB_FAIL(init_if_need(block_addr, block_data, read_info))) {
    LOG_WARN("failed to do inner init", K(ret), K(block_addr), K(block_data), K(read_info));
  } else if (OB_FAIL(setup_row(row_idx, row_len, row_data))) {
    LOG_WARN("Failed to setup row", K(ret), K(row_idx), K(block_addr), K(block_data));
  } else {
    row.row_flag_.reset();
    row.mvcc_row_flag_.reset();
    if (OB_FAIL(get_all_columns(row_data, row_len, row_idx, row))) {
      LOG_WARN("failed to get left columns", K(ret), K(row_idx), KPC_(header));
    } else {
      row.count_ = request_cnt_;
      row.row_flag_.set_flag(ObDmlFlag::DF_INSERT);
    }
  }
  return ret;
}

int ObEncodeBlockGetReader::get_row_id(
    const ObMicroBlockAddr &block_addr,
    const ObMicroBlockData &block_data,
    const ObDatumRowkey &rowkey,
    const ObITableReadInfo &read_info,
    int64_t &row_id)
{
  int ret = OB_SUCCESS;
  bool found = false;
  const char *row_data = NULL;
  int64_t row_len = 0;

  if (OB_FAIL(init_if_need(block_addr, block_data, read_info))) {
    LOG_WARN("failed to do inner init", K(ret), K(block_addr), K(block_data), K(read_info));
  } else if (OB_FAIL(locate_row(rowkey, read_info.get_datum_utils(), row_data, row_len, row_id, found))) {
    LOG_WARN("failed to locate row", K(ret), K(rowkey), K(block_addr), K(block_data));
  } else if (!found) {
    ret = OB_BEYOND_THE_RANGE;
  }
  return ret;
}

////////////////////////// ObMicroBlockDecoderV2 ////////////////////////////////

ObMicroBlockDecoder::ObMicroBlockDecoder()
  : header_(nullptr),
    request_cnt_(0),
    col_header_(nullptr),
    meta_data_(nullptr),
    row_data_(nullptr),
    var_row_index_(),
    fix_row_index_(),
    cached_decoder_(nullptr),
    row_index_(&var_row_index_),
    decoders_(nullptr),
    ctxs_(nullptr),
    decoder_allocator_(ObModIds::OB_DECODER_CTX),
    buf_allocator_(SET_IGNORE_MEM_VERSION(ObMemAttr(MTL_ID(), "OB_MICB_DECODER")), OB_MALLOC_NORMAL_BLOCK_SIZE),
    allocated_decoders_buf_(nullptr),
    allocated_decoders_buf_size_(0)
{
  reader_type_ = Decoder;
}

ObMicroBlockDecoder::~ObMicroBlockDecoder()
{
  reset();
}

template <typename Allocator>
int ObMicroBlockDecoder::acquire(
    Allocator &allocator,
    const ObMicroBlockHeader &header,
    const ObColumnHeader &col_header,
    const char *meta_data,
    const ObIColumnDecoder *&decoder)
{
  int ret = OB_SUCCESS;
  decoder = NULL;
  if (OB_UNLIKELY(!col_header.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid column header", K(ret), K(col_header));
  } else {
    switch (col_header.type_)
    {
      case ObColumnHeader::RAW: {
        ObRawDecoder *d = NULL;
        if (OB_FAIL(allocator.alloc(d))) {
          LOG_WARN("alloc failed", K(ret));
        } else if (OB_FAIL(d->init(header, col_header, meta_data))) {
          LOG_WARN("init raw decoder failed", K(ret));
        } else {
          decoder = d;
        }
        break;
      }
      case ObColumnHeader::DICT: {
        ObDictDecoder *d = NULL;
        if (OB_FAIL(allocator.alloc(d))) {
          LOG_WARN("alloc failed", K(ret));
        } else {
          if (OB_FAIL(d->init(header, col_header, meta_data))) {
            LOG_WARN("init dict decoder failed", K(ret));
          } else {
            decoder = d;
          }
        }
        break;
      }
      case ObColumnHeader::INTEGER_BASE_DIFF: {
        ObIntegerBaseDiffDecoder *d = NULL;
        if (OB_FAIL(allocator.alloc(d))) {
          LOG_WARN("alloc failed", K(ret));
        } else {
          if (OB_FAIL(d->init(header, col_header, meta_data))) {
            LOG_WARN("init integer base diff decoder failed", K(ret));
          } else {
            decoder = d;
          }
        }
        break;
      }
      case ObColumnHeader::STRING_DIFF: {
        ObStringDiffDecoder *d = NULL;
        if (OB_FAIL(allocator.alloc(d))) {
          LOG_WARN("alloc failed", K(ret));
        } else if (OB_FAIL(d->init(header, col_header, meta_data))) {
          LOG_WARN("init string diff decoder failed", K(ret));
        } else {
          decoder = d;
        }
        break;
      }
      case ObColumnHeader::HEX_PACKING: {
        ObHexStringDecoder *d = NULL;
        if (OB_FAIL(allocator.alloc(d))) {
          LOG_WARN("alloc failed", K(ret));
        } else if (OB_FAIL(d->init(header, col_header, meta_data))) {
          LOG_WARN("init hex packing decoder failed", K(ret));
        } else {
          decoder = d;
        }
        break;
      }
      case ObColumnHeader::RLE: {
        ObRLEDecoder *d = NULL;
        if (OB_FAIL(allocator.alloc(d))) {
          LOG_WARN("alloc failed", K(ret));
        } else if (OB_FAIL(d->init(header, col_header, meta_data))) {
          LOG_WARN("init rle decoder failed", K(ret));
        } else {
          decoder = d;
        }
        break;
      }
      case ObColumnHeader::CONST: {
        ObConstDecoder *d = NULL;
        if (OB_FAIL(allocator.alloc(d))) {
          LOG_WARN("alloc failed", K(ret));
        } else if (OB_FAIL(d->init(header, col_header, meta_data))) {
          LOG_WARN("init const decoder failed", K(ret));
        } else {
          decoder = d;
        }
        break;
      }
      case ObColumnHeader::STRING_PREFIX: {
        ObStringPrefixDecoder *d = NULL;
        if (OB_FAIL(allocator.alloc(d))) {
          LOG_WARN("alloc failed", K(ret));
        } else if (OB_FAIL(d->init(header, col_header, meta_data))) {
          LOG_WARN("init string prefix decoder failed", K(ret));
        } else {
          decoder = d;
        }
        break;
      }
      case ObColumnHeader::COLUMN_EQUAL: {
        ObColumnEqualDecoder *d = NULL;
        if (OB_FAIL(allocator.alloc(d))) {
          LOG_WARN("alloc failed", K(ret));
        } else if (OB_FAIL(d->init(header, col_header, meta_data))) {
          LOG_WARN("init column equal decoder failed", K(ret));
        } else {
          decoder = d;
        }
        break;
      }
      case ObColumnHeader::COLUMN_SUBSTR: {
        ObInterColSubStrDecoder *d = NULL;
        if (OB_FAIL(allocator.alloc(d))) {
          LOG_WARN("alloc failed", K(ret));
        } else if (OB_FAIL(d->init(header, col_header, meta_data))) {
          LOG_WARN("init column substr decoder failed", K(ret));
        } else {
          decoder = d;
        }
        break;
      }
      default:
        ret = OB_INNER_STAT_ERROR;
        LOG_WARN("unsupported encoding type", K(ret), "type", col_header.type_);
    }
  }
  if (OB_FAIL(ret) && NULL != decoder) {
    allocator.free(const_cast<ObIColumnDecoder *>(decoder));
    decoder = NULL;
  }
  return ret;
}

// called before inited
// performance critical, do not check parameters
int ObMicroBlockDecoder::acquire(const int64_t store_idx,
                                 int64_t &decoders_buf_pos,
                                 const ObIColumnDecoder *&decoder)
{
  int ret = OB_SUCCESS;
  decoder = nullptr;
  if (NULL != cached_decoder_ && store_idx < cached_decoder_->count_) {
    decoder = &cached_decoder_->at(store_idx);
  } else {
    const int64_t decoder_size = decoder_sizes[col_header_[store_idx].type_];
    if (OB_UNLIKELY(decoders_buf_pos + decoder_size > allocated_decoders_buf_size_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("decoders buf not enough", K(ret), K(decoders_buf_pos),
          K(decoder_size), K(allocated_decoders_buf_size_));
    } else if (OB_FAIL(new_decoder_funcs_[col_header_[store_idx].type_](
        allocated_decoders_buf_ + decoders_buf_pos,
        *header_, col_header_[store_idx], meta_data_, decoder))) {
      LOG_WARN("fail to new decoder", K(ret), K(store_idx), K(col_header_[store_idx]));
    } else {
      decoders_buf_pos += decoder_size;
    }
  }
  return ret;
}

int ObMicroBlockDecoder::alloc_decoders_buf(const bool by_read_info, int64_t &decoders_buf_pos)
{
  int ret = OB_SUCCESS;
  int64_t size = 0;
  decoders_buf_pos = 0;

  if (OB_UNLIKELY(by_read_info && read_info_ == nullptr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null read_info", K(ret));
  } else {
    size += request_cnt_ * sizeof(ObColumnDecoder); // for decoders_

    int64_t decoder_ctx_cnt = nullptr == read_info_ ? header_->column_count_ : MAX(header_->column_count_,
        read_info_->get_schema_column_count() + storage::ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt());

    size += sizeof(ObColumnDecoderCtx) * decoder_ctx_cnt;  // for decoder ctxs

    const ObColumnIndexArray *cols_index = by_read_info ? &(read_info_->get_columns_index()) : nullptr;
    int64_t decoders_cnt = by_read_info ? request_cnt_ : header_->column_count_;
    for (int64_t i = 0; i < decoders_cnt; ++i) {
      const int64_t store_idx = by_read_info ? cols_index->at(i) : i;
      if (store_idx < 0 || store_idx >= header_->column_count_) {
        // non exist column
      } else if (NULL != cached_decoder_ && store_idx < cached_decoder_->count_) {
        // ref decoder may not in cached_decoder_
        int64_t ref_col_idx = cached_decoder_->col_[store_idx].ref_col_idx_;
        if (ref_col_idx >= 0 && ref_col_idx >= cached_decoder_->count_) {
          size += MAX_DECODER_SIZE;
        }
      } else {
        size += decoder_sizes[col_header_[store_idx].type_];
        if (col_header_[store_idx].type_ == ObColumnHeader::COLUMN_EQUAL ||
            col_header_[store_idx].type_ == ObColumnHeader::COLUMN_SUBSTR) {
          // for decoder which has ref deocoder, we don't know the exact type of ref decoder,
          // so use the maximum size.
          size += MAX_DECODER_SIZE;
        }
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
      decoders_ = reinterpret_cast<ObColumnDecoder *>(allocated_decoders_buf_);
      decoders_buf_pos = request_cnt_ * sizeof(ObColumnDecoder);

      ctxs_ = reinterpret_cast<ObColumnDecoderCtx*>(allocated_decoders_buf_ + decoders_buf_pos);
      decoders_buf_pos += sizeof(ObColumnDecoderCtx) * decoder_ctx_cnt;
    }
  }
  return ret;
}

void ObMicroBlockDecoder::inner_reset()
{
  cached_decoder_ = NULL;
  header_ = NULL;
  col_header_ = NULL;
  meta_data_ = NULL;
  row_data_ = NULL;
  ObIMicroBlockReader::reset();
  decoder_allocator_.reuse();
  ctxs_ = nullptr;
}

void ObMicroBlockDecoder::reset()
{
  inner_reset();
  request_cnt_ = 0;
  buf_allocator_.reset();
  decoder_allocator_.reset();
  allocated_decoders_buf_ = nullptr;
  allocated_decoders_buf_size_ = 0;
}

int ObMicroBlockDecoder::init_decoders()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(NULL == header_ || NULL == col_header_ ||
     (NULL != read_info_ && !read_info_->is_valid()))) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("header should be set while init decoders",
             K(ret), KPC_(read_info), KP_(header), KP_(col_header));
  } else {
    // perfetch meta data
    /*
    const int64_t meta_len = row_data_ - meta_data_;
    for (int64_t i = 0; i < meta_len - CPU_CACHE_LINE_SIZE; i += CPU_CACHE_LINE_SIZE) {
      __builtin_prefetch(meta_data_ + i);
    }
    */
    int64_t decoders_buf_pos = 0;
    if (OB_ISNULL(read_info_) || typeid(ObRowkeyReadInfo) == typeid(*read_info_)) {
      ObObjMeta col_type;
      if (OB_UNLIKELY((header_->column_count_ < request_cnt_ && nullptr == read_info_) ||
                      (header_->column_count_ > request_cnt_))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("for empty read info, request cnt is invalid", KR(ret), KP(read_info_), KPC(header_), K(request_cnt_));
      } else if (OB_FAIL(alloc_decoders_buf(false/*by_read_info*/, decoders_buf_pos))) {
        LOG_WARN("fail to alloc decoders buf", K(ret));
      } else {
        int64_t i = 0;
        for ( ; OB_SUCC(ret) && i < header_->column_count_; ++i) {
          col_type.set_type(static_cast<ObObjType>(col_header_[i].obj_type_));
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
  }
  return ret;
}

int ObMicroBlockDecoder::add_decoder(
    const int64_t store_idx,
    const ObObjMeta &obj_meta,
    const ObColumnParam *col_param,
    int64_t &decoders_buf_pos,
    ObColumnDecoder &dest)
{
  int ret = OB_SUCCESS;
  if (store_idx < header_->column_count_ && !obj_meta.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(store_idx), K(obj_meta));
  } else {
    if (store_idx < 0) {
      dest.decoder_ = &none_exist_column_decoder_;
      dest.ctx_ = &none_exist_column_decoder_ctx_;
    } else if (store_idx >= header_->column_count_) { // new added column
      dest.decoder_ = &new_column_decoder_;
      dest.ctx_ = &ctxs_[store_idx];
      dest.ctx_->fill_for_new_column(col_param, &decoder_allocator_.get_inner_allocator());
    } else {
      const ObIColumnDecoder *decoder = NULL;
      if (OB_FAIL(acquire(store_idx, decoders_buf_pos, decoder))) {
        LOG_WARN("acquire decoder failed", K(ret), K(obj_meta), K(store_idx));
      } else {
        dest.decoder_ = decoder;
        dest.ctx_ = &ctxs_[store_idx];
        dest.ctx_->fill(obj_meta, header_, &col_header_[store_idx], &decoder_allocator_.get_inner_allocator(), store_idx);

        int64_t ref_col_idx = -1;
        if (NULL != cached_decoder_ && cached_decoder_->count_ > store_idx) {
          ref_col_idx = cached_decoder_->col_[store_idx].ref_col_idx_;
        } else {
          if (OB_FAIL(decoder->get_ref_col_idx(ref_col_idx))) {
            LOG_WARN("get context param failed", K(ret));
          }
        }

        if (OB_SUCC(ret) && ref_col_idx >= 0) {
          if (OB_FAIL(acquire(ref_col_idx, decoders_buf_pos, decoder))) {
            LOG_WARN("acquire decoder failed", K(ret), K(obj_meta), K(ref_col_idx));
          } else {
            dest.ctx_->ref_decoder_ = decoder;
            dest.ctx_->ref_ctx_ = &ctxs_[ref_col_idx];
            dest.ctx_->ref_ctx_->fill(obj_meta, header_, &col_header_[ref_col_idx], &decoder_allocator_.get_inner_allocator(), ref_col_idx);
          }
        }
      }
    }
  }
  return ret;
}

int ObMicroBlockDecoder::get_micro_metas(
    const ObMicroBlockHeader *&header,
    const ObColumnHeader *&col_header,
    const char *&meta_data,
    const char *block,
    const int64_t block_size)
{
  int ret = OB_SUCCESS;
  if (nullptr == block || block_size <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(block), K(block_size));
  } else {
    header = reinterpret_cast<const ObMicroBlockHeader *>(block);
    block += header->header_size_;
    col_header = reinterpret_cast<const ObColumnHeader *>(block);
    meta_data = block + sizeof(*col_header) * header->column_count_;
    if (meta_data - block > block_size || header->row_data_offset_ > block_size) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("block data buffer not enough",
               K(ret), KP(block), K(block_size), "meta data offset", meta_data - block, K(*header));
    }
  }
  return ret;
}

int ObMicroBlockDecoder::init(
    const ObMicroBlockData &block_data,
    const ObITableReadInfo &read_info)
{
  int ret = OB_SUCCESS;
  // can be init twice
  if (OB_UNLIKELY(block_data.get_buf_size() <= 0 || !read_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(block_data), K(read_info));
  } else {
    if (is_inited_) {
      inner_reset();
    }
    read_info_ = &read_info;
    datum_utils_ = &(read_info_->get_datum_utils());
    request_cnt_ = read_info.get_request_count();
    if (OB_FAIL(do_init(block_data))) {
      LOG_WARN("do init failed", K(ret));
    }
  }
  return ret;
}

int ObMicroBlockDecoder::init(
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


int ObMicroBlockDecoder::do_init(const ObMicroBlockData &block_data)
{
  int ret = OB_SUCCESS;
  int16_t col_cnt = 0;
  if (OB_FAIL(get_micro_metas(header_, col_header_, meta_data_,
                                     block_data.get_buf(), block_data.get_buf_size()))) {
    LOG_WARN("get micro block meta failed", K(ret), K(block_data));
  } else {
    if (NULL == read_info_) {
      request_cnt_ = header_->column_count_;
    } else {
      request_cnt_ = read_info_->get_request_count();
    }
    row_count_ = header_->row_count_;
    original_data_length_ = header_->original_length_;
    row_data_ = block_data.get_buf() + header_->row_data_offset_;
    const int64_t row_data_len = block_data.get_buf_size() - header_->row_data_offset_;

    if (block_data.type_ == ObMicroBlockData::Type::DATA_BLOCK
        && NULL != block_data.get_extra_buf() && block_data.get_extra_size() > 0) {
      cached_decoder_ = reinterpret_cast<const ObBlockCachedDecoderHeader *>(
          block_data.get_extra_buf());
    }

    if (header_->row_index_byte_ > 0) {
      if (OB_FAIL(var_row_index_.init(row_data_, row_data_len,
                                      header_->row_count_, header_->row_index_byte_))) {
        LOG_WARN("init var row index failed",
                 K(ret), KP_(row_data), K(row_data_len), K_(header));
      }
      row_index_ = &var_row_index_;
    } else {
      if (OB_FAIL(fix_row_index_.init(row_data_, row_data_len, header_->row_count_))) {
        LOG_WARN("init fix row index failed",
                 K(ret), KP_(row_data), K(row_data_len), K_(header));
      }
      row_index_ = &fix_row_index_;
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_UNLIKELY(!header_->is_valid())) {
      ret = OB_INNER_STAT_ERROR;
      LOG_ERROR("invalid micro_block_header", K(ret), "header", *header_);
    } else if (OB_FAIL(init_decoders())) {
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

int ObMicroBlockDecoder::decode_cells(const uint64_t row_id,
                                        const int64_t row_len,
                                        const char *row_data,
                                        const int64_t col_begin,
                                        const int64_t col_end,
                                        ObStorageDatum *datums)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(col_begin < 0 || col_begin > col_end || col_end > request_cnt_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(col_begin), K(col_end), K_(request_cnt));
  } else {
    ObBitStream bs(reinterpret_cast<unsigned char *>(const_cast<char *>(row_data)), row_len);
    for (int64_t i = col_begin; OB_SUCC(ret) && i < col_end; ++i) {
      datums[i].reuse();
      if (OB_FAIL(decoders_[i].decode(datums[i], row_id, bs, row_data, row_len))) {
        LOG_WARN("decode cell failed", K(ret), K(row_id), K(i), K(bs), KP(row_data), K(row_len));
      }
    }
  }
  return ret;
}

int ObMicroBlockDecoder::get_row(const int64_t index, ObDatumRow &row)
{
  int ret = OB_SUCCESS;
  decoder_allocator_.reuse();
  if (OB_FAIL(get_row_impl(index, row))) {
    LOG_WARN("fail to get row", K(ret), K(index));
  }
  return ret;
}

OB_INLINE int ObMicroBlockDecoder::get_row_impl(int64_t index, ObDatumRow &row)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(index >= row_count_ || !row.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument to get row", K(ret), K(index), K_(row_count), K(row));
  } else {
    int64_t row_len = 0;
    const char *row_data = NULL;
    if (OB_FAIL(row_index_->get(index, row_data, row_len))) {
      LOG_WARN("get row data failed", K(ret), K(index));
    } else if (row.get_capacity() < request_cnt_) {
      ret = OB_BUF_NOT_ENOUGH;
      LOG_WARN("obj buf is not enough", K(ret), "expect_obj_count", request_cnt_, K(row));
    } else if (OB_FAIL(decode_cells(index, row_len, row_data, 0, request_cnt_, row.storage_datums_))) {
      LOG_WARN("decode cells failed", K(ret), K(index), K_(request_cnt));
    } else {
      row.row_flag_.reset();
      row.row_flag_.set_flag(ObDmlFlag::DF_INSERT);
      row.count_ = request_cnt_;
      row.mvcc_row_flag_.reset();
      row.fast_filter_skipped_ = false;
    }
  }
  return ret;
}

const ObRowHeader ObMicroBlockDecoder::init_major_store_row_header()
{
  ObRowHeader rh;
  rh.set_row_flag(ObDmlFlag::DF_INSERT);
  return rh;
}

int ObMicroBlockDecoder::compare_rowkey(const ObDatumRowkey &rowkey, const int64_t index, int32_t &compare_result)
{
  int ret = OB_SUCCESS;
  compare_result = 0;
  int64_t row_len = 0;
  const char *row_data = nullptr;
  if (OB_UNLIKELY(index  >= row_count_ || nullptr == datum_utils_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(index), K_(row_count), KPC_(datum_utils));
  } else if (OB_FAIL(row_index_->get(index, row_data, row_len))) {
    LOG_WARN("get row data failed", K(ret), K(index));
  } else {
    const ObStorageDatumUtils &datum_utils = *datum_utils_;
    ObBitStream bs(reinterpret_cast<unsigned char *>(const_cast<char *>(row_data)), row_len);
    int64_t compare_column_count = rowkey.get_datum_cnt();
    if (OB_UNLIKELY(datum_utils.get_rowkey_count() < compare_column_count)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "Unexpected datum utils to compare rowkey", K(ret), K(compare_column_count), K(datum_utils));
    } else {
      ObStorageDatum store_datum;
      for (int64_t i = 0; OB_SUCC(ret) && i < compare_column_count && 0 == compare_result; ++i) {
        // before calling decode, datum ptr should point to the local buffer
        store_datum.reuse();
        if (OB_FAIL((decoders_ + i)->decode(store_datum, index, bs, row_data, row_len))) {
          LOG_WARN("fail to decode datum", K(ret), K(index), K(i));
        } else if (OB_FAIL(datum_utils.get_cmp_funcs().at(i).compare(store_datum, rowkey.datums_[i], compare_result))) {
          STORAGE_LOG(WARN, "Failed to compare datums", K(ret), K(i), K(store_datum), K(rowkey));
        }
      }
    }
  }
  return ret;
}

int ObMicroBlockDecoder::compare_rowkey(const ObDatumRange &range,
                                        const int64_t index,
                                        int32_t &start_key_compare_result,
                                        int32_t &end_key_compare_result)
{
  int ret = OB_SUCCESS;
  start_key_compare_result = 0;
  end_key_compare_result = 0;
  int64_t row_len = 0;
  const char *row_data = nullptr;
  const ObDatumRowkey &start_rowkey = range.get_start_key();
  const ObDatumRowkey &end_rowkey = range.get_end_key();
  if (OB_UNLIKELY(index >= row_count_ || nullptr == datum_utils_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(index), K_(row_count), KPC_(datum_utils));
  } else if (OB_FAIL(row_index_->get(index, row_data, row_len))) {
    LOG_WARN("get row data failed", K(ret), K(index));
  } else {
    const ObStorageDatumUtils &datum_utils = *datum_utils_;
    ObBitStream bs(reinterpret_cast<unsigned char *>(const_cast<char *>(row_data)), row_len);
    int64_t compare_column_count = start_rowkey.get_datum_cnt();
    if (OB_UNLIKELY(datum_utils.get_rowkey_count() < compare_column_count)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "Unexpected datum utils to compare rowkey", K(ret), K(compare_column_count), K(datum_utils));
    } else {
      ObStorageDatum store_datum;
      for (int64_t i = 0; OB_SUCC(ret) && i < compare_column_count && 0 == start_key_compare_result; ++i) {
        // before calling decode, datum ptr should point to the local buffer
        store_datum.reuse();
        if (OB_FAIL((decoders_ + i)->decode(store_datum, index, bs, row_data, row_len))) {
          LOG_WARN("fail to decode datum", K(ret), K(index), K(i));
        } else if (OB_FAIL(datum_utils.get_cmp_funcs().at(i).compare(
                store_datum, start_rowkey.datums_[i], start_key_compare_result))) {
          STORAGE_LOG(WARN, "Failed to compare datums", K(ret), K(i), K(store_datum), K(start_rowkey));
        } else {
          if (start_key_compare_result >= 0 && 0 == end_key_compare_result) {
            if (OB_FAIL(datum_utils.get_cmp_funcs().at(i).compare(
                    store_datum, end_rowkey.datums_[i], end_key_compare_result))) {
              STORAGE_LOG(WARN, "Failed to compare datums", K(ret), K(i), K(store_datum), K(end_rowkey));
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObMicroBlockDecoder::get_decoder_cache_size(
    const char *block,
    const int64_t block_size,
    int64_t &size)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == block || block_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(block), K(block_size));
  } else {
    size = sizeof(ObBlockCachedDecoderHeader);
    const ObMicroBlockHeader *header = nullptr;
    const ObColumnHeader *col_header = nullptr;
    const char *meta_data = nullptr;
    if (OB_FAIL(get_micro_metas(header, col_header, meta_data, block, block_size))) {
      LOG_WARN("get micro block meta failed", K(ret), KP(block), K(block_size));
    } else {
      const int64_t offset_size = sizeof(ObBlockCachedDecoderHeader::Col);
      for (int64_t i = 0; size < MAX_CACHED_DECODER_BUF_SIZE && i < header->column_count_; i++) {
        if (size + offset_size + decoder_sizes[col_header[i].type_]
            > MAX_CACHED_DECODER_BUF_SIZE) {
          break;
        }
        size += offset_size + decoder_sizes[col_header[i].type_];
      }
    }
  }
  return ret;
}

int ObMicroBlockDecoder::cache_decoders(
    char *buf,
    const int64_t size,
    const char *block,
    const int64_t block_size)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == buf || size <= sizeof(ObBlockCachedDecoderHeader) ||
                  nullptr == block || block_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(buf), K(size), KP(block), K(block_size));
  } else {
    const ObMicroBlockHeader *header = nullptr;
    const ObColumnHeader *col_header = nullptr;
    const char *meta_data = nullptr;
    if (OB_FAIL(get_micro_metas(header, col_header, meta_data, block, block_size))) {
      LOG_WARN("get micro block meta failed", K(ret), KP(block), K(block_size));
    } else {
      MEMSET(buf, 0, size);
      ObBlockCachedDecoderHeader *h = reinterpret_cast<ObBlockCachedDecoderHeader *>(buf);
      h->col_count_ = header->column_count_;
      int64_t used = sizeof(*h);
      int64_t offset = 0;
      for (int64_t i = 0; used < size; i++) {
        h->col_[i].offset_ = static_cast<uint16_t>(offset);
        const int64_t decoder_size = decoder_sizes[col_header[i].type_];
        used += sizeof(h->col_[0]) + decoder_size;
        offset += decoder_size;
        h->count_++;
      }

      LOG_DEBUG("cache decoders", K(size), K(header->column_count_), "cached_col_cnt", h->count_);

      ObDecoderArrayAllocator allocator(reinterpret_cast<char *>(&h->col_[h->count_]));
      for (int64_t i = 0; OB_SUCC(ret) && i < h->count_; ++i) {
        const ObIColumnDecoder *d = nullptr;
        int64_t ref_col_idx = -1;
        if (OB_FAIL(acquire(allocator, *header, col_header[i], meta_data, d))) {
          LOG_WARN("acquire allocator failed",
                   K(ret), "micro_block_header", *header, "col_header", col_header[i]);
        } else if (OB_FAIL(d->get_ref_col_idx(ref_col_idx))) {
          LOG_WARN("get context param failed", K(ret));
        } else {
          h->col_[i].ref_col_idx_ = static_cast<int16_t>(ref_col_idx);
        }
      }
    }
  }
  return ret;
}

int ObMicroBlockDecoder::update_cached_decoders(char *cache, const int64_t cache_size,
    const char *old_block, const char *cur_block, const int64_t block_size)
{
  int ret = OB_SUCCESS;
  ObBlockCachedDecoderHeader *h = NULL;
  if (NULL == cache || cache_size < sizeof(*h) || NULL == old_block || NULL == cur_block) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(cache), K(cache_size),
        KP(old_block), KP(cur_block), K(block_size));
  } else {
    h = reinterpret_cast<ObBlockCachedDecoderHeader *>(cache);
    char *base = reinterpret_cast<char *>(&h->col_[h->count_]);
    for (int64_t i = 0; OB_SUCC(ret) && i < h->count_; ++i) {
      ObIColumnDecoder *decoder = reinterpret_cast<ObIColumnDecoder *>(base + h->col_[i].offset_);
      decoder->update_pointer(old_block, cur_block);
    }
  }
  return ret;
}

int ObMicroBlockDecoder::get_row_header(
    const int64_t row_idx,
    const ObRowHeader *&row_header)
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

int ObMicroBlockDecoder::get_row_count(int64_t &row_count)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    row_count = header_->row_count_;
  }
  return ret;
}

int ObMicroBlockDecoder::get_multi_version_info(
    const int64_t row_idx,
    const int64_t schema_rowkey_cnt,
    const ObRowHeader *&row_header,
    int64_t &trans_version,
    int64_t &sql_sequence)
{
  UNUSEDx(row_idx, schema_rowkey_cnt, row_header, trans_version, sql_sequence);
  return OB_NOT_SUPPORTED;
}

int ObMicroBlockDecoder::filter_pushdown_filter(
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
  const bool has_lob_out_row = param->has_lob_column_out() && header_->has_lob_out_row();
  if (OB_UNLIKELY(pd_filter_info.start_ < 0 ||
                  pd_filter_info.start_ + pd_filter_info.count_ > row_count_ ||
                  (has_lob_out_row && nullptr == context->lob_locator_helper_))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument",
             K(ret), K(row_count_), K(pd_filter_info.start_), K(pd_filter_info.count_),
             K(has_lob_out_row), KP(context->lob_locator_helper_));
  } else if (OB_FAIL(validate_filter_info(pd_filter_info, filter, datum_buf, col_capacity, header_))) {
    LOG_WARN("Failed to validate filter info", K(ret));
  } else {
    const int64_t col_count = filter.get_col_count();
    const int64_t trans_col_idx = header_->rowkey_column_count_ > 0 ? read_info_->get_schema_rowkey_count() : INT32_MIN;
    const ObColumnIndexArray &cols_index = read_info_->get_columns_index();
    const common::ObIArray<int32_t> &col_offsets = filter.get_col_offsets(pd_filter_info.is_pd_to_cg_);
    const sql::ColumnParamFixedArray &col_params = filter.get_col_params();
    decoder_allocator_.reuse();
    int64_t row_idx = 0;
    bool need_reuse_lob_locator = false;
    for (int64_t offset = 0; OB_SUCC(ret) && offset < pd_filter_info.count_; ++offset) {
      row_idx = offset + pd_filter_info.start_;
      if (nullptr != parent && parent->can_skip_filter(offset)) {
        continue;
      } else if (0 < col_count) {
        int64_t row_len = 0;
        const char *row_data = nullptr;
        if (OB_FAIL(row_index_->get(row_idx, row_data, row_len))) {
          LOG_WARN("get row data failed", K(ret), K(row_idx));
        } else {
          ObBitStream bs(reinterpret_cast<unsigned char *>(const_cast<char *>(row_data)), row_len);
          for (int64_t i = 0; OB_SUCC(ret) && i < col_count; i++) {
            ObStorageDatum &datum = datum_buf[i];
            datum.reuse();
            if (OB_FAIL(decoders_[col_offsets.at(i)].decode(datum, row_idx, bs, row_data, row_len))) {
              LOG_WARN("decode cell failed", K(ret), K(row_idx), K(i), K(datum), K(bs), KP(row_data), K(row_len));
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
      }
      bool filtered = false;
      if (OB_SUCC(ret)) {
        if (OB_FAIL(filter.filter(datum_buf, col_count, *pd_filter_info.skip_bit_, filtered))) {
          LOG_WARN("Failed to filter row with black filter", K(ret), "datum_buf", common::ObArrayWrap<ObStorageDatum>(datum_buf, col_count));
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
    LOG_TRACE("[PUSHDOWN] micro block black pushdown filter row", K(ret), K(pd_filter_info), K(col_offsets),
              K(result_bitmap.popcnt()), K(result_bitmap), KPC_(header), K(filter));
  }
  return ret;
}

int ObMicroBlockDecoder::filter_pushdown_filter(
    const sql::ObPushdownFilterExecutor *parent,
    sql::ObWhiteFilterExecutor &filter,
    const sql::PushdownFilterInfo &pd_filter_info,
    ObBitmap &result_bitmap)
{
  int ret = OB_SUCCESS;
  int32_t col_offset = 0;
  ObStorageDatum *datum_buf = pd_filter_info.datum_buf_;
  const common::ObIArray<int32_t> &col_offsets = filter.get_col_offsets(pd_filter_info.is_pd_to_cg_);
  const sql::ColumnParamFixedArray &col_params =filter.get_col_params();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("Micro block decoder not inited", K(ret));
  } else if (OB_UNLIKELY(1 != filter.get_col_count() ||
                         pd_filter_info.start_ < 0 ||
                         pd_filter_info.start_ + pd_filter_info.count_ > row_count_ ||
                         nullptr == datum_buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret), K(filter.get_col_count()),
             K(row_count_), K(pd_filter_info.start_), K(pd_filter_info.count_), K(datum_buf));
  } else if (FALSE_IT(col_offset = col_offsets.at(0))) {
  } else if (OB_UNLIKELY(0 > col_offset)) {
    ret = OB_INDEX_OUT_OF_RANGE;
    LOG_WARN("Filter column offset out of range", K(ret), K(header_->column_count_), K(col_offset));
  } else {
    ObColumnDecoder* column_decoder = decoders_ + col_offset;
    const sql::ObWhiteFilterOperatorType op_type = filter.get_op_type();
    column_decoder->ctx_->set_col_param(col_params.at(0));
    if (filter.null_param_contained() &&
        op_type != sql::WHITE_OP_NU &&
        op_type != sql::WHITE_OP_NN) {
    } else if (!header_->all_lob_in_row_) {
      if (OB_FAIL(filter_pushdown_retro(parent,
                                        filter,
                                        pd_filter_info,
                                        col_offset,
                                        col_params.at(0),
                                        datum_buf[0],
                                        result_bitmap))) {
        LOG_WARN("[PUSHDOWN] Retrograde path failed", K(ret), K(filter), KPC(column_decoder->ctx_));
      }
    } else if (OB_FAIL(column_decoder->decoder_->pushdown_operator(
                parent,
                *column_decoder->ctx_,
                filter,
                meta_data_,
                row_index_,
                pd_filter_info,
                result_bitmap))) {
      if (OB_LIKELY(ret == OB_NOT_SUPPORTED)) {
        LOG_TRACE("[PUSHDOWN] Column specific operator failed, switch to retrograde filter pushdown",
                  K(ret), K(filter));
        // reuse result bitmap as null objs set
        result_bitmap.reuse();
        if (OB_FAIL(filter_pushdown_retro(parent,
                                          filter,
                                          pd_filter_info,
                                          col_offset,
                                          col_params.at(0),
                                          datum_buf[0],
                                          result_bitmap))) {
          LOG_WARN("Retrograde path failed.", K(ret), K(filter), KPC(column_decoder->ctx_));
        }
      }
    }
  }

  LOG_TRACE("[PUSHDOWN] white pushdown filter row", K(ret), "need_padding", nullptr != col_params.at(0), K(col_offset),
            K(pd_filter_info), K(result_bitmap.popcnt()), K(result_bitmap), KPC_(header), K(filter));
  return ret;
}

/**
 * Retrograde path for filter pushdown. Scan the microblock by row and set @result_bitmap.
 * This path do not use any meta_data from microblock but ensure the pushdown logic
 *    is safe from unsupported type.
 */
int ObMicroBlockDecoder::filter_pushdown_retro(
    const sql::ObPushdownFilterExecutor *parent,
    sql::ObWhiteFilterExecutor &filter,
    const sql::PushdownFilterInfo &pd_filter_info,
    const int32_t col_offset,
    const share::schema::ObColumnParam *col_param,
    ObStorageDatum &decoded_datum,
    common::ObBitmap &result_bitmap)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("Micro Block decoder not inited", K(ret));
  } else if (OB_UNLIKELY(0 > col_offset)) {
    ret = OB_INDEX_OUT_OF_RANGE;
    LOG_WARN("Filter column id out of range", K(ret), K(col_offset), K(header_->column_count_));
  } else if (OB_UNLIKELY(sql::WHITE_OP_MAX <= filter.get_op_type()
                         || !result_bitmap.is_inited())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid operator type of Filter node",
             K(ret), K(filter), K(result_bitmap.is_inited()));
  } else {
    const sql::ObWhiteFilterOperatorType op_type = filter.get_op_type();
    decoder_allocator_.reuse();
    int64_t row_id = 0;
    for (int64_t offset = 0; OB_SUCC(ret) && offset < pd_filter_info.count_; ++offset) {
      row_id = offset + pd_filter_info.start_;
      if (nullptr != parent && parent->can_skip_filter(offset)) {
        continue;
      }
      const char *row_data = nullptr;
      int64_t row_len = 0;
      if (OB_FAIL(row_index_->get(row_id, row_data, row_len))) {
        LOG_WARN("get row data failed", K(ret), K(index));
      } else {
        ObBitStream bs(reinterpret_cast<unsigned char *>(const_cast<char *>(row_data)), row_len);
        const ObObjMeta &obj_meta = read_info_->get_columns_desc().at(col_offset).col_type_;
        decoded_datum.reuse();
        if (OB_FAIL(decoders_[col_offset].decode(decoded_datum, row_id, bs, row_data, row_len))) {
          LOG_WARN("decode cell failed", K(ret), K(row_id), K(bs), KP(row_data), K(row_len));
        } else if (need_padding(filter.is_padding_mode(), obj_meta) && OB_FAIL(
                storage::pad_column(obj_meta, col_param->get_accuracy(), decoder_allocator_.get_inner_allocator(), decoded_datum))) {
          LOG_WARN("Failed to pad column", K(ret), K(col_offset), K(row_id));
        }

        bool filtered = false;
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(filter_white_filter(filter, decoded_datum, filtered))) {
          LOG_WARN("Failed to filter row with white filter", K(ret), K(row_id), K(decoded_datum));
        } else if (!filtered && OB_FAIL(result_bitmap.set(offset))) {
          LOG_WARN("Failed to set result bitmap", K(ret), K(row_id));
        }
      }
    }
  }
  LOG_TRACE("[PUSHDOWN] Retrograde when pushdown filter", K(ret), K(col_offset),
            K(result_bitmap.popcnt()), K(result_bitmap), KPC_(header), KP(col_param), K(filter));
  return ret;
}

int ObMicroBlockDecoder::filter_black_filter_batch(
    const sql::ObPushdownFilterExecutor *parent,
    sql::ObBlackFilterExecutor &filter,
    sql::PushdownFilterInfo &pd_filter_info,
    common::ObBitmap &result_bitmap,
    bool &filter_applied)
{
  int ret = OB_SUCCESS;
  filter_applied = false;
  if (OB_UNLIKELY(pd_filter_info.start_ < 0 ||
                  pd_filter_info.start_ + pd_filter_info.count_ > row_count_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret), K_(row_count), K(pd_filter_info.start_), K(pd_filter_info.count_));
  } else {
    const common::ObIArray<int32_t> &col_offsets = filter.get_col_offsets(pd_filter_info.is_pd_to_cg_);
    const sql::ColumnParamFixedArray &col_params = filter.get_col_params();
    if (1 == col_offsets.count() &&
        (decoders_[col_offsets.at(0)].decoder_->is_new_column() ||
          ObColumnHeader::DICT == decoders_[col_offsets.at(0)].ctx_->col_header_->type_)) {
      ObColumnDecoder* column_decoder = decoders_ + col_offsets.at(0);
      if (!column_decoder->decoder_->fast_decode_valid(*column_decoder->ctx_)) {
        column_decoder->ctx_->set_col_param(col_params.at(0));
        if (OB_FAIL(column_decoder->decoder_->pushdown_operator(
                    parent,
                    *column_decoder->ctx_,
                    filter,
                    meta_data_,
                    row_index_,
                    pd_filter_info,
                    result_bitmap,
                    filter_applied))) {
          LOG_WARN("Fail to pushdown operator", K(ret), K(filter));
        }
      }
    }
  }
  return ret;
}

int ObMicroBlockDecoder::filter_pushdown_truncate_filter(
    const sql::ObPushdownFilterExecutor *parent,
    sql::ObPushdownFilterExecutor &filter,
    const sql::PushdownFilterInfo &pd_filter_info,
    common::ObBitmap &result_bitmap)
{
  int ret = OB_SUCCESS;
  bool filter_applied = false;
  if (OB_UNLIKELY(pd_filter_info.start_ < 0 ||
                  pd_filter_info.start_ + pd_filter_info.count_ > row_count_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret), K(row_count_), K(pd_filter_info.start_), K(pd_filter_info.count_));
  } else if (OB_UNLIKELY(!filter.is_truncate_filter_node())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected truncate filter type", K(ret), K(filter));
  } else if (OB_LIKELY(filter.is_filter_white_node())) {
    int64_t col_offset = 0;
    ObColumnDecoder *column_decoder = nullptr;
    ObTruncateWhiteFilterExecutor *truncate_executor = static_cast<ObTruncateWhiteFilterExecutor*>(&filter);
    const common::ObIArray<int32_t> &col_idxs = truncate_executor->get_col_idxs();
    ObTruncateWhiteFilterExecutor::FilterBatchGuard filter_guard(*truncate_executor);
    if (OB_UNLIKELY(1 != col_idxs.count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected col idx count for white filter", K(ret), K(col_idxs));
    } else if (FALSE_IT(col_offset = col_idxs.at(0))) {
    } else if (OB_UNLIKELY(col_offset < 0 || col_offset >= header_->column_count_)) {
      ret = OB_INDEX_OUT_OF_RANGE;
      LOG_WARN("column offset out of range", K(ret), K(header_->column_count_), K(col_offset));
    } else if (FALSE_IT(column_decoder = decoders_ + col_offset)) {
    } else if (OB_FAIL(column_decoder->decoder_->pushdown_operator(parent, *column_decoder->ctx_, *truncate_executor,
        meta_data_, row_index_, pd_filter_info, result_bitmap))) {
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
    LOG_TRACE("[TRUNCATE INFO] micro block black pushdown filter row", K(ret), K(pd_filter_info), K(result_bitmap.popcnt()), K(result_bitmap),
              KPC(truncate_executor), KPC_(header), K(filter));
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
        continue;
      } else {
        int64_t row_len = 0;
        const char *row_data = nullptr;
        if (OB_FAIL(row_index_->get(row_idx, row_data, row_len))) {
          LOG_WARN("get row data failed", K(ret), K(index));
        } else {
          ObBitStream bs(reinterpret_cast<unsigned char *>(const_cast<char *>(row_data)), row_len);
          for (int64_t i = 0; OB_SUCC(ret) && i < col_count; i++) {
            ObStorageDatum &datum = datum_buf[i];
            datum.reuse();
            if (OB_FAIL(decoders_[col_idxs.at(i)].decode(datum, row_idx, bs, row_data, row_len))) {
              LOG_WARN("decode cell failed", K(ret), K(row_idx), K(i), K(datum), K(bs), KP(row_data), K(row_len));
            } else if (OB_UNLIKELY(header_->is_trans_version_column_idx(col_idxs.at(i)))) {
              if (OB_FAIL(storage::reverse_trans_version_val(datum))) {
                LOG_WARN("Failed to reverse trans version val", K(ret));
              }
            }
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
    LOG_TRACE("[TRUNCATE INFO] micro block black pushdown filter row", K(ret), K(pd_filter_info), K(result_bitmap.popcnt()), K(result_bitmap),
              KPC(truncate_executor), KPC_(header), K(filter));
  }
  return ret;
}

int ObMicroBlockDecoder::get_rows(
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
  decoder_allocator_.reuse();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(nullptr == row_ids || nullptr == cell_datas ||
                         cols.count() != datum_infos.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(row_ids), KP(cell_datas),
             K(cols.count()), K(datum_infos.count()));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < cols.count(); i++) {
      int32_t col_id = cols.at(i);
      common::ObDatum *col_datums = datum_infos.at(i).datum_ptr_ + datum_offset;
      if (OB_FAIL(get_col_datums(col_id, row_ids, cell_datas, row_cap, col_datums))) {
        LOG_WARN("Failed to get col datums", K(ret), K(i), K(col_id), K(row_cap));
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

int ObMicroBlockDecoder::get_row_count(
    int32_t col_id,
    const int32_t *row_ids,
    const int64_t row_cap,
    const bool contains_null,
    const share::schema::ObColumnParam *col_param,
    int64_t &count)
{
  UNUSED(col_param);
  int ret = OB_SUCCESS;
  decoder_allocator_.reuse();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(decoders_[col_id].get_row_count(
              row_index_,
              row_ids,
              row_cap,
              contains_null,
              count))) {
    LOG_WARN("fail to get datums from decoder", K(ret), K(col_id), K(row_cap),
             "row_ids", common::ObArrayWrap<const int32_t>(row_ids, row_cap));
  }
  return ret;
}

int ObMicroBlockDecoder::get_aggregate_result(
    const ObTableIterParam &iter_param,
    const ObTableAccessContext &context,
    const int32_t col_offset,
    const share::schema::ObColumnParam &col_param,
    const int32_t *row_ids,
    const int64_t row_cap,
    storage::ObAggDatumBuf &agg_datum_buf,
    storage::ObAggCell &agg_cell)
{
  int ret = OB_SUCCESS;
  decoder_allocator_.reuse();
  const char **cell_datas = agg_datum_buf.get_cell_datas();
  ObDatum *datum_buf = agg_datum_buf.get_datums();
  if (OB_FAIL(get_col_datums(col_offset, row_ids, cell_datas, row_cap, datum_buf))) {
    LOG_WARN("Failed to get col datums", K(ret), K(col_offset), K(row_cap));
  } else if (agg_cell.need_padding() && OB_FAIL(storage::pad_on_datums(
                                          col_param.get_accuracy(),
                                          col_param.get_meta_type().get_collation_type(),
                                          decoder_allocator_.get_inner_allocator(),
                                          row_cap,
                                          datum_buf))) {
    LOG_WARN("fail to pad on datums", K(ret), K(row_cap));
  } else if (col_param.get_meta_type().is_lob_storage() && header_->has_lob_out_row() &&
        OB_FAIL(fill_datums_lob_locator(iter_param, context, col_param, row_cap, datum_buf))) {
    LOG_WARN("Fail to fill lob locator", K(ret));
  } else if (OB_FAIL(agg_cell.eval_batch(datum_buf, row_cap))) {
    LOG_WARN("Failed to eval batch", K(ret));
  }
  LOG_DEBUG("get_aggregate_result", K(ret), K(agg_cell));
  return ret;
}

int ObMicroBlockDecoder::get_col_datums(
    int32_t col_id,
    const int32_t *row_ids,
    const char **cell_datas,
    const int64_t row_cap,
    common::ObDatum *col_datums)
{
  int ret = OB_SUCCESS;
  if (!decoders_[col_id].decoder_->can_vectorized()) {
    // normal path
    common::ObObj cell;
    int64_t row_len = 0;
    const char *row_data = NULL;
    const int row_header_size = ObRowHeader::get_serialized_size();
    int64_t row_id = common::OB_INVALID_INDEX;
    for (int64_t idx = 0; OB_SUCC(ret) && idx < row_cap; idx++) {
      row_id = row_ids[idx];
      if (OB_FAIL(row_index_->get(row_id, row_data, row_len))) {
        LOG_WARN("get row data failed", K(ret), K(row_id));
      } else {
        ObBitStream bs(reinterpret_cast<unsigned char *>(const_cast<char *>(row_data)), row_len);
        if (OB_FAIL(decoders_[col_id].decode(col_datums[idx], row_id, bs, row_data, row_len))) {
          LOG_WARN("Decode cell failed", K(ret));
        }
      }
    }
  } else if (OB_FAIL(decoders_[col_id].batch_decode(
              row_index_,
              row_ids,
              cell_datas,
              row_cap,
              col_datums))) {
    LOG_WARN("fail to get datums from decoder", K(ret), K(col_id), K(row_cap),
              "row_ids", common::ObArrayWrap<const int32_t>(row_ids, row_cap));
  }
  return ret;
}

int ObMicroBlockDecoder::get_raw_column_datum(
    const int32_t col_offset,
    const int64_t row_index,
    ObStorageDatum &datum)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObMicroBlockDecoder is not init", K(ret));
  } else if (OB_UNLIKELY(col_offset >= header_->column_count_ || row_index >= row_count_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument to get row", K(ret), K(col_offset), K(row_index));
  } else {
    int64_t row_len = 0;
    const char *row_data = NULL;
    const int row_header_size = ObRowHeader::get_serialized_size();
    if (OB_FAIL(row_index_->get(row_index, row_data, row_len))) {
      LOG_WARN("get row data failed", K(ret), K(row_index));
    } else {
      ObBitStream bs(reinterpret_cast<unsigned char *>(const_cast<char *>(row_data)), row_len);
      datum.reuse();
      if (OB_FAIL(decoders_[col_offset].decode(datum, row_index, bs, row_data, row_len))) {
        LOG_WARN("Decode cell failed", K(ret));
      }
    }
  }
  return ret;
}

int ObMicroBlockDecoder::get_distinct_count(const int32_t group_by_col, int64_t&distinct_cnt) const
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

int ObMicroBlockDecoder::read_distinct(
    const int32_t group_by_col,
    const char **cell_datas,
    const bool is_padding_mode,
    storage::ObGroupByCellBase &group_by_cell) const
{
  UNUSED(is_padding_mode);
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObMicroBlockDecoder is not init", K(ret));
  } else {
    if (OB_FAIL(decoders_[group_by_col].read_distinct(cell_datas, group_by_cell))) {
      LOG_WARN("Failed to read distinct", K(ret));
    }
  }
  LOG_DEBUG("[GROUP BY PUSHDOWN]", K(ret), K(group_by_cell));
  return ret;
}

int ObMicroBlockDecoder::read_reference(
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

int ObMicroBlockDecoder::get_group_by_aggregate_result(
    const ObTableIterParam &iter_param,
    const ObTableAccessContext &context,
    const int32_t *row_ids,
    const char **cell_datas,
    const int64_t row_cap,
    storage::ObGroupByCell &group_by_cell)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObMicroBlockDecoder is not init", K(ret));
  } else {
    const int32_t group_by_col = group_by_cell.get_group_by_col_offset();
    int32_t last_agg_col_offset = INT32_MIN;
    for (int64_t i = 0; OB_SUCC(ret) && i < group_by_cell.get_agg_cells().count(); ++i) {
      storage::ObAggCell *agg_cell =  group_by_cell.get_agg_cells().at(i);
      const int32_t agg_col_offset = agg_cell->get_col_offset();
      common::ObDatum *col_datums = agg_cell->get_col_datums();
      const share::schema::ObColumnParam *col_param = agg_cell->get_col_param();
      bool need_get_col_datum = (0 == i || agg_col_offset != last_agg_col_offset) && agg_cell->need_access_data();
      if (agg_col_offset == group_by_col) {
        // agg on group by column
        if (OB_FAIL(group_by_cell.eval_batch(group_by_cell.get_group_by_col_datums(), row_cap, i, true))) {
          LOG_WARN("Failed to eval batch", K(ret), K(i));
        }
      } else {
        if (need_get_col_datum) {
          if (OB_FAIL(get_col_datums(agg_col_offset, row_ids, cell_datas, row_cap, col_datums))) {
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

int ObMicroBlockDecoder::get_group_by_aggregate_result(
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
        if (need_get_col) {
          sql::ObExpr &expr = *(agg_cell->get_project_expr());
          const bool need_padding = agg_cell->need_padding();
          const share::schema::ObColumnParam *col_param = agg_cell->get_col_param();
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
            } else if (OB_FAIL(get_col_datums(agg_col_offset, row_ids, cell_datas, row_cap, col_datums))) {
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

int ObMicroBlockDecoder::get_rows(
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
  } else if (OB_UNLIKELY(nullptr == row_ids || nullptr == cell_datas ||
                         cols.count() != exprs.count() || cols.count() != col_params.count() ||
                         (nullptr != default_datums && cols.count() != default_datums->count()))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(row_ids), KP(cell_datas),
             K(cols.count()), K(exprs.count()), K(col_params.count()), KPC(default_datums));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < cols.count(); i++) {
      const int32_t col_id = cols.at(i);
      sql::ObExpr &expr = *(exprs.at(i));
      const bool is_need_padding = nullptr != col_params.at(i) && need_padding(is_padding_mode, col_params.at(i)->get_meta_type());
      if (0 == vec_offset) {
        if (need_init_vector) {
          const VectorFormat format = (is_need_padding) ? VectorFormat::VEC_DISCRETE : expr.get_default_res_format();
          if (OB_FAIL(storage::init_expr_vector_header(expr, eval_ctx, eval_ctx.max_batch_size_, format))) {
            LOG_WARN("Fail to init vector", K(ret));
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

int ObMicroBlockDecoder::get_col_data(const int32_t col_id, ObVectorDecodeCtx &vector_ctx)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(decoders_[col_id].decode_vector(row_index_, vector_ctx))) {
    LOG_WARN("fail to get column data from decoder", K(ret), K(header_->column_count_), K(col_id), K(vector_ctx));
  }
  return ret;
}

}
}
