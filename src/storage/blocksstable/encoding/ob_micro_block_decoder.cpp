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
#include "common/sql_mode/ob_sql_mode_utils.h"
#include "share/rc/ob_tenant_base.h"
#include "storage/access/ob_pushdown_aggregate.h"
#include "storage/access/ob_table_access_context.h"

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

void ObDecoderCtxArray::reset()
{
  int ret = OB_SUCCESS;
  if (ctx_blocks_.count() > 0) {
    ObDecodeResourcePool *decode_res_pool = MTL(ObDecodeResourcePool*);
    if (OB_ISNULL(decode_res_pool)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("NULL tenant decode resource pool", K(ret));
    } else {
      FOREACH(it, ctx_blocks_) {
        ObColumnDecoderCtxBlock *ctx_block = *it;
        if (OB_FAIL(decode_res_pool->free(ctx_block))) {
          LOG_ERROR("failed to free decoder ctx block", K(ret), KP(ctx_block));
        }
      }
      ctx_blocks_.reset();
      ctxs_.reset();
    }
  }
}

int ObDecoderCtxArray::get_ctx_array(ObDecoderCtx **&ctxs, int64_t size)
{
  int ret = OB_SUCCESS;
  ctxs = NULL;
  ObDecodeResourcePool *decode_res_pool = MTL(ObDecodeResourcePool*);
  if (size <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (OB_ISNULL(decode_res_pool)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL tenant decode resource pool", K(ret), K(MTL_ID()));
  } else if (ctxs_.count() < size) {
    int need_ctx_count = size - ctxs_.count();
    int need_block_count = need_ctx_count / ObColumnDecoderCtxBlock::CTX_NUMS;
    if (need_ctx_count % ObColumnDecoderCtxBlock::CTX_NUMS > 0) {
      need_block_count ++;
    }
    ObColumnDecoderCtxBlock *ctx_block = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && i < need_block_count; ++i) {
      if (OB_FAIL(decode_res_pool->alloc(ctx_block))) {
        LOG_WARN("alloc decoder ctx block failed", K(ret), K(i), K(need_block_count));
      } else if (OB_FAIL(ctx_blocks_.push_back(ctx_block))) {
        LOG_WARN("ctx block array push back failed", K(ret), KP(ctx_block));
        int tmp_ret = OB_SUCCESS;
        if (OB_TMP_FAIL(decode_res_pool->free(ctx_block))) {
          LOG_ERROR("failed to free decoder ctx block", K(tmp_ret), KP(ctx_block));
        } else {
          ctx_block = NULL;
        }
      } else {
        for (int64_t j = 0; OB_SUCC(ret) && j < ObColumnDecoderCtxBlock::CTX_NUMS; ++j) {
          if (OB_FAIL(ctxs_.push_back(&ctx_block->ctxs_[j]))) {
            LOG_WARN("ctx array push back failed", K(ret), K(j), KP(ctx_block));
          }
        }
      }
    }

    if (OB_SUCC(ret)) {
      ctxs = &ctxs_.at(0);
    }
  } else {
    ctxs = &ctxs_.at(0);
  }
  return ret;
}

ObNoneExistColumnDecoder ObMicroBlockDecoder::none_exist_column_decoder_;
ObColumnDecoderCtx ObMicroBlockDecoder::none_exist_column_decoder_ctx_;

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

/////////////////////// acquire decoder from local ///////////////////////
typedef int (*local_decode_acquire_func)(ObDecoderPool &local_decoder_pool,
                                   const ObMicroBlockHeader &header,
                                   const ObColumnHeader &col_header,
                                   const char *meta_data,
                                   const ObIColumnDecoder *&decoder);
typedef int(*local_decode_release_func)(ObDecoderPool &local_decoder_pool, ObIColumnDecoder *decoder);

static local_decode_acquire_func acquire_local_funcs_[ObColumnHeader::MAX_TYPE] =
{
    acquire_local_decoder<ObRawDecoder>,
    acquire_local_decoder<ObDictDecoder>,
    acquire_local_decoder<ObRLEDecoder>,
    acquire_local_decoder<ObConstDecoder>,
    acquire_local_decoder<ObIntegerBaseDiffDecoder>,
    acquire_local_decoder<ObStringDiffDecoder>,
    acquire_local_decoder<ObHexStringDecoder>,
    acquire_local_decoder<ObStringPrefixDecoder>,
    acquire_local_decoder<ObColumnEqualDecoder>,
    acquire_local_decoder<ObInterColSubStrDecoder>
};

static local_decode_release_func release_local_funcs_[ObColumnHeader::MAX_TYPE] =
{
    release_local_decoder<ObRawDecoder>,
    release_local_decoder<ObDictDecoder>,
    release_local_decoder<ObRLEDecoder>,
    release_local_decoder<ObConstDecoder>,
    release_local_decoder<ObIntegerBaseDiffDecoder>,
    release_local_decoder<ObStringDiffDecoder>,
    release_local_decoder<ObHexStringDecoder>,
    release_local_decoder<ObStringPrefixDecoder>,
    release_local_decoder<ObColumnEqualDecoder>,
    release_local_decoder<ObInterColSubStrDecoder>
};

template <class Decoder>
int acquire_local_decoder(ObDecoderPool &local_decoder_pool,
                    const ObMicroBlockHeader &header,
                    const ObColumnHeader &col_header,
                    const char *meta_data,
                    const ObIColumnDecoder *&decoder)
{
  int ret = OB_SUCCESS;
  Decoder *d = nullptr;
  if (OB_FAIL(local_decoder_pool.alloc<Decoder>(d))) {
    LOG_WARN("alloc decoder from local decoder pool failed", K(ret));
  } else if (OB_UNLIKELY(!col_header.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid column header", K(ret), K(header), K(col_header));
  } else if (OB_FAIL(d->init(header, col_header, meta_data))) {
    LOG_WARN("init decoder failed", K(ret));
  } else {
    decoder = d;
  }

  if (OB_FAIL(ret) && nullptr != d) {
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(local_decoder_pool.free<Decoder>(d))) {
      LOG_ERROR("failed to free decoder", K(tmp_ret), "type", decoder->get_type(), KP(d));
    } else {
      d = nullptr;
      decoder = nullptr;
    }
  }
  return ret;
}

template <class Decoder>
int release_local_decoder(ObDecoderPool &local_decoder_pool, ObIColumnDecoder *decoder)
{
  int ret = OB_SUCCESS;
  Decoder *d = nullptr;
  if (nullptr == decoder) {
    //do nothing
  } else if (FALSE_IT(d = static_cast<Decoder *>(decoder))) {
  } else if (OB_FAIL(local_decoder_pool.free<Decoder>(d))) {
    LOG_ERROR("failed to free decoder", K(ret), "type", decoder->get_type(), KP(d));
  } else {
    d = nullptr;
    decoder = nullptr;
  }
  return ret;
}

//////////////////////////ObIEncodeBlockGetReader/////////////////////////
ObNoneExistColumnDecoder ObIEncodeBlockReader::none_exist_column_decoder_;
ObColumnDecoderCtx ObIEncodeBlockReader::none_exist_column_decoder_ctx_;
ObIEncodeBlockReader::ObIEncodeBlockReader()
  : request_cnt_(0),
    header_(NULL), col_header_(NULL), cached_decocer_(NULL), meta_data_(NULL), row_data_(NULL),
    var_row_index_(), fix_row_index_(), row_index_(&var_row_index_),
    decoders_(nullptr),
    need_release_decoders_(nullptr), need_release_decoder_cnt_(0),
    default_decoders_(), default_release_decoders_(),
    local_decoder_pool_(nullptr), ctx_array_(), ctxs_(NULL),
    decoder_allocator_(common::ObModIds::OB_DECODER_CTX, OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()),
    buf_allocator_("OB_IENB_READER", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()),
    store_id_array_(NULL), column_type_array_(NULL),
    default_store_ids_(), default_column_types_(),
    need_cast_(false)
{}

ObIEncodeBlockReader::~ObIEncodeBlockReader()
{
  (void)free_decoders();
  ctx_array_.reset();
  if (NULL != local_decoder_pool_) {
    local_decoder_pool_->reset();
    local_decoder_pool_ = NULL;
  }
}

void ObIEncodeBlockReader::reuse()
{
  (void)free_decoders();
  cached_decocer_ = nullptr;
  header_ = nullptr;
  request_cnt_ = 0;
  decoders_ = nullptr;
  need_cast_ = false;
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
  (void)free_decoders();
  cached_decocer_ = NULL;
  header_ = NULL;
  request_cnt_ = 0;
  decoders_ = nullptr;
  need_cast_ = false;
  col_header_ = NULL;
  meta_data_ = NULL;
  row_data_ = NULL;
  ctx_array_.reset();
  if (NULL != local_decoder_pool_) {
    local_decoder_pool_->reset();
    local_decoder_pool_ = NULL;
  }
  ctxs_ = NULL;
  store_id_array_ = NULL;
  column_type_array_ = NULL;
  decoder_allocator_.reset();
  buf_allocator_.reset();
}

int ObIEncodeBlockReader::free_decoders()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (nullptr != need_release_decoders_) {
    for (int64_t i = 0; i < need_release_decoder_cnt_; ++i) {
      if (OB_TMP_FAIL(release_local_funcs_[need_release_decoders_[i]->get_type()]
          (*local_decoder_pool_, const_cast<ObIColumnDecoder *>(need_release_decoders_[i])))) {
        ret = tmp_ret;
        LOG_ERROR("failed to free decoder", K(tmp_ret), K(i), K(need_release_decoder_cnt_),
            "type", need_release_decoders_[i]->get_type(), KP(need_release_decoders_[i]));
      }
    }
    need_release_decoders_ = nullptr;
  }
  need_release_decoder_cnt_ = 0;
  return ret;
}

int ObIEncodeBlockReader::prepare(const uint64_t tenant_id, const int64_t column_cnt)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ret)){
  } else if (column_cnt > DEFAULT_DECODER_CNT) {
    // Size is doubled because we may need two decoders for one column.
    // e.g. column equal encoding, one for column equal decoder and one for referenced column decoder.
    // And total decoders include decoders_ and version_decorder_.
    const int64_t release_count = (column_cnt + 1) * 2;
    if (OB_FAIL(ret)) {
    } else if (nullptr
        == (store_id_array_ = reinterpret_cast<int64_t*>(decoder_allocator_.alloc(
            sizeof(int64_t) * column_cnt)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc memory for store ids fail", K(ret), K(column_cnt));
    } else if (nullptr
        == (column_type_array_ = reinterpret_cast<ObObjMeta*>(decoder_allocator_.alloc(
            sizeof(ObObjMeta) * column_cnt)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc memory for column types fail", K(ret), K(column_cnt));
    } else if (nullptr
        == (decoders_ = reinterpret_cast<ObColumnDecoder*>(decoder_allocator_.alloc(
            sizeof(ObColumnDecoder) * column_cnt)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc memory for decoders fail", K(ret), K(column_cnt));
    } else if (nullptr
        == (need_release_decoders_ = reinterpret_cast<const ObIColumnDecoder**>(decoder_allocator_.alloc(
            sizeof(ObIColumnDecoder*) * release_count)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc memory for need release decoders fail", K(ret), K(column_cnt), K(release_count));
    }
  } else {
    store_id_array_ = &default_store_ids_[0];
    column_type_array_ = &default_column_types_[0];
    decoders_ = &default_decoders_[0];
    need_release_decoders_ = &default_release_decoders_[0];
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
    if (OB_FAIL(ctx_array_.get_ctx_array(ctxs_, MAX(request_cnt_, header_->column_count_)))) {
      LOG_WARN("get decoder context array failed", K(ret));
    } else if (OB_ISNULL((local_decoder_pool_))
        && OB_ISNULL(local_decoder_pool_ = OB_NEWx(ObDecoderPool, &buf_allocator_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc local decoder pool memory", K(ret));
    } else {
      row_data_ = block_data.get_buf() + header_->row_data_offset_;
      const int64_t row_data_len = block_data.get_buf_size() - header_->row_data_offset_;

      if (NULL != block_data.get_extra_buf() && block_data.get_extra_size() > 0) {
        cached_decocer_ = reinterpret_cast<const ObBlockCachedDecoderHeader *>(
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
  if (OB_UNLIKELY(NULL == store_id_array_ || NULL == column_type_array_
      || nullptr == decoders_ || nullptr == need_release_decoders_
      || NULL == header_ || NULL == col_header_)) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("header should be set while init decoders", K(ret), KP_(store_id_array),
        KP_(column_type_array), KP(decoders_), KP(need_release_decoders_),
        KP_(header), KP_(col_header));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < request_cnt_; ++i) {
      if (OB_FAIL(add_decoder(store_id_array_[i], column_type_array_[i], decoders_[i]))) {
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

int ObIEncodeBlockReader::add_decoder(const int64_t store_idx, const ObObjMeta &obj_meta, ObColumnDecoder &dest)
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
      if (OB_FAIL(acquire(store_idx, decoder))) {
        LOG_WARN("acquire decoder failed", K(ret), K(obj_meta), K(store_idx));
      } else {
        dest.decoder_ = decoder;
        dest.ctx_ = ctxs_[store_idx];
        dest.ctx_->fill(obj_meta, header_, &col_header_[store_idx], &decoder_allocator_);

        int64_t ref_col_idx = -1;
        if (NULL != cached_decocer_ && cached_decocer_->count_ > store_idx) {
          ref_col_idx = cached_decocer_->col_[store_idx].ref_col_idx_;
        } else {
          if (OB_FAIL(decoder->get_ref_col_idx(ref_col_idx))) {
            LOG_WARN("get context param failed", K(ret));
          }
        }
        if (OB_SUCC(ret) && ref_col_idx >= 0) {
          if (OB_FAIL(acquire(ref_col_idx, ref_decoder))) {
            LOG_WARN("acquire decoder failed", K(ret), K(obj_meta), K(ref_col_idx));
          } else {
            dest.ctx_->ref_decoder_ = ref_decoder;
            dest.ctx_->ref_ctx_ = ctxs_[ref_col_idx];
            dest.ctx_->ref_ctx_->fill(obj_meta, header_, &col_header_[ref_col_idx], &decoder_allocator_);
          }
        }
      }
    }
  }
  return ret;
}

// called before inited
// performance critical, do not check parameters
int ObIEncodeBlockReader::acquire(const int64_t store_idx, const ObIColumnDecoder *&decoder)
{
  int ret = OB_SUCCESS;
  if (NULL != cached_decocer_ && store_idx < cached_decocer_->count_) {
    decoder = &cached_decocer_->at(store_idx);
  } else {
    if (OB_FAIL(acquire_local_funcs_[col_header_[store_idx].type_](
        *local_decoder_pool_, *header_, col_header_[store_idx], meta_data_, decoder))) {
      LOG_WARN("acquire decoder failed", K(ret), K(store_idx), K(col_header_[store_idx]));
    } else {
      need_release_decoders_[need_release_decoder_cnt_++] = decoder;
    }
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
    if (OB_FAIL(prepare(MTL_ID(), request_cnt))) {
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

    if (OB_SUCC(ret) && OB_FAIL(do_init(block_data, request_cnt))) {
      LOG_WARN("failed to do init", K(ret), K(block_data), K(request_cnt));
    }
  }
  return ret;
}

int ObEncodeBlockGetReader::get_row(
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

  reuse();
  if (OB_FAIL(init_by_read_info(block_data, read_info))) {
    LOG_WARN("failed to do inner init", K(ret), K(block_data), K(read_info));
  } else if (OB_FAIL(locate_row(rowkey, read_info.get_datum_utils(), row_data, row_len, row_id, found))) {
    LOG_WARN("failed to locate row", K(ret), K(rowkey));
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
  if (OB_FAIL(prepare(MTL_ID(), MAX(schema_rowkey_cnt, request_cnt)))) {
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
    const ObMicroBlockData &block_data,
    const ObITableReadInfo &read_info,
    const uint32_t row_idx,
    ObDatumRow &row)
{
  int ret = OB_SUCCESS;
  const char *row_data = NULL;
  int64_t row_len = 0;

  reuse();
  if (OB_FAIL(init_by_read_info(block_data, read_info))) {
    LOG_WARN("failed to do inner init", K(ret), K(block_data), K(read_info));
  } else if (OB_FAIL(setup_row(row_idx, row_len, row_data))) {
    LOG_WARN("Failed to setup row", K(ret), K(row_idx));
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
    const ObMicroBlockData &block_data,
    const ObDatumRowkey &rowkey,
    const ObITableReadInfo &read_info,
    int64_t &row_id)
{
  int ret = OB_SUCCESS;
  bool found = false;
  const char *row_data = NULL;
  int64_t row_len = 0;

  reuse();
  if (OB_UNLIKELY(!read_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret), K(read_info));
  } else if (OB_FAIL(init_by_read_info(block_data, read_info))) {
    LOG_WARN("failed to do inner init", K(ret), K(block_data), K(read_info));
  } else if (OB_FAIL(locate_row(rowkey, read_info.get_datum_utils(), row_data, row_len, row_id, found))) {
    LOG_WARN("failed to locate row", K(ret), K(rowkey));
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
    decoder_buf_(nullptr),
    decoders_(nullptr),
    need_release_decoders_(),
    need_release_decoder_cnt_(0),
    flat_row_reader_(),
    local_decoder_pool_(nullptr),
    ctx_array_(),
    ctxs_(nullptr),
    decoder_allocator_(ObModIds::OB_DECODER_CTX),
    buf_allocator_("OB_MICB_DECODER", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID())
{
  need_release_decoders_.set_allocator(&buf_allocator_);
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
int ObMicroBlockDecoder::acquire(const int64_t store_idx, const ObIColumnDecoder *&decoder)
{
  int ret = OB_SUCCESS;
  decoder = nullptr;
  if (NULL != cached_decoder_ && store_idx < cached_decoder_->count_) {
    decoder = &cached_decoder_->at(store_idx);
  } else {
    if (OB_FAIL(acquire_local_funcs_[col_header_[store_idx].type_](
        *local_decoder_pool_, *header_, col_header_[store_idx], meta_data_, decoder))) {
      LOG_WARN("acquire decoder failed", K(ret), K(store_idx),
          "column_header", col_header_[store_idx]);
    } else if (OB_FAIL(need_release_decoders_.push_back(decoder))) {
      LOG_WARN("add decoder failed", K(ret), K(store_idx), "column_header", col_header_[store_idx]);
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(release_local_funcs_[decoder->get_type()]
          (*local_decoder_pool_, const_cast<ObIColumnDecoder *>(decoder)))) {
        LOG_ERROR("failed to free decoder", K(tmp_ret), "type", decoder->get_type(), KP(decoder));
      } else {
        decoder = nullptr;
      }
    } else {
      ++need_release_decoder_cnt_;
    }
  }
  return ret;
}


int ObMicroBlockDecoder::free_decoders()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  for (int64_t i = 0; i < need_release_decoder_cnt_; ++i) {
    if (OB_TMP_FAIL(release_local_funcs_[need_release_decoders_[i]->get_type()]
        (*local_decoder_pool_, const_cast<ObIColumnDecoder *>(need_release_decoders_[i])))) {
      ret = tmp_ret;
      LOG_ERROR("failed to free decoder", K(tmp_ret), K(i), K(need_release_decoder_cnt_),
          "type", need_release_decoders_[i]->get_type(), KP(need_release_decoders_[i]));
    }
  }
  need_release_decoders_.clear();
  need_release_decoder_cnt_ = 0;
  return ret;
}

void ObMicroBlockDecoder::inner_reset()
{
  (void)free_decoders();
  cached_decoder_ = NULL;
  header_ = NULL;
  col_header_ = NULL;
  meta_data_ = NULL;
  row_data_ = NULL;
  ctxs_ = NULL;
  ObIMicroBlockReader::reset();
  decoder_allocator_.reuse();
  flat_row_reader_.reset();
}

void ObMicroBlockDecoder::reset()
{
  inner_reset();
  ctx_array_.reset();
  if (NULL != local_decoder_pool_) {
    local_decoder_pool_->reset();
    local_decoder_pool_ = NULL;
  }
  decoder_buf_ = NULL;
  request_cnt_ = 0;
  need_release_decoders_.destroy();
  buf_allocator_.reset();
  decoder_allocator_.reset();
}

int ObMicroBlockDecoder::init_decoders()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(NULL == header_ || NULL == col_header_ ||
     (NULL != read_info_ && !read_info_->is_valid()))) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("header should be set while init decoders",
             K(ret), KPC_(read_info), KP_(header), KP_(col_header));
  } else if (OB_FAIL(need_release_decoders_.reserve(request_cnt_ * 2))) {
    LOG_WARN("fail to init release decoders", K(ret), K(request_cnt_));
  } else if (OB_ISNULL(decoder_buf_) &&
       OB_ISNULL(decoder_buf_ = buf_allocator_.alloc((request_cnt_) * sizeof(ObColumnDecoder)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc decoder_buf_ memory", K(ret));
  } else {
    decoders_ = reinterpret_cast<ObColumnDecoder *>(decoder_buf_);

    // perfetch meta data
    /*
    const int64_t meta_len = row_data_ - meta_data_;
    for (int64_t i = 0; i < meta_len - CPU_CACHE_LINE_SIZE; i += CPU_CACHE_LINE_SIZE) {
      __builtin_prefetch(meta_data_ + i);
    }
    */
    if (OB_ISNULL(read_info_) || typeid(ObRowkeyReadInfo) == typeid(*read_info_)) {
      ObObjMeta col_type;
      if (OB_UNLIKELY(header_->column_count_ < request_cnt_ && nullptr == read_info_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("for empty read info, request cnt is invalid", KR(ret), KP(read_info_), KPC(header_), K(request_cnt_));
      }
      int64_t i = 0;
      for ( ; OB_SUCC(ret) && i < header_->column_count_; ++i) {
        col_type.set_type(static_cast<ObObjType>(col_header_[i].obj_type_));
        if (OB_FAIL(add_decoder(i, col_type, decoders_[i]))) {
          LOG_WARN("add_decoder failed", K(ret), K(i), K(col_type));
        }
      }
      for ( ; OB_SUCC(ret) && i < request_cnt_; ++i) { // add nop decoder for not-exist col
        decoders_[i].decoder_ = &none_exist_column_decoder_;
        decoders_[i].ctx_ = &none_exist_column_decoder_ctx_;
      }
    } else {
      const ObColumnIndexArray &cols_index = read_info_->get_columns_index();
      const ObColDescIArray &cols_desc = read_info_->get_columns_desc();
      for (int64_t i = 0; OB_SUCC(ret) && i < request_cnt_; ++i) {
        if (OB_FAIL(add_decoder(
	       cols_index.at(i),
	       cols_desc.at(i).col_type_,
	       decoders_[i]))) {
          LOG_WARN("add_decoder failed", K(ret), "request_idx", i);
	}
      }
    }

    if (OB_FAIL(ret)) {
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(free_decoders())) {
        LOG_ERROR("fail to free decoders", K(tmp_ret));
      }
    }
  }
  return ret;
}

int ObMicroBlockDecoder::add_decoder(const int64_t store_idx, const ObObjMeta &obj_meta, ObColumnDecoder &dest)
{
  int ret = OB_SUCCESS;
  if (store_idx < header_->column_count_ && !obj_meta.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(store_idx), K(obj_meta));
  } else {
    if (store_idx >= header_->column_count_ || store_idx < 0) { // non exist column
      dest.decoder_ = &none_exist_column_decoder_;
      dest.ctx_ = &none_exist_column_decoder_ctx_;
    } else {
      const ObIColumnDecoder *decoder = NULL;
      if (OB_FAIL(acquire(store_idx, decoder))) {
        LOG_WARN("acquire decoder failed", K(ret), K(obj_meta), K(store_idx));
      } else {
        dest.decoder_ = decoder;
        dest.ctx_ = ctxs_[store_idx];
        dest.ctx_->fill(obj_meta, header_, &col_header_[store_idx], &decoder_allocator_.get_inner_allocator());

        int64_t ref_col_idx = -1;
        if (NULL != cached_decoder_ && cached_decoder_->count_ > store_idx) {
          ref_col_idx = cached_decoder_->col_[store_idx].ref_col_idx_;
        } else {
          if (OB_FAIL(decoder->get_ref_col_idx(ref_col_idx))) {
            LOG_WARN("get context param failed", K(ret));
          }
        }

        if (OB_SUCC(ret) && ref_col_idx >= 0) {
          if (OB_FAIL(acquire(ref_col_idx, decoder))) {
            LOG_WARN("acquire decoder failed", K(ret), K(obj_meta), K(ref_col_idx));
          } else {
            dest.ctx_->ref_decoder_ = decoder;
            dest.ctx_->ref_ctx_ = ctxs_[ref_col_idx];
            dest.ctx_->ref_ctx_->fill(obj_meta, header_, &col_header_[ref_col_idx], &decoder_allocator_.get_inner_allocator());
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
      if (OB_LIKELY(request_cnt_ >= read_info.get_request_count() && nullptr != decoder_buf_)) {
        // reuse decoder buffer
        // TODO add capacity to ensure space even the request_cnt decrease
        inner_reset();
      } else {
        reset();
      }
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
      reset();
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
  if (OB_FAIL(get_micro_metas(header_, col_header_, meta_data_,
                                     block_data.get_buf(), block_data.get_buf_size()))) {
    LOG_WARN("get micro block meta failed", K(ret), K(block_data));
  } else if (OB_FAIL(ctx_array_.get_ctx_array(ctxs_, MAX(request_cnt_, header_->column_count_)))) {
    LOG_WARN("get decoder context array failed", K(ret));
  } else if (OB_ISNULL((local_decoder_pool_))
      && OB_ISNULL(local_decoder_pool_ = OB_NEWx(ObDecoderPool, &buf_allocator_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc local decoder pool memory", K(ret));
  } else {
    if (NULL == read_info_) {
      request_cnt_ = header_->column_count_;
    } else {
      request_cnt_ = read_info_->get_request_count();
    }
    row_count_ = header_->row_count_;
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
    int64_t col_count = filter.get_col_count();
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
          LOG_WARN("get row data failed", K(ret), K(index));
        } else {
          ObBitStream bs(reinterpret_cast<unsigned char *>(const_cast<char *>(row_data)), row_len);
          for (int64_t i = 0; OB_SUCC(ret) && i < col_count; i++) {
            ObStorageDatum &datum = datum_buf[i];
            datum.reuse();
            if (OB_FAIL(decoders_[col_offsets.at(i)].decode(datum, row_idx, bs, row_data, row_len))) {
              LOG_WARN("decode cell failed", K(ret), K(row_idx), K(i), K(datum), K(bs), KP(row_data), K(row_len));
            } else if (nullptr == col_params.at(i) || datum.is_null()) {
            } else if (col_params.at(i)->get_meta_type().is_fixed_len_char_type()) {
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
    LOG_TRACE("[PUSHDOWN] micro block black pushdown filter row", K(ret), K(col_params),
              K(pd_filter_info), K(col_offsets),
              K(result_bitmap.popcnt()), K(result_bitmap.size()), K(filter));
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
  } else if (OB_UNLIKELY(0 > col_offset || header_->column_count_ <= col_offset)) {
    ret = OB_INDEX_OUT_OF_RANGE;
    LOG_WARN("Filter column offset out of range", K(ret), K(header_->column_count_), K(col_offset));
  } else {
    col_offset = col_offsets.at(0);
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
            K(header_->row_count_), K(pd_filter_info.start_), K(pd_filter_info.count_),
            K(result_bitmap.popcnt()), K(result_bitmap.size()), K(filter));
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
  } else if (OB_UNLIKELY(0 > col_offset || header_->column_count_ <= col_offset)) {
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
    bool need_padding = nullptr != col_param && col_param->get_meta_type().is_fixed_len_char_type();
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
        } else if (need_padding && OB_FAIL(
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
  LOG_TRACE("[PUSHDOWN] Retrograde when pushdown filter", K(ret), K(filter), KP(col_param),
            K(col_offset), K(result_bitmap.popcnt()), K(result_bitmap.size()), K(filter));
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
        ObColumnHeader::DICT == decoders_[col_offsets.at(0)].ctx_->col_header_->type_) {
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

int ObMicroBlockDecoder::get_rows(
    const common::ObIArray<int32_t> &cols,
    const common::ObIArray<const share::schema::ObColumnParam *> &col_params,
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
      const bool need_padding = nullptr != col_params.at(i) && col_params.at(i)->get_meta_type().is_fixed_len_char_type();
      if (OB_FAIL(get_col_datums(col_id, row_ids, cell_datas, row_cap, col_datums))) {
        LOG_WARN("Failed to get col datums", K(ret), K(i), K(col_id), K(row_cap));
      } else if (need_padding) {
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
  const bool need_padding = is_pad_char_to_full_length(context.sql_mode_) &&
                            col_param.get_meta_type().is_fixed_len_char_type();
  if (OB_FAIL(get_col_datums(col_offset, row_ids, cell_datas, row_cap, datum_buf))) {
    LOG_WARN("Failed to get col datums", K(ret), K(col_offset), K(row_cap));
  } else if (need_padding && OB_FAIL(storage::pad_on_datums(
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
  if (OB_UNLIKELY(col_id >= header_->column_count_)) {
    ret = OB_INDEX_OUT_OF_RANGE;
    LOG_WARN("Vector store col id greate than store cnt", K(ret), K(header_->column_count_), K(col_id));
  } else if (!decoders_[col_id].decoder_->can_vectorized()) {
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

int ObMicroBlockDecoder::get_column_datum(
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
      } else if (col_param.get_meta_type().is_lob_storage() && !datum.is_null() && !datum.get_lob_data().in_row_) {
        if (OB_FAIL(context.lob_locator_helper_->fill_lob_locator_v2(datum, col_param, iter_param, context))) {
          LOG_WARN("Failed to fill lob loactor", K(ret), K(datum), K(context), K(iter_param));
        }
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
  } else if (OB_UNLIKELY(group_by_col >= header_->column_count_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret), K(group_by_col), K(header_->column_count_));
  } else {
     ret = decoders_[group_by_col].get_distinct_count(distinct_cnt);
  }
  return ret;
}

int ObMicroBlockDecoder::read_distinct(
    const int32_t group_by_col,
    const char **cell_datas,
    storage::ObGroupByCell &group_by_cell) const
{
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
    storage::ObGroupByCell &group_by_cell) const
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
      bool need_get_col_datum = (0 == i || agg_col_offset != last_agg_col_offset) && agg_cell->need_access_data();
      if (agg_col_offset == group_by_col) {
        // agg on group by column
        if (OB_FAIL(group_by_cell.eval_batch(group_by_cell.get_group_by_col_datums(), row_cap, i, true))) {
          LOG_WARN("Failed to eval batch", K(ret), K(i));
        }
      } else if (need_get_col_datum && OB_FAIL(get_col_datums(agg_col_offset, row_ids, cell_datas, row_cap, col_datums))) {
        LOG_WARN("Failed to get col datums", K(ret), K(i), K(agg_col_offset), K(row_cap));
      } else if (OB_FAIL(group_by_cell.eval_batch(col_datums, row_cap, i))) {
        LOG_WARN("Failed to eval batch", K(ret), K(i));
      } else if (need_get_col_datum) {
        last_agg_col_offset = agg_col_offset;
      }
      LOG_DEBUG("[GROUP BY PUSHDOWN]", K(ret), K(i), K(group_by_col), K(agg_col_offset), K(group_by_cell), K(need_get_col_datum));
    }
  }
  return ret;
}

int ObMicroBlockDecoder::get_rows(
    const common::ObIArray<int32_t> &cols,
    const common::ObIArray<const share::schema::ObColumnParam *> &col_params,
    const int32_t *row_ids,
    const int64_t row_cap,
    const char **cell_datas,
    const int64_t vec_offset,
    uint32_t *len_array,
    sql::ObEvalCtx &eval_ctx,
    sql::ObExprPtrIArray &exprs)
{
  int ret = OB_SUCCESS;
  decoder_allocator_.reuse();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(nullptr == row_ids || nullptr == cell_datas ||
                         cols.count() != exprs.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(row_ids), KP(cell_datas),
             K(cols.count()), K(exprs.count()));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < cols.count(); i++) {
      const int32_t col_id = cols.at(i);
      sql::ObExpr &expr = *(exprs.at(i));
      const bool need_padding = nullptr != col_params.at(i) && col_params.at(i)->get_meta_type().is_fixed_len_char_type();
      if (0 == vec_offset) {
        const VectorFormat format = need_padding ? VectorFormat::VEC_DISCRETE : expr.get_default_res_format();
        if (OB_FAIL(storage::init_expr_vector_header(expr, eval_ctx, eval_ctx.max_batch_size_, format))) {
          LOG_WARN("Fail to init vector", K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        ObVectorDecodeCtx vector_decode_ctx(
            cell_datas, len_array, row_ids, row_cap, vec_offset, expr.get_vector_header(eval_ctx));
        if (OB_FAIL(get_col_data(col_id, vector_decode_ctx))) {
          LOG_WARN("Failed to get col datums", K(ret), K(i), K(col_id), K(vector_decode_ctx));
        } else if (need_padding && OB_FAIL(storage::pad_on_rich_format_columns(
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
  if (OB_UNLIKELY(col_id >= header_->column_count_)) {
    ret = OB_INDEX_OUT_OF_RANGE;
    LOG_WARN("Vector store col id greate than store cnt", K(ret), K(header_->column_count_), K(col_id));
  } else if (OB_FAIL(decoders_[col_id].decode_vector(row_index_, vector_ctx))) {
    LOG_WARN("fail to get column data from decoder", K(ret), K(col_id), K(vector_ctx));
  }
  return ret;
}

}
}
