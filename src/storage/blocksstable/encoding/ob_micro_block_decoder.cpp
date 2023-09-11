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
#include "share/rc/ob_tenant_base.h"
#include "storage/access/ob_block_row_store.h"

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

class ObDecoderCtxArray
{
public:
  typedef ObColumnDecoderCtx ObDecoderCtx;
  ObDecoderCtxArray() : ctxs_()
  {
    ObMemAttr attr(ob_thread_tenant_id(), "TLDecoderCtxArr");
    attr.sub_ctx_id_ = ObSubCtxIds::THREAD_LOCAL_DECODE_CTX_ID;
    ctxs_.set_attr(attr);
  };
  virtual ~ObDecoderCtxArray()
  {
    ObMemAttr attr(ob_thread_tenant_id(), "TLDecoderCtx");
    attr.sub_ctx_id_ = ObSubCtxIds::THREAD_LOCAL_DECODE_CTX_ID;
    FOREACH(it, ctxs_) {
      ObDecoderCtx *c = *it;
      OB_DELETE(ObDecoderCtx, attr, c);
    }
    ctxs_.reset();
  }

  TO_STRING_KV(K_(ctxs));

  ObDecoderCtx **get_ctx_array(int64_t size)
  {
    int ret = OB_SUCCESS;
    ObDecoderCtx **ctxs = NULL;
    if (size <= 0) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(ret));
    } else {
      ObMemAttr attr(ob_thread_tenant_id(), "TLDecoderCtx");
      attr.sub_ctx_id_ = ObSubCtxIds::THREAD_LOCAL_DECODE_CTX_ID;
      if (ctxs_.count() < size) {
        for (int64_t i = ctxs_.count(); OB_SUCC(ret) && i < size; ++i) {
          ObDecoderCtx *ctx = OB_NEW(ObDecoderCtx, attr);
          if (NULL == ctx) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("alloc memory failed", K(ret));
          } else if (OB_FAIL(ctxs_.push_back(ctx))) {
            LOG_WARN("array push back failed", K(ret));
            OB_DELETE(ObDecoderCtx, attr, ctx);
          }
        }
        if (OB_SUCC(ret)) {
          ctxs = &ctxs_.at(0);
        }
      } else {
        ctxs = &ctxs_.at(0);
      }
    }
    return ctxs;
  }

private:
  static const int64_t LOCAL_CTX_CNT = 128;
  ObSEArray<ObDecoderCtx *, LOCAL_CTX_CNT> ctxs_;

  DISALLOW_COPY_AND_ASSIGN(ObDecoderCtxArray);
};

ObNoneExistColumnDecoder ObMicroBlockDecoder::none_exist_column_decoder_;
ObColumnDecoderCtx ObMicroBlockDecoder::none_exist_column_decoder_ctx_;

class ObTLDecoderCtxArray
{
public:
  ObTLDecoderCtxArray() : ctxs_array_()
  {
    ObMemAttr attr(ob_thread_tenant_id(), "TLDecoderCtxArr");
    attr.sub_ctx_id_ = ObSubCtxIds::THREAD_LOCAL_DECODE_CTX_ID;
    ctxs_array_.set_attr(attr);
  }

  virtual ~ObTLDecoderCtxArray()
  {
    ObMemAttr attr(ob_thread_tenant_id(), "TLDecoderCtx");
    attr.sub_ctx_id_ = ObSubCtxIds::THREAD_LOCAL_DECODE_CTX_ID;
    FOREACH(it, ctxs_array_) {
      ObDecoderCtxArray *ctxs = *it;
      OB_DELETE(ObDecoderCtxArray, attr, ctxs);
    }
  }

  static ObDecoderCtxArray *alloc()
  {
    int ret = OB_SUCCESS;
    ObDecoderCtxArray *ctxs = NULL;
    ObTLDecoderCtxArray *tl_array = instance();
    if (NULL == tl_array) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("NULL instance", K(ret));
    } else if (tl_array->ctxs_array_.empty()) {
      ObMemAttr attr(ob_thread_tenant_id(), "TLDecoderCtx");
      attr.sub_ctx_id_ = ObSubCtxIds::THREAD_LOCAL_DECODE_CTX_ID;
      ctxs = OB_NEW(ObDecoderCtxArray, attr);
      if (NULL == ctxs) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("alloc memory failed", K(ret));
      }
    } else {
      ctxs = tl_array->ctxs_array_.at(tl_array->ctxs_array_.count() - 1);
      tl_array->ctxs_array_.pop_back();
    }
    return ctxs;
  }

  static void free(ObDecoderCtxArray *ctxs)
  {
    int ret = OB_SUCCESS;
    ObMemAttr attr(ob_thread_tenant_id(), "TLDecoderCtx");
    attr.sub_ctx_id_ = ObSubCtxIds::THREAD_LOCAL_DECODE_CTX_ID;
    ObTLDecoderCtxArray *tl_array = instance();
    if (NULL == tl_array) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("NULL instance", K(ret));
    } else if (NULL == ctxs) {
      // do nothing
    } else if (tl_array->ctxs_array_.count() >= MAX_ARRAY_CNT) {
      // reach the threshold, release memory
      OB_DELETE(ObDecoderCtxArray, attr, ctxs);
    } else if (OB_FAIL(tl_array->ctxs_array_.push_back(ctxs))) {
      LOG_WARN("array push back failed", K(ret));
      OB_DELETE(ObDecoderCtxArray, attr, ctxs);
    }
  }

private:
  static ObTLDecoderCtxArray *instance() { return GET_TSI_MULT(ObTLDecoderCtxArray, 1); }

private:
  static const int64_t MAX_ARRAY_CNT = 32;
  ObSEArray<ObDecoderCtxArray *, MAX_ARRAY_CNT> ctxs_array_;

  DISALLOW_COPY_AND_ASSIGN(ObTLDecoderCtxArray);
};

// performance critical, do not check parameters
int ObColumnDecoder::decode(common::ObObj &cell, const int64_t row_id,
    const ObBitStream &bs, const char *data, const int64_t len)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(decoder_->decode(*ctx_, cell, row_id, bs, data, len))) {
    LOG_WARN("decode fail", K(ret), K(row_id), K(len), KP(data), K(bs));
  }
  return ret;
}

int ObColumnDecoder::batch_decode(
    const ObIRowIndex *row_index,
    const int64_t *row_ids,
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
      K(ret), K(row_cap), K(*ctx_), K(ObArrayWrap<int64_t>(row_ids, row_cap)),
      K(ObArrayWrap<ObDatum>(datums, row_cap)));
  return ret;
}

int ObColumnDecoder::get_row_count(
    const ObIRowIndex *row_index,
    const int64_t *row_ids,
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
  ObObj right;
  ObStorageDatum right_datum;
  cmp_ret = 0;
  if (OB_FAIL(decoder_->decode(*ctx_, right, row_id, bs, data, len))) {
    LOG_WARN("decode fail", K(ret), K(row_id), K(len), KP(data), K(bs));
  } else if (OB_FAIL(right_datum.from_obj_enhance(right))) {
    STORAGE_LOG(WARN, "Failed to transfer from  obj to datum", K(ret), K(right));
  } else if (OB_FAIL(cmp_func.compare(left, right_datum, cmp_ret))) {
    STORAGE_LOG(WARN, "Failed to compare datums", K(ret), K(left), K(right_datum));
  }
  return ret;
}


//////////////////////////ObIEncodeBlockGetReader/////////////////////////
ObNoneExistColumnDecoder ObIEncodeBlockReader::none_exist_column_decoder_;
ObColumnDecoderCtx ObIEncodeBlockReader::none_exist_column_decoder_ctx_;
ObIEncodeBlockReader::decode_acquire_func ObIEncodeBlockReader::acquire_funcs_[ObColumnHeader::MAX_TYPE] =
{
    acquire_decoder<ObRawDecoder>,
    acquire_decoder<ObDictDecoder>,
    acquire_decoder<ObRLEDecoder>,
    acquire_decoder<ObConstDecoder>,
    acquire_decoder<ObIntegerBaseDiffDecoder>,
    acquire_decoder<ObStringDiffDecoder>,
    acquire_decoder<ObHexStringDecoder>,
    acquire_decoder<ObStringPrefixDecoder>,
    acquire_decoder<ObColumnEqualDecoder>,
    acquire_decoder<ObInterColSubStrDecoder>
};

ObIEncodeBlockReader::ObIEncodeBlockReader()
  : request_cnt_(0),
    header_(NULL), col_header_(NULL), cached_decocer_(NULL), meta_data_(NULL), row_data_(NULL),
    var_row_index_(), fix_row_index_(), row_index_(&var_row_index_),
    decoders_(nullptr),
    need_release_decoders_(nullptr), need_release_decoder_cnt_(0),
    default_decoders_(), default_release_decoders_(),
    allocator_(NULL), ctx_array_(NULL), ctxs_(NULL),
    decoder_mem_(nullptr), decoder_allocator_(nullptr),
    store_id_array_(NULL), column_type_array_(NULL),
    default_store_ids_(), default_column_types_(),
    need_cast_(false)
{}

ObIEncodeBlockReader::~ObIEncodeBlockReader()
{
  free_decoders();
  if (nullptr != ctx_array_) {
    ObTLDecoderCtxArray::free(ctx_array_);
    ctx_array_ = nullptr;
  }
  if (NULL != decoder_mem_) {
    DESTROY_CONTEXT(decoder_mem_);
    decoder_mem_ = nullptr;
  }
}

void ObIEncodeBlockReader::reuse()
{
  free_decoders();
  cached_decocer_ = nullptr;
  header_ = nullptr;
  request_cnt_ = 0;
  decoders_ = nullptr;
  need_cast_ = false;
  col_header_ = nullptr;
  meta_data_ = nullptr;
  row_data_ = nullptr;
  if (nullptr != ctx_array_) {
    ObTLDecoderCtxArray::free(ctx_array_);
    ctx_array_ = nullptr;
  }
  ctxs_ = nullptr;
  store_id_array_ = nullptr;
  column_type_array_ = nullptr;
  if (nullptr != decoder_mem_) {
    decoder_mem_->reuse_arena();
  }
}

void ObIEncodeBlockReader::reset()
{
  free_decoders();
  cached_decocer_ = NULL;
  header_ = NULL;
  request_cnt_ = 0;
  decoders_ = nullptr;
  need_cast_ = false;
  col_header_ = NULL;
  meta_data_ = NULL;
  row_data_ = NULL;
  allocator_ = NULL;
  if (NULL != ctx_array_) {
    ObTLDecoderCtxArray::free(ctx_array_);
    ctx_array_ = NULL;
  }
  ctxs_ = NULL;
  store_id_array_ = NULL;
  column_type_array_ = NULL;
  decoder_allocator_ = nullptr;
  if (NULL != decoder_mem_) {
    DESTROY_CONTEXT(decoder_mem_);
    decoder_mem_ = nullptr;
  }
}

void ObIEncodeBlockReader::free_decoders()
{
  if (nullptr != need_release_decoders_) {
    for (int64_t i = 0; i < need_release_decoder_cnt_; ++i) {
      release(need_release_decoders_[i]);
    }
    need_release_decoders_ = nullptr;
  }
  need_release_decoder_cnt_ = 0;
}

void ObIEncodeBlockReader::release(const ObIColumnDecoder *decoder)
{
  if (nullptr != decoder) {
    allocator_->free(const_cast<ObIColumnDecoder *>(decoder));
  }
}

int ObIEncodeBlockReader::prepare(const uint64_t tenant_id, const int64_t column_cnt)
{
  int ret = OB_SUCCESS;
  if (nullptr == decoder_mem_) {
    MemoryContext &current_mem = CURRENT_CONTEXT;
    ContextParam param;
    param.set_mem_attr(tenant_id, common::ObModIds::OB_DECODER_CTX, common::ObCtxIds::DEFAULT_CTX_ID)
      .set_properties(USE_TL_PAGE_OPTIONAL);
    if (OB_FAIL(current_mem->CREATE_CONTEXT(decoder_mem_, param))) {
      LOG_WARN("fail to create entity", K(ret));
    } else if (nullptr == decoder_mem_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("memory entity is null", K(ret));
    } else {
      decoder_allocator_ = &decoder_mem_->get_arena_allocator();
    }
  }

  if (OB_FAIL(ret)){
  } else if (column_cnt > DEFAULT_DECODER_CNT) {
    // Size is doubled because we may need two decoders for one column.
    // e.g. column equal encoding, one for column equal decoder and one for referenced column decoder.
    // And total decoders include decoders_ and version_decorder_.
    const int64_t release_count = (column_cnt + 1) * 2;
    if (OB_FAIL(ret)) {
    } else if (nullptr
        == (store_id_array_ = reinterpret_cast<int64_t*>(decoder_allocator_->alloc(
            sizeof(int64_t) * column_cnt)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc memory for store ids fail", K(ret), K(column_cnt));
    } else if (nullptr
        == (column_type_array_ = reinterpret_cast<ObObjMeta*>(decoder_allocator_->alloc(
            sizeof(ObObjMeta) * column_cnt)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc memory for column types fail", K(ret), K(column_cnt));
    } else if (nullptr
        == (decoders_ = reinterpret_cast<ObColumnDecoder*>(decoder_allocator_->alloc(
            sizeof(ObColumnDecoder) * column_cnt)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc memory for decoders fail", K(ret), K(column_cnt));
    } else if (nullptr
        == (need_release_decoders_ = reinterpret_cast<const ObIColumnDecoder**>(decoder_allocator_->alloc(
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

    if (nullptr == allocator_ && OB_ISNULL(allocator_ = get_decoder_allocator())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("get decoder allocator failed", K(ret));
    } else if (OB_ISNULL(ctx_array_ = ObTLDecoderCtxArray::alloc())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc thread local decoder ctx array failed", K(ret));
    } else if (OB_ISNULL(
        ctxs_ = ctx_array_->get_ctx_array(MAX(request_cnt_, header_->column_count_)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("get decoder context array failed", K(ret));
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
        dest.ctx_->fill(obj_meta, header_, &col_header_[store_idx], decoder_allocator_);

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
            dest.ctx_->ref_ctx_->fill(obj_meta, header_, &col_header_[ref_col_idx], decoder_allocator_);
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
    if (OB_FAIL(ObIEncodeBlockReader::acquire_funcs_[col_header_[store_idx].type_](
        *allocator_, *header_, col_header_[store_idx], meta_data_, decoder))) {
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

template <class Decoder>
int ObIEncodeBlockReader::acquire_decoder(ObDecoderAllocator &allocator,
                                 const ObMicroBlockHeader &header,
                                 const ObColumnHeader &col_header,
                                 const char *meta_data,
                                 const ObIColumnDecoder *&decoder)
{
  int ret = OB_SUCCESS;
  Decoder *d = nullptr;
  if (OB_FAIL(allocator.alloc(d))) {
    LOG_WARN("alloc failed", K(ret));
  } else if (OB_UNLIKELY(!col_header.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid column header", K(ret), K(header), K(col_header));
  } else if (OB_FAIL(d->init(header, col_header, meta_data))) {
    LOG_WARN("init decoder failed", K(ret));
  } else {
    decoder = d;
  }

  if (OB_FAIL(ret) && nullptr != d) {
    allocator.free(d);
    decoder = nullptr;
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
  } else if (OB_FAIL(locate_row(rowkey, read_info.get_datum_utils(), row_data, row_len, row_id, found, row))) {
    LOG_WARN("failed to locate row", K(ret), K(rowkey));
  } else {
    if (found) {
      if (OB_FAIL(get_all_columns(row_data, row_len, row_id, row))) {
        LOG_WARN("failed to get left columns", K(ret), K(rowkey), K(row_id));
      }
    } else {
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
  ObObj obj;
  ObBitStream bs(reinterpret_cast<unsigned char *>(const_cast<char *>(row_data)), row_len);
  for (int64_t i = 0; OB_SUCC(ret) && i < request_cnt_; ++i) {
    if (OB_FAIL(decoders_[i].decode(obj, row_id, bs, row_data, row_len))) {
      LOG_WARN("decode cell failed", K(ret), K(row_id), K(i), KP(row_data), K(row_len));
    } else if (OB_FAIL(row.storage_datums_[i].from_obj_enhance(obj))) {
      STORAGE_LOG(WARN, "Failed to transform obj to datum", K(ret), K(i), K(obj));
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
    bool &found,
    ObDatumRow &row)
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
      } else if (row.get_capacity() < request_cnt_) {
        ret = OB_BUF_NOT_ENOUGH;
        LOG_WARN("datum buf is not enough", K(ret), "expect_obj_count", request_cnt_,
            "actual_datum_capacity", row.get_capacity());
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

    if (OB_SUCC(ret)) {
      row.row_flag_.reset();
      if (found) {
        row.count_ = request_cnt_;
        row.row_flag_.set_flag(ObDmlFlag::DF_INSERT);
        row.mvcc_row_flag_.reset();
      } else {
        // not found
        row.row_flag_.set_flag(ObDmlFlag::DF_NOT_EXIST);
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
  } else {
    //TODO @hanhui pass the allocator or datum row from the sstable
    ObArenaAllocator allocator;
    ObDatumRow row;
    if (OB_FAIL(row.init(allocator, rowkey_cnt))) {
      STORAGE_LOG(WARN, "Failed to init ob datum row", K(ret));
    } else if (OB_FAIL(locate_row(rowkey, read_info.get_datum_utils(), row_data, row_len, row_id, found, row))) {
      LOG_WARN("failed to locate row", K(ret), K(rowkey));
    } else if (found) {
      exist = !row.row_flag_.is_delete();
    }
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
    allocator_(nullptr),
    ctx_array_(nullptr),
    ctxs_(nullptr),
    decoder_allocator_(ObModIds::OB_DECODER_CTX, OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()),
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
  if (NULL != cached_decoder_ && store_idx < cached_decoder_->count_) {
    decoder = &cached_decoder_->at(store_idx);
  } else {
    if (OB_FAIL(acquire(*allocator_, *header_, col_header_[store_idx], meta_data_, decoder))) {
      LOG_WARN("acquire decoder failed", K(ret), K(store_idx),
          "column_header", col_header_[store_idx]);
    } else if (OB_FAIL(need_release_decoders_.push_back(decoder))) {
      LOG_WARN("add decoder failed", K(ret), K(store_idx), "column_header", col_header_[store_idx]);
    } else {
      ++need_release_decoder_cnt_;
    }
  }
  return ret;
}

void ObMicroBlockDecoder::release(const ObIColumnDecoder *decoder)
{
  if (NULL != decoder && NULL != allocator_) {
    allocator_->free(const_cast<ObIColumnDecoder *>(decoder));
  }
}

void ObMicroBlockDecoder::free_decoders()
{
  for (int64_t i = 0; i < need_release_decoder_cnt_; ++i) {
    release(need_release_decoders_[i]);
  }
  need_release_decoders_.clear();
  need_release_decoder_cnt_ = 0;
}

void ObMicroBlockDecoder::inner_reset()
{
  free_decoders();
  cached_decoder_ = NULL;
  header_ = NULL;
  col_header_ = NULL;
  meta_data_ = NULL;
  row_data_ = NULL;
  allocator_ = NULL;
  if (NULL != ctx_array_) {
    ObTLDecoderCtxArray::free(ctx_array_);
    ctx_array_ = NULL;
  }
  ctxs_ = NULL;
  ObIMicroBlockReader::reset();
  decoder_allocator_.reuse();
  flat_row_reader_.reset();
}

void ObMicroBlockDecoder::reset()
{
  inner_reset();
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
      const int64_t col_cnt = MIN(request_cnt_, header_->column_count_);
      int64_t i = 0;
      for ( ; OB_SUCC(ret) && i < col_cnt; ++i) {
        col_type.set_type(static_cast<ObObjType>(col_header_[i].obj_type_));
        if (OB_FAIL(add_decoder(i, col_type, decoders_[i]))) {
          LOG_WARN("add_decoder failed", K(ret), K(i), K(col_type));
        }
      }
      if (OB_SUCC(ret) && i < request_cnt_ && OB_ISNULL(read_info_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("for empty read info, request cnt is invalid", KR(ret), KP(read_info_), KPC(header_), K(request_cnt_));
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
      free_decoders();
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
        dest.ctx_->fill(obj_meta, header_, &col_header_[store_idx], &decoder_allocator_);

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
            dest.ctx_->ref_ctx_->fill(obj_meta, header_, &col_header_[ref_col_idx], &decoder_allocator_);
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
  } else if (OB_ISNULL(allocator_ = get_decoder_allocator())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("get decoder allocator failed", K(ret));
  } else if (OB_ISNULL(ctx_array_ = ObTLDecoderCtxArray::alloc())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc thread local decoder ctx array failed", K(ret));
  } else if (OB_ISNULL(ctxs_ = ctx_array_->get_ctx_array(MAX(request_cnt_, header_->column_count_)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("get decoder context array failed", K(ret));
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
                                        common::ObObj *objs)
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
      if (OB_FAIL(decoders_[i].decode(objs[i], row_id, bs, row_data, row_len))) {
        LOG_WARN("decode cell failed", K(ret), K(row_id), K(i), K(bs), KP(row_data), K(row_len));
      }
    }
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
    ObObj obj;
    ObBitStream bs(reinterpret_cast<unsigned char *>(const_cast<char *>(row_data)), row_len);
    for (int64_t i = col_begin; OB_SUCC(ret) && i < col_end; ++i) {
      if (OB_FAIL(decoders_[i].decode(obj, row_id, bs, row_data, row_len))) {
        LOG_WARN("decode cell failed", K(ret), K(row_id), K(i), K(bs), KP(row_data), K(row_len));
      } else if (OB_FAIL(datums[i].from_obj_enhance(obj))) {
        STORAGE_LOG(WARN, "Failed to decode obj to datum", K(ret), K(obj));
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

class EncodingCompareV2
{
public:
  EncodingCompareV2(int &ret, bool &equal, ObMicroBlockDecoder *reader)
  : ret_(ret), equal_(equal), reader_(reader) {}
  ~EncodingCompareV2() {}
  inline bool operator() (const int64_t row_idx, const ObDatumRowkey &rowkey)
  {
    return compare(row_idx, rowkey, true);
  }
  inline bool operator() (const ObDatumRowkey &rowkey, const int64_t row_idx)
  {
    return compare(row_idx, rowkey, false);
  }
private:
  inline bool compare(const int64_t row_idx, const ObDatumRowkey &rowkey, const bool lower_bound)
  {
    bool bret = false;
    int &ret = ret_;
    int32_t compare_result = 0;
    if (OB_FAIL(ret)) {
      // do nothing
    } else if (OB_FAIL(reader_->compare_rowkey(rowkey, row_idx, compare_result))) {
      LOG_WARN("fail to compare rowkey", K(ret));
    } else {
      bret = lower_bound ? compare_result < 0 : compare_result > 0;
      // binary search will keep searching after find the first equal item,
      // if we need the equal result, must prevent it from being modified again
      if (0 == compare_result && !equal_) {
        equal_ = true;
      }
    }
    return bret;
  }
private:
  int &ret_;
  bool &equal_;
  ObMicroBlockDecoder *reader_;
};

class EncodingRangeCompareV2
{
public:
  EncodingRangeCompareV2(int &ret, bool &equal, ObMicroBlockDecoder *reader, int64_t &end_key_begin_idx, int64_t &end_key_end_idx)
  : compare_with_range_(true),
    ret_(ret),
    equal_(equal),
    reader_(reader),
    end_key_begin_idx_(end_key_begin_idx),
    end_key_end_idx_(end_key_end_idx) {}
  ~EncodingRangeCompareV2() {}
  inline bool operator() (const int64_t row_idx, const ObDatumRange &range)
  {
    return compare(row_idx, range, true);
  }
  inline bool operator() (const ObDatumRange &range, const int64_t row_idx)
  {
    return compare(row_idx, range, false);
  }
private:
  inline bool compare(const int64_t row_idx, const ObDatumRange &range, const bool lower_bound)
  {
    bool bret = false;
    int &ret = ret_;
    int32_t start_key_compare_result = 0;
    int32_t end_key_compare_result = 0;
    if (OB_FAIL(ret)) {
      // do nothing
    } else if (compare_with_range_ && OB_FAIL(reader_->compare_rowkey(range, row_idx, start_key_compare_result, end_key_compare_result))) {
      LOG_WARN("fail to compare rowkey", K(ret));
    } else if (!compare_with_range_ && OB_FAIL(reader_->compare_rowkey(range.get_start_key(), row_idx, start_key_compare_result))) {
      LOG_WARN("fail to compare rowkey", K(ret));
    } else {
      bret = lower_bound ? start_key_compare_result < 0 : start_key_compare_result > 0;
      // binary search will keep searching after find the first equal item,
      // if we need the equal result, must prevent it from being modified again
      if (0 == start_key_compare_result && !equal_) {
        equal_ = true;
      }

      if (compare_with_range_) {
        if (start_key_compare_result > 0) {
          if (end_key_compare_result < 0) {
            end_key_begin_idx_ = row_idx;
          }
          if (end_key_compare_result > 0 && row_idx < end_key_end_idx_) {
            end_key_end_idx_ = row_idx;
          }
        }

        if (start_key_compare_result >= 0 && end_key_compare_result < 0) {
          compare_with_range_ = false;
        }
      }
    }
    return bret;
  }
private:
  bool compare_with_range_;
  int &ret_;
  bool &equal_;
  ObMicroBlockDecoder *reader_;
  int64_t &end_key_begin_idx_;
  int64_t &end_key_end_idx_;
};

int ObMicroBlockDecoder::find_bound(
    const ObDatumRowkey &key,
    const bool lower_bound,
    const int64_t begin_idx,
    int64_t &row_idx,
    bool &equal)
{
  int ret = OB_SUCCESS;
  equal = false;
  row_idx = ObIMicroBlockReaderInfo::INVALID_ROW_INDEX;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init");
  } else if (OB_UNLIKELY(!key.is_valid() || begin_idx < 0 || nullptr == datum_utils_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(begin_idx), K_(row_count), KPC_(datum_utils));
  } else if (key.get_datum_cnt() <= 0 || key.get_datum_cnt() > datum_utils_->get_rowkey_count()) {
    ret = common::OB_INVALID_ARGUMENT;
    LOG_WARN("invalid compare column count", K(ret), K(key.get_datum_cnt()), K(datum_utils_->get_rowkey_count()));
  } else {
    EncodingCompareV2 encoding_compare(ret, equal, this);
    ObRowIndexIterator begin_iter(begin_idx);
    ObRowIndexIterator end_iter(row_count_);
    ObRowIndexIterator found_iter;
    if (lower_bound) {
      found_iter = std::lower_bound(begin_iter, end_iter, key, encoding_compare);
    } else {
      found_iter = std::upper_bound(begin_iter, end_iter, key, encoding_compare);
    }
    if (OB_FAIL(ret)) {
      LOG_WARN("fail to lower bound rowkey", K(ret));
    } else {
      row_idx = *found_iter;
    }
  }
  return ret;
}

int ObMicroBlockDecoder::find_bound(
    const ObDatumRange &range,
    const int64_t begin_idx,
    int64_t &row_idx,
    bool &equal,
    int64_t &end_key_begin_idx,
    int64_t &end_key_end_idx)
{
  int ret = OB_SUCCESS;
  equal = false;
  row_idx = ObIMicroBlockReaderInfo::INVALID_ROW_INDEX;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init");
  } else if (OB_UNLIKELY(!range.is_valid() || begin_idx < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(begin_idx), K_(row_count));
  } else {
    EncodingRangeCompareV2 encoding_compare(ret, equal, this, end_key_begin_idx, end_key_end_idx);
    ObRowIndexIterator begin_iter(begin_idx);
    ObRowIndexIterator end_iter(row_count_);
    ObRowIndexIterator found_iter;
    found_iter = std::lower_bound(begin_iter, end_iter, range, encoding_compare);
    if (OB_FAIL(ret)) {
      LOG_WARN("fail to lower bound rowkey", K(ret));
    } else {
      row_idx = *found_iter;
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
      ObObj store_obj;
      ObObjMeta col_type;
      for (int64_t i = 0; OB_SUCC(ret) && i < compare_column_count && 0 == compare_result; ++i) {
        col_type.set_type(static_cast<ObObjType>(col_header_[i].obj_type_));
        store_obj.set_meta_type(col_type);
        if (OB_FAIL((decoders_ + i)->decode(store_obj, index, bs, row_data, row_len))) {
          LOG_WARN("fail to decode obj", K(ret), K(index), K(i));
        } else if (OB_FAIL(store_datum.from_obj_enhance(store_obj))) {
          STORAGE_LOG(WARN, "failed to transfer obj to datum", K(ret), K(store_obj));
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
      ObObjMeta col_type;
      ObObj store_obj;
      ObStorageDatum store_datum;
      for (int64_t i = 0; OB_SUCC(ret) && i < compare_column_count && 0 == start_key_compare_result; ++i) {
	col_type.set_type(static_cast<ObObjType>(col_header_[i].obj_type_));
        store_obj.set_meta_type(col_type);
        if (OB_FAIL((decoders_ + i)->decode(store_obj, index, bs, row_data, row_len))) {
          LOG_WARN("fail to decode obj", K(ret), K(index), K(i));
        } else if (OB_FAIL(store_datum.from_obj_enhance(store_obj))) {
          STORAGE_LOG(WARN, "failed to transfer obj to datum", K(ret), K(store_obj));
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
    int64_t &version,
    int64_t &sql_sequence)
{
  UNUSEDx(row_idx, schema_rowkey_cnt, row_header, version, sql_sequence);
  return OB_NOT_SUPPORTED;
}

int ObMicroBlockDecoder::filter_pushdown_filter(
    const sql::ObPushdownFilterExecutor *parent,
    sql::ObBlackFilterExecutor &filter,
    const storage::PushdownFilterInfo &pd_filter_info,
    common::ObBitmap &result_bitmap)
{
  int ret = OB_SUCCESS;
  common::ObObj *col_buf = pd_filter_info.col_buf_;
  const int64_t col_capacity = pd_filter_info.col_capacity_;
  if (OB_UNLIKELY(pd_filter_info.start_ < 0 || pd_filter_info.end_ > row_count_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret), K(row_count_), K(pd_filter_info.start_), K(pd_filter_info.end_));
  } else if (OB_FAIL(validate_filter_info(filter, col_buf, col_capacity, header_))) {
    LOG_WARN("Failed to validate filter info", K(ret));
  } else {
    int64_t col_count = filter.get_col_count();
    const common::ObIArray<int32_t> &col_offsets = filter.get_col_offsets();
    const sql::ColumnParamFixedArray &col_params = filter.get_col_params();
    decoder_allocator_.reuse();
    for (int64_t row_idx = pd_filter_info.start_; OB_SUCC(ret) && row_idx < pd_filter_info.end_; row_idx++)  {
      if (nullptr != parent && parent->can_skip_filter(row_idx)) {
        continue;
      } else if (0 < col_count) {
        int64_t row_len = 0;
        const char *row_data = nullptr;
        if (OB_FAIL(row_index_->get(row_idx, row_data, row_len))) {
          LOG_WARN("get row data failed", K(ret), K(index));
        } else {
          ObBitStream bs(reinterpret_cast<unsigned char *>(const_cast<char *>(row_data)), row_len);
          for (int64_t i = 0; OB_SUCC(ret) && i < col_count; i++) {
            ObObj &obj = col_buf[i];
            int64_t col_idx = col_offsets.at(i);
            if (OB_FAIL(decoders_[col_idx].decode(obj, row_idx, bs, row_data, row_len))) {
              LOG_WARN("decode cell failed", K(ret), K(row_idx), K(i), K(obj), K(bs), KP(row_data), K(row_len));
            } else if (nullptr != col_params.at(i) &&
                       OB_FAIL(storage::pad_column(col_params.at(i)->get_accuracy(), decoder_allocator_, obj))) {
              LOG_WARN("Failed to pad column", K(ret), K(i), K(col_idx));
            }
          }
        }
      }
      bool filtered = false;
      if (OB_SUCC(ret)) {
        if (OB_FAIL(filter.filter(col_buf, col_count, filtered))) {
          LOG_WARN("Failed to filter row with black filter", K(ret), "col_buf", common::ObArrayWrap<common::ObObj>(col_buf, col_count));
        } else if (!filtered) {
          if (OB_FAIL(result_bitmap.set(row_idx))) {
            LOG_WARN("Failed to set result bitmap", K(ret), K(row_idx));
          }
        }
      }
    }
    LOG_TRACE("[PUSHDOWN] micro block black pushdown filter row", K(ret), K(col_params),
              K(col_offsets), K(result_bitmap.popcnt()), K(result_bitmap.size()), K(filter));
  }
  return ret;
}

int ObMicroBlockDecoder::filter_pushdown_filter(
    const sql::ObPushdownFilterExecutor *parent,
    sql::ObWhiteFilterExecutor &filter,
    const storage::PushdownFilterInfo &pd_filter_info,
    ObBitmap &result_bitmap)
{
  int ret = OB_SUCCESS;
  int32_t col_offset = 0;
  ObColumnDecoder* column_decoder = nullptr;
  common::ObObj *col_buf = pd_filter_info.col_buf_;
  const int64_t col_capacity = pd_filter_info.col_capacity_;
  const common::ObIArray<int32_t> &col_offsets = filter.get_col_offsets();
  const sql::ColumnParamFixedArray &col_params =filter.get_col_params();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("Micro block decoder not inited", K(ret));
  } else if (OB_UNLIKELY(pd_filter_info.start_ < 0 || pd_filter_info.end_ > row_count_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret), K(row_count_), K(pd_filter_info.start_), K(pd_filter_info.end_));
  } else if (1 != filter.get_col_count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected col_ids count: not 1", K(ret), K(filter));
  } else if (OB_UNLIKELY(sql::WHITE_OP_MAX <= filter.get_op_type() ||
                         !result_bitmap.is_inited())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid operator type of Filter node",
             K(ret), K(filter), K(result_bitmap.is_inited()));
  } else if (OB_ISNULL(col_buf) && 0 < col_capacity) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected null col buf", K(ret), K(col_buf), K(col_capacity));
  } else if (FALSE_IT(col_offset = col_offsets.at(0))) {
  } else if (OB_UNLIKELY(0 > col_offset || header_->column_count_ <= col_offset)) {
    ret = OB_INDEX_OUT_OF_RANGE;
    LOG_WARN("Filter column offset out of range", K(ret), K(header_->column_count_), K(col_offset));
  } else if (OB_ISNULL(column_decoder = decoders_ + col_offset)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Null pointer of column decoder", K(ret));
  } else {
    const sql::ObWhiteFilterOperatorType op_type = filter.get_op_type();
    column_decoder->ctx_->set_col_param(col_params.at(0));
    if (filter.null_param_contained() &&
        op_type != sql::WHITE_OP_NU &&
        op_type != sql::WHITE_OP_NN &&
        op_type != sql::WHITE_OP_IN) {
    } else if (OB_FAIL(column_decoder->decoder_->pushdown_operator(
                parent,
                *column_decoder->ctx_,
                filter,
                meta_data_,
                row_index_,
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
                                          col_buf[0],
                                          result_bitmap))) {
          LOG_WARN("Retrograde path failed.", K(ret), K(filter), KPC(column_decoder->ctx_));
        }
      }
    }
  }

  LOG_TRACE("[PUSHDOWN] white pushdown filter row", K(ret), "need_padding", nullptr != col_params.at(0), K(col_offset),
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
    const storage::PushdownFilterInfo &pd_filter_info,
    const int32_t col_offset,
    const share::schema::ObColumnParam *col_param,
    common::ObObj &decoded_obj,
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
    const ObIArray<ObObj> &ref_objs = filter.get_objs();
    const sql::ObWhiteFilterOperatorType op_type = filter.get_op_type();
    decoder_allocator_.reuse();
    for (int64_t row_id = pd_filter_info.start_; OB_SUCC(ret) && row_id < pd_filter_info.end_; ++row_id) {
      if (nullptr != parent && parent->can_skip_filter(row_id)) {
        continue;
      }
      const char *row_data = nullptr;
      int64_t row_len = 0;
      if (OB_FAIL(row_index_->get(row_id, row_data, row_len))) {
        LOG_WARN("get row data failed", K(ret), K(index));
      } else {
        ObBitStream bs(reinterpret_cast<unsigned char *>(const_cast<char *>(row_data)), row_len);
        if (OB_FAIL(decoders_[col_offset].decode(decoded_obj, row_id, bs, row_data, row_len))) {
          LOG_WARN("decode cell failed", K(ret), K(row_id), K(bs), KP(row_data), K(row_len));
        } else if (nullptr != col_param &&
                   OB_FAIL(storage::pad_column(col_param->get_accuracy(), decoder_allocator_, decoded_obj))) {
          LOG_WARN("Failed to pad column", K(ret), K(col_offset), K(row_id));
        }

        bool filtered = false;
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(filter_white_filter(filter, decoded_obj, filtered))) {
          LOG_WARN("Failed to filter row with white filter", K(ret), K(row_id), K(decoded_obj));
        } else if (!filtered && OB_FAIL(result_bitmap.set(row_id))) {
          LOG_WARN("Failed to set result bitmap", K(ret), K(row_id));
        }
      }
    }
  }
  LOG_TRACE("[PUSHDOWN] Retrograde when pushdown filter", K(ret), K(filter), KP(col_param),
            K(col_offset), K(result_bitmap.popcnt()), K(result_bitmap.size()), K(filter));
  return ret;
}

int ObMicroBlockDecoder::get_rows(
    const common::ObIArray<int32_t> &cols,
    const common::ObIArray<const share::schema::ObColumnParam *> &col_params,
    const int64_t *row_ids,
    const char **cell_datas,
    const int64_t row_cap,
    common::ObIArray<ObDatum *> &datums)
{
  int ret = OB_SUCCESS;
  decoder_allocator_.reuse();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(nullptr == row_ids || nullptr == cell_datas ||
                         cols.count() != datums.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(row_ids), KP(cell_datas),
             K(cols.count()), K(datums.count()));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < cols.count(); i++) {
      int32_t col_id = cols.at(i);
      common::ObDatum *col_datums = datums.at(i);
      if (OB_FAIL(get_col_datums(col_id, row_ids, cell_datas, row_cap, col_datums))) {
        LOG_WARN("Failed to get col datums", K(ret), K(i), K(col_id), K(row_cap));
      } else if (nullptr != col_params.at(i)) {
        // need padding
        if (OB_FAIL(storage::pad_on_datums(
                    col_params.at(i)->get_accuracy(),
                    col_params.at(i)->get_meta_type().get_collation_type(),
                    decoder_allocator_,
                    row_cap,
                    datums.at(i)))) {
          LOG_WARN("fail to pad on datums", K(ret), K(i), K(row_cap));
        }
      }
    }
  }
  return ret;
}

int ObMicroBlockDecoder::get_row_count(
    int32_t col_id,
    const int64_t *row_ids,
    const int64_t row_cap,
    const bool contains_null,
    int64_t &count)
{
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
             "row_ids", common::ObArrayWrap<const int64_t>(row_ids, row_cap));
  }
  return ret;
}

int ObMicroBlockDecoder::get_min_or_max(
    int32_t col_id,
    const int64_t *row_ids,
    const char **cell_datas,
    const int64_t row_cap,
    ObDatum *datum_buf,
    ObMicroBlockAggInfo<ObDatum> &agg_info)
{
  int ret = OB_SUCCESS;
  decoder_allocator_.reuse();
  if (OB_FAIL(get_col_datums(col_id, row_ids, cell_datas, row_cap, datum_buf))) {
    LOG_WARN("Failed to get col datums", K(ret), K(col_id), K(row_cap));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < row_cap; ++i) {
      if (datum_buf[i].is_nop()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected datum, can not process in batch", K(ret), K(i));
      } else if (OB_FAIL(agg_info.update_min_or_max(datum_buf[i]))) {
        LOG_WARN("fail to update_min_or_max", K(ret), K(i), K(datum_buf[i]), K(agg_info));
      } else {
        LOG_DEBUG("update min/max", K(i), K(datum_buf[i]), K(agg_info));
      }
    }
  }
  return ret;
}

int ObMicroBlockDecoder::get_col_datums(
    int32_t col_id,
    const int64_t *row_ids,
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
        if (OB_FAIL(decoders_[col_id].decode(cell, row_id, bs, row_data, row_len))) {
          LOG_WARN("Decode cell failed", K(ret));
        } else if (OB_FAIL(col_datums[idx].from_obj(cell))) {
          LOG_WARN("Failed to convert object from datum", K(ret), K(cell));
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
              "row_ids", common::ObArrayWrap<const int64_t>(row_ids, row_cap));
  }
  return ret;
}

}
}
