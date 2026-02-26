/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX SERVER
#include "observer/table_load/backup/ob_table_load_backup_micro_block_decoder.h"

namespace oceanbase
{
namespace observer
{
namespace table_load_backup
{

using namespace common;
using namespace storage;

static const ObMemAttr tl_decode_attr(common::OB_SERVER_TENANT_ID, "TLD_DecoderCtx");

/**
 * ObMicroBlockDecoder
 */
template <typename T>
int ObDecoderArrayAllocator::alloc(T *&t)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == buf_)) {
    LOG_WARN("not init", KR(ret));
  } else {
    t = new (buf_ + offset_) T();
    offset_ += sizeof(T);
  }
  return ret;
}

/**
 * ObDecoderCtxArray
 */
ObColumnDecoderCtx **ObDecoderCtxArray::get_ctx_array(int64_t size)
{
  int ret = OB_SUCCESS;
  ObDecoderCtx **ctxs = nullptr;
  if (OB_UNLIKELY(size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret));
  } else {
    if (ctxs_.count() < size) {
      for (int64_t i = ctxs_.count(); OB_SUCC(ret) && i < size; ++i) {
        ObDecoderCtx *ctx = nullptr;
        if (OB_ISNULL(ctx = OB_NEW(ObDecoderCtx, tl_decode_attr))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("alloc memory failed", KR(ret));
        } else if (OB_FAIL(ctxs_.push_back(ctx))) {
          LOG_WARN("array push back failed", KR(ret));
          OB_DELETE(ObDecoderCtx, tl_decode_attr, ctx);
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

/**
 * ObTLDecoderCtxArray
 */
ObDecoderCtxArray *ObTLDecoderCtxArray::alloc()
{
  int ret = OB_SUCCESS;
  ObDecoderCtxArray *ctxs = nullptr;
  ObTLDecoderCtxArray *tl_array = instance();
  if (OB_UNLIKELY(nullptr == tl_array)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("nullptr instance", KR(ret));
  } else if (tl_array->ctxs_array_.empty()) {
    ctxs = nullptr;
    if (OB_ISNULL(ctxs = OB_NEW(ObDecoderCtxArray, tl_decode_attr))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc memory failed", KR(ret));
    }
  } else {
    ctxs = tl_array->ctxs_array_.at(tl_array->ctxs_array_.count() - 1);
    tl_array->ctxs_array_.pop_back();
  }
  return ctxs;
}

void ObTLDecoderCtxArray::free(ObDecoderCtxArray *ctxs)
{
  int ret = OB_SUCCESS;
  ObTLDecoderCtxArray *tl_array = instance();
  if (OB_UNLIKELY(nullptr == tl_array)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("nullptr instance", KR(ret));
  } else if (nullptr == ctxs) {
    // do nothing
  } else if (tl_array->ctxs_array_.count() >= MAX_ARRAY_CNT) {
    // reach the threshold, release memory
    OB_DELETE(ObDecoderCtxArray, tl_decode_attr, ctxs);
  } else if (OB_FAIL(tl_array->ctxs_array_.push_back(ctxs))) {
    LOG_WARN("array push back failed", KR(ret));
    OB_DELETE(ObDecoderCtxArray, tl_decode_attr, ctxs);
  }
}

/**
 * ObColumnDecoder
 */
int ObColumnDecoder::decode(
    common::ObObj &cell,
    const int64_t row_id,
    const ObBitStream &bs,
    const char *data,
    const int64_t len)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(decoder_->decode(*ctx_, cell, row_id, bs, data, len))) {
    LOG_WARN("decode fail", KR(ret), K(row_id), K(len), KP(data), K(bs));
  } else if (OB_NOT_NULL(ctx_->col_header_) && ctx_->col_header_->is_store_nbr_int() && cell.is_int()) {
    number::ObNumber number;
    if (OB_FAIL(number.from(cell.get_int(), *ctx_->allocator_))) {
      LOG_WARN("convert to number fail", KR(ret), K(cell));
    } else {
      if (ctx_->int_decode_unbr_) {
        cell.set_unumber(number);
      } else {
        cell.set_number(number);
      }
    }
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
  if (OB_UNLIKELY(nullptr == row_index || nullptr == row_ids || nullptr == cell_datas ||
      nullptr == datums || 0 >= row_cap)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid nullptr row_iter or datums", KR(ret), KP(row_index), KP(row_ids),
        KP(cell_datas), KP(datums), K(row_cap));
  } else if (OB_UNLIKELY(!decoder_->can_vectorized())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpect column batch_decdoe not supported.", KR(ret));
  } else if (OB_FAIL(decoder_->batch_decode(*ctx_, row_index, row_ids, cell_datas, row_cap, datums))) {
    LOG_WARN("Failed to batch decode data to datum in column decoder", KR(ret), K(*ctx_));
  }

  LOG_TRACE("[Batch decode] Batch decoded datums: ", KR(ret), K(row_cap), K(*ctx_),
      K(ObArrayWrap<int64_t>(row_ids, row_cap)), K(ObArrayWrap<ObDatum>(datums, row_cap)));
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
    LOG_WARN("Failed to get nullptr count from column decoder", KR(ret), K(*ctx_));
  } else {
    count = row_cap - null_count;
  }
  return ret;
}

// performance critical, do not check parameters
int ObColumnDecoder::quick_compare(
    const ObObj &left,
    const int64_t row_id,
    const ObBitStream &bs,
    const char *data,
    const int64_t len,
    int32_t &cmp_ret)
{
  int ret = OB_SUCCESS;
  ObObj right;
  cmp_ret = 0;
  if (OB_FAIL(decoder_->decode(*ctx_, right, row_id, bs, data, len))) {
    LOG_WARN("decode fail", KR(ret), K(row_id), K(len), KP(data), K(bs));
  } else if (OB_ISNULL(ctx_->col_header_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("col header is nullptr", KR(ret));
  } else if (ctx_->col_header_->is_store_nbr_int() && right.is_int()) {
    ObObj convert_left;
    bool is_converted = false;
    if (OB_FAIL(try_convert_number_to_int(left, convert_left, is_converted))) {
      LOG_WARN("try convert number fail", KR(ret), K(left));
    } else if (is_converted) { // can use integer compare
      const int64_t left_int = convert_left.get_int();
      const int64_t right_int = right.get_int();
      cmp_ret = (left_int == right_int) ? ObObjCmpFuncs::CR_EQ :
          ((left_int > right_int) ? ObObjCmpFuncs::CR_GT : ObObjCmpFuncs::CR_LT);
    } else { // convert right back and use number compare
      const int64_t MAX_DIGIT_CNT = 16;
      char buf_alloc[MAX_DIGIT_CNT];
      ObDataBuffer num_allocator(buf_alloc, MAX_DIGIT_CNT);
      number::ObNumber right_number;
      if (OB_FAIL(right_number.from(right.get_int(), num_allocator))) {
        LOG_WARN("convert to number fail", KR(ret), K(right));
      } else {
        if (ctx_->int_decode_unbr_) {
          right.set_unumber(right_number);
        } else {
          right.set_number(right_number);
        }
        cmp_ret = left.compare(right);
      }
    }
  } else {
    cmp_ret = left.compare(right);
  }
  return ret;
}

int ObColumnDecoder::try_convert_number_to_int(
    const ObObj &original,
    ObObj &target,
    bool &is_converted)
{
  int ret = OB_SUCCESS;
  is_converted = false;
  number::ObNumber ori_number;
  int64_t target_int = 0;
  target.reset();
  if (original.is_number() && OB_FAIL(original.get_number(ori_number))) {
    LOG_WARN("cast to number fail", KR(ret), K(original));
  } else if (original.is_unumber() && OB_FAIL(original.get_unumber(ori_number))) {
    LOG_WARN("cast to unumber fail", KR(ret), K(original));
  } else if (OB_FAIL(ori_number.cast_to_int64(target_int))) {
    if (OB_INTEGER_PRECISION_OVERFLOW == ret) {
      ret = OB_SUCCESS; // ori_number is not an int64, ignore
    } else {
      LOG_WARN("cast to integer fail", KR(ret), K(original), K(ori_number));
    }
  } else {
    target.set_int(target_int);
    is_converted = true;
  }
  return ret;
}

/**
 * ObMicroBlockDecoder
 */
ObNoneExistColumnDecoder ObMicroBlockDecoder::none_exist_column_decoder_;
ObColumnDecoderCtx ObMicroBlockDecoder::none_exist_column_decoder_ctx_;

ObMicroBlockDecoder::ObMicroBlockDecoder()
  : allocator_(nullptr),
    buf_allocator_("TLD_MiBDB"),
    cast_allocator_("TLD_MiBDC"),
    decoder_allocator_("TLD_MiBDD"),
    column_map_(nullptr),
    request_cnt_(0),
    store_cnt_(0),
    rowkey_cnt_(0),
    decoder_buf_(nullptr),
    decoders_(nullptr),
    block_header_(nullptr),
    col_header_(nullptr),
    meta_data_(nullptr),
    ctx_array_(nullptr),
    ctxs_(nullptr),
    row_data_(nullptr),
    cached_decoder_header_(nullptr),
    need_cast_(false),
    row_index_(&var_row_index_),
    need_release_decoders_(),
    need_release_decoder_cnt_(0),
    is_inited_(false)
{
  buf_allocator_.set_tenant_id(MTL_ID());
  cast_allocator_.set_tenant_id(MTL_ID());
  decoder_allocator_.set_tenant_id(MTL_ID());
}

ObMicroBlockDecoder::~ObMicroBlockDecoder()
{
  decoder_buf_ = nullptr;
  buf_allocator_.reset();
  cast_allocator_.reset();
  decoder_allocator_.reset();
}

int ObMicroBlockDecoder::init(
    const ObMicroBlockData *micro_block_data,
    const ObTableLoadBackupVersion &backup_version,
    const ObIColumnMap *column_map)
{
  UNUSED(backup_version);
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("already init", KR(ret));
  } else if (OB_UNLIKELY(micro_block_data == nullptr || !micro_block_data->is_valid() || column_map == nullptr || !column_map->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(micro_block_data), KP(column_map));
  } else {
    column_map_ = column_map;
    request_cnt_ = column_map_->get_request_count();
    store_cnt_ = column_map_->get_store_count();
    rowkey_cnt_ = column_map_->get_rowkey_store_count();
    if (OB_FAIL(get_micro_metas(
        micro_block_data->get_buf(), micro_block_data->get_buf_size(), store_cnt_, block_header_, col_header_, meta_data_))) {
      LOG_WARN("get micro block meta failed", KR(ret), KPC(micro_block_data), K_(store_cnt));
    } else if (OB_UNLIKELY(!block_header_->is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid micro_block_header", KR(ret), K(*block_header_));
    } else if (OB_ISNULL(allocator_ = get_decoder_allocator())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("get decoder allocator failed", KR(ret));
    } else if (OB_ISNULL(ctx_array_ = ObTLDecoderCtxArray::alloc())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc thread local decoder ctx array failed", KR(ret));
    } else if (OB_ISNULL(ctxs_ = ctx_array_->get_ctx_array(std::max(request_cnt_, store_cnt_)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("get decoder context array failed", KR(ret));
    } else {
      row_data_ = micro_block_data->get_buf() + block_header_->row_data_offset_;
      const int64_t row_data_len = micro_block_data->get_buf_size() - block_header_->row_data_offset_;

      if (nullptr != micro_block_data->get_extra_buf() && micro_block_data->get_extra_size() > 0) {
        cached_decoder_header_ = reinterpret_cast<const ObBlockCachedDecoderHeader *>(micro_block_data->get_extra_buf());
      }

      for (int64_t i = 0; !need_cast_ && i < request_cnt_; ++i) {
        const ObIColumnIndexItem *column_index_item = column_map_->get_column_index(i);
        if (!column_index_item->get_is_column_type_matched() &&
            column_index_item->get_store_index() >= 0) {
          need_cast_ = true;
        }
      }

      if (block_header_->row_index_byte_ > 0) {
        if (OB_FAIL(var_row_index_.init(
            row_data_, row_data_len, block_header_->row_count_, block_header_->row_index_byte_))) {
          LOG_WARN("init var row idx failed", KR(ret), KP_(row_data), K(row_data_len), K(*block_header_));
        }
        row_index_ = &var_row_index_;
      } else {
        if (OB_FAIL(fix_row_index_.init(row_data_, row_data_len, block_header_->row_count_))) {
          LOG_WARN("init fix row idx failed", KR(ret), KP_(row_data), K(row_data_len), K(*block_header_));
        }
        row_index_ = &fix_row_index_;
      }

      if (OB_SUCC(ret)) {
        if (decoder_buf_ == nullptr) {
          if (OB_ISNULL(decoder_buf_ = buf_allocator_.alloc((request_cnt_) * sizeof(ObColumnDecoder)))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("fail to alloc memory", KR(ret));
          } else {
            decoders_ = reinterpret_cast<ObColumnDecoder *>(decoder_buf_);
          }
        }
      }

      if (OB_SUCC(ret)) {
        if (OB_FAIL(init_decoders())) {
          LOG_WARN("init decoders failed", KR(ret));
        } else {
          for (int64_t i = 0; OB_SUCC(ret) && i < request_cnt_; i++) {

            const int64_t idx = column_map->get_column_index(i)->get_store_index();
            if (idx >= 0 && idx < rowkey_cnt_) {
              if (OB_ISNULL(decoders_[i].decoder_)) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("rowkey column not exist in column map", KR(ret), K(i));
              }
            }
          }
        }
      }
      if (OB_SUCC(ret)) {
        is_inited_ = true;
      }
    }
  }
  return ret;
}

int ObMicroBlockDecoder::get_micro_metas(
    const char *block,
    const int64_t block_size,
    const int64_t col_cnt,
    const ObMicroBlockHeaderV2 *&header,
    const ObColumnHeader *&col_header,
    const char *&meta_data)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == block || block_size <= 0 || col_cnt <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KP(block), K(block_size), K(col_cnt));
  } else {
    header = reinterpret_cast<const ObMicroBlockHeaderV2 *>(block);
    block += sizeof(*header);
    col_header = reinterpret_cast<const ObColumnHeader *>(block);
    meta_data = block + sizeof(*col_header) * col_cnt;
    if (meta_data - block > block_size || header->row_data_offset_ > block_size) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("block data buffer not enough", KR(ret), KP(block), K(block_size), K(meta_data - block), K(*header));
    }
  }
  return ret;
}

int ObMicroBlockDecoder::init_decoders()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == block_header_ || nullptr == col_header_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("header should be set while init decoders", KR(ret), KP(block_header_), KP(col_header_));
  } else {
    ObObjMeta obj_meta;
    int64_t store_idx = -1;
    for (int64_t i = 0; OB_SUCC(ret) && i < request_cnt_; ++i) {
      const ObIColumnIndexItem *column_index_item = column_map_->get_column_index(i);
      store_idx = column_index_item->get_store_index();
      obj_meta = column_index_item->get_request_column_type();
      if (store_idx >= 0 && col_header_[store_idx].is_store_nbr_int()) {
        ctxs_[store_idx]->int_decode_unbr_ = obj_meta.is_unumber();
        obj_meta.set_int();
      }
      if (OB_FAIL(add_decoder(store_idx, obj_meta, decoders_[i]))) {
        LOG_WARN("add_decoder failed", KR(ret), K(i), K(request_cnt_));
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
  if (store_idx >= 0 && !obj_meta.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), K(store_idx), K(obj_meta));
  } else {
    if (store_idx < 0) { // non exist column
      dest.decoder_ = &none_exist_column_decoder_;
      dest.ctx_ = &none_exist_column_decoder_ctx_;
    } else {
      const ObIColumnDecoder *decoder = nullptr;
      if (OB_FAIL(acquire(obj_meta, store_idx, decoder))) {
        LOG_WARN("acquire decoder failed", KR(ret), K(obj_meta), K(store_idx));
      } else {
        dest.decoder_ = decoder;
        dest.ctx_ = ctxs_[store_idx];
        dest.ctx_->fill(obj_meta, block_header_, &col_header_[store_idx], &decoder_allocator_);

        int64_t ref_col_idx = -1;
        if (nullptr != cached_decoder_header_ && cached_decoder_header_->count_ > store_idx) {
          ref_col_idx = cached_decoder_header_->col_[store_idx].ref_col_idx_;
        } else {
          if (OB_FAIL(decoder->get_ref_col_idx(ref_col_idx))) {
            LOG_WARN("get context param failed", KR(ret));
          }
        }

        if (OB_SUCC(ret) && ref_col_idx >= 0) {
          if (OB_FAIL(acquire(obj_meta, ref_col_idx, decoder))) {
            LOG_WARN("acquire decoder failed", KR(ret), K(obj_meta), K(ref_col_idx));
          } else {
            dest.ctx_->ref_decoder_ = decoder;
            dest.ctx_->ref_ctx_ = ctxs_[ref_col_idx];
            dest.ctx_->ref_ctx_->fill(obj_meta, block_header_, &col_header_[ref_col_idx], &decoder_allocator_);
          }
        }
      }
    }
  }
  return ret;
}

template <typename Allocator>
int ObMicroBlockDecoder::acquire(Allocator &allocator, const ObObjMeta &obj_meta,
    const ObMicroBlockHeaderV2 &header, const ObColumnHeader &col_header,
    const char *meta_data, const ObIColumnDecoder *&decoder)
{
  int ret = OB_SUCCESS;
  decoder = nullptr;
  if (OB_UNLIKELY(!obj_meta.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid obj_meta", KR(ret), K(obj_meta));
  } else {
    switch (col_header.type_)
    {
      case ObColumnHeader::RAW: {
        ObRawDecoder *d = nullptr;
        if (OB_FAIL(allocator.alloc(d))) {
          LOG_WARN("alloc failed", KR(ret));
        } else if (OB_FAIL(d->init(obj_meta, header, col_header, meta_data))) {
          LOG_WARN("init raw decoder failed", KR(ret));
        } else {
          decoder = d;
        }
        break;
      }
      case ObColumnHeader::DICT: {
        ObDictDecoder *d = nullptr;
        if (OB_FAIL(allocator.alloc(d))) {
          LOG_WARN("alloc failed", KR(ret));
        } else {
          if (OB_FAIL(d->init(obj_meta, header, col_header, meta_data))) {
            LOG_WARN("init dict decoder failed", KR(ret));
          } else {
            decoder = d;
          }
        }
        break;
      }
      case ObColumnHeader::INTEGER_BASE_DIFF: {
        ObIntegerBaseDiffDecoder *d = nullptr;
        if (OB_FAIL(allocator.alloc(d))) {
          LOG_WARN("alloc failed", KR(ret));
        } else {
          if (OB_FAIL(d->init(obj_meta, header, col_header, meta_data))) {
            LOG_WARN("init integer base diff decoder failed", KR(ret));
          } else {
            decoder = d;
          }
        }
        break;
      }
      case ObColumnHeader::STRING_DIFF: {
        ObStringDiffDecoder *d = nullptr;
        if (OB_FAIL(allocator.alloc(d))) {
          LOG_WARN("alloc failed", KR(ret));
        } else if (OB_FAIL(d->init(obj_meta, header, col_header, meta_data))) {
          LOG_WARN("init integer base diff decoder failed", KR(ret));
        } else {
          decoder = d;
        }
        break;
      }
      case ObColumnHeader::HEX_PACKING: {
        ObHexStringDecoder *d = nullptr;
        if (OB_FAIL(allocator.alloc(d))) {
          LOG_WARN("alloc failed", KR(ret));
        } else if (OB_FAIL(d->init(obj_meta, header, col_header, meta_data))) {
          LOG_WARN("init integer base diff decoder failed", KR(ret));
        } else {
          decoder = d;
        }
        break;
      }
      case ObColumnHeader::RLE: {
        ObRLEDecoder *d = nullptr;
        if (OB_FAIL(allocator.alloc(d))) {
          LOG_WARN("alloc failed", KR(ret));
        } else if (OB_FAIL(d->init(obj_meta, header, col_header, meta_data))) {
          LOG_WARN("init integer base diff decoder failed", KR(ret));
        } else {
          decoder = d;
        }
        break;
      }
      case ObColumnHeader::CONST: {
        ObConstDecoder *d = nullptr;
        if (OB_FAIL(allocator.alloc(d))) {
          LOG_WARN("alloc failed", KR(ret));
        } else if (OB_FAIL(d->init(obj_meta, header, col_header, meta_data))) {
          LOG_WARN("init integer base diff decoder failed", KR(ret));
        } else {
          decoder = d;
        }
        break;
      }
      case ObColumnHeader::STRING_PREFIX: {
        ObStringPrefixDecoder *d = nullptr;
        if (OB_FAIL(allocator.alloc(d))) {
          LOG_WARN("alloc failed", KR(ret));
        } else if (OB_FAIL(d->init(obj_meta, header, col_header, meta_data))) {
          LOG_WARN("init integer base diff decoder failed", KR(ret));
        } else {
          decoder = d;
        }
        break;
      }
      case ObColumnHeader::COLUMN_EQUAL: {
        ObColumnEqualDecoder *d = nullptr;
        if (OB_FAIL(allocator.alloc(d))) {
          LOG_WARN("alloc failed", KR(ret));
        } else if (OB_FAIL(d->init(obj_meta, header, col_header, meta_data))) {
          LOG_WARN("init integer base diff decoder failed", KR(ret));
        } else {
          decoder = d;
        }
        break;
      }
      case ObColumnHeader::COLUMN_SUBSTR: {
        ObInterColSubStrDecoder *d = nullptr;
        if (OB_FAIL(allocator.alloc(d))) {
          LOG_WARN("alloc failed", KR(ret));
        } else if (OB_FAIL(d->init(obj_meta, header, col_header, meta_data))) {
          LOG_WARN("init integer base diff decoder failed", KR(ret));
        } else {
          decoder = d;
        }
        break;
      }
      default:
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected encoding type", KR(ret), K(col_header.type_));
    }
  }
  if (OB_FAIL(ret) && nullptr != decoder) {
    allocator.free(const_cast<ObIColumnDecoder *>(decoder));
    decoder = nullptr;
  }
  return ret;
}

// called before inited
// performance critical, do not check parameters
int ObMicroBlockDecoder::acquire(const ObObjMeta &obj_meta, const int64_t store_idx,
    const ObIColumnDecoder *&decoder)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr != cached_decoder_header_ && store_idx < cached_decoder_header_->count_)) {
    decoder = &cached_decoder_header_->at(store_idx);
  } else {
    if (OB_FAIL(acquire(*allocator_, obj_meta, *block_header_, col_header_[store_idx], meta_data_, decoder))) {
      LOG_WARN("acquire decoder failed", KR(ret), K(store_idx), K(col_header_[store_idx]));
    } else {
      need_release_decoders_[need_release_decoder_cnt_++] = decoder;
    }
  }
  return ret;
}

void ObMicroBlockDecoder::free_decoders()
{
  for (int64_t i = 0; i < need_release_decoder_cnt_; ++i) {
    release(need_release_decoders_[i]);
  }
  need_release_decoder_cnt_ = 0;
}

void ObMicroBlockDecoder::release(const ObIColumnDecoder *decoder)
{
  if (nullptr != decoder && nullptr != allocator_) {
    allocator_->free(const_cast<ObIColumnDecoder *>(decoder));
  }
}

void ObMicroBlockDecoder::reset()
{
  free_decoders();
  allocator_ = nullptr;
  cast_allocator_.reuse();
  decoder_allocator_.reuse();
  column_map_ = nullptr;
  request_cnt_ = 0;
  store_cnt_ = 0;
  rowkey_cnt_ = 0;
  block_header_ = nullptr;
  col_header_ = nullptr;
  meta_data_ = nullptr;
  if (nullptr != ctx_array_) {
    ObTLDecoderCtxArray::free(ctx_array_);
    ctx_array_ = nullptr;
  }
  ctxs_ = nullptr;
  row_data_ = nullptr;
  cached_decoder_header_ = nullptr;
  need_cast_ = false;
  is_inited_ = false;
}

int ObMicroBlockDecoder::get_row(const int64_t idx, ObNewRow &row)
{
  int ret = OB_SUCCESS;
  cast_allocator_.reuse();
  decoder_allocator_.reuse();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(idx < 0 || idx > block_header_->row_count_ || !row.is_valid() || row.count_ < request_cnt_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(idx), K(block_header_->row_count_), K(row), K(request_cnt_));
  } else if (idx == block_header_->row_count_) {
    ret = OB_ITER_END;
  } else {
    int64_t row_len = 0;
    const char *row_data = nullptr;
    if (OB_FAIL(row_index_->get(idx, row_data, row_len))) {
      LOG_WARN("get row data failed", KR(ret), K(idx));
    } else {
      if (block_header_->store_row_header_) {
        const int row_header_size = ObRowHeaderV2::get_serialized_size();
        row_data += row_header_size;
        row_len -= row_header_size;
      }
      if (OB_FAIL(decode_cells(idx, row_len, row_data, 0, request_cnt_, row.cells_))) {
        LOG_WARN("decode cells failed", KR(ret), K(idx), K_(request_cnt));
      }
    }
  }
  return ret;
}

int ObMicroBlockDecoder::decode_cells(
    const uint64_t row_id,
    const int64_t row_len,
    const char *row_data,
    const int64_t col_begin,
    const int64_t col_end,
    common::ObObj *objs)
{
  int ret = OB_SUCCESS;
  ObBitStream bs(reinterpret_cast<unsigned char *>(const_cast<char *>(row_data)), row_len);
  for (int64_t i = col_begin; OB_SUCC(ret) && i < col_end; ++i) {
    if (OB_FAIL(decoders_[i].decode(objs[i], row_id, bs, row_data, row_len))) {
      LOG_WARN("decode cell failed", KR(ret), K(row_id), K(i), K(bs), KP(row_data), K(row_len));
    }
  }
  for (int64_t i = col_begin; need_cast_ && OB_SUCC(ret) && i < col_end; ++i) {
    // only init with column map need cast
    const ObIColumnIndexItem *column_index_item = column_map_->get_column_index(i);
    if (!column_index_item->get_is_column_type_matched() && column_index_item->get_store_index() >= 0) {
      // types don't match, need cast obj
      if (OB_FAIL(storage::cast_obj(column_index_item->get_request_column_type(), cast_allocator_, objs[i]))) {
        LOG_WARN("failed to cast obj", KR(ret), K(i), K(objs[i]), K(column_index_item->get_request_column_type()));
      }
    }
  }
  return ret;
}

} // table_load_backup
} // namespace observer
} // namespace oceanbase
