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

#include "ob_micro_block_cs_encoder.h"
#include "storage/blocksstable/index_block/ob_index_block_aggregator.h"

namespace oceanbase
{
using namespace storage;

namespace blocksstable
{
using namespace common;

int ObVecBatchInfo::init(const int32_t row_count,
                         const int32_t fixed_len,
                         const uint32_t *offsets,
                         sql::ObBitVector *nulls,
                         const uint32_t start_offset,
                         const bool is_integer)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(row_count <= 0 || (fixed_len < 0 &&  nullptr == offsets)
      || (fixed_len >= 0 && nullptr != offsets))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(row_count), K(fixed_len), KP(offsets));
  } else {
    row_count_ = row_count;
    fixed_len_ = fixed_len;
    offsets_ = offsets;
    nulls_ = nulls;
    start_offset_ = start_offset;
    is_integer_ = is_integer;
  }
  return ret;
}

template <typename T>
T *ObMicroBlockCSEncoder::alloc_encoder_()
{
  T *encoder = NULL;
  int ret = OB_SUCCESS;
  if (OB_FAIL(encoder_allocator_.alloc(encoder))) {
    LOG_WARN("alloc encoder failed", K(ret));
  }
  return encoder;
}

void ObMicroBlockCSEncoder::free_encoder_(ObIColumnCSEncoder *encoder)
{
  if (OB_NOT_NULL(encoder)) {
    encoder_allocator_.free(encoder);
  }
}

template <typename T>
int ObMicroBlockCSEncoder::alloc_and_init_encoder_(const int64_t column_index, ObIColumnCSEncoder *&encoder)
{
  int ret = OB_SUCCESS;
  encoder = NULL;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(column_index < 0 || column_index > ctx_.column_cnt_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(column_index));
  } else {
    T *e = alloc_encoder_<T>();
    if (OB_ISNULL(e)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc encoder failed", K(ret));
    } else {
      col_ctxs_.at(column_index).try_set_need_sort(
          e->get_type(), column_index, has_lob_out_row_, ctx_.major_working_cluster_version_);
      if (OB_FAIL(e->init(col_ctxs_.at(column_index), column_index, appended_row_count_))) {
        LOG_WARN("init column encoder failed", K(ret), K(column_index));
      }
      if (OB_FAIL(ret)) {
        free_encoder_(e);
        e = NULL;
      } else {
        encoder = e;
      }
    }
  }
  return ret;
}

ObMicroBlockCSEncoder::ObMicroBlockCSEncoder()
  : allocator_("CSEncAlloc", OB_MALLOC_MIDDLE_BLOCK_SIZE),
    ctx_(), row_buf_holder_(), data_buffer_(), all_string_data_buffer_(),
    all_col_datums_(OB_MALLOC_NORMAL_BLOCK_SIZE, ModulePageAllocator("CSBlkEnc", MTL_ID())),
    pivot_allocator_(lib::ObMemAttr(MTL_ID(), blocksstable::OB_ENCODING_LABEL_PIVOT), OB_MALLOC_MIDDLE_BLOCK_SIZE),
    datum_row_offset_arr_(OB_MALLOC_NORMAL_BLOCK_SIZE, ModulePageAllocator("CSBlkEnc", MTL_ID())),
    vec_batch_info_arrs_(OB_MALLOC_NORMAL_BLOCK_SIZE, ModulePageAllocator("CSBlkEnc", MTL_ID())),
    appended_batch_count_(0), appended_row_count_(0),
    estimate_size_(0), estimate_size_limit_(0), all_headers_size_(0),
    expand_pct_(DEFAULT_ESTIMATE_REAL_SIZE_PCT),
    encoders_(OB_MALLOC_NORMAL_BLOCK_SIZE, ModulePageAllocator("CSBlkEnc", MTL_ID())),
    stream_offsets_(OB_MALLOC_NORMAL_BLOCK_SIZE, ModulePageAllocator("CSBlkEnc", MTL_ID())),
    encoder_allocator_(cs_encoder_sizes, lib::ObMemAttr(MTL_ID(), "CSBlkEnc")),
    hashtables_(OB_MALLOC_NORMAL_BLOCK_SIZE, ModulePageAllocator("CSBlkEnc", MTL_ID())),
    hashtable_factory_(),
    col_ctxs_(OB_MALLOC_NORMAL_BLOCK_SIZE, ModulePageAllocator("CSBlkEnc", MTL_ID())),
    length_(0), is_inited_(false),
    is_all_column_force_raw_(false),
    encoder_freezed_(false),
    block_generated_(false),
    all_string_data_len_(0)
{
}

ObMicroBlockCSEncoder::~ObMicroBlockCSEncoder()
{
  reset();
}

int ObMicroBlockCSEncoder::init(const ObMicroBlockEncodingCtx &ctx)
{
  // can be init twice
  int ret = OB_SUCCESS;
  if (is_inited_) {
    reuse();
    is_inited_ = false;
  }

  if (OB_UNLIKELY(!ctx.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid encoder context", K(ret), K(ctx));
  } else if (OB_FAIL(encoders_.reserve(ctx.column_cnt_))) {
    LOG_WARN("reserve array failed", K(ret), "size", ctx.column_cnt_);
  } else if (OB_FAIL(encoder_allocator_.init())) {
    LOG_WARN("encoder_allocator init failed", K(ret));
  } else if (OB_FAIL(hashtables_.reserve(ctx.column_cnt_))) {
    LOG_WARN("reserve array failed", K(ret), "size", ctx.column_cnt_);
  } else if (OB_FAIL(col_ctxs_.reserve(ctx.column_cnt_))) {
    LOG_WARN("reserve array failed", K(ret), "size", ctx.column_cnt_);
  } else if (OB_FAIL(init_all_col_values_(ctx))) {
    LOG_WARN("init all_col_values failed", K(ret), K(ctx));
  } else if (OB_FAIL(checksum_helper_.init(ctx.col_descs_, true/*need_opt_row_checksum*/))) {
    LOG_WARN("fail to init checksum_helper", K(ret), K(ctx));
  } else if (FALSE_IT(ctx_ = ctx)) {
  } else if (OB_FAIL(ctx_.previous_cs_encoding_.init(ctx.column_cnt_))) {
    LOG_WARN("fail to init previous_cs_encoding_info", K(ret));
  } else {
    all_headers_size_ =
      ObMicroBlockHeader::get_serialize_size(ctx.column_cnt_, ctx.need_calc_column_chksum_) +
      sizeof(ObAllColumnHeader) + sizeof(ObCSColumnHeader) * ctx.column_cnt_;
    for (int64_t i = 0; i < ctx.column_cnt_; ++i) {
      if (!need_check_lob_ && ctx.col_descs_->at(i).col_type_.is_lob_storage()) {
        need_check_lob_ = true;
      }
    }
    encoder_freezed_ = false;
    is_inited_ = true;
  }
  return ret;
}

int ObMicroBlockCSEncoder::inner_init_()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObMicroBlockCSEncoder not inited ", K(ret));
  } else if (data_buffer_.length() > 0) {
    // has been inner_inited, do nothing
  } else {
    if (OB_UNLIKELY(!ctx_.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected invalid ctx", K(ret), K_(ctx));
    } else if (!data_buffer_.is_inited()) {
      if (OB_FAIL(data_buffer_.init(ObCSEncodingUtil::MAX_BLOCK_ENCODING_STORE_SIZE))) {
        LOG_WARN("fail to init data_buffer", K(ret));
      } else if (OB_FAIL(row_buf_holder_.init(ObCSEncodingUtil::DEFAULT_DATA_BUFFER_SIZE))) {
        LOG_WARN("fail to init row_buf_holder", K(ret));
      } else if (OB_FAIL(all_string_data_buffer_.init(ObCSEncodingUtil::DEFAULT_DATA_BUFFER_SIZE))) {
        LOG_WARN("fail to init all_string_data_buffer_", K(ret), K(all_string_data_buffer_));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(reserve_header_(ctx_))) {
      LOG_WARN("fail to reserve micro header", K(ret));
    } else {
      update_estimate_size_limit_(ctx_);
    }
  }
  return ret;
}

void ObMicroBlockCSEncoder::reset()
{
  ObIMicroBlockWriter::reset();
  is_inited_ = false;
  // ctx_
  data_buffer_.reset();
  allocator_.reset();
  FOREACH(cv, all_col_datums_)
  {
    ObColDatums *p = *cv;
    if (nullptr != p) {
      p->~ObColDatums();
      pivot_allocator_.free(p);
    }
  }
  all_col_datums_.reset();

  FOREACH(iter, vec_batch_info_arrs_)
  {
    ObVecBatchInfoArr *arr = *iter;
    if (nullptr != arr) {
      arr->~ObVecBatchInfoArr();
      pivot_allocator_.free(arr);
    }
  }
  vec_batch_info_arrs_.reset();
  appended_batch_count_ = 0;
  appended_row_count_ = 0;
  pivot_allocator_.reset();
  datum_row_offset_arr_.reset();
  estimate_size_ = 0;
  estimate_size_limit_ = 0;
  all_headers_size_ = 0;
  expand_pct_ = DEFAULT_ESTIMATE_REAL_SIZE_PCT;
  row_buf_holder_.reset();
  all_string_data_buffer_.reset();
  free_encoders_();
  encoders_.reset();
  stream_offsets_.reset();
  hashtables_.reset();
  col_ctxs_.reset();
  length_ = 0;
  is_all_column_force_raw_ = false;
  block_generated_ = false;
  all_string_data_len_ = 0;
  encoder_freezed_ = false;
}

void ObMicroBlockCSEncoder::reuse()
{
  ObIMicroBlockWriter::reuse();
  // is_inited_
  // ctx_
  data_buffer_.reuse();
  allocator_.reuse();
  FOREACH(c, all_col_datums_)
  {
    (*c)->reuse();
  }

  FOREACH(arr, vec_batch_info_arrs_)
  {
    (*arr)->reuse();
  }
  appended_batch_count_ = 0;
  appended_row_count_ = 0;
  // pivot_allocator_  pivot array memory is cached until encoder reset()
  row_buf_holder_.reuse();
  all_string_data_buffer_.reuse();
  datum_row_offset_arr_.reuse();
  estimate_size_ = 0;
  // estimate_size_limit_
  // all_headers_size_
  // expand_pct_
  free_encoders_();
  encoders_.reuse();
  stream_offsets_.reuse();
  hashtables_.reuse();
  col_ctxs_.reuse();
  length_ = 0;
  is_all_column_force_raw_ = false;
  block_generated_ = false;
  all_string_data_len_ = 0;
  encoder_freezed_ = false;
}

void ObMicroBlockCSEncoder::dump_diagnose_info()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!ctx_.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected encoding ctx", K_(ctx));
  } else {
    ObIMicroBlockWriter::dump_diagnose_info();
    ObDatumRow datum_row;
    ObStoreRow store_row;
    int64_t orig_checksum = 0;
    const int64_t column_cnt = ctx_.column_cnt_;
    ObArenaAllocator allocator("DumpMicroInfo");
    ObMicroBlockChecksumHelper tmp_checksum_helper;
    blocksstable::ObNewRowBuilder new_row_builder;
    if (OB_FAIL(tmp_checksum_helper.init(ctx_.col_descs_, true))) {
      LOG_WARN("Failed to init ObMicroBlockChecksumHelper", K(ret), KPC_(ctx_.col_descs));
    } else if (OB_FAIL(new_row_builder.init(*ctx_.col_descs_, allocator))) {
      LOG_WARN("Failed to init ObNewRowBuilder", K(ret), KPC_(ctx_.col_descs));
    } else if (OB_FAIL(datum_row.init(column_cnt))) {
      LOG_WARN("fail to init datum row", K_(ctx), K(column_cnt));
    } else {
      bool can_dump_buffered_row = false;
      if (0 == appended_batch_count_) {
        // non-batch interface
        if (OB_FAIL(set_datum_rows_ptr_())) {
          LOG_WARN("fail to set datum rows ptr", K(ret));
        } else {
          can_dump_buffered_row = true;
        }
      } else {
        if (!encoder_freezed_) {
          if (OB_FAIL(build_all_col_datums_())) {
            LOG_WARN("fail to build all column datums before dump diagnose info", K(ret));
          } else {
            can_dump_buffered_row = true;
          }
        } else {
          can_dump_buffered_row = true;
          for (int64_t col_idx = 0; col_idx < column_cnt && can_dump_buffered_row; ++col_idx) {
            if (all_col_datums_.at(col_idx)->count() != appended_row_count_) {
              can_dump_buffered_row = false;
              LOG_WARN("can not dump buffered rows since buffered datum is invalid", K(col_idx),
                  K(all_col_datums_.at(col_idx)->count()), K_(appended_row_count));
            }
          }
        }
      }

      for (int64_t row_id = 0; OB_SUCC(ret) && can_dump_buffered_row && row_id < appended_row_count_; ++row_id) {
        for (int64_t col_idx = 0; col_idx < column_cnt; ++col_idx) {
          datum_row.storage_datums_[col_idx].ptr_ = all_col_datums_.at(col_idx)->at(row_id).ptr_;
          datum_row.storage_datums_[col_idx].pack_ = all_col_datums_.at(col_idx)->at(row_id).pack_;
        }
        if (OB_TMP_FAIL(tmp_checksum_helper.cal_row_checksum(datum_row.storage_datums_, column_cnt))) {
          LOG_WARN("Failed to cal row checksum", K(ret), K(datum_row));
        }
        FLOG_WARN("error micro block row (original)", K(row_id), K(datum_row),
            "orig_checksum", tmp_checksum_helper.get_row_checksum());
        if (OB_FAIL(new_row_builder.build_store_row(datum_row, store_row))) {
          LOG_WARN("Failed to transfer datum row to store row", K(ret), K(datum_row));
        } else {
          FLOG_WARN("error micro block store_row (original)", K(row_id), K(store_row));
        }
      }
      encoder_freezed_ = true;
    }
  }
  // ignore ret
}

int ObMicroBlockCSEncoder::init_all_col_values_(const ObMicroBlockEncodingCtx &ctx)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(all_col_datums_.reserve(ctx.column_cnt_))) {
    LOG_WARN("reserve array failed", K(ret), "size", ctx.column_cnt_);
  }
  for (int64_t i = all_col_datums_.count(); i < ctx.column_cnt_ && OB_SUCC(ret); ++i) {
    ObColDatums *c = OB_NEWx(ObColDatums, &pivot_allocator_, pivot_allocator_);
    if (OB_ISNULL(c)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc memory failed", K(ret), K(ctx));
    } else if (OB_FAIL(all_col_datums_.push_back(c))) {
      LOG_WARN("push back column values failed", K(ret));
      if (nullptr != c) {
        c->~ObColDatums();
        pivot_allocator_.free(c);
      }
    }
  }
  return ret;
}

int ObMicroBlockCSEncoder::init_vec_batch_info_arrs_(const ObMicroBlockEncodingCtx &ctx)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(vec_batch_info_arrs_.reserve(ctx.column_cnt_))) {
    LOG_WARN("reserve array failed", K(ret), "size", ctx.column_cnt_);
  }
  for (int64_t i = vec_batch_info_arrs_.count(); i < ctx.column_cnt_ && OB_SUCC(ret); ++i) {
    ObVecBatchInfoArr *arr = OB_NEWx(ObVecBatchInfoArr, &pivot_allocator_, pivot_allocator_);
    if (OB_ISNULL(arr)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc memory failed", K(ret), K(ctx));
    } else if (OB_FAIL(vec_batch_info_arrs_.push_back(arr))) {
      LOG_WARN("push back vec_batch_info_arrs_ failed", K(ret));
    }
  }
  return ret;
}

void ObMicroBlockCSEncoder::print_micro_block_encoder_status_()
{
  FLOG_INFO("Build micro block failed, print encoder status: ",  K_(estimate_size),
    K_(estimate_size_limit), K_(all_headers_size), K_(expand_pct), K_(length));
  int64_t idx = 0;
  FOREACH(e, encoders_)
  {
    FLOG_INFO("Print column encoder: ", K(idx), KPC(*e));
    ++idx;
  }
}

void ObMicroBlockCSEncoder::update_estimate_size_limit_(const ObMicroBlockEncodingCtx &ctx)
{
  expand_pct_ = DEFAULT_ESTIMATE_REAL_SIZE_PCT;
  if (ctx.real_block_size_ > 0) {
    const int64_t prev_expand_pct = ctx.estimate_block_size_ * 100 / ctx.real_block_size_;
    expand_pct_ = prev_expand_pct != 0 ? prev_expand_pct : 1;
  }

  // We should make sure expand_pct_ never equal to 0
  if (OB_UNLIKELY(0 == expand_pct_)) {
    LOG_ERROR_RET(OB_ERR_UNEXPECTED, "expand_pct equal to zero", K_(expand_pct), K(ctx.estimate_block_size_),
      K(ctx.real_block_size_));
  }
  const int64_t data_size_limit = (2 * ctx.micro_block_size_ - all_headers_size_) > 0
    ? (2 * ctx.micro_block_size_ - all_headers_size_) : (2 * ctx.micro_block_size_);
  // reserve some space to prevent the encoded data from exceeding
  // the size of the macroblock in extreme scenarios
  const int64_t max_estimate_size_limit = ctx.macro_block_size_ > RESERVE_SIZE_FOR_ESTIMATE_LIMIT ?
      ctx.macro_block_size_ - RESERVE_SIZE_FOR_ESTIMATE_LIMIT : ctx.macro_block_size_;
  estimate_size_limit_ = std::min(data_size_limit * expand_pct_ / 100, max_estimate_size_limit);

  LOG_TRACE("estimate size expand percent", K(expand_pct_), K_(estimate_size_limit), K(ctx));
}

int ObMicroBlockCSEncoder::try_to_append_row_(const int64_t &store_size)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(store_size + all_headers_size_ + estimate_size_ > block_size_upper_bound_)) {
    ret = OB_BUF_NOT_ENOUGH;
  }
  return ret;
}

int ObMicroBlockCSEncoder::append_row(const ObDatumRow &row)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!row.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(row));
  } else if (OB_UNLIKELY(row.get_column_count() != ctx_.column_cnt_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("column count mismatch", K(ret), "ctx", ctx_, K(row));
  } else if (OB_UNLIKELY(encoder_freezed_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected encoder status", K(ret), K_(encoder_freezed), K_(appended_row_count), K_(appended_batch_count));
  } else if (OB_FAIL(inner_init_())) {
    LOG_WARN("failed to inner init", K(ret));
  } else if (0 == appended_row_count_) {
    if (OB_FAIL(init_column_ctxs_())) {
      LOG_WARN("fail to init_column_ctxs_", K(ret), K_(ctx));
    }
  }

  if (OB_SUCC(ret)) {
    int64_t store_size = 0;
    if (OB_UNLIKELY(appended_row_count_ >= ObCSEncodingUtil::MAX_MICRO_BLOCK_ROW_CNT
          || appended_row_count_ > ctx_.encoding_granularity_)) {
      ret = OB_BUF_NOT_ENOUGH;
      LOG_DEBUG("Try to encode more rows than maximum of row cnt in header, force to build a block",
          K_(appended_row_count), K(row), K(ctx_.encoding_granularity_));
    } else if (OB_FAIL(copy_and_append_row_(row, store_size))) {
      if (OB_UNLIKELY(OB_BUF_NOT_ENOUGH != ret)) {
        LOG_WARN("copy and append row failed", K(ret));
      }
    } else if (get_header(data_buffer_)->has_column_checksum_ && OB_FAIL(checksum_helper_.cal_column_checksum(
        row, get_header(data_buffer_)->column_checksums_))) {
      LOG_WARN("cal column checksum failed", K(ret), K(row));
    } else {
      if (need_cal_row_checksum()
          && OB_FAIL(checksum_helper_.cal_row_checksum(row.storage_datums_, row.get_column_count()))) {
        STORAGE_LOG(WARN, "fail to cal row chksum", K(ret), K(row), K_(checksum_helper));
      }
      cal_row_stat(row);
      estimate_size_ += store_size;
      LOG_DEBUG("cs encoder append row", K_(estimate_size), K(store_size), K_(appended_row_count));
    }
  }

  return ret;
}

int ObMicroBlockCSEncoder::append_batch(const ObBatchDatumRows &vec_batch,
                                        const int64_t start,
                                        const int64_t row_count)

{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(vec_batch.vectors_.count() != ctx_.column_cnt_ || row_count <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("column count mismatch", K(ret), K_(ctx), K(vec_batch), K(row_count));
  } else if (OB_UNLIKELY(encoder_freezed_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected encoder status", K(ret), K_(encoder_freezed), K_(appended_row_count), K_(appended_batch_count));
  } else if (OB_FAIL(inner_init_())) {
    LOG_WARN("failed to inner init", K(ret));
  } else if (0 == appended_batch_count_) {
    if (OB_FAIL(init_column_ctxs_())) {
      LOG_WARN("fail to init_column_ctxs_", K(ret), K_(ctx));
    } else if (vec_batch_info_arrs_.count() < ctx_.column_cnt_ &&
        OB_FAIL(init_vec_batch_info_arrs_(ctx_))) {
      LOG_WARN("init vec_batch_info_arrs_ failed", K(ret), K_(ctx));
    }
  }
  if (OB_SUCC(ret)) {
    int64_t store_size = 0;
    if (OB_UNLIKELY(appended_row_count_ >= ObCSEncodingUtil::MAX_MICRO_BLOCK_ROW_CNT
          || appended_row_count_ > ctx_.encoding_granularity_)) {
      ret = OB_BUF_NOT_ENOUGH;
      LOG_INFO("Try to encode more rows than maximum of row cnt in header, force to build a block",
          K_(appended_row_count), K_(appended_batch_count), K(ctx_.encoding_granularity_));
    } else if (OB_FAIL(copy_and_append_batch_(vec_batch, start, row_count, store_size))) {
      if (OB_UNLIKELY(OB_BUF_NOT_ENOUGH != ret)) {
        LOG_WARN("copy and append row failed", K(ret));
      } else if (0 == appended_batch_count_ && 1 == row_count) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("At least encode one row", K(ret), K(vec_batch), K(start), K(row_count));
      } else if (OB_UNLIKELY(0 == appended_batch_count_)) {
        reuse();
      }
    } else if (get_header(data_buffer_)->has_column_checksum_ && OB_FAIL(checksum_helper_.cal_column_checksum(
          vec_batch.vectors_, start, row_count, get_header(data_buffer_)->column_checksums_))) {
      LOG_WARN("cal column checksum failed", K(ret));
    } else {
      // The statistics here are only meaningful for the mini/minor sstable,
      // and cs-encoding is only used for major sstable, so there is no need to call this func.
      // cal_row_stat(row);
      estimate_size_ += store_size;
      LOG_DEBUG("cs encoder append batch", K_(estimate_size), K(store_size), K_(appended_row_count));
    }
  }

  return ret;
}

int ObMicroBlockCSEncoder::copy_and_append_batch_(const ObBatchDatumRows &vec_batch,
                                                  const int64_t start,
                                                  const int64_t row_count,
                                                  int64_t &store_size)
{
  int ret = OB_SUCCESS;
  // performance critical, do not double check parameters in private method
  const int64_t column_cnt = vec_batch.vectors_.count();
  const int64_t start_offset = length_;
  if (appended_row_count_ > 0 && estimate_size_ >= estimate_size_limit_) {
    ret = OB_BUF_NOT_ENOUGH;
  } else {
    bool is_finish = false;
    while(OB_SUCC(ret) && !is_finish) {
      store_size = 0;
      length_ = start_offset;
      is_finish = true;
      bool is_row_holder_not_enough = false;
      for (int64_t col_idx = 0; OB_SUCC(ret) && col_idx < column_cnt; ++col_idx) {
        if (OB_FAIL(copy_vector_(vec_batch.vectors_.at(col_idx),
                                 start,
                                 row_count,
                                 col_idx,
                                 store_size,
                                 is_row_holder_not_enough))) {
          if (OB_UNLIKELY(OB_BUF_NOT_ENOUGH != ret)) {
            LOG_WARN("fail to copy vector", K(ret), K(col_idx), K(store_size));
          } else {
            // failed to append batch due to buf not enough, but some vec batch may has been push back, need rollback
            int tmp_ret = OB_SUCCESS;
            if (OB_TMP_FAIL(remove_invalid_vec_batch_info_(col_idx))) {
              LOG_WARN("fail to remove_invalid_vec_batch_info_", K(ret), K(tmp_ret), K(column_cnt));
              ret = tmp_ret;
            }
          }
        } else if (is_row_holder_not_enough) {
          int64_t need_size = 0;
          if (OB_FAIL(calc_batch_data_size_(vec_batch.vectors_, start, row_count, need_size))) {
            LOG_WARN("fail to calc_vec_batch_data_size", K(ret), K(row_count));
          } else if (OB_UNLIKELY(need_size <= row_buf_holder_.size() - row_buf_holder_.length())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected row holder buf not enough", K(ret), K(need_size), K(row_buf_holder_));
          } else if (OB_FAIL(row_buf_holder_.ensure_space(need_size))) {
            LOG_WARN("failed to ensure space", K(ret), K(need_size), K(row_buf_holder_));
          } else if (OB_FAIL(remove_invalid_vec_batch_info_(col_idx))) {
            LOG_WARN("fail to remove_invalid_vec_batch_info_", K(ret), K(column_cnt));
          } else {
            is_finish = false;
            break;
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(row_buf_holder_.write_nop(length_ - start_offset))) {
        STORAGE_LOG(WARN, "fail to write nop", K(ret), K(length_), K(start_offset), K(row_buf_holder_));
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(try_to_append_row_(store_size))) {
    if (OB_UNLIKELY(OB_BUF_NOT_ENOUGH != ret)) {
      LOG_WARN("fail to try append row", K(ret));
    } else {
      // failed to try_to_append_row due to buf not enough, but this row has been push back, need rollback
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(remove_invalid_vec_batch_info_(column_cnt))) {
        LOG_WARN("fail to remove_invalid_vec_batch_info_", K(ret), K(tmp_ret), K(column_cnt));
        ret = tmp_ret;
      }
    }
  } else {
    appended_batch_count_++;
    appended_row_count_ += row_count;
  }
  return ret;
}

int ObMicroBlockCSEncoder::copy_vector_(const ObIVector *vector,
                                        const int64_t start,
                                        const int64_t row_count,
                                        const int64_t col_idx,
                                        int64_t &store_size,
                                        bool &is_row_holder_not_enough)
{
  int ret = OB_SUCCESS;
  const ObColDesc &col_desc = ctx_.col_descs_->at(col_idx);
  ObObjTypeStoreClass store_class = get_store_class_map()[col_desc.col_type_.get_type_class()];
  const bool is_int_sc = store_class == ObIntSC || store_class == ObUIntSC;
  is_row_holder_not_enough = false;
  int64_t extra_store_size_for_var_string = 0;
  const VectorFormat vec_format = vector->get_format();
  const int16_t precision = col_desc.col_type_.is_decimal_int() ?
      col_desc.col_type_.get_stored_precision() : PRECISION_UNKNOWN_YET;
  const VecValueTypeClass vec_tc = common::get_vec_value_tc(
      col_desc.col_type_.get_type(), col_desc.col_type_.get_scale(), precision);
  int64_t vec_data_size = 0;
  ObDataFormatType data_format_type = ObDataFormatType::MAX;

  if (OB_FAIL(calc_col_batch_data_size_(vector, start, row_count, col_idx, vec_data_size))) {
    LOG_WARN("fail to calc_col_batch_data_size_", K(ret), K(col_desc));
  } else if (is_int_sc) {
    store_size += sizeof(uint64_t) * row_count;
    if (VectorFormat::VEC_FIXED == vec_format) {
      if (ObCSEncodingUtil::is_int64_vec_value_tc(vec_tc)) {
        data_format_type = INT_64BIT_FIXED;
      } else {
        data_format_type = INT_NOT_64BIT_FIXED;
      }
    } else if (VectorFormat::VEC_CONTINUOUS == vec_format) {
      data_format_type = INT_CONTINUOUS;
    } else if (VectorFormat::VEC_DISCRETE == vec_format) {
      data_format_type = INT_DISCRETE;
    } else if (VectorFormat::VEC_UNIFORM == vec_format) {
      data_format_type = INT_UNIFORM;
    } else if (VectorFormat::VEC_UNIFORM_CONST == vec_format) {
      data_format_type = INT_UNIFORM_CONST;
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected vector format", K(ret), K(vec_format));
    }
  } else { // for string
    if (ObCSEncodingUtil::is_variable_len_store_class(store_class)) {
      extra_store_size_for_var_string = sizeof(uint64_t) * row_count; // for var length string offset array
    }
    store_size += vec_data_size + extra_store_size_for_var_string;

    if (VectorFormat::VEC_FIXED == vec_format) {
      data_format_type = STR_FIXED;
    } else if (VectorFormat::VEC_CONTINUOUS == vec_format) {
      data_format_type = STR_CONTINUOUS;
    } else if (VectorFormat::VEC_DISCRETE == vec_format) {
      data_format_type = STR_DISCRETE;
    } else if (VectorFormat::VEC_UNIFORM == vec_format) {
      data_format_type = STR_UNIFORM;
    } else if (VectorFormat::VEC_UNIFORM_CONST == vec_format) {
      data_format_type = STR_UNIFORM_CONST;
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected vector format", K(ret), K(vec_format));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (appended_batch_count_ > 0 && estimate_size_ + store_size >= estimate_size_limit_) {
    ret = OB_BUF_NOT_ENOUGH;
  } else if (row_count > 1 && estimate_size_ + store_size >= estimate_size_limit_) {
    ret = OB_BUF_NOT_ENOUGH;
  // appended_batch_count_ == 0 && row_count == 1 represent a large row, do not return OB_BUF_NOT_ENOUGH
  } else if (row_buf_holder_.size() < length_ + vec_data_size) {
    is_row_holder_not_enough = true;
  } else if (OB_FAIL(do_copy_vector_(vector,
                                     start,
                                     row_count,
                                     col_idx,
                                     vec_data_size,
                                     data_format_type,
                                     store_class == ObIntSC/*is_signed*/))) {
    LOG_WARN("fail to do copy vector", K(ret),
        K(data_format_type), K(col_desc), K(row_count), K(vec_data_size));
  }
  return ret;
}

int ObMicroBlockCSEncoder::do_copy_vector_(const ObIVector *vector,
                                           const int64_t start,
                                           const int64_t row_count,
                                           const int64_t col_idx,
                                           const int64_t vec_data_size,
                                           const ObDataFormatType type,
                                           const bool is_signed)
{

#define DEFINE_MASK_FOR_SIGNED_INT                 \
  uint64_t value = 0;                              \
  uint64_t mask = 0;                               \
  uint64_t reverse_mask = 0;                       \
  if (is_signed) {                                 \
    mask = INTEGER_MASK_TABLE[type_store_size];    \
    reverse_mask = ~mask;                          \
  }

#define COPY_INT_AND_COMPLETE_SIGNED_BIT(src, src_len)                \
  ENCODING_ADAPT_MEMCPY(&value, src, src_len)                         \
  if (is_signed) {                                                    \
    value = value & mask;                                             \
    if (0 != reverse_mask && (value & (reverse_mask >> 1))) {         \
      value |= reverse_mask;                                          \
    }                                                                 \
  }                                                                   \
  ENCODING_ADAPT_MEMCPY(buf + length_, &value, sizeof(uint64_t));

#define DEEP_COPY_NULLS_BITMAP(vector)                                                         \
  if (vector->has_null()) {                                                                    \
    if (OB_ISNULL(nulls_buf = allocator_.alloc(sql::ObBitVector::memory_size(row_count)))) {   \
      ret = OB_ALLOCATE_MEMORY_FAILED;                                                         \
      LOG_WARN("fail to allocate mem", K(ret), K(row_count));                                  \
    } else {                                                                                   \
      nulls = sql::to_bit_vector(nulls_buf);                                                   \
      const sql::ObBitVector *src_nulls = vector->get_nulls();                                 \
      if (start == 0) {                                                                        \
        nulls->deep_copy(*src_nulls, row_count);                                               \
      } else {                                                                                 \
        MEMSET(nulls_buf, 0, sql::ObBitVector::memory_size(row_count));                        \
        for (int64_t i = start, j = 0; j < row_count; i++, j++) {                              \
          if (src_nulls->at(i)) {                                                              \
            nulls->set(j);                                                                     \
          }                                                                                    \
        }                                                                                      \
      }                                                                                        \
    }                                                                                          \
  }

  int ret = OB_SUCCESS;
  ObVecBatchInfo batch_info;
  const ObColDesc &col_desc = ctx_.col_descs_->at(col_idx);
  char *buf = row_buf_holder_.data();
  const int64_t vec_start_offset = length_;
  uint32_t *offsets = nullptr;
  int32_t fixed_len = -1;
  sql::ObBitVector *nulls = nullptr;
  void *nulls_buf = nullptr;
  const int64_t type_store_size = get_type_size_map()[col_desc.col_type_.get_type()];
  bool is_integer =false;
  const int64_t end = start + row_count;

  switch (type) {
    case INT_64BIT_FIXED: {
      is_integer = true;
      const ObFixedLengthBase *fix_vec = static_cast<const ObFixedLengthBase *>(vector);
      fixed_len = fix_vec->get_length();
      OB_ASSERT(fixed_len == sizeof(uint64_t));
      const char *src = fix_vec->get_data() + start * fixed_len;
      MEMCPY(buf + length_, src, vec_data_size);
      length_ += fixed_len * row_count;
      DEEP_COPY_NULLS_BITMAP(fix_vec);
      break;
    }

    case INT_NOT_64BIT_FIXED: {
      is_integer = true;
      const ObFixedLengthBase *fix_vec = static_cast<const ObFixedLengthBase *>(vector);
      fixed_len = fix_vec->get_length();
      const char *src = fix_vec->get_data() + start * fixed_len;
      DEFINE_MASK_FOR_SIGNED_INT;
      for (int64_t i = 0; i < row_count; i++) {
        COPY_INT_AND_COMPLETE_SIGNED_BIT(src + i * fixed_len, fixed_len);
        length_ += sizeof(uint64_t);
      }
      DEEP_COPY_NULLS_BITMAP(fix_vec);
      break;
    }

    case INT_CONTINUOUS: {  // INT don't use continuous format?
      is_integer = true;
      fixed_len = 0;
      const ObContinuousFormat *conti_vec = static_cast<const ObContinuousFormat *>(vector);
      DEFINE_MASK_FOR_SIGNED_INT;
      for (int64_t i = start; i < end; i++) {
        const uint32_t len = conti_vec->get_length(i);
        if (len != 0) { // len == 0 if the element is null, no need copy
          fixed_len = len;
          COPY_INT_AND_COMPLETE_SIGNED_BIT(conti_vec->get_payload(i), len);
        }
        length_ += sizeof(uint64_t);
      }
      DEEP_COPY_NULLS_BITMAP(conti_vec);
      break;
    }

    case INT_DISCRETE: { // INT don't use discrete format?
      is_integer = true;
      fixed_len = 0;
      const ObDiscreteFormat *disc_vec = static_cast<const ObDiscreteFormat *>(vector);
      DEFINE_MASK_FOR_SIGNED_INT;
      for (int64_t i = start; i < end; i++) {
        if (!disc_vec->has_null() || !disc_vec->is_null(i)) { // not null
          const uint32_t len = disc_vec->get_length(i);
          fixed_len = len;
          COPY_INT_AND_COMPLETE_SIGNED_BIT(disc_vec->get_payload(i), len);
        }
        length_ += sizeof(uint64_t);
      }
      DEEP_COPY_NULLS_BITMAP(disc_vec);
      break;
    }

    case INT_UNIFORM: {
      is_integer = true;
      fixed_len = 0;
      const ObUniformFormat<false> *uni_vec = static_cast<const ObUniformFormat<false> *>(vector);
      DEFINE_MASK_FOR_SIGNED_INT;
      for (int64_t i = start, j = 0; OB_SUCC(ret) && i < end; i++, j++) {
        if (!uni_vec->is_null(i)) {
          const uint32_t len = uni_vec->get_length(i);
          fixed_len = len;
          COPY_INT_AND_COMPLETE_SIGNED_BIT(uni_vec->get_payload(i), len);
        } else {
          if (OB_ISNULL(nulls_buf)) {
            if (OB_ISNULL(nulls_buf = allocator_.alloc(sql::ObBitVector::memory_size(row_count)))) {
              ret = OB_ALLOCATE_MEMORY_FAILED;
              LOG_WARN("fail to allocate mem", K(ret), K(row_count));
            } else {
              MEMSET(nulls_buf, 0, sql::ObBitVector::memory_size(row_count));
              nulls = sql::to_bit_vector(nulls_buf);
            }
          }
          if (OB_SUCC(ret)) {
            nulls->set(j);
          }
        }
        length_ += sizeof(uint64_t);
      }
      break;
    }

    case INT_UNIFORM_CONST: {
      is_integer = true;
      fixed_len = 0;
      const ObUniformFormat<true> *uni_vec = static_cast<const ObUniformFormat<true> *>(vector);
      DEFINE_MASK_FOR_SIGNED_INT;
      if (!uni_vec->is_null(start)) {
        const uint32_t len = uni_vec->get_length(start);
        fixed_len = len;
        const char *payload = uni_vec->get_payload(start);
        for (int64_t i = start; i < end; i++) {
          COPY_INT_AND_COMPLETE_SIGNED_BIT(payload, len);
          length_ += sizeof(uint64_t);
        }
      } else { // const is null
        if (OB_ISNULL(nulls_buf = allocator_.alloc(sql::ObBitVector::memory_size(row_count)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to allocate mem", K(ret), K(row_count));
        } else {
          MEMSET(nulls_buf, 0xFF, sql::ObBitVector::memory_size(row_count));
          nulls = sql::to_bit_vector(nulls_buf);
          length_ += sizeof(uint64_t) * row_count;
        }
      }
      break;
    }

    case STR_FIXED:  {
      const ObFixedLengthBase *fix_vec = static_cast<const ObFixedLengthBase *>(vector);
      fixed_len = fix_vec->get_length();
      const char *src = fix_vec->get_data() + start * fixed_len;
      MEMCPY(buf + length_, src, vec_data_size);
      length_ += fixed_len * row_count;
      DEEP_COPY_NULLS_BITMAP(fix_vec);
      break;
    }

    case STR_CONTINUOUS: {
      const ObContinuousFormat *conti_vec = static_cast<const ObContinuousFormat *>(vector);
      if (OB_ISNULL(offsets = (uint32_t*)allocator_.alloc(sizeof(uint32_t) * (row_count + 1)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to allocate mem", K(ret), K(row_count));
      } else {
        MEMCPY(buf + length_, conti_vec->get_payload(start), vec_data_size);
        for (int64_t i = start, j = 0; i < end; i++, j++) {
          const uint32_t len = conti_vec->get_length(i);
          offsets[j] = length_;
          length_ += len;
        }
        offsets[row_count] = length_;
        DEEP_COPY_NULLS_BITMAP(conti_vec);
      }
      break;
    }

    case STR_DISCRETE: {
      const ObDiscreteFormat *disc_vec = static_cast<const ObDiscreteFormat *>(vector);
      if (OB_ISNULL(offsets = (uint32_t*)allocator_.alloc(sizeof(uint32_t) * (row_count + 1)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to allocate mem", K(ret), K(row_count));
      } else {
        for (int64_t i = start, j = 0; i < end; i++, j++) {
          if (!disc_vec->has_null() || !disc_vec->is_null(i)) { // not null
            const uint32_t len = disc_vec->get_length(i);
            MEMCPY(buf + length_, disc_vec->get_payload(i), len);
            offsets[j] = length_;
            length_ += len;
          } else {
            offsets[j] = length_;
          }
        }
        offsets[row_count] = length_;
        DEEP_COPY_NULLS_BITMAP(disc_vec);
      }
      break;
    }

    case STR_UNIFORM: {
      const ObUniformFormat<false> *uni_vec = static_cast<const ObUniformFormat<false> *>(vector);
      if (OB_ISNULL(offsets = (uint32_t*)allocator_.alloc(sizeof(uint32_t) * (row_count + 1)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to allocate mem", K(ret), K(row_count));
      } else {
        for (int64_t i = start, j = 0; OB_SUCC(ret) && i < end; i++, j++) {
          if (!uni_vec->is_null(i)) {
            const uint32_t len = uni_vec->get_length(i);
            MEMCPY(buf + length_, uni_vec->get_payload(i), len);
            offsets[j] = length_;
            length_ += len;
          } else {
            if (OB_ISNULL(nulls_buf)) {
              if (OB_ISNULL(nulls_buf = allocator_.alloc(sql::ObBitVector::memory_size(row_count)))) {
                ret = OB_ALLOCATE_MEMORY_FAILED;
                LOG_WARN("fail to allocate mem", K(ret), K(row_count));
              } else {
                MEMSET(nulls_buf, 0, sql::ObBitVector::memory_size(row_count));
                nulls = sql::to_bit_vector(nulls_buf);
              }
            }
            if (OB_SUCC(ret)) {
              nulls->set(j);
              offsets[j] = length_;
            }
          }
        }
        offsets[row_count] = length_;
      }
      break;
    }

    case STR_UNIFORM_CONST: {
      const ObUniformFormat<true> *uni_vec = static_cast<const ObUniformFormat<true> *>(vector);
      fixed_len = 0;
      if (!uni_vec->is_null(start)) {
        const uint32_t len = uni_vec->get_length(start);
        const char *payload = uni_vec->get_payload(start);
        fixed_len = len;
        for (int64_t i = start; i < end; i++) {
          MEMCPY(buf + length_, payload, len);
          length_ += len;
        }
      } else {  // const is null
        if (OB_ISNULL(nulls_buf = allocator_.alloc(sql::ObBitVector::memory_size(row_count)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to allocate mem", K(ret), K(row_count));
        } else {
          MEMSET(nulls_buf, 0xFF, sql::ObBitVector::memory_size(row_count));
          nulls = sql::to_bit_vector(nulls_buf);
        }
      }
      break;
    }

    default: {
      ret = OB_ERR_UNDEFINED;
      LOG_ERROR("unexpected data format type", K(type), K(col_desc), K(is_signed));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_UNLIKELY(length_ - vec_start_offset != vec_data_size)) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("vector data len not match", K(ret), K(length_), K(vec_start_offset), K(vec_data_size));
  } else if (OB_FAIL(batch_info.init(row_count,
                                     fixed_len,
                                     offsets,
                                     nulls,
                                     vec_start_offset,
                                     is_integer))) {
    LOG_ERROR("fail to init vector batch info", K(ret), K(row_count), K(fixed_len), KP(offsets),
        KP(nulls), K(vec_start_offset), K(is_integer));
  } else if (OB_FAIL(vec_batch_info_arrs_.at(col_idx)->push_back(batch_info))) {
    LOG_WARN("fail to push batch info", K(ret), K(batch_info));
  }
  return ret;
}

int ObMicroBlockCSEncoder::build_all_col_datums_()
{
  int ret = OB_SUCCESS;
  const char *data_buf = row_buf_holder_.data();

  for (int64_t col_idx = 0; OB_SUCC(ret) && col_idx < ctx_.column_cnt_; col_idx++) {
    const ObVecBatchInfoArr &batch_info_arr = *vec_batch_info_arrs_[col_idx];
    int64_t total_row_count = 0;

    for (int64_t batch_idx = 0; OB_SUCC(ret) && batch_idx < appended_batch_count_; batch_idx++) {
      const ObVecBatchInfo &batch_info = batch_info_arr.at(batch_idx);
      ObColDatums &col_datums = *all_col_datums_[col_idx];
      total_row_count += batch_info.row_count_;

      if (batch_info.offsets_ != nullptr) { // continuous format
        const uint32_t *offsets = batch_info.offsets_;
        OB_ASSERT(!batch_info.is_integer_);
        OB_ASSERT(offsets[0] ==  batch_info.start_offset_);
        for (int64_t row_idx = 0; OB_SUCC(ret) && row_idx < batch_info.row_count_; row_idx++) {
          ObDatum datum;
          if (batch_info.nulls_ == nullptr || !batch_info.nulls_->at(row_idx)) {
            datum.ptr_ = data_buf + offsets[row_idx];
            datum.len_ = offsets[row_idx + 1] - offsets[row_idx];
          } else {
            datum.set_null();
          }
          if (OB_FAIL(col_datums.push_back(datum))) {
            LOG_WARN("fail to push back datum", K(ret), K(datum), K(col_idx), K(batch_idx), K(row_idx));
          }
        }
      } else if (OB_UNLIKELY(batch_info.fixed_len_ < 0)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected fixed_len when offsets in nullptr", K(ret), K(batch_info));
      } else { // fixed format
        // integer is formatted to 8byte int row_buf_holder_
        const int32_t buf_step = batch_info.is_integer_ ? sizeof(uint64_t) : batch_info.fixed_len_;
        for (int64_t row_idx = 0; OB_SUCC(ret) && row_idx < batch_info.row_count_; row_idx++) {
          ObDatum datum;
          if (batch_info.nulls_ == nullptr || !batch_info.nulls_->at(row_idx)) {
            datum.ptr_ = data_buf + batch_info.start_offset_ + row_idx * buf_step;
            datum.len_ = batch_info.fixed_len_;
          } else {
            datum.set_null();
          }
          if (OB_FAIL(col_datums.push_back(datum))) {
            LOG_WARN("fail to push back datum", K(ret), K(datum), K(col_idx), K(batch_idx), K(row_idx));
          }
        }
      }
    } // end of for build one column

    if (OB_SUCC(ret) && total_row_count != appended_row_count_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("row_count mismatch", K(ret), K(total_row_count), K_(appended_row_count));
    }
  }
  return ret;
}

int ObMicroBlockCSEncoder::reserve_header_(const ObMicroBlockEncodingCtx &ctx)
{
  int ret = OB_SUCCESS;
  if (ctx.column_cnt_ < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("column_count was invalid", K(ret), K(ctx));
  } else {
    // reserve ObMicroBlockHeader
    int32_t header_size =
      ObMicroBlockHeader::get_serialize_size(ctx.column_cnt_, ctx.need_calc_column_chksum_);

    if (OB_FAIL(data_buffer_.write_nop(header_size, true))) {
      LOG_WARN("data buffer fail to advance headers size.", K(ret), K(header_size));
    } else {
      ObMicroBlockHeader *header = get_header(data_buffer_);
      header->magic_ = MICRO_BLOCK_HEADER_MAGIC;
      header->version_ = MICRO_BLOCK_HEADER_VERSION;
      header->header_size_ = header_size;
      header->row_store_type_ = ctx.row_store_type_;
      header->column_count_ = ctx.column_cnt_;
      header->rowkey_column_count_ = ctx.rowkey_column_cnt_;
      header->compressor_type_ = ctx.compressor_type_;
      header->has_column_checksum_ = ctx.need_calc_column_chksum_;
      if (header->has_column_checksum_) {
        header->column_checksums_ = reinterpret_cast<int64_t *>(
          data_buffer_.data() + ObMicroBlockHeader::COLUMN_CHECKSUM_PTR_OFFSET);
      } else {
        header->column_checksums_ = nullptr;
      }
    }
  }
  return ret;
}

int ObMicroBlockCSEncoder::store_columns_(int64_t &column_data_offset)
{
  int ret = OB_SUCCESS;
  int64_t need_store_size = 0;
  column_data_offset = data_buffer_.length();

  if (OB_UNLIKELY(column_data_offset != all_headers_size_)) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("column data offset must equal to all_headers_size_",
        K(ret), K(column_data_offset), K(all_headers_size_));
  } else if (OB_FAIL(all_string_data_buffer_.ensure_space(all_string_data_len_))) {
    LOG_WARN("fail to ensure space", K(ret), K(all_string_data_len_));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < encoders_.count(); ++i) {
    ObIColumnCSEncoder &encoder = *encoders_.at(i);
    if (OB_FAIL(encoder.get_maximal_encoding_store_size(need_store_size))) {
      LOG_WARN("fail to get_maximal_encoding_store_size", K(ret), K(i));
    } else if (OB_FAIL(data_buffer_.ensure_space(need_store_size))) {
      LOG_WARN("fail to ensure space", K(ret), K(need_store_size), K_(data_buffer));
    } else if (OB_FAIL(encoder.store_column(data_buffer_))) {
      LOG_WARN("fail to store column", K(ret), K(i));
    } else if (OB_FAIL(update_previous_info_after_encoding_(i, encoder))) {
      LOG_WARN("failt to update_previous_info_after_encoding", K(ret), K(i));
    } else if (OB_FAIL(encoder.get_stream_offsets(stream_offsets_))) {
      LOG_WARN("fail to get stream offsets", K(ret));
    }
  }
  return ret;
}

int ObMicroBlockCSEncoder::store_all_string_data_(uint32_t &data_size, bool &use_compress)
{
  int ret = OB_SUCCESS;
  use_compress = false;
  data_size = all_string_data_len_;
  common::ObCompressor *compressor = nullptr;
  if (all_string_data_len_ != all_string_data_buffer_.length()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("all_string_data_len_ mismatch", K(ret), K(all_string_data_len_), K(all_string_data_buffer_));
  } else if (0 == all_string_data_len_) {
    // do nothing
  } else {
    int64_t max_overflow_size = 0;
    if (OB_UNLIKELY(ctx_.compressor_type_ == INVALID_COMPRESSOR)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid compressor type", K(ret), K(ctx_));
    } else if (ctx_.compressor_type_ != NONE_COMPRESSOR) {
      use_compress = true;
      if (OB_FAIL(ObCompressorPool::get_instance().get_compressor(
          static_cast<ObCompressorType>(ctx_.compressor_type_), compressor))) {
        LOG_WARN("fail to get compressor", K(ret), K_(ctx));
      } else if (OB_FAIL(compressor->get_max_overflow_size(all_string_data_len_, max_overflow_size))) {
        LOG_WARN("fail to get_max_overflow_size", K(ret), K(all_string_data_len_), K_(ctx));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(data_buffer_.ensure_space(all_string_data_len_ + max_overflow_size))) {
      LOG_WARN("fail to ensure space", K(ret), K(all_string_data_len_), K(max_overflow_size));
    } else if (use_compress) {
      int64_t compressed_size = 0;
      int64_t remain_buf_len = data_buffer_.remain_buffer_size();
      if (OB_FAIL(compressor->compress(all_string_data_buffer_.data(),
          all_string_data_len_, data_buffer_.current(), remain_buf_len, compressed_size))) {
        LOG_WARN("fail to compress", K(ret), K(all_string_data_len_));
      } else if (compressed_size >= all_string_data_len_) {
        use_compress = false;
      } else if (OB_FAIL(data_buffer_.advance(compressed_size))) {
        LOG_WARN("fail to advance", K(compressed_size), K(ret));
      } else {
        data_size = compressed_size;
      }
    }

    if (OB_SUCC(ret)) {
      if (!use_compress) {
        MEMCPY(data_buffer_.current(), all_string_data_buffer_.data(), all_string_data_len_);
        if (OB_FAIL(data_buffer_.advance(all_string_data_len_))) {
          LOG_WARN("fail to advance", K(all_string_data_len_), K(ret));
        }
      }
    }
    LOG_DEBUG("store_all_string_data_", K(use_compress), K(all_string_data_len_), K(data_size), K(data_buffer_.length()));
  }

  return ret;
}

int ObMicroBlockCSEncoder::store_stream_offsets_(int64_t &stream_offsets_length)
{
  int ret = OB_SUCCESS;
  if (stream_offsets_.empty()) {
    stream_offsets_length = 0; // maybe dict encoding and all datum is null
  } else {
    int64_t need_store_size = 0;
    ObIntegerStreamEncoderCtx enc_ctx;
    uint32_t end_offset = stream_offsets_.at(stream_offsets_.count() - 1);
    if (OB_FAIL(enc_ctx.build_offset_array_stream_meta(
        end_offset, false/*force raw*/, ctx_.major_working_cluster_version_))) {
      LOG_WARN("fail to build_offset_array_stream_meta", K(ret));
    } else if(OB_FAIL(enc_ctx.build_stream_encoder_info(
                                     false/*has_null*/,
                                     true/*monotonic inc*/,
                                     &ctx_.cs_encoding_opt_,
                                     nullptr/*previous_encoding*/,
                                     -1/*stream_idx*/,
                                     ctx_.compressor_type_,
                                     &allocator_))) {
      LOG_WARN("fail to build_stream_encoder_info", K(ret));
    } else if (FALSE_IT(need_store_size = sizeof(ObIntegerStreamMeta) +
        common::ObCodec::get_moderate_encoding_size(
            enc_ctx.meta_.get_uint_width_size() * stream_offsets_.count()))) {
    } else if (OB_FAIL(data_buffer_.ensure_space(need_store_size))) {
      LOG_WARN("fail to ensure space", K(ret), K(need_store_size), K_(data_buffer));
    } else {
      int64_t pos_bak = data_buffer_.length();
      const uint32_t width_size = enc_ctx.meta_.get_uint_width_size();
      switch(width_size) {
      case 1 : {
        if (OB_FAIL(do_encode_stream_offsets_<uint8_t>(enc_ctx))) {
          LOG_WARN("fail to do_encode_offset_stream_", K(ret), K_(ctx));
        }
        break;
      }
      case 2 : {
        if (OB_FAIL(do_encode_stream_offsets_<uint16_t>(enc_ctx))) {
          LOG_WARN("fail to do_encode_offset_stream_", K(ret), K_(ctx));
        }
        break;
      }
      case 4 : {
        if (OB_FAIL(do_encode_stream_offsets_<uint32_t>(enc_ctx))) {
          LOG_WARN("fail to do_encode_offset_stream_", K(ret), K_(ctx));
        }
        break;
      }
      default:
        ret = OB_INVALID_ARGUMENT;
        STORAGE_LOG(WARN, "uint byte width size not invalid", K(ret), K(width_size));
        break;
      }
      if (OB_SUCC(ret)) {
        stream_offsets_length = data_buffer_.length() - pos_bak;
      }
    }
  }

  return ret;
}

int ObMicroBlockCSEncoder::prepare_for_build_block_()
{
  int ret = OB_SUCCESS;
  if (appended_batch_count_ == 0) { // use singel-row interface
    if (OB_FAIL(set_datum_rows_ptr_())) {
      LOG_WARN("fail to set datum rows ptr", K(ret));
    } else if (OB_FAIL(process_out_row_columns_())) {
      LOG_WARN("failed to process out row columns", K(ret));
    }
  } else { // use batch-row interface
    if (OB_FAIL(build_all_col_datums_())) {
      LOG_WARN("fail to build all col datums", K(ret));
    } else if (OB_FAIL(process_out_row_columns_())) {
      LOG_WARN("failed to process out row columns", K(ret));
    } else if (need_cal_row_checksum() &&
        OB_FAIL(checksum_helper_.cal_rows_checksum(all_col_datums_, appended_row_count_))) {
      LOG_WARN("fail to cal row chksum", K(ret), K_(appended_row_count), K_(checksum_helper));
    }
  }
  return ret;
}

int ObMicroBlockCSEncoder::build_block(char *&buf, int64_t &size)
{
  int ret = OB_SUCCESS;
  int32_t all_column_header_size = sizeof(ObAllColumnHeader);
  int32_t column_headers_size = sizeof(ObCSColumnHeader) * ctx_.column_cnt_;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(0 == appended_row_count_)) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("empty micro block", K(ret));
  } else if (OB_UNLIKELY(encoder_freezed_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected encoder status", K(ret), K_(encoder_freezed), K_(appended_row_count), K_(appended_batch_count));
  } else if (FALSE_IT(encoder_freezed_ = true)) {
  } else if (OB_FAIL(prepare_for_build_block_())) {
    LOG_WARN("fail to prepare_for_build_block", K(ret));
  } else if (OB_FAIL(encoder_detection_())) {
    LOG_WARN("detect column encoding failed", K(ret));
  } else if (OB_FAIL(data_buffer_.write_nop(all_column_header_size + column_headers_size))) {
    LOG_WARN("fail to ensure space", K(ret), K_(data_buffer), K(all_column_header_size), K(column_headers_size));
  } else {
    LOG_DEBUG("build micro block", K_(estimate_size), K_(all_headers_size),
        K(column_headers_size), K_(expand_pct), K_(appended_row_count), K(ctx_));

    int64_t column_data_offset = 0;
    int64_t stream_offsets_length = 0;
    bool is_all_string_compress = false;
    uint32_t all_string_data_size = 0;
    // <1> store all columns, include column meta and column data
    if (OB_FAIL(store_columns_(column_data_offset))) {
      LOG_WARN("fail to store columns", K(ret), K_(ctx), K_(appended_row_count), K(estimate_size_));
    // <2> store all string data
    } else if (OB_FAIL(store_all_string_data_(all_string_data_size, is_all_string_compress))) {
      LOG_WARN("fail to store_all_string_data_", K(ret));

    // <3> store stream offsets
    } else if (OB_FAIL(store_stream_offsets_(stream_offsets_length))) {
      LOG_WARN("fail to store stream offsets", K(ret));

    } else {
      ObMicroBlockHeader *header = get_header(data_buffer_);
      const int64_t header_size = header->header_size_;
      char *tmp_buf = data_buffer_.data() + header_size;
      ObAllColumnHeader *all_column_header = reinterpret_cast<ObAllColumnHeader*>(tmp_buf);
      ObCSColumnHeader *column_headers = reinterpret_cast<ObCSColumnHeader*>(tmp_buf + sizeof(ObAllColumnHeader));
      // <4> fill column headers
      for (int64_t i = 0; i < encoders_.count(); i++) {
        column_headers[i] = encoders_.at(i)->get_column_header();
      }
      // <5> fill all column header
      all_column_header->reuse();
      all_column_header->all_string_data_length_ = all_string_data_size;
      all_column_header->stream_offsets_length_ = stream_offsets_length;
      all_column_header->stream_count_ = stream_offsets_.count();
      if (is_all_string_compress) {
        all_column_header->set_is_all_string_compressed();
      }
      // <6> fill micro header
      header->row_count_ = appended_row_count_;
      header->has_string_out_row_ = has_string_out_row_;
      header->all_lob_in_row_ = !has_lob_out_row_;
      header->max_merged_trans_version_ = max_merged_trans_version_;

      // update encoding context
      ctx_.estimate_block_size_ += estimate_size_;
      ctx_.real_block_size_ += data_buffer_.length() - column_data_offset;
      ctx_.micro_block_cnt_++;

      buf = data_buffer_.data();
      size = data_buffer_.length();
      block_generated_ = true;

      LOG_DEBUG("finish build one micro block", KP(this), K(encoders_.count()),
          K_(appended_row_count), K(column_data_offset), K(size), K_(estimate_size), K_(estimate_size_limit),
          K_(expand_pct), K(ctx_.micro_block_cnt_), K_(block_size_upper_bound), K(stream_offsets_.count()),
          K(ctx_.compressor_type_), KP(buf), K(size), K_(all_string_data_len));
    }

    if (OB_FAIL(ret)) {
      // Print status of current encoders for debugging
      print_micro_block_encoder_status_();
    }
  }

  return ret;
}

int ObMicroBlockCSEncoder::init_column_ctxs_()
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    ObColumnCSEncodingCtx cc;
    cc.encoding_ctx_ = &ctx_;
    cc.allocator_ = &allocator_;
    cc.all_string_buf_writer_ = &all_string_data_buffer_;

    for (int64_t i = 0; OB_SUCC(ret) && i < ctx_.column_cnt_; ++i) {
      if (OB_FAIL(col_ctxs_.push_back(cc))) {
        LOG_WARN("failed to push back column ctx", K(ret));
      }
    }
  }

  return ret;
}

int ObMicroBlockCSEncoder::set_datum_rows_ptr_()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(appended_row_count_ != datum_row_offset_arr_.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("appended_row_count is unexpected", K(ret),
        K_(appended_row_count), K(datum_row_offset_arr_.count()));
  } else if (OB_UNLIKELY(row_buf_holder_.has_expand())) {
    char *data = row_buf_holder_.data();
    const int64_t column_cnt = ctx_.column_cnt_;
    for (int64_t row_id = 0; OB_SUCC(ret) && row_id < appended_row_count_; ++row_id) {
      const char *orig_row_start_ptr = all_col_datums_.at(0)->at(row_id).ptr_;
      char *curr_row_start_ptr = data + datum_row_offset_arr_.at(row_id);
      // ptr is invalid and need update
      if (orig_row_start_ptr != curr_row_start_ptr) {
        for (int64_t col_idx = 0; OB_SUCC(ret) && col_idx < column_cnt; ++col_idx) {
          ObDatum &datum = all_col_datums_.at(col_idx)->at(row_id);
          if (datum.ptr_ < orig_row_start_ptr) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected datum ptr", K(ret), K(datum), K(orig_row_start_ptr), K(row_id), K(col_idx));
          } else {
            datum.ptr_ = curr_row_start_ptr + (datum.ptr_ - orig_row_start_ptr);
          }
        }
      }
    }
  }

  return ret;
}

int ObMicroBlockCSEncoder::process_out_row_columns_()
{
  // make sure in&out row status of all values in a column are same
  int ret = OB_SUCCESS;
  if (!need_check_lob_) {
  } else if (OB_UNLIKELY(all_col_datums_.count() != col_ctxs_.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected column count not match", K(ret));
  } else {
    const int64_t col_count = all_col_datums_.count();
    for (int64_t col_idx = 0;
        !has_lob_out_row_ && OB_SUCC(ret) && col_idx < col_count; col_idx++) {
      if (!ctx_.col_descs_->at(col_idx).col_type_.is_lob_storage()) {
        // skip non-lob column
      } else {
        ObColDatums &col_datums = *all_col_datums_.at(col_idx);
        for (int64_t row_idx = 0;
            !has_lob_out_row_ && OB_SUCC(ret) && row_idx < appended_row_count_; ++row_idx) {
          ObDatum &datum = col_datums.at(row_idx);
          if (datum.is_nop() || datum.is_null()) {
          } else if (datum.len_ < sizeof(ObLobCommon)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("Unexpected lob datum len", K(ret), K(row_idx),
                K(ctx_.col_descs_->at(col_idx).col_type_), K(datum));
          } else {
            const ObLobCommon &lob_common = datum.get_lob_data();
            has_lob_out_row_ = !lob_common.in_row_;
            LOG_DEBUG("chaser debug lob out row", K(has_lob_out_row_), K(lob_common), K(datum));
          }
        }
      }
    }
  }
  // uncomment this after varchar overflow supported
  //} else if (need_check_string_out) {
  //  if (!has_string_out_row_ && row.storage_datums_[i].is_outrow()) {
  //    has_string_out_row_ = true;
  //   }
  //}
  return ret;
}

int ObMicroBlockCSEncoder::copy_and_append_row_(const ObDatumRow &src, int64_t &store_size)
{
  int ret = OB_SUCCESS;
  // performance critical, do not double check parameters in private method
  const int64_t column_cnt = src.get_column_count();
  const int64_t datum_row_offset = length_;
  ObDatum dst_datum;
  if (appended_row_count_ > 0 && estimate_size_ >= estimate_size_limit_) {
    ret = OB_BUF_NOT_ENOUGH;
  } else {
    bool is_finish = false;
    while(OB_SUCC(ret) && !is_finish) {
      store_size = 0;
      length_ = datum_row_offset;
      is_finish = true;
      bool is_row_holder_not_enough = false;
      for (int64_t col_idx = 0; OB_SUCC(ret) && col_idx < column_cnt; ++col_idx) {
        if (OB_FAIL(copy_cell_(ctx_.col_descs_->at(col_idx),
                               src.storage_datums_[col_idx],
                               dst_datum,
                               store_size,
                               is_row_holder_not_enough))) {
          if (OB_UNLIKELY(OB_BUF_NOT_ENOUGH != ret)) {
            LOG_WARN("fail to copy cell", K(ret), K(col_idx), K(src), K(store_size));
          } else {
            // failed to append row due to buf not enough, but some columns may has been push back, need rollback
            int tmp_ret = OB_SUCCESS;
            if (OB_TMP_FAIL(remove_invalid_datums_(col_idx))) {
              LOG_WARN("fail to remove_invalid_datums_", K(ret), K(tmp_ret), K(column_cnt));
              ret = tmp_ret;
            }
          }
        } else if (is_row_holder_not_enough) {
          const int64_t need_size = calc_datum_row_size_(src);
          if (OB_UNLIKELY(need_size <= row_buf_holder_.size() - row_buf_holder_.length())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected row holder buf not enough", K(ret), K(need_size), K(row_buf_holder_));
          } else if (OB_FAIL(row_buf_holder_.ensure_space(need_size))) {
            LOG_WARN("failed to ensure space", K(ret), K(need_size), K(row_buf_holder_));
          } else {
            int tmp_ret = OB_SUCCESS;
            if (OB_TMP_FAIL(remove_invalid_datums_(col_idx))) {
              LOG_WARN("fail to remove_invalid_datums_", K(ret), K(tmp_ret), K(column_cnt));
              ret = tmp_ret;
            }
            is_finish = false;
            break;
          }
        } else if (OB_FAIL(all_col_datums_.at(col_idx)->push_back(dst_datum))) {
          LOG_WARN("fail to push back dst datum", K(ret), K(col_idx), K(src), K(dst_datum));
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(row_buf_holder_.write_nop(length_ - datum_row_offset))) {
        STORAGE_LOG(WARN, "fail to write nop", K(ret), K(length_), K(datum_row_offset), K(row_buf_holder_));
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(try_to_append_row_(store_size))) {
    if (OB_UNLIKELY(OB_BUF_NOT_ENOUGH != ret)) {
      LOG_WARN("fail to try append row", K(ret));
    } else {
      // failed to try_to_append_row due to buf not enough, but this row has been push back, need rollback
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(remove_invalid_datums_(column_cnt))) {
        LOG_WARN("fail to remove_invalid_datums_", K(ret), K(tmp_ret), K(column_cnt));
        ret = tmp_ret;
      }
    }
  } else if (OB_FAIL(datum_row_offset_arr_.push_back(datum_row_offset))) {
    LOG_WARN("fail to push back datum_row_offset", K(ret), K(datum_row_offset));
  } else {
    appended_row_count_++;
  }

  return ret;
}

int ObMicroBlockCSEncoder::remove_invalid_datums_(const int32_t column_cnt)
{
  int ret = OB_SUCCESS;
  for (int64_t i  = 0; OB_SUCC(ret) && i < column_cnt; i++) {
    if (OB_FAIL(all_col_datums_.at(i)->resize(appended_row_count_))) {
      LOG_ERROR("fail to resize all_col_datums_", K(ret), K_(appended_row_count), K(i), K(column_cnt));
    }
  }
  return ret;
}

int ObMicroBlockCSEncoder::remove_invalid_vec_batch_info_(const int32_t column_cnt)
{
  int ret = OB_SUCCESS;
  for (int64_t i  = 0; OB_SUCC(ret) && i < column_cnt; i++) {
    if (OB_FAIL(vec_batch_info_arrs_.at(i)->resize(appended_batch_count_))) {
      LOG_ERROR("fail to resize vec_batch_info_arrs_", K(ret), K_(appended_batch_count), K(i), K(column_cnt));
    }
  }
  return ret;
}


int ObMicroBlockCSEncoder::copy_cell_(const ObColDesc &col_desc, const
    ObStorageDatum &src, ObDatum &dest, int64_t &store_size, bool &is_row_holder_not_enough)
{
  // For IntSC and UIntSC, normalize to 8 byte
  int ret = OB_SUCCESS;
  ObObjTypeStoreClass store_class = get_store_class_map()[col_desc.col_type_.get_type_class()];
  const bool is_int_sc = store_class == ObIntSC || store_class == ObUIntSC;
  int64_t datum_size = 0;
  is_row_holder_not_enough = false;
  dest.ptr_ = row_buf_holder_.data() + length_;
  dest.pack_ = src.pack_;
  int64_t extra_store_size_for_var_string = 0;
  if (src.is_null()) {
    dest.set_null();
    datum_size = sizeof(uint64_t);
  } else if (OB_UNLIKELY(src.is_ext())) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("unsupported store extend datum type", K(ret), K(src));
  } else if (is_int_sc) {
    datum_size = sizeof(uint64_t);
  } else if (ObCSEncodingUtil::is_variable_len_store_class(store_class)) {
    datum_size = dest.len_;
    extra_store_size_for_var_string = sizeof(uint64_t); // for var length string offset array
  } else { // decimal and fixed string
    datum_size = dest.len_;
  }

  if (OB_FAIL(ret)) {
  } else if (FALSE_IT(store_size += datum_size + extra_store_size_for_var_string)) {
  } else if (appended_row_count_ > 0 && estimate_size_ + store_size >= estimate_size_limit_) {
    ret = OB_BUF_NOT_ENOUGH;
  // appended_row_count_ == 0 represent a large row, do not return OB_BUF_NOT_ENOUGH
  } else if (row_buf_holder_.size() < length_ + datum_size) {
    is_row_holder_not_enough = true;
  } else {
    if (is_int_sc) {
      // In theory, the sql layer has carried out the complete sign bit operation,
      // that is the sign bit of negative number is replaced by 1 and the sign bit of
      // positive number is replaced by 0. However, the storage tier cannot guarantee this.
      // Therefore, the complement operation is carried out here, and the datum can directly
      // used as a 64-bit integer.
      uint64_t value = 0;
      MEMCPY(&value, src.ptr_, src.len_);
      if (store_class == ObIntSC) {
        const int64_t type_store_size = get_type_size_map()[col_desc.col_type_.get_type()];
        uint64_t mask = INTEGER_MASK_TABLE[type_store_size];
        uint64_t reverse_mask = ~mask;
        value = value & mask;
        if (0 != reverse_mask && (value & (reverse_mask >> 1))) {
          value |= reverse_mask;
        }
      }
      ENCODING_ADAPT_MEMCPY(const_cast<char *>(dest.ptr_), &value, datum_size);
    } else { // decimal and string
      MEMCPY(const_cast<char *>(dest.ptr_), src.ptr_, dest.len_);
    }
    length_ += datum_size;
  }
  return ret;
}

int64_t ObMicroBlockCSEncoder::calc_datum_row_size_(const ObDatumRow &src) const
{
  int64_t need_size = 0;
  for (int64_t col_idx = 0; col_idx < src.get_column_count(); ++col_idx) {
    const ObColDesc &col_desc = ctx_.col_descs_->at(col_idx);
    ObObjTypeStoreClass store_class = get_store_class_map()[col_desc.col_type_.get_type_class()];
    ObStorageDatum &datum = src.storage_datums_[col_idx];
    if (!datum.is_null()) {
      if (store_class == ObIntSC || store_class == ObUIntSC) {
        need_size += sizeof(uint64_t);
      } else { // decimal and string
        need_size += datum.len_;
      }
    } else {
      need_size += sizeof(uint64_t);
    }
  }
  return need_size;
}

int ObMicroBlockCSEncoder::calc_col_batch_data_size_(const ObIVector *vector,
                                                     const int64_t start,
                                                     const int64_t row_count,
                                                     const int64_t col_idx,
                                                     int64_t &size) const
{
  int ret = OB_SUCCESS;
  size = 0;
  const ObColDesc &col_desc = ctx_.col_descs_->at(col_idx);
  ObObjTypeStoreClass store_class = get_store_class_map()[col_desc.col_type_.get_type_class()];
  const VectorFormat vec_format = vector->get_format();
  const int64_t end = start + row_count;

  if (store_class == ObIntSC || store_class == ObUIntSC) {
    size += row_count * sizeof(uint64_t); // Whether it's null or not, occupy 8 bytes for one integer
  } else {
    switch (vec_format) {
      case VEC_FIXED : {
        const ObFixedLengthBase *fix_vec = static_cast<const ObFixedLengthBase *>(vector);
        size += row_count * fix_vec->get_length();
        break;
      }
      case VEC_CONTINUOUS: {
        const ObContinuousFormat *conti_vec = static_cast<const ObContinuousFormat *>(vector);
        const uint32_t *offsets = conti_vec->get_offsets();
        size += offsets[end] - offsets[start];
        break;
      }
      case VEC_DISCRETE: {
        const ObDiscreteFormat *disc_vec = static_cast<const ObDiscreteFormat *>(vector);
        for (int64_t i = start; i < end; i++) {
          if (!disc_vec->has_null() || !disc_vec->is_null(i)) {
            size += disc_vec->get_length(i);
          }
        }
        break;
      }
      case VEC_UNIFORM:{
        const ObUniformFormat<false> *uni_vec = static_cast<const ObUniformFormat<false> *>(vector);
        for (int64_t i = start; i < end; i++) {
          if (!uni_vec->is_null(i)) {
            size += uni_vec->get_length(i);
          }
        }
        break;
      }
      case VEC_UNIFORM_CONST:{
        const ObUniformFormat<true> *uni_vec = static_cast<const ObUniformFormat<true> *>(vector);
        if (!uni_vec->is_null(start)) {
          size += uni_vec->get_length(start) * row_count;
        }
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected vector format", K(ret), K(vec_format));
      }
    }
  }
  return ret;
}

int ObMicroBlockCSEncoder::calc_batch_data_size_(const ObIArray<ObIVector *> &vectors,
                                                 const int64_t start,
                                                 const int64_t row_count,
                                                 int64_t &size) const
{
  int ret = OB_SUCCESS;
  size = 0;
  int64_t col_data_size = 0;
  for (int64_t col_idx = 0; OB_SUCC(ret) && col_idx < vectors.count(); ++col_idx) {
    if (OB_FAIL(calc_col_batch_data_size_(vectors.at(col_idx), start, row_count, col_idx, col_data_size))) {
      LOG_WARN("fail to calc_col_batch_data_size_", K(ret), K(col_idx));
    } else {
      size += col_data_size;
    }
  }
  return ret;
}

int ObMicroBlockCSEncoder::prescan_(const int64_t column_index)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    const ObColDesc &col_desc = ctx_.col_descs_->at(column_index);
    const ObObjMeta column_type = col_desc.col_type_;
    const ObObjTypeStoreClass store_class = get_store_class_map()[ob_obj_type_class(column_type.get_type())];
    const int64_t precision_bytes = column_type.is_decimal_int() ?
        wide::ObDecimalIntConstValue::get_int_bytes_by_precision(column_type.get_stored_precision()) : -1;
    ObColumnCSEncodingCtx &col_ctx = col_ctxs_.at(column_index);
    col_ctx.col_datums_ = all_col_datums_.at(column_index);

    // build hashtable
    ObDictEncodingHashTable *ht = nullptr;
    ObDictEncodingHashTableBuilder *builder = nullptr;
    bool need_build_hash_table = true;
    ObPreviousColumnEncoding *previous_encoding = ctx_.previous_cs_encoding_.get_column_encoding(column_index);
    if (nullptr != ctx_.column_encodings_
        && ctx_.column_encodings_[column_index] >= 0
        && ctx_.column_encodings_[column_index] < ObCSColumnHeader::Type::MAX_TYPE) {
      // colum encoding is specified which is only for test, always build hash table in this case
      need_build_hash_table = true;
    } else if (previous_encoding->is_column_encoding_type_valid() && !previous_encoding->column_need_redetect_) {
      // previous column encoding type is used and is not dict encoding, so no need to build hash table
      if (ObCSColumnHeader::INTEGER == previous_encoding->identifier_.column_encoding_type_ ||
          ObCSColumnHeader::STRING == previous_encoding->identifier_.column_encoding_type_) {
        need_build_hash_table = false;
      }
    }

    if (need_build_hash_table) {
      // next power of 2
      uint64_t bucket_num = appended_row_count_ << 1;
      if (0 != (bucket_num & (bucket_num - 1))) {
        while (0 != (bucket_num & (bucket_num - 1))) {
          bucket_num = bucket_num & (bucket_num - 1);
        }
        bucket_num = bucket_num << 1;
      }
      const int64_t node_num = appended_row_count_;
      if (OB_UNLIKELY(node_num != col_ctx.col_datums_->count())) {
        ret = OB_INNER_STAT_ERROR;
        LOG_ERROR("row_count and col_datums_count is not requal",
            K(ret), K(node_num), KPC(col_ctx.col_datums_));
      } else if (OB_FAIL(hashtable_factory_.create(bucket_num, node_num, ht))) {
        LOG_WARN("create hashtable failed", K(ret), K(bucket_num), K(node_num));
      } else if (FALSE_IT(builder = static_cast<ObDictEncodingHashTableBuilder *>(ht))) {
      } else if (OB_FAIL(builder->build(*col_ctx.col_datums_, col_desc))) {
        LOG_WARN("build hash table failed", K(ret), K(column_index), K(column_type));
      }
    }

    if (OB_SUCC(ret)) {
      col_ctx.ht_ = ht;
      if (OB_FAIL(ObCSEncodingUtil::build_cs_column_encoding_ctx(store_class, precision_bytes, col_ctx))) {
        LOG_WARN("build_column_encoding_ctx failed", K(ret), KP(ht), K(store_class), K(precision_bytes));
      } else if (ht != nullptr && OB_FAIL(hashtables_.push_back(ht))) {
        LOG_WARN("failed to push back", K(ret));
      }
      LOG_DEBUG("hash table", K(column_index), KPC(ht), K(col_ctx));
    }

    if (OB_FAIL(ret) && ht != nullptr) {
      // avoid overwirte ret
      int temp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (temp_ret = hashtable_factory_.recycle(true, ht))) {
        LOG_WARN("recycle hashtable failed", K(temp_ret));
      }
    }
  }

  return ret;
}

int ObMicroBlockCSEncoder::encoder_detection_()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < ctx_.column_cnt_; ++i) {
      if (OB_FAIL(prescan_(i))) {
        LOG_WARN("failed to prescan", K(ret), K(i));
      }
    }
    uint32_t string_data_len = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < ctx_.column_cnt_; ++i) {
      if (OB_FAIL(fast_encoder_detect_(i))) {
        LOG_WARN("fast encoder detect failed", K(ret), K(i));
      } else if (encoders_.count() <= i && OB_FAIL(choose_encoder_(i))) {
        LOG_WARN("choose_encoder failed", K(ret), K(i));
      } else if (OB_FAIL(update_previous_info_after_choose_encoder_(i, *encoders_[i]))) {
        LOG_WARN("fail to update previous info after choose encoder", K(ret), K(i));
      } else if (OB_FAIL(encoders_[i]->get_string_data_len(string_data_len))) {
        LOG_WARN("fail to get string data len", K(ret), K(i));
      } else {
        all_string_data_len_ += string_data_len;
      }
    }
    if (OB_FAIL(ret)) {
      free_encoders_();
    }
  }

  return ret;
}

int ObMicroBlockCSEncoder::fast_encoder_detect_(const int64_t column_idx)
{
  int ret = OB_SUCCESS;
  ObIColumnCSEncoder *e = nullptr;
  const ObObjMeta column_type = ctx_.col_descs_->at(column_idx).col_type_;
  const ObObjTypeClass type_class = ob_obj_type_class(column_type.get_type());
  const ObObjTypeStoreClass store_class = get_store_class_map()[type_class];

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (nullptr != ctx_.column_encodings_
      && ctx_.column_encodings_[column_idx] >= 0
      && ctx_.column_encodings_[column_idx] < ObCSColumnHeader::Type::MAX_TYPE) {
    const ObCSColumnHeader::Type type =
        static_cast<ObCSColumnHeader::Type>(ctx_.column_encodings_[column_idx]);
    if (OB_FAIL(choose_specified_encoder_(column_idx, store_class, type, e))) {
      LOG_WARN("fail to choose_specified_encoder", K(column_idx), K(store_class), K(type), K(column_type));
    }
  } else if (is_all_column_force_raw_) {
    ObColumnCSEncodingCtx &col_ctx = col_ctxs_.at(column_idx);
    col_ctx.force_raw_encoding_ = true;
    if (is_integer_store_(store_class, col_ctx.is_wide_int_)) {
      if (OB_FAIL(alloc_and_init_encoder_<ObIntegerColumnEncoder>(column_idx, e))) {
        LOG_WARN("fail to alloc encoder", K(ret), K(column_idx), K(column_type), K(type_class));
      }
    } else if (is_string_store_(store_class, col_ctx.is_wide_int_)) {
      if (OB_FAIL(alloc_and_init_encoder_<ObStringColumnEncoder>(column_idx, e))) {
        LOG_WARN("fail to alloc encoder", K(ret), K(column_idx), K(column_type), K(type_class));
      }
    } else {
      ret = OB_INNER_STAT_ERROR;
      LOG_WARN("not supported store class", K(ret), K(store_class));
    }
  } else if (OB_FAIL(try_use_previous_encoder_(column_idx, store_class, e))) {
    LOG_WARN("fail to choose_specified_encoder", K(column_idx), K(store_class));
  }

  if (OB_SUCC(ret) && nullptr != e) {
    LOG_DEBUG("used encoder (fast)", K(column_idx), KPC(e));
    if (OB_FAIL(encoders_.push_back(e))) {
      LOG_WARN("push back encoder failed", K(ret));
      free_encoder_(e);
      e = nullptr;
    }
  }
  return ret;
}

int ObMicroBlockCSEncoder::choose_specified_encoder_(const int64_t column_idx,
                                                    const ObObjTypeStoreClass store_class,
                                                    const ObCSColumnHeader::Type type,
                                                    ObIColumnCSEncoder *&e)
{
  int ret = OB_SUCCESS;

  LOG_INFO("specified encoding type", K(column_idx), K(type), K(is_all_column_force_raw_));
  ObColumnCSEncodingCtx &col_ctx = col_ctxs_.at(column_idx);
  if (is_all_column_force_raw_) {
    col_ctx.force_raw_encoding_ = true;
  }
  if (is_integer_store_(store_class, col_ctx.is_wide_int_)) {
    if (ObCSColumnHeader::Type::INTEGER  == type) {
      if (OB_FAIL(alloc_and_init_encoder_<ObIntegerColumnEncoder>(column_idx, e))) {
        LOG_WARN("fail to alloc encoder", K(ret), K(column_idx), K(store_class));
      }
    } else if (ObCSColumnHeader::Type::INT_DICT == type) {
      if (OB_FAIL(alloc_and_init_encoder_<ObIntDictColumnEncoder>(column_idx, e))) {
        LOG_WARN("fail to alloc encoder", K(ret), K(column_idx), K(store_class));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("specified unexpected econding type", K(ret), K(column_idx), K(store_class), K(col_ctx));
    }
  } else if (is_string_store_(store_class, col_ctx.is_wide_int_)) {
    if (ObCSColumnHeader::Type::STRING == type) {
      if (OB_FAIL(alloc_and_init_encoder_<ObStringColumnEncoder>(column_idx, e))) {
        LOG_WARN("fail to alloc encoder", K(ret), K(column_idx), K(store_class));
      }
    } else if (ObCSColumnHeader::Type::STR_DICT == type) {
      if (OB_FAIL(alloc_and_init_encoder_<ObStrDictColumnEncoder>(column_idx, e))) {
        LOG_WARN("fail to alloc encoder", K(ret), K(column_idx), K(store_class));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("specified unexpected econding type", K(ret), K(type), K(store_class), K(col_ctx));
    }
  } else {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("not supported store class", K(ret), K(store_class));
  }

  return ret;
}

int ObMicroBlockCSEncoder::try_use_previous_encoder_(const int64_t column_idx,
                                                     const ObObjTypeStoreClass store_class,
                                                     ObIColumnCSEncoder *&e)
{
  int ret = OB_SUCCESS;
  ObPreviousColumnEncoding *previous_encoding = ctx_.previous_cs_encoding_.get_column_encoding(column_idx);

  if (previous_encoding->is_column_encoding_type_valid() && !previous_encoding->column_need_redetect_) {
    const ObCSColumnHeader::Type previous_type = previous_encoding->identifier_.column_encoding_type_;
    ObCSColumnHeader::Type curr_type = ObCSColumnHeader::Type::MAX_TYPE;
    ObColumnCSEncodingCtx &col_ctx = col_ctxs_.at(column_idx);

    if (is_integer_store_(store_class, col_ctx.is_wide_int_)) {
      if (ObCSColumnHeader::Type::INTEGER  == previous_type || ObCSColumnHeader::Type::INT_DICT == previous_type) {
        curr_type = previous_type;
      } else if (ObCSColumnHeader::Type::STRING == previous_type || ObCSColumnHeader::Type::STR_DICT == previous_type) {
        if (OB_UNLIKELY(store_class != ObDecimalIntSC || col_ctx.is_wide_int_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected store class", K(ret), K(store_class), K(col_ctx));
        } else if (ObCSColumnHeader::Type::STRING == previous_type) {
          curr_type = ObCSColumnHeader::Type::INTEGER;
        } else {
          curr_type = ObCSColumnHeader::Type::INT_DICT;
        }
      }
    } else if (is_string_store_(store_class, col_ctx.is_wide_int_)) {
      if (ObCSColumnHeader::Type::STRING == previous_type || ObCSColumnHeader::Type::STR_DICT == previous_type) {
        curr_type = previous_type;
      } else if (ObCSColumnHeader::Type::INTEGER  == previous_type || ObCSColumnHeader::Type::INT_DICT == previous_type) {
        if (OB_UNLIKELY(store_class != ObDecimalIntSC || !col_ctx.is_wide_int_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected store class", K(ret), K(store_class), K(col_ctx));
        } else if (ObCSColumnHeader::Type::INTEGER == previous_type) {
          curr_type = ObCSColumnHeader::Type::STRING;
        } else {
          curr_type = ObCSColumnHeader::Type::STR_DICT;
        }
      }
    } else {
      ret = OB_INNER_STAT_ERROR;
      LOG_WARN("not supported store class", K(ret), K(store_class));
    }

    if (OB_SUCC(ret)) {
      if (ObCSColumnHeader::Type::INTEGER == curr_type) {
        if (OB_FAIL(alloc_and_init_encoder_<ObIntegerColumnEncoder>(column_idx, e))) {
          LOG_WARN("fail to alloc encoder", K(ret), K(column_idx), K(store_class));
        }
      } else if (ObCSColumnHeader::Type::INT_DICT == curr_type) {
        if (OB_FAIL(alloc_and_init_encoder_<ObIntDictColumnEncoder>(column_idx, e))) {
          LOG_WARN("fail to alloc encoder", K(ret), K(column_idx), K(store_class));
        }
      } else if (ObCSColumnHeader::Type::STRING == curr_type) {
        if (OB_FAIL(alloc_and_init_encoder_<ObStringColumnEncoder>(column_idx, e))) {
          LOG_WARN("fail to alloc encoder", K(ret), K(column_idx), K(store_class));
        }
      } else if (ObCSColumnHeader::Type::STR_DICT == curr_type) {
        if (OB_FAIL(alloc_and_init_encoder_<ObStrDictColumnEncoder>(column_idx, e))) {
          LOG_WARN("fail to alloc encoder", K(ret), K(column_idx), K(store_class));
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected econding type", K(ret), K(curr_type), K(store_class), K(col_ctx));
      }
    }
  } else {
    // can't use previous column encoding type
  }
  return ret;
}

int ObMicroBlockCSEncoder::choose_encoder_(const int64_t column_idx)
{
  int ret = OB_SUCCESS;
  ObIColumnCSEncoder *e = nullptr;
  const ObObjMeta column_type = ctx_.col_descs_->at(column_idx).col_type_;
  const ObObjTypeStoreClass store_class =
    get_store_class_map()[ob_obj_type_class(column_type.get_type())];

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (is_integer_store_(store_class, col_ctxs_.at(column_idx).is_wide_int_)) {
    if (OB_FAIL(choose_encoder_for_integer_(column_idx, e))) {
      LOG_WARN("fail to choose encoder for integer", K(ret));
    }
  } else if (is_string_store_(store_class, col_ctxs_.at(column_idx).is_wide_int_)) {
    if (OB_FAIL(choose_encoder_for_string_(column_idx, e))) {
      LOG_WARN("fail to choose encoder for variable length type", K(ret));
    }
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not supported store class", K(ret), K(store_class));
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(encoders_.push_back(e))) {
      LOG_WARN("push back encoder failed");
    }
  }
  if (OB_FAIL(ret)) {
    if (nullptr != e) {
      free_encoder_(e);
      e = nullptr;
    }
  }

  return ret;
}

int ObMicroBlockCSEncoder::choose_encoder_for_integer_(
  const int64_t column_idx, ObIColumnCSEncoder *&e)
{
  int ret = OB_SUCCESS;
  ObIColumnCSEncoder *integer_encoder = nullptr;
  ObIColumnCSEncoder *dict_encoder = nullptr;
  if (OB_FAIL(alloc_and_init_encoder_<ObIntegerColumnEncoder>(column_idx, integer_encoder))) {
    LOG_WARN("fail to alloc encoder", K(ret), K(column_idx));
  } else if (OB_FAIL(alloc_and_init_encoder_<ObIntDictColumnEncoder>(column_idx, dict_encoder))) {
    LOG_WARN("fail to alloc encoder", K(ret), K(column_idx));
  } else {
    int64_t integer_estimate_size = integer_encoder->estimate_store_size();
    int64_t dict_estimate_size = dict_encoder->estimate_store_size();
    bool use_dict = false;
    const int64_t row_count =  dict_encoder->get_row_count();
    const int64_t distinct_cnt = static_cast<ObIntDictColumnEncoder*>(dict_encoder)->get_distinct_cnt();

    if (ctx_.major_working_cluster_version_ <= DATA_VERSION_4_3_5_0) {
      use_dict = dict_estimate_size < integer_estimate_size;
    } else {
      use_dict = (dict_estimate_size < integer_estimate_size * 70 / 100) ||
          (dict_estimate_size < integer_estimate_size && distinct_cnt < row_count * 50 / 100);
    }
    if (use_dict) {
      e = dict_encoder;
      free_encoder_(integer_encoder);
      integer_encoder = nullptr;
    } else {
      e = integer_encoder;
      free_encoder_(dict_encoder);
      dict_encoder = nullptr;
    }
    LOG_DEBUG("choose encoder for integer", K(ret), K(column_idx), K(integer_estimate_size),
        K(dict_estimate_size), K(row_count), K(distinct_cnt), KPC(integer_encoder), KPC(dict_encoder));
  }

  if (OB_FAIL(ret)) {
    if (nullptr != integer_encoder) {
      free_encoder_(integer_encoder);
      integer_encoder = nullptr;
    }
    if (nullptr != dict_encoder) {
      free_encoder_(dict_encoder);
      dict_encoder = nullptr;
    }
  }


  return ret;
}

int ObMicroBlockCSEncoder::choose_encoder_for_string_(
  const int64_t column_idx, ObIColumnCSEncoder *&e)
{
  int ret = OB_SUCCESS;
  ObIColumnCSEncoder *string_encoder = nullptr;
  ObIColumnCSEncoder *dict_encoder = nullptr;
  if (OB_FAIL(alloc_and_init_encoder_<ObStringColumnEncoder>(column_idx, string_encoder))) {
    LOG_WARN("fail to alloc encoder", K(ret), K(column_idx));
  } else if (OB_FAIL(alloc_and_init_encoder_<ObStrDictColumnEncoder>(column_idx, dict_encoder))) {
    LOG_WARN("fail to alloc encoder", K(ret), K(column_idx));
  } else {
    int64_t string_estimate_size = string_encoder->estimate_store_size();
    int64_t dict_estimate_size = dict_encoder->estimate_store_size();
    bool use_dict = false;
    const int64_t row_count = dict_encoder->get_row_count();
    const int64_t distinct_cnt = static_cast<ObStrDictColumnEncoder*>(dict_encoder)->get_distinct_cnt();

    if (ctx_.major_working_cluster_version_ <= DATA_VERSION_4_3_5_0) {
      use_dict = dict_estimate_size < string_estimate_size;
    } else {
      use_dict = (dict_estimate_size < string_estimate_size * 70 / 100) ||
          (dict_estimate_size < string_estimate_size && distinct_cnt < row_count * 50 / 100);
    }
    if (use_dict) {
      e = dict_encoder;
      free_encoder_(string_encoder);
      string_encoder = nullptr;
    } else {
      e = string_encoder;
      free_encoder_(dict_encoder);
      dict_encoder = nullptr;
    }
    LOG_DEBUG("choose encoder for var length type", K(ret), K(column_idx),
      K(string_estimate_size), K(dict_estimate_size), KPC(string_encoder), KPC(dict_encoder));
  }

  if (OB_FAIL(ret)) {
    if (nullptr != string_encoder) {
      free_encoder_(string_encoder);
      string_encoder = nullptr;
    }
    if (nullptr != dict_encoder) {
      free_encoder_(dict_encoder);
      dict_encoder = nullptr;
    }
  }

  return ret;
}

int ObMicroBlockCSEncoder::update_previous_info_after_choose_encoder_(const int32_t col_idx, ObIColumnCSEncoder &e)
{
  int ret = OB_SUCCESS;
  int32_t int_stream_count = 0;
  const ObIntegerStream::EncodingType *types = nullptr;
  ObColumnEncodingIdentifier identifier;
  if (OB_FAIL(e.get_identifier_and_stream_types(identifier, types))) {
    LOG_WARN("fail to get_identifier_and_stream_types", K(ret));
  } else if (OB_FAIL(ctx_.previous_cs_encoding_.update_column_detect_info(
      col_idx, identifier, ctx_.micro_block_cnt_, ctx_.major_working_cluster_version_))) {
    LOG_WARN("fail to check_and_set_identifier", K(ret), K(col_idx), K(identifier), K(ctx_.micro_block_cnt_));
  }
  return ret;
}

int ObMicroBlockCSEncoder::update_previous_info_after_encoding_(const int32_t col_idx, ObIColumnCSEncoder &e)
{
  int ret = OB_SUCCESS;
  const ObIntegerStream::EncodingType *stream_types = nullptr;
  ObColumnEncodingIdentifier identifier;
  if (OB_FAIL(e.get_identifier_and_stream_types(identifier, stream_types))) {
    LOG_WARN("fail to get_identifier_and_stream_types", K(ret), K(col_idx));
  } else if (OB_FAIL(ctx_.previous_cs_encoding_.update_stream_detect_info(
      col_idx, identifier, stream_types, ctx_.major_working_cluster_version_))) {
    LOG_WARN("fail to check_and_set_valid", K(ret), K(col_idx), K(identifier));
  }

  return ret;
}

void ObMicroBlockCSEncoder::free_encoders_()
{
  int ret = OB_SUCCESS;
  FOREACH(e, encoders_)
  {
    free_encoder_(*e);
  }
  FOREACH(ht, hashtables_)
  {
    // should continue even fail
    if (OB_FAIL(hashtable_factory_.recycle(true, *ht))) {
      LOG_WARN("recycle hashtable failed", K(ret));
    }
  }
  encoders_.reuse();
  hashtables_.reuse();
}

int ObMicroBlockCSEncoder::get_pre_agg_param(const int64_t col_idx, ObMicroDataPreAggParam &pre_agg_param) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!block_generated_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("need to build block before get pre-agg parameters", K(ret));
  } else if (OB_UNLIKELY(col_idx >= ctx_.column_cnt_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid column index", K(ret), K(col_idx), K_(ctx));
  } else {
    pre_agg_param.reset();
    const ObColumnCSEncodingCtx &ctx = col_ctxs_.at(col_idx);
    const ObObjTypeStoreClass sc = get_store_class_map()[ctx_.col_descs_->at(col_idx).col_type_.get_type_class()];
    pre_agg_param.col_datums_ = ctx.col_datums_;
    pre_agg_param.cs_encoding_ht_ = ctx.ht_;
    pre_agg_param.null_cnt_ = ctx.null_cnt_;
    pre_agg_param.is_cs_encoding_ = true;
    if (sc == ObIntSC || sc == ObUIntSC) {
      pre_agg_param.min_integer_ = ctx.integer_min_;
      pre_agg_param.max_integer_ = ctx.integer_max_;
      pre_agg_param.is_integer_aggregated_ = true;
    }
  }
  return ret;
}

}  // namespace blocksstable
}  // namespace oceanbase
