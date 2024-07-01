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

#include "ob_micro_block_encoder.h"
#include "lib/container/ob_array_iterator.h"
#include "ob_icolumn_encoder.h"
#include "ob_bit_stream.h"
#include "ob_integer_array.h"
#include "share/config/ob_server_config.h"
#include "share/ob_task_define.h"
#include "share/ob_force_print_log.h"
#include "ob_raw_encoder.h"
#include "ob_dict_encoder.h"
#include "ob_integer_base_diff_encoder.h"
#include "ob_string_diff_encoder.h"
#include "ob_hex_string_encoder.h"
#include "ob_rle_encoder.h"
#include "ob_const_encoder.h"
#include "ob_column_equal_encoder.h"
#include "ob_encoding_hash_util.h"
#include "ob_string_prefix_encoder.h"
#include "ob_inter_column_substring_encoder.h"

namespace oceanbase
{
using namespace storage;

namespace blocksstable
{
using namespace common;

template<typename T>
T *ObMicroBlockEncoder::alloc_encoder()
{
  T *encoder = NULL;
  int ret = OB_SUCCESS;
  if (OB_FAIL(encoder_allocator_.alloc(encoder))) {
    LOG_WARN("alloc encoder failed", K(ret));
  }
  return encoder;
}

void ObMicroBlockEncoder::free_encoder(ObIColumnEncoder *encoder)
{
  if (OB_NOT_NULL(encoder)) {
    encoder_allocator_.free(encoder);
  }
}

template <typename T>
int ObMicroBlockEncoder::try_encoder(ObIColumnEncoder *&encoder, const int64_t column_index)
{
  int ret = OB_SUCCESS;
  encoder = NULL;
  bool suitable = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(column_index < 0 || column_index > ctx_.column_cnt_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(column_index));
  } else if (ctx_.encoder_opt_.enable<T>()) {
    T *e = alloc_encoder<T>();
    if (NULL == e) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc encoder failed", K(ret));
    } else if (FALSE_IT(col_ctxs_.at(column_index).try_set_need_sort(e->get_type(), column_index))) {
    } else if (OB_FAIL(e->init(col_ctxs_.at(column_index), column_index, datum_rows_))) {
      LOG_WARN("init encoder failed", K(ret), K(column_index));
    } else if (OB_FAIL(e->traverse(suitable))) {
      LOG_WARN("traverse encoder failed", K(ret));
    }
    if (NULL != e && (OB_FAIL(ret) || !suitable)) {
      free_encoder(e);
      e = NULL;
    } else {
      encoder = e;
    }
  }
  return ret;
}

#define MICRO_BLOCK_PAGE_ALLOCATOR ModulePageAllocator(common::ObModIds::OB_ENCODER_ALLOCATOR, MTL_ID())
ObMicroBlockEncoder::ObMicroBlockEncoder() : ctx_(), header_(NULL),
    encoding_meta_allocator_(),
    data_buffer_(),
    datum_rows_(OB_MALLOC_NORMAL_BLOCK_SIZE, MICRO_BLOCK_PAGE_ALLOCATOR),
    all_col_datums_(OB_MALLOC_NORMAL_BLOCK_SIZE, MICRO_BLOCK_PAGE_ALLOCATOR),
    pivot_allocator_(lib::ObMemAttr(MTL_ID(), blocksstable::OB_ENCODING_LABEL_PIVOT), OB_MALLOC_MIDDLE_BLOCK_SIZE),
    estimate_size_(0), estimate_size_limit_(0),
    header_size_(0), expand_pct_(DEFAULT_ESTIMATE_REAL_SIZE_PCT),
    row_buf_holder_(),
    encoders_(OB_MALLOC_NORMAL_BLOCK_SIZE, MICRO_BLOCK_PAGE_ALLOCATOR),
    fix_data_encoders_(OB_MALLOC_NORMAL_BLOCK_SIZE, MICRO_BLOCK_PAGE_ALLOCATOR),
    var_data_encoders_(OB_MALLOC_NORMAL_BLOCK_SIZE, MICRO_BLOCK_PAGE_ALLOCATOR),
    encoder_allocator_(encoder_sizes, ObMemAttr(MTL_ID(), common::ObModIds::OB_ENCODER_ALLOCATOR)),
    row_indexs_(OB_MALLOC_NORMAL_BLOCK_SIZE, MICRO_BLOCK_PAGE_ALLOCATOR),
    hashtables_(OB_MALLOC_NORMAL_BLOCK_SIZE, MICRO_BLOCK_PAGE_ALLOCATOR),
    multi_prefix_trees_(OB_MALLOC_NORMAL_BLOCK_SIZE, MICRO_BLOCK_PAGE_ALLOCATOR),
    deep_copy_indexes_(OB_MALLOC_NORMAL_BLOCK_SIZE, MICRO_BLOCK_PAGE_ALLOCATOR),
    string_col_cnt_(0), estimate_base_store_size_(0),
    col_ctxs_(OB_MALLOC_NORMAL_BLOCK_SIZE, MICRO_BLOCK_PAGE_ALLOCATOR),
    length_(0),
    is_inited_(false)
{
  encoding_meta_allocator_.set_attr(ObMemAttr(MTL_ID(), "MicroBlkEncoder"));
  datum_rows_.set_attr(ObMemAttr(MTL_ID(), "MicroBlkEncoder"));
  all_col_datums_.set_attr(ObMemAttr(MTL_ID(), "MicroBlkEncoder"));
  encoders_.set_attr(ObMemAttr(MTL_ID(), "MicroBlkEncoder"));
  fix_data_encoders_.set_attr(ObMemAttr(MTL_ID(), "MicroBlkEncoder"));
  var_data_encoders_.set_attr(ObMemAttr(MTL_ID(), "MicroBlkEncoder"));
  row_indexs_.set_attr(ObMemAttr(MTL_ID(), "MicroBlkEncoder"));
  hashtables_.set_attr(ObMemAttr(MTL_ID(), "MicroBlkEncoder"));
  multi_prefix_trees_.set_attr(ObMemAttr(MTL_ID(), "MicroBlkEncoder"));
  deep_copy_indexes_.set_attr(ObMemAttr(MTL_ID(), "MicroBlkEncoder"));
  col_ctxs_.set_attr(ObMemAttr(MTL_ID(), "MicroBlkEncoder"));
}
#undef MICRO_BLOCK_PAGE_ALLOCATOR

ObMicroBlockEncoder::~ObMicroBlockEncoder()
{
  reset();
}

int ObMicroBlockEncoder::init(const ObMicroBlockEncodingCtx &ctx)
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
  } else if (OB_FAIL(multi_prefix_trees_.reserve(ctx.column_cnt_))) {
    LOG_WARN("reserve array failed", K(ret), "size", ctx.column_cnt_);
  } else if (OB_FAIL(col_ctxs_.reserve(ctx.column_cnt_))) {
    LOG_WARN("reserve array failed", K(ret), "size", ctx.column_cnt_);
  } else if (OB_FAIL(init_all_col_values(ctx))) {
    LOG_WARN("init all_col_values failed", K(ret), K(ctx));
  } else if (OB_FAIL(checksum_helper_.init(ctx.col_descs_, true/*need_opt_row_checksum*/))) {
    LOG_WARN("fail to init checksum_helper", K(ret), K(ctx));
  } else {
    // TODO bin.lb: shrink all_col_values_ size
    estimate_base_store_size_ = 0;
    for (int64_t i = 0; i < ctx.column_cnt_; ++i) {
      estimate_base_store_size_
          += get_estimate_base_store_size_map()[ctx.col_descs_->at(i).col_type_.get_type()];
      if (!need_check_lob_ && ctx.col_descs_->at(i).col_type_.is_lob_storage()) {
        need_check_lob_ = true;
      }
    }
    ctx_ = ctx;
    is_inited_ = true;
  }
  return ret;
}

int ObMicroBlockEncoder::inner_init()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("fail to reserve enough space for micro block buffer", K(ret));
  } else if (data_buffer_.length() > 0) {
    // has been inner_inited, do nothing
  } else {
    if (OB_UNLIKELY(!ctx_.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected invalid ctx", K(ret), K_(ctx));
    } else if (!data_buffer_.is_inited()) {
      if (OB_FAIL(data_buffer_.init(DEFAULT_DATA_BUFFER_SIZE))) {
        STORAGE_LOG(WARN, "fail to init data buffer", K(ret));
      } else if (OB_FAIL(row_buf_holder_.init(2 * DEFAULT_DATA_BUFFER_SIZE))) {
        STORAGE_LOG(WARN, "fail to init row_buf_holder", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(reserve_header(ctx_))) {
      LOG_WARN("fail to reserve micro header", K(ret));
    } else {
      update_estimate_size_limit(ctx_);
    }
  }
  return ret;
}

void ObMicroBlockEncoder::reset()
{
  ObIMicroBlockWriter::reset();
  is_inited_ = false;
  //ctx_
  encoding_meta_allocator_.reset();
  data_buffer_.reset();
  datum_rows_.reset();
  FOREACH(cv, all_col_datums_) {
    ObColDatums *p = *cv;
    if (nullptr != p) {
      p->~ObColDatums();
      pivot_allocator_.free(p);
    }
  }
  all_col_datums_.reset();
  pivot_allocator_.reset();
  estimate_size_ = 0;
  estimate_size_limit_ = 0;
  header_size_ = 0;
  expand_pct_ = DEFAULT_ESTIMATE_REAL_SIZE_PCT;
  estimate_base_store_size_ = 0;
  row_buf_holder_.reset();
  fix_data_encoders_.reset();
  var_data_encoders_.reset();
  free_encoders();
  encoders_.reset();
  hashtables_.reset();
  multi_prefix_trees_.reset();
  row_indexs_.reset();
  deep_copy_indexes_.reset();
  col_ctxs_.reset();
  string_col_cnt_ = 0;
  length_ = 0;
}

void ObMicroBlockEncoder::reuse()
{
  ObIMicroBlockWriter::reuse();
  // is_inited_
  // ctx_
  encoding_meta_allocator_.reuse();
  data_buffer_.reuse();
  datum_rows_.reuse();
  FOREACH(c, all_col_datums_) {
    (*c)->reuse();
  }
  // pivot_allocator_  pivot array memory is cached until encoder reset()
  row_buf_holder_.reuse();
  estimate_size_ = 0;
  // estimate_size_limit_
  // header_size_
  // expand_pct_
  // estimate_base_store_size_ should only recalculate with initialization
  fix_data_encoders_.reuse();
  var_data_encoders_.reuse();
  free_encoders();
  encoders_.reuse();
  hashtables_.reuse();
  multi_prefix_trees_.reuse();
  row_indexs_.reuse();
  col_ctxs_.reuse();
  string_col_cnt_ = 0;
  length_ = 0;
}

void ObMicroBlockEncoder::dump_diagnose_info() const
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
    print_micro_block_encoder_status();
    ObDatumRow datum_row;
    ObStoreRow store_row;
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
      for (int64_t it = 0; OB_SUCC(ret) && it < datum_rows_.count(); ++it) {
        for (int64_t col_idx = 0; col_idx < column_cnt; ++col_idx) {
          datum_row.storage_datums_[col_idx].ptr_ = datum_rows_.at(it).datums_[col_idx].ptr_;
          datum_row.storage_datums_[col_idx].pack_ = datum_rows_.at(it).datums_[col_idx].pack_;
        }
        if (OB_TMP_FAIL(tmp_checksum_helper.cal_row_checksum(datum_row.storage_datums_, column_cnt))) {
          LOG_WARN("Failed to cal row checksum", K(ret), K(datum_row));
        }
        FLOG_WARN("error micro block row (original)", K(it), K(datum_row),
            "orig_checksum", tmp_checksum_helper.get_row_checksum());
        if (OB_FAIL(new_row_builder.build_store_row(datum_row, store_row))) {
          LOG_WARN("Failed to transfer datum row to store row", K(ret), K(datum_row));
        } else {
          FLOG_WARN("error micro block store_row (original)", K(it), K(store_row));
        }
      }
    }
  }
  // ignore ret
}
int ObMicroBlockEncoder::init_all_col_values(const ObMicroBlockEncodingCtx &ctx)
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

void ObMicroBlockEncoder::print_micro_block_encoder_status() const
{
  FLOG_INFO("Build micro block failed, print encoder status: ", K_(ctx),
    K_(estimate_size), K_(estimate_size_limit), K_(header_size),
      K_(expand_pct), K_(string_col_cnt), K_(estimate_base_store_size), K_(length));
  int64_t idx = 0;
  FOREACH(e, encoders_) {
    FLOG_INFO("Print column encoder: ", K(idx), KPC(*e));
    ++idx;
  }
}

void ObMicroBlockEncoder::update_estimate_size_limit(const ObMicroBlockEncodingCtx &ctx)
{
  expand_pct_ = DEFAULT_ESTIMATE_REAL_SIZE_PCT;
  if (ctx.real_block_size_ > 0) {
    const int64_t prev_expand_pct = ctx.estimate_block_size_ * 100 / ctx.real_block_size_;
    expand_pct_ = prev_expand_pct != 0 ? prev_expand_pct : 1;
  }

  // We should make sure expand_pct_ never equal to 0
  if (OB_UNLIKELY(0 == expand_pct_)) {
    LOG_ERROR_RET(OB_ERR_UNEXPECTED, "expand_pct equal to zero",
        K_(expand_pct), K(ctx.estimate_block_size_), K(ctx.real_block_size_));
  }

  header_size_ =
      ObMicroBlockHeader::get_serialize_size(ctx.column_cnt_, ctx.need_calc_column_chksum_)
      + (sizeof(ObColumnHeader)) * ctx.column_cnt_;
  int64_t data_size_limit = (2 * ctx.micro_block_size_ - header_size_) > 0
                                  ? (2 * ctx.micro_block_size_ - header_size_)
                                  : (2 * ctx.micro_block_size_);
  if(ctx.major_working_cluster_version_ >= DATA_VERSION_4_1_0_0 ) {
    data_size_limit = MAX(data_size_limit, DEFAULT_MICRO_MAX_SIZE);
  }
  estimate_size_limit_ = std::min(data_size_limit * expand_pct_ / 100, ctx.macro_block_size_);
  LOG_TRACE("estimate size expand percent", K(expand_pct_), K_(estimate_size_limit), K(ctx));
}

int ObMicroBlockEncoder::try_to_append_row(const int64_t &store_size)
{
  int ret = OB_SUCCESS;
  // header_size_ = micro_header_size + column_header_size
  if (OB_UNLIKELY(store_size + estimate_size_ + header_size_ > block_size_upper_bound_)) {
    ret = OB_BUF_NOT_ENOUGH;
  }
  return ret;
}

int ObMicroBlockEncoder::append_row(const ObDatumRow &row)
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
  } else if (OB_FAIL(inner_init())) {
    LOG_WARN("failed to inner init", K(ret));
  } else {
    if (datum_rows_.empty()) {
      if (OB_FAIL(init_column_ctxs())) {
        LOG_WARN("build deep copy indexes failed", K(ret), K_(ctx));
      }
    }
  }

  if (OB_SUCC(ret)) {
    int64_t store_size = 0;
    ObConstDatumRow datum_row;
    ObMicroBlockHeader *header = get_header(data_buffer_);
    if (OB_UNLIKELY(datum_rows_.count() >= MAX_MICRO_BLOCK_ROW_CNT)) {
      ret = OB_BUF_NOT_ENOUGH;
      LOG_INFO("Try to encode more rows than maximum of row cnt in header, force to build a block",
          K(datum_rows_.count()), K(row));
    } else if (OB_FAIL(process_out_row_columns(row))) {
      LOG_WARN("failed to process out row columns", K(ret));
    } else if (OB_FAIL(copy_and_append_row(row, store_size))) {
      if (OB_UNLIKELY(OB_BUF_NOT_ENOUGH != ret)) {
        LOG_WARN("copy and append row failed", K(ret));
      }
    } else if (header->has_column_checksum_ && OB_FAIL(checksum_helper_.cal_column_checksum(
        row, header->column_checksums_))) {
      LOG_WARN("cal column checksum failed", K(ret), K(row));
    } else {
      if (need_cal_row_checksum()
          && OB_FAIL(checksum_helper_.cal_row_checksum(row.storage_datums_, row.get_column_count()))) {
        STORAGE_LOG(WARN, "fail to cal row chksum", K(ret), K(row), K_(checksum_helper));
      }
      cal_row_stat(row);
      estimate_size_ += store_size;
    }
  }
  return ret;
}

int ObMicroBlockEncoder::pivot()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(datum_rows_.empty())) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("empty micro block", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < ctx_.column_cnt_; ++i) {
      ObColDatums &c = *all_col_datums_.at(i);
      if (OB_FAIL(c.resize(datum_rows_.count()))) {
        LOG_WARN("resize column values failed", K(ret), K(ret), "count", datum_rows_.count());
      } else {
        int64_t pos = 0;
        for (; pos + 8 <= datum_rows_.count(); pos += 8) {
          c.at(pos + 0) = datum_rows_.at(pos + 0).get_datum(i);
          c.at(pos + 1) = datum_rows_.at(pos + 1).get_datum(i);
          c.at(pos + 2) = datum_rows_.at(pos + 2).get_datum(i);
          c.at(pos + 3) = datum_rows_.at(pos + 3).get_datum(i);
          c.at(pos + 4) = datum_rows_.at(pos + 4).get_datum(i);
          c.at(pos + 5) = datum_rows_.at(pos + 5).get_datum(i);
          c.at(pos + 6) = datum_rows_.at(pos + 6).get_datum(i);
          c.at(pos + 7) = datum_rows_.at(pos + 7).get_datum(i);
        }
        for (; pos < datum_rows_.count(); ++pos) {
          c.at(pos) = datum_rows_.at(pos).get_datum(i);
        }
      }
    }
  }
  return ret;
}

int ObMicroBlockEncoder::reserve_header(const ObMicroBlockEncodingCtx &ctx)
{
  int ret = OB_SUCCESS;
  if (ctx.column_cnt_ < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("column_count was invalid", K(ret), K(ctx));
  } else {
    int32_t header_size = ObMicroBlockHeader::get_serialize_size(
        ctx.column_cnt_, ctx.need_calc_column_chksum_);
    if (OB_FAIL(data_buffer_.write_nop(header_size, true))) {
      LOG_WARN("data buffer fail to advance header size.", K(ret), K(header_size));
    } else {
      ObMicroBlockHeader *header = get_header(data_buffer_);
      header->magic_ = MICRO_BLOCK_HEADER_MAGIC;
      header->version_ = MICRO_BLOCK_HEADER_VERSION;
      header->header_size_ = header_size;
      header->row_store_type_ = ctx.row_store_type_;
      header->column_count_ = ctx.column_cnt_;
      header->rowkey_column_count_ = ctx.rowkey_column_cnt_;
      header->has_column_checksum_ = ctx.need_calc_column_chksum_;
      if (header->has_column_checksum_) {
        header->column_checksums_ = reinterpret_cast<int64_t *>(
            data_buffer_.data() + ObMicroBlockHeader::COLUMN_CHECKSUM_PTR_OFFSET);
      }
    }
  }
  return ret;
}
int ObMicroBlockEncoder::store_encoding_meta_and_fix_cols(ObBufferWriter &buf_writer, int64_t &encoding_meta_offset)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    ObMicroBlockHeader *header = get_header(data_buffer_);
    // detect extend value bit
    header->extend_value_bit_ = 0;
    FOREACH(e, encoders_) {
      ObIColumnEncoder::EncoderDesc &desc = (*e)->get_desc();
      if (desc.has_null_ || desc.has_nope_) {
        header->extend_value_bit_ = 1;
        if (desc.has_nope_) {
          header->extend_value_bit_ = 2;
          break;
        }
      }
    }
    FOREACH(e, encoders_) {
      ((*e))->set_extend_value_bit(header->extend_value_bit_);
    }

    const int64_t header_size = data_buffer_.length();
    const int64_t col_header_size = ctx_.column_cnt_ * (sizeof(ObColumnHeader));
    encoding_meta_offset = header_size + col_header_size;

    if (OB_FAIL(data_buffer_.write_nop(col_header_size))) {
      LOG_WARN("advance data buffer failed", K(ret), K(col_header_size), K(encoding_meta_offset));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < encoders_.count(); ++i) {
        int64_t pos_bak = buf_writer.length();
        ObIColumnEncoder::EncoderDesc &desc = encoders_.at(i)->get_desc();
        if (OB_FAIL(encoders_.at(i)->store_meta(buf_writer))) {
          LOG_WARN("store encoding meta failed", K(ret));
        } else {
          ObColumnHeader &ch = encoders_.at(i)->get_column_header();
          if (buf_writer.length() > pos_bak) {
            ch.offset_ = static_cast<uint32_t>(pos_bak);
            ch.length_ = static_cast<uint32_t>(buf_writer.length() - pos_bak);
          } else if (ObColumnHeader::RAW == encoders_.at(i)->get_type()) {
            // column header offset records the start pos of the fix data, if needed
            ch.offset_ = static_cast<uint32_t>(pos_bak);
          }
          ch.obj_type_ = static_cast<uint8_t>(encoders_.at(i)->get_obj_type());
        }

        if (OB_SUCC(ret) && !desc.is_var_data_ && desc.need_data_store_) {
          if (OB_FAIL(encoders_.at(i)->store_fix_data(buf_writer))) {
            LOG_WARN("failed to store fixed data", K(ret));
          }
        }
      }
    }

    if (OB_SUCC(ret)) {
      get_header(data_buffer_)->row_data_offset_ = static_cast<int32_t>(encoding_meta_offset + buf_writer.length());
    }
  }
  return ret;
}

int ObMicroBlockEncoder::build_block(char *&buf, int64_t &size)
{
  int ret = OB_SUCCESS;
  int64_t encoders_need_size = 0;
  const int64_t col_header_size = ctx_.column_cnt_ * (sizeof(ObColumnHeader));
  char *encoding_meta_buf = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(datum_rows_.empty())) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("empty micro block", K(ret));
  } else if (OB_FAIL(set_datum_rows_ptr())) {
    STORAGE_LOG(WARN, "fail to set datum rows ptr", K(ret));
  } else if (OB_FAIL(pivot())) {
    LOG_WARN("pivot rows to columns failed", K(ret));
  } else if (OB_FAIL(row_indexs_.reserve(datum_rows_.count()))) {
    LOG_WARN("array reserve failed", K(ret), "count", datum_rows_.count());
  } else if (OB_FAIL(encoder_detection(encoders_need_size))) {
    LOG_WARN("detect column encoding failed", K(ret));
  } else if (OB_FAIL(data_buffer_.ensure_space(col_header_size + encoders_need_size))) {
    STORAGE_LOG(WARN, "fail to ensure space", K(ret), K(data_buffer_));
  // encoder pointers depend on this memory during build_block(), and its life cycyle needs to be longer than build_block().
  } else if (OB_ISNULL(encoding_meta_buf = static_cast<char *>(encoding_meta_allocator_.alloc(encoders_need_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "fail to alloc fix header buf", K(ret), K(encoders_need_size));
  } else {
    STORAGE_LOG(DEBUG, "[debug] build micro block", K_(estimate_size), K_(header_size), K_(expand_pct),
        K(datum_rows_.count()), K(ctx_));

    // <1> store encoding metas and fix cols data in encoding_meta_buffer
    int64_t encoding_meta_offset = 0;
    int64_t encoding_meta_size = 0;
    ObBufferWriter meta_buf_writer(encoding_meta_buf, encoders_need_size, 0);
    if (OB_FAIL(store_encoding_meta_and_fix_cols(meta_buf_writer, encoding_meta_offset))) {
      LOG_WARN("failed to store encoding meta and fixed col data", K(ret));
    } else if (FALSE_IT(encoding_meta_size = meta_buf_writer.length())) {
    } else if (OB_FAIL(data_buffer_.write_nop(encoding_meta_size))) {
      STORAGE_LOG(WARN, "failed to write nop", K(ret), K(meta_buf_writer), K(data_buffer_));
    }

    // <2> set row data store offset
    int64_t fix_data_size = 0;
    if (OB_SUCC(ret)) {
      if (OB_FAIL(set_row_data_pos(fix_data_size))) {
        LOG_WARN("set row data position failed", K(ret));
      } else {
        get_header(data_buffer_)->var_column_count_ = static_cast<uint16_t>(var_data_encoders_.count());
      }
    }

    // <3> fill row data (i.e. var data)
    if (OB_SUCC(ret)) {
      if (OB_FAIL(fill_row_data(fix_data_size))) {
        LOG_WARN("fill row data failed", K(ret));
      }
    }

    // <4> fill row index
    if (OB_SUCC(ret)) {
      if (var_data_encoders_.empty()) {
        get_header(data_buffer_)->row_index_byte_ = 0;
      } else {
        get_header(data_buffer_)->row_index_byte_ = 2;
        if (row_indexs_.at(row_indexs_.count() - 1) > UINT16_MAX) {
          get_header(data_buffer_)->row_index_byte_ = 4;
        }
        ObIntegerArrayGenerator gen;
        const int64_t row_index_size = row_indexs_.count() * get_header(data_buffer_)->row_index_byte_;
        if (OB_FAIL(data_buffer_.ensure_space(row_index_size))) {
          STORAGE_LOG(WARN, "fail to ensure space", K(ret), K(row_index_size), K(data_buffer_));
        } else if (OB_FAIL(gen.init(data_buffer_.data() + data_buffer_.length(), get_header(data_buffer_)->row_index_byte_))) {
          LOG_WARN("init integer array generator failed",
              K(ret), "byte", get_header(data_buffer_)->row_index_byte_);
        } else if (OB_FAIL(data_buffer_.write_nop(row_index_size))) {
          LOG_WARN("advance data buffer failed", K(ret), K(row_index_size));
        } else {
          for (int64_t idx = 0; idx < row_indexs_.count(); ++idx) {
            gen.get_array().set(idx, row_indexs_.at(idx));
          }
        }
      }
    }

    // <5> fill header, encoding_meta and fix cols data
    if (OB_SUCC(ret)) {
      get_header(data_buffer_)->row_count_ = static_cast<uint32_t>(datum_rows_.count());
      get_header(data_buffer_)->has_string_out_row_ = has_string_out_row_;
      get_header(data_buffer_)->all_lob_in_row_ = !has_lob_out_row_;
      get_header(data_buffer_)->max_merged_trans_version_ = max_merged_trans_version_;
      const int64_t header_size = get_header(data_buffer_)->header_size_;
      char *data = data_buffer_.data() + header_size;
      FOREACH(e, encoders_) {
        MEMCPY(data, &(*e)->get_column_header(), sizeof(ObColumnHeader));
        data += sizeof(ObColumnHeader);
      }
      // fill encoding meta and fix cols data
      MEMCPY(data_buffer_.data() + encoding_meta_offset, encoding_meta_buf, encoding_meta_size);
    }

    if (OB_SUCC(ret)) {
      // update encoding context
      ctx_.estimate_block_size_ += estimate_size_;
      ctx_.real_block_size_ += data_buffer_.length() - encoding_meta_offset;
      ctx_.micro_block_cnt_++;
      ObPreviousEncoding pe;
      for (int64_t idx = 0; OB_SUCC(ret) && idx < encoders_.count(); ++idx) {
        ObIColumnEncoder *e = encoders_.at(idx);
        pe.type_ = static_cast<ObColumnHeader::Type>(e->get_column_header().type_);
        if (ObColumnHeader::is_inter_column_encoder(pe.type_)) {
          pe.ref_col_idx_ = static_cast<ObSpanColumnEncoder *>(e)->get_ref_col_idx();
        } else {
          pe.ref_col_idx_ = 0;
        }
        if (ObColumnHeader::STRING_PREFIX == pe.type_) {
          pe.last_prefix_length_ = col_ctxs_.at(idx).last_prefix_length_;
        }
        if (idx < ctx_.previous_encodings_.count()) {
          if (OB_FAIL(ctx_.previous_encodings_.at(idx).put(pe))) {
            LOG_WARN("failed to store previous encoding", K(ret), K(idx), K(pe));
          }

          //if (ctx_->previous_encodings_.at(idx).last_1 != pe.type_) {
            //LOG_DEBUG("encoder changing", K(idx),
                //"previous type", ctx_->previous_encodings_.at(idx).last_,
                //"current type", pe);
          //}
        } else {
          ObPreviousEncodingArray<ObMicroBlockEncodingCtx::MAX_PREV_ENCODING_COUNT> pe_array;
          if (OB_FAIL(pe_array.put(pe))) {
            LOG_WARN("failed to store previous encoding", K(ret), K(idx), K(pe));
          } else if (OB_FAIL(ctx_.previous_encodings_.push_back(pe_array))) {
            LOG_WARN("push back previous encoding failed");
          }
        }
      }
    } else {
      // Print status of current encoders for debugging
      print_micro_block_encoder_status();
      if (OB_BUF_NOT_ENOUGH == ret) {
        // buffer not enough when building a block with pivoted rows, probably caused by estimate
        // maximum block size after encoding failed, rewrite errno with OB_ENCODING_EST_SIZE_OVERFLOW
        // to force compaction task retry with flat row store type.
        ret = OB_ENCODING_EST_SIZE_OVERFLOW;
        LOG_WARN("build block failed by probably estimated maximum encoding data size overflow", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      buf = data_buffer_.data();
      size = data_buffer_.length();
    }
  }

  return ret;
}

int ObMicroBlockEncoder::set_row_data_pos(int64_t &fix_data_size)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(datum_rows_.empty())) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("empty micro block", K(ret));
  } else {

    // detect extend value bit size and dispatch encoders.
    int64_t ext_bit_size = 0;
    ObMicroBlockHeader *header = get_header(data_buffer_);
    FOREACH_X(e, encoders_, OB_SUCC(ret)) {
      ObIColumnEncoder::EncoderDesc &desc = (*e)->get_desc();
      if (desc.need_data_store_ && desc.is_var_data_) {
        if (OB_FAIL(var_data_encoders_.push_back(*e))) {
          LOG_WARN("add encoder to array failed", K(ret));
        } else if (desc.need_extend_value_bit_store_) {
          (*e)->get_column_header().extend_value_index_ = static_cast<int16_t>(ext_bit_size);
          ext_bit_size += header->extend_value_bit_;
        }
      }
    }
    fix_data_size = (ext_bit_size + CHAR_BIT - 1) / CHAR_BIT;

    // set var data position
    for (int64_t i = 0; OB_SUCC(ret) && i < var_data_encoders_.count(); ++i) {
      var_data_encoders_.at(i)->get_desc().row_offset_ = fix_data_size;
      var_data_encoders_.at(i)->get_desc().row_length_ = i;
      if (OB_FAIL(var_data_encoders_.at(i)->set_data_pos(fix_data_size, i))) {
        LOG_WARN("set data position failed", K(ret), K(fix_data_size), "index", i);
      }
    }
    if (OB_SUCC(ret) && !var_data_encoders_.empty()) {
      ObColumnHeader &ch = var_data_encoders_.at(var_data_encoders_.count() - 1)
          ->get_column_header();
      ch.attr_ |= ObColumnHeader::LAST_VAR_FIELD;
    }
  }
  return ret;
}

OB_INLINE int ObMicroBlockEncoder::store_data(ObIColumnEncoder &e,
    const int64_t row_id, ObBitStream &bs, char *buf, const int64_t len)
{
  // performance critical, do not check parameters
  /* use switch case and inline
  int ret = OB_SUCCESS;
  switch (e.get_column_header().type_) {
    case ObColumnHeader::RAW: {
      ret = static_cast<ObRawEncoder &>(e).ObRawEncoder::store_data(row_id, bs, buf, len);
      break;
    };
    case ObColumnHeader::DICT: {
      ret = static_cast<ObDictEncoder &>(e).ObDictEncoder::store_data(row_id, bs, buf, len);
      break;
    };
    case ObColumnHeader::RLE: {
      ret = static_cast<ObRLEEncoder &>(e).ObRLEEncoder::store_data(row_id, bs, buf, len);
      break;
    };
    case ObColumnHeader::CONST: {
      ret = static_cast<ObConstEncoder &>(e).ObConstEncoder::store_data(row_id, bs, buf, len);
      break;
    };
    case ObColumnHeader::INTEGER_BASE_DIFF: {
      ret = static_cast<ObIntegerBaseDiffEncoder &>(e).ObIntegerBaseDiffEncoder::store_data(row_id, bs, buf, len);
      break;
    };
    case ObColumnHeader::STRING_DIFF: {
      ret = static_cast<ObStringDiffEncoder &>(e).ObStringDiffEncoder::store_data(row_id, bs, buf, len);
      break;
    };
    case ObColumnHeader::HEX_PACKING: {
      ret = static_cast<ObHexStringEncoder &>(e).ObHexStringEncoder::store_data(row_id, bs, buf, len);
      break;
    };
    default:
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected type", K(ret), "type", e.get_column_header().type_);
  }
  return ret;
  */

  // use c++ virtual function mechanism
  return e.store_data(row_id, bs, buf, len);
}

int ObMicroBlockEncoder::fill_row_data(const int64_t fix_data_size)
{
  int ret = OB_SUCCESS;
  ObArray<int64_t> var_lengths;
  var_lengths.set_attr(ObMemAttr(MTL_ID(), "MicroBlkEncoder"));
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(datum_rows_.empty())) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("empty micro block", K(ret));
  } else if (OB_FAIL(var_lengths.reserve(var_data_encoders_.count()))) {
    LOG_WARN("reserve array failed", K(ret), "count", var_data_encoders_.count());
  } else if (OB_FAIL(row_indexs_.push_back(0))) {
    LOG_WARN("add row index failed", K(ret));
  } else {
    const int64_t row_data_offset = get_header(data_buffer_)->row_data_offset_;
    for (int64_t i = 0; OB_SUCC(ret) && i < var_data_encoders_.count(); ++i) {
      if (OB_FAIL(var_lengths.push_back(0))) {
        LOG_WARN("add var data length failed", K(ret));
      }
    }

    for (int64_t row_id = 0; OB_SUCC(ret) && row_id < datum_rows_.count(); ++row_id) {
      int64_t var_data_size = 0;
      int64_t column_index_byte = 0;
      int64_t row_size = 0;
      // detect column index byte
      if (var_data_encoders_.count() > 0) {
        for (int64_t i = 0; OB_SUCC(ret) && i < var_data_encoders_.count(); ++i) {
          int64_t length = 0;
          if (OB_FAIL(var_data_encoders_.at(i)->get_var_length(row_id, length))) {
            LOG_WARN("get var data length failed", K(ret), K(row_id));
          } else {
            var_lengths.at(i) = length;
            if (i > 0 && i == var_data_encoders_.count() - 1) {
              column_index_byte = var_data_size <= UINT8_MAX ? 1 : 2;
              if (OB_UNLIKELY(var_data_size > UINT16_MAX)) {
                column_index_byte = 4;
              }
            }
            var_data_size += length;
          }
        }
      }

      // fill var data
      row_size = fix_data_size + var_data_size + (column_index_byte > 0 ? 1 : 0)
          + column_index_byte * (var_data_encoders_.count() - 1);
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(data_buffer_.ensure_space(row_size))) {
        STORAGE_LOG(WARN, "failed to ensure space", K(ret), K(row_size), K(data_buffer_));
      } else {
        char *data = data_buffer_.current();
        ObBitStream bs;
        if (OB_FAIL(data_buffer_.write_nop(row_size))) {
          LOG_WARN("advance buffer failed", K(ret), K_(data_buffer), K(row_size));
        } else if (OB_FAIL(bs.init(reinterpret_cast<unsigned char *>(data), fix_data_size))) {
          LOG_WARN("init bit stream failed", K(ret), K(fix_data_size));
        } else {
          MEMSET(data, 0, row_size);
          char *column_index_byte_ptr = data + fix_data_size;

          ObIntegerArrayGenerator gen;
          if (OB_SUCC(ret) && column_index_byte > 0) {
            if (OB_FAIL(gen.init(data + fix_data_size + 1, column_index_byte))) {
              LOG_WARN("init integer array generator failed ", K(ret), K(column_index_byte));
            }
          }
          if (OB_SUCC(ret) && !var_data_encoders_.empty()) {
            char *var_data = column_index_byte_ptr;
            if (column_index_byte > 0) {
              *column_index_byte_ptr = static_cast<int8_t>(column_index_byte);
              var_data +=  1 + column_index_byte * (var_data_encoders_.count() - 1);
            }
            int64_t offset = 0;
            for (int64_t i = 0; OB_SUCC(ret) && i < var_data_encoders_.count(); ++i) {
              if (i > 0) {
                gen.get_array().set(i - 1, offset);
              }
              const int64_t length = var_lengths.at(i);
              if (OB_FAIL(store_data(*var_data_encoders_.at(i),
                  row_id, bs, var_data + offset, length))) {
                LOG_WARN("store var length data failed",
                    K(ret), K(row_id), K(offset), K(length));
              }
              offset += length;
            }
          }
        }
      }

      if (OB_SUCC(ret)) {
        if (OB_FAIL(row_indexs_.push_back(data_buffer_.length() - row_data_offset))) {
          LOG_WARN("add row index failed", K(ret));
        }
      }
    }
  }

  return ret;
}

int ObMicroBlockEncoder::init_column_ctxs()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    ObColumnEncodingCtx cc;
    for (int64_t i = 0; OB_SUCC(ret) && i < ctx_.column_cnt_; ++i) {
      cc.reset();
      cc.encoding_ctx_ = &ctx_;
      if (OB_FAIL(col_ctxs_.push_back(cc))) {
        LOG_WARN("failed to push back column ctx", K(ret));
      }
    }
  }
  return ret;
}

int ObMicroBlockEncoder::set_datum_rows_ptr()
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(row_buf_holder_.has_expand())) {
    for (int64_t i = 0; OB_SUCC(ret) && i < datum_rows_.count(); i++) {
      ObConstDatumRow &datum_row = datum_rows_.at(i);
      if (OB_FAIL(datum_row.set_datums_ptr(row_buf_holder_.data()))) {
        STORAGE_LOG(WARN, "fail to set datums ptr", K(ret), K(datum_row), K(row_buf_holder_));
      }
    }
  }

  return ret;
}

int ObMicroBlockEncoder::process_out_row_columns(const ObDatumRow &row)
{
  // make sure in&out row status of all values in a column are same
  int ret = OB_SUCCESS;
  if (!need_check_lob_) {
  } else if (OB_UNLIKELY(row.get_column_count() != col_ctxs_.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected column count not match", K(ret));
  } else if (!has_lob_out_row_) {
    for (int64_t i = 0; !has_lob_out_row_ && OB_SUCC(ret) && i < row.get_column_count(); ++i) {
      ObStorageDatum &datum = row.storage_datums_[i];
      if (ctx_.col_descs_->at(i).col_type_.is_lob_storage()) {
        if (datum.is_nop() || datum.is_null()) {
        } else if (datum.len_ < sizeof(ObLobCommon)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("Unexpected lob datum len", K(ret), K(i), K(ctx_.col_descs_->at(i).col_type_), K(datum));
        } else {
          const ObLobCommon &lob_common = datum.get_lob_data();
          has_lob_out_row_ = !lob_common.in_row_;
          LOG_DEBUG("chaser debug lob out row", K(has_lob_out_row_), K(lob_common), K(datum));
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

int ObMicroBlockEncoder::copy_and_append_row(const ObDatumRow &src, int64_t &store_size)
{
  int ret = OB_SUCCESS;
  // performance critical, do not double check parameters in private method
  const int64_t datums_len = sizeof(ObDatum) * src.get_column_count();
  bool is_large_row = false;
  const int64_t datum_row_offset = length_;
  ObDatum *datum_arr = nullptr;
  if (datum_rows_.count() > 0
      && (length_ + datums_len >= estimate_size_limit_ || estimate_size_ >= estimate_size_limit_)) {
    ret = OB_BUF_NOT_ENOUGH;
  } else if (0 == datum_rows_.count() && length_ + datums_len >= estimate_size_limit_) {
    is_large_row = true;
  } else if (OB_FAIL(row_buf_holder_.ensure_space(datums_len))) {
    STORAGE_LOG(WARN, "fail to ensure space", K(ret), K(datums_len), K(row_buf_holder_));
  } else {
    bool is_finish = false;
    while(OB_SUCC(ret) && !is_finish) {
      char *datum_arr_buf = row_buf_holder_.data() + datum_row_offset;
      const int64_t buffer_size = row_buf_holder_.size();
      MEMSET(datum_arr_buf, 0, datums_len);
      datum_arr = reinterpret_cast<ObDatum *>(datum_arr_buf);
      length_ = datum_row_offset + datums_len;
      store_size = 0;
      is_finish = true;
      bool is_buffer_not_enough = false;
      for (int64_t col_idx = 0;
          OB_SUCC(ret) && col_idx < src.get_column_count() && !is_large_row; ++col_idx) {
        if (OB_FAIL(copy_cell(
            ctx_.col_descs_->at(col_idx),
            src.storage_datums_[col_idx],
            datum_arr[col_idx],
            store_size,
            is_large_row,
            is_buffer_not_enough))) {
          if (OB_UNLIKELY(OB_BUF_NOT_ENOUGH != ret)) {
            LOG_WARN("fail to copy cell", K(ret), K(col_idx), K(src), K(store_size), K(is_large_row));
          }
        } else if (is_buffer_not_enough) {
          int64_t var_len_column_cnt = 0;
          int64_t need_size = calc_datum_row_size(src, var_len_column_cnt);
          if (ctx_.major_working_cluster_version_ >= DATA_VERSION_4_1_0_0) {
            need_size = need_size + var_len_column_cnt * sizeof(uint64_t);
          }
          if (OB_UNLIKELY(need_size <= row_buf_holder_.size() - row_buf_holder_.length())) {
            ret = OB_ERR_UNEXPECTED;
            STORAGE_LOG(WARN, "unexpected buffer not enough", K(ret), K(need_size), K(row_buf_holder_));
          } else if (OB_FAIL(row_buf_holder_.ensure_space(need_size))) {// ensure cell of row is contiguous
            STORAGE_LOG(WARN, "failed to ensure space", K(ret), K(need_size), K(row_buf_holder_));
          } else {
            is_finish = false;
            break;
          }
        }
      }
    }
    if (OB_SUCC(ret) && !is_large_row) {
      if (OB_FAIL(row_buf_holder_.write_nop(length_ - datum_row_offset))) {
        STORAGE_LOG(WARN, "fail to write nop", K(ret), K(length_), K(datum_row_offset), K(row_buf_holder_));
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (is_large_row && OB_FAIL(process_large_row(src, datum_arr, store_size))) {
    LOG_WARN("fail to process large row", K(ret));
  } else if (OB_FAIL(try_to_append_row(store_size))) {
    if (OB_UNLIKELY(OB_BUF_NOT_ENOUGH != ret)) {
      LOG_WARN("fail to try append row", K(ret));
    }
  } else {
    ObConstDatumRow datum_row(datum_arr, src.get_column_count(), datum_row_offset);
    if (OB_FAIL(datum_rows_.push_back(datum_row))) {
      LOG_WARN("append row to array failed", K(ret), K(src));
    }
  }
  return ret;
}

int ObMicroBlockEncoder::copy_cell(
    const ObColDesc &col_desc,
    const ObStorageDatum &src,
    ObDatum &dest,
    int64_t &store_size,
    bool &is_large_row,
    bool &is_buffer_not_enough)
{
  // For IntSC and UIntSC, normalize to 8 byte
  int ret = OB_SUCCESS;
  ObObjTypeStoreClass store_class = get_store_class_map()[col_desc.col_type_.get_type_class()];
  const bool is_int_sc = store_class == ObIntSC || store_class == ObUIntSC;
  int64_t datum_size = 0;
  is_buffer_not_enough = false;
  dest.ptr_ = row_buf_holder_.data() + length_; //reset by set_datum_rows_ptr
  dest.pack_ = src.pack_;
  if (src.is_null()) {
    dest.set_null();
    datum_size = sizeof(uint64_t);
  } else if (src.is_nop()) {
    datum_size = dest.len_;
  } else if (OB_UNLIKELY(src.is_ext())) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("unsupported store extend datum type", K(ret), K(src));
  } else {
    datum_size = is_int_sc ? sizeof(uint64_t) : dest.len_;
    if (ctx_.major_working_cluster_version_ >= DATA_VERSION_4_1_0_0) {
      if (is_var_length_type(store_class)) {
        // For var-length type column, we need to add extra 8 bytes for estimate safety
        // e.g: ref size for dictionary, meta data for encoding etc.
        datum_size += sizeof(uint64_t);
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (FALSE_IT(store_size += datum_size)) {
  } else if (datum_rows_.count() > 0 && estimate_size_ + store_size >= estimate_size_limit_) {
    ret = OB_BUF_NOT_ENOUGH;
    // for large row whose size larger than a micro block default size,
    // we still use micro block to store it, but its size is unknown, need special treat
  } else if (datum_rows_.count() == 0 && estimate_size_ + store_size >= estimate_size_limit_) {
    is_large_row = true;
  } else if (row_buf_holder_.size() < length_ + datum_size) {
    is_buffer_not_enough = true;
  } else {
    if (is_int_sc) {
      MEMSET(const_cast<char *>(dest.ptr_), 0, datum_size);
    }
    MEMCPY(const_cast<char *>(dest.ptr_), src.ptr_, dest.len_);
    length_ += datum_size;
  }
  return ret;
}

int ObMicroBlockEncoder::process_large_row(
    const ObDatumRow &src,
    ObDatum *&datum_arr,
    int64_t &store_size)
{
  int ret = OB_SUCCESS;
  // copy_size is the size in memory, including the ObDatum struct and extra space for data.
  // store_size represents for the serialized data size on disk,
  const int64_t datums_len = sizeof(ObDatum) * src.get_column_count();
  int64_t var_len_column_cnt = 0;
  int64_t copy_size = calc_datum_row_size(src, var_len_column_cnt);
  if (ctx_.major_working_cluster_version_ >= DATA_VERSION_4_1_0_0) {
    store_size = copy_size - datums_len + var_len_column_cnt * sizeof(uint64_t);
  } else {
    store_size = copy_size - datums_len;
  }
  if (OB_UNLIKELY(row_buf_holder_.length() != 0)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "unexpected row buffer len", K(ret), K(row_buf_holder_));
  } else if (OB_FAIL(row_buf_holder_.write_nop(copy_size))) {
    LOG_WARN("Fail to ensure_space", K(ret), K(copy_size), K(row_buf_holder_));
  } else {
    char *buf = row_buf_holder_.data();
    MEMSET(buf, 0, datums_len);
    datum_arr = reinterpret_cast<ObDatum *>(buf);
    buf += datums_len;

    for (int64_t col_idx = 0; col_idx < src.get_column_count(); ++col_idx) {
      const ObColDesc &col_desc = ctx_.col_descs_->at(col_idx);
      ObObjTypeStoreClass store_class = get_store_class_map()[col_desc.col_type_.get_type_class()];
      const bool is_int_sc = (store_class == ObIntSC || store_class == ObUIntSC);
      ObStorageDatum &src_datum = src.storage_datums_[col_idx];
      ObDatum &dest_datum = datum_arr[col_idx];
      if (src_datum.is_null()) {
        dest_datum.set_null();
        MEMSET(buf, 0, sizeof(uint64_t));
        buf += sizeof(uint64_t);
      } else {
        dest_datum.ptr_ = buf;
        dest_datum.pack_ = src_datum.pack_;
        const int64_t datum_data_size = is_int_sc ? sizeof(uint64_t) : src_datum.len_;
        if (is_int_sc) {
          MEMSET(const_cast<char *>(dest_datum.ptr_), 0, datum_data_size);
        }
        MEMCPY(const_cast<char *>(dest_datum.ptr_), src_datum.ptr_, src_datum.len_);
        buf += datum_data_size;
      }
    }
  }
  return ret;
}

int64_t ObMicroBlockEncoder::calc_datum_row_size(const ObDatumRow &src, int64_t &var_len_column_cnt) const
{
  int64_t need_size = sizeof(ObDatum) * src.get_column_count();
  var_len_column_cnt = 0;

  for (int64_t col_idx = 0; col_idx < src.get_column_count(); ++col_idx) {
    const ObColDesc &col_desc = ctx_.col_descs_->at(col_idx);
    ObObjTypeStoreClass store_class = get_store_class_map()[col_desc.col_type_.get_type_class()];
    ObStorageDatum &datum = src.storage_datums_[col_idx];
    if (!datum.is_null()) {
      if (store_class == ObIntSC || store_class == ObUIntSC) {
        need_size += sizeof(uint64_t);
      } else {
        need_size += datum.len_;
        if (is_var_length_type(store_class)) {
          ++var_len_column_cnt;
        }
      }
    } else {
      need_size += sizeof(uint64_t);
    }
  }
  return need_size;
}

int ObMicroBlockEncoder::prescan(const int64_t column_index)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    const ObColDesc &col_desc = ctx_.col_descs_->at(column_index);
    const ObObjMeta column_type = col_desc.col_type_;
    const ObObjTypeStoreClass store_class =
        get_store_class_map()[ob_obj_type_class(column_type.get_type())];
    int64_t type_store_size = get_type_size_map()[column_type.get_type()];
    ObColumnEncodingCtx &col_ctx = col_ctxs_.at(column_index);
    col_ctx.col_datums_ = all_col_datums_.at(column_index);

    // build hashtable
    ObEncodingHashTable *ht = nullptr;
    ObEncodingHashTableBuilder *builder = nullptr;
    ObMultiPrefixTree *prefix_tree = nullptr;
    // next power of 2
    uint64_t bucket_num = datum_rows_.count() * 2;
    if (0 != (bucket_num & (bucket_num - 1))) {
      while (0 != (bucket_num & (bucket_num - 1))) {
        bucket_num = bucket_num & (bucket_num - 1);
      }
      bucket_num = bucket_num * 2;
    }
    const int64_t node_num = datum_rows_.count();
    if (OB_FAIL(hashtable_factory_.create(bucket_num, node_num, ht))) {
      LOG_WARN("create hashtable failed", K(ret), K(bucket_num), K(node_num));
    } else if (OB_FAIL(multi_prefix_tree_factory_.create(node_num, bucket_num, prefix_tree))) {
      LOG_WARN("failed to create multi-prefix tree", K(ret), K(bucket_num));
    } else if (FALSE_IT(builder = static_cast<ObEncodingHashTableBuilder *>(ht))) {
    } else if (OB_FAIL(builder->build(*col_ctx.col_datums_, col_desc))) {
      LOG_WARN("build hash table failed", K(ret), K(column_index), K(column_type));
    }

    if (OB_SUCC(ret)) {
      col_ctx.prefix_tree_ = prefix_tree;
      if (OB_FAIL(build_column_encoding_ctx(ht, store_class, type_store_size, col_ctx))) {
        LOG_WARN("build_column_encoding_ctx failed",
            K(ret), KP(ht), K(store_class), K(type_store_size));
      } else if (OB_FAIL(hashtables_.push_back(ht))) {
        LOG_WARN("failed to push back", K(ret));
      } else if (OB_FAIL(multi_prefix_trees_.push_back(prefix_tree))) {
        LOG_WARN("failed to push back multi-prefix tree", K(ret));
      }
      LOG_DEBUG("hash table", K(column_index), K(*ht));
    }

    if (OB_FAIL(ret)) {
      // avoid overwirte ret
      int temp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (temp_ret = hashtable_factory_.recycle(false, ht))) {
        LOG_WARN("recycle hashtable failed", K(temp_ret));
      }
      if (OB_SUCCESS != (temp_ret = multi_prefix_tree_factory_.recycle(prefix_tree))) {
        LOG_WARN("failed to recycle multi-prefix tree", K(temp_ret));
      }
    }
  }
  return ret;
}

int ObMicroBlockEncoder::encoder_detection(int64_t &encoders_need_size)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < ctx_.column_cnt_; ++i) {
      if (OB_FAIL(prescan(i))) {
        LOG_WARN("failed to prescan", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      int64_t extend_value_bit = 1;
      FOREACH(c, col_ctxs_) {
        if (c->nope_cnt_ > 0) {
          extend_value_bit = 2;
          break;
        }
      }
      FOREACH(c, col_ctxs_) {
        c->extend_value_bit_ = extend_value_bit;
      }
    }
    encoders_need_size = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i <ctx_.column_cnt_; ++i) {
      if (OB_FAIL(fast_encoder_detect(i, col_ctxs_.at(i)))) {
        LOG_WARN("fast encoder detect failed", K(ret), K(i));
      } else {
        if (encoders_.count() <= i) {
          if (col_ctxs_.at(i).only_raw_encoding_) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("The column should only use raw_encoding", "col_ctx", col_ctxs_.at(i), K(i), K(ret));
          } else if (OB_FAIL(choose_encoder(i, col_ctxs_.at(i)))) {
            LOG_WARN("choose_encoder failed", K(ret), K(i));
          }
        }
      }
      int64_t need_size = 0;
      int64_t encoder_cnt = encoders_.count();
      if (OB_FAIL(ret)) {
      } else if (OB_UNLIKELY(encoder_cnt <= i)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "unexpected encoder cnt", K(ret), K(encoder_cnt), K(i));
      } else if (OB_FAIL(encoders_.at(encoder_cnt - 1)->get_encoding_store_meta_need_space(need_size))) {
        STORAGE_LOG(WARN, "fail to get_encoding_store_meta_need_space", K(ret), K(i), K(encoders_));
      } else {
        need_size += encoders_.at(encoder_cnt - 1)->calc_encoding_fix_data_need_space();
        encoders_need_size += need_size;
      }
    }
    if (OB_FAIL(ret)) {
      free_encoders();
    }
  }

  return ret;
}

int ObMicroBlockEncoder::fast_encoder_detect(
    const int64_t column_idx, const ObColumnEncodingCtx &cc)
{
  int ret = OB_SUCCESS;
  ObIColumnEncoder *e = NULL;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(column_idx < 0 || column_idx >= ctx_.column_cnt_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid column_idx", K(ret), K(column_idx));
  } else if (nullptr != ctx_.column_encodings_
      && ctx_.column_encodings_[column_idx] > 0
      && ctx_.column_encodings_[column_idx] < ObColumnHeader::Type::MAX_TYPE) {
    LOG_INFO("specified encoding", K(column_idx), K(ctx_.column_encodings_[column_idx]));
    if (OB_FAIL(try_encoder(e, column_idx,
        static_cast<ObColumnHeader::Type>(ctx_.column_encodings_[column_idx]), 0, -1))) {
      LOG_WARN("try encoding failed", K(ret), K(column_idx),
          K(ctx_.column_encodings_[column_idx]));
    } else if (OB_ISNULL(e)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("specified encoder is NULL", K(ret), K(column_idx),
          K(ctx_.column_encodings_[column_idx]));
    }
  } else if (cc.only_raw_encoding_) {
    if (OB_FAIL(force_raw_encoding(column_idx, true, e))) {
      LOG_WARN("force raw encoding failed", K(ret), K(column_idx));
    } else if (OB_ISNULL(e)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("raw encoder is NULL", K(ret), K(column_idx));
    }
  } else if (cc.ht_->distinct_cnt() <= 1) {
    if (OB_FAIL(try_encoder<ObConstEncoder>(e, column_idx))) {
      LOG_WARN("try encoder failed");
    }
  }
  if (OB_SUCC(ret) && NULL != e) {
    LOG_DEBUG("used encoder (fast)", K(column_idx),
        "column_header", e->get_column_header(),
        "data_desc", e->get_desc());
    if (OB_FAIL(encoders_.push_back(e))) {
      LOG_WARN("push back encoder failed", K(ret));
      free_encoder(e);
      e = NULL;
    }
  }
  return ret;
}

int ObMicroBlockEncoder::try_previous_encoder(ObIColumnEncoder *&e,
    const int64_t column_index, const ObPreviousEncoding &previous)
{
  return try_encoder(e, column_index, previous.type_,
      previous.last_prefix_length_, previous.ref_col_idx_);
}

int ObMicroBlockEncoder::try_encoder(ObIColumnEncoder *&e, const int64_t column_index,
      const ObColumnHeader::Type type, const int64_t last_prefix_length,
      const int64_t ref_col_idx)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(column_index < 0 || column_index > ctx_.column_cnt_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(column_index));
  } else {
    switch (type) {
      case ObColumnHeader::RAW: {
        ret = try_encoder<ObRawEncoder>(e, column_index);
        break;
      }
      case ObColumnHeader::DICT: {
        ret = try_encoder<ObDictEncoder>(e, column_index);
        break;
      }
      case ObColumnHeader::RLE: {
        ret = try_encoder<ObRLEEncoder>(e, column_index);
        break;
      }
      case ObColumnHeader::CONST: {
        ret = try_encoder<ObConstEncoder>(e, column_index);
        break;
      }
      case ObColumnHeader::INTEGER_BASE_DIFF: {
        ret = try_encoder<ObIntegerBaseDiffEncoder>(e, column_index);
        break;
      }
      case ObColumnHeader::STRING_DIFF: {
        ret = try_encoder<ObStringDiffEncoder>(e, column_index);
        break;
      }
      case ObColumnHeader::HEX_PACKING: {
        ret = try_encoder<ObHexStringEncoder>(e, column_index);
        break;
      }
      case ObColumnHeader::STRING_PREFIX: {
        col_ctxs_.at(column_index).last_prefix_length_ = last_prefix_length;
        ret = try_encoder<ObStringPrefixEncoder>(e, column_index);
        break;
      }
      case ObColumnHeader::COLUMN_EQUAL: {
        ret = ref_col_idx >= 0
              ? try_span_column_encoder<ObColumnEqualEncoder>(e, column_index, ref_col_idx)
              : try_span_column_encoder<ObColumnEqualEncoder>(e, column_index);
        break;
      }
      case ObColumnHeader::COLUMN_SUBSTR: {
        ret = ref_col_idx >= 0
              ? try_span_column_encoder<ObInterColSubStrEncoder>(e, column_index, ref_col_idx)
              : try_span_column_encoder<ObInterColSubStrEncoder>(e, column_index);
        break;
      }
      default:
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unknown encoding type", K(ret), K(type));
    }

    if (OB_FAIL(ret)) {
      LOG_WARN("try encoder failed", K(ret), K(type), K(column_index),
          K(last_prefix_length), K(ref_col_idx));
    }
  }
  return ret;
}

int ObMicroBlockEncoder::try_previous_encoder(ObIColumnEncoder *&choose,
    const int64_t column_index, const int64_t acceptable_size, bool &try_more)
{
  int ret = OB_SUCCESS;
  ObIColumnEncoder *e = NULL;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(column_index < 0 || column_index > ctx_.column_cnt_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(column_index));
  } else {
    bool need_calc = false;
    int64_t cycle_cnt = 0;
    if (32 < ctx_.micro_block_cnt_) {
      cycle_cnt = 16;
    } else if (16 < ctx_.micro_block_cnt_) {
      cycle_cnt = 8;
    } else {
      cycle_cnt = 4;
    }
    need_calc = (0 == ctx_.micro_block_cnt_ % cycle_cnt);

    if (column_index < ctx_.previous_encodings_.count()) {
      int64_t pos = ctx_.previous_encodings_.at(column_index).last_pos_;
      ObPreviousEncoding *prev = NULL;
      for (int64_t i = 0; OB_SUCC(ret) && i < ctx_.previous_encodings_.at(column_index).size_; ++i) {
        prev = &ctx_.previous_encodings_.at(column_index).prev_encodings_[pos];
        if (!col_ctxs_.at(column_index).detected_encoders_[prev->type_]) {
          col_ctxs_.at(column_index).detected_encoders_[prev->type_] = true;
          if (OB_FAIL(try_previous_encoder(e, column_index, *prev))) {
            LOG_WARN("try previous encoding failed", K(ret), K(column_index), "ctx", ctx_);
          } else if (NULL != e) {
            if (e->calc_size() <= choose->calc_size()) {
              free_encoder(choose);
              choose = e;
              if (!need_calc || choose->calc_size() <= acceptable_size) {
                try_more = false;
                LOG_DEBUG("choose encoding by previous micro block encoding", K(column_index),
                    "previous encoding array", ctx_.previous_encodings_.at(column_index));
              }
            } else {
              free_encoder(e);
              e = NULL;
            }
          }
        }
        pos = (pos == ctx_.previous_encodings_.at(column_index).size_ - 1) ? 0 : pos + 1;
      }
    }
  }
  return ret;
}

template <typename T>
int ObMicroBlockEncoder::try_span_column_encoder(ObIColumnEncoder *&e,
    const int64_t column_index)
{
  int ret = OB_SUCCESS;
  e = NULL;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(column_index < 0 || column_index > ctx_.column_cnt_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(column_index), "column_cnt", ctx_.column_cnt_);
  } else if (ctx_.encoder_opt_.enable<T>() && !col_ctxs_.at(column_index).is_refed_) {
    bool suitable = false;
    T *encoder = alloc_encoder<T>();
    if (NULL == encoder) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate memory failed", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < ctx_.column_cnt_ && !suitable; ++i) {
        if (column_index == i) {
        } else if (i < column_index && ObColumnHeader::is_inter_column_encoder(encoders_.at(i)->get_type())) {
          // column i refes others, continue
        } else if (ctx_.col_descs_->at(i).col_type_ == ctx_.col_descs_->at(column_index).col_type_) {
          encoder->reuse();
          if (OB_FAIL(try_span_column_encoder(encoder, column_index, i, suitable))) {
            LOG_WARN("try_span_column_encoder failed",
                K(ret), K(column_index), "ref_column_index", i);
          } else if (suitable) {
            col_ctxs_.at(i).is_refed_ = true;
            LOG_DEBUG("column is refed", K(column_index), "refed column", i);
          }
        }
      }
    }

    if (OB_SUCC(ret) && suitable) {
      e = encoder;
    } else if (NULL != encoder) {
      free_encoder(encoder);
      encoder = NULL;
    }
  }
  return ret;
}

template <typename T>
int ObMicroBlockEncoder::try_span_column_encoder(ObIColumnEncoder *&e,
    const int64_t column_index, const int64_t ref_column_index)
{
  int ret = OB_SUCCESS;
  e = NULL;
  bool suitable = false;
  if (ctx_.encoder_opt_.enable<T>() && !col_ctxs_.at(column_index).is_refed_) {
    if (ref_column_index < column_index
        && ObColumnHeader::is_inter_column_encoder(encoders_[ref_column_index]->get_type())) {
    } else {
      T *encoder = alloc_encoder<T>();
      if (NULL == encoder) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate memory failed", K(ret));
      } else if (OB_FAIL(try_span_column_encoder(encoder, column_index, ref_column_index, suitable))) {
        LOG_WARN("try_span_column_encoder failed", K(ret), K(column_index), K(ref_column_index));
      }

      if (OB_SUCC(ret) && suitable) {
        e = encoder;
      } else if (NULL != encoder) {
        free_encoder(encoder);
        encoder = NULL;
      }
    }
  }
  return ret;
}

int ObMicroBlockEncoder::try_span_column_encoder(ObSpanColumnEncoder *encoder,
    const int64_t column_index, const int64_t ref_column_index,
    bool &suitable)
{
  int ret = OB_SUCCESS;
  suitable = false;
  if (OB_UNLIKELY(
      nullptr == encoder
      || column_index < 0
      || ref_column_index < 0
      || ref_column_index > ctx_.column_cnt_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(encoder), K(column_index), K(ref_column_index));
  } else if (ref_column_index > UINT16_MAX) {
    // not suitable
  } else if (FALSE_IT(col_ctxs_.at(column_index).try_set_need_sort(encoder->get_type(), column_index))) {
  } else if (OB_FAIL(encoder->init(col_ctxs_[column_index], column_index, datum_rows_))) {
      LOG_WARN("encoder init failed", K(ret),
          "column_ctx", col_ctxs_[column_index], K(column_index));
  } else if (OB_FAIL(encoder->set_ref_col_idx(ref_column_index,
      col_ctxs_[ref_column_index]))) {
    LOG_WARN("set_ref_col_idx failed", K(ret), K(ref_column_index));
  } else if (OB_FAIL(encoder->traverse(suitable))) {
    LOG_WARN("traverse failed", K(ret));
  }
  return ret;
}

int ObMicroBlockEncoder::choose_encoder(const int64_t column_idx,
                                        ObColumnEncodingCtx &cc)
{
  int ret = OB_SUCCESS;
  ObIColumnEncoder *e = NULL;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(column_idx < 0 || column_idx >= ctx_.column_cnt_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid column_idx", K(column_idx), K(ret));
  } else if (OB_FAIL(try_encoder<ObRawEncoder>(e, column_idx))) {
    LOG_WARN("try raw encoder failed", K(ret));
  } else if (NULL == e) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("raw encoder can not be disabled and must always be suitable",
        K(ret), K(column_idx));
  } else {
    bool try_more = true;
    ObIColumnEncoder *choose = e;
    int64_t acceptable_size = choose->calc_size() / 4;
    if (OB_FAIL(try_encoder<ObDictEncoder>(e, column_idx))) {
      LOG_WARN("try dict encoder failed", K(ret), K(column_idx));
    } else if (NULL != e) {
      if (e->calc_size() < choose->calc_size()) {
        free_encoder(choose);
        choose = e;
      } else {
        free_encoder(e);
        e = NULL;
      }
    }

    // try previous micro block encoding
    if (OB_SUCC(ret) && try_more) {
      if (OB_FAIL(try_previous_encoder(choose, column_idx, acceptable_size, try_more))) {
        LOG_WARN("try previous encoder failed", K(ret), K(column_idx));
      }
    }

    // try column equal
    if (OB_SUCC(ret) && try_more && !cc.is_refed_ /* not refed by other */) {
      if (cc.detected_encoders_[ObColumnEqualEncoder::type_]) {
      } else if (OB_FAIL(try_span_column_encoder<ObColumnEqualEncoder>(e, column_idx))) {
        LOG_WARN("try span column encoder failed", K(ret), K(column_idx));
      } else if (NULL != e) {
        if (e->calc_size() < choose->calc_size()) {
          free_encoder(choose);
          choose = e;
          if (choose->calc_size() <= acceptable_size) {
            try_more = false;
          }
        } else {
          free_encoder(e);
          e = NULL;
        }
      }
    }

    // try inter column substring
    const ObObjTypeClass tc = ob_obj_type_class(ctx_.col_descs_->at(column_idx).col_type_.get_type());
    const ObObjTypeStoreClass sc = get_store_class_map()[tc];
    if (OB_SUCC(ret) && try_more && !cc.is_refed_ /* not refed by other*/) {
      if (is_string_encoding_valid(sc)) {
        if (cc.detected_encoders_[ObInterColSubStrEncoder::type_]) {
        } else if (OB_FAIL(try_span_column_encoder<ObInterColSubStrEncoder>(e, column_idx))) {
          LOG_WARN("try inter column substring encoder failed", K(ret), K(column_idx));
        } else if (NULL != e) {
          if (e->calc_size() < choose->calc_size()) {
            free_encoder(choose);
            choose = e;
            if (choose->calc_size() <= acceptable_size) {
              try_more = false;
            }
          } else {
            free_encoder(e);
            e = NULL;
          }
        }
      }
    }

    // try rle and const
    if (OB_SUCC(ret) && try_more && cc.ht_->distinct_cnt() <= datum_rows_.count() / 2) {
      if (cc.detected_encoders_[ObRLEEncoder::type_]) {
      } else if (OB_FAIL(try_encoder<ObRLEEncoder>(e, column_idx))) {
        LOG_WARN("try rle encoder failed", K(ret), K(column_idx));
      } else if (NULL != e) {
        if (e->calc_size() < choose->calc_size()) {
          free_encoder(choose);
          choose = e;
        } else {
          free_encoder(e);
          e = NULL;
        }
      }

      if (OB_FAIL(ret)) {
      } else if (cc.detected_encoders_[ObConstEncoder::type_]) {
      } else if (OB_FAIL(try_encoder<ObConstEncoder>(e, column_idx))) {
        LOG_WARN("try const encoder failed", K(ret), K(column_idx));
      } else if (NULL != e) {
        if (e->calc_size() < choose->calc_size()) {
          free_encoder(choose);
          choose = e;
        } else {
          free_encoder(e);
          e = NULL;
        }
      }
    }

    if (OB_SUCC(ret) && try_more && choose->calc_size() <= acceptable_size) {
      try_more = false;
    }

    if (OB_SUCC(ret) && try_more) {
      // if ((ObIntSC == sc || ObUIntSC == sc) && ObFloatTC != tc && ObDoubleTC != tc) {
      if ((ObIntSC == sc || ObUIntSC == sc)) {
        if (cc.detected_encoders_[ObIntegerBaseDiffEncoder::type_]) {
        } else if (OB_FAIL(try_encoder<ObIntegerBaseDiffEncoder>(e, column_idx))) {
          LOG_WARN("try integer base diff encoder failed", K(ret), K(column_idx));
        } else if (NULL != e) {
          int64_t size = e->calc_size();
          if (size < choose->calc_size()) {
            free_encoder(choose);
            choose = e;
            try_more = size <= acceptable_size;
          } else {
            free_encoder(e);
            e = NULL;
          }
        }
      }
    }

    bool string_diff_suitable = false;
    if (OB_SUCC(ret) && try_more) {
      if (is_string_encoding_valid(sc) && cc.fix_data_size_ > 0) {
        if (cc.detected_encoders_[ObStringDiffEncoder::type_]) {
        } else if (OB_FAIL(try_encoder<ObStringDiffEncoder>(e, column_idx))) {
          LOG_WARN("try string diff encoder failed", K(ret), K(column_idx));
        } else if (NULL != e) {
          int64_t size = e->calc_size();
          if (size < choose->calc_size()) {
            free_encoder(choose);
            choose = e;
            try_more = size <= acceptable_size;
            string_diff_suitable = true;
          } else {
            free_encoder(e);
            e = NULL;
          }
        }
      }
    }

    bool string_prefix_suitable = false;
    if (OB_SUCC(ret) && try_more) {
      if (is_string_encoding_valid(sc)) {
        if (cc.detected_encoders_[ObStringPrefixEncoder::type_]) {
        } else if (OB_FAIL(try_encoder<ObStringPrefixEncoder>(e, column_idx))) {
          LOG_WARN("try string prefix encoder failed", K(ret), K(column_idx));
        } else if (NULL != e) {
          int64_t size = e->calc_size();
          if (size < choose->calc_size()) {
            free_encoder(choose);
            choose = e;
            try_more = size <= acceptable_size;
            string_prefix_suitable = true;
          } else {
            free_encoder(e);
            e = NULL;
          }
        }
      }
    }

    if (OB_SUCC(ret) && try_more && !string_diff_suitable && !string_prefix_suitable) {
      if (is_string_encoding_valid(sc)) {
        if (cc.detected_encoders_[ObHexStringEncoder::type_]) {
        } else if (OB_FAIL(try_encoder<ObHexStringEncoder>(e, column_idx))) {
          LOG_WARN("try hex string encoder failed", K(ret), K(column_idx));
        } else if (NULL != e) {
          int64_t size = e->calc_size();
          if (size < choose->calc_size()) {
            free_encoder(choose);
            choose = e;
            try_more = size <= acceptable_size;
          } else {
            free_encoder(e);
            e = NULL;
          }
        }
      }
    }

    if (OB_SUCC(ret)) {
      LOG_DEBUG("used encoder", K(column_idx),
          "column_header", choose->get_column_header(),
          "data_desc", choose->get_desc());
      if (ObColumnHeader::is_inter_column_encoder(choose->get_type())) {
        const int64_t ref_col_idx = static_cast<ObSpanColumnEncoder *>(choose)->get_ref_col_idx();
        col_ctxs_.at(ref_col_idx).is_refed_ = true;
        LOG_DEBUG("column reference", K(column_idx), K(ref_col_idx));
      }
      if (OB_FAIL(encoders_.push_back(choose))) {
        LOG_WARN("push back encoder failed");
      }
    }
    if (OB_FAIL(ret)) {
      if (NULL != choose) {
        free_encoder(choose);
        choose = NULL;
      }
    }
  }
  return ret;
}

void ObMicroBlockEncoder::free_encoders()
{
  int ret = OB_SUCCESS;
  FOREACH(e, encoders_) {
    free_encoder(*e);
  }
  FOREACH(ht, hashtables_) {
    // should continue even fail
    if (OB_FAIL(hashtable_factory_.recycle(false, *ht))) {
      // overwrite ret
      LOG_WARN("recycle hashtable failed", K(ret));
    }
  }
  FOREACH(pt, multi_prefix_trees_) {
    // should continue even fail
    if (OB_FAIL(multi_prefix_tree_factory_.recycle(*pt))) {
      // overwrite ret
      LOG_WARN("recycle multi-prefix tree failed", K(ret));
    }
  }
  encoders_.reuse();
  hashtables_.reuse();
  multi_prefix_trees_.reuse();
}

int ObMicroBlockEncoder::force_raw_encoding(const int64_t column_idx)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(column_idx < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid column_idx", K(ret), K(column_idx));
  } else {
    const bool force_var_store = true;
    if (NULL != encoders_[column_idx]) {
      free_encoder(encoders_[column_idx]);
      encoders_[column_idx] = NULL;
    }

    ObIColumnEncoder *e = NULL;
    if (OB_FAIL(force_raw_encoding(column_idx, force_var_store, e))) {
      LOG_WARN("force_raw_encoding failed", K(ret), K(column_idx), K(force_var_store));
    } else if (OB_ISNULL(e)){
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("encoder is NULL", K(ret), K(column_idx));
    } else {
      encoders_[column_idx] = e;
    }
  }
  return ret;
}

int ObMicroBlockEncoder::force_raw_encoding(const int64_t column_idx,
    const bool force_var_store, ObIColumnEncoder *&encoder)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(column_idx < 0 || column_idx >= ctx_.column_cnt_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid column_idx", K(ret), K(column_idx));
  } else {
    bool suitable = false;
    ObRawEncoder *e = alloc_encoder<ObRawEncoder>();
    if (NULL == e) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc encoder failed", K(ret));
    } else if (OB_FAIL(e->init(col_ctxs_.at(column_idx), column_idx, datum_rows_))) {
      LOG_WARN("init encoder failed", K(ret), K(column_idx));
    } else if (OB_FAIL(e->traverse(force_var_store, suitable))) {
      LOG_WARN("traverse encoder failed", K(ret), K(force_var_store));
    }

    if (OB_SUCC(ret)) {
      if (!suitable) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("find raw encoder not suitable", K(ret));
      } else {
        e->set_extend_value_bit(get_header(data_buffer_)->extend_value_bit_);
      }
    }

    if (NULL != e && OB_FAIL(ret)) {
      free_encoder(e);
      e = NULL;
    } else {
      encoder = e;
    }
  }
  return ret;
}

} // end namespace blocksstable
} // end namespace oceanbase
