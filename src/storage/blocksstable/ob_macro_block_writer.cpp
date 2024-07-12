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

#include "common/row/ob_row.h"
#include "common/ob_store_format.h"
#include "lib/compress/ob_compressor_pool.h"
#include "lib/utility/ob_tracepoint.h"
#include "share/config/ob_server_config.h"
#include "share/ob_force_print_log.h"
#include "share/ob_task_define.h"
#include "share/schema/ob_table_schema.h"
#include "storage/blocksstable/index_block/ob_index_block_builder.h"
#include "storage/blocksstable/index_block/ob_index_block_dual_meta_iterator.h"
#include "storage/blocksstable/index_block/ob_index_block_row_struct.h"
#include "storage/blocksstable/ob_macro_block_writer.h"
#include "storage/blocksstable/cs_encoding/ob_micro_block_cs_encoder.h"
#include "storage/ddl/ob_ddl_redo_log_writer.h"
#include "storage/ob_i_store.h"
#include "storage/ob_sstable_struct.h"
#include "storage/blocksstable/ob_logic_macro_id.h"
#include "storage/blocksstable/cs_encoding/ob_cs_encoding_util.h"

namespace oceanbase
{
using namespace common;
using namespace storage;
using namespace share::schema;
using namespace share;
namespace blocksstable
{

ObMicroBlockBufferHelper::ObMicroBlockBufferHelper()
  : data_store_desc_(nullptr),
    micro_block_merge_verify_level_(0),
    compressor_(),
    encryption_(),
    check_reader_helper_(),
    checksum_helper_(),
    check_datum_row_(),
    allocator_("BlockBufHelper")
{
}

int ObMicroBlockBufferHelper::open(
    const ObDataStoreDesc &data_store_desc,
    common::ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  reset();
  if (OB_UNLIKELY(!data_store_desc.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid input argument.", K(ret), K(data_store_desc));
  } else if (ObStoreFormat::is_row_store_type_with_cs_encoding(data_store_desc.get_row_store_type())) {
    if (OB_FAIL(compressor_.init(data_store_desc.get_micro_block_size(), ObCompressorType::NONE_COMPRESSOR))) {
      STORAGE_LOG(WARN, "Fail to init micro block compressor, ", K(ret), K(data_store_desc));
    }
  } else if (OB_FAIL(compressor_.init(data_store_desc.get_micro_block_size(), data_store_desc.get_compressor_type()))) {
    STORAGE_LOG(WARN, "Fail to init micro block compressor, ", K(ret), K(data_store_desc));
  }

  if (OB_FAIL(ret)) {
#ifdef OB_BUILD_TDE_SECURITY
  } else if (OB_FAIL(encryption_.init(
      data_store_desc.get_encrypt_id(),
      MTL_ID(),
      data_store_desc.get_master_key_id(),
      data_store_desc.get_encrypt_key(),
      OB_MAX_TABLESPACE_ENCRYPT_KEY_LENGTH))) {
    STORAGE_LOG(WARN, "fail to init micro block encryption", K(ret), K(data_store_desc));
#endif
  } else if (OB_FAIL(check_datum_row_.init(allocator, data_store_desc.get_row_column_count()))) {
    STORAGE_LOG(WARN, "Failed to init datum row", K(ret), K(data_store_desc.get_row_column_count()));
  } else if (OB_FAIL(check_reader_helper_.init(allocator))) {
    STORAGE_LOG(WARN, "Failed to init reader helper", K(ret));
  } else if (OB_FAIL(checksum_helper_.init( // ATTENTION!!!: it may be reused
      &data_store_desc.get_col_desc_array(), data_store_desc.contain_full_col_descs()))) {
    STORAGE_LOG(WARN, "Failed to init checksum helper", K(ret));
  } else {
    data_store_desc_ = &data_store_desc;
    micro_block_merge_verify_level_ = GCONF.micro_block_merge_verify_level;
  }
  return ret;
}

void ObMicroBlockBufferHelper::reset()
{
  data_store_desc_ = nullptr;
  micro_block_merge_verify_level_ = 0;
  compressor_.reset();
#ifdef OB_BUILD_TDE_SECURITY
  encryption_.reset();
#endif
  check_reader_helper_.reset();
  check_datum_row_.reset();
}

int ObMicroBlockBufferHelper::compress_encrypt_micro_block(ObMicroBlockDesc &micro_block_desc,
                                                           const int64_t seq,
                                                           const int64_t offset)
{
  int ret = OB_SUCCESS;
  const char *block_buffer = micro_block_desc.buf_;
  int64_t block_size = micro_block_desc.buf_size_;
  const char *compress_buf = NULL;
  int64_t compress_buf_size = 0;
  if (OB_UNLIKELY(!micro_block_desc.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid micro block desc", K(ret), K(micro_block_desc));
  } else if (OB_FAIL(compressor_.compress(block_buffer, block_size, compress_buf, compress_buf_size))) {
    STORAGE_LOG(WARN, "macro block writer fail to compress.",
        K(ret), K(OB_P(block_buffer)), K(block_size));
  } else if (MICRO_BLOCK_MERGE_VERIFY_LEVEL::NONE != micro_block_merge_verify_level_
      && OB_FAIL(check_micro_block(compress_buf, compress_buf_size,
            block_buffer, block_size, micro_block_desc))) {
    STORAGE_LOG(WARN, "failed to check micro block", K(ret));
#ifndef OB_BUILD_TDE_SECURITY
  } else {
    ObMicroBlockHeader *header = const_cast<ObMicroBlockHeader *>(micro_block_desc.header_);
    micro_block_desc.buf_ = compress_buf;
    micro_block_desc.buf_size_ = compress_buf_size;
#else
  } else if (OB_FAIL(encryption_.generate_iv(seq, offset))) {
    STORAGE_LOG(WARN, "failed to generate iv", K(ret));
  } else if (OB_FAIL(encryption_.encrypt(compress_buf, compress_buf_size, micro_block_desc.buf_, micro_block_desc.buf_size_))) {
    STORAGE_LOG(WARN, "fail to encrypt micro block", K(ret));
  } else {
    // fill header after compress/encrypt
    ObMicroBlockHeader *header = const_cast<ObMicroBlockHeader *>(micro_block_desc.header_);
#endif
    header->data_length_ = block_size;
    header->data_zlength_ = micro_block_desc.buf_size_;
    header->data_checksum_ = ob_crc64_sse42(0, micro_block_desc.buf_, micro_block_desc.buf_size_);
    header->original_length_ = micro_block_desc.original_size_;
    header->set_header_checksum();
  }
  return ret;
}

int ObMicroBlockBufferHelper::check_micro_block(
    const char *compressed_buf,
    const int64_t compressed_size,
    const char *uncompressed_buf,
    const int64_t uncompressed_size,
    const ObMicroBlockDesc &micro_desc)
{
  int ret = OB_SUCCESS;
  const char *decomp_buf = nullptr;
  int64_t real_decomp_size = 0;
  if (MICRO_BLOCK_MERGE_VERIFY_LEVEL::ENCODING == micro_block_merge_verify_level_) {
    decomp_buf = const_cast<char *>(uncompressed_buf);
  } else if (OB_FAIL(compressor_.decompress(compressed_buf, compressed_size, uncompressed_size,
          decomp_buf, real_decomp_size))) {
    STORAGE_LOG(WARN, "failed to decompress data", K(ret));
  } else if (uncompressed_size != real_decomp_size) {
    ret = OB_CHECKSUM_ERROR;
    LOG_DBA_ERROR(OB_CHECKSUM_ERROR, "msg", "decompressed size is not equal to original size", K(ret),
        K(uncompressed_size), K(real_decomp_size));
  }
  if (OB_SUCC(ret)) {
    const int64_t buf_size = micro_desc.header_->header_size_ + uncompressed_size;
    int64_t pos = 0;
    char *block_buf = nullptr;
    allocator_.reuse();
    if (OB_ISNULL(block_buf = static_cast<char *>(allocator_.alloc(buf_size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "failed to alloc mem", K(ret), K(buf_size), K(micro_desc));
    } else if (OB_FAIL(micro_desc.header_->serialize(block_buf, buf_size, pos))) {
      STORAGE_LOG(WARN, "failed to serialize header", K(ret), K(micro_desc));
    } else {
      // extra copy when decomp wrongly
      MEMCPY(block_buf + pos, decomp_buf, uncompressed_size);
      if (OB_FAIL(check_micro_block_checksum(block_buf, buf_size, micro_desc.block_checksum_))) {
        STORAGE_LOG(WARN, "failed to check_micro_block_checksum", K(ret), K(micro_desc));
      }
    }
  }
  return ret;
}

int ObMicroBlockBufferHelper::check_micro_block_checksum(
    const char *buf,
    const int64_t size,
    const int64_t checksum)
{
  int ret = OB_SUCCESS;
  ObIMicroBlockReader *micro_reader = NULL;
  if (OB_FAIL(prepare_micro_block_reader(buf, size, micro_reader))) {
    STORAGE_LOG(WARN, "failed to preapre micro block reader", K(ret), K(buf), K(size));
  } else if (OB_ISNULL(micro_reader)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "micro block reader is null", K(ret), K(buf), K(size), K(micro_reader));
  } else {
    checksum_helper_.reuse();
    for (int64_t it = 0; OB_SUCC(ret) && it != micro_reader->row_count(); ++it) {
      check_datum_row_.reuse();
      if (OB_FAIL(micro_reader->get_row(it, check_datum_row_))) {
        STORAGE_LOG(WARN, "get_row failed", K(ret), K(it), K(*data_store_desc_));
      } else if (OB_FAIL(checksum_helper_.cal_row_checksum(check_datum_row_.storage_datums_,
          check_datum_row_.get_column_count()))) {
        STORAGE_LOG(WARN, "fail to cal row checksum", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      const int64_t new_checksum = checksum_helper_.get_row_checksum();
      if (checksum != new_checksum) {
        ret = OB_CHECKSUM_ERROR; // ignore print error code
        LOG_DBA_ERROR(OB_CHECKSUM_ERROR, "msg", "micro block checksum is not equal", K(new_checksum),
            K(checksum), K(ret), KPC(data_store_desc_), K(checksum_helper_));
      }
#ifdef ERRSIM
  if (data_store_desc_->encoding_enabled()) {
    ret = OB_E(EventTable::EN_BUILD_DATA_MICRO_BLOCK) ret;
  }
#endif
      if (OB_UNLIKELY(OB_CHECKSUM_ERROR == ret)) {
        print_micro_block_row(micro_reader);
      }
    }
  }
  return ret;
}

int ObMicroBlockBufferHelper::prepare_micro_block_reader(
    const char *buf,
    const int64_t size,
    ObIMicroBlockReader *&micro_reader)
{
  int ret = OB_SUCCESS;
  micro_reader = nullptr;
  ObMicroBlockData block_data;
  block_data.buf_ = buf;
  block_data.size_ = size;
  const ObMicroBlockHeader *header = reinterpret_cast<const ObMicroBlockHeader *>(buf);
  ObRowStoreType row_store_type = static_cast<ObRowStoreType>(header->row_store_type_);
  if (OB_FAIL(check_reader_helper_.get_reader(row_store_type, micro_reader))) {
    STORAGE_LOG(WARN, "failed to get micro reader", K(ret), K(row_store_type));
  } else if (OB_ISNULL(micro_reader)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "unexpected null micro reader", K(ret), KP(micro_reader));
  } else if (OB_FAIL(micro_reader->init(block_data, nullptr))) {
    STORAGE_LOG(WARN, "failed to init micro block reader",
        K(ret), K(block_data), KPC(header));
  }
  return ret;
}

void ObMicroBlockBufferHelper::print_micro_block_row(ObIMicroBlockReader *micro_reader)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(micro_reader)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "get_row failed", K(ret), K(micro_reader));
  } else {
    checksum_helper_.reuse();
    for (int64_t it = 0; OB_SUCC(ret) && it != micro_reader->row_count(); ++it) {
      check_datum_row_.reuse();
      if (OB_FAIL(micro_reader->get_row(it, check_datum_row_))) {
        STORAGE_LOG(WARN, "get_row failed", K(ret), K(it), K(*data_store_desc_));
      } else if (OB_FAIL(checksum_helper_.cal_row_checksum(check_datum_row_.storage_datums_,
          check_datum_row_.get_column_count()))) {
        STORAGE_LOG(WARN, "fail to cal row checksum", K(ret));
      } else {
        FLOG_WARN("error micro block row", K(it), K_(check_datum_row), "new_checksum", checksum_helper_.get_row_checksum(), K_(checksum_helper));
      }
    }
  }
  return;
}

int ObMicroBlockBufferHelper::dump_micro_block_writer_buffer(const char *buf, const int64_t size)
{
  int ret = OB_SUCCESS;
  ObIMicroBlockReader *micro_reader = NULL;
  if (OB_FAIL(prepare_micro_block_reader(buf, size, micro_reader))) {
    STORAGE_LOG(WARN, "failed to preapre micro block reader", K(ret), K(micro_reader));
  } else {
    print_micro_block_row(micro_reader);
  }
  return ret;
}

/**
 * ---------------------------------------------------------ObMicroBlockAdaptiveSplitter--------------------------------------------------------------
 */
ObMicroBlockAdaptiveSplitter::ObMicroBlockAdaptiveSplitter()
  : macro_store_size_(0),
    min_micro_row_count_(0),
    is_use_adaptive_(false)
{}

ObMicroBlockAdaptiveSplitter::~ObMicroBlockAdaptiveSplitter()
{
}

int ObMicroBlockAdaptiveSplitter::init(const int64_t macro_store_size, const int64_t min_micro_row_count, const bool is_use_adaptive)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(macro_store_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid micro block adaptive split input argument", K(ret), K(macro_store_size));
  } else {
    reset();
    macro_store_size_ = macro_store_size;
    is_use_adaptive_ = is_use_adaptive;
    min_micro_row_count_ = MAX(min_micro_row_count, MICRO_ROW_MIN_COUNT);
  }

  return ret;
}

void ObMicroBlockAdaptiveSplitter::reset()
{
  min_micro_row_count_ = 0;
  for (int64_t i = 0; i <= DEFAULT_MICRO_ROW_COUNT; i++) {
    compression_infos_[i].reset();
  }
}

int ObMicroBlockAdaptiveSplitter::check_need_split(const int64_t micro_size,
                                                   const int64_t micro_row_count,
                                                   const int64_t split_size,
                                                   const int64_t current_macro_size,
                                                   const bool is_keep_space,
                                                   bool &is_split) const
{
  int ret = OB_SUCCESS;
  is_split = false;
  if(OB_UNLIKELY(micro_size <= 0 || split_size <= 0 || micro_row_count <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid size argument", K(ret), K(micro_size), K(split_size), K(micro_row_count));
  } else if (micro_size < split_size) {
    is_split = false;
  } else if (!is_use_adaptive_ || micro_size >= ObIMicroBlockWriter::DEFAULT_MICRO_MAX_SIZE) {
    is_split = true;
  } else {
    const int64_t adaptive_row_count = MAX(min_micro_row_count_, DEFAULT_MICRO_ROW_COUNT - (micro_size - split_size) / split_size);
    const int64_t compression_ratio = micro_row_count <= DEFAULT_MICRO_ROW_COUNT ?
      compression_infos_[micro_row_count].compression_ratio_ : compression_infos_[0].compression_ratio_;
    const int64_t estimate_micro_size = micro_size * compression_ratio / 100;
    if (estimate_micro_size < split_size) {
      is_split = false;
    } else if (micro_row_count >= adaptive_row_count ||
      (is_keep_space && current_macro_size + estimate_micro_size > macro_store_size_ /* for pct_free */)) {
      is_split = true;
    }
  }

  return ret;
}

int ObMicroBlockAdaptiveSplitter::update_compression_info(const int64_t micro_row_count,
                                                          const int64_t original_size,
                                                          const int64_t compressed_size)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(micro_row_count <= 0 || original_size < 0 || compressed_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid compression info argument", K(micro_row_count), K(original_size), K(compressed_size));
  } else {
    compression_infos_[0].update(original_size, compressed_size);
    if (micro_row_count <= DEFAULT_MICRO_ROW_COUNT) {
      compression_infos_[micro_row_count].update(original_size, compressed_size);
    }
  }
  return ret;
}

ObMicroBlockAdaptiveSplitter::ObMicroCompressionInfo::ObMicroCompressionInfo()
  : original_size_(0),
    compressed_size_(0),
    compression_ratio_(100)
{}

void ObMicroBlockAdaptiveSplitter::ObMicroCompressionInfo::update(const int64_t original_size, const int64_t compressed_size)
{
    original_size_ += original_size;
    compressed_size_ += compressed_size;
    if (OB_UNLIKELY(original_size_ <= 0)) {
      compression_ratio_ = 100;
    } else {
      compression_ratio_ = 100 * compressed_size_ / original_size_;
    }
}

/**
 * ---------------------------------------------------------ObMacroBlockWriter--------------------------------------------------------------
 */
ObMacroBlockWriter::ObMacroBlockWriter(const bool is_need_macro_buffer)
  : data_store_desc_(nullptr),
    merge_info_(nullptr),
    micro_writer_(nullptr),
    reader_helper_(),
    hash_index_builder_(),
    micro_helper_(),
    current_index_(0),
    current_macro_seq_(0),
    last_micro_size_(INT64_MAX),
    last_micro_expand_pct_(100),
    block_write_ctx_(),
    last_key_(),
    last_key_with_L_flag_(false),
    is_macro_or_micro_block_reused_(false),
    is_need_macro_buffer_(is_need_macro_buffer),
    is_try_full_fill_macro_block_(true),
    curr_micro_column_checksum_(NULL),
    allocator_("MaBlkWriter"),
    rowkey_allocator_("MaBlkWriter"),
    macro_reader_(),
    micro_rowkey_hashs_(),
    lock_(common::ObLatchIds::MACRO_WRITER_LOCK),
    datum_row_(),
    aggregated_row_(nullptr),
    data_aggregator_(nullptr),
    callback_(nullptr),
    builder_(NULL),
    data_block_pre_warmer_(),
    io_buf_(nullptr)
{
}

ObMacroBlockWriter::~ObMacroBlockWriter()
{
  reset();
}

void ObMacroBlockWriter::reset()
{
  data_store_desc_ = nullptr;
  if (OB_NOT_NULL(micro_writer_)) {
    micro_writer_->~ObIMicroBlockWriter();
    allocator_.free(micro_writer_);
    micro_writer_ = nullptr;
  }
  reader_helper_.reset();
  hash_index_builder_.reset();
  micro_helper_.reset();
  macro_blocks_[0].reset();
  macro_blocks_[1].reset();
  bf_cache_writer_[0].reset();
  bf_cache_writer_[1].reset();
  current_index_ = 0;
  current_macro_seq_ = 0;
  last_micro_size_ = INT64_MAX;
  last_micro_expand_pct_ = 100;
  block_write_ctx_.reset();
  macro_handles_[0].reset();
  macro_handles_[1].reset();
  last_key_.reset();
  last_key_with_L_flag_ = false;
  is_macro_or_micro_block_reused_ = false;
  micro_rowkey_hashs_.reset();
  datum_row_.reset();
  if (OB_NOT_NULL(builder_)) {
    builder_->~ObDataIndexBlockBuilder();
    builder_ = nullptr;
  }
  micro_block_adaptive_splitter_.reset();
  release_pre_agg_util();
  allocator_.reset();
  rowkey_allocator_.reset();
  io_buf_ = nullptr;
  data_block_pre_warmer_.reset();
}


int ObMacroBlockWriter::open(
    const ObDataStoreDesc &data_store_desc,
    const ObMacroDataSeq &start_seq,
    ObIMacroBlockFlushCallback *callback)
{
  int ret = OB_SUCCESS;
  reset();
  if (OB_UNLIKELY(!data_store_desc.is_valid() || !start_seq.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid macro block writer input argument.", K(ret), K(data_store_desc), K(start_seq));
  } else {
    STORAGE_LOG(DEBUG, "open macro block writer: ", K(data_store_desc), K(start_seq));
    ObSSTableIndexBuilder *sstable_index_builder = data_store_desc.sstable_index_builder_;
    callback_ = callback;
    data_store_desc_ = &data_store_desc;
    merge_info_ = data_store_desc_->merge_info_;
    const int64_t task_idx = start_seq.get_parallel_idx();
    current_macro_seq_ = start_seq.get_data_seq();
    if (data_store_desc.is_cg()) {
      last_key_.set_min_rowkey(); // used to protect cg sstable
    }

    if (OB_FAIL(init_hash_index_builder(start_seq))) {
      STORAGE_LOG(WARN, "Failed to build hash_index builder", K(ret));
    } else if (OB_FAIL(init_data_pre_warmer(start_seq))) {
      STORAGE_LOG(WARN, "Failed to build data pre warmer", K(ret));
    } else if (OB_FAIL(build_micro_writer(data_store_desc_,
                                          allocator_,
                                          micro_writer_,
                                          GCONF.micro_block_merge_verify_level))) {
      STORAGE_LOG(WARN, "fail to build micro writer", K(ret));
    } else if (OB_FAIL(datum_row_.init(allocator_, data_store_desc.get_row_column_count()))) {
      STORAGE_LOG(WARN, "Failed to init datum row", K(ret), K(data_store_desc.get_row_column_count()));
    } else if (OB_FAIL(micro_helper_.open(data_store_desc, allocator_))) {
      STORAGE_LOG(WARN, "Failed to open micro helper", K(ret), K(data_store_desc));
    } else if (OB_FAIL(reader_helper_.init(allocator_))) {
      STORAGE_LOG(WARN, "Failed to init reader helper", K(ret));
    } else if (OB_FAIL(init_pre_agg_util(data_store_desc))) {
      STORAGE_LOG(WARN, "Failed to init pre aggregate utilities", K(ret));
    } else {
      const bool is_use_adaptive = !data_store_desc_->is_major_merge_type()
       || data_store_desc_->get_major_working_cluster_version() >= DATA_VERSION_4_1_0_0;
      if (OB_FAIL(micro_block_adaptive_splitter_.init(data_store_desc.get_macro_store_size(), 0/*min_micro_row_count*/, is_use_adaptive))) {
        STORAGE_LOG(WARN, "Failed to init micro block adaptive split", K(ret),
          "macro_store_size", data_store_desc.get_macro_store_size());
      } else if (data_store_desc_->get_major_working_cluster_version() < DATA_VERSION_4_2_0_0) {
        if (data_store_desc_->is_major_merge_type()) {
          is_need_macro_buffer_ = true;
        }
        is_try_full_fill_macro_block_ = false;
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(macro_blocks_[0].init(data_store_desc, start_seq.get_data_seq()))) {
      STORAGE_LOG(WARN, "Fail to init 0th macro block, ", K(ret));
    } else if (is_need_macro_buffer_ && OB_FAIL(macro_blocks_[1].init(data_store_desc, start_seq.get_data_seq() + 1))) {
      STORAGE_LOG(WARN, "Fail to init 1th macro block, ", K(ret));
    } else if (data_store_desc_->is_major_merge_type()) {
      if (OB_ISNULL(curr_micro_column_checksum_ = static_cast<int64_t *>(
          allocator_.alloc(sizeof(int64_t) * data_store_desc_->get_row_column_count())))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        STORAGE_LOG(WARN, "fail to allocate memory for curr micro block column checksum", K(ret));
      } else {
       MEMSET(curr_micro_column_checksum_, 0,
           sizeof(int64_t) * data_store_desc_->get_row_column_count());
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_NOT_NULL(sstable_index_builder)) {
      if (OB_FAIL(sstable_index_builder->new_index_builder(builder_, data_store_desc, allocator_))) {
        STORAGE_LOG(WARN, "fail to alloc index builder", K(ret));
      } else if (OB_ISNULL(builder_)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "unexpected null builder", K(ret), KPC(sstable_index_builder));
      } else if (OB_FAIL(builder_->set_parallel_task_idx(task_idx))) {
        STORAGE_LOG(WARN, "fail to set_parallel_task_idx", K(ret), K(task_idx));
      }
    } else {
      builder_ = nullptr;
    }
  }
  return ret;
}

int ObMacroBlockWriter::append_row(const ObDatumRow &row, const ObMacroBlockDesc *curr_macro_desc)
{
  int ret = OB_SUCCESS;

  UNUSED(curr_macro_desc);
  STORAGE_LOG(DEBUG, "append row", K(row));

  if (OB_UNLIKELY(nullptr == data_store_desc_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "The ObMacroBlockWriter has not been opened", K(ret));
  } else if (OB_FAIL(append_row(row, data_store_desc_->get_micro_block_size()))) {
    STORAGE_LOG(WARN, "Fail to append row", K(ret));
  } else if (OB_FAIL(try_active_flush_macro_block())) {
    STORAGE_LOG(WARN, "Fail to try_active_flush_macro_block", K(ret));
  } else if (nullptr != merge_info_) {
    ++merge_info_->incremental_row_count_;
    STORAGE_LOG(DEBUG, "Success to append row, ", "tablet_id", data_store_desc_->get_tablet_id(), K(row));
  }
  return ret;
}

int ObMacroBlockWriter::append(const ObDataMacroBlockMeta &macro_meta)
{
  int ret = OB_SUCCESS;

  if (micro_writer_->get_row_count() > 0 && OB_FAIL(build_micro_block())) {
    LOG_WARN("Fail to build current micro block", K(ret));
  } else if (OB_FAIL(try_switch_macro_block())) {
    LOG_WARN("Fail to flush and switch macro block", K(ret));
  } else if (OB_UNLIKELY(!macro_meta.is_valid()) || OB_ISNULL(builder_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid arguments", K(ret), KP(builder_), K(macro_meta));
  } else if (OB_FAIL(builder_->append_macro_block(macro_meta))) {
    LOG_WARN("Fail to append index block rows", K(ret), KP(builder_), K(macro_meta));
  } else if (OB_FAIL(flush_reuse_macro_block(macro_meta))) {
      LOG_WARN("Fail to flush reuse macro block", K(ret), K(macro_meta));
  } else {
    is_macro_or_micro_block_reused_ = true;
    last_key_with_L_flag_ = false; // clear flag
    if (nullptr != merge_info_) {
      merge_info_->multiplexed_macro_block_count_++;
      merge_info_->macro_block_count_++;
      merge_info_->total_row_count_ += macro_meta.val_.row_count_;
      merge_info_->occupy_size_
          += macro_meta.val_.occupy_size_;
    }
  }

  if (OB_SUCC(ret) && !data_store_desc_->is_cg()) {
    ObDatumRowkey rowkey;
    if (OB_FAIL(macro_meta.get_rowkey(rowkey))) {
      LOG_WARN("Fail to assign rowkey", K(ret), K(macro_meta));
    } else if (rowkey.get_datum_cnt() != data_store_desc_->get_rowkey_column_count()) {
      ret = OB_ERR_SYS;
      LOG_ERROR("Rowkey column not match, can not reuse macro block", K(ret),
          "reused merge info rowkey count", rowkey.get_datum_cnt(),
          "data store descriptor rowkey count", data_store_desc_->get_rowkey_column_count());
    } else if (OB_FAIL(save_last_key(rowkey))) {
      LOG_WARN("Fail to copy last key", K(ret), K(rowkey));
    }
  }
  return ret;
}

int ObMacroBlockWriter::append(const ObDatumRow &row)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(append_row_and_hash_index(row))) {
    if (OB_BUF_NOT_ENOUGH == ret) {
      if (0 == micro_writer_->get_row_count()) {
        ret = OB_NOT_SUPPORTED;
        STORAGE_LOG(ERROR, "The single row is too large, ", K(ret), K(row));
      } else if (OB_FAIL(build_micro_block())) {
        STORAGE_LOG(WARN, "Fail to build micro block, ", K(ret));
      } else if (OB_FAIL(append_row_and_hash_index(row))) {
        STORAGE_LOG(ERROR, "Fail to append row to micro block, ", K(ret), K(row));
      }
    }
  }

  return ret;
}

int ObMacroBlockWriter::append_row(const ObDatumRow &row, const int64_t split_size)
{
  int ret = OB_SUCCESS;
  const ObDatumRow *row_to_append = &row;
  if (OB_ISNULL(data_store_desc_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "The ObMacroBlockWriter has not been opened, ", K(ret), KP(data_store_desc_));
  } else if (OB_UNLIKELY(split_size < data_store_desc_->get_micro_block_size())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid split_size", K(ret), K(split_size));
  } else if (!data_store_desc_->is_cg() && OB_FAIL(check_order(row))) {
    STORAGE_LOG(WARN, "macro block writer fail to check order.", K(row));
  }
  if (OB_SUCC(ret)) {
    is_macro_or_micro_block_reused_ = false;
    bool is_split = false;
    if (OB_FAIL(append(*row_to_append))) {
      STORAGE_LOG(WARN, "Fail to append row to micro block", K(ret), K(row));
    } else if (OB_FAIL(update_micro_commit_info(*row_to_append))) {
      STORAGE_LOG(WARN, "Fail to update_micro_commit_info", K(ret), K(row));
    } else if (OB_FAIL(save_last_key(*row_to_append))) {
      STORAGE_LOG(WARN, "Fail to save last key, ", K(ret), K(row));
    } else if (nullptr != data_aggregator_ && OB_FAIL(data_aggregator_->eval(*row_to_append))) {
      STORAGE_LOG(WARN, "Fail to evaluate aggregate data", K(ret));
    } else if (OB_FAIL(micro_block_adaptive_splitter_.check_need_split(micro_writer_->get_block_size(), micro_writer_->get_row_count(),
          split_size, macro_blocks_[current_index_].get_data_size(), is_keep_freespace(), is_split))) {
      STORAGE_LOG(WARN, "Failed to check need split", K(ret), KPC(micro_writer_));
    } else if (is_split && OB_FAIL(build_micro_block())) {
      STORAGE_LOG(WARN, "Fail to build micro block, ", K(ret));
    }
  }
  return ret;
}

int ObMacroBlockWriter::append_macro_block(const ObMacroBlockDesc &macro_desc)
{
  int ret = OB_SUCCESS;
  const ObDataMacroBlockMeta *data_block_meta = macro_desc.macro_meta_;

  if (OB_UNLIKELY(nullptr == data_store_desc_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "The ObMacroBlockWriter has not been opened", K(ret));
  } else if (OB_UNLIKELY(nullptr == data_block_meta)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "unexpected null meta", K(ret));
  } else if (OB_FAIL(append(*data_block_meta))) {
    STORAGE_LOG(WARN, "failed to append macro block", K(ret), K(macro_desc));
  }
  return ret;
}

int ObMacroBlockWriter::append_micro_block(const ObMicroBlock &micro_block, const ObMacroBlockDesc *curr_macro_desc)
{
  int ret = OB_SUCCESS;

  UNUSED(curr_macro_desc);
  bool need_merge = false;
  STORAGE_LOG(DEBUG, "append micro_block", K(micro_block));
  if (NULL == data_store_desc_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "The ObMacroBlockWriter has not been opened", K(ret));
  } else if (!micro_block.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid micro_block", K(ret));
  } else if (OB_FAIL(check_micro_block_need_merge(micro_block, need_merge))) {
    STORAGE_LOG(WARN, "check_micro_block_need_merge failed", K(ret), K(micro_block));
  } else if (!need_merge) {
    if (micro_writer_->get_row_count() > 0) {
      if (OB_FAIL(build_micro_block())) {
        STORAGE_LOG(WARN, "build_micro_block failed", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      ObMicroBlockDesc micro_block_desc;
      ObMicroBlockHeader header_for_rewrite;
      if (OB_FAIL(build_micro_block_desc(micro_block, micro_block_desc, header_for_rewrite))) {
        STORAGE_LOG(WARN, "build_micro_block_desc failed", K(ret), K(micro_block));
      } else if (OB_FAIL(agg_micro_block(*micro_block.micro_index_info_))) {
        STORAGE_LOG(WARN, "Failed to eval aggregated data from reused micro block", K(ret));
      } else if (OB_FAIL(write_micro_block(micro_block_desc))) {
        STORAGE_LOG(WARN, "Failed to write micro block, ", K(ret), K(micro_block_desc));
      } else if (NULL != merge_info_) {
        merge_info_->multiplexed_micro_count_in_new_macro_++;
      }

      if (OB_SUCC(ret) && nullptr != data_aggregator_) {
        data_aggregator_->reuse();
      }
    }
  } else {
    if (OB_FAIL(merge_micro_block(micro_block))) {
      STORAGE_LOG(WARN, "merge_micro_block failed", K(micro_block), K(ret));
    } else {
      STORAGE_LOG(TRACE, "merge micro block", K(micro_block));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(try_active_flush_macro_block())) {
      STORAGE_LOG(WARN, "Fail to try_active_flush_macro_block", K(ret));
    } else {
      is_macro_or_micro_block_reused_ = true;
    }
  }
  return ret;
}

int ObMacroBlockWriter::append_micro_block(
    ObMicroBlockDesc &micro_block_desc,
    ObMicroIndexInfo &micro_index_info) {
  int ret = OB_SUCCESS;
  if (OB_ISNULL(data_store_desc_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "The ObMacroBlockWriter has not been opened, ", K(ret), KP(data_store_desc_));
  } else if (OB_FAIL(save_last_key(micro_block_desc.last_rowkey_))) {
    STORAGE_LOG(WARN, "fail to save last ke", K(ret), K(micro_block_desc));
  } else if (OB_FAIL(agg_micro_block(micro_index_info))) {
    STORAGE_LOG(WARN, "fail to aggregate micro block", K(ret), K(micro_index_info));
  } else if (OB_FAIL(write_micro_block(micro_block_desc))) {
    STORAGE_LOG(WARN, "fail to write micro block", K(ret), K(micro_block_desc));
  } else if (nullptr != data_aggregator_) {
    data_aggregator_->reuse();
  }
  return ret;
}

int ObMacroBlockWriter::check_data_macro_block_need_merge(const ObMacroBlockDesc &macro_desc, bool &need_merge)
{
  int ret = OB_SUCCESS;
  need_merge = false;

  if (!macro_desc.is_valid_with_macro_meta()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid macro desc", K(ret), K(macro_desc));
  } else if (macro_desc.macro_meta_->val_.data_zsize_
     < data_store_desc_->get_macro_block_size() * DEFAULT_MACRO_BLOCK_REWRTIE_THRESHOLD / 100) {
      need_merge = true;
  }

  return ret;
}

int ObMacroBlockWriter::close()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_ISNULL(data_store_desc_) || OB_ISNULL(micro_writer_))) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "exceptional situation", K(ret), K_(data_store_desc), K_(micro_writer));
  } else if (micro_writer_->get_row_count() > 0 && OB_FAIL(build_micro_block())) {
    STORAGE_LOG(WARN, "macro block writer fail to build current micro block.", K(ret));
  } else {
    ObMacroBlock &current_block = macro_blocks_[current_index_];
    ObMacroBloomFilterCacheWriter &current_bf_writer = bf_cache_writer_[current_index_];
    if (OB_SUCC(ret) && current_block.is_dirty()) {
      int32_t row_count = current_block.get_row_count();
      if (OB_FAIL(flush_macro_block(current_block))) {
        STORAGE_LOG(WARN, "macro block writer fail to flush macro block.", K(ret),
            K_(current_index));
      }
    }
    if (OB_SUCC(ret)) {
      // wait last macro block io finish
      // we also need wait prev macro block to finish due to force_split in build_micro_block
      ObMacroBlockHandle &curr_handle = macro_handles_[current_index_];
      ObMacroBlockHandle &prev_handle = macro_handles_[(current_index_ + 1) % 2];
      if (OB_NOT_NULL(callback_) && OB_FAIL(callback_->wait())) {
        STORAGE_LOG(WARN, "fail to wait callback flush", K(ret));
      } else if (OB_UNLIKELY(!is_need_macro_buffer_ && current_index_ != 0)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "unexpected current index", K(ret));
      } else if (is_need_macro_buffer_ && OB_FAIL(wait_io_finish(prev_handle))) {
        STORAGE_LOG(WARN, "Fail to wait io finish, ", K(ret));
      } else if (OB_FAIL(wait_io_finish(curr_handle))) {
        STORAGE_LOG(WARN, "Fail to wait io finish, ", K(ret));
      }
    }

    if (OB_SUCC(ret) && OB_NOT_NULL(builder_)) {
      if (OB_FAIL(builder_->close(last_key_, &block_write_ctx_))) {
        STORAGE_LOG(WARN, "fail to close data index builder", K(ret), K(last_key_));
      }
    }
  }
  return ret;
}


int ObMacroBlockWriter::check_order(const ObDatumRow &row)
{
  int ret = OB_SUCCESS;
  const int64_t trans_version_col_idx =
    ObMultiVersionRowkeyHelpper::get_trans_version_col_store_index(
        data_store_desc_->get_schema_rowkey_col_cnt(),
        data_store_desc_->get_rowkey_column_count() != data_store_desc_->get_schema_rowkey_col_cnt());
  const int64_t sql_sequence_col_idx =
    ObMultiVersionRowkeyHelpper::get_sql_sequence_col_store_index(
        data_store_desc_->get_schema_rowkey_col_cnt(),
        data_store_desc_->get_rowkey_column_count() != data_store_desc_->get_schema_rowkey_col_cnt());
  int64_t cur_row_version = 0;
  int64_t cur_sql_sequence = 0;
  if (!row.is_valid() || row.get_column_count() != data_store_desc_->get_row_column_count()) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(ERROR, "invalid macro block writer input argument.",
        K(row), "row_column_count", data_store_desc_->get_row_column_count(), K(ret));
  } else if (OB_UNLIKELY(!row.mvcc_row_flag_.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(ERROR, "invalid mvcc_row_flag", K(ret), K(row.mvcc_row_flag_));
  } else {
    ObMacroBlock &curr_block = macro_blocks_[current_index_];
    cur_row_version = row.storage_datums_[trans_version_col_idx].get_int();
    cur_sql_sequence = row.storage_datums_[sql_sequence_col_idx].get_int();
    if (cur_row_version >= 0 || cur_sql_sequence > 0) {
      bool is_ghost_row_flag = false;
      if (OB_FAIL(blocksstable::ObGhostRowUtil::is_ghost_row(row.mvcc_row_flag_, is_ghost_row_flag))) {
        STORAGE_LOG(ERROR, "failed to check ghost row", K(ret), K(row));
      } else if (!is_ghost_row_flag) {
        ret = OB_ERR_SYS;
        STORAGE_LOG(ERROR, "invalid trans_version or sql_sequence", K(ret), K(row), K(trans_version_col_idx),
                    K(sql_sequence_col_idx));
      }
    } else if (nullptr != merge_info_
               && is_major_merge_type(merge_info_->merge_type_)
               && -cur_row_version > data_store_desc_->get_snapshot_version()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("Unexpected current row trans version in major merge", K(ret), K(row),
        "snapshot_version", data_store_desc_->get_snapshot_version());
    } else if (!row.mvcc_row_flag_.is_uncommitted_row()) { // update max commit version
      const int64_t cluster_version = data_store_desc_->get_major_working_cluster_version();
      if (data_store_desc_->is_major_merge_type() && not_compat_for_queuing_mode_42x(cluster_version) && cluster_version < DATA_VERSION_4_3_0_0) {
        micro_writer_->update_max_merged_trans_version(-cur_row_version);
      }
      if (!row.mvcc_row_flag_.is_shadow_row()) {
        const_cast<ObDatumRow&>(row).storage_datums_[sql_sequence_col_idx].reuse(); // make sql sequence positive
        const_cast<ObDatumRow&>(row).storage_datums_[sql_sequence_col_idx].set_int(0); // make sql sequence positive
      } else if (OB_UNLIKELY(row.storage_datums_[sql_sequence_col_idx].get_int() != -INT64_MAX)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Unexpected shadow row", K(ret), K(row));
      }
    }
    if (OB_SUCC(ret) && last_key_.is_valid()) {
      ObDatumRowkey cur_key;
      ObDatumRowkey last_key;
      int32_t compare_result = 0;

      if (OB_FAIL(cur_key.assign(row.storage_datums_, data_store_desc_->get_schema_rowkey_col_cnt()))) {
        STORAGE_LOG(WARN, "Failed to assign cur key", K(ret));
      } else if (OB_FAIL(last_key.assign(last_key_.datums_, data_store_desc_->get_schema_rowkey_col_cnt()))) {
        STORAGE_LOG(WARN, "Failed to assign last key", K(ret));
      } else if (OB_FAIL(cur_key.compare(last_key, data_store_desc_->get_datum_utils(), compare_result))) {
        STORAGE_LOG(WARN, "Failed to compare last key", K(ret), K(cur_key), K(last_key));
      } else if (OB_UNLIKELY(compare_result < 0)) {
        ret = OB_ROWKEY_ORDER_ERROR;
        STORAGE_LOG(ERROR, "input rowkey is less then last rowkey.", K(cur_key), K(last_key), K(ret));
      } else if (OB_UNLIKELY(0 == compare_result)) { // same schema rowkey
        if (last_key_with_L_flag_) {
          ret = OB_ROWKEY_ORDER_ERROR;
          STORAGE_LOG(ERROR, "have output row with L flag before", K(ret), K(last_key_), K(row));
        } else {
          //dump data, check version
          int64_t last_row_version = last_key_.datums_[trans_version_col_idx].get_int();
          if (cur_row_version < last_row_version) {
            ret = OB_ROWKEY_ORDER_ERROR;
            STORAGE_LOG(ERROR, "cur row version is less than last row version, ", K(ret), K(cur_row_version), K(last_row_version));
          } else if (cur_row_version == last_row_version) {
            int64_t last_row_sql_seq = last_key_.datums_[sql_sequence_col_idx].get_int();
            if (cur_sql_sequence == last_row_sql_seq) {
              ret = OB_ERR_PRIMARY_KEY_DUPLICATE;
              if (data_store_desc_->get_is_ddl()) {
                STORAGE_LOG(WARN, "input rowkey is equal with last rowkey in DDL", K(ret), K(cur_sql_sequence), K_(last_key), K(row));
              } else {
                STORAGE_LOG(ERROR, "cur row sql_sequence is equal with last row", K(ret), K(cur_sql_sequence), K_(last_key), K(row));
              }
            } else if (cur_sql_sequence < last_row_sql_seq) {
              ret = OB_ROWKEY_ORDER_ERROR;
              STORAGE_LOG(ERROR, "cur row sql_sequence is less than last row", K(ret), K(cur_sql_sequence), K_(last_key), K(row));
            }
          }
        }
      } else { // another schema rowkey
        if (nullptr != merge_info_
            && !is_major_or_meta_merge_type(merge_info_->merge_type_)
            && !is_macro_or_micro_block_reused_
            && !last_key_with_L_flag_) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(ERROR, "meet another rowkey but not meet Last flag", K(ret), K(last_key_), K(row), K(last_key_with_L_flag_));
        }
      }
    }

    if (OB_ROWKEY_ORDER_ERROR == ret || OB_ERR_UNEXPECTED == ret || OB_ERR_PRIMARY_KEY_DUPLICATE == ret) {
      dump_micro_block(*micro_writer_); // print micro block have output
    }
  }
  return ret;
}

int ObMacroBlockWriter::update_micro_commit_info(const ObDatumRow &row)
{
  int ret = OB_SUCCESS;
  bool is_ghost_row_flag = false;
  if (OB_FAIL(blocksstable::ObGhostRowUtil::is_ghost_row(row.mvcc_row_flag_, is_ghost_row_flag))) {
    STORAGE_LOG(ERROR, "failed to check ghost row", K(ret), K(row));
  } else if (data_store_desc_->is_cg() || is_ghost_row_flag) { //skip cg block & ghost row
  } else if (row.mvcc_row_flag_.is_uncommitted_row()) {
    micro_writer_->set_contain_uncommitted_row();
    LOG_TRACE("meet uncommited trans row", K(row));
  } else {
    const int64_t trans_version_col_idx = data_store_desc_->get_schema_rowkey_col_cnt();
    const int64_t cur_row_version = row.storage_datums_[trans_version_col_idx].get_int();
    // data_store_desc_->get_major_working_cluster_version() only set in major merge. it is 0 for mini/minor
    const int64_t cluster_version = data_store_desc_->get_major_working_cluster_version();
    if (!data_store_desc_->is_major_merge_type() || cluster_version >= DATA_VERSION_4_3_0_0) {
      // see ObMicroBlockWriter::build_block, column_checksums_ and min_merged_trans_version_ share the same memory space.
      // Only major merge set column_checksums_, so we can set min_merged_trans_version_ regardless of data version.
      micro_writer_->update_merged_trans_version(-cur_row_version);
    } else if (!not_compat_for_queuing_mode_42x(cluster_version)) {
      micro_writer_->update_max_merged_trans_version(-cur_row_version);
    }
  }
  return ret;
}

int ObMacroBlockWriter::init_hash_index_builder(const ObMacroDataSeq &start_seq)
{
  int ret = OB_SUCCESS;
  if (data_store_desc_->get_tablet_id().is_user_tablet()
      && !data_store_desc_->is_major_or_meta_merge_type()
      && start_seq.is_data_block()) {
    // only build hash index for data block in minor
    if (OB_FAIL(hash_index_builder_.init_if_needed(data_store_desc_))) {
      STORAGE_LOG(WARN, "Failed to build hash_index builder", K(ret));
    }
  }
  return ret;
}

int ObMacroBlockWriter::init_data_pre_warmer(const ObMacroDataSeq &start_seq)
{
  int ret = OB_SUCCESS;
  const ObTabletID &tablet_id = data_store_desc_->get_tablet_id();
  if (tablet_id.is_user_tablet()) {
    bool need_pre_warm = false;
    int tmp_ret = OB_SUCCESS;
    if (start_seq.is_data_block()) {
      ObTabletStatAnalyzer tablet_analyzer;
      if (OB_TMP_FAIL(MTL(ObTenantTabletStatMgr *)->get_tablet_analyzer(
              data_store_desc_->get_ls_id(), tablet_id, tablet_analyzer))) {
        STORAGE_LOG(WARN, "Failed to get tablet stat analyzer", K(tmp_ret));
      } else {
        need_pre_warm = true;
      }
    } else if (start_seq.is_index_block()) {
      need_pre_warm = true;
    }
    if (need_pre_warm) {
      if (OB_TMP_FAIL(data_block_pre_warmer_.init(nullptr))) {
        STORAGE_LOG(WARN, "Failed to init pre warmer", K(tmp_ret));
      }
    }
  }

  return ret;
}

int ObMacroBlockWriter::append_row_and_hash_index(const ObDatumRow &row)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(micro_writer_->append_row(row))) {
    if (ret != OB_BUF_NOT_ENOUGH) {
      STORAGE_LOG(WARN, "Failed to append row in micro writer", K(ret), K(row));
    }
  } else if (hash_index_builder_.is_valid()) {
    if (OB_UNLIKELY(FLAT_ROW_STORE != data_store_desc_->get_row_store_type())) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "Unexpected row store type", K(ret), K(data_store_desc_->get_row_store_type()));
    } else {
      int64_t hash_index_size = hash_index_builder_.estimate_size(true);
      if (OB_UNLIKELY(!micro_writer_->has_enough_space_for_hash_index(hash_index_size))) {
        ret = OB_BUF_NOT_ENOUGH;
      } else if (OB_FAIL(hash_index_builder_.add(row))) {
        if (ret != OB_NOT_SUPPORTED) {
          STORAGE_LOG(WARN, "Failed to append hash index", K(ret), K(row));
        } else {
          ret = OB_SUCCESS;
        }
        hash_index_builder_.reset();
      }
    }
  }
  return ret;
}

int ObMacroBlockWriter::append_index_micro_block(ObMicroBlockDesc &micro_block_desc)
{
  // used to append normal index micro block
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(micro_block_desc.row_count_ <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "micro desc is empty", K(ret));
  } else if (OB_UNLIKELY(nullptr != builder_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "expect null builder for index macro writer", K(ret), K_(builder));
  } else if (OB_FAIL(micro_helper_.compress_encrypt_micro_block(micro_block_desc,
                                              macro_blocks_[current_index_].get_current_macro_seq(),
                                              macro_blocks_[current_index_].get_data_size()))) {
    // do not dump micro_writer_ here
    STORAGE_LOG(WARN, "failed to compress and encrypt micro block", K(ret), K(micro_block_desc));
  } else if (OB_FAIL(write_micro_block(micro_block_desc))) {
    STORAGE_LOG(WARN, "fail to build micro block", K(ret), K(micro_block_desc));
  }
  STORAGE_LOG(DEBUG, "build micro block desc index", "tablet_id", data_store_desc_->get_tablet_id(),
    K(micro_block_desc), "lbt", lbt(), K(ret));
  return ret;
}

int ObMacroBlockWriter::get_estimate_meta_block_size(const ObDataMacroBlockMeta &macro_meta, int64_t &estimate_size)
{
  int ret = OB_SUCCESS;
  estimate_size = 0;

  if (OB_ISNULL(builder_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "unexpected null builder", K(ret));
  } else if (OB_FAIL(builder_->cal_macro_meta_block_size(macro_meta.end_key_, estimate_size))) {
    STORAGE_LOG(WARN, "fail to cal macro meta size", K(ret), K(macro_meta));
  }

  return ret;
}

int ObMacroBlockWriter::build_micro_block()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  int64_t block_size = 0;
  ObMicroBlockDesc micro_block_desc;
  if (micro_writer_->get_row_count() <= 0) {
    ret = OB_INNER_STAT_ERROR;
    STORAGE_LOG(WARN, "micro_block_writer is empty", K(ret));
  } else if (OB_FAIL(build_hash_index_block())) {
    STORAGE_LOG(WARN, "Failed to build hash index block", K(ret));
  } else if (OB_FAIL(micro_writer_->build_micro_block_desc(micro_block_desc))) {
    STORAGE_LOG(WARN, "failed to build micro block desc", K(ret));
  } else {
    micro_block_desc.last_rowkey_ = last_key_;
    block_size = micro_block_desc.buf_size_;
    if (data_block_pre_warmer_.is_valid()
        && OB_TMP_FAIL(data_block_pre_warmer_.reserve_kvpair(micro_block_desc))) {
      if (OB_BUF_NOT_ENOUGH != tmp_ret) {
        STORAGE_LOG(WARN, "Fail to reserve data block cache value", K(tmp_ret));
      }
    }
    if (OB_FAIL(micro_helper_.compress_encrypt_micro_block(micro_block_desc,
                                              macro_blocks_[current_index_].get_current_macro_seq(),
                                              macro_blocks_[current_index_].get_data_size()))) {
      micro_writer_->dump_diagnose_info(); // ignore dump error
      STORAGE_LOG(WARN, "failed to compress and encrypt micro block", K(ret), K(micro_block_desc));
    } else {
      if (OB_FAIL(write_micro_block(micro_block_desc))) {
        STORAGE_LOG(WARN, "fail to write micro block ", K(ret), K(micro_block_desc));
      } else if (OB_FAIL(micro_block_adaptive_splitter_.update_compression_info(micro_block_desc.row_count_,
          block_size, micro_block_desc.buf_size_))) {
        STORAGE_LOG(WARN, "Fail to update_compression_info", K(ret), K(micro_block_desc));
      }
      if (OB_FAIL(ret) || !data_block_pre_warmer_.is_valid() || OB_TMP_FAIL(tmp_ret)) {
      } else if (OB_TMP_FAIL(data_block_pre_warmer_.update_and_put_kvpair(micro_block_desc))) {
        STORAGE_LOG(WARN, "Fail to build data cache key and put into cache", K(tmp_ret));
      }
    }
    data_block_pre_warmer_.reuse();
  }

#ifdef ERRSIM
  if (data_store_desc_->encoding_enabled()) {
    ret = OB_E(EventTable::EN_BUILD_DATA_MICRO_BLOCK) ret;
  }
#endif

  STORAGE_LOG(DEBUG, "build micro block desc", "tablet_id", data_store_desc_->get_tablet_id(),
    K(micro_block_desc), "lbt", lbt(), K(ret), K(tmp_ret));

  if (OB_SUCC(ret)) {
    micro_writer_->reuse();
    if (hash_index_builder_.is_valid()) {
      hash_index_builder_.reuse();
    }
    if (OB_NOT_NULL(merge_info_)) {
      merge_info_->original_size_ += block_size;
      merge_info_->compressed_size_ += micro_block_desc.buf_size_;
      merge_info_->new_micro_count_in_new_macro_++;
    }
    if (nullptr != data_aggregator_) {
      data_aggregator_->reuse();
    }
  }
  return ret;
}

int ObMacroBlockWriter::build_micro_block_desc(
    const ObMicroBlock &micro_block,
    ObMicroBlockDesc &micro_block_desc,
    ObMicroBlockHeader &header_for_rewrite)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!micro_block.micro_index_info_->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(micro_block));
  } else if (OB_UNLIKELY(!micro_block.header_.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expect valid micro header", K(ret), K(micro_block.header_));
  } else if (micro_block.header_.has_column_checksum_
      && micro_block.micro_index_info_->row_header_->get_schema_version() == data_store_desc_->get_schema_version()) {
    if (OB_FAIL(build_micro_block_desc_with_reuse(micro_block, micro_block_desc))) {
      LOG_WARN("fail to build micro block desc v3", K(ret), K(micro_block), K(micro_block_desc));
    }
  } else if (OB_FAIL(build_micro_block_desc_with_rewrite(micro_block, micro_block_desc, header_for_rewrite))) {
    LOG_WARN("fail to build micro block desc v2", K(ret), K(micro_block), K(micro_block_desc));
  }
  STORAGE_LOG(DEBUG, "build micro block desc", K(micro_block), K(micro_block_desc));
  return ret;
}

int ObMacroBlockWriter::build_hash_index_block()
{
  int ret = OB_SUCCESS;
  if (hash_index_builder_.is_valid()) {
    if (OB_UNLIKELY(FLAT_ROW_STORE != data_store_desc_->get_row_store_type())) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "Unexpected row store type", K(ret), K(data_store_desc_->get_row_store_type()));
    } else if (OB_FAIL(micro_writer_->append_hash_index(hash_index_builder_))) {
      if (ret != OB_NOT_SUPPORTED) {
        LOG_WARN("Failed to append hash index to micro block writer", K(ret));
      } else {
        ret = OB_SUCCESS;
      }
      hash_index_builder_.reset();
    }
  }
  return ret;
}

int ObMacroBlockWriter::build_micro_block_desc_with_reuse(
    const ObMicroBlock &micro_block,
    ObMicroBlockDesc &micro_block_desc)
{
  int ret = OB_SUCCESS;

  const ObMicroBlockHeader &header = micro_block.header_;
  if (OB_UNLIKELY(!header.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expect valid micro header", K(ret), K(header));
  } else if (OB_FAIL(save_last_key(micro_block.range_.get_end_key()))) {
    LOG_WARN("Fail to save last key, ", K(ret), K(micro_block.range_.get_end_key()));
  } else {
    micro_block_desc.header_ = &header;
    micro_block_desc.last_rowkey_ = micro_block.range_.get_end_key();
    micro_block_desc.data_size_ = header.data_length_;
    micro_block_desc.column_count_ = header.column_count_;
    micro_block_desc.row_count_ = header.row_count_;
    micro_block_desc.buf_ = micro_block.payload_data_.get_buf() + header.header_size_;
    micro_block_desc.buf_size_ = header.data_zlength_;
    micro_block_desc.has_string_out_row_ = micro_block.micro_index_info_->has_string_out_row();
    micro_block_desc.has_lob_out_row_ = micro_block.micro_index_info_->has_lob_out_row();
    micro_block_desc.original_size_ = header.original_length_;
  }
  STORAGE_LOG(DEBUG, "build micro block desc reuse", "tablet_id", data_store_desc_->get_tablet_id(),
    K(micro_block_desc), "lbt", lbt(), K(ret));
  return ret;
}

int ObMacroBlockWriter::build_micro_block_desc_with_rewrite(
    const ObMicroBlock &micro_block,
    ObMicroBlockDesc &micro_block_desc,
    ObMicroBlockHeader &header)
{
  int ret = OB_SUCCESS;
  ObIMicroBlockReader *reader = NULL;
  ObMicroBlockData decompressed_data;
  const bool deep_copy_des_meta = false;
  ObMicroBlockDesMeta micro_des_meta;
  header = micro_block.header_;
  ObRowStoreType row_store_type = static_cast<ObRowStoreType>(header.row_store_type_);

  if (OB_UNLIKELY(!micro_block.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(micro_block));
  } else if (OB_UNLIKELY(!header.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expect valid micro header", K(ret), K(header));
  } else if (OB_FAIL(reader_helper_.get_reader(row_store_type, reader))) {
    LOG_WARN("fail to get reader", K(ret), K(micro_block));
  } else if (OB_ISNULL(reader)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("reader is null", K(ret), KP(reader));
  } else if (OB_FAIL(micro_block.micro_index_info_->row_header_->fill_micro_des_meta(
      deep_copy_des_meta, micro_des_meta))) {
    LOG_WARN("fail to fill micro block deserialize meta", K(ret), KPC(micro_block.micro_index_info_));
  } else {
    bool is_compressed = false;
    reader->reset();

    if (OB_FAIL(macro_reader_.decrypt_and_decompress_data(micro_des_meta, micro_block.data_.get_buf(),
        micro_block.data_.get_buf_size(), decompressed_data.get_buf(), decompressed_data.get_buf_size(), is_compressed))) {
      LOG_WARN("fail to decrypt and decompress data", K(ret));
    } else if (OB_FAIL(reader->init(decompressed_data, nullptr))) {
      LOG_WARN("reader init failed", K(micro_block), K(ret));
    } else if (OB_FAIL(save_last_key(micro_block.range_.get_end_key()))) {
      LOG_WARN("Fail to save last key, ", K(ret), K(micro_block.range_.get_end_key()));
    } else {
      micro_block_desc.header_ = &header;
      micro_block_desc.buf_ = micro_block.payload_data_.get_buf() + header.header_size_; // get original data_buf
      micro_block_desc.buf_size_ = header.data_zlength_;
      // rewrite column_count, column_checksum, and header_size
      header.column_count_ = data_store_desc_->get_row_column_count();
      header.has_column_checksum_ = data_store_desc_->is_major_merge_type();
      header.column_checksums_ = header.has_column_checksum_ ? curr_micro_column_checksum_ : NULL;
      header.header_size_ = header.get_serialize_size();

      micro_block_desc.last_rowkey_ = micro_block.range_.get_end_key();
      micro_block_desc.data_size_ = header.data_length_;
      micro_block_desc.original_size_ = header.original_length_;
      micro_block_desc.column_count_ = header.column_count_;
      micro_block_desc.row_count_ = header.row_count_;
      micro_block_desc.has_string_out_row_ = micro_block.micro_index_info_->has_string_out_row();
      micro_block_desc.has_lob_out_row_ = micro_block.micro_index_info_->has_lob_out_row();
      if (header.has_column_checksum_) {
        MEMSET(curr_micro_column_checksum_, 0, sizeof(int64_t) * data_store_desc_->get_row_column_count());
        if (OB_FAIL(calc_micro_column_checksum(header.column_count_, *reader, curr_micro_column_checksum_))) {
          STORAGE_LOG(WARN, "fail to calc micro block column checksum", K(ret));
        }
      }
    }
  }
  STORAGE_LOG(DEBUG, "build micro block desc rewrite", "tablet_id", data_store_desc_->get_tablet_id(),
    K(micro_block_desc), "lbt", lbt(), K(ret));
  return ret;
}

int ObMacroBlockWriter::write_micro_block(ObMicroBlockDesc &micro_block_desc)
{
  int ret = OB_SUCCESS;
  int64_t data_offset = 0;
  if (OB_FAIL(alloc_block())) {
    STORAGE_LOG(WARN, "Fail to pre-alloc block", K(ret));
  } else if (OB_NOT_NULL(builder_)) {
    // we use builder_->append_row() to judge whether the micro_block can be added
    // only used to write data block
    micro_block_desc.macro_id_ = ObIndexBlockRowHeader::DEFAULT_IDX_ROW_MACRO_ID;
    micro_block_desc.block_offset_ = macro_blocks_[current_index_].get_data_size();
    if (nullptr != data_aggregator_ &&
        OB_FAIL(data_aggregator_->get_aggregated_row(micro_block_desc.aggregated_row_))) {
      STORAGE_LOG(WARN, "Fail to get aggregated row", K(ret), KPC_(data_aggregator));
    } else if (OB_FAIL(builder_->append_row(micro_block_desc, macro_blocks_[current_index_]))) {
      if (OB_BUF_NOT_ENOUGH != ret) {
        STORAGE_LOG(ERROR, "Fail to write micro block, ", K(ret), K(micro_block_desc));
      } else if (OB_FAIL(try_switch_macro_block())) {
        STORAGE_LOG(WARN, "Fail to switch macro block, ", K(ret));
      } else if (OB_FAIL(alloc_block())) {
        STORAGE_LOG(WARN, "Fail to pre-alloc block", K(ret));
      } else {
        micro_block_desc.block_offset_ = macro_blocks_[current_index_].get_data_size();
        if (OB_FAIL(builder_->append_row(micro_block_desc, macro_blocks_[current_index_]))) {
          STORAGE_LOG(WARN, "fail to append row", K(ret), K(micro_block_desc));
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else if (FALSE_IT(micro_block_desc.macro_id_ = macro_handles_[current_index_].get_macro_id())) {
    } else if (OB_FAIL(macro_blocks_[current_index_].write_micro_block(micro_block_desc, data_offset))) {
      STORAGE_LOG(WARN, "Fail to write micro block, ", K(ret), K(micro_block_desc));
    } else if (OB_UNLIKELY(micro_block_desc.block_offset_ != data_offset)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "expect block offset equal ", K(ret), K(micro_block_desc), K(data_offset));
    }
  } else {
    // we use macro_block.write_micro_block() to judge whether the micro_block can be added
    if (OB_FAIL(macro_blocks_[current_index_].write_micro_block(micro_block_desc, data_offset))) {
      if (OB_BUF_NOT_ENOUGH == ret) {
        if (OB_FAIL(try_switch_macro_block())) {
          STORAGE_LOG(WARN, "Fail to switch macro block, ", K(ret));
        } else if (OB_FAIL(alloc_block())) {
          STORAGE_LOG(WARN, "Fail to pre-alloc block", K(ret));
        } else if (OB_FAIL(macro_blocks_[current_index_].write_micro_block(micro_block_desc, data_offset))) {
          STORAGE_LOG(WARN, "Fail to write micro block, ", K(ret));
        }
      } else {
        STORAGE_LOG(ERROR, "Fail to write micro block, ", K(ret), K(micro_block_desc));
      }
    }
    if (OB_SUCC(ret)) {
      micro_block_desc.macro_id_ = macro_handles_[current_index_].get_macro_id();
      micro_block_desc.block_offset_ = data_offset;
    }
  }

  if (OB_SUCC(ret)) {
    last_micro_size_ = micro_block_desc.data_size_;
    last_micro_expand_pct_ = micro_block_desc.original_size_  * 100 / micro_block_desc.data_size_;
  }

  return ret;
}

int ObMacroBlockWriter::try_active_flush_macro_block()
{
  int ret = OB_SUCCESS;
  const int64_t index_and_meta_block_size = builder_ == nullptr ? 0 :
      builder_->get_estimate_index_block_size() + builder_->get_estimate_meta_block_size();
  ObMacroBlock &macro_block = macro_blocks_[current_index_];
  const int64_t macro_remain_size = macro_block.get_remain_size();
  const int64_t estimate_macro_remain_size = macro_remain_size - index_and_meta_block_size;
  if (!is_need_macro_buffer_ && micro_writer_->get_row_count() != 0) {
    if (is_try_full_fill_macro_block_) {
      if (estimate_macro_remain_size < DEFAULT_MINIMUM_CS_ENCODING_BLOCK_SIZE) {
        if (OB_UNLIKELY(current_index_ != 0)) {
          STORAGE_LOG(WARN, "unexpected current index", K(ret));
        } else if (OB_FAIL(flush_macro_block(macro_block))) {
          STORAGE_LOG(WARN, "macro block writer fail to flush macro block.", K(ret));
        }
      } else if (estimate_macro_remain_size < data_store_desc_->get_micro_block_size()) {
        // Reserving 1/4 of DEFAULT_MINIMUM_CS_ENCODING_BLOCK_SIZE for micro header makes it
        // more likely that the data micro block can be put into macro block.
        const int64_t estimate_upper_bound = (estimate_macro_remain_size - DEFAULT_MINIMUM_CS_ENCODING_BLOCK_SIZE / 4) * last_micro_expand_pct_ / 100;
        micro_writer_->set_block_size_upper_bound(estimate_macro_remain_size);
      }
    } else {
      const int64_t micro_size = MIN(last_micro_size_, data_store_desc_->get_micro_block_size());
      if (estimate_macro_remain_size < micro_size) {
        if (OB_UNLIKELY(current_index_ != 0)) {
          STORAGE_LOG(WARN, "unexpected current index", K(ret));
        } else if (OB_FAIL(flush_macro_block(macro_block))) {
          STORAGE_LOG(WARN, "macro block writer fail to flush macro block.", K(ret));
        }
      }
    }
  }

  return ret;
}

int ObMacroBlockWriter::flush_macro_block(ObMacroBlock &macro_block)
{
  int ret = OB_SUCCESS;
  ObLogicMacroBlockId cur_logic_id;
  cur_logic_id.logic_version_ = data_store_desc_->get_logical_version();
  cur_logic_id.column_group_idx_ = data_store_desc_->get_table_cg_idx();
  cur_logic_id.data_seq_.macro_data_seq_ = current_macro_seq_;
  cur_logic_id.tablet_id_ = data_store_desc_->get_tablet_id().id();
  cur_logic_id.is_mds_ = is_mds_merge(data_store_desc_->get_merge_type());

  ObMacroBlockHandle &macro_handle = macro_handles_[current_index_];
  ObMacroBlockHandle &prev_handle = macro_handles_[(current_index_ + 1) % 2];
  const int64_t ddl_start_row_offset = callback_ == nullptr ? -1 : callback_->get_ddl_start_row_offset();
  if (OB_UNLIKELY(!macro_block.is_dirty())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "empty macro block has no pre-alloc macro id", K(ret), K(current_index_));
  } else if (OB_NOT_NULL(callback_) && OB_FAIL(callback_->wait())) {
    STORAGE_LOG(WARN, "fail to wait callback flush", K(ret));
  } else if (is_need_macro_buffer_ && OB_FAIL(wait_io_finish(prev_handle))) {
    STORAGE_LOG(WARN, "Fail to wait io finish, ", K(ret));
  } else if (OB_NOT_NULL(builder_)
      && OB_FAIL(builder_->generate_macro_row(macro_block, macro_handle.get_macro_id(), ddl_start_row_offset))) {
    STORAGE_LOG(WARN, "fail to generate macro row", K(ret), K_(current_macro_seq));
  } else if (OB_FAIL(macro_block.flush(macro_handle, block_write_ctx_))) {
    STORAGE_LOG(WARN, "macro block writer fail to flush macro block.", K(ret));
  } else if (OB_NOT_NULL(callback_) && OB_FAIL(callback_->write(macro_handle,
                                                                cur_logic_id,
                                                                macro_block.get_data_buf(),
                                                                upper_align(macro_block.get_data_size(), DIO_ALIGN_SIZE),
                                                                macro_block.get_row_count()))) {
    STORAGE_LOG(WARN, "fail to do callback flush", K(ret));
  }
  if (OB_SUCC(ret)) {
    if (nullptr != callback_) {
      DEBUG_SYNC(AFTER_DDL_WRITE_MACRO_BLOCK);
    }
    ++current_macro_seq_;
    const int64_t current_macro_seq = is_need_macro_buffer_ ? current_macro_seq_ + 1 :
        current_macro_seq_;
    if (OB_FAIL(macro_block.init(*data_store_desc_, current_macro_seq))) {
      STORAGE_LOG(WARN, "macro block writer fail to init.", K(ret));
    }
  }
  return ret;
}

int ObMacroBlockWriter::flush_reuse_macro_block(const ObDataMacroBlockMeta &macro_meta)
{
  int ret = OB_SUCCESS;
  const MacroBlockId &macro_id = macro_meta.get_macro_id();
  const bool is_normal_cg = data_store_desc_->is_cg();
  const uint16_t table_cg_idx = data_store_desc_->get_table_cg_idx();
  if (OB_FAIL(block_write_ctx_.add_macro_block_id(macro_id))) {
    LOG_WARN("failed to add macro block meta", K(ret), K(macro_id));
  } else {
    block_write_ctx_.increment_old_block_count();
    FLOG_INFO("Async reuse macro block", K(macro_meta.end_key_), "macro_block_id", macro_id, K(is_normal_cg), K(macro_meta), K(table_cg_idx));
  }
  return ret;
}


int ObMacroBlockWriter::try_switch_macro_block()
{
  int ret = OB_SUCCESS;
  ObMacroBlock &macro_block = macro_blocks_[current_index_];
  if (macro_block.is_dirty()) {
    int32_t row_count = macro_block.get_row_count();
    if (OB_FAIL(flush_macro_block(macro_block))) {
      STORAGE_LOG(WARN, "macro block writer fail to flush macro block.", K(ret), K_(current_index));
    } else if (is_need_macro_buffer_) {
      current_index_ = (current_index_ + 1) % 2;
    }
  }
  // do not pre-alloc macro_id here, may occupy extra macro block
  return ret;
}

int ObMacroBlockWriter::check_write_complete(const MacroBlockId &macro_block_id)
{
  int ret = OB_SUCCESS;

  ObMacroBlockReadInfo read_info;
  read_info.macro_block_id_ = macro_block_id;
  read_info.size_ = OB_SERVER_BLOCK_MGR.get_macro_block_size();
  read_info.io_desc_.set_wait_event(ObWaitEventIds::DB_FILE_COMPACT_READ);
  read_info.io_timeout_ms_ = std::max(GCONF._data_storage_io_timeout / 1000, DEFAULT_IO_WAIT_TIME_MS);
  ObMacroBlockHandle read_handle;
  if (OB_ISNULL(io_buf_) && OB_ISNULL(io_buf_ =
      reinterpret_cast<char*>(allocator_.alloc(read_info.size_)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "failed to alloc macro read info buffer", K(ret));
  } else {
    read_info.buf_ = io_buf_;
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ObBlockManager::async_read_block(read_info, read_handle))) {
    STORAGE_LOG(WARN, "fail to async read macro block", K(ret), K(read_info));
  } else if (OB_FAIL(read_handle.wait())) {
    STORAGE_LOG(WARN, "fail to wait io finish", K(ret), K(read_info));
  } else if (OB_FAIL(ObSSTableMacroBlockChecker::check(
      read_info.buf_,
      read_handle.get_data_size(),
      CHECK_LEVEL_PHYSICAL))) {
    STORAGE_LOG(WARN, "fail to verity macro block", K(ret), K(macro_block_id));
  }
  return ret;
}

int ObMacroBlockWriter::wait_io_finish(ObMacroBlockHandle &macro_handle)
{
  // wait prev_handle io finish
  int ret = OB_SUCCESS;
  const bool is_normal_cg = data_store_desc_->is_cg();
  if (OB_FAIL(macro_handle.wait())) {
    STORAGE_LOG(WARN, "macro block writer fail to wait io finish", K(ret));
  } else {
    if (!macro_handle.is_empty()) {
      FLOG_INFO("wait io finish", K(macro_handle.get_macro_id()), K(data_store_desc_->get_table_cg_idx()), K(is_normal_cg));
      int64_t check_level = 0;
      if (OB_ISNULL(micro_writer_)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "micro_writer is null", K(ret));
      } else if (FALSE_IT(check_level = micro_writer_->get_micro_block_merge_verify_level())) {
      } else if (MICRO_BLOCK_MERGE_VERIFY_LEVEL::ENCODING_AND_COMPRESSION_AND_WRITE_COMPLETE == check_level) {
        if (OB_FAIL(check_write_complete(macro_handle.get_macro_id()))) {
          STORAGE_LOG(WARN, "fail to check io complete", K(ret));
        }
      }
    }
    macro_handle.reset();
  }
  return ret;
}

int ObMacroBlockWriter::alloc_block()
{
  int ret = OB_SUCCESS;
  ObMacroBlockHandle &macro_handle = macro_handles_[current_index_];
  if (macro_blocks_[current_index_].is_dirty()) { // has been allocated
  } else if (OB_UNLIKELY(!is_need_macro_buffer_ && current_index_ != 0)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "unexpected current index", K(ret));
  } else if (!is_need_macro_buffer_ && OB_FAIL(wait_io_finish(macro_handle))) {
    STORAGE_LOG(WARN, "fail to wait io finish", K(ret), K(macro_handle));
  } else if (OB_UNLIKELY(macro_handle.is_valid())) {
    STORAGE_LOG(INFO, "block maybe wrong", K(macro_handle));
  } else if (OB_FAIL(OB_SERVER_BLOCK_MGR.alloc_block(macro_handle))) {
    STORAGE_LOG(WARN, "Fail to pre-alloc block for new macro block",
        K(ret), K_(current_index), K_(current_macro_seq));
  }
  return ret;
}

int ObMacroBlockWriter::check_micro_block_need_merge(
    const ObMicroBlock &micro_block, bool &need_merge)
{
  int ret = OB_SUCCESS;
  need_merge = true;
  if (!micro_block.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid micro_block", K(micro_block), K(ret));
  } else {
    ObRowStoreType row_store_type = static_cast<ObRowStoreType>(micro_block.header_.row_store_type_);
    if (row_store_type != data_store_desc_->get_row_store_type()) {
      need_merge = true;
    } else if (micro_writer_->get_row_count() <= 0
        && micro_block.header_.data_length_ > data_store_desc_->get_micro_block_size() / 2) {
      need_merge = false;
    } else if (micro_writer_->get_block_size() > data_store_desc_->get_micro_block_size() / 2
        && micro_block.header_.data_length_ > data_store_desc_->get_micro_block_size() / 2) {
      need_merge = false;
    } else {
      need_merge = true;
    }
    STORAGE_LOG(DEBUG, "check micro block need merge", K(micro_writer_->get_row_count()), K(micro_block.data_.get_buf_size()),
        K(micro_writer_->get_block_size()), K(data_store_desc_->get_micro_block_size()), K(need_merge));
  }
  return ret;
}

int ObMacroBlockWriter::merge_micro_block(const ObMicroBlock &micro_block)
{
  int ret = OB_SUCCESS;
  ObIMicroBlockReader *micro_reader = NULL;
  const bool deep_copy_des_meta = false;
  ObMicroBlockDesMeta micro_des_meta;
  ObRowStoreType row_store_type = static_cast<ObRowStoreType>(micro_block.header_.row_store_type_);

  if (OB_ISNULL(data_store_desc_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not opened", K(ret));
  } else if (OB_FAIL(reader_helper_.get_reader(row_store_type, micro_reader))) {
    STORAGE_LOG(WARN, "fail to get reader", K(ret), K(micro_block));
  } else if (OB_ISNULL(micro_reader)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "The micro reader is NULL, ", K(ret));
  } else if (!micro_block.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid micro_block", K(micro_block), K(ret));
  } else if (OB_UNLIKELY(!data_store_desc_->is_major_or_meta_merge_type())) {
    // forbid micro block level merge for minor merge now
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "minor merge does not allow micro block level merge", K(ret));
  } else if (OB_FAIL(micro_block.micro_index_info_->row_header_->fill_micro_des_meta(
      deep_copy_des_meta, micro_des_meta))) {
    STORAGE_LOG(WARN, "fail to fill micro block deserialize meta", K(ret), K(micro_block));
  } else {
    int64_t split_size = 0;
    const int64_t merged_size = micro_writer_->get_block_size() + micro_block.header_.data_length_;
    ObMicroBlockData decompressed_data;
    bool is_compressed = false;
    if (merged_size > 2 * data_store_desc_->get_micro_block_size()) {
      split_size = merged_size / 2;
    } else {
      split_size = 2 * data_store_desc_->get_micro_block_size();
    }

    micro_reader->reset();
    if (OB_FAIL(macro_reader_.decrypt_and_decompress_data(micro_des_meta, micro_block.data_.get_buf(),
        micro_block.data_.get_buf_size(), decompressed_data.get_buf(), decompressed_data.get_buf_size(), is_compressed))) {
      STORAGE_LOG(WARN, "fail to decrypt and decompress data", K(ret));
    } else if (OB_FAIL(micro_reader->init(decompressed_data, nullptr))) {
      STORAGE_LOG(WARN, "micro_block_reader init failed", K(micro_block), K(ret));
    } else {
      for (int64_t it = 0; OB_SUCC(ret) && it != micro_reader->row_count(); ++it) {
        if (OB_FAIL(micro_reader->get_row(it, datum_row_))) {
          STORAGE_LOG(WARN, "get_row failed", K(ret));
        } else if (OB_FAIL(append_row(datum_row_, split_size))) {
          STORAGE_LOG(WARN, "append_row failed", K_(datum_row), K(split_size), K(ret));
        }
      }

      if (OB_SUCC(ret) && micro_writer_->get_block_size() >= data_store_desc_->get_micro_block_size()) {
        if (OB_FAIL(build_micro_block())) {
          LOG_WARN("build_micro_block failed", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObMacroBlockWriter::save_last_key(const ObDatumRow &row)
{
  int ret = OB_SUCCESS;
  ObDatumRowkey rowkey;
  if (data_store_desc_->is_cg()) {
    //skip save last key for cg
  } else if (OB_FAIL(rowkey.assign(row.storage_datums_, data_store_desc_->get_rowkey_column_count()))) {
    STORAGE_LOG(WARN, "Failed to assign rowkey", K(ret));
  } else if (OB_FAIL(save_last_key(rowkey))) {
    STORAGE_LOG(WARN, "Fail to save last rowkey, ", K(ret), K(rowkey));
  } else {
    last_key_with_L_flag_ = row.mvcc_row_flag_.is_last_multi_version_row();
  }
  return ret;
}
int ObMacroBlockWriter::save_last_key(const ObDatumRowkey &last_key)
{
  int ret = OB_SUCCESS;
  last_key_.reset();
  rowkey_allocator_.reuse();
  if (data_store_desc_->is_cg()) {
    last_key_.set_min_rowkey(); // used to protect cg sstable
  } else if (OB_FAIL(last_key.deep_copy(last_key_, rowkey_allocator_))) {
    STORAGE_LOG(WARN, "Fail to copy last key", K(ret), K(last_key));
  } else {
    STORAGE_LOG(DEBUG, "save last key", K(last_key_));
  }
  return ret;
}

int ObMacroBlockWriter::calc_micro_column_checksum(const int64_t column_cnt,
                                                   ObIMicroBlockReader &reader,
                                                   int64_t *column_checksum)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(NULL == column_checksum || column_cnt <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", K(ret), KP(column_checksum), K(column_cnt));
  } else {
    for (int64_t iter = 0; OB_SUCC(ret) && iter != reader.row_count(); ++iter) {
      if (OB_FAIL(reader.get_row(iter, datum_row_))) {
        STORAGE_LOG(WARN, "fail to get row", K(ret), K(iter));
      } else if (datum_row_.get_column_count() != column_cnt) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "error unexpected, row column count is invalid", K(ret), K(datum_row_), K(column_cnt));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < column_cnt; ++i) {
          column_checksum[i] += datum_row_.storage_datums_[i].checksum(0);
        }
      }
    }
  }
  return ret;
}

int ObMacroBlockWriter::build_micro_writer(const ObDataStoreDesc *data_store_desc,
                                           ObIAllocator &allocator,
                                           ObIMicroBlockWriter *&micro_writer,
                                           const int64_t verify_level)
{
  int ret = OB_SUCCESS;
  void *buf = nullptr;
  ObMicroBlockEncoder *encoding_writer = nullptr;
  ObMicroBlockCSEncoder *cs_encoding_writer = nullptr;
  ObMicroBlockWriter *flat_writer = nullptr;
  if (data_store_desc->encoding_enabled()) {
    ObMicroBlockEncodingCtx encoding_ctx;
    encoding_ctx.macro_block_size_ = data_store_desc->get_macro_block_size();
    encoding_ctx.micro_block_size_ = data_store_desc->get_micro_block_size();
    encoding_ctx.column_cnt_ = data_store_desc->get_row_column_count();
    encoding_ctx.rowkey_column_cnt_ = data_store_desc->get_rowkey_column_count();
    encoding_ctx.col_descs_ = &data_store_desc->get_full_stored_col_descs();
    encoding_ctx.encoder_opt_ = data_store_desc->encoder_opt_;
    encoding_ctx.column_encodings_ = nullptr;
    encoding_ctx.major_working_cluster_version_ = data_store_desc->get_major_working_cluster_version();
    encoding_ctx.row_store_type_ = data_store_desc->get_row_store_type();
    encoding_ctx.need_calc_column_chksum_ = data_store_desc->is_major_merge_type();
    encoding_ctx.compressor_type_ = data_store_desc->get_compressor_type();
    if (ObStoreFormat::is_row_store_type_with_pax_encoding(data_store_desc->get_row_store_type())) {
      if (OB_ISNULL(buf = allocator.alloc(sizeof(ObMicroBlockEncoder)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        STORAGE_LOG(WARN, "fail to alloc memory", K(ret));
      } else if (OB_ISNULL(encoding_writer = new (buf) ObMicroBlockEncoder())) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "fail to new encoding writer", K(ret));
      } else if (OB_FAIL(encoding_writer->init(encoding_ctx))) {
        STORAGE_LOG(WARN, "Fail to init micro block encoder, ", K(ret));
      } else {
        encoding_writer->set_micro_block_merge_verify_level(verify_level);
        micro_writer = encoding_writer;
      }
    } else if (ObStoreFormat::is_row_store_type_with_cs_encoding(data_store_desc->get_row_store_type())) {
      if (OB_ISNULL(buf = allocator.alloc(sizeof(ObMicroBlockCSEncoder)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        STORAGE_LOG(WARN, "fail to alloc memory", K(ret));
      } else if (OB_ISNULL(cs_encoding_writer = new (buf) ObMicroBlockCSEncoder())) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "fail to cs encoding writer", K(ret));
      } else if (OB_FAIL(cs_encoding_writer->init(encoding_ctx))) {
        STORAGE_LOG(WARN, "Fail to init micro block encoder, ", K(ret));
      } else {
        cs_encoding_writer->set_micro_block_merge_verify_level(verify_level);
        micro_writer = cs_encoding_writer;
      }
    } else {
      ret = OB_INNER_STAT_ERROR;
      LOG_WARN("unknow row store type", K(ret), K(data_store_desc->get_row_store_type()));
    }
  } else {
    if (OB_ISNULL(buf = allocator.alloc(sizeof(ObMicroBlockWriter)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "fail to alloc memory", K(ret));
    } else if (OB_ISNULL(flat_writer = new (buf) ObMicroBlockWriter())) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "fail to new encoding writer", K(ret));
    } else if (OB_FAIL(flat_writer->init(
        data_store_desc->get_micro_block_size_limit(),
        data_store_desc->get_rowkey_column_count(),
        data_store_desc->get_row_column_count(),
        &data_store_desc->get_rowkey_col_descs(),
        data_store_desc->contain_full_col_descs(),
        data_store_desc->is_major_merge_type()))) {
      STORAGE_LOG(WARN, "Fail to init micro block flat writer, ", K(ret));
    } else {
      flat_writer->set_micro_block_merge_verify_level(verify_level);
      micro_writer = flat_writer;
    }
  }
  if (OB_FAIL(ret)) {
    if (OB_NOT_NULL(encoding_writer)) {
      encoding_writer->~ObMicroBlockEncoder();
      allocator.free(encoding_writer);
      encoding_writer = nullptr;
    }
    if (OB_NOT_NULL(cs_encoding_writer)) {
      cs_encoding_writer->~ObMicroBlockCSEncoder();
      allocator.free(cs_encoding_writer);
      cs_encoding_writer = nullptr;
    }
    if (OB_NOT_NULL(flat_writer)) {
      flat_writer->~ObMicroBlockWriter();
      allocator.free(flat_writer);
      flat_writer = nullptr;
    }
  }
  return ret;
}

int ObMacroBlockWriter::open_bf_cache_writer(
    const ObDataStoreDesc &desc,
    const int64_t bloomfilter_size)
{
  int ret = OB_SUCCESS;
  bf_cache_writer_[0].reset();
  bf_cache_writer_[1].reset();
  return ret;
}

void ObMacroBlockWriter::dump_block_and_writer_buffer()
{
  // dump cur_macro_block and micro_writer_buffer
  dump_micro_block(*micro_writer_);
  dump_macro_block(macro_blocks_[current_index_]);
  FLOG_WARN_RET(OB_SUCCESS, "dump block and writer buffer", K(this),
      K_(current_index), K_(current_macro_seq), KPC_(data_store_desc));
}

void ObMacroBlockWriter::dump_micro_block(ObIMicroBlockWriter &micro_writer)
{
  int ret = OB_SUCCESS;
  char *buf = NULL;
  int64_t size = 0;
  if (micro_writer.get_row_count() > 0) {
    if (OB_FAIL(micro_writer.build_block(buf, size))) {
      STORAGE_LOG(WARN, "failed to build micro block", K(ret));
    } else if (OB_FAIL(micro_helper_.dump_micro_block_writer_buffer(buf, size))) {
      STORAGE_LOG(WARN, "failed to dump micro block", K(ret));
    }
  }
  return;
}

void ObMacroBlockWriter::dump_macro_block(ObMacroBlock &macro_block)
{
  // dump incomplete macro block
  int ret = OB_SUCCESS;
  if (macro_block.is_dirty()) {
    const int64_t block_cnt = macro_block.get_micro_block_count();
    const int64_t data_offset =
        ObMacroBlock::calc_basic_micro_block_data_offset(
          data_store_desc_->get_row_column_count(),
          data_store_desc_->get_rowkey_column_count(),
          data_store_desc_->get_fixed_header_version());
    const char *data_buf = macro_block.get_data_buf() + data_offset;
    const int64_t data_size = macro_block.get_data_size() - data_offset;
    int64_t pos = 0;
    ObMicroBlockDesMeta micro_des_meta(
        data_store_desc_->get_compressor_type(), data_store_desc_->get_row_store_type(),
        data_store_desc_->get_encrypt_id(), data_store_desc_->get_master_key_id(), data_store_desc_->get_encrypt_key());
    ObMicroBlockHeader header;
    ObMicroBlockData micro_data;
    ObMicroBlockData decompressed_data;
    int64_t block_idx = 0;
    while (OB_SUCC(ret) && pos < data_size && block_idx < block_cnt) {
      const char *buf = data_buf + pos;
      if (OB_FAIL(header.deserialize(data_buf, data_size, pos))) {
        STORAGE_LOG(WARN, "fail to deserialize micro header", K(ret), K(pos), K(data_size));
      } else if (OB_FAIL(header.check_header_checksum())) {
        STORAGE_LOG(WARN, "check micro header failed", K(ret), K(header));
      } else {
        FLOG_WARN("error micro block header (not flushed)", K(header), K(block_idx));
        micro_data.buf_ = buf;
        micro_data.size_ = header.header_size_ + header.data_zlength_;
        bool is_compressed = false;
        if (OB_FAIL(macro_reader_.decrypt_and_decompress_data(micro_des_meta,
            micro_data.get_buf(), micro_data.get_buf_size(),
            decompressed_data.get_buf(), decompressed_data.get_buf_size(), is_compressed))) {
          STORAGE_LOG(WARN, "fail to decrypt and decomp data", K(ret), K(micro_des_meta));
        } else if (OB_FAIL(micro_helper_.dump_micro_block_writer_buffer(
            decompressed_data.get_buf(), decompressed_data.get_buf_size()))) {
          STORAGE_LOG(WARN, "fail to dump current micro block", K(ret), K(micro_data));
        } else {
          pos += header.data_zlength_;
        }
      }
    }
  }
  return;
}

int ObMacroBlockWriter::init_pre_agg_util(const ObDataStoreDesc &data_store_desc)
{
  int ret = OB_SUCCESS;
  const ObIArray<ObSkipIndexColMeta> &full_agg_metas = data_store_desc.get_agg_meta_array();
  const bool need_pre_aggregation =
      data_store_desc.is_major_or_meta_merge_type()
      && nullptr != data_store_desc.sstable_index_builder_
      && full_agg_metas.count() > 0;
  if (!need_pre_aggregation) {
    // Skip
  } else {
    char *row_buf = nullptr;
    char *aggregator_buf = nullptr;
    if (OB_ISNULL(row_buf = static_cast<char *>(allocator_.alloc(sizeof(ObDatumRow))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("Fail to allocate memory for pre-aggregated row", K(ret));
    } else if (OB_ISNULL(aggregator_buf = static_cast<char *>(
        allocator_.alloc(sizeof(ObSkipIndexAggregator))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("Fail to allocate memory for aggrgator", K(ret));
    } else {
      aggregated_row_ = new (row_buf) ObDatumRow();
      data_aggregator_ = new (aggregator_buf) ObSkipIndexAggregator();
      if (OB_FAIL(aggregated_row_->init(allocator_, full_agg_metas.count()))) {
        LOG_WARN("Fail to init aggregated row", K(ret), K(data_store_desc));
      } else if (OB_FAIL(data_aggregator_->init(
          full_agg_metas,
          data_store_desc.get_col_desc_array(),
          true, /* aggregate from data */
          *aggregated_row_,
          allocator_))) {
        LOG_WARN("Fail to init aggregator", K(ret), K(data_store_desc));
      }
    }

    if (OB_FAIL(ret)) {
      release_pre_agg_util();
    }
  }
  return ret;
}

void ObMacroBlockWriter::release_pre_agg_util()
{
  if (nullptr != data_aggregator_) {
    data_aggregator_->~ObSkipIndexAggregator();
    allocator_.free(data_aggregator_);
    data_aggregator_ = nullptr;
  }
  if (nullptr != aggregated_row_) {
    aggregated_row_->~ObDatumRow();
    allocator_.free(aggregated_row_);
    aggregated_row_ = nullptr;
  }
}

int ObMacroBlockWriter::agg_micro_block(const ObMicroIndexInfo &micro_index_info)
{
  int ret = OB_SUCCESS;
  if (micro_index_info.is_pre_aggregated() && nullptr != data_aggregator_) {
    if (OB_FAIL(data_aggregator_->eval(micro_index_info.agg_row_buf_,
        micro_index_info.agg_buf_size_, micro_index_info.get_row_count()))) {
      LOG_WARN("Fail to evaluate by micro block", K(ret), K(micro_index_info));
    }
  }
  return ret;
}

}//end namespace blocksstable
}//end namespace oceanbase