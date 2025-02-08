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

#include "ob_macro_block_writer.h"
#include "src/storage/blocksstable/index_block/ob_sstable_sec_meta_iterator.h"
#include "storage/blocksstable/cs_encoding/ob_micro_block_cs_encoder.h"
#include "src/storage/ddl/ob_ddl_clog.h"
#ifdef OB_BUILD_SHARED_STORAGE
#include "storage/compaction/ob_major_pre_warmer.h"
#endif
namespace oceanbase
{
using namespace common;
using namespace storage;
using namespace share::schema;
using namespace share;
using namespace compaction;
namespace blocksstable
{
ERRSIM_POINT_DEF(EN_NO_NEED_MERGE_MICRO_BLK);

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
    ret = OB_ERR_COMPRESS_DECOMPRESS_DATA;
    LOG_DBA_ERROR(OB_ERR_COMPRESS_DECOMPRESS_DATA, "msg", "decompressed size is not equal to original size", K(ret),
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
        ret = OB_ERR_COMPRESS_DECOMPRESS_DATA;
        LOG_DBA_ERROR(OB_ERR_COMPRESS_DECOMPRESS_DATA, "msg", "micro block write check failed",
            K(new_checksum), K(checksum), K(ret), KPC(data_store_desc_), K(checksum_helper_));
      }
#ifdef ERRSIM
  if (data_store_desc_->encoding_enabled()) {
    ret = OB_E(EventTable::EN_BUILD_DATA_MICRO_BLOCK) ret;
  }
#endif
      if (OB_UNLIKELY(OB_ERR_COMPRESS_DECOMPRESS_DATA == ret)) {
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
    merge_block_info_(),
    micro_writer_(nullptr),
    reader_helper_(),
    hash_index_builder_(),
    micro_helper_(),
    current_index_(0),
    macro_seq_generator_(nullptr),
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
    datum_row_(),
    aggregated_row_(nullptr),
    data_aggregator_(nullptr),
    callback_(nullptr),
    device_handle_(nullptr),
    builder_(NULL),
    pre_warmer_(NULL),
    object_cleaner_(nullptr),
    io_buf_(nullptr),
    validator_(NULL),
    is_cs_encoding_writer_(false)
{
}

ObMacroBlockWriter::~ObMacroBlockWriter()
{
  reset();
}

void ObMacroBlockWriter::reset()
{
  data_store_desc_ = nullptr;
  merge_block_info_.reset();
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
  current_index_ = 0;
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
  device_handle_ = nullptr;
  if (OB_NOT_NULL(builder_)) {
    builder_->~ObDataIndexBlockBuilder();
    builder_ = nullptr;
  }
  micro_block_adaptive_splitter_.reset();
  release_pre_agg_util();
  if (OB_NOT_NULL(macro_seq_generator_)) {
    macro_seq_generator_->~ObMacroSeqGenerator();
    allocator_.free(macro_seq_generator_);
    macro_seq_generator_ = nullptr;
  }
  if (NULL != pre_warmer_) {
    pre_warmer_->~ObIPreWarmer();
    allocator_.free(pre_warmer_);
    pre_warmer_ = nullptr;
  }
  allocator_.reset();
  rowkey_allocator_.reset();
  io_buf_ = nullptr;
  validator_ = nullptr;
  is_cs_encoding_writer_ = false;
}


int ObMacroBlockWriter::open(
    const ObDataStoreDesc &data_store_desc,
    const int64_t parallel_idx,
    const blocksstable::ObMacroSeqParam &macro_seq_param,
    const share::ObPreWarmerParam &pre_warm_param,
    ObSSTablePrivateObjectCleaner &object_cleaner,
    ObIMacroBlockFlushCallback *callback,
    ObIMacroBlockValidator *validator,
    ObIODevice *device_handle)
{
  int ret = OB_SUCCESS;
  reset();
  if (OB_UNLIKELY(!data_store_desc.is_valid() || parallel_idx < 0 ||
                  !macro_seq_param.is_valid() || !pre_warm_param.is_valid() ||
                  (!data_store_desc.is_for_index_or_meta() &&
                   is_validate_exec_mode(data_store_desc.get_exec_mode()) && NULL == validator))) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid macro block writer input argument.", K(ret), K(data_store_desc), K(parallel_idx),
      K(macro_seq_param), K(pre_warm_param), KP(validator));
  } else if (OB_UNLIKELY(nullptr != callback && !is_need_macro_buffer_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument, ddl callback should used with double buffer",
             K(ret), KP(callback), K(is_need_macro_buffer_));
  } else {
    ObSSTableIndexBuilder *sstable_index_builder = data_store_desc.sstable_index_builder_;
    object_cleaner_ = &object_cleaner;
    callback_ = callback;
    validator_ = validator;
    device_handle_ = device_handle;
    data_store_desc_ = &data_store_desc;
    if (data_store_desc.is_cg()) {
      last_key_.set_min_rowkey(); // used to protect cg sstable
    }

    if (OB_FAIL(init_macro_seq_generator(macro_seq_param))) {
      LOG_WARN("init macro_seq_param failed", K(ret));
    } else if (OB_FAIL(init_pre_warmer(pre_warm_param))) {
      LOG_WARN("failed to init pre warmer", K(ret), K(pre_warm_param));
    } else if (OB_FAIL(init_hash_index_builder())) {
      STORAGE_LOG(WARN, "Failed to build hash_index builder", K(ret));
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
      is_cs_encoding_writer_ = data_store_desc_->encoding_enabled() && ObStoreFormat::is_row_store_type_with_cs_encoding(data_store_desc_->get_row_store_type());
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
    int64_t tmp_macro_seq = -1; // to get first macro seq
    if (FAILEDx(macro_seq_generator_->get_next(tmp_macro_seq))) {
      LOG_WARN("get next macro seq failed", K(ret));
    } else if (OB_FAIL(macro_blocks_[0].init(data_store_desc, tmp_macro_seq, merge_block_info_))) {
      STORAGE_LOG(WARN, "Fail to init 0th macro block, ", K(ret));
    } else if (is_need_macro_buffer_ && OB_FAIL(macro_seq_generator_->preview_next(macro_seq_generator_->get_current(), tmp_macro_seq))) {
      LOG_WARN("get next macro seq failed", K(ret), K(macro_seq_generator_->get_current()), K(tmp_macro_seq));
    } else if (is_need_macro_buffer_ && OB_FAIL(macro_blocks_[1].init(data_store_desc, tmp_macro_seq, merge_block_info_))) {
      STORAGE_LOG(WARN, "Fail to init 1th macro block, ", K(ret));
    } else if (!data_store_desc_->is_major_merge_type()) {
      // do nothing
    } else if (OB_ISNULL(curr_micro_column_checksum_ = static_cast<int64_t *>(
          allocator_.alloc(sizeof(int64_t) * data_store_desc_->get_row_column_count())))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "fail to allocate memory for curr micro block column checksum", K(ret));
    } else {
      MEMSET(curr_micro_column_checksum_, 0, sizeof(int64_t) * data_store_desc_->get_row_column_count());
    }
    if (OB_FAIL(ret)) {
    } else if (OB_NOT_NULL(sstable_index_builder)) {
      if (OB_FAIL(sstable_index_builder->new_index_builder(builder_, data_store_desc, allocator_, macro_seq_param, pre_warm_param, callback))) {
        STORAGE_LOG(WARN, "fail to alloc index builder", K(ret));
      } else if (OB_ISNULL(builder_)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "unexpected null builder", K(ret), KPC(sstable_index_builder));
      } else if (OB_FAIL(builder_->set_parallel_task_idx(parallel_idx))) {
        STORAGE_LOG(WARN, "fail to set_parallel_task_idx", K(ret), K(parallel_idx));
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
  LOG_DEBUG("append row", K(row));

  if (OB_UNLIKELY(nullptr == data_store_desc_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("The ObMacroBlockWriter has not been opened", K(ret));
  } else if (OB_FAIL(append_row(row, data_store_desc_->get_micro_block_size()))) {
    LOG_WARN("Fail to append row", K(ret));
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(try_active_flush_macro_block())) {
    LOG_WARN("Fail to try_active_flush_macro_block", K(ret));
  } else {
    LOG_DEBUG("Success to append row, ", "tablet_id", data_store_desc_->get_tablet_id(), K(row));
  }
  return ret;
}

int ObMacroBlockWriter::append_batch(const ObBatchDatumRows &datum_rows,
                                     const ObMacroBlockDesc *curr_macro_desc)
{
  int ret = OB_SUCCESS;

  if (!is_cs_encoding_writer_) {
    ObDatumRow &row = datum_row_;
    for (int64_t i = 0; OB_SUCC(ret) && i < datum_rows.row_count_; i ++) {
      if (OB_FAIL(datum_rows.to_datum_row(i, row))) {
        LOG_WARN("fail to to datum row", KR(ret), K(i));
      } else if (OB_FAIL(append_row(row, curr_macro_desc))) {
        LOG_WARN("fail to append row", K(row), KR(ret));
      }
    }
  } else {
    if (OB_UNLIKELY(nullptr == data_store_desc_)) {
      ret = OB_NOT_INIT;
      STORAGE_LOG(WARN, "The ObMacroBlockWriter has not been opened", K(ret));
    } else if (OB_FAIL(append_batch(datum_rows, data_store_desc_->get_micro_block_size()))) {
      STORAGE_LOG(WARN, "Fail to append row", K(ret));
    } else if (OB_FAIL(try_active_flush_macro_block())) {
      STORAGE_LOG(WARN, "Fail to try_active_flush_macro_block", K(ret));
    }
  }

  return ret;
}


int ObMacroBlockWriter::append(const ObDataMacroBlockMeta &macro_meta,
                               const ObMicroBlockData *micro_block_data)
{
  int ret = OB_SUCCESS;

  if (micro_writer_->get_row_count() > 0 && OB_FAIL(build_micro_block())) {
    LOG_WARN("Fail to build current micro block", K(ret));
  } else if (OB_FAIL(try_switch_macro_block())) {
    LOG_WARN("Fail to flush and switch macro block", K(ret));
  } else if (OB_UNLIKELY(!macro_meta.is_valid()) || OB_ISNULL(builder_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid arguments", K(ret), KP(builder_), K(macro_meta));
  } else if (OB_FAIL(builder_->append_macro_block(macro_meta, micro_block_data))) {
    LOG_WARN("Fail to append index block rows", K(ret), KP(builder_), K(macro_meta));
  } else if (OB_FAIL(flush_reuse_macro_block(macro_meta))) {
    LOG_WARN("Fail to flush reuse macro block", K(ret), K(macro_meta));
  } else {
    is_macro_or_micro_block_reused_ = true;
    last_key_with_L_flag_ = false; // clear flag
    merge_block_info_.multiplexed_macro_block_count_++;
    merge_block_info_.macro_block_count_++;
    merge_block_info_.total_row_count_ += macro_meta.val_.row_count_;
    merge_block_info_.occupy_size_ += macro_meta.val_.occupy_size_;
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


int ObMacroBlockWriter::append(const ObBatchDatumRows &datum_rows, const int64_t start, const int64_t write_row_count)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(append_batch_to_micro_block(datum_rows, start, write_row_count))) {
    if (OB_BUF_NOT_ENOUGH == ret) {
      if (0 == micro_writer_->get_row_count()) {
        ret = OB_NOT_SUPPORTED;
        STORAGE_LOG(ERROR, "The single row is too large, ", K(ret));
      } else if (OB_FAIL(build_micro_block())) {
        STORAGE_LOG(WARN, "Fail to build micro block, ", K(ret));
      } else if (OB_FAIL(append_batch_to_micro_block(datum_rows, start, write_row_count))) {
        STORAGE_LOG(ERROR, "Fail to append row to micro block, ", K(ret));
      }
    }
  }

  return ret;
}

int ObMacroBlockWriter::append(const ObBatchDatumRows &datum_rows)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(append_batch_to_micro_block(datum_rows, 0, datum_rows.row_count_))) {
    if (OB_BUF_NOT_ENOUGH == ret) {
      if (0 == micro_writer_->get_row_count()) {
        ret = OB_SUCCESS;
        for (int64_t i = 0; OB_SUCC(ret) && i < datum_rows.row_count_; i ++) {
          if (OB_FAIL(append(datum_rows, i, 1))) {
            LOG_WARN("fail to append row", KR(ret));
          }
        }
      } else if (OB_FAIL(build_micro_block())) {
        STORAGE_LOG(WARN, "Fail to build micro block, ", K(ret));
      } else if (OB_FAIL(append_batch_to_micro_block(datum_rows, 0, datum_rows.row_count_))) {
        if (OB_BUF_NOT_ENOUGH == ret) {
          ret = OB_SUCCESS;
          for (int64_t i = 0; OB_SUCC(ret) && i < datum_rows.row_count_; i ++) {
            if (OB_FAIL(append(datum_rows, i, 1))) {
              LOG_WARN("fail to append row", KR(ret));
            }
          }
        } else {
          STORAGE_LOG(ERROR, "Fail to append row to micro block, ", K(ret));
        }
      }
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


int ObMacroBlockWriter::data_aggregator_eval(const ObBatchDatumRows &datum_rows, const int64_t start, const int64_t write_row_count)
{

  int ret = OB_SUCCESS;
  ObDatumRow &row = datum_row_;
  for (int64_t i = 0; OB_SUCC(ret) && i < write_row_count; i ++) {
    if (OB_FAIL(datum_rows.to_datum_row(i + start, row))) {
      LOG_WARN("fail to get row", K(i), KR(ret));
    } else if (OB_FAIL(data_aggregator_->eval(row))) {
      LOG_WARN("fail to eval row", KR(ret));
    }
  }
  return ret;
}

int ObMacroBlockWriter::append_batch(const ObBatchDatumRows &datum_rows, const int64_t split_size)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(data_store_desc_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "The ObMacroBlockWriter has not been opened, ", K(ret), KP(data_store_desc_));
  } else if (OB_UNLIKELY(split_size < data_store_desc_->get_micro_block_size())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid split_size", K(ret), K(split_size));
  }

  if (OB_SUCC(ret)) {
    bool is_split = false;
    if (OB_FAIL(append(datum_rows))) {
      STORAGE_LOG(WARN, "Fail to append row to micro block", K(ret));
    } else if (OB_FAIL(micro_block_adaptive_splitter_.check_need_split(micro_writer_->get_block_size(), micro_writer_->get_row_count(),
          split_size, macro_blocks_[current_index_].get_data_size(), is_keep_freespace(), is_split))) {
      STORAGE_LOG(WARN, "Failed to check need split", K(ret), KPC(micro_writer_));
    } else if (is_split && OB_FAIL(build_micro_block())) {
      STORAGE_LOG(WARN, "Fail to build micro block, ", K(ret));
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
    STORAGE_LOG(WARN, "macro block writer fail to check order", K(row), KPC(data_store_desc_));
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

int ObMacroBlockWriter::append_macro_block(
    const ObMacroBlockDesc &macro_desc,
    const ObMicroBlockData *micro_block_data)
{
  int ret = OB_SUCCESS;
  bool is_micro_index_clustered = micro_index_clustered();
  const ObDataMacroBlockMeta *data_block_meta = macro_desc.macro_meta_;

  if (OB_UNLIKELY(nullptr == data_store_desc_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "The ObMacroBlockWriter has not been opened", K(ret));
  } else if (OB_UNLIKELY(nullptr == data_block_meta)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "unexpected null meta", K(ret));
  } else if (OB_UNLIKELY(macro_desc.is_clustered_index_tree_ != is_micro_index_clustered)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "unexpected index tree type", K(ret), K(macro_desc), K(is_micro_index_clustered));
  }

  if (OB_FAIL(ret)) {
  } else if (!is_micro_index_clustered) {
    if (OB_FAIL(append(*data_block_meta, nullptr))) {
      STORAGE_LOG(WARN, "failed to append macro block", K(ret), K(macro_desc), K(is_micro_index_clustered));
    }
  } else {
    if (OB_ISNULL(micro_block_data)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "unexpected clustered index desc", K(ret),
                  K(is_micro_index_clustered), KP(micro_block_data));
    } else if (OB_FAIL(append(*data_block_meta, micro_block_data))) {
      STORAGE_LOG(WARN, "failed to append macro block", K(ret), K(macro_desc), K(is_micro_index_clustered));
    }
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
      // Insert micro block into macro block bloom filter.
      } else if (OB_FAIL(write_micro_block(micro_block_desc))) {
        STORAGE_LOG(WARN, "Failed to write micro block, ", K(ret), K(micro_block_desc));
      } else {
        merge_block_info_.multiplexed_micro_count_in_new_macro_++;
      }

      if (OB_SUCC(ret) && nullptr != data_aggregator_) {
        data_aggregator_->reuse();
      }

      // Insert micro block into macro block bloom filter.
      if (OB_FAIL(ret)) {
      } else if (data_store_desc_->enable_macro_block_bloom_filter()
                 && OB_FAIL(
                     macro_blocks_[current_index_].get_macro_block_bloom_filter()->insert_micro_block(micro_block))) {
        LOG_WARN("fail to insert micro block to bloom filter",
                K(ret), K(micro_block), KPC(macro_blocks_[current_index_].get_macro_block_bloom_filter()));
      }
    }
  } else {
    // We don't need to insert into bloom filter specially during the `merge_micro_block`, as it is performed by
    // `append_row`, and `append_row` will automatically insert row into bloom filter.
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
    const ObMicroIndexInfo &micro_index_info) {
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
  }

  if (OB_SUCC(ret) && nullptr != data_aggregator_) {
    data_aggregator_->reuse();
  }

  // Insert micro block into macro block bloom filter.
  if (OB_FAIL(ret)) {
  } else if (data_store_desc_->enable_macro_block_bloom_filter()
             && OB_FAIL(
                 macro_blocks_[current_index_].get_macro_block_bloom_filter()->insert_micro_block(micro_block_desc,
                                                                                                  micro_index_info))) {
    LOG_WARN("fail to insert micro block to bloom filter",
            K(ret), K(micro_block_desc), K(micro_index_info),
            KPC(macro_blocks_[current_index_].get_macro_block_bloom_filter()));
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

int ObMacroBlockWriter::check_meta_macro_block_need_rewrite(bool &need_rewrite) const
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(builder_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "invalid meta block writer", K(ret));
  } else {
    need_rewrite = get_macro_data_size()
      < data_store_desc_->get_macro_block_size() * DEFAULT_MACRO_BLOCK_REWRTIE_THRESHOLD / 100;
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
      ObStorageObjectHandle &curr_handle = macro_handles_[current_index_];
      ObStorageObjectHandle &prev_handle = macro_handles_[(current_index_ + 1) % 2];
      ObMacroBlock *prev_macro_block = is_need_macro_buffer_ ? &macro_blocks_[(current_index_ + 1) % 2] : nullptr;
      ObMacroBlock *curr_macro_block = is_need_macro_buffer_ ? &macro_blocks_[current_index_] : nullptr;
      if (OB_UNLIKELY(!is_need_macro_buffer_ && current_index_ != 0)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "unexpected current index", K(ret));
      } else if (is_need_macro_buffer_ && OB_FAIL(wait_io_finish(prev_handle, prev_macro_block))) {
        STORAGE_LOG(WARN, "Fail to wait io finish, ", K(ret));
      } else if (OB_FAIL(wait_io_finish(curr_handle, curr_macro_block))) {
        STORAGE_LOG(WARN, "Fail to wait io finish, ", K(ret));
      } else if (OB_NOT_NULL(callback_) && OB_FAIL(callback_->wait())) {
        STORAGE_LOG(WARN, "fail to wait callback flush", K(ret));
      }
    }

    if (OB_SUCC(ret) && OB_NOT_NULL(builder_)) {
      if (OB_FAIL(builder_->close(last_key_, &block_write_ctx_))) {
        STORAGE_LOG(WARN, "fail to close data index builder", K(ret), K(last_key_));
      }
    }
    if (OB_SUCC(ret) && (NULL != pre_warmer_) && OB_FAIL(pre_warmer_->close())) {
      STORAGE_LOG(WARN, "failed to close pre warmer", KR(ret), KPC_(pre_warmer));
    }
#ifdef OB_BUILD_SHARED_STORAGE
    if (OB_NOT_NULL(validator_)) {
      validator_->close();
    }
#endif
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
    } else if (!data_store_desc_->is_for_index_or_meta()
               && data_store_desc_->is_major_merge_type()
               && -cur_row_version > data_store_desc_->get_snapshot_version()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("Unexpected current row trans version in major merge", K(ret), K(cur_row_version), K(row),
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
        if (!data_store_desc_->is_major_or_meta_merge_type()
            && !data_store_desc_->is_for_index_or_meta()
            && !is_macro_or_micro_block_reused_
            && !last_key_with_L_flag_) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(ERROR, "meet another rowkey but not meet Last flag", K(ret), K(last_key_), K(row), K(last_key_with_L_flag_), KPC(data_store_desc_));
        }
      }
    }

    if (OB_ROWKEY_ORDER_ERROR == ret || OB_ERR_UNEXPECTED == ret || OB_ERR_PRIMARY_KEY_DUPLICATE == ret) {
      dump_micro_block(*micro_writer_); // print micro block have output
    }
  }
  return ret;
}

int ObMacroBlockWriter::update_micro_commit_info(const ObBatchDatumRows &datum_rows, const int64_t start, const int64_t write_row_count)
{
  int ret = OB_SUCCESS;
  bool is_ghost_row_flag = false;
  if (OB_FAIL(blocksstable::ObGhostRowUtil::is_ghost_row(datum_rows.mvcc_row_flag_, is_ghost_row_flag))) {
    STORAGE_LOG(ERROR, "failed to check ghost row", K(ret), K(datum_rows));
  } else if (data_store_desc_->is_cg() || is_ghost_row_flag) { //skip cg block & ghost row
  } else if (datum_rows.mvcc_row_flag_.is_uncommitted_row()) {
    micro_writer_->set_contain_uncommitted_row();
    LOG_TRACE("meet uncommited trans row", K(datum_rows));
  } else {
    const int64_t trans_version_col_idx = data_store_desc_->get_schema_rowkey_col_cnt();
    ObIVector *vec = nullptr;
    if (trans_version_col_idx >= datum_rows.vectors_.count()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid args", K(trans_version_col_idx), K(datum_rows.vectors_.count()), KR(ret));
    } else {
      vec = datum_rows.vectors_.at(trans_version_col_idx);
      if (vec == nullptr) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("vec should not be nullptr", KR(ret));
      }
    }

    const char *pay_load = nullptr;
    bool is_null = false;
    ObLength length = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < write_row_count; i ++) {
      vec->get_payload(i + start, is_null, pay_load, length);
      const int64_t cur_row_version = ObDatum(pay_load, length, is_null).get_int();
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

int ObMacroBlockWriter::init_hash_index_builder()
{
  int ret = OB_SUCCESS;
  if (data_store_desc_->get_tablet_id().is_user_tablet()
      && !data_store_desc_->is_major_or_meta_merge_type()
      && !data_store_desc_->is_for_index_or_meta()
      && data_store_desc_->get_row_store_type() == FLAT_ROW_STORE) {
    // only build hash index for data block in minor
    if (OB_FAIL(hash_index_builder_.init_if_needed(data_store_desc_))) {
      STORAGE_LOG(WARN, "Failed to build hash_index builder", K(ret));
    }
  }
  return ret;
}

int ObMacroBlockWriter::init_macro_seq_generator(const blocksstable::ObMacroSeqParam &macro_seq_param)
{
  int ret = OB_SUCCESS;
  if (nullptr == macro_seq_generator_) {
    if (ObMacroSeqParam::SEQ_TYPE_INC == macro_seq_param.seq_type_) {
      macro_seq_generator_ = OB_NEWx(ObMacroIncSeqGenerator, &allocator_);
    } else if (ObMacroSeqParam::SEQ_TYPE_SKIP == macro_seq_param.seq_type_) {
      macro_seq_generator_ = OB_NEWx(ObMacroSkipSeqGenerator, &allocator_);
    } else {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("macro sequence generator not supported", K(ret), K(macro_seq_param));
    }
    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(macro_seq_generator_)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate memory for macro sequence generator failed", K(ret), K(macro_seq_param));
    } else if (OB_FAIL(macro_seq_generator_->init(macro_seq_param))) {
      LOG_WARN("init macro sequence generator failed", K(ret), K(macro_seq_param));
    }
  }
  return ret;
}

int ObMacroBlockWriter::append_batch_to_micro_block(const ObBatchDatumRows &datum_rows, const int64_t start, const int64_t write_row_count)
{
  int ret = OB_SUCCESS;
  ObDatumRow &last_row = datum_row_;

  if (OB_FAIL(datum_rows.to_datum_row(start + write_row_count - 1, last_row))) {
    LOG_WARN("fail to get last row", KR(ret));
  } else if (OB_FAIL(micro_writer_->append_batch(datum_rows, start, write_row_count))) {
    if (ret != OB_BUF_NOT_ENOUGH) {
      STORAGE_LOG(WARN, "Failed to append row in micro writer", K(ret));
    }
  } else if (OB_FAIL(update_micro_commit_info(datum_rows, start, write_row_count))) {
    STORAGE_LOG(WARN, "Fail to update_micro_commit_info", K(ret), K(datum_rows), K(start), K(write_row_count));
  } else if (OB_FAIL(save_last_key(last_row))) {
    STORAGE_LOG(WARN, "Fail to save last key, ", K(ret), K(last_row));
  } else if (nullptr != data_aggregator_ && OB_FAIL(data_aggregator_eval(datum_rows, start, write_row_count))) {
    STORAGE_LOG(WARN, "Fail to evaluate aggregate data", K(ret));
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
  // Insert row into macro block bloom filter if `append_row` succeed.
  } else if (data_store_desc_->enable_macro_block_bloom_filter()
             && OB_FAIL(macro_blocks_[current_index_].get_macro_block_bloom_filter()->insert_row(datum_row_))) {
    LOG_WARN("fail to insert row to bloom filter",
             K(ret), K(datum_row_), KPC(macro_blocks_[current_index_].get_macro_block_bloom_filter()));
  } else if (hash_index_builder_.is_valid()) {
    if (OB_FAIL(hash_index_builder_.add(row))) {
      if (ret != OB_NOT_SUPPORTED) {
        STORAGE_LOG(WARN, "Failed to append hash index", K(ret), K(row));
      } else {
        ret = OB_SUCCESS;
      }
      hash_index_builder_.reset();
      }
  }
  return ret;
}

int ObMacroBlockWriter::append_index_micro_block(ObMicroBlockDesc &micro_block_desc)
{
  // used to append normal index micro block
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (OB_UNLIKELY(micro_block_desc.row_count_ <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "micro desc is empty", K(ret));
  } else if (OB_UNLIKELY(nullptr != builder_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "expect null builder for index macro writer", K(ret), K_(builder));
  } else {
    bool reserve_succ_flag = false;
    if (is_for_index() && (NULL != pre_warmer_)) {
      IGNORE_RETURN pre_warmer_->reserve(micro_block_desc, reserve_succ_flag);
    }
    if (OB_FAIL(micro_helper_.compress_encrypt_micro_block(micro_block_desc,
                                      macro_blocks_[current_index_].get_current_macro_seq(),
                                      macro_blocks_[current_index_].get_data_size()))) {
      // do not dump micro_writer_ here
      STORAGE_LOG(WARN, "failed to compress and encrypt micro block", K(ret), K(micro_block_desc));
    } else if (OB_FAIL(write_micro_block(micro_block_desc))) {
      STORAGE_LOG(WARN, "fail to build micro block", K(ret), K(micro_block_desc));
    }
    if (OB_SUCC(ret) && is_for_index() && (NULL != pre_warmer_)) {
      IGNORE_RETURN pre_warmer_->add(micro_block_desc, reserve_succ_flag);
    }
    if (is_for_index() && (NULL != pre_warmer_)) {
      pre_warmer_->reuse();
    }
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
    bool reserve_succ_flag = false;
    micro_block_desc.last_rowkey_ = last_key_;
    block_size = micro_block_desc.buf_size_;
    if (NULL != pre_warmer_) {
      IGNORE_RETURN pre_warmer_->reserve(micro_block_desc, reserve_succ_flag);
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
      if (OB_SUCC(ret) && (NULL != pre_warmer_)) {
        IGNORE_RETURN pre_warmer_->add(micro_block_desc, reserve_succ_flag);
      }
    }
    if (NULL != pre_warmer_) {
      pre_warmer_->reuse();
    }
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
    merge_block_info_.original_size_ += block_size;
    merge_block_info_.compressed_size_ += micro_block_desc.buf_size_;
    merge_block_info_.new_micro_count_in_new_macro_++;
    if (data_store_desc_->is_for_index_or_meta()) {
      merge_block_info_.new_micro_info_.add_meta_micro_size(micro_block_desc.buf_size_);
    } else {
      merge_block_info_.new_micro_info_.add_data_micro_size(micro_block_desc.buf_size_);
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
    micro_block_desc.logic_micro_id_ = micro_block.micro_index_info_->get_logic_micro_id();
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
    const bool need_fill_logic_id = !data_store_desc_->is_for_index_or_meta() &&
                                    data_store_desc_->is_major_merge_type() &&
                                    data_store_desc_->get_major_working_cluster_version() >= DATA_VERSION_4_3_3_0 &&
                                    !micro_block_desc.logic_micro_id_.is_valid();
    micro_block_desc.macro_id_ = macro_handles_[current_index_].get_macro_id();
    micro_block_desc.block_offset_ = macro_blocks_[current_index_].get_data_size();
    if (need_fill_logic_id) {
      ObLogicMacroBlockId cur_logic_id;
      gen_logic_macro_id(cur_logic_id);
      micro_block_desc.logic_micro_id_.init(micro_block_desc.block_offset_, cur_logic_id);
    }

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
        micro_block_desc.macro_id_ = macro_handles_[current_index_].get_macro_id();
        micro_block_desc.block_offset_ = macro_blocks_[current_index_].get_data_size();
        if (need_fill_logic_id) {
          ObLogicMacroBlockId cur_logic_id;
          gen_logic_macro_id(cur_logic_id);
          micro_block_desc.logic_micro_id_.init(micro_block_desc.block_offset_, cur_logic_id);
        }
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

  ObStorageObjectHandle &macro_handle = macro_handles_[current_index_];
  ObStorageObjectHandle &prev_handle = macro_handles_[(current_index_ + 1) % 2];
  ObMacroBlock *prev_macro_block = is_need_macro_buffer_ ? &macro_blocks_[(current_index_ + 1) % 2] : nullptr;

  if (OB_UNLIKELY(!macro_block.is_dirty())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "empty macro block has no pre-alloc macro id", K(ret), K(current_index_));
  } else if (is_need_macro_buffer_ && OB_FAIL(wait_io_finish(prev_handle, prev_macro_block))) {
    STORAGE_LOG(WARN, "Fail to wait io finish, ", K(ret));
  }
  const int64_t ddl_start_row_offset = callback_ == nullptr ? -1 : callback_->get_ddl_start_row_offset();
  /* callback will set offset in wait io finish, keep it after wait io finish */
  if (OB_FAIL(ret)) {
  } else if (OB_NOT_NULL(builder_)
      && OB_FAIL(builder_->generate_macro_row(macro_block, macro_handle.get_macro_id(), ddl_start_row_offset))) {
    STORAGE_LOG(WARN, "fail to generate macro row", K(ret), "current_macro_seq", macro_seq_generator_->get_current());
  } else if (OB_FAIL(macro_block.flush(macro_handle, block_write_ctx_, device_handle_))) { // will not flush macro if !is_flush_macro_exec_mode
    STORAGE_LOG(WARN, "macro block writer fail to flush macro block.", K(ret));
#ifdef OB_BUILD_SHARED_STORAGE
  } else if (is_validate_exec_mode(data_store_desc_->get_exec_mode())) { // need serialize header to dump macro
    if (OB_NOT_NULL(validator_)) {
      // need compare macro checksum & dump macro for different ckm
      validator_->validate_and_dump(macro_block);
    }
#endif
  }
  if (OB_SUCC(ret)) {
    int64_t current_macro_seq = -1;
    if (OB_FAIL(macro_seq_generator_->get_next(current_macro_seq))) {
      LOG_WARN("get next macro sequence failed", K(ret));
    } else if (is_need_macro_buffer_) {
      /* if use buffer, init in wait_io_finish */
    } else if (OB_FAIL(macro_block.init(*data_store_desc_, current_macro_seq, merge_block_info_))) {
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

  ObStorageObjectReadInfo read_info;
  read_info.offset_ = 0;
  read_info.size_ = OB_STORAGE_OBJECT_MGR.get_macro_block_size();
  read_info.io_desc_.set_mode(ObIOMode::READ);
  read_info.io_desc_.set_wait_event(ObWaitEventIds::DB_FILE_COMPACT_READ);
  read_info.io_desc_.set_sys_module_id(ObIOModule::SSTABLE_MACRO_BLOCK_WRITE_IO);
  read_info.io_timeout_ms_ = std::max(GCONF._data_storage_io_timeout / 1000, DEFAULT_IO_WAIT_TIME_MS);
  read_info.macro_block_id_ = macro_block_id;
  read_info.mtl_tenant_id_ = MTL_ID();

  ObStorageObjectHandle read_handle;
  if (OB_ISNULL(io_buf_) && OB_ISNULL(io_buf_ =
      reinterpret_cast<char*>(allocator_.alloc(read_info.size_)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "failed to alloc macro read info buffer", K(ret));
  } else {
    read_info.buf_ = io_buf_;
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ObObjectManager::async_read_object(read_info, read_handle))) {
    STORAGE_LOG(WARN, "fail to async read macro block", K(ret), K(read_info));
  } else if (OB_FAIL(read_handle.wait())) {
    STORAGE_LOG(WARN, "fail to wait io finish", K(ret), K(read_info));
  } else if (OB_FAIL(ObSSTableMacroBlockChecker::check(
      read_handle.get_buffer(),
      read_handle.get_data_size(),
      CHECK_LEVEL_PHYSICAL))) {
    STORAGE_LOG(WARN, "fail to verity macro block", K(ret), K(macro_block_id));
  }
  return ret;
}

int ObMacroBlockWriter::wait_io_finish(ObStorageObjectHandle &macro_handle, ObMacroBlock *macro_block)
{
  // wait prev_handle io finish
  int ret = OB_SUCCESS;
  const bool is_normal_cg = data_store_desc_->is_cg();
  if (OB_FAIL(macro_handle.wait())) {
    STORAGE_LOG(WARN, "macro block writer fail to wait io finish", K(ret));
  } else if ((nullptr != macro_block) != is_need_macro_buffer_) { /* when use buffer macor block must not null, otherwise null*/
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("should set not null macro block when use buffer", K(ret), K(is_need_macro_buffer_), KP(macro_block));
  } else if (OB_NOT_NULL(device_handle_)) {
    macro_handle.reset();
  } else {
    if (!macro_handle.is_empty()) {
      int64_t block_io_us;
      if (OB_SUCCESS == macro_handle.get_io_time_us(block_io_us)) {
        merge_block_info_.block_io_us_ += block_io_us;
      }
      int64_t check_level = 0;
      if (OB_ISNULL(micro_writer_) || OB_UNLIKELY(!macro_handle.is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "invalid micro writer or macro handle", K(ret), KP(micro_writer_), K(macro_handle));
      } else if (FALSE_IT(check_level = micro_writer_->get_micro_block_merge_verify_level())) {
      } else if (MICRO_BLOCK_MERGE_VERIFY_LEVEL::ENCODING_AND_COMPRESSION_AND_WRITE_COMPLETE == check_level) {
        if (OB_FAIL(check_write_complete(macro_handle.get_macro_id()))) {
          STORAGE_LOG(WARN, "fail to check io complete", K(ret));
        }
      }

      if (OB_FAIL(ret)) {
      } else if (nullptr == callback_) {
        /* do nothing */
      } else if (OB_FAIL(exec_callback(macro_handle, macro_block))) {
        LOG_WARN("failed to exec callback func", K(ret), K(macro_handle));
      }
    } else if (!data_store_desc_->get_need_submit_io() && OB_NOT_NULL(callback_)) {
      // column store replica do not submit macro block io, but need ddl redo callback
      if (OB_FAIL(exec_callback(macro_handle, macro_block))) {
        LOG_WARN("failed to exec callback func", K(ret), K(macro_handle));
      }
    }
    /* init macro seq when need buffer (macro_should not be null) */
    int64_t current_macro_seq = -1;
    if (OB_FAIL(ret)) {
    } else if (!is_need_macro_buffer_) {
    } else if (OB_FAIL(macro_seq_generator_->preview_next(macro_seq_generator_->get_current(), current_macro_seq))) {
      LOG_WARN("try get next macro sequence failed", K(ret), K(macro_seq_generator_->get_current()), K(current_macro_seq));
    } else if (OB_FAIL(macro_block->init(*data_store_desc_, current_macro_seq, merge_block_info_))) {
      STORAGE_LOG(WARN, "macro block writer fail to init.", K(ret));
    }
    macro_handle.reset();
  }
  return ret;
}

int ObMacroBlockWriter::exec_callback(const ObStorageObjectHandle &macro_handle, ObMacroBlock *macro_block)
{
  int ret = OB_SUCCESS;
  if (nullptr == macro_block || nullptr == callback_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(macro_block), KP(callback_), K(macro_handle));
  } else {
    ObLogicMacroBlockId unused_logic_id;
    unused_logic_id.logic_version_ = data_store_desc_->get_logical_version();
    unused_logic_id.column_group_idx_ = data_store_desc_->get_table_cg_idx();
    unused_logic_id.data_seq_.macro_data_seq_ = macro_seq_generator_->get_current();
    unused_logic_id.tablet_id_ = data_store_desc_->get_tablet_id().id();
    if (!macro_block->is_dirty()) {
    } else if (OB_NOT_NULL(callback_) && OB_FAIL(callback_->wait())) {
      STORAGE_LOG(WARN, "fail to wait callback flush", K(ret));
    } else if (OB_NOT_NULL(callback_) && OB_FAIL(callback_->write(macro_handle,
                                                                  unused_logic_id,
                                                                  macro_block->get_data_buf(),
                                                                  upper_align(macro_block->get_data_size(),DIO_ALIGN_SIZE),
                                                                  macro_block->get_row_count()))) {
      STORAGE_LOG(WARN, "fail to do callback flush", K(ret));
    } else if (nullptr != callback_) {
      DEBUG_SYNC(AFTER_DDL_WRITE_MACRO_BLOCK);
    }
  }
  return ret;
}

bool ObMacroBlockWriter::micro_index_clustered() const
{
  return data_store_desc_->micro_index_clustered();
}

int ObMacroBlockWriter::alloc_block()
{
  int ret = OB_SUCCESS;
  ObStorageObjectHandle &macro_handle = macro_handles_[current_index_];
  ObStorageObjectOpt storage_opt;
  if (!is_local_exec_mode(data_store_desc_->get_exec_mode())) {
    // for ss mode, need set shared block type
    if (data_store_desc_->is_for_index_or_meta()) {
      storage_opt.set_ss_share_meta_macro_object_opt(
        data_store_desc_->get_tablet_id().id(),
        macro_seq_generator_->get_current(),
        data_store_desc_->get_table_cg_idx());
    } else {
      storage_opt.set_ss_share_data_macro_object_opt(
        data_store_desc_->get_tablet_id().id(),
        macro_seq_generator_->get_current(),
        data_store_desc_->get_table_cg_idx());
    }
  } else {
    if (data_store_desc_->is_for_index_or_meta()) {
      storage_opt.set_private_meta_macro_object_opt(data_store_desc_->get_tablet_id().id(),
                                                    data_store_desc_->get_tablet_transfer_seq());
    } else {
      storage_opt.set_private_object_opt(data_store_desc_->get_tablet_id().id(),
                                         data_store_desc_->get_tablet_transfer_seq());
    }
  }
  if (macro_blocks_[current_index_].is_dirty()) { // has been allocated
  } else if (OB_UNLIKELY(!is_need_macro_buffer_ && current_index_ != 0)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "unexpected current index", K(ret));
  } else if (!is_need_macro_buffer_ && OB_FAIL(wait_io_finish(macro_handle, nullptr /* single buffer not need flush prev */))) {
    STORAGE_LOG(WARN, "fail to wait io finish", K(ret), K(macro_handle));
  } else if (OB_UNLIKELY(macro_handle.is_valid())) {
    STORAGE_LOG(INFO, "block maybe wrong", K(macro_handle));
  } else if (OB_NOT_NULL(device_handle_)) {
    if (OB_FAIL(alloc_block_from_device(macro_handle))) {
      STORAGE_LOG(WARN, "Fail to pre-alloc block for new macro block from device",
          K(ret), K_(current_index));
    }
  } else if (OB_FAIL(OB_STORAGE_OBJECT_MGR.alloc_object(storage_opt, macro_handle))) {
    STORAGE_LOG(WARN, "Fail to pre-alloc block for new macro block",
        K(ret), K_(current_index), "current_macro_seq", macro_seq_generator_->get_current());
  } else if (OB_FAIL(object_cleaner_->add_new_macro_block_id(macro_handle.get_macro_id()))) {
    STORAGE_LOG(WARN, "fail to add new macro block id to cleaner", K(ret), K(macro_handle.get_macro_id()));
  }
  return ret;
}

int ObMacroBlockWriter::alloc_block_from_device(ObStorageObjectHandle &macro_handle)
{
  int ret = OB_SUCCESS;
  MacroBlockId macro_id;
  ObIOFd io_fd;
  ObIODOpts opts;
  if (OB_ISNULL(device_handle_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "unexpected null device handle", K(ret), KP_(device_handle));
  } else if (OB_FAIL(device_handle_->alloc_block(&opts, io_fd))) {
    LOG_ERROR("Failed to alloc block from device handle", K(ret));
  } else {
    macro_id.reset();
    macro_id.set_from_io_fd(io_fd);
    if (OB_FAIL(macro_handle.set_macro_block_id(macro_id))) {
      LOG_ERROR("Failed to set macro block id", K(ret), K(macro_id));
    } else {
      FLOG_INFO("successfully alloc block from device", K(macro_id));
    }
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
    const int64_t data_version = data_store_desc_->get_major_working_cluster_version();
    const ObRowStoreType row_store_type = static_cast<ObRowStoreType>(micro_block.header_.row_store_type_);
    if (row_store_type == data_store_desc_->get_row_store_type()) {
      const int64_t max_block_row_count = data_store_desc_->static_desc_->encoding_granularity_;
      if (data_version >= DATA_VERSION_4_3_4_1 && max_block_row_count > 0) {
        // we should consider row count first
        if (micro_block.header_.row_count_ >= max_block_row_count /  2) {
          need_merge = false;
        } else if (micro_block.header_.row_count_ >= max_block_row_count / 3 &&
            micro_writer_->get_row_count() >= max_block_row_count / 3) {
          need_merge = false;
        }
#ifdef ERRSIM
        if (OB_SUCC(ret)) {
          // always reuse micro block for test
          if (OB_UNLIKELY(EN_NO_NEED_MERGE_MICRO_BLK)) {
            need_merge = false;
            FLOG_INFO("ERRSIM EN_NO_NEED_MERGE_MICRO_BLK", KR(ret));
            ret = OB_SUCCESS;
          }
        }
#endif
      }
      if (!need_merge) {
      } else if (micro_writer_->get_row_count() <= 0
          && micro_block.header_.data_length_ > data_store_desc_->get_micro_block_size() / 2) {
        need_merge = false;
      } else if (micro_writer_->get_block_size() > data_store_desc_->get_micro_block_size() / 2
          && micro_block.header_.data_length_ > data_store_desc_->get_micro_block_size() / 2) {
        need_merge = false;
      }
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
    encoding_ctx.encoding_granularity_ = data_store_desc->static_desc_->encoding_granularity_ > 0 ?
                    data_store_desc->static_desc_->encoding_granularity_ : UINT64_MAX;
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

void ObMacroBlockWriter::dump_block_and_writer_buffer()
{
  // dump cur_macro_block and micro_writer_buffer
  dump_micro_block(*micro_writer_);
  dump_macro_block(macro_blocks_[current_index_]);
  FLOG_WARN_RET(OB_SUCCESS, "dump block and writer buffer", K(this),
      K_(current_index), "current_macro_seq", get_last_macro_seq(), KPC_(data_store_desc));
}

void ObMacroBlockWriter::dump_micro_block(ObIMicroBlockWriter &micro_writer)
{
  int ret = OB_SUCCESS;
  char *buf = NULL;
  int64_t size = 0;
  if (micro_writer.get_row_count() > 0) {
    if (data_store_desc_->encoding_enabled()) {
      micro_writer.dump_diagnose_info();
    } else {
      if (OB_FAIL(micro_writer.build_block(buf, size))) {
        STORAGE_LOG(WARN, "failed to build micro block", K(ret));
      } else if (OB_FAIL(micro_helper_.dump_micro_block_writer_buffer(buf, size))) {
        STORAGE_LOG(WARN, "failed to dump micro block", K(ret));
      }
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

int ObMacroBlockWriter::init_pre_warmer(const share::ObPreWarmerParam &pre_warm_param)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObPreWarmerType tmp_type =  pre_warm_param.type_;
  if (OB_ISNULL(data_store_desc_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("data store desc should not be null", KR(ret));
  } else if (data_store_desc_->get_tablet_id().is_user_tablet()
    && PRE_WARM_TYPE_NONE == tmp_type
    && data_store_desc_->is_for_index()) {
    // for user tablet & index macro, use mem pre warm. (meta tree could not pre_warm)
    tmp_type = MEM_PRE_WARM;
  }
  if (OB_FAIL(ret)) {
  } else if (OB_UNLIKELY(!pre_warm_param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(pre_warm_param));
  } else if (PRE_WARM_TYPE_NONE == tmp_type) {
    // do nothing
  } else if (MEM_PRE_WARM == tmp_type) {
    if (OB_FAIL(create_pre_warmer(MEM_PRE_WARM, pre_warm_param))) {
      LOG_WARN("fail to create pre warmer", KR(tmp_ret), K(pre_warm_param));
    }
  } else if (MEM_AND_FILE_PRE_WARM == tmp_type) {
#ifdef OB_BUILD_SHARED_STORAGE
    const ObMajorPreWarmerParam *param = static_cast<const ObMajorPreWarmerParam *>(&pre_warm_param);
    if ((ObSSMajorPrewarmLevel::PREWARM_NONE_LEVEL == param->pre_warm_level_)
        || ((ObSSMajorPrewarmLevel::PREWARM_ONLY_META_LEVEL == param->pre_warm_level_)
            && !data_store_desc_->is_for_index())) {
      if (OB_FAIL(create_pre_warmer(MEM_PRE_WARM, pre_warm_param))) {
        LOG_WARN("fail to create pre warmer", KR(tmp_ret), K(pre_warm_param));
      }
    } else if (OB_FAIL(create_pre_warmer(MEM_AND_FILE_PRE_WARM, pre_warm_param))) {
      LOG_WARN("fail to create pre warmer", KR(ret), K(pre_warm_param));
    }
#else
    ret = OB_NOT_SUPPORTED;
#endif
  }
  if (OB_SUCC(ret) && OB_NOT_NULL(pre_warmer_) && OB_FAIL(pre_warmer_->init(nullptr))) {
    LOG_WARN("fail to init pre warmer", KR(ret));
  }
  return ret;
}

int ObMacroBlockWriter::create_pre_warmer(
    const ObPreWarmerType pre_warmer_type,
    const ObPreWarmerParam &pre_warm_param)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(((ObPreWarmerType::MEM_PRE_WARM != pre_warmer_type) &&
                   (ObPreWarmerType::MEM_AND_FILE_PRE_WARM != pre_warmer_type)) ||
                  !pre_warm_param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), K(pre_warmer_type), K(pre_warm_param));
  } else if (OB_ISNULL(data_store_desc_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("data store desc should not be null", K(ret));
  } else if (ObPreWarmerType::MEM_PRE_WARM == pre_warmer_type) {
    if (data_store_desc_->is_for_index()) {
      if (OB_ISNULL(pre_warmer_ = OB_NEWx(ObIndexBlockCachePreWarmer, &allocator_))) {
        int tmp_ret = OB_ALLOCATE_MEMORY_FAILED; // use tmp_ret, allow not pre warm mem block cache
        LOG_WARN("fail to new mem pre warmer", KR(tmp_ret));
      }
    } else { // !is_for_index
      if (OB_ISNULL(pre_warmer_ = OB_NEWx(ObDataBlockCachePreWarmer, &allocator_))) {
        int tmp_ret = OB_ALLOCATE_MEMORY_FAILED; // use tmp_ret, allow not pre warm mem block cache
        LOG_WARN("fail to new mem pre warmer", KR(tmp_ret));
      }
    }
  } else if (ObPreWarmerType::MEM_AND_FILE_PRE_WARM == pre_warmer_type) {
#ifdef OB_BUILD_SHARED_STORAGE
    const ObMajorPreWarmerParam *param = static_cast<const ObMajorPreWarmerParam *>(&pre_warm_param);
    if (data_store_desc_->is_for_index()) {
      if (OB_ISNULL(pre_warmer_ = OB_NEWx(ObMajorPreWarmer<ObIndexBlockCachePreWarmer>, &allocator_,
                                          param->pre_warm_writer_.meta_writer_))) {
        ret = OB_ALLOCATE_MEMORY_FAILED; // use ret, must pre warm disk micro cache
        LOG_WARN("fail to new major pre warmer", KR(ret));
      }
    } else if (ObPreWarmerType::MEM_AND_FILE_PRE_WARM == pre_warmer_type) {
      if (OB_ISNULL(pre_warmer_ = OB_NEWx(ObMajorPreWarmer<ObDataBlockCachePreWarmer>, &allocator_,
                                          param->pre_warm_writer_.data_writer_))) {
        ret = OB_ALLOCATE_MEMORY_FAILED; // use ret, must pre warm disk micro cache
        LOG_WARN("fail to new major pre warmer", KR(ret));
      }
    }
#else
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("do not support create mem and file pre warmer", KR(ret));
#endif
  }
  return ret;
}

bool ObMacroBlockWriter::is_for_index() const
{
  return OB_NOT_NULL(data_store_desc_) && data_store_desc_->is_for_index();
}

void ObMacroBlockWriter::gen_logic_macro_id(ObLogicMacroBlockId &logic_macro_id)
{
  logic_macro_id.logic_version_ = data_store_desc_->get_logical_version();
  logic_macro_id.column_group_idx_ = data_store_desc_->get_table_cg_idx();
  logic_macro_id.data_seq_.macro_data_seq_ = macro_seq_generator_->get_current();
  logic_macro_id.tablet_id_ = data_store_desc_->get_tablet_id().id();
}

}//end namespace blocksstable
}//end namespace oceanbase
