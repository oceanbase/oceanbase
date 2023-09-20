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
#include "lib/compress/ob_compressor_pool.h"
#include "lib/utility/ob_tracepoint.h"
#include "share/config/ob_server_config.h"
#include "share/ob_force_print_log.h"
#include "share/ob_task_define.h"
#include "share/schema/ob_table_schema.h"
#include "storage/blocksstable/ob_index_block_builder.h"
#include "storage/blocksstable/ob_index_block_macro_iterator.h"
#include "storage/blocksstable/ob_index_block_row_struct.h"
#include "storage/blocksstable/ob_macro_block_writer.h"
#include "storage/ddl/ob_ddl_redo_log_writer.h"
#include "storage/ob_i_store.h"
#include "storage/ob_sstable_struct.h"
#include "storage/blocksstable/ob_logic_macro_id.h"

namespace oceanbase
{
using namespace common;
using namespace storage;
using namespace share::schema;
using namespace share;
namespace blocksstable
{

ObMicroBlockBufferHelper::ObMicroBlockBufferHelper()
  :data_store_desc_(nullptr),
   micro_block_merge_verify_level_(0),
   compressor_(),
   encryption_(),
   check_reader_helper_(),
   check_datum_row_(),
   allocator_("BlockBufHelper", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID())
   {}

int ObMicroBlockBufferHelper::open(
    ObDataStoreDesc &data_store_desc,
    const ObITableReadInfo &read_info,
    common::ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  reset();
  if (OB_UNLIKELY(!data_store_desc.is_valid() || !read_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid input argument.", K(ret), K(data_store_desc), K(read_info));
  } else if (OB_FAIL(compressor_.init(data_store_desc.micro_block_size_, data_store_desc.compressor_type_))) {
    STORAGE_LOG(WARN, "Fail to init micro block compressor, ", K(ret), K(data_store_desc));
#ifdef OB_BUILD_TDE_SECURITY
  } else if (OB_FAIL(encryption_.init(
      data_store_desc.encrypt_id_,
      MTL_ID(),
      data_store_desc.master_key_id_,
      data_store_desc.encrypt_key_,
      OB_MAX_TABLESPACE_ENCRYPT_KEY_LENGTH))) {
    STORAGE_LOG(WARN, "fail to init micro block encryption", K(ret), K(data_store_desc));
#endif
  } else if (OB_FAIL(check_datum_row_.init(allocator, read_info.get_request_count()))) {
    STORAGE_LOG(WARN, "Failed to init datum row", K(ret), K(read_info));
  } else if (OB_FAIL(check_reader_helper_.init(allocator))) {
    STORAGE_LOG(WARN, "Failed to init reader helper", K(ret));
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
    int64_t new_checksum = 0;
    for (int64_t it = 0; OB_SUCC(ret) && it != micro_reader->row_count(); ++it) {
      check_datum_row_.reuse();
      if (OB_FAIL(micro_reader->get_row(it, check_datum_row_))) {
        STORAGE_LOG(WARN, "get_row failed", K(ret), K(it), K(*data_store_desc_));
      } else {
        new_checksum = ObIMicroBlockWriter::cal_row_checksum(check_datum_row_, new_checksum);
      }
    }
    if (OB_SUCC(ret)) {
      if (checksum != new_checksum) {
        ret = OB_CHECKSUM_ERROR; // ignore print error code
        LOG_DBA_ERROR(OB_CHECKSUM_ERROR, "msg", "micro block checksum is not equal", K(new_checksum),
            K(checksum), K(ret), KPC(data_store_desc_));
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
    int64_t new_checksum = 0;
    for (int64_t it = 0; OB_SUCC(ret) && it != micro_reader->row_count(); ++it) {
      check_datum_row_.reuse();
      if (OB_FAIL(micro_reader->get_row(it, check_datum_row_))) {
        STORAGE_LOG(WARN, "get_row failed", K(ret), K(it), K(*data_store_desc_));
      } else {
        new_checksum = ObIMicroBlockWriter::cal_row_checksum(check_datum_row_, new_checksum);
        FLOG_WARN("error micro block row", K(it), K_(check_datum_row), K(new_checksum));
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
    is_use_adaptive_(false)
{}

ObMicroBlockAdaptiveSplitter::~ObMicroBlockAdaptiveSplitter()
{
}

int ObMicroBlockAdaptiveSplitter::init(const int64_t macro_store_size, const bool is_use_adaptive)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(macro_store_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid micro block adaptive split input argument", K(ret), K(macro_store_size));
  } else {
    reset();
    macro_store_size_ = macro_store_size;
    is_use_adaptive_ = is_use_adaptive;
  }

  return ret;
}

void ObMicroBlockAdaptiveSplitter::reset()
{
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
    const int64_t adaptive_row_count = MAX(MICRO_ROW_MIN_COUNT, DEFAULT_MICRO_ROW_COUNT - (micro_size - split_size) / split_size);
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
ObMacroBlockWriter::ObMacroBlockWriter()
  :data_store_desc_(nullptr),
   micro_writer_(nullptr),
   reader_helper_(),
   hash_index_builder_(),
   micro_helper_(),
   read_info_(),
   current_index_(0),
   current_macro_seq_(0),
   block_write_ctx_(),
   last_key_(),
   last_key_with_L_flag_(false),
   is_macro_or_micro_block_reused_(false),
   curr_micro_column_checksum_(NULL),
   allocator_("MaBlkWriter", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()),
   rowkey_allocator_("MaBlkWriter", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()),
   macro_reader_(),
   micro_rowkey_hashs_(),
   lock_(common::ObLatchIds::MACRO_WRITER_LOCK),
   datum_row_(),
   check_datum_row_(),
   callback_(nullptr),
   builder_(NULL),
   data_block_pre_warmer_()
{
  //macro_blocks_, macro_handles_
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
  read_info_.reset();
  macro_blocks_[0].reset();
  macro_blocks_[1].reset();
  bf_cache_writer_[0].reset();
  bf_cache_writer_[1].reset();
  current_index_ = 0;
  current_macro_seq_ = 0;
  block_write_ctx_.reset();
  macro_handles_[0].reset();
  macro_handles_[1].reset();
  last_key_.reset();
  last_key_with_L_flag_ = false;
  is_macro_or_micro_block_reused_ = false;
  micro_rowkey_hashs_.reset();
  datum_row_.reset();
  check_datum_row_.reset();
  if (OB_NOT_NULL(builder_)) {
    builder_->~ObDataIndexBlockBuilder();
    builder_ = nullptr;
  }
  micro_block_adaptive_splitter_.reset();
  allocator_.reset();
  rowkey_allocator_.reset();
  data_block_pre_warmer_.reset();
}


int ObMacroBlockWriter::open(
    ObDataStoreDesc &data_store_desc,
    const ObMacroDataSeq &start_seq,
    ObIMacroBlockFlushCallback *callback)
{
  int ret = OB_SUCCESS;
  reset();
  if (OB_UNLIKELY(!data_store_desc.is_valid() || !start_seq.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid macro block writer input argument.", K(ret), K(data_store_desc), K(start_seq));
  } else if (OB_FAIL(macro_blocks_[0].init(data_store_desc, start_seq.get_data_seq()))) {
    STORAGE_LOG(WARN, "Fail to init 0th macro block, ", K(ret));
  } else if (OB_FAIL(macro_blocks_[1].init(data_store_desc, start_seq.get_data_seq() + 1))) {
    STORAGE_LOG(WARN, "Fail to init 1th macro block, ", K(ret));
  } else {
    STORAGE_LOG(DEBUG, "open macro block writer: ", K(data_store_desc), K(start_seq));
    ObSSTableIndexBuilder *sstable_index_builder = data_store_desc.sstable_index_builder_;
    callback_ = callback;
    data_store_desc_ = &data_store_desc;
    current_macro_seq_ = start_seq.get_data_seq();
    if (OB_FAIL(init_hash_index_builder())) {
      STORAGE_LOG(WARN, "Failed to build hash_index builder", K(ret));
    } else if (OB_FAIL(build_micro_writer(data_store_desc_,
                                          allocator_,
                                          micro_writer_,
                                          GCONF.micro_block_merge_verify_level))) {
      STORAGE_LOG(WARN, "fail to build micro writer", K(ret));
    } else if (OB_FAIL(read_info_.init(allocator_, data_store_desc))) {
      STORAGE_LOG(WARN, "failed to init read info", K(data_store_desc), K(ret));
    } else if (OB_FAIL(datum_row_.init(allocator_, read_info_.get_request_count()))) {
      STORAGE_LOG(WARN, "Failed to init datum row", K(ret), K_(read_info));
    } else if (OB_FAIL(micro_helper_.open(data_store_desc, read_info_, allocator_))) {
      STORAGE_LOG(WARN, "Failed to open micro helper", K(ret), K_(read_info));
    } else if (OB_FAIL(check_datum_row_.init(allocator_, read_info_.get_request_count()))) {
      STORAGE_LOG(WARN, "Failed to init datum row", K(ret), K_(read_info));
    } else if (OB_FAIL(reader_helper_.init(allocator_))) {
      STORAGE_LOG(WARN, "Failed to init reader helper", K(ret));
    } else {
      //TODO  use 4.1.0.0 for version judgment
      const bool is_use_adaptive = !data_store_desc_->is_major_merge()
       || data_store_desc_->major_working_cluster_version_ >= DATA_VERSION_4_1_0_0;
      if (OB_FAIL(micro_block_adaptive_splitter_.init(data_store_desc.macro_store_size_, is_use_adaptive))) {
        STORAGE_LOG(WARN, "Failed to init micro block adaptive split", K(ret), K(data_store_desc.macro_store_size_));
      }
    }
    if (OB_SUCC(ret) && data_store_desc_->is_major_merge()) {
      if (OB_ISNULL(curr_micro_column_checksum_ = static_cast<int64_t *>(
          allocator_.alloc(sizeof(int64_t) * data_store_desc_->row_column_count_)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        STORAGE_LOG(WARN, "fail to allocate memory for curr micro block column checksum", K(ret));
      } else {
       MEMSET(curr_micro_column_checksum_, 0,
           sizeof(int64_t) * data_store_desc_->row_column_count_);
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_NOT_NULL(sstable_index_builder)) {
      if (OB_FAIL(sstable_index_builder->new_index_builder(builder_, data_store_desc, allocator_))) {
        STORAGE_LOG(WARN, "fail to alloc index builder", K(ret));
      } else if (data_store_desc.need_pre_warm_) {
        data_block_pre_warmer_.init();
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
  if (OB_FAIL(append_row(row, data_store_desc_->micro_block_size_))) {
    STORAGE_LOG(WARN, "Fail to append row", K(ret));
  } else if (nullptr != data_store_desc_->merge_info_) {
    ++data_store_desc_->merge_info_->incremental_row_count_;
    STORAGE_LOG(DEBUG, "Success to append row, ", K(data_store_desc_->tablet_id_), K(row));
  }
  return ret;
}

int ObMacroBlockWriter::append_row(const ObDatumRow &row, const int64_t split_size)
{
  int ret = OB_SUCCESS;
  const ObDatumRow *row_to_append = &row;
  bool is_need_set_micro_upper_bound = false;
  int64_t estimate_remain_size = 0;
  if (nullptr == data_store_desc_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "The ObMacroBlockWriter has not been opened, ", K(ret), KP(data_store_desc_));
  } else if (split_size < data_store_desc_->micro_block_size_) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid split_size", K(ret), K(split_size));
  } else if (OB_FAIL(check_order(row))) {
    STORAGE_LOG(WARN, "macro block writer fail to check order.", K(row));
  }
  if (OB_SUCC(ret)) {
    is_macro_or_micro_block_reused_ = false;
    const ObStorageDatumUtils &datum_utils = read_info_.get_datum_utils();
    if (OB_FAIL(append_row_and_hash_index(*row_to_append))) {
      if (OB_BUF_NOT_ENOUGH == ret) {
        if (0 == micro_writer_->get_row_count()) {
          ret = OB_NOT_SUPPORTED;
          STORAGE_LOG(ERROR, "The single row is too large, ", K(ret), K(row));
        } else if (OB_FAIL(build_micro_block())) {
          STORAGE_LOG(WARN, "Fail to build micro block, ", K(ret));
        } else if (OB_FAIL(OB_FAIL(append_row_and_hash_index(*row_to_append)))) {
          STORAGE_LOG(ERROR, "Fail to append row to micro block, ", K(ret), K(row));
        } else if (OB_FAIL(save_last_key(*row_to_append))) {
          STORAGE_LOG(WARN, "Fail to save last key, ", K(ret), K(row));
        }
        if (OB_SUCC(ret) && data_store_desc_->need_prebuild_bloomfilter_) {
          ObDatumRowkey rowkey;
          uint64_t hash = 0;
          if (OB_FAIL(rowkey.assign(row_to_append->storage_datums_, data_store_desc_->bloomfilter_rowkey_prefix_))) {
            STORAGE_LOG(WARN, "Failed to assign rowkey", K(ret), KPC(row_to_append));
          } else if (OB_FAIL(rowkey.murmurhash(0, datum_utils, hash))) {
            STORAGE_LOG(WARN, "Failed to calc rowkey hash", K(ret), K(rowkey), K(datum_utils));
          } else if (OB_FAIL(micro_rowkey_hashs_.push_back(static_cast<uint32_t>(hash)))) {
            STORAGE_LOG(WARN, "Fail to put rowkey hash to array ", K(ret), K(rowkey));
            micro_rowkey_hashs_.reuse();
            ret = OB_SUCCESS;
          }
        }
      } else {
        STORAGE_LOG(WARN, "Fail to append row to micro block, ", K(ret), K(row));
      }
    } else {
      bool is_split = false;
      if (data_store_desc_->need_prebuild_bloomfilter_) {
        ObDatumRowkey rowkey;
        uint64_t hash = 0;
        if (OB_FAIL(rowkey.assign(row_to_append->storage_datums_, data_store_desc_->bloomfilter_rowkey_prefix_))) {
          STORAGE_LOG(WARN, "Failed to assign rowkey", K(ret), KPC(row_to_append));
        } else if (OB_FAIL(rowkey.murmurhash(0, datum_utils, hash))) {
          STORAGE_LOG(WARN, "Failed to calc rowkey hash", K(ret), K(rowkey), K(datum_utils));
        } else if (OB_FAIL(micro_rowkey_hashs_.push_back(static_cast<uint32_t>(hash)))) {
          STORAGE_LOG(WARN, "Fail to put rowkey hash to array ", K(ret), K(rowkey));
          micro_rowkey_hashs_.reuse();
          ret = OB_SUCCESS;
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(save_last_key(*row_to_append))) {
        STORAGE_LOG(WARN, "Fail to save last key, ", K(ret), K(row));
      } else if (OB_FAIL(micro_block_adaptive_splitter_.check_need_split(micro_writer_->get_block_size(), micro_writer_->get_row_count(),
            split_size, macro_blocks_[current_index_].get_data_size(), is_keep_freespace(), is_split))) {
        STORAGE_LOG(WARN, "Failed to check need split", K(ret), KPC(micro_writer_));
      } else if (is_split && OB_FAIL(build_micro_block())) {
        STORAGE_LOG(WARN, "Fail to build micro block, ", K(ret));
      }
    }
  }
  return ret;
}

int ObMacroBlockWriter::append_macro_block(const ObMacroBlockDesc &macro_desc)
{
  int ret = OB_SUCCESS;
  const ObDataMacroBlockMeta *data_block_meta = macro_desc.macro_meta_;

  if (micro_writer_->get_row_count() > 0 && OB_FAIL(build_micro_block())) {
    LOG_WARN("Fail to build current micro block", K(ret));
  }

  if (OB_FAIL(ret)) {
    // skip
  } else if (OB_FAIL(try_switch_macro_block())) {
    LOG_WARN("Fail to flush and switch macro block", K(ret));
  } else if (OB_UNLIKELY(!macro_desc.is_valid_with_macro_meta())
      || OB_ISNULL(builder_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid arguments", K(ret), KP(builder_), K(macro_desc));
  } else if (OB_FAIL(builder_->append_macro_block(macro_desc))) {
    LOG_WARN("Fail to append index block rows", K(ret), KP(builder_), K(macro_desc));
  } else {
    ObDatumRowkey rowkey;
    if (OB_FAIL(flush_reuse_macro_block(*data_block_meta))) {
      LOG_WARN("Fail to flush reuse macro block", K(ret), KPC(data_block_meta));
    } else if (OB_FAIL(data_block_meta->get_rowkey(rowkey))) {
      LOG_WARN("Fail to assign rowkey", K(ret), KPC(data_block_meta));
    } else if (rowkey.get_datum_cnt() != data_store_desc_->rowkey_column_count_) {
      ret = OB_ERR_SYS;
      LOG_ERROR("Rowkey column not match, can not reuse macro block", K(ret),
          "reused merge info rowkey count", rowkey.get_datum_cnt(),
          "data store descriptor rowkey count", data_store_desc_->rowkey_column_count_);
    } else if (OB_FAIL(save_last_key(rowkey))) {
      LOG_WARN("Fail to copy last key", K(ret), K(rowkey));
    }

    if (OB_SUCC(ret)) {
      is_macro_or_micro_block_reused_ = true;
      last_key_with_L_flag_ = false; // clear flag
      if (nullptr != data_store_desc_->merge_info_) {
        data_store_desc_->merge_info_->multiplexed_macro_block_count_++;
        data_store_desc_->merge_info_->macro_block_count_++;
        data_store_desc_->merge_info_->total_row_count_ += macro_desc.row_count_;
        data_store_desc_->merge_info_->occupy_size_
            += static_cast<const ObDataMacroBlockMeta *>(data_block_meta)->val_.occupy_size_;
      }
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
    STORAGE_LOG(WARN, "check_micro_block_need_merge failed", K(micro_block), K(ret));
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
      } else if (OB_FAIL(write_micro_block(micro_block_desc))) {
        STORAGE_LOG(WARN, "Failed to write micro block, ", K(ret), K(micro_block_desc));
      } else if (NULL != data_store_desc_->merge_info_) {
        data_store_desc_->merge_info_->multiplexed_micro_count_in_new_macro_++;
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
    is_macro_or_micro_block_reused_ = true;
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
     < data_store_desc_->macro_block_size_ * DEFAULT_MACRO_BLOCK_REWRTIE_THRESHOLD / 100) {
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
      if (OB_SUCC(ret) && data_store_desc_->need_prebuild_bloomfilter_) {
        flush_bf_to_cache(current_bf_writer, row_count);
      }
    }
    if (OB_SUCC(ret)) {
      // wait last macro block io finish
      // we also need wait prev macro block to finish due to force_split in build_micro_block
      ObMacroBlockHandle &curr_handle = macro_handles_[current_index_];
      ObMacroBlockHandle &prev_handle = macro_handles_[(current_index_ + 1) % 2];
      if (OB_NOT_NULL(callback_) && OB_FAIL(callback_->wait())) {
        STORAGE_LOG(WARN, "fail to wait callback flush", K(ret));
      } else if (OB_FAIL(wait_io_finish(prev_handle))) {
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
        data_store_desc_->schema_rowkey_col_cnt_,
        data_store_desc_->rowkey_column_count_ != data_store_desc_->schema_rowkey_col_cnt_);
  const int64_t sql_sequence_col_idx =
    ObMultiVersionRowkeyHelpper::get_sql_sequence_col_store_index(
        data_store_desc_->schema_rowkey_col_cnt_,
        data_store_desc_->rowkey_column_count_ != data_store_desc_->schema_rowkey_col_cnt_);
  int64_t cur_row_version = 0;
  int64_t cur_sql_sequence = 0;
  if (!row.is_valid() || row.get_column_count() != data_store_desc_->row_column_count_) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(ERROR, "invalid macro block writer input argument.",
        K(row), "row_column_count", data_store_desc_->row_column_count_, K(ret));
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
    } else if (nullptr != data_store_desc_->merge_info_ &&
               MAJOR_MERGE == data_store_desc_->merge_info_->merge_type_ &&
               -cur_row_version > data_store_desc_->snapshot_version_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("Unexpected current row trans version in major merge", K(ret), K(row), K(data_store_desc_->snapshot_version_));
    } else if (!row.mvcc_row_flag_.is_uncommitted_row()) { // update max commit version
      micro_writer_->update_max_merged_trans_version(-cur_row_version);
      if (!row.mvcc_row_flag_.is_shadow_row()) {
        const_cast<ObDatumRow&>(row).storage_datums_[sql_sequence_col_idx].reuse(); // make sql sequence positive
        const_cast<ObDatumRow&>(row).storage_datums_[sql_sequence_col_idx].set_int(0); // make sql sequence positive
      } else if (OB_UNLIKELY(row.storage_datums_[sql_sequence_col_idx].get_int() != -INT64_MAX)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Unexpected shadow row", K(ret), K(row));
      }
    } else { // not committed
      micro_writer_->set_contain_uncommitted_row();
      LOG_TRACE("meet uncommited trans row", K(row));
    }
    if (OB_SUCC(ret) && last_key_.is_valid()) {
      ObDatumRowkey cur_key;
      ObDatumRowkey last_key;
      int32_t compare_result = 0;
      const ObStorageDatumUtils &datum_utils = read_info_.get_datum_utils();

      if (OB_FAIL(cur_key.assign(row.storage_datums_, data_store_desc_->schema_rowkey_col_cnt_))) {
        STORAGE_LOG(WARN, "Failed to assign cur key", K(ret));
      } else if (OB_FAIL(last_key.assign(last_key_.datums_, data_store_desc_->schema_rowkey_col_cnt_))) {
        STORAGE_LOG(WARN, "Failed to assign last key", K(ret));
      } else if (OB_FAIL(cur_key.compare(last_key, datum_utils, compare_result))) {
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
              if (data_store_desc_->is_ddl_) {
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
        if (nullptr != data_store_desc_->merge_info_
            && !is_major_merge_type(data_store_desc_->merge_info_->merge_type_)
            && !is_meta_major_merge(data_store_desc_->merge_info_->merge_type_)
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

int ObMacroBlockWriter::init_hash_index_builder()
{
  int ret = OB_SUCCESS;
  if (data_store_desc_->need_build_hash_index_for_micro_block_
        && OB_FAIL(hash_index_builder_.init(data_store_desc_))) {
    STORAGE_LOG(WARN, "Failed to build hash_index builder", K(ret));
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
    if (OB_UNLIKELY(FLAT_ROW_STORE != data_store_desc_->row_store_type_)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "Unexpected row store type", K(ret), K(data_store_desc_->row_store_type_));
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
  STORAGE_LOG(DEBUG, "build micro block desc index", K(data_store_desc_->tablet_id_), K(micro_block_desc), "lbt", lbt(), K(ret));
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
  } else if (OB_FAIL(micro_writer_->build_micro_block_desc(micro_block_desc))) {
    STORAGE_LOG(WARN, "failed to build micro block desc", K(ret));
  } else if (OB_FAIL(build_hash_index_block(micro_block_desc))) {
    STORAGE_LOG(WARN, "Failed to build hash index block", K(ret));
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

  if (OB_SUCC(ret)) {
    micro_writer_->reuse();
    if (data_store_desc_->need_build_hash_index_for_micro_block_) {
      hash_index_builder_.reuse();
    }
    if (data_store_desc_->need_prebuild_bloomfilter_ && micro_rowkey_hashs_.count() > 0) {
      micro_rowkey_hashs_.reuse();
    }
    if (OB_NOT_NULL(data_store_desc_->merge_info_)) {
      data_store_desc_->merge_info_->original_size_ += block_size;
      data_store_desc_->merge_info_->compressed_size_ += micro_block_desc.buf_size_;
      data_store_desc_->merge_info_->new_micro_count_in_new_macro_++;
    }
  }
  STORAGE_LOG(DEBUG, "build micro block desc", K(data_store_desc_->tablet_id_), K(micro_block_desc), "lbt", lbt(),
                                               K(ret), K(tmp_ret));
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
      && micro_block.micro_index_info_->row_header_->get_schema_version() == data_store_desc_->schema_version_) {
    if (OB_FAIL(build_micro_block_desc_with_reuse(micro_block, micro_block_desc))) {
      LOG_WARN("fail to build micro block desc v3", K(ret), K(micro_block), K(micro_block_desc));
    }
  } else if (OB_FAIL(build_micro_block_desc_with_rewrite(micro_block, micro_block_desc, header_for_rewrite))) {
    LOG_WARN("fail to build micro block desc v2", K(ret), K(micro_block), K(micro_block_desc));
  }
  STORAGE_LOG(DEBUG, "build micro block desc", K(micro_block), K(micro_block_desc));
  return ret;
}

int ObMacroBlockWriter::build_hash_index_block(ObMicroBlockDesc &micro_block_desc)
{
  int ret = OB_SUCCESS;
  if (hash_index_builder_.is_valid()) {
    if (OB_UNLIKELY(FLAT_ROW_STORE != data_store_desc_->row_store_type_)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "Unexpected row store type", K(ret), K(data_store_desc_->row_store_type_));
    } else if (OB_FAIL(micro_writer_->append_hash_index(hash_index_builder_))) {
      if (ret != OB_NOT_SUPPORTED) {
        LOG_WARN("Failed to append hash index to micro block writer", K(ret));
      } else {
        ret = OB_SUCCESS;
      }
      hash_index_builder_.reset();
    } else {
      const int64_t hash_index_size = hash_index_builder_.estimate_size();
      micro_block_desc.buf_size_ += hash_index_size;
      micro_block_desc.data_size_ += hash_index_size;
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
  STORAGE_LOG(DEBUG, "build micro block desc reuse", K(data_store_desc_->tablet_id_), K(micro_block_desc), "lbt", lbt(), K(ret));
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
      header.column_count_ = data_store_desc_->row_column_count_;
      header.has_column_checksum_ = data_store_desc_->is_major_merge();
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
        MEMSET(curr_micro_column_checksum_, 0, sizeof(int64_t) * data_store_desc_->row_column_count_);
        if (OB_FAIL(calc_micro_column_checksum(header.column_count_, *reader, curr_micro_column_checksum_))) {
          STORAGE_LOG(WARN, "fail to calc micro block column checksum", K(ret));
        }
      }
    }
  }
  STORAGE_LOG(DEBUG, "build micro block desc rewrite", K(data_store_desc_->tablet_id_), K(micro_block_desc), "lbt", lbt(), K(ret));
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
    if (OB_FAIL(builder_->append_row(micro_block_desc, macro_blocks_[current_index_]))) {
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

  // TODO(zhuixin.gsy): ensure bloomfilter correct for index micro block
  if (OB_SUCC(ret)) {
    if (data_store_desc_->need_prebuild_bloomfilter_) {
      //if macro has micro reuse, don't build bloomfilter
      ObMacroBloomFilterCacheWriter &current_writer = bf_cache_writer_[current_index_];
      if (!current_writer.is_valid()) {
        //Estimate macroblock rowcount base on micro_block_desc
        const int64_t estimate_row_count = data_store_desc_->macro_block_size_
            / (micro_block_desc.buf_size_ + 1)
            * micro_block_desc.row_count_;
        if (OB_FAIL(open_bf_cache_writer(*data_store_desc_, estimate_row_count))) {
          STORAGE_LOG(WARN, "Failed to open bloomfilter cache writer, ", K(ret));
          ret = OB_SUCCESS;
        }
      }
      if (micro_rowkey_hashs_.count() != micro_block_desc.row_count_) {
        //count=0 ,when micro block reused
        if(OB_UNLIKELY(micro_rowkey_hashs_.count() > 0)) {
          STORAGE_LOG(WARN,"build bloomfilter: micro_rowkey_hashs_ and micro_block_desc count not same ",
                      K(micro_rowkey_hashs_.count()),
                      K(micro_block_desc.row_count_));
        }
        current_writer.set_not_need_build();
      } else if (current_writer.is_need_build()
                 && OB_LIKELY(current_writer.get_rowkey_column_count() == data_store_desc_->bloomfilter_rowkey_prefix_)
                 && OB_FAIL(current_writer.append(micro_rowkey_hashs_))) {
        STORAGE_LOG(WARN, "Fail to append rowkey hash to macro block, ", K(ret));
        current_writer.set_not_need_build();
        ret = OB_SUCCESS;
      }
      micro_rowkey_hashs_.reuse();
    }
  }

  return ret;
}

int ObMacroBlockWriter::flush_macro_block(ObMacroBlock &macro_block)
{
  int ret = OB_SUCCESS;
  ObLogicMacroBlockId cur_logic_id; // TODO(zhuixin.gsy) rm this if DDL Rebuild Ready
  cur_logic_id.logic_version_ = data_store_desc_->get_logical_version();
  cur_logic_id.data_seq_ = current_macro_seq_;
  cur_logic_id.tablet_id_ = data_store_desc_->tablet_id_.id();

  ObMacroBlockHandle &macro_handle = macro_handles_[current_index_];
  ObMacroBlockHandle &prev_handle = macro_handles_[(current_index_ + 1) % 2];

  if (OB_UNLIKELY(!macro_block.is_dirty())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "empty macro block has no pre-alloc macro id", K(ret), K(current_index_));;
  } else if (OB_NOT_NULL(callback_) && OB_FAIL(callback_->wait())) {
    STORAGE_LOG(WARN, "fail to wait callback flush", K(ret));
  } else if (OB_FAIL(wait_io_finish(prev_handle))) {
    STORAGE_LOG(WARN, "Fail to wait io finish, ", K(ret));
  } else if (OB_NOT_NULL(builder_)
      && OB_FAIL(builder_->generate_macro_row(macro_block, macro_handle.get_macro_id()))) {
    STORAGE_LOG(WARN, "fail to generate macro row", K(ret), K_(current_macro_seq));
  } else if (OB_FAIL(macro_block.flush(macro_handle, block_write_ctx_))) {
    STORAGE_LOG(WARN, "macro block writer fail to flush macro block.", K(ret));
  } else if (OB_NOT_NULL(callback_) && OB_FAIL(callback_->write(macro_handle,
                                                                cur_logic_id,
                                                                macro_block.get_data_buf(),
                                                                upper_align(macro_block.get_data_size(), DIO_ALIGN_SIZE),
                                                                current_macro_seq_))) {
    STORAGE_LOG(WARN, "fail to do callback flush", K(ret));
  }
  if (OB_SUCC(ret)) {
    if (nullptr != callback_) {
      DEBUG_SYNC(AFTER_DDL_WRITE_MACRO_BLOCK);
    }
    ++current_macro_seq_;
    if (OB_FAIL(macro_block.init(*data_store_desc_, current_macro_seq_ + 1))) {
      STORAGE_LOG(WARN, "macro block writer fail to init.", K(ret));
    }
  }
  return ret;
}

int ObMacroBlockWriter::flush_reuse_macro_block(const ObDataMacroBlockMeta &macro_meta)
{
  int ret = OB_SUCCESS;
  const MacroBlockId &macro_id = macro_meta.get_macro_id();
  ObMacroBlockHandle &prev_handle = macro_handles_[(current_index_ + 1) % 2];

  if (OB_FAIL(wait_io_finish(prev_handle))) {
    LOG_WARN("Fail to wait io finish", K(ret));
  } else if (OB_FAIL(block_write_ctx_.add_macro_block_id(macro_id))) {
    LOG_WARN("failed to add macro block meta", K(ret), K(macro_id));
  } else {
    block_write_ctx_.increment_old_block_count();
    FLOG_INFO("Async reuse macro block", K(macro_meta.end_key_), "macro_block_id", macro_id);
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
    } else if (data_store_desc_->need_prebuild_bloomfilter_
        && OB_FAIL(flush_bf_to_cache(bf_cache_writer_[current_index_], row_count))) {
      LOG_WARN("Fail to flush bloom filter to cache", K(ret), K(row_count));
    } else {
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
  const int64_t io_timeout_ms = std::max(GCONF._data_storage_io_timeout / 1000, DEFAULT_IO_WAIT_TIME_MS);
  ObMacroBlockHandle read_handle;
  if (OB_FAIL(ObBlockManager::async_read_block(read_info, read_handle))) {
    STORAGE_LOG(WARN, "fail to async read macro block", K(ret), K(read_info));
  } else if (OB_FAIL(read_handle.wait(io_timeout_ms))) {
    STORAGE_LOG(WARN, "fail to wait io finish", K(ret), K(io_timeout_ms));
  } else if (OB_FAIL(macro_block_checker_.check(
      read_handle.get_buffer(),
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
  const int64_t io_timeout_ms = std::max(GCONF._data_storage_io_timeout / 1000, DEFAULT_IO_WAIT_TIME_MS);
  if (OB_FAIL(macro_handle.wait(io_timeout_ms))) {
    STORAGE_LOG(WARN, "macro block writer fail to wait io finish", K(ret), K(io_timeout_ms));
  } else {
    if (!macro_handle.is_empty()) {
      FLOG_INFO("wait io finish", K(macro_handle.get_macro_id()));
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
    if (micro_writer_->get_row_count() <= 0
        && micro_block.header_.data_length_ > data_store_desc_->micro_block_size_ / 2) {
      need_merge = false;
    } else if (micro_writer_->get_block_size() > data_store_desc_->micro_block_size_ / 2
        && micro_block.header_.data_length_ > data_store_desc_->micro_block_size_ / 2) {
      need_merge = false;
    } else {
      need_merge = true;
    }
    STORAGE_LOG(DEBUG, "check micro block need merge", K(micro_writer_->get_row_count()), K(micro_block.data_.get_buf_size()),
        K(micro_writer_->get_block_size()), K(data_store_desc_->micro_block_size_), K(need_merge));
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
  } else if (OB_UNLIKELY(!data_store_desc_->is_major_merge())) {
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
    if (merged_size > 2 * data_store_desc_->micro_block_size_) {
      split_size = merged_size / 2;
    } else {
      split_size = 2 * data_store_desc_->micro_block_size_;
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

      if (OB_SUCC(ret) && micro_writer_->get_block_size() >= data_store_desc_->micro_block_size_) {
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
  if (OB_FAIL(rowkey.assign(row.storage_datums_, data_store_desc_->rowkey_column_count_))) {
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
  if (OB_FAIL(last_key.deep_copy(last_key_, rowkey_allocator_))) {
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

int ObMacroBlockWriter::build_micro_writer(ObDataStoreDesc *data_store_desc,
                                           ObIAllocator &allocator,
                                           ObIMicroBlockWriter *&micro_writer,
                                           const int64_t verify_level)
{
  int ret = OB_SUCCESS;
  void *buf = nullptr;
  ObMicroBlockEncoder *encoding_writer = nullptr;
  ObMicroBlockWriter *flat_writer = nullptr;
  if (data_store_desc->encoding_enabled()) {
    ObMicroBlockEncodingCtx encoding_ctx;
    encoding_ctx.macro_block_size_ = data_store_desc->macro_block_size_;
    encoding_ctx.micro_block_size_ = data_store_desc->micro_block_size_;
    encoding_ctx.column_cnt_ = data_store_desc->row_column_count_;
    encoding_ctx.rowkey_column_cnt_ = data_store_desc->rowkey_column_count_;
    encoding_ctx.col_descs_ = &data_store_desc->get_full_stored_col_descs();
    encoding_ctx.encoder_opt_ = data_store_desc->encoder_opt_;
    encoding_ctx.column_encodings_ = nullptr;
    encoding_ctx.major_working_cluster_version_ = data_store_desc->major_working_cluster_version_;
    encoding_ctx.row_store_type_ = data_store_desc->row_store_type_;
    encoding_ctx.need_calc_column_chksum_ = data_store_desc->is_major_merge();
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
  } else {
    if (OB_ISNULL(buf = allocator.alloc(sizeof(ObMicroBlockWriter)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "fail to alloc memory", K(ret));
    } else if (OB_ISNULL(flat_writer = new (buf) ObMicroBlockWriter())) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "fail to new encoding writer", K(ret));
    } else if (OB_FAIL(flat_writer->init(
        data_store_desc->micro_block_size_limit_,
        data_store_desc->rowkey_column_count_,
        data_store_desc->row_column_count_,
        &data_store_desc->get_rowkey_col_descs(),
        data_store_desc->is_major_merge()))) {
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
  if (OB_UNLIKELY(!desc.need_prebuild_bloomfilter_
                 || 0 == bloomfilter_size
                 || 0 >= desc.bloomfilter_rowkey_prefix_
                 || desc.bloomfilter_rowkey_prefix_ > desc.schema_rowkey_col_cnt_)) {
    //do nothing
  } else if (OB_FAIL(bf_cache_writer_[0].init(desc.bloomfilter_rowkey_prefix_, bloomfilter_size))) {
    STORAGE_LOG(WARN, "Fail to init 0th bf_merge_writer, ", K(ret), K(bloomfilter_size));
  } else if (OB_FAIL(bf_cache_writer_[1].init(desc.bloomfilter_rowkey_prefix_, bloomfilter_size))) {
    STORAGE_LOG(WARN, "Fail to init 1th bf_merge_writer, ", K(ret), K(bloomfilter_size));
  }
  if (OB_FAIL(ret)) {
    bf_cache_writer_[0].reset();
    bf_cache_writer_[1].reset();
  }
  return ret;
}

int ObMacroBlockWriter::flush_bf_to_cache(ObMacroBloomFilterCacheWriter &bf_writer, const int32_t row_count)
{
  int ret = OB_SUCCESS;
  ObMacroBlockHandle &macro_handle = macro_handles_[current_index_];
  if (OB_LIKELY(data_store_desc_->need_prebuild_bloomfilter_)) {
    if (!bf_writer.is_valid()) {
      if (OB_FAIL(open_bf_cache_writer(*data_store_desc_, row_count))) {
        STORAGE_LOG(WARN, "Failed to open bloomfilter cache writer, ", K(ret));
      } else {
        //bloomfilter size use the first macro block row count, and become effective in next macroblock
        bf_cache_writer_[0].set_not_need_build();
        bf_cache_writer_[1].set_not_need_build();
      }
    } else if (bf_writer.is_need_build() && bf_writer.get_row_count() == row_count) {
      if (OB_FAIL(bf_writer.flush_to_cache(MTL_ID(), macro_handle.get_macro_id()))) {
        STORAGE_LOG(WARN, "bloomfilter cache writer failed flush to cache, ", K(ret), K(bf_writer));
      } else if (OB_NOT_NULL(data_store_desc_->merge_info_)) {
        data_store_desc_->merge_info_->macro_bloomfilter_count_++;
      }
    }
    bf_writer.reuse();
  }
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
          data_store_desc_->row_column_count_, data_store_desc_->rowkey_column_count_, data_store_desc_->get_fixed_header_version());
    const char *data_buf = macro_block.get_data_buf() + data_offset;
    const int64_t data_size = macro_block.get_data_size() - data_offset;
    int64_t pos = 0;
    ObMicroBlockDesMeta micro_des_meta(
        data_store_desc_->compressor_type_, data_store_desc_->encrypt_id_,
        data_store_desc_->master_key_id_, data_store_desc_->encrypt_key_);
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


}//end namespace blocksstable
}//end namespace oceanbase
