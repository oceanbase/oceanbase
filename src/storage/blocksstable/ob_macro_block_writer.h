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

#ifndef OCEANBASE_STORAGE_BLOCKSSTABLE_MACRO_BLOCK_WRITER_H_
#define OCEANBASE_STORAGE_BLOCKSSTABLE_MACRO_BLOCK_WRITER_H_
#include "share/io/ob_io_manager.h"
#include "encoding/ob_micro_block_decoder.h"
#include "encoding/ob_micro_block_encoder.h"
#include "lib/compress/ob_compressor.h"
#include "lib/container/ob_array_wrap.h"
#include "index_block/ob_index_block_row_struct.h"
#include "index_block/ob_index_block_aggregator.h"
#include "ob_block_manager.h"
#include "ob_macro_block_checker.h"
#include "ob_macro_block_reader.h"
#include "ob_macro_block.h"
#include "ob_macro_block_struct.h"
#include "ob_micro_block_encryption.h"
#include "ob_micro_block_reader.h"
#include "ob_micro_block_writer.h"
#include "share/schema/ob_table_schema.h"
#include "ob_bloom_filter_cache.h"
#include "ob_micro_block_reader_helper.h"
#include "share/cache/ob_kvcache_pre_warmer.h"
#include "ob_macro_block_bare_iterator.h"
#include "ob_micro_block_checksum_helper.h"
#include "storage/compaction/ob_compaction_memory_context.h"
#include "storage/blocksstable/ob_macro_seq_generator.h"
#include "storage/compaction/ob_compaction_util.h"
#include "storage/compaction/ob_sstable_merge_history.h"
#include "storage/blocksstable/ob_batch_datum_rows.h"

namespace oceanbase
{
namespace compaction
{
#ifdef OB_BUILD_SHARED_STORAGE
  enum class ObSSMajorPrewarmLevel : uint8_t;
#endif
}
namespace blocksstable
{
class ObDataIndexBlockBuilder;
class ObSSTableIndexBuilder;
class ObSSTableSecMetaIterator;
class ObSSTablePrivateObjectCleaner;
struct ObIndexBlockRowDesc;
struct ObMacroBlockDesc;
class ObIMacroBlockFlushCallback;
struct ObIMacroBlockValidator;
// macro block store struct
//  |- ObMacroBlockCommonHeader
//  |- ObSSTableMacroBlockHeader
//  |- column types
//  |- column orders
//  |- column checksum
//  |- MicroBlock 1
//  |- MicroBlock 2
//  |- MicroBlock N

class ObMicroBlockBufferHelper
{
public:
  ObMicroBlockBufferHelper();
  ~ObMicroBlockBufferHelper() = default;
  int open(
      const ObDataStoreDesc &data_store_desc,
      common::ObIAllocator &allocator);
  int compress_encrypt_micro_block(ObMicroBlockDesc &micro_block_desc, const int64_t macro_seq, const int64_t micro_offset);
  int dump_micro_block_writer_buffer(const char *buf, const int64_t size);
  void reset();
private:
  int prepare_micro_block_reader(
      const char *buf,
      const int64_t size,
      ObIMicroBlockReader *&micro_reader);
  int check_micro_block_checksum(
      const char *buf,
      const int64_t size,
      const int64_t checksum/*check this checksum*/);
  int check_micro_block(
      const char *compressed_buf,
      const int64_t compressed_size,
      const char *uncompressed_buf,
      const int64_t uncompressed_size,
      const ObMicroBlockDesc &micro_block_desc/*check for this micro block*/);
  void print_micro_block_row(ObIMicroBlockReader *micro_reader);

private:
  const ObDataStoreDesc *data_store_desc_;
  int64_t micro_block_merge_verify_level_;
  ObMicroBlockCompressor compressor_;
  ObMicroBlockEncryption encryption_;
  ObMicroBlockReaderHelper check_reader_helper_;
  ObMicroBlockChecksumHelper checksum_helper_;
  blocksstable::ObDatumRow check_datum_row_;
  compaction::ObLocalArena allocator_;
};

class ObMicroBlockAdaptiveSplitter
{
struct ObMicroCompressionInfo{
  ObMicroCompressionInfo();
  virtual ~ObMicroCompressionInfo() {};
  void update(const int64_t original_size, const int64_t compressed_size);
  inline void reset() { original_size_ = compressed_size_ = 0; compression_ratio_ = 100; }
  int64_t original_size_;
  int64_t compressed_size_;
  int64_t compression_ratio_;
};

public:
  ObMicroBlockAdaptiveSplitter();
  ~ObMicroBlockAdaptiveSplitter();
  int init(const int64_t macro_store_size, const int64_t min_micro_row_count, const bool is_use_adaptive);
  void reset();
  int check_need_split(const int64_t micro_size,
                       const int64_t micro_row_count,
                       const int64_t split_size,
                       const int64_t current_macro_size,
                       const bool is_keep_space,
                       bool &check_need_split) const;
int update_compression_info(const int64_t micro_row_count, const int64_t original_size, const int64_t compressed_size);
private:
  static const int64_t DEFAULT_MICRO_ROW_COUNT = 16;
  static const int64_t MICRO_ROW_MIN_COUNT = 3;

private:
  int64_t macro_store_size_;
  int64_t min_micro_row_count_;
  bool is_use_adaptive_;
  ObMicroCompressionInfo compression_infos_[DEFAULT_MICRO_ROW_COUNT + 1]; //compression_infos_[0] for total compression info
};

class ObMacroBlockWriter
{
public:
  ObMacroBlockWriter(const bool is_need_macro_buffer = false);
  virtual ~ObMacroBlockWriter();
  virtual void reset();
  virtual int open(
      const ObDataStoreDesc &data_store_desc,
      const int64_t parallel_idx,
      const blocksstable::ObMacroSeqParam &macro_seq_param,
      const share::ObPreWarmerParam &pre_warm_param,
      ObSSTablePrivateObjectCleaner &object_cleaner,
      ObIMacroBlockFlushCallback *callback = nullptr,
      ObIMacroBlockValidator *validator = nullptr,
      ObIODevice *device_handle = nullptr);
  int init_pre_warmer(const share::ObPreWarmerParam &pre_warm_param);
  virtual int append_macro_block(const ObMacroBlockDesc &macro_desc,
                                 const ObMicroBlockData *micro_block_data);
  virtual int append_micro_block(const ObMicroBlock &micro_block, const ObMacroBlockDesc *curr_macro_desc = nullptr);
  virtual int append_row(const ObDatumRow &row, const ObMacroBlockDesc *curr_macro_desc = nullptr);
  int data_aggregator_eval(const ObBatchDatumRows &datum_rows, const int64_t start, const int64_t write_row_count);
  virtual int append_batch(const ObBatchDatumRows &datum_rows,
                           const ObMacroBlockDesc *curr_macro_desc = nullptr);
  // TODO(baichangmin): SSTableRebuilder disabled in SS mode. Finish SN route later.
  int append_macro_block(const ObDataMacroBlockMeta &macro_meta)
  {
    int ret = OB_SUCCESS;
    if (OB_UNLIKELY(nullptr == data_store_desc_)) {
      ret = OB_NOT_INIT;
      STORAGE_LOG(WARN, "The ObMacroBlockWriter has not been opened", K(ret));
    } else if (OB_FAIL(append(macro_meta, nullptr))) {
      STORAGE_LOG(WARN, "fail to append", K(ret));
    }
    return ret;
  }
  int append_micro_block(ObMicroBlockDesc &micro_block_desc, const ObMicroIndexInfo &micro_index_info);
  int append_index_micro_block(ObMicroBlockDesc &micro_block_desc);
  int get_estimate_meta_block_size(const ObDataMacroBlockMeta &macro_meta, int64_t &estimate_size);
  int check_data_macro_block_need_merge(const ObMacroBlockDesc &macro_desc, bool &need_merge);
  int check_meta_macro_block_need_rewrite(bool &need_rewrite) const;
  int close();
  void dump_block_and_writer_buffer();
  inline ObMacroBlocksWriteCtx &get_macro_block_write_ctx() { return block_write_ctx_; }
  inline int64_t get_last_macro_seq() const { return OB_ISNULL(macro_seq_generator_) ? -1 : macro_seq_generator_->get_current(); } /* save our seq num */
  TO_STRING_KV(K_(block_write_ctx));
  static int build_micro_writer(const ObDataStoreDesc *data_store_desc,
                                ObIAllocator &allocator,
                                ObIMicroBlockWriter *&micro_writer,
                                const int64_t verify_level = MICRO_BLOCK_MERGE_VERIFY_LEVEL::ENCODING_AND_COMPRESSION);
  inline int64_t get_macro_data_size() const { return macro_blocks_[current_index_].get_data_size() + micro_writer_->get_block_size(); }
  const compaction::ObMergeBlockInfo& get_merge_block_info() const { return merge_block_info_; }
  void inc_incremental_row_count() { ++merge_block_info_.incremental_row_count_; }
protected:
  virtual int build_micro_block();
  virtual int try_switch_macro_block();
  virtual bool is_keep_freespace() const {return false; }
  inline bool is_dirty() const { return macro_blocks_[current_index_].is_dirty() || 0 != micro_writer_->get_row_count(); }
  inline int64_t get_curr_micro_writer_row_count() const { return micro_writer_->get_row_count(); }

private:
  int append_row(const ObDatumRow &row, const int64_t split_size);
  int append_batch(const ObBatchDatumRows &datum_rows,
                   const int64_t split_size);
  int append(const ObDatumRow &row);
  int append(const ObBatchDatumRows &datum_rows);
  int append(const ObBatchDatumRows &datum_rows, const int64_t start, const int64_t write_row_count);
  int append(const ObDataMacroBlockMeta &macro_meta, const ObMicroBlockData *micro_block_data);
  int check_order(const ObDatumRow &row);
  int init_macro_seq_generator(const blocksstable::ObMacroSeqParam &macro_seq_param);
  int init_hash_index_builder();
  int append_row_and_hash_index(const ObDatumRow &row);
  int append_batch_to_micro_block(const ObBatchDatumRows &datum_rows, const int64_t start, const int64_t write_row_count);
  int init_pre_agg_util(const ObDataStoreDesc &data_store_desc);
  void release_pre_agg_util();
  int agg_micro_block(const ObMicroIndexInfo &micro_index_info);
  int build_micro_block_desc(
      const ObMicroBlock &micro_block,
      ObMicroBlockDesc &micro_block_desc,
      ObMicroBlockHeader &header_for_rewrite);
  int build_hash_index_block();
  int build_micro_block_desc_with_rewrite(
      const ObMicroBlock &micro_block,
      ObMicroBlockDesc &micro_block_desc,
      ObMicroBlockHeader &header);
  int build_micro_block_desc_with_reuse(const ObMicroBlock &micro_block, ObMicroBlockDesc &micro_block_desc);
  int write_micro_block(ObMicroBlockDesc &micro_block_desc);
  int check_micro_block_need_merge(const ObMicroBlock &micro_block, bool &need_merge);
  int merge_micro_block(const ObMicroBlock &micro_block);
  int flush_macro_block(ObMacroBlock &macro_block);
  int try_active_flush_macro_block();
  int wait_io_finish(ObStorageObjectHandle &macro_handle, ObMacroBlock *macro_block);
  int alloc_block();
  int alloc_block_from_device(ObStorageObjectHandle &macro_handle);
  int check_write_complete(const MacroBlockId &macro_block_id);
  int save_last_key(const ObDatumRow &row);
  int save_last_key(const ObDatumRowkey &last_key);
  int calc_micro_column_checksum(
      const int64_t column_cnt,
      ObIMicroBlockReader &reader,
      int64_t *column_checksum);
  int flush_reuse_macro_block(const ObDataMacroBlockMeta &macro_meta);
  int update_micro_commit_info(const ObDatumRow &row);
  int update_micro_commit_info(const ObBatchDatumRows &datum_rows, const int64_t start, const int64_t write_row_count);
  void dump_micro_block(ObIMicroBlockWriter &micro_writer);
  void dump_macro_block(ObMacroBlock &macro_block);
  int exec_callback(const ObStorageObjectHandle &macro_handle, ObMacroBlock *macro_block);
  void gen_logic_macro_id(ObLogicMacroBlockId &logic_macro_id);
  bool micro_index_clustered() const;
  int create_pre_warmer(const share::ObPreWarmerType pre_warmer_type,
                        const share::ObPreWarmerParam &pre_warm_param);
  bool is_for_index() const;
public:
  static const int64_t DEFAULT_MACRO_BLOCK_REWRTIE_THRESHOLD = 30;
private:
  static const int64_t DEFAULT_MINIMUM_CS_ENCODING_BLOCK_SIZE = 16 << 10; // 16KB
protected:
  const ObDataStoreDesc *data_store_desc_;
  compaction::ObMergeBlockInfo merge_block_info_;
private:
  ObIMicroBlockWriter *micro_writer_;
  ObMicroBlockReaderHelper reader_helper_;
  ObMicroBlockHashIndexBuilder hash_index_builder_;
  ObMicroBlockBufferHelper micro_helper_;
  ObMacroBlock macro_blocks_[2];
  int64_t current_index_;
  blocksstable::ObMacroSeqGenerator *macro_seq_generator_; // set by sstable layer;
  int64_t last_micro_size_;
  int64_t last_micro_expand_pct_;
  ObMacroBlocksWriteCtx block_write_ctx_;
  ObStorageObjectHandle macro_handles_[2];
  ObDatumRowkey last_key_;
  bool last_key_with_L_flag_;
  bool is_macro_or_micro_block_reused_;
  bool is_need_macro_buffer_;
  bool is_try_full_fill_macro_block_;
  int64_t *curr_micro_column_checksum_;
  compaction::ObLocalArena allocator_;
  compaction::ObLocalArena rowkey_allocator_;
  blocksstable::ObMacroBlockReader macro_reader_;
  common::ObArray<uint32_t> micro_rowkey_hashs_;
  blocksstable::ObDatumRow datum_row_;
  blocksstable::ObDatumRow *aggregated_row_;
  ObSkipIndexAggregator *data_aggregator_;
  ObIMacroBlockFlushCallback *callback_;
  ObIODevice *device_handle_;
  ObDataIndexBlockBuilder *builder_;
  ObMicroBlockAdaptiveSplitter micro_block_adaptive_splitter_;
  share::ObIPreWarmer *pre_warmer_;
  ObSSTablePrivateObjectCleaner *object_cleaner_;
  char *io_buf_;
  ObIMacroBlockValidator *validator_;
  bool is_cs_encoding_writer_;
};

struct ObIMacroBlockValidator
{
  ObIMacroBlockValidator() = default;
  virtual ~ObIMacroBlockValidator() = default;
  virtual void validate_and_dump(const blocksstable::ObMacroBlock &macro_block) = 0;
  virtual void close() = 0;
};

}//end namespace blocksstable
}//end namespace oceanbase
#endif
