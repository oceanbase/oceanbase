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
#include "ob_micro_block_writer.h"
#include "ob_micro_block_index_writer.h"
#include "ob_micro_block_reader.h"
#include "lib/compress/ob_compressor.h"
#include "lib/io/ob_io_manager.h"
#include "lib/container/ob_array_wrap.h"
#include "share/schema/ob_table_schema.h"
#include "ob_row_reader.h"
#include "ob_column_map.h"
#include "ob_micro_block_reader.h"
#include "storage/ob_multi_version_col_desc_generate.h"
#include "ob_store_file.h"
#include "storage/blocksstable/ob_block_mark_deletion_maker.h"
#include "storage/blocksstable/ob_macro_block.h"
#include "storage/blocksstable/ob_lob_merge_writer.h"
#include "storage/blocksstable/ob_store_file_system.h"
#include "storage/blocksstable/ob_macro_block_reader.h"
#include "storage/blocksstable/ob_sparse_micro_block_reader.h"
#include "storage/ob_pg_mgr.h"
#include "ob_block_index_intermediate.h"

namespace oceanbase {
namespace blocksstable {

// macro block store struct
//  |- ObCommonHeader
//  |- ObSSTableMacroBlockHeader
//  |- column id list
//  |- column type list
//  |- column checksum
//  |- MicroBlock 1
//  |- MicroBlock 2
//  |- MicroBlock N
//  |- MicroBlock index
//  |- index endkey char stream
class ObMacroBlockWriter {
public:
  ObMacroBlockWriter();
  virtual ~ObMacroBlockWriter();
  void reset();
  int open(ObDataStoreDesc& data_store_desc, const ObMacroDataSeq& start_seq,
      const ObIArray<ObMacroBlockInfoPair>* lob_blocks = NULL, ObMacroBlockWriter* index_writer = NULL);
  int append_macro_block(const ObMacroBlockCtx& macro_block_ctx);
  int append_micro_block(const ObMicroBlock& micro_block);
  int append_row(const storage::ObStoreRow& row, const bool virtual_append = false);
  int close(storage::ObStoreRow* root = NULL, char* root_buf = NULL);
  inline ObMacroBlocksWriteCtx& get_macro_block_write_ctx()
  {
    return block_write_ctx_;
  }
  inline ObMacroBlocksWriteCtx& get_lob_macro_block_write_ctx()
  {
    return lob_writer_.get_macro_block_write_ctx();
  }
  inline ObMacroBlocksWriteCtx& get_index_macro_block_write_ctx()
  {
    return task_index_writer_->get_macro_block_write_ctx();
  }
  TO_STRING_KV(K_(block_write_ctx));

  struct IndexMicroBlockBuilder {
  public:
    IndexMicroBlockBuilder()
    {}
    virtual ~IndexMicroBlockBuilder() = default;
    TO_STRING_KV(K(writer_.get_row_count()));
    ObMicroBlockWriter writer_;
    ObBlockIntermediateBuilder row_builder_;
  };
  struct IndexMicroBlockDesc {
  public:
    IndexMicroBlockDesc()
    {}
    virtual ~IndexMicroBlockDesc() = default;
    TO_STRING_KV(K(writer_.get_row_count()));
    ObMicroBlockWriter writer_;
    char last_key_buf_[common::OB_MAX_ROW_KEY_LENGTH];
    common::ObStoreRowkey last_key_;
  };
  struct IndexMicroBlockDescCompare final {
  public:
    IndexMicroBlockDescCompare()
    {}
    ~IndexMicroBlockDescCompare() = default;
    bool operator()(const IndexMicroBlockDesc* left, const IndexMicroBlockDesc* right)
    {
      int32_t cmp_ret = left->last_key_.compare(right->last_key_);
      return cmp_ret < 0;
    }
  };

private:
  int append_row(const storage::ObStoreRow& row, const int64_t split_size, const bool ignore_lob = false);
  int check_order(const storage::ObStoreRow& row);
  int build_micro_block(const bool force_split = false);
  int build_micro_block_desc(const ObMicroBlock& micro_block, ObMicroBlockDesc& micro_block_desc);
  int build_micro_block_desc_with_rewrite(const ObMicroBlock& micro_block, ObMicroBlockDesc& micro_block_desc);
  int build_micro_block_desc_with_reuse(const ObMicroBlock& micro_block, ObMicroBlockDesc& micro_block_desc);
  int write_micro_block(const ObMicroBlockDesc& micro_block_desc, const bool force_split = false);
  int check_micro_block_need_merge(const ObMicroBlock& micro_block, bool& need_merge);
  int merge_micro_block(const ObMicroBlock& micro_block);
  int flush_macro_block(ObMacroBlock& macro_block);
  int flush_index_macro_block(ObMacroBlock& macro_block);
  int try_switch_macro_block();
  int wait_io_finish();
  int check_write_complete(const MacroBlockId& macro_block_id);
  int can_mark_deletion(const ObStoreRowkey& start_key, const ObStoreRowkey& end_key, bool& can_mark_deletion);
  int save_last_key(const ObStoreRowkey& last_key);
  int save_pre_micro_last_key(const ObStoreRowkey& pre_micro_last_key);
  int add_row_checksum(const common::ObNewRow& row);
  int calc_micro_column_checksum(const int64_t column_cnt, ObIMicroBlockReader& reader, int64_t* column_checksum);
  int flush_reuse_macro_block(const ObMacroBlockCtx& macro_block_ctx);
  ObIMicroBlockReader* get_micro_block_reader(const int64_t row_store_type);
  inline bool enable_sparse_format() const
  {
    return data_store_desc_->enable_sparse_format();
  }
  int check_micro_block_checksum(const char* buf, const int64_t size,
      const ObIMicroBlockWriter* micro_writer /*do checksum for this micro writer*/);
  int check_micro_block(const char* compressed_buf, const int64_t compressed_size, const char* uncompressed_buf,
      const int64_t uncompressed_size, ObIMicroBlockWriter* micro_writer /*check for this micro writer*/);
  int build_column_map(const ObDataStoreDesc* data_desc, ObColumnMap& column_map);
  int open_bf_cache_writer(const ObDataStoreDesc& desc);
  int flush_bf_to_cache(ObMacroBloomFilterCacheWriter& bf_cache_writer, const int32_t row_count);

private:
  int build_index_micro_block(const int32_t height, const storage::ObStoreRow*& mid_row);
  int build_intermediate_row(const ObString& rowkey, const ObBlockIntermediateHeader& header,
      ObBlockIntermediateBuilder& builder, const storage::ObStoreRow*& row);
  int build_intermediate_row(const ObStoreRowkey& rowkey, const ObBlockIntermediateHeader& header,
      ObBlockIntermediateBuilder& builder, const storage::ObStoreRow*& row);
  int close_index_tree();
  int create_index_block_builder();
  int do_sort_micro_block();
  OB_INLINE int64_t get_current_macro_seq()
  {
    return current_macro_seq_;
  }
  int get_root_row(storage::ObStoreRow*& root, char* root_buf);
  int get_index_block_desc(IndexMicroBlockDesc*& index_micro_block_desc);
  int merge_root_micro_block();
  int save_root_micro_block();
  int update_index_tree(const int32_t height, const storage::ObStoreRow* intermediate_row);
  int write_micro_block(const ObMicroBlockDesc& micro_block_desc, int64_t& data_offset);

  int prepare_micro_block_reader(const char* buf, const int64_t size, ObIMicroBlockReader*& micro_reader);
  int print_micro_block_row(ObIMicroBlockReader* micro_reader);
  int dump_micro_block_writer_buffer();

private:
  static const int64_t DEFAULT_MACRO_BLOCK_COUNT = 128;
  static const int64_t DEFAULT_MICRO_BLOCK_TREE_HIGH = 4;
  static const int64_t DEFAULT_MICRO_BLOCK_WRITER_COUNT = 64;
  static const int64_t INDEX_MACRO_BLOCK_MAX_SEQ_NUM = 0x100000;  // 1048576
  typedef common::ObSEArray<MacroBlockId, DEFAULT_MACRO_BLOCK_COUNT> MacroBlockList;
  typedef common::ObSEArray<IndexMicroBlockBuilder*, DEFAULT_MICRO_BLOCK_TREE_HIGH> IndexMicroBlockBuildList;
  typedef common::ObSEArray<IndexMicroBlockDesc*, DEFAULT_MICRO_BLOCK_WRITER_COUNT> IndexMicroBlockDescList;

private:
  ObDataStoreDesc* data_store_desc_;
  ObDataStoreDesc* index_store_desc_;
  ObMicroBlockCompressor compressor_;
  IndexMicroBlockBuildList index_block_builders_;
  IndexMicroBlockDescList task_top_block_descs_;
  ObIMicroBlockWriter* micro_writer_;
  ObMicroBlockWriter flat_writer_;
  ObRowWriter row_writer_;
  char rowkey_buf_[common::OB_MAX_ROW_KEY_LENGTH];
  ObMicroBlockReader flat_reader_;
  ObSparseMicroBlockReader sparse_reader_;
  ObMacroBlockWriter* sstable_index_writer_;
  ObMacroBlockWriter* task_index_writer_;
  ObMacroBlock macro_blocks_[2];
  ObMacroBloomFilterCacheWriter bf_cache_writer_[2];  // associate with macro_blocks
  int64_t current_index_;
  int64_t current_macro_seq_;  // set by sstable layer;
  ObMacroBlocksWriteCtx block_write_ctx_;
  ObMacroBlockHandle macro_handle_;
  char last_key_buf_[common::OB_MAX_ROW_KEY_LENGTH];
  common::ObStoreRowkey last_key_;
  bool last_key_with_L_flag_;
  // for mark deletion
  ObBlockMarkDeletionMaker* mark_deletion_maker_;
  bool need_deletion_check_;
  bool pre_deletion_flag_;  // record the delete flag of prev micro block
  char pre_micro_last_key_buf_[common::OB_MAX_ROW_KEY_LENGTH];
  common::ObStoreRowkey pre_micro_last_key_;
  bool has_lob_;
  blocksstable::ObLobMergeWriter lob_writer_;
  int64_t* curr_micro_column_checksum_;
  common::ObArenaAllocator allocator_;
  ObColumnMap column_map_;
  ObColumnMap index_column_map_;
  blocksstable::ObMacroBlockReader macro_reader_;
  char obj_buf_[common::OB_ROW_MAX_COLUMNS_COUNT * sizeof(common::ObObj)];          // for reader to get row
  char checker_obj_buf_[common::OB_ROW_MAX_COLUMNS_COUNT * sizeof(common::ObObj)];  // for calc or varify checksum, can
                                                                                    // NOT use same buf of data row
  ObMicroBlockReader check_flat_reader_;
  ObSparseMicroBlockReader check_sparse_reader_;
  common::ObArray<uint32_t> micro_rowkey_hashs_;
  storage::ObSSTableRowkeyHelper* rowkey_helper_;
  ObSSTableMacroBlockChecker macro_block_checker_;
  common::SpinRWLock lock_;
};

}  // end namespace blocksstable
}  // end namespace oceanbase
#endif
