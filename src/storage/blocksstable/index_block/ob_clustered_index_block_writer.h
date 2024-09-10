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

#ifndef OCEANBASE_STORAGE_BLOCKSSTABLE_OB_CLUSTERED_INDEX_BLOCK_WRITER_H_
#define OCEANBASE_STORAGE_BLOCKSSTABLE_OB_CLUSTERED_INDEX_BLOCK_WRITER_H_

#include "storage/blocksstable/index_block/ob_index_block_row_struct.h"
#include "storage/blocksstable/ob_macro_block_writer.h"
#include "storage/blocksstable/ob_micro_block_reader.h"
#include "storage/blocksstable/ob_data_store_desc.h"

namespace oceanbase
{
namespace blocksstable
{

class ObDataIndexBlockBuilder;
class ObIndexTreeRootCtx;

struct ObClusteredIndexBlockMicroInfos final
{
  ObClusteredIndexBlockMicroInfos() = default;
  ObClusteredIndexBlockMicroInfos(
      const MacroBlockId &macro_id, const int64_t block_offset,
      const int64_t block_size, const ObLogicMicroBlockId &logic_micro_id)
      : macro_id_(macro_id), block_offset_(block_offset),
        block_size_(block_size), logic_micro_id_(logic_micro_id)
  {}
  MacroBlockId macro_id_;
  int64_t block_offset_;
  int64_t block_size_;
  ObLogicMicroBlockId logic_micro_id_;
  TO_STRING_KV(K_(macro_id), K_(block_offset), K_(block_size), K_(logic_micro_id));
};

class ObClusteredIndexBlockWriter final
{
public:
  ObClusteredIndexBlockWriter();
  ~ObClusteredIndexBlockWriter();
  void reset();
  int init(const ObDataStoreDesc &data_store_desc,
           ObDataStoreDesc &leaf_block_desc,
           const blocksstable::ObMacroSeqParam &macro_seq_param,
           const share::ObPreWarmerParam &pre_warm_param,
           ObIndexTreeRootCtx *root_ctx,
           common::ObIAllocator &task_allocator);
  // Build clustered index row and append to clustered index micro writer.
  int append_row(const ObIndexBlockRowDesc &row_desc);
  // Build clustered index micro block and append to clustered index macro writer.
  int build_and_append_clustered_index_micro_block();
  int reuse_clustered_micro_block(const MacroBlockId &macro_id,
                                  const ObMicroBlockData &micro_block_data);
  // Rewrite clustered index micro block.
  int rewrite_and_append_clustered_index_micro_block(
      const char *macro_buf,
      const int64_t macro_size,
      const ObDataMacroBlockMeta &macro_meta);
  int rewrite_and_append_clustered_index_micro_block(const ObDataMacroBlockMeta &macro_data);
  int close();

private:
  DISALLOW_COPY_AND_ASSIGN(ObClusteredIndexBlockWriter);

private:
  int build_clustered_index_micro_block(
    ObMicroBlockDesc &clustered_index_micro_block_desc);
  int check_order(const ObIndexBlockRowDesc &row_desc);
  int decompress_and_make_clustered_index_micro_block(
      const char *micro_buffer, const int64_t micro_size,
      const MacroBlockId &macro_id,
      const ObDataMacroBlockMeta &macro_meta);
  int decompress_micro_block_data(const ObDataMacroBlockMeta &macro_meta,
                                  ObMicroBlockData &micro_block_data);
  int make_clustered_index_micro_block_with_rewrite(
      const ObMicroBlockData &micro_block_data,
      const MacroBlockId &macro_id);
  int make_clustered_index_micro_block_with_reuse(
      const ObMicroBlockData &micro_block_data,
      const MacroBlockId &macro_id);
  int print_macro_ids();

private:
  ObDataStoreDesc clustered_index_store_desc_;
  const ObDataStoreDesc * data_store_desc_;
  const ObDataStoreDesc * leaf_block_desc_;
  ObIndexBlockRowBuilder row_builder_;
  ObIndexTreeRootCtx * root_ctx_;  // pointer to ObDataIndexBlockBuilder::index_tree_root_ctx_
  ObMacroBlockWriter * macro_writer_;
  ObIMicroBlockWriter * micro_writer_;
  common::ObIAllocator * task_allocator_;
  common::ObArenaAllocator row_allocator_;
  compaction::ObLocalArena macro_block_io_allocator_;
  common::ObArenaAllocator decompress_allocator_;
  ObDatumRowkey last_rowkey_;
  bool is_inited_;
};

} // namespace blocksstable
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_BLOCKSSTABLE_OB_CLUSTERED_INDEX_BLOCK_WRITER_H_