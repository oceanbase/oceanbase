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

#ifndef STORAGE_COMPACTION_OB_SSTABLE_BUILDER_H_
#define STORAGE_COMPACTION_OB_SSTABLE_BUILDER_H_

#include "storage/blocksstable/ob_macro_block.h"
#include "storage/blocksstable/ob_macro_block_writer.h"
#include "storage/blocksstable/index_block/ob_index_block_builder.h"
#include "storage/compaction/ob_compaction_memory_context.h"

namespace oceanbase
{
namespace blocksstable
{
class MacroBlockId;
}
namespace compaction
{
struct ObStaticMergeParam;

class ObSSTableRebuildMicroBlockIter final
{
public:
  ObSSTableRebuildMicroBlockIter(
    const ObIArray<blocksstable::MacroBlockId> &macro_id_array,
    const ObITableReadInfo &index_read_info)
    : allocator_("RebuildIter"),
      io_allocator_("SSRMB_IOUB", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()),
      macro_id_array_(macro_id_array),
      index_read_info_(index_read_info),
      mirco_block_iter_(),
      macro_io_handle_(),
      iter_idx_(-1),
      prefetch_idx_(-1)
  {}
  ~ObSSTableRebuildMicroBlockIter() = default;
  int init();
  int prepare() { return prefetch_idx_ != -1 ? OB_ERR_UNEXPECTED : prefetch(); }
  int open_next_macro_block();
  int get_next_micro_block(
      blocksstable::ObMicroBlockDesc &micro_block_desc,
      blocksstable::ObMicroIndexInfo &micro_index_info);
  inline bool is_iter_end() const { return iter_idx_ >= macro_id_array_.count() - 1; }

  TO_STRING_KV(K_(macro_id_array), K_(mirco_block_iter), K_(iter_idx));
private:
  int prefetch();
  inline blocksstable::ObMacroBlockHandle &get_curr_macro_handle()
  {
    return macro_io_handle_[iter_idx_ % PREFETCH_DEPTH];
  }
  static const int64_t PREFETCH_DEPTH = 3;
private:
  ObArenaAllocator allocator_;
  ObArenaAllocator io_allocator_;
  const ObIArray<blocksstable::MacroBlockId> &macro_id_array_;
  const ObITableReadInfo &index_read_info_;
  blocksstable::ObMicroBlockBareIterator mirco_block_iter_;
  blocksstable::ObMacroBlockHandle macro_io_handle_[PREFETCH_DEPTH];
  int64_t iter_idx_;
  int64_t prefetch_idx_;
  char *io_buf_[PREFETCH_DEPTH];
};


class ObSSTableBuilder final
{
public:
  ObSSTableBuilder();
  ~ObSSTableBuilder();
  void reset();
  int set_index_read_info(const ObITableReadInfo *read_info);
  int prepare_index_builder();
  int build_sstable_merge_res(const ObStaticMergeParam &merge_param, ObSSTableMergeInfo &sstable_merge_info_, blocksstable::ObSSTableMergeRes &res);
  int build_reused_small_sst_merge_res(
      const int64_t macro_read_size,
      const int64_t macro_offset,
      blocksstable::ObSSTableMergeRes &res)
  {
    return index_builder_.close(res, macro_read_size, macro_offset);
  }
  blocksstable::ObSSTableIndexBuilder *get_index_builder() { return &index_builder_; }
  blocksstable::ObWholeDataStoreDesc& get_data_desc() { return data_store_desc_; }
  TO_STRING_KV(K_(data_store_desc), K_(macro_writer), K_(index_builder));
  using MetaIter = blocksstable::ObSSTableIndexBuilder::ObMacroMetaIter;
private:
  int open_macro_writer();
  int rebuild_macro_block(const ObIArray<blocksstable::MacroBlockId> &macro_id_array, MetaIter &iter);
  int check_cur_macro_need_merge(
      const int64_t last_macro_blocks_sum,
      const blocksstable::ObDataMacroBlockMeta &curr_macro_meta,
      bool &need_merge);
  int rewrite_macro_block(ObSSTableRebuildMicroBlockIter &micro_iter);
  int check_need_rebuild(const ObStaticMergeParam &merge_param,
                         ObIArray<blocksstable::MacroBlockId> &macro_id_array,
                         MetaIter &iter,
                         int64_t &multiplexed_macro_block_count);
  int pre_check_rebuild(const ObStaticMergeParam &merge_param, MetaIter &iter, bool &need_check_rebuild);
  bool check_macro_block_could_merge(const blocksstable::ObDataMacroBlockMeta &macro_meta) const
  {
    return data_store_desc_.get_desc().get_row_store_type() == macro_meta.val_.row_store_type_
        && data_store_desc_.get_desc().get_schema_version() == macro_meta.val_.schema_version_
        && data_store_desc_.get_desc().get_progressive_merge_round() == macro_meta.val_.progressive_merge_round_;
  }
private:
  blocksstable::ObSSTableIndexBuilder index_builder_;
  blocksstable::ObSSTableIndexBuilder rebuild_index_builder_;
  blocksstable::ObWholeDataStoreDesc data_store_desc_;
  blocksstable::ObMacroBlockWriter macro_writer_;
  const ObITableReadInfo *index_read_info_;
  static const int64_t REBUILD_MACRO_BLOCK_THRESOLD = 20;
  static const int64_t DEFAULT_MACRO_ID_COUNT = 32;
};

} // namespace compaction
} // namespace oceanbase

#endif
