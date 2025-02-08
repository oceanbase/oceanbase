/**
 * Copyright (c) 2022 OceanBase
 * OceanBase is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_BLOCKSSTABLE_OB_INDEX_BLOCK_DUMPER
#define OCEANBASE_BLOCKSSTABLE_OB_INDEX_BLOCK_DUMPER

#include "storage/blocksstable/index_block/ob_index_block_aggregator.h"
#include "storage/blocksstable/ob_macro_block_writer.h"
#include "storage/blocksstable/ob_macro_block_bare_iterator.h"
#include "storage/blocksstable/ob_storage_object_handle.h"

namespace oceanbase
{
namespace blocksstable
{

static const int64_t DEFAULT_MACRO_LEVEL_ROWS_COUNT = 8;
typedef common::ObSEArray<ObIndexBlockRowDesc *, DEFAULT_MACRO_LEVEL_ROWS_COUNT>
  ObNextLevelRowsArray;
typedef common::ObSEArray<ObDataMacroBlockMeta *, DEFAULT_MACRO_LEVEL_ROWS_COUNT>
    ObMacroMetasArray;

enum ObMacroMetaStorageState : int8_t
{
  IN_ARRAY = 0,         // only for macro meta, in mem array
  IN_MEM = 1,           // only for macro meta, in mem micro block
  IN_DISK = 2,          // only for macro meta, in disk macro block
  INVALID_STORAGE_STATE = 3
};


struct ObDataBlockInfo final
{
public:
  ObDataBlockInfo();
  ~ObDataBlockInfo() = default;
  TO_STRING_KV(K_(data_column_checksums), K_(meta_data_checksums), K_(data_column_cnt),
      K_(occupy_size), K_(original_size), K_(micro_block_cnt));
  common::ObSEArray<int64_t, 1> data_column_checksums_;
  common::ObSEArray<int64_t, DEFAULT_MACRO_LEVEL_ROWS_COUNT> meta_data_checksums_;
  int64_t data_column_cnt_;
  int64_t occupy_size_;
  int64_t original_size_;
  int64_t micro_block_cnt_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObDataBlockInfo);
};


struct ObIndexBlockInfo final
{
public:
  ObIndexBlockInfo();
  ~ObIndexBlockInfo();
  void reset();
  OB_INLINE bool is_valid() const { return empty() || in_array() || in_mem() || in_disk(); }
  OB_INLINE bool empty() const
  {
    return micro_block_desc_ == nullptr && block_write_ctx_ == nullptr &&
           micro_block_cnt_ == 0 && row_count_ == 0 &&
           state_ == ObMacroMetaStorageState::INVALID_STORAGE_STATE &&
           need_rewrite_ == false && next_level_rows_list_ == nullptr &&
           agg_info_ == nullptr;
  }
  OB_INLINE bool in_disk() const
  {
    return (state_ == ObMacroMetaStorageState::IN_DISK) && micro_block_desc_ == nullptr && agg_info_ == nullptr
        && block_write_ctx_ != nullptr && !block_write_ctx_->is_empty()
        && ((next_level_rows_list_ != nullptr && !need_rewrite_)
        || next_level_rows_list_ == nullptr);

  }
  OB_INLINE bool in_mem() const
  {
    return (state_ == ObMacroMetaStorageState::IN_MEM) && !need_rewrite_ &&
           micro_block_desc_ != nullptr && block_write_ctx_ == nullptr &&
           next_level_rows_list_ == nullptr &&
           ((is_meta_ && agg_info_ == nullptr) || (!is_meta_ && agg_info_ != nullptr));
  }
  // in array state only for meta_tree_info.
  OB_INLINE bool in_array() const {
    return is_meta_ && state_ == ObMacroMetaStorageState::IN_ARRAY &&
           micro_block_desc_ == nullptr && block_write_ctx_ == nullptr &&
           next_level_rows_list_ == nullptr & agg_info_ == nullptr &&
           micro_block_cnt_ == 0 && need_rewrite_ == false;
  }
  // check if need load block from disk
  OB_INLINE bool not_need_load_block() const
  {
    return in_mem() || in_array() || (in_disk() && next_level_rows_list_ != nullptr && !need_rewrite_);
  }
  OB_INLINE int64_t get_row_count() const { return row_count_; }
  OB_INLINE int64_t get_micro_block_count() const { return micro_block_cnt_; }

public:
  ObMacroMetasArray *macro_meta_list_; // for disable dump disk, and only for meta info
  ObMicroBlockDesc *micro_block_desc_;
  ObMacroBlocksWriteCtx *block_write_ctx_;
  ObNextLevelRowsArray *next_level_rows_list_; // generate next level rows to avoid load block from disk
  ObIndexRowAggInfo *agg_info_;// only for index mem block
  int64_t micro_block_cnt_;
  int64_t row_count_;
  ObMacroMetaStorageState state_;
  bool is_meta_;  // true : meta_tree_info, false : index_tree_info
  bool need_rewrite_; // reorganize disk space usage for meta blocks
  TO_STRING_KV(K_(is_meta), K_(state), K_(need_rewrite), K_(row_count),
               K_(micro_block_cnt), KPC_(macro_meta_list),
               KPC_(micro_block_desc), KPC_(agg_info), KPC_(block_write_ctx),
               KP_(next_level_rows_list));

private:
  DISALLOW_COPY_AND_ASSIGN(ObIndexBlockInfo);
};


class ObBaseIndexBlockDumper
{
public:
  ObBaseIndexBlockDumper();
  virtual ~ObBaseIndexBlockDumper();
  void reset();
  int init(const ObDataStoreDesc &index_store_desc,
           const ObDataStoreDesc &container_store_desc,
           ObSSTableIndexBuilder *sstable_index_builder,
		       common::ObIAllocator &sstable_allocator,
           common::ObIAllocator &task_allocator,
           bool need_check_order = true,
           bool enable_dump_disk = true,
           ObIODevice *device_handle = nullptr);
  int append_row(const ObDatumRow &row);
  int close(ObIndexBlockInfo& index_block_info);
  OB_INLINE const ObDatumRowkey& get_last_rowkey() const { return last_rowkey_;}
  OB_INLINE int64_t get_row_count() const { return row_count_; }

  TO_STRING_KV(K_(is_inited), K_(is_meta), K_(row_count), K_(micro_block_cnt), K_(last_rowkey),
      K_(need_build_next_row), K_(need_check_order), KP_(next_level_rows_list),
      KP_(task_allocator), KP_(meta_micro_writer), KP_(meta_macro_writer),
      KP_(container_store_desc), KP_(index_store_desc));
protected:
  int new_macro_writer();
  virtual void init_start_seq(ObMacroDataSeq &start_seq)  {start_seq.set_macro_meta_block(); }
  virtual int append_next_level_row(const ObMicroBlockDesc &micro_block_desc);
  virtual int close_to_mem(ObIndexBlockInfo& index_block_info);
  int close_to_disk(ObIndexBlockInfo& index_block_info);
  int close_to_array(ObIndexBlockInfo& index_block_info);
  int init_next_level_array();
  int check_order(const ObDatumRow &row);
  int build_and_append_block();
  virtual void clean_status();
protected:
  const ObDataStoreDesc *index_store_desc_;
  const ObDataStoreDesc *container_store_desc_;
  ObSSTableIndexBuilder *sstable_index_builder_;
  ObIODevice *device_handle_;
  common::ObIAllocator *sstable_allocator_;
  common::ObIAllocator *task_allocator_;       // for micro writer and macro writer
  common::ObArenaAllocator row_allocator_;
  ObIMicroBlockWriter *meta_micro_writer_;
  ObMacroBlockWriter *meta_macro_writer_;
  ObMicroBlockAdaptiveSplitter micro_block_adaptive_splitter_;
  ObNextLevelRowsArray *next_level_rows_list_; // for disk
  ObMacroMetasArray *macro_metas_; // for ObMacroMetaOptimizationMode::DISABLE
  ObDatumRowkey last_rowkey_;
  int64_t micro_block_cnt_;
  int64_t row_count_;
  bool is_meta_;
  bool enable_dump_disk_;
  bool need_build_next_row_;
  bool need_check_order_;
  bool is_inited_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObBaseIndexBlockDumper);
};
class ObIndexTreeBlockDumper : public ObBaseIndexBlockDumper
{
public:
  ObIndexTreeBlockDumper();
  virtual ~ObIndexTreeBlockDumper() = default;
  int init(const ObDataStoreDesc &data_store_desc,
           const ObDataStoreDesc &index_store_desc,
           ObSSTableIndexBuilder *sstable_index_builder,
           const ObDataStoreDesc &container_store_desc,
		       common::ObIAllocator &sstable_allocator,
           common::ObIAllocator &task_allocator,
           bool need_check_order = true,
           bool enable_dump_disk = true,
           ObIODevice *device_handle = nullptr);
  int append_row(const ObIndexBlockRowDesc &row_desc);
protected:
  virtual void init_start_seq(ObMacroDataSeq &start_seq) override { start_seq.set_index_block(); }
  virtual int append_next_level_row(const ObMicroBlockDesc &micro_block_desc) override;
  virtual int close_to_mem(ObIndexBlockInfo& index_block_info) override;
  virtual void clean_status() override;
private:
  ObIndexBlockRowBuilder row_builder_;
  ObIndexBlockAggregator index_block_aggregator_;
  int64_t row_offset_;
  DISALLOW_COPY_AND_ASSIGN(ObIndexTreeBlockDumper);
};



// Either get only rows or only blocks
class ObIndexBlockLoader final
{
public:
  ObIndexBlockLoader();
  ~ObIndexBlockLoader();
  void reset();
  void reuse();
  int init(common::ObIAllocator &allocator, const uint64_t data_version);
  int open(const ObIndexBlockInfo& index_block_info);
  int get_next_row(ObDatumRow &row);
  // get micro block only for disk info
  int get_next_micro_block_desc(blocksstable::ObMicroBlockDesc &micro_block_desc,
                                const ObDataStoreDesc &data_store_desc,
                                ObIAllocator &allocator);
  // for disk
  OB_INLINE bool is_last() const
  {
    return macro_id_array_ != nullptr && cur_block_idx_ == macro_id_array_->count() - 1;
  }
  TO_STRING_KV(K_(is_inited), K_(curr_block_row_idx), K_(curr_block_row_cnt), K_(cur_block_idx),
      K_(prefetch_idx), K_(data_version), KPC_(index_block_info), KPC_(macro_id_array));

private:
  OB_INLINE int not_open_mem_block() const { return curr_block_row_idx_ < 0; }
  OB_INLINE bool is_disk_iter_end() const { return cur_block_idx_ >= macro_id_array_->count() - 1; }
  int prefetch();
  int open_mem_block();
  int open_next_macro_block();
  int get_next_mem_row(ObDatumRow &row);
  int get_next_disk_row(ObDatumRow &row);
  int get_next_array_row(ObDatumRow &row);
  int open_micro_block(ObMicroBlockData &micro_block_data);
  int get_next_disk_micro_block_data(ObMicroBlockData &micro_block_data);

private:
  static const int64_t PREFETCH_DEPTH = 1;

private:
  blocksstable::ObStorageObjectHandle macro_io_handle_[PREFETCH_DEPTH];
  ObMicroBlockReaderHelper micro_reader_helper_;
  ObMicroBlockData cur_micro_block_;
  ObMicroBlockBareIterator micro_iter_;
  ObArenaAllocator row_allocator_;
  ObIMicroBlockReader *micro_reader_;
  const ObIndexBlockInfo *index_block_info_;
  common::ObIArray<blocksstable::MacroBlockId> *macro_id_array_;
  ObIAllocator *io_allocator_;
  char *io_buf_[PREFETCH_DEPTH]; // for io
  int64_t curr_block_row_idx_;
  int64_t curr_block_row_cnt_;
  int64_t cur_block_idx_; // in disk: macro block, in mem: micro block
  int64_t prefetch_idx_;
  uint64_t data_version_;
  bool is_inited_;
  DISALLOW_COPY_AND_ASSIGN(ObIndexBlockLoader);
};


} // blocksstable
} // oceanbase

#endif
