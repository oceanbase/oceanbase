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
#include "common/row/ob_row.h"
#include "share/schema/ob_table_schema.h"
#include "storage/ob_i_store.h"
#include "storage/ob_sstable.h"
#include "lib/compress/ob_compressor_pool.h"
#include "lib/utility/ob_tracepoint.h"
#include "share/config/ob_server_config.h"
#include "share/ob_task_define.h"
#include "share/ob_force_print_log.h"
#include "storage/ob_file_system_util.h"

namespace oceanbase {
using namespace common;
using namespace storage;
using namespace share::schema;
using namespace share;
namespace blocksstable {
/**
 * ---------------------------------------------------------ObMacroBlockWriter--------------------------------------------------------------
 */
ObMacroBlockWriter::ObMacroBlockWriter()
    : data_store_desc_(NULL),
      index_store_desc_(NULL),
      compressor_(),
      micro_writer_(&flat_writer_),
      flat_writer_(),
      row_writer_(),
      flat_reader_(),
      sstable_index_writer_(NULL),
      task_index_writer_(NULL),
      current_index_(0),
      current_macro_seq_(0),
      block_write_ctx_(),
      macro_handle_(),
      last_key_(),
      last_key_with_L_flag_(false),
      mark_deletion_maker_(NULL),
      need_deletion_check_(false),
      pre_deletion_flag_(false),
      pre_micro_last_key_(),
      has_lob_(false),
      lob_writer_(),
      curr_micro_column_checksum_(NULL),
      allocator_("MacrBlocWriter"),
      macro_reader_(),
      micro_rowkey_hashs_(),
      rowkey_helper_(nullptr)
{
  // macro_blocks_
}

ObMacroBlockWriter::~ObMacroBlockWriter()
{
  COMMON_LOG(INFO, "ObMacroBlockWriter is destructed");
}

void ObMacroBlockWriter::reset()
{
  for (int i = 0; i < index_block_builders_.count(); i++) {
    index_block_builders_.at(i)->~IndexMicroBlockBuilder();
    allocator_.free(index_block_builders_.at(i));
    index_block_builders_.at(i) = NULL;
  }
  for (int32_t i = 0; i < task_top_block_descs_.count(); i++) {
    task_top_block_descs_.at(i)->~IndexMicroBlockDesc();
    allocator_.free(task_top_block_descs_.at(i));
    task_top_block_descs_.at(i) = NULL;
  }
  index_block_builders_.reset();
  task_top_block_descs_.reset();
  // data_store_desc_
  // block_size_spec_
  micro_writer_ = &flat_writer_;
  flat_writer_.reuse();
  flat_reader_.reset();
  sstable_index_writer_ = NULL;
  task_index_writer_ = NULL;
  macro_blocks_[0].reset();
  macro_blocks_[1].reset();
  bf_cache_writer_[0].reset();
  bf_cache_writer_[1].reset();
  current_index_ = 0;
  block_write_ctx_.reset();
  macro_handle_.reset();
  last_key_.assign(NULL, 0);
  last_key_with_L_flag_ = false;
  // comp_buf_
  // column_map_
  mark_deletion_maker_ = NULL;
  pre_deletion_flag_ = false;
  pre_micro_last_key_.reset();
  last_key_.reset();
  has_lob_ = false;
  lob_writer_.reset();
  check_flat_reader_.reset();
  check_sparse_reader_.reset();
  micro_rowkey_hashs_.reset();
  rowkey_helper_ = nullptr;
  allocator_.reuse();
}

int ObMacroBlockWriter::open(ObDataStoreDesc& data_store_desc, const ObMacroDataSeq& start_seq,
    const ObIArray<ObMacroBlockInfoPair>* lob_blocks, ObMacroBlockWriter* index_writer)
{
  int ret = OB_SUCCESS;
  reset();
  if (OB_UNLIKELY(!data_store_desc.is_valid() || !start_seq.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid macro block writer input argument.", K(ret), K(data_store_desc), K(start_seq));
  } else if (OB_FAIL(compressor_.init(data_store_desc.micro_block_size_, data_store_desc.compressor_name_))) {
    STORAGE_LOG(WARN, "Fail to init micro block compressor, ", K(ret), K(data_store_desc));
  } else if (OB_FAIL(macro_blocks_[0].init(data_store_desc))) {
    STORAGE_LOG(WARN, "Fail to init 0th macro block, ", K(ret));
  } else if (OB_FAIL(macro_blocks_[1].init(data_store_desc))) {
    STORAGE_LOG(WARN, "Fail to init 1th macro block, ", K(ret));
  } else if (OB_FAIL(OB_FILE_SYSTEM.init_file_ctx(STORE_FILE_MACRO_BLOCK, block_write_ctx_.file_ctx_))) {
    LOG_WARN("failed to init write ctx", K(ret));
  } else if (!block_write_ctx_.file_handle_.is_valid() &&
             OB_FAIL(block_write_ctx_.file_handle_.assign(data_store_desc.file_handle_))) {
    STORAGE_LOG(WARN, "fail to set file handle", K(ret), K(data_store_desc.file_handle_));
  } else {
    macro_handle_.set_file(data_store_desc.file_handle_.get_storage_file());
    // need to build index tree with leaf node
    if (data_store_desc.need_index_tree_ && OB_NOT_NULL(index_writer)) {
      index_store_desc_ = index_writer->data_store_desc_;
      sstable_index_writer_ = index_writer;
      void* buf = NULL;
      if (OB_ISNULL(buf = allocator_.alloc(sizeof(ObMacroBlockWriter)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        STORAGE_LOG(WARN, "fail to alloc memory", K(ret));
      } else if (OB_ISNULL(task_index_writer_ = new (buf) ObMacroBlockWriter())) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "fail to new a ObMacroBlockWriter", K(ret));
      } else if (OB_FAIL(task_index_writer_->open(*index_store_desc_, start_seq))) {
        STORAGE_LOG(WARN, "fail to open task index writer", K(ret));
      }
      if (OB_FAIL(ret)) {
        if (OB_NOT_NULL(buf)) {
          allocator_.free(buf);
        }
      }
    } else if (data_store_desc.need_index_tree_ && OB_ISNULL(index_writer)) {
      // need to build index tree with non-leaf node
      index_store_desc_ = &data_store_desc;
      sstable_index_writer_ = this;
      task_index_writer_ = this;
    } else if (!data_store_desc.need_index_tree_ && OB_ISNULL(index_writer)) {
      // no need to build index tree
      index_store_desc_ = NULL;
      sstable_index_writer_ = NULL;
      task_index_writer_ = NULL;
    } else {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "exceptional situation", K(ret), K(data_store_desc), KP_(sstable_index_writer));
    }
    if (OB_SUCC(ret)) {
      data_store_desc_ = &data_store_desc;
      current_macro_seq_ = start_seq.get_data_seq() + INDEX_MACRO_BLOCK_MAX_SEQ_NUM;
      mark_deletion_maker_ = data_store_desc.mark_deletion_maker_;
      need_deletion_check_ = OB_ISNULL(mark_deletion_maker_) ? false : true;
      pre_deletion_flag_ = false;
      pre_micro_last_key_.reset();
      last_key_.reset();

      if (OB_FAIL(build_column_map(data_store_desc_, column_map_))) {
        STORAGE_LOG(WARN, "failed to build column map", K(data_store_desc), K(ret));
      } else if (OB_ISNULL(index_store_desc_)) {
        // nothing to do.
      } else if (OB_FAIL(build_column_map(index_store_desc_, index_column_map_))) {
        STORAGE_LOG(WARN, "failed to build index column map", K(data_store_desc), K(ret));
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(flat_writer_.init(data_store_desc_->micro_block_size_limit_,
                data_store_desc_->rowkey_column_count_,
                data_store_desc_->row_column_count_))) {
          STORAGE_LOG(WARN, "Fail to init micro block flat writer, ", K(ret));
        } else {
          micro_writer_ = &flat_writer_;
        }
      }

      if (OB_FAIL(ret)) {
      } else if (data_store_desc.has_lob_column_) {
        if (OB_FAIL(lob_writer_.init(start_seq, data_store_desc, lob_blocks))) {
          STORAGE_LOG(WARN, "Failed to init lob writer", K(ret));
        } else {
          has_lob_ = true;
        }
      } else {
        has_lob_ = false;
      }

      if (OB_SUCC(ret) && data_store_desc_->need_calc_column_checksum_) {
        if (OB_ISNULL(curr_micro_column_checksum_ = static_cast<int64_t*>(
                          allocator_.alloc(sizeof(int64_t) * data_store_desc_->row_column_count_)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          STORAGE_LOG(WARN, "fail to allocate memory for curr micro block column checksum", K(ret));
        } else {
          MEMSET(curr_micro_column_checksum_, 0, sizeof(int64_t) * data_store_desc_->row_column_count_);
        }
      }

      if (OB_SUCC(ret) && data_store_desc_->need_prebuild_bloomfilter_ && data_store_desc_->bloomfilter_size_ > 0) {
        if (OB_FAIL(open_bf_cache_writer(*data_store_desc_))) {
          STORAGE_LOG(WARN, "Failed to open bloomfilter cache writer, ", K(ret));
          ret = OB_SUCCESS;
        }
      }

      if (OB_SUCC(ret) && OB_NOT_NULL(data_store_desc_->rowkey_helper_)) {
        if (data_store_desc_->rowkey_helper_->is_valid()) {
          rowkey_helper_ = data_store_desc_->rowkey_helper_;
          STORAGE_LOG(INFO, "Succ to find sstable rowkey helper for macro writer", K(*rowkey_helper_));
        }
      }

      // need to build index tree
      if (OB_SUCC(ret) && data_store_desc.need_index_tree_) {
        if (OB_FAIL(create_index_block_builder())) {
          STORAGE_LOG(WARN, "Fail to push one index micro block builder", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObMacroBlockWriter::append_row(const ObStoreRow& row, const bool virtual_append)
{
  int ret = OB_SUCCESS;
  STORAGE_LOG(DEBUG, "append row", K(virtual_append), K(row));
  if (virtual_append) {
    if (has_lob_ && row.flag_ == ObActionFlag::OP_DEL_ROW) {
      ObStoreRowkey rowkey(row.row_val_.cells_, data_store_desc_->rowkey_column_count_);
      if (OB_FAIL(lob_writer_.skip_lob_macro_blocks(rowkey))) {
        STORAGE_LOG(WARN, "Failed to skip lob macro blocks", K(rowkey), K(ret));
      }
    }
  } else if (OB_FAIL(append_row(row, data_store_desc_->micro_block_size_))) {
    STORAGE_LOG(WARN, "Fail to append row", K(ret));
  } else {
    STORAGE_LOG(DEBUG, "Success to append row, ", K(data_store_desc_->table_id_), K(row));
  }

  return ret;
}

int ObMacroBlockWriter::append_row(const ObStoreRow& row, const int64_t split_size, const bool ignore_lob)
{
  int ret = OB_SUCCESS;
  const ObStoreRow* row_to_append = &row;
  if (NULL == data_store_desc_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "The ObMacroBlockWriter has not been opened, ", K(ret));
  } else if (split_size < data_store_desc_->micro_block_size_) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid split_size", K(ret), K(split_size));
  } else if (OB_FAIL(check_order(row))) {
    STORAGE_LOG(WARN, "macro block writer fail to check order.", K(row));
  } else if (has_lob_ && !ignore_lob) {
    if (OB_FAIL(lob_writer_.overflow_lob_objs(row, row_to_append))) {
      STORAGE_LOG(WARN, "Failed to overflow lob objs in the row", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(micro_writer_->append_row(*row_to_append))) {
      if (OB_BUF_NOT_ENOUGH == ret) {
        if (0 == micro_writer_->get_row_count()) {
          ret = OB_NOT_SUPPORTED;
          STORAGE_LOG(ERROR, "The single row is too large, ", K(ret), K(row));
        } else if (OB_FAIL(build_micro_block(false))) {
          // for compatibility
          STORAGE_LOG(WARN, "Fail to build micro block, ", K(ret));
        } else if (OB_FAIL(micro_writer_->append_row(*row_to_append))) {
          STORAGE_LOG(ERROR, "Fail to append row to micro block, ", K(ret), K(row));
        } else if (data_store_desc_->need_calc_column_checksum_ && OB_FAIL(add_row_checksum(row_to_append->row_val_))) {
          STORAGE_LOG(WARN, "fail to add column checksum", K(ret));
        }
        if (OB_SUCC(ret) && data_store_desc_->need_prebuild_bloomfilter_) {
          const ObStoreRowkey rowkey(row_to_append->row_val_.cells_, data_store_desc_->bloomfilter_rowkey_prefix_);
          if (OB_FAIL(micro_rowkey_hashs_.push_back(static_cast<uint32_t>(rowkey.murmurhash(0))))) {
            STORAGE_LOG(WARN, "Fail to put rowkey hash to array ", K(ret), K(rowkey));
            micro_rowkey_hashs_.reuse();
            ret = OB_SUCCESS;
          }
        }
      } else {
        STORAGE_LOG(WARN, "Fail to append row to micro block, ", K(ret), K(row));
      }
    } else {
      if (data_store_desc_->need_prebuild_bloomfilter_) {
        const ObStoreRowkey rowkey(row_to_append->row_val_.cells_, data_store_desc_->bloomfilter_rowkey_prefix_);
        if (OB_FAIL(micro_rowkey_hashs_.push_back(static_cast<uint32_t>(rowkey.murmurhash(0))))) {
          STORAGE_LOG(WARN, "Fail to put rowkey hash to array ", K(ret), K(rowkey));
          micro_rowkey_hashs_.reuse();
          ret = OB_SUCCESS;
        }
      }
      if (data_store_desc_->need_calc_column_checksum_ && OB_FAIL(add_row_checksum(row_to_append->row_val_))) {
        STORAGE_LOG(WARN, "fail to add column checksum", K(ret));
      } else if (micro_writer_->get_block_size() >= split_size) {
        if (OB_FAIL(build_micro_block())) {
          STORAGE_LOG(WARN, "Fail to build micro block, ", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObMacroBlockWriter::append_macro_block(const ObMacroBlockCtx& macro_block_ctx)
{
  int ret = OB_SUCCESS;
  ObFullMacroBlockMeta full_meta;
  if (OB_FAIL(close())) {
    STORAGE_LOG(WARN, "Fail to close macro writer, ", K(ret));
  } else if (!macro_block_ctx.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(macro_block_ctx));
  } else if (OB_FAIL(flush_reuse_macro_block(macro_block_ctx))) {
    LOG_WARN("failed to flush reuse macro block", K(ret), K(macro_block_ctx));
  } else if (OB_FAIL(macro_block_ctx.sstable_->get_meta(macro_block_ctx.get_macro_block_id(), full_meta))) {
    STORAGE_LOG(WARN, "Fail to get macro meta, ", K(ret));
  } else if (!full_meta.is_valid()) {
    ret = OB_ERR_SYS;
    LOG_ERROR("meta must not null", K(ret), K(full_meta));
  } else {
    ObStoreRowkey rowkey;
    const ObMacroBlockMetaV2* block_meta = full_meta.meta_;
    rowkey.assign(block_meta->endkey_, data_store_desc_->rowkey_column_count_);

    if (block_meta->rowkey_column_number_ != data_store_desc_->rowkey_column_count_) {
      ret = OB_ERR_SYS;
      LOG_ERROR("rowkey column not match, cannot reuse macro block",
          K(ret),
          "block_meta_rowkey_count",
          block_meta->rowkey_column_number_,
          "data_store_desc_rowkey_count",
          data_store_desc_->rowkey_column_count_);
    } else if (OB_FAIL(save_last_key(rowkey))) {
      STORAGE_LOG(WARN, "Fail to copy last key, ", K(ret), K(rowkey));
    } else if (OB_FAIL(save_pre_micro_last_key(rowkey))) {
      STORAGE_LOG(WARN, "Fail to copy pre micro last key, ", K(ret), K(rowkey));
    } else if (has_lob_) {
      if (OB_FAIL(lob_writer_.reuse_lob_macro_blocks(rowkey))) {
        STORAGE_LOG(WARN, "Fail to reuse lob macro blocks", K(rowkey), K(ret));
      }
    }
    if (OB_SUCC(ret)) {  // clear flag
      last_key_with_L_flag_ = false;
    }

    if (OB_SUCC(ret) && NULL != data_store_desc_->merge_info_) {
      data_store_desc_->merge_info_->use_old_macro_block_count_++;
      data_store_desc_->merge_info_->macro_block_count_++;
      data_store_desc_->merge_info_->occupy_size_ += block_meta->occupy_size_;
      if (block_meta->is_sstable_data_block()) {
        data_store_desc_->merge_info_->total_row_count_ += block_meta->row_count_;
      }
    }

    // need to build index tree with leaf node
    if (OB_SUCC(ret) && data_store_desc_->need_index_tree_ && this != sstable_index_writer_) {
      IndexMicroBlockBuilder* builder = NULL;
      const ObStoreRow* leaf_row = NULL;
      ObBlockIntermediateHeader header(index_store_desc_->data_version_,
          block_meta->data_seq_,
          block_meta->micro_block_index_offset_,
          block_meta->get_index_size(),
          block_meta->row_count_,
          true);
      if (OB_ISNULL(builder = index_block_builders_.at(0))) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "fail to get the 0th index_block_builder", K(ret));
      } else if (OB_FAIL(build_intermediate_row(rowkey, header, builder->row_builder_, leaf_row))) {
        STORAGE_LOG(WARN, "fail to build intermediate row", K(ret));
      } else if (OB_FAIL(update_index_tree(0 /* update from 0th level */, leaf_row))) {
        STORAGE_LOG(WARN, "macro blcok writer fail to update index tree.", K(ret));
      }
    }
  }
  return ret;
}

int ObMacroBlockWriter::append_micro_block(const ObMicroBlock& micro_block)
{
  int ret = OB_SUCCESS;
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
  } else if (has_lob_ && OB_FAIL(lob_writer_.reuse_lob_macro_blocks(micro_block.range_.get_end_key()))) {
    STORAGE_LOG(WARN, "Fail to reuse lob macro blocks", K(micro_block), K(ret));
  } else if (!need_merge) {
    if (micro_writer_->get_row_count() > 0) {
      if (OB_FAIL(build_micro_block())) {
        STORAGE_LOG(WARN, "build_micro_block failed", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      ObMicroBlockDesc micro_block_desc;
      if (OB_FAIL(build_micro_block_desc(micro_block, micro_block_desc))) {
        STORAGE_LOG(WARN, "build_micro_block_desc failed", K(ret), K(micro_block));
      } else if (OB_FAIL(write_micro_block(micro_block_desc))) {
        STORAGE_LOG(WARN, "Failed to write micro block, ", K(ret), K(micro_block_desc));
      } else if (NULL != data_store_desc_->merge_info_) {
        data_store_desc_->merge_info_->rewrite_macro_old_micro_block_count_++;
      }
    }
  } else {
    if (OB_FAIL(merge_micro_block(micro_block))) {
      STORAGE_LOG(WARN, "merge_micro_block failed", K(micro_block), K(ret));
    } else {
      STORAGE_LOG(TRACE, "merge micro block", K(micro_block));
    }
  }
  return ret;
}

int ObMacroBlockWriter::close(storage::ObStoreRow* root, char* root_buf)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_ISNULL(data_store_desc_) || OB_ISNULL(micro_writer_))) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "exceptional situation", K(ret), K_(data_store_desc), K_(micro_writer));
  } else if (micro_writer_->get_row_count() > 0 && OB_FAIL(build_micro_block())) {
    STORAGE_LOG(WARN, "macro block writer fail to build current micro block.", K(ret));
  } else {
    ObMacroBlock& current_block = macro_blocks_[current_index_];
    ObMacroBlock& prev_block = macro_blocks_[1 - current_index_];
    ObMacroBloomFilterCacheWriter& current_bf_writer = bf_cache_writer_[current_index_];
    ObMacroBloomFilterCacheWriter& prev_bf_writer = bf_cache_writer_[1 - current_index_];
    if (!(data_store_desc_->need_index_tree_ && this == sstable_index_writer_) && prev_block.can_merge(current_block)) {
      // no need to build index tree or with leaf node
      if (OB_FAIL(prev_block.merge(current_block))) {
        STORAGE_LOG(WARN, "Fail to merge current block, ", K(ret));
      } else if (OB_FAIL(current_block.init(*data_store_desc_))) {
        STORAGE_LOG(WARN, "Fail to init current block, ", K(ret));
      }
      if (OB_SUCC(ret) && data_store_desc_->need_prebuild_bloomfilter_) {
        if (!prev_bf_writer.can_merge(current_bf_writer)) {
          prev_bf_writer.reuse();
          current_bf_writer.reuse();
        } else if (OB_FAIL(prev_bf_writer.merge(current_bf_writer))) {
          STORAGE_LOG(WARN, "Fail to merge current bf cache writer, ", K(ret));
          prev_bf_writer.reuse();
          current_bf_writer.reuse();
          ret = OB_SUCCESS;
        } else {
          current_bf_writer.reuse();
        }
      }
    } else if (data_store_desc_->need_index_tree_ && this == sstable_index_writer_ && NULL != root) {
      // need to build index tree with non-leaf node
      // we should close the tree first, and then close the macro block
      if (task_top_block_descs_.count() > 0) {
        if (OB_FAIL(merge_root_micro_block())) {
          STORAGE_LOG(WARN, "Fail to merge index sub-tree", K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(close_index_tree())) {
          STORAGE_LOG(WARN, "Fail to close index tree", K(ret));
        } else if (OB_FAIL(get_root_row(root, root_buf))) {
          STORAGE_LOG(WARN, "Fail to get root micro block", K(ret));
        }
      }
    }
    if (OB_SUCC(ret) && prev_block.is_dirty()) {
      int32_t row_count = prev_block.get_row_count();
      if (data_store_desc_->need_index_tree_ && this == sstable_index_writer_) {
        // need to build index tree with non-leaf node
        if (OB_FAIL(flush_index_macro_block(prev_block))) {
          STORAGE_LOG(WARN, "macro blcok writer fail to flush macro block.", K(ret), K_(current_index));
        }
      } else if (OB_FAIL(flush_macro_block(prev_block))) {
        STORAGE_LOG(WARN, "macro blcok writer fail to flush macro block.", K(ret), K_(current_index));
      }
      if (OB_SUCC(ret) && data_store_desc_->need_prebuild_bloomfilter_) {
        flush_bf_to_cache(prev_bf_writer, row_count);
      }
    }
    if (OB_SUCC(ret) && current_block.is_dirty()) {
      int32_t row_count = current_block.get_row_count();
      if (data_store_desc_->need_index_tree_ && this == sstable_index_writer_) {
        // need to build index tree with non-leaf node
        if (OB_FAIL(flush_index_macro_block(current_block))) {
          STORAGE_LOG(WARN, "macro blcok writer fail to flush macro block.", K(ret), K_(current_index));
        }
      } else if (OB_FAIL(flush_macro_block(current_block))) {
        STORAGE_LOG(WARN, "macro blcok writer fail to flush macro block.", K(ret), K_(current_index));
      }
      if (OB_SUCC(ret) && data_store_desc_->need_prebuild_bloomfilter_) {
        flush_bf_to_cache(current_bf_writer, row_count);
      }
    }
    if (OB_SUCC(ret) && data_store_desc_->need_index_tree_ && this != sstable_index_writer_) {
      // need to build index tree with leaf node
      // we should close the macro block first, and then close the index tree
      if (OB_FAIL(close_index_tree())) {
        STORAGE_LOG(WARN, "Fail to close index tree", K(ret));
      } else if (OB_FAIL(task_index_writer_->close())) {
        STORAGE_LOG(WARN, "Fail to close task index writer", K(ret));
      } else if (OB_FAIL(save_root_micro_block())) {
        STORAGE_LOG(WARN, "Fail to get root micro block", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(wait_io_finish())) {
        STORAGE_LOG(WARN, "Fail to wait io finish, ", K(ret));
      } else {
        pre_deletion_flag_ = false;
        need_deletion_check_ = OB_ISNULL(mark_deletion_maker_) ? false : true;
      }
    }
  }

  return ret;
}

int ObMacroBlockWriter::write_micro_block(const ObMicroBlockDesc& micro_block_desc, int64_t& data_offset)
{
  int ret = OB_SUCCESS;
  if (data_store_desc_->need_index_tree_ && this == sstable_index_writer_) {
    // need to build index tree with non-leaf node
    if (OB_FAIL(macro_blocks_[current_index_].write_micro_block(micro_block_desc, data_offset))) {
      if (OB_BUF_NOT_ENOUGH == ret) {
        if (OB_FAIL(try_switch_macro_block())) {
          STORAGE_LOG(WARN, "Fail to switch macro block, ", K(ret));
        } else if (OB_FAIL(macro_blocks_[current_index_].write_micro_block(micro_block_desc, data_offset))) {
          STORAGE_LOG(WARN, "Fail to write micro block, ", K(ret));
        }
      } else {
        STORAGE_LOG(WARN, "Fail to write micro block, ", K(ret), K(micro_block_desc));
      }
    }
    if (OB_SUCC(ret)) {
      if (macro_blocks_[current_index_].get_data_size() >= data_store_desc_->macro_store_size_) {
        if (OB_FAIL(try_switch_macro_block())) {
          STORAGE_LOG(WARN, "macro block writer fail to try switch macro block.", K(ret));
        }
      }
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "this append interface only for non-leaf node of index tree", K(ret));
  }
  return ret;
}

int ObMacroBlockWriter::get_root_row(ObStoreRow*& root, char* root_buf)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(index_block_builders_.count() <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "the index_block_builders_ is null", K(ret));
  } else {
    IndexMicroBlockBuilder* builder = index_block_builders_.at(index_block_builders_.count() - 1);
    const ObStoreRow* row = NULL;
    if (builder->writer_.get_row_count() <= 0) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "the highest writer is null", K(ret), K(builder->writer_.get_row_count()));
    } else if (OB_FAIL(build_index_micro_block(index_block_builders_.count(), row))) {
      STORAGE_LOG(WARN, "Fail to build index micro block, ", K(ret));
    } else {
      int64_t copy_size = row->get_deep_copy_size();
      int64_t pos = 0;
      if (OB_FAIL(root->deep_copy(*row, root_buf, copy_size, pos))) {
        STORAGE_LOG(WARN, "Fail to deep_copy root row", K(ret));
      }
    }
  }
  for (int32_t i = 0; i < index_block_builders_.count(); i++) {
    index_block_builders_.at(i)->~IndexMicroBlockBuilder();
    allocator_.free(index_block_builders_.at(i));
    index_block_builders_.at(i) = NULL;
  }
  for (int32_t i = 0; i < task_top_block_descs_.count(); i++) {
    task_top_block_descs_.at(i)->~IndexMicroBlockDesc();
    allocator_.free(task_top_block_descs_.at(i));
    task_top_block_descs_.at(i) = NULL;
  }
  index_block_builders_.reset();
  task_top_block_descs_.reset();
  return ret;
}

int ObMacroBlockWriter::get_index_block_desc(IndexMicroBlockDesc*& index_micro_block_desc)
{
  int ret = OB_SUCCESS;
  void* buf = NULL;
  IndexMicroBlockDesc* desc = NULL;
  SpinWLockGuard guard(lock_);
  if (!(data_store_desc_->need_index_tree_ && this == sstable_index_writer_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "this interface only for non-leaf node of index tree", K(ret));
  } else if (OB_ISNULL(buf = allocator_.alloc(sizeof(IndexMicroBlockDesc)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "fail to alloc memory", K(ret));
  } else if (OB_ISNULL(desc = new (buf) IndexMicroBlockDesc())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "fail to new a IndexMicroBlockDesc", K(ret));
  } else if (OB_FAIL(task_top_block_descs_.push_back(desc))) {
    STORAGE_LOG(WARN, "fail to push into index_block_descs", K(ret));
  } else {
    index_micro_block_desc = task_top_block_descs_.at(task_top_block_descs_.count() - 1);
  }
  if (OB_FAIL(ret)) {
    if (OB_NOT_NULL(buf)) {
      allocator_.free(buf);
    }
  }
  return ret;
}

int ObMacroBlockWriter::save_root_micro_block()
{
  int ret = OB_SUCCESS;
  char* block_buffer = NULL;
  int64_t block_size = 0;
  const char* compress_buf = NULL;
  int64_t compress_buf_size = 0;
  ObMicroBlockReader micro_reader;
  ObMicroBlockData block_data;
  if (data_store_desc_->need_index_tree_ && this != sstable_index_writer_) {
    // need to build index tree with leaf node
    IndexMicroBlockBuilder* builder = index_block_builders_.at(index_block_builders_.count() - 1);
    if (OB_FAIL(builder->writer_.build_block(block_buffer, block_size))) {
      STORAGE_LOG(WARN, "Fail to build block, ", K(ret));
    } else if (OB_FAIL(compressor_.compress(block_buffer, block_size, compress_buf, compress_buf_size))) {
      STORAGE_LOG(WARN, "macro block writer fail to compress.", K(ret), K(OB_P(block_buffer)), K(block_size));
    } else if (MICRO_BLOCK_MERGE_VERIFY_LEVEL::NONE != builder->writer_.get_micro_block_merge_verify_level()) {
      block_data.buf_ = compress_buf;
      block_data.size_ = compress_buf_size;
    } else {
      block_data.buf_ = block_buffer;
      block_data.size_ = block_size;
    }
    if (OB_SUCC(ret)) {
      ObStoreRow row;
      IndexMicroBlockDesc* task_top_block_descs;
      if (OB_FAIL(micro_reader.init(block_data, &index_column_map_))) {
        STORAGE_LOG(WARN, "failed to init micro block reader", K(ret));
      } else if (OB_FAIL(sstable_index_writer_->get_index_block_desc(task_top_block_descs))) {
        STORAGE_LOG(WARN, "failed to get index micro block", K(ret));
      } else if (OB_FAIL(task_top_block_descs->writer_.init(index_store_desc_->micro_block_size_limit_,
                     index_store_desc_->rowkey_column_count_,
                     index_store_desc_->row_column_count_))) {  // row column count of the non-leaf node
        STORAGE_LOG(WARN, "Fail to init index micro block writer, ", K(ret));
      } else if (OB_FAIL(last_key_.deep_copy(task_top_block_descs->last_key_,
                     task_top_block_descs->last_key_buf_,
                     common::OB_MAX_ROW_KEY_LENGTH))) {
        STORAGE_LOG(WARN, "Fail to copy last key", K(ret), K_(last_key));
      }
      for (int64_t it = micro_reader.begin(); OB_SUCC(ret) && it != micro_reader.end(); ++it) {
        row.row_val_.cells_ = reinterpret_cast<ObObj*>(checker_obj_buf_);
        row.row_val_.count_ = OB_ROW_MAX_COLUMNS_COUNT;
        if (OB_FAIL(micro_reader.get_row(it, row))) {
          STORAGE_LOG(WARN, "get_row failed", K(ret), K(it), K_(*index_store_desc));
        } else if (OB_FAIL(task_top_block_descs->writer_.append_row(row))) {
          STORAGE_LOG(WARN, "fail to append row", K(ret), K(it), K_(*index_store_desc));
        }
      }
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "don't need the index tree", K(ret), K(*data_store_desc_));
  }

  for (int32_t i = 0; i < index_block_builders_.count(); i++) {
    index_block_builders_.at(i)->~IndexMicroBlockBuilder();
    allocator_.free(index_block_builders_.at(i));
    index_block_builders_.at(i) = NULL;
  }
  index_block_builders_.reset();
  return ret;
}

int ObMacroBlockWriter::do_sort_micro_block()
{
  int ret = OB_SUCCESS;
  if (task_top_block_descs_.count() == 0) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "the index_block_descs_ is null", K(ret));
  } else if (task_top_block_descs_.count() == 1) {
    // nothing to do.
  } else {
    std::sort(task_top_block_descs_.begin(), task_top_block_descs_.end(), IndexMicroBlockDescCompare());
  }
  return ret;
}

int ObMacroBlockWriter::merge_root_micro_block()
{
  int ret = OB_SUCCESS;
  char* block_buffer = NULL;
  int64_t block_size = 0;
  const char* compress_buf = NULL;
  int64_t compress_buf_size = 0;
  ObMicroBlockReader micro_reader;
  ObMicroBlockData block_data;
  IndexMicroBlockBuilder* builder = NULL;
  ObMicroBlockWriter* micro_writer = NULL;
  if (!(data_store_desc_->need_index_tree_ && this == sstable_index_writer_)) {
    // no need to build index tree or with leaf node
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "don't need the index tree", K(ret), K(*data_store_desc_));
  } else if (OB_UNLIKELY(task_top_block_descs_.count() <= 0 || index_block_builders_.count() != 1)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "unexpected error", K(ret), K(task_top_block_descs_.count()), K(index_block_builders_.count()));
  } else if (OB_FAIL(do_sort_micro_block())) {
    STORAGE_LOG(WARN, "fail to do sort root micro block", K(ret));
  } else {
    for (int32_t i = 0; OB_SUCC(ret) && i < task_top_block_descs_.count(); i++) {
      micro_writer = &task_top_block_descs_.at(i)->writer_;
      micro_reader.reset();
      if (OB_FAIL(micro_writer->build_block(block_buffer, block_size))) {
        STORAGE_LOG(WARN, "Fail to build block, ", K(ret));
      } else if (OB_FAIL(compressor_.compress(block_buffer, block_size, compress_buf, compress_buf_size))) {
        STORAGE_LOG(WARN, "macro block writer fail to compress.", K(ret), K(OB_P(block_buffer)), K(block_size));
      } else if (MICRO_BLOCK_MERGE_VERIFY_LEVEL::NONE != micro_writer->get_micro_block_merge_verify_level()) {
        block_data.buf_ = compress_buf;
        block_data.size_ = compress_buf_size;
      } else {
        block_data.buf_ = block_buffer;
        block_data.size_ = block_size;
      }
      if (OB_SUCC(ret)) {
        ObStoreRow row;
        if (OB_FAIL(micro_reader.init(block_data, &index_column_map_))) {
          STORAGE_LOG(WARN, "failed to init micro block reader", K(ret));
        }
        for (int64_t it = micro_reader.begin(); OB_SUCC(ret) && it != micro_reader.end(); ++it) {
          row.row_val_.cells_ = reinterpret_cast<ObObj*>(checker_obj_buf_);
          row.row_val_.count_ = OB_ROW_MAX_COLUMNS_COUNT;
          if (OB_FAIL(micro_reader.get_row(it, row))) {
            STORAGE_LOG(WARN, "get_row failed", K(ret), K(it), K(*data_store_desc_));
          } else if (OB_FAIL(update_index_tree(0 /* update from 0th level */, &row))) {
            STORAGE_LOG(WARN, "fail to update_index_tree", K(ret), K(it), K(*data_store_desc_));
          }
        }
      }
    }
  }
  return ret;
}

int ObMacroBlockWriter::check_order(const ObStoreRow& row)
{
  int ret = OB_SUCCESS;
  const int64_t trans_version_col_idx =
      ObMultiVersionRowkeyHelpper::get_trans_version_col_store_index(data_store_desc_->schema_rowkey_col_cnt_,
          data_store_desc_->rowkey_column_count_ - data_store_desc_->schema_rowkey_col_cnt_);
  const int64_t sql_sequence_col_idx =
      ObMultiVersionRowkeyHelpper::get_sql_sequence_col_store_index(data_store_desc_->schema_rowkey_col_cnt_,
          data_store_desc_->rowkey_column_count_ - data_store_desc_->schema_rowkey_col_cnt_);
  int64_t cur_row_version = 0;
  int64_t cur_sql_sequence = 0;
  if (!row.is_valid() || (!row.is_sparse_row_ && row.row_val_.count_ != data_store_desc_->row_column_count_)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(ERROR,
        "invalid macro block writer input argument.",
        K(row),
        "row_column_count",
        data_store_desc_->row_column_count_,
        K(ret));
  } else if (!data_store_desc_->need_check_order_) {
  } else {
    if (data_store_desc_->is_multi_version_minor_sstable()) {
      ObMacroBlock& curr_block = macro_blocks_[current_index_];
      cur_row_version = row.row_val_.cells_[trans_version_col_idx].get_int();
      cur_sql_sequence = row.row_val_.cells_[sql_sequence_col_idx].get_int();
      if (cur_row_version >= 0 || cur_sql_sequence > 0) {
        bool is_magic_row_flag = false;
        if (OB_FAIL(storage::ObMagicRowManager::is_magic_row(row.row_type_flag_, is_magic_row_flag))) {
          STORAGE_LOG(ERROR, "failed to check magic row", K(ret), K(row));
        } else if (!is_magic_row_flag) {
          ret = OB_ERR_SYS;
          STORAGE_LOG(ERROR,
              "invalid trans_version or sql_sequence",
              K(ret),
              K(row),
              K(trans_version_col_idx),
              K(sql_sequence_col_idx));
        }
      } else if (!row.row_type_flag_.is_uncommitted_row()) {  // update max commit version
        micro_writer_->update_max_merged_trans_version(-cur_row_version);
      } else {  // not committed
        micro_writer_->set_contain_uncommitted_row();
        LOG_TRACE("meet uncommited trans row", K(row));
      }
    }
    if (OB_SUCC(ret) && last_key_.is_valid()) {
      ObStoreRowkey cur_key;
      cur_key.assign(row.row_val_.cells_, data_store_desc_->schema_rowkey_col_cnt_);
      ObStoreRowkey last_key;
      last_key.assign(const_cast<ObObj*>(last_key_.get_obj_ptr()), data_store_desc_->schema_rowkey_col_cnt_);

      int32_t compare_result = 0;
      if (OB_NOT_NULL(rowkey_helper_)) {
        compare_result = rowkey_helper_->compare(cur_key, last_key);
      } else {
        compare_result = cur_key.compare(last_key);
      }
      if (OB_UNLIKELY(compare_result < 0)) {
        ret = OB_ROWKEY_ORDER_ERROR;
        STORAGE_LOG(ERROR, "input rowkey is less then last rowkey.", K(cur_key), K(last_key), K(ret));
      } else if (OB_UNLIKELY(0 == compare_result)) {  // same schema rowkey
        if (last_key_with_L_flag_) {
          ret = OB_ROWKEY_ORDER_ERROR;
          STORAGE_LOG(ERROR, "have output row with L flag before", K(ret), K(last_key_), K(row));
        } else if (data_store_desc_->is_multi_version_minor_sstable()) {
          // dump data, check version
          int64_t last_row_version = last_key_.get_obj_ptr()[trans_version_col_idx].get_int();
          if (cur_row_version < last_row_version) {
            ret = OB_ROWKEY_ORDER_ERROR;
            STORAGE_LOG(ERROR,
                "cur row version is less than last row version, ",
                K(ret),
                K(cur_row_version),
                K(last_row_version));
          } else if (cur_row_version == last_row_version) {
            int64_t last_row_sql_seq = last_key_.get_obj_ptr()[sql_sequence_col_idx].get_int();
            if (cur_sql_sequence <= last_row_sql_seq) {
              ret = OB_ROWKEY_ORDER_ERROR;
              STORAGE_LOG(ERROR, "cur row sql_sequence is less than last row", K(ret), K_(last_key), K(row));
            }
          }
        } else {
          // baseline data
          ret = OB_ERR_PRIMARY_KEY_DUPLICATE;
          STORAGE_LOG(ERROR, "input rowkey is equal with last rowkey", K(cur_key), K(last_key), K(ret));
        }
      } else {
        // normal case
        if (need_deletion_check_) {
          need_deletion_check_ =
              (row.row_type_flag_.is_compacted_multi_version_row() && row.dml_ == T_DML_DELETE) ? true : false;
        }
      }
    }

    if (OB_SUCC(ret)) {
      ObStoreRowkey rowkey;
      rowkey.assign(row.row_val_.cells_, data_store_desc_->rowkey_column_count_);
      last_key_with_L_flag_ = row.row_type_flag_.is_last_multi_version_row();

      if (OB_FAIL(save_last_key(rowkey))) {
        STORAGE_LOG(WARN, "Fail to save last rowkey, ", K(ret), K(rowkey));
      }
    } else if (OB_ROWKEY_ORDER_ERROR == ret) {
      // print micro block have output
      dump_micro_block_writer_buffer();  // ignore failure
    }
  }
  return ret;
}

int ObMacroBlockWriter::build_micro_block(const bool force_split)
{
  int ret = OB_SUCCESS;
  char* block_buffer = NULL;
  int64_t block_size = 0;
  bool mark_deletion = false;
  ObMicroBlockDesc micro_block_desc;

  if (micro_writer_->get_row_count() <= 0) {
    ret = OB_INNER_STAT_ERROR;
    STORAGE_LOG(WARN, "micro_block_writer is empty", K(ret));
  } else if (OB_FAIL(micro_writer_->build_block(block_buffer, block_size))) {
    STORAGE_LOG(WARN, "Fail to build block, ", K(ret));
  } else if (OB_FAIL(
                 compressor_.compress(block_buffer, block_size, micro_block_desc.buf_, micro_block_desc.buf_size_))) {
    STORAGE_LOG(WARN, "macro block writer fail to compress.", K(ret), K(OB_P(block_buffer)), K(block_size));
  } else if (MICRO_BLOCK_MERGE_VERIFY_LEVEL::NONE != micro_writer_->get_micro_block_merge_verify_level() &&
             OB_FAIL(check_micro_block(
                 micro_block_desc.buf_, micro_block_desc.buf_size_, block_buffer, block_size, micro_writer_))) {
    STORAGE_LOG(WARN, "failed to check micro block", K(ret));
  } else if (OB_FAIL(can_mark_deletion(pre_micro_last_key_, last_key_, mark_deletion))) {
    STORAGE_LOG(WARN, "fail to run can mark deletion", K(ret));
  } else if (OB_FAIL(save_pre_micro_last_key(last_key_))) {
    STORAGE_LOG(WARN, "Fail to save pre micro last key, ", K(ret), K_(last_key));
  } else {
    micro_block_desc.last_rowkey_ = micro_writer_->get_last_rowkey();
    micro_block_desc.data_size_ = micro_writer_->get_data_size();
    micro_block_desc.row_count_ = micro_writer_->get_row_count();
    micro_block_desc.column_count_ = micro_writer_->get_column_count();
    micro_block_desc.row_count_delta_ = micro_writer_->get_row_count_delta();
    micro_block_desc.can_mark_deletion_ = mark_deletion;
    micro_block_desc.column_checksums_ =
        data_store_desc_->need_calc_column_checksum_ ? curr_micro_column_checksum_ : NULL;
    if (data_store_desc_->is_multi_version_minor_sstable()) {
      micro_block_desc.max_merged_trans_version_ = micro_writer_->get_max_merged_trans_version();
      micro_block_desc.contain_uncommitted_row_ = micro_writer_->is_contain_uncommitted_row();
    }
    if (OB_FAIL(write_micro_block(micro_block_desc, force_split))) {
      STORAGE_LOG(WARN, "build_micro_block failed", K(micro_block_desc), K(force_split), K(ret));
    } else {
      micro_writer_->reuse();
      if (data_store_desc_->need_prebuild_bloomfilter_ && micro_rowkey_hashs_.count() > 0) {
        micro_rowkey_hashs_.reuse();
      }
    }
  }
  STORAGE_LOG(
      DEBUG, "build micro block desc", K(data_store_desc_->table_id_), K(micro_block_desc), "lbt", lbt(), K(ret));
  return ret;
}

int ObMacroBlockWriter::build_micro_block_desc(const ObMicroBlock& micro_block, ObMicroBlockDesc& micro_block_desc)
{
  int ret = OB_SUCCESS;
  if (RECORD_HEADER_VERSION_V2 != micro_block.header_version_ &&
      RECORD_HEADER_VERSION_V3 != micro_block.header_version_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(micro_block));
  } else if (RECORD_HEADER_VERSION_V3 == micro_block.header_version_ &&
             micro_block.meta_.meta_->schema_version_ == data_store_desc_->schema_version_) {
    if (OB_FAIL(build_micro_block_desc_with_reuse(micro_block, micro_block_desc))) {
      LOG_WARN("fail to build micro block desc v3", K(ret), K(micro_block), K(micro_block_desc));
    }
  } else {
    if (OB_FAIL(build_micro_block_desc_with_rewrite(micro_block, micro_block_desc))) {
      LOG_WARN("fail to build micro block desc v2", K(ret), K(micro_block), K(micro_block_desc));
    }
  }
  STORAGE_LOG(DEBUG, "build micro block desc", K(micro_block), K(micro_block_desc));
  return ret;
}

int ObMacroBlockWriter::build_micro_block_desc_with_reuse(
    const ObMicroBlock& micro_block, ObMicroBlockDesc& micro_block_desc)
{
  int ret = OB_SUCCESS;
  int64_t rowkey_length = 0;
  ObNewRow rowkey;
  bool mark_deletion = false;
  rowkey.cells_ = const_cast<ObObj*>(micro_block.range_.get_end_key().get_obj_ptr());
  rowkey.count_ = micro_block.range_.get_end_key().get_obj_cnt();
  MEMSET(rowkey_buf_, 0, OB_MAX_ROW_KEY_LENGTH);

  if (OB_FAIL(row_writer_.write(
          rowkey, rowkey_buf_, OB_MAX_ROW_KEY_LENGTH, data_store_desc_->row_store_type_, rowkey_length))) {
    LOG_WARN("row_writer_ write failed", K(rowkey), "buf_length", OB_MAX_ROW_KEY_LENGTH, K(ret));
  } else if (OB_FAIL(can_mark_deletion(pre_micro_last_key_, micro_block.range_.get_end_key(), mark_deletion))) {
    LOG_WARN("fail to run can mark deletion", K(ret));
  } else if (OB_FAIL(save_last_key(micro_block.range_.get_end_key()))) {
    LOG_WARN("Fail to save last key, ", K(ret), K(micro_block.range_.get_end_key()));
  } else if (OB_FAIL(save_pre_micro_last_key(micro_block.range_.get_end_key()))) {
    LOG_WARN("Fail to save pre micro last key, ", K(ret), K(micro_block.range_.get_end_key()));
  } else {
    micro_block_desc.last_rowkey_.assign_ptr(rowkey_buf_, static_cast<ObString::obstr_size_t>(rowkey_length));
    micro_block_desc.data_size_ = micro_block.origin_data_size_;
    micro_block_desc.column_count_ = data_store_desc_->row_column_count_;
    micro_block_desc.can_mark_deletion_ = mark_deletion;
    micro_block_desc.row_count_ = micro_block.row_count_;
    micro_block_desc.column_checksums_ = micro_block.column_checksums_;
    micro_block_desc.buf_size_ = micro_block.payload_data_.get_buf_size();
    micro_block_desc.buf_ = micro_block.payload_data_.get_buf();
  }
  STORAGE_LOG(
      DEBUG, "build micro block desc reuse", K(data_store_desc_->table_id_), K(micro_block_desc), "lbt", lbt(), K(ret));
  return ret;
}

int ObMacroBlockWriter::build_micro_block_desc_with_rewrite(
    const ObMicroBlock& micro_block, ObMicroBlockDesc& micro_block_desc)
{
  int ret = OB_SUCCESS;
  int64_t rowkey_length = 0;
  ObNewRow rowkey;
  rowkey.cells_ = const_cast<ObObj*>(micro_block.range_.get_end_key().get_obj_ptr());
  rowkey.count_ = micro_block.range_.get_end_key().get_obj_cnt();
  MEMSET(rowkey_buf_, 0, OB_MAX_ROW_KEY_LENGTH);
  ObIMicroBlockReader* reader = NULL;
  bool mark_deletion = false;
  ObMicroBlockData decompressed_data;
  if (OB_UNLIKELY(!micro_block.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(micro_block));
  } else if (NULL == (reader = get_micro_block_reader(micro_block.row_store_type_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("reader is null", K(ret), KP(reader));
  } else {
    bool is_compressed = false;
    reader->reset();
    if (OB_FAIL(macro_reader_.decompress_data(micro_block.meta_,
            micro_block.data_.get_buf(),
            micro_block.data_.get_buf_size(),
            decompressed_data.get_buf(),
            decompressed_data.get_buf_size(),
            is_compressed))) {
      LOG_WARN("fail to decompress data", K(ret));
    } else if (OB_FAIL(reader->init(decompressed_data, micro_block.column_map_))) {
      LOG_WARN("reader init failed", K(micro_block), K(ret));
    } else if (OB_FAIL(row_writer_.write(
                   rowkey, rowkey_buf_, OB_MAX_ROW_KEY_LENGTH, data_store_desc_->row_store_type_, rowkey_length))) {
      LOG_WARN("row_writer_ write failed", K(rowkey), "buf_length", OB_MAX_ROW_KEY_LENGTH, K(ret));
    } else if (OB_FAIL(can_mark_deletion(pre_micro_last_key_, micro_block.range_.get_end_key(), mark_deletion))) {
      LOG_WARN("fail to run can mark deletion", K(ret));
    } else if (OB_FAIL(save_last_key(micro_block.range_.get_end_key()))) {
      LOG_WARN("Fail to save last key, ", K(ret), K(micro_block.range_.get_end_key()));
    } else if (OB_FAIL(save_pre_micro_last_key(micro_block.range_.get_end_key()))) {
      LOG_WARN("Fail to save pre micro last key, ", K(ret), K(micro_block.range_.get_end_key()));
    } else {
      micro_block_desc.last_rowkey_.assign_ptr(rowkey_buf_, static_cast<ObString::obstr_size_t>(rowkey_length));
      micro_block_desc.data_size_ = micro_block.origin_data_size_;
      micro_block_desc.column_count_ = data_store_desc_->row_column_count_;
      micro_block_desc.can_mark_deletion_ = mark_deletion;
      micro_block_desc.buf_ = micro_block.payload_data_.get_buf();
      micro_block_desc.buf_size_ = micro_block.payload_data_.get_buf_size();
      if (OB_FAIL(reader->get_row_count(micro_block_desc.row_count_))) {
        LOG_WARN("get_row_count failed", K(ret));
      } else if (data_store_desc_->need_calc_column_checksum_) {
        micro_block_desc.column_checksums_ = curr_micro_column_checksum_;
        MEMSET(micro_block_desc.column_checksums_, 0, sizeof(int64_t) * data_store_desc_->row_column_count_);
        if (OB_FAIL(calc_micro_column_checksum(
                micro_block_desc.column_count_, *reader, micro_block_desc.column_checksums_))) {
          STORAGE_LOG(WARN, "fail to calc micro block column checksum", K(ret));
        }
      }
    }
  }
  STORAGE_LOG(DEBUG,
      "build micro block desc rewrite",
      K(data_store_desc_->table_id_),
      K(micro_block_desc),
      "lbt",
      lbt(),
      K(ret));
  return ret;
}

int ObMacroBlockWriter::write_micro_block(const ObMicroBlockDesc& micro_block_desc, const bool force_split)
{
  int ret = OB_SUCCESS;
  int64_t data_offset = 0;
  if (OB_FAIL(macro_blocks_[current_index_].write_micro_block(micro_block_desc, data_offset))) {
    if (OB_BUF_NOT_ENOUGH == ret) {
      if (OB_FAIL(try_switch_macro_block())) {
        STORAGE_LOG(WARN, "Fail to switch macro block, ", K(ret));
      } else if (OB_FAIL(macro_blocks_[current_index_].write_micro_block(micro_block_desc, data_offset))) {
        STORAGE_LOG(WARN, "Fail to write micro block, ", K(ret));
      }
    } else {
      STORAGE_LOG(ERROR, "Fail to write micro block, ", K(ret), K(micro_block_desc));
    }
  }

  if (OB_SUCC(ret)) {
    if (data_store_desc_->need_prebuild_bloomfilter_) {
      // if macro has micro reuse, don't build bloomfilter
      ObMacroBloomFilterCacheWriter& current_writer = bf_cache_writer_[current_index_];
      if (!current_writer.is_valid() && 0 == data_store_desc_->bloomfilter_size_) {
        // Estimate macroblock rowcount base on micro_block_desc
        data_store_desc_->bloomfilter_size_ =
            data_store_desc_->macro_block_size_ / (micro_block_desc.buf_size_ + 1) * micro_block_desc.row_count_;
        if (OB_FAIL(open_bf_cache_writer(*data_store_desc_))) {
          STORAGE_LOG(WARN, "Failed to open bloomfilter cache writer, ", K(ret));
          ret = OB_SUCCESS;
        }
      }
      if (micro_rowkey_hashs_.count() != micro_block_desc.row_count_) {
        // count=0 ,when micro block reused
        if (OB_UNLIKELY(micro_rowkey_hashs_.count() > 0)) {
          STORAGE_LOG(WARN,
              "build bloomfilter: micro_rowkey_hashs_ and micro_block_desc count not same ",
              K(micro_rowkey_hashs_.count()),
              K(micro_block_desc.row_count_));
        }
        current_writer.set_not_need_build();
      } else if (current_writer.is_need_build() &&
                 OB_LIKELY(current_writer.get_rowkey_column_count() == data_store_desc_->bloomfilter_rowkey_prefix_) &&
                 OB_FAIL(current_writer.append(micro_rowkey_hashs_))) {
        STORAGE_LOG(WARN, "Fail to append rowkey hash to macro block, ", K(ret));
        current_writer.set_not_need_build();
        ret = OB_SUCCESS;
      }
      micro_rowkey_hashs_.reuse();
    }
    if (force_split || macro_blocks_[current_index_].get_data_size() >= data_store_desc_->macro_store_size_) {
      if (OB_FAIL(try_switch_macro_block())) {
        STORAGE_LOG(WARN, "macro block writer fail to try switch macro block.", K(ret));
      }
    }
    need_deletion_check_ = OB_ISNULL(mark_deletion_maker_) ? false : true;
  }

  if (OB_SUCC(ret) && NULL != data_store_desc_->merge_info_) {
    data_store_desc_->merge_info_->rewrite_macro_total_micro_block_count_++;
  }

  if (OB_SUCC(ret) && data_store_desc_->need_calc_column_checksum_) {
    MEMSET(curr_micro_column_checksum_, 0, sizeof(int64_t) * data_store_desc_->row_column_count_);
  }

  return ret;
}

int ObMacroBlockWriter::flush_macro_block(ObMacroBlock& macro_block)
{
  int ret = OB_SUCCESS;
  ObString last_rowkey;
  if (OB_FAIL(wait_io_finish())) {
    STORAGE_LOG(WARN, "Fail to wait io finish, ", K(ret));
  } else if (OB_FAIL(macro_block.get_last_rowkey(last_rowkey))) {
    STORAGE_LOG(WARN, "fail to get last rowkey", K(ret));
  } else if (OB_FAIL(macro_block.flush(current_macro_seq_, macro_handle_, block_write_ctx_))) {
    STORAGE_LOG(WARN, "macro block writer fail to flush macro block.", K(ret));
  } else if (data_store_desc_->need_index_tree_ && this != sstable_index_writer_) {
    // need to build index tree with leaf node
    IndexMicroBlockBuilder* builder = NULL;
    const ObStoreRow* leaf_row = NULL;
    ObBlockIntermediateHeader header(index_store_desc_->data_version_,
        current_macro_seq_,
        macro_block.get_index_offset(),
        macro_block.get_index_size(),
        macro_block.get_row_count(),
        true);
    if (OB_ISNULL(builder = index_block_builders_.at(0))) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "fail to get the 0th index_block_builder", K(ret));
    } else if (OB_FAIL(build_intermediate_row(last_rowkey, header, builder->row_builder_, leaf_row))) {
      STORAGE_LOG(WARN, "fail to build intermediate row", K(ret));
    } else if (OB_FAIL(update_index_tree(0 /* update from 0th level */, leaf_row))) {
      STORAGE_LOG(WARN, "macro blcok writer fail to update index tree.", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(macro_block.init(*data_store_desc_))) {
      STORAGE_LOG(WARN, "macro block writer fail to init.", K(ret));
    } else {
      ++current_macro_seq_;
    }
  }
  return ret;
}

int ObMacroBlockWriter::flush_index_macro_block(ObMacroBlock& macro_block)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(wait_io_finish())) {
    STORAGE_LOG(WARN, "Fail to wait io finish, ", K(ret));
  } else if (OB_FAIL(macro_block.flush(current_macro_seq_, macro_handle_, block_write_ctx_))) {
    STORAGE_LOG(WARN, "macro block writer fail to flush macro block.", K(ret));
  } else if (OB_FAIL(macro_block.init(*data_store_desc_))) {
    STORAGE_LOG(WARN, "macro block writer fail to init.", K(ret));
  } else {
    ++current_macro_seq_;
  }
  return ret;
}

int ObMacroBlockWriter::flush_reuse_macro_block(const ObMacroBlockCtx& macro_block_ctx)
{
  int ret = OB_SUCCESS;

  ObFullMacroBlockMeta meta;
  ObMacroBlockWriteInfo write_info;
  write_info.buffer_ = NULL;
  write_info.size_ = 0;
  write_info.meta_.reset();
  write_info.io_desc_.category_ = SYS_IO;
  write_info.io_desc_.wait_event_no_ = ObWaitEventIds::DB_FILE_COMPACT_WRITE;
  write_info.block_write_ctx_ = &block_write_ctx_;
  write_info.reuse_block_ctx_ = &macro_block_ctx;

  if (OB_FAIL(wait_io_finish())) {
    STORAGE_LOG(WARN, "Fail to wait io finish, ", K(ret));
  } else if (OB_FAIL(macro_block_ctx.sstable_->get_meta(macro_block_ctx.get_macro_block_id(), meta))) {
    STORAGE_LOG(WARN, "fail to get meta", K(ret));
  } else if (!meta.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "error unexpected, meta must not be null", K(ret), K(meta));
  } else {
    write_info.meta_ = meta;
    ObStorageFile* file = data_store_desc_->file_handle_.get_storage_file();
    if (OB_ISNULL(file)) {
      ret = OB_ERR_SYS;
      STORAGE_LOG(WARN, "pg_file_ is null", K(ret), KP(file));
    } else if (FALSE_IT(macro_handle_.set_file(file))) {
    } else if (OB_FAIL(file->async_write_block(write_info, macro_handle_))) {
      STORAGE_LOG(WARN, "Fail to async write block, ", K(ret));
    }
  }

  return ret;
}

int ObMacroBlockWriter::try_switch_macro_block()
{
  int ret = OB_SUCCESS;
  current_index_ = (current_index_ + 1) % 2;
  ObMacroBlock& macro_block = macro_blocks_[current_index_];
  if (macro_block.is_dirty()) {
    int32_t row_count = macro_block.get_row_count();
    if (data_store_desc_->need_index_tree_ && this == sstable_index_writer_) {
      // need to build index tree with non-leaf node
      if (OB_FAIL(flush_index_macro_block(macro_block))) {
        STORAGE_LOG(WARN, "macro blcok writer fail to flush macro block.", K(ret), K_(current_index));
      }
    } else if (OB_FAIL(flush_macro_block(macro_block))) {
      STORAGE_LOG(WARN, "macro blcok writer fail to flush macro block.", K(ret), K_(current_index));
    }
    if (OB_SUCC(ret) && data_store_desc_->need_prebuild_bloomfilter_) {
      flush_bf_to_cache(bf_cache_writer_[current_index_], row_count);
    }
  }
  return ret;
}

int ObMacroBlockWriter::check_write_complete(const MacroBlockId& macro_block_id)
{
  int ret = OB_SUCCESS;

  ObMacroBlockReadInfo read_info;
  ObMacroBlockCtx macro_block_ctx;
  macro_block_ctx.file_ctx_ = &block_write_ctx_.file_ctx_;
  macro_block_ctx.sstable_block_id_.macro_block_id_ = macro_block_id;
  read_info.macro_block_ctx_ = &macro_block_ctx;
  read_info.size_ = OB_FILE_SYSTEM.get_macro_block_size();
  read_info.io_desc_.category_ = SYS_IO;
  read_info.io_desc_.wait_event_no_ = ObWaitEventIds::DB_FILE_COMPACT_READ;
  const int64_t io_timeout_ms = std::max(GCONF._data_storage_io_timeout / 1000, DEFAULT_IO_WAIT_TIME_MS);
  ObFullMacroBlockMeta meta;
  const int64_t macro_meta_cnt = block_write_ctx_.macro_block_meta_list_.count();
  ObMacroBlockHandle read_handle;
  ObStorageFile* file = data_store_desc_->file_handle_.get_storage_file();
  if (OB_ISNULL(file)) {
    ret = OB_ERR_SYS;
    STORAGE_LOG(WARN, "pg_file_ is null", K(ret), KP(file));
  } else if (FALSE_IT(read_handle.set_file(file))) {
  } else if (OB_FAIL(file->async_read_block(read_info, read_handle))) {
    STORAGE_LOG(WARN, "fail to async read macro block", K(ret), K(read_info));
  } else if (OB_FAIL(read_handle.wait(io_timeout_ms))) {
    STORAGE_LOG(WARN, "fail to wait io finish", K(ret), K(io_timeout_ms));
  } else if (OB_FAIL(block_write_ctx_.macro_block_meta_list_.at(macro_meta_cnt - 1, meta))) {
    STORAGE_LOG(WARN, "fail to get macro block meta", K(ret));
  } else if (!meta.is_valid()) {
    ret = OB_ERR_SYS;
    STORAGE_LOG(WARN, "error sys, meta must not be null", K(ret), K(meta));
  } else if (OB_FAIL(macro_block_checker_.check(read_handle.get_buffer(),
                 read_handle.get_data_size(),
                 meta,
                 ObMacroBlockCheckLevel::CHECK_LEVEL_MACRO))) {
    STORAGE_LOG(WARN, "fail to verity macro block", K(ret));
  }
  return ret;
}

int ObMacroBlockWriter::wait_io_finish()
{
  int ret = OB_SUCCESS;
  const int64_t io_timeout_ms = std::max(GCONF._data_storage_io_timeout / 1000, DEFAULT_IO_WAIT_TIME_MS);
  if (OB_FAIL(macro_handle_.wait(io_timeout_ms))) {
    STORAGE_LOG(WARN, "macro block writer fail to wait io finish", K(ret), K(io_timeout_ms));
  } else {
    if (!macro_handle_.is_empty()) {
      int64_t check_level = 0;
      if (OB_ISNULL(micro_writer_)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "micro_writer is null", K(ret));
      } else if (FALSE_IT(check_level = micro_writer_->get_micro_block_merge_verify_level())) {
      } else if (MICRO_BLOCK_MERGE_VERIFY_LEVEL::ENCODING_AND_COMPRESSION_AND_WRITE_COMPLETE == check_level) {
        if (OB_FAIL(check_write_complete(macro_handle_.get_macro_id()))) {
          STORAGE_LOG(WARN, "fail to check io complete", K(ret));
        }
      }
    }
    macro_handle_.reset();
  }
  return ret;
}

int ObMacroBlockWriter::check_micro_block_need_merge(const ObMicroBlock& micro_block, bool& need_merge)
{
  int ret = OB_SUCCESS;
  need_merge = true;
  if (!micro_block.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid micro_block", K(micro_block), K(ret));
  } else {
    if (micro_writer_->get_row_count() <= 0 &&
        micro_block.origin_data_size_ > data_store_desc_->micro_block_size_ / 2) {
      need_merge = false;
    } else if (micro_writer_->get_block_size() > data_store_desc_->micro_block_size_ / 2 &&
               micro_block.origin_data_size_ > data_store_desc_->micro_block_size_ / 2) {
      need_merge = false;
    } else {
      need_merge = true;
    }
    STORAGE_LOG(DEBUG,
        "check micro block need merge",
        K(micro_writer_->get_row_count()),
        K(micro_block.data_.get_buf_size()),
        K(micro_writer_->get_block_size()),
        K(data_store_desc_->micro_block_size_),
        K(need_merge));
  }
  return ret;
}

int ObMacroBlockWriter::merge_micro_block(const ObMicroBlock& micro_block)
{
  int ret = OB_SUCCESS;
  ObIMicroBlockReader* micro_reader = NULL;

  if (NULL == data_store_desc_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not opened", K(ret));
  } else if (NULL == (micro_reader = get_micro_block_reader(micro_block.row_store_type_))) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "The micro reader is NULL, ", K(ret));
  } else if (!micro_block.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid micro_block", K(micro_block), K(ret));
  } else if (data_store_desc_->is_multi_version_minor_sstable()) {
    // forbid micro block level merge for minor merge now
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "minor merge does not allow micro block level merge", K(ret));
  } else {
    int64_t split_size = 0;
    const int64_t merged_size = micro_writer_->get_block_size() + micro_block.origin_data_size_;
    ObMicroBlockData decompressed_data;
    bool is_compressed = false;
    if (merged_size > 2 * data_store_desc_->micro_block_size_) {
      split_size = merged_size / 2;
    } else {
      split_size = 2 * data_store_desc_->micro_block_size_;
    }

    micro_reader->reset();
    if (OB_FAIL(macro_reader_.decompress_data(micro_block.meta_,
            micro_block.data_.get_buf(),
            micro_block.data_.get_buf_size(),
            decompressed_data.get_buf(),
            decompressed_data.get_buf_size(),
            is_compressed))) {
      STORAGE_LOG(WARN, "fail to decompress data", K(ret));
    } else if (OB_FAIL(micro_reader->init(decompressed_data, micro_block.column_map_))) {
      STORAGE_LOG(WARN, "micro_block_reader init failed", K(micro_block), K(ret));
    } else {
      ObStoreRow row;
      for (int64_t it = micro_reader->begin(); OB_SUCC(ret) && it != micro_reader->end(); ++it) {
        row.row_val_.cells_ = reinterpret_cast<ObObj*>(obj_buf_);
        row.row_val_.count_ = OB_ROW_MAX_COLUMNS_COUNT;
        row.capacity_ = OB_ROW_MAX_COLUMNS_COUNT;
        if (OB_FAIL(micro_reader->get_row(it, row))) {
          STORAGE_LOG(WARN, "get_row failed", K(ret));
        } else if (OB_FAIL(append_row(row, split_size, true))) {
          STORAGE_LOG(WARN, "append_row failed", "row", row, K(split_size), K(ret));
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

int ObMacroBlockWriter::can_mark_deletion(
    const ObStoreRowkey& start_key, const ObStoreRowkey& end_key, bool& can_mark_deletion)
{
  int ret = OB_SUCCESS;
  can_mark_deletion = false;

  if (OB_ISNULL(mark_deletion_maker_)) {
    // do nothing
  } else {
    if (need_deletion_check_) {
      ObExtStoreRange range;
      range.get_range().set_table_id(data_store_desc_->table_id_);
      range.get_range().get_end_key().assign(
          const_cast<ObObj*>(end_key.get_obj_ptr()), data_store_desc_->schema_rowkey_col_cnt_);
      // right closed
      range.get_range().get_border_flag().set_inclusive_end();
      if (!start_key.is_valid()) {
        range.get_range().get_start_key().set_min();
      } else {
        range.get_range().get_start_key().assign(
            const_cast<ObObj*>(start_key.get_obj_ptr()), data_store_desc_->schema_rowkey_col_cnt_);
      }
      // left opened
      range.get_range().get_border_flag().unset_inclusive_start();

      if (range.get_range().empty()) {
        can_mark_deletion = pre_deletion_flag_;
      } else {
        if (OB_FAIL(mark_deletion_maker_->can_mark_delete(can_mark_deletion, range))) {
          STORAGE_LOG(WARN, "fail to check can mark delete", K(ret));
        } else {
          pre_deletion_flag_ = can_mark_deletion;
        }
      }
    }
  }
  return ret;
}

int ObMacroBlockWriter::save_last_key(const ObStoreRowkey& last_key)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(last_key.deep_copy(last_key_, last_key_buf_, common::OB_MAX_ROW_KEY_LENGTH))) {
    STORAGE_LOG(WARN, "Fail to copy last key, ", K(ret), K(last_key));
  } else {
    STORAGE_LOG(DEBUG, "save last key", K(last_key_));
  }
  return ret;
}

int ObMacroBlockWriter::save_pre_micro_last_key(const ObStoreRowkey& pre_micro_last_key)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(
          pre_micro_last_key.deep_copy(pre_micro_last_key_, pre_micro_last_key_buf_, common::OB_MAX_ROW_KEY_LENGTH))) {
    STORAGE_LOG(WARN, "fail to copy macro block end key", K(ret), K(pre_micro_last_key));
  } else {
    STORAGE_LOG(DEBUG, "save pre micro last key", K(pre_micro_last_key_));
  }
  return ret;
}

int ObMacroBlockWriter::add_row_checksum(const common::ObNewRow& row)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!row.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", K(ret), K(row));
  } else {
    for (int64_t i = 0; i < row.count_; ++i) {
      curr_micro_column_checksum_[i] += row.cells_[i].checksum_v2(0);
    }
  }
  return ret;
}

int ObMacroBlockWriter::calc_micro_column_checksum(
    const int64_t column_cnt, ObIMicroBlockReader& reader, int64_t* column_checksum)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(NULL == column_checksum || column_cnt <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", K(ret), KP(column_checksum), K(column_cnt));
  } else {
    ObStoreRow row;
    for (int64_t iter = reader.begin(); OB_SUCC(ret) && iter != reader.end(); ++iter) {
      row.row_val_.cells_ = reinterpret_cast<ObObj*>(checker_obj_buf_);
      row.row_val_.count_ = OB_ROW_MAX_COLUMNS_COUNT;
      row.capacity_ = OB_ROW_MAX_COLUMNS_COUNT;
      if (OB_FAIL(reader.get_row(iter, row))) {
        STORAGE_LOG(WARN, "fail to get row", K(ret), K(iter));
      } else if (row.row_val_.count_ != column_cnt) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(
            WARN, "error unexpected, row column count is invalid", K(ret), K(row.row_val_.count_), K(column_cnt));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < column_cnt; ++i) {
          column_checksum[i] += row.row_val_.cells_[i].checksum_v2(0);
        }
      }
    }
  }
  return ret;
}

OB_INLINE ObIMicroBlockReader* ObMacroBlockWriter::get_micro_block_reader(const int64_t row_store_type)
{
  ObIMicroBlockReader* reader = NULL;
  switch (row_store_type) {
    case FLAT_ROW_STORE: {
      reader = &flat_reader_;
      break;
    }
    case SPARSE_ROW_STORE: {
      reader = &sparse_reader_;
      break;
    }
    default:
      STORAGE_LOG(WARN, "invalid store type", K(row_store_type));
      break;
  }
  return reader;
}

int ObMacroBlockWriter::build_column_map(const ObDataStoreDesc* data_desc, ObColumnMap& column_map)
{
  int ret = OB_SUCCESS;
  ObColDesc col_desc;
  ObSEArray<share::schema::ObColDesc, OB_DEFAULT_SE_ARRAY_COUNT> out_cols;
  ObSEArray<share::schema::ObColDesc, OB_DEFAULT_SE_ARRAY_COUNT> index_out_cols;
  if (OB_ISNULL(data_desc)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", K(ret), KP(data_desc));
  } else {
    column_map.reset();
    for (int64_t i = 0; OB_SUCC(ret) && i < data_desc->row_column_count_; ++i) {
      col_desc.col_id_ = data_desc->column_ids_[i];
      col_desc.col_type_ = data_desc->column_types_[i];
      col_desc.col_order_ = data_desc->column_orders_[i];
      if (OB_FAIL(out_cols.push_back(col_desc))) {
        STORAGE_LOG(WARN, "Fail to push col desc to array, ", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(column_map.init(allocator_,
              data_desc->schema_version_,
              data_desc->rowkey_column_count_,
              data_desc->row_column_count_,
              out_cols))) {
        STORAGE_LOG(WARN, "failed to init column map", K(ret), K(data_desc));
      }
    }
  }
  return ret;
}

int ObMacroBlockWriter::prepare_micro_block_reader(
    const char* buf, const int64_t size, ObIMicroBlockReader*& micro_reader)
{
  int ret = OB_SUCCESS;
  micro_reader = nullptr;
  ObMicroBlockData block_data;
  block_data.buf_ = buf;
  block_data.size_ = size;
  ObRowStoreType read_out_type = MAX_ROW_STORE;
  ObColumnMap* column_map_ptr = nullptr;
  if (data_store_desc_->need_index_tree_) {
    micro_reader = static_cast<ObIMicroBlockReader*>(&check_flat_reader_);
    if (OB_FAIL(micro_reader->init(block_data, &index_column_map_))) {
      STORAGE_LOG(WARN, "failed to init micro block reader", K(ret));
    }
  } else {
    if (FLAT_ROW_STORE == data_store_desc_->row_store_type_) {
      micro_reader = static_cast<ObIMicroBlockReader*>(&check_flat_reader_);
      read_out_type = FLAT_ROW_STORE;
      column_map_ptr = &column_map_;
    } else if (SPARSE_ROW_STORE == data_store_desc_->row_store_type_) {
      micro_reader = static_cast<ObIMicroBlockReader*>(&check_sparse_reader_);
      read_out_type = SPARSE_ROW_STORE;  // read row type is sparse row
      column_map_ptr = nullptr;          // make reader read full sparse row
    } else {
      ret = OB_NOT_SUPPORTED;
      STORAGE_LOG(WARN, "Unexpeceted row store type", K(ret), K(data_store_desc_->row_store_type_));
    }
    if (OB_FAIL(ret)) {

    } else if (OB_FAIL(micro_reader->init(block_data, column_map_ptr, read_out_type))) {
      STORAGE_LOG(WARN, "failed to init micro block reader", K(ret), K(block_data));
    }
  }
  return ret;
}

int ObMacroBlockWriter::print_micro_block_row(ObIMicroBlockReader* micro_reader)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(micro_reader)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "get_row failed", K(ret), K(micro_reader));
  } else {
    ObStoreRow row;
    row.row_val_.cells_ = reinterpret_cast<ObObj*>(checker_obj_buf_);
    row.capacity_ = OB_ROW_MAX_COLUMNS_COUNT;
    for (int64_t it = micro_reader->begin(); OB_SUCC(ret) && it != micro_reader->end(); ++it) {
      row.row_val_.count_ = OB_ROW_MAX_COLUMNS_COUNT;
      if (OB_FAIL(micro_reader->get_row(it, row))) {
        STORAGE_LOG(WARN, "get_row failed", K(ret), K(it), K(*data_store_desc_));
      } else {  // print error row
        FLOG_WARN("error micro block row", K(it), K(row));
      }
    }
  }
  return ret;
}

int ObMacroBlockWriter::check_micro_block_checksum(
    const char* buf, const int64_t size, const ObIMicroBlockWriter* micro_writer)
{
  int ret = OB_SUCCESS;
  ObIMicroBlockReader* micro_reader = NULL;
  if (OB_FAIL(prepare_micro_block_reader(buf, size, micro_reader))) {
    STORAGE_LOG(WARN, "failed to preapre micro block reader", K(buf), K(size), K(micro_reader));
  } else if (OB_ISNULL(micro_reader)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "micro block reader is null", K(buf), K(size), K(micro_reader));
  } else {
    ObStoreRow row;
    int64_t new_checksum = 0;
    for (int64_t it = micro_reader->begin(); OB_SUCC(ret) && it != micro_reader->end(); ++it) {
      row.row_val_.cells_ = reinterpret_cast<ObObj*>(checker_obj_buf_);
      row.row_val_.count_ = OB_ROW_MAX_COLUMNS_COUNT;
      row.capacity_ = OB_ROW_MAX_COLUMNS_COUNT;
      if (OB_FAIL(micro_reader->get_row(it, row))) {
        STORAGE_LOG(WARN, "get_row failed", K(ret), K(it), K(*data_store_desc_), K(column_map_));
      } else {
        new_checksum = ObIMicroBlockWriter::cal_row_checksum(row, new_checksum);
      }
    }
    if (OB_SUCC(ret)) {
      if (micro_writer->get_micro_block_checksum() != new_checksum) {
        if (OB_FAIL(print_micro_block_row(micro_reader))) {
          STORAGE_LOG(WARN, "failed to print micro block buffer", K(ret));
        }
        ret = OB_CHECKSUM_ERROR;  // ignore print error code
        FLOG_ERROR("micro block checksum is not equal",
            K(new_checksum),
            K(micro_writer->get_micro_block_checksum()),
            K(ret),
            KPC(data_store_desc_));
      }
    }
  }
  return ret;
}

int ObMacroBlockWriter::dump_micro_block_writer_buffer()
{
  int ret = OB_SUCCESS;
  if (micro_writer_->get_row_count() > 0) {
    char* buf = NULL;
    int64_t size = 0;
    ObIMicroBlockReader* micro_reader = NULL;
    if (OB_FAIL(micro_writer_->build_block(buf, size))) {  // build micro block index
      STORAGE_LOG(WARN, "failed to build micro block", KPC(micro_writer_), K(micro_reader));
    } else if (OB_FAIL(prepare_micro_block_reader(buf, size, micro_reader))) {
      STORAGE_LOG(WARN, "failed to preapre micro block reader", KPC(micro_writer_), K(micro_reader));
    } else if (OB_FAIL(print_micro_block_row(micro_reader))) {
      STORAGE_LOG(WARN, "failed to print micro block buffer", K(ret), KPC(micro_writer_));
    }
  }
  return ret;
}

int ObMacroBlockWriter::check_micro_block(const char* compressed_buf, const int64_t compressed_size,
    const char* uncompressed_buf, const int64_t uncompressed_size, ObIMicroBlockWriter* micro_writer)
{
  int ret = OB_SUCCESS;
  const char* decomp_buf = nullptr;
  int64_t real_decomp_size = 0;
  if (MICRO_BLOCK_MERGE_VERIFY_LEVEL::ENCODING == micro_writer->get_micro_block_merge_verify_level()) {
    decomp_buf = const_cast<char*>(uncompressed_buf);
  } else if (OB_FAIL(compressor_.decompress(
                 compressed_buf, compressed_size, uncompressed_size, decomp_buf, real_decomp_size))) {
    STORAGE_LOG(WARN, "failed to decompress data", K(ret));
  } else if (uncompressed_size != real_decomp_size) {
    ret = OB_CHECKSUM_ERROR;
    STORAGE_LOG(
        ERROR, "decompressed size is not equal to original size", K(ret), K(uncompressed_size), K(real_decomp_size));
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(check_micro_block_checksum(decomp_buf, uncompressed_size, micro_writer))) {
      STORAGE_LOG(WARN, "failed to check_micro_block_checksum", K(ret));
    }
  }
  return ret;
}

int ObMacroBlockWriter::open_bf_cache_writer(const ObDataStoreDesc& desc)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!desc.need_prebuild_bloomfilter_ || 0 == desc.bloomfilter_size_ ||
                  0 >= desc.bloomfilter_rowkey_prefix_ ||
                  desc.bloomfilter_rowkey_prefix_ > desc.schema_rowkey_col_cnt_)) {
    // do nothing
  } else if (OB_FAIL(bf_cache_writer_[0].init(desc.bloomfilter_rowkey_prefix_, desc.bloomfilter_size_))) {
    STORAGE_LOG(WARN, "Fail to init 0th bf_merge_writer, ", K(ret));
  } else if (OB_FAIL(bf_cache_writer_[1].init(desc.bloomfilter_rowkey_prefix_, desc.bloomfilter_size_))) {
    STORAGE_LOG(WARN, "Fail to init 1th bf_merge_writer, ", K(ret));
  }
  if (OB_FAIL(ret)) {
    bf_cache_writer_[0].reset();
    bf_cache_writer_[1].reset();
  }
  return ret;
}

int ObMacroBlockWriter::flush_bf_to_cache(ObMacroBloomFilterCacheWriter& bf_writer, const int32_t row_count)
{
  int ret = OB_SUCCESS;
  if (OB_LIKELY(data_store_desc_->need_prebuild_bloomfilter_)) {
    if (!bf_writer.is_valid()) {
      if (OB_LIKELY(0 == data_store_desc_->bloomfilter_size_)) {
        data_store_desc_->bloomfilter_size_ = row_count;
        if (OB_FAIL(open_bf_cache_writer(*data_store_desc_))) {
          STORAGE_LOG(WARN, "Failed to open bloomfilter cache writer, ", K(ret));
        } else {
          // bloomfilter size use the first macro block row count, and become effective in next macroblock
          bf_cache_writer_[0].set_not_need_build();
          bf_cache_writer_[1].set_not_need_build();
        }
      } else {
        STORAGE_LOG(
            WARN, "Unexpected bloomfilter cache writer, ", K(bf_writer), K(data_store_desc_->bloomfilter_size_));
      }
    } else if (bf_writer.is_need_build() && bf_writer.get_row_count() == row_count) {
      ObStorageFile* file = data_store_desc_->file_handle_.get_storage_file();
      if (OB_ISNULL(file)) {
        ret = OB_ERR_SYS;
        STORAGE_LOG(WARN, "pg_file_ is null", K(ret), KP(file));
      } else if (OB_FAIL(bf_writer.flush_to_cache(
                     data_store_desc_->table_id_, macro_handle_.get_macro_id(), file->get_file_id()))) {
        STORAGE_LOG(WARN, "bloomfilter cache writer failed flush to cache, ", K(bf_writer));
      } else if (OB_NOT_NULL(data_store_desc_->merge_info_)) {
        data_store_desc_->merge_info_->macro_bloomfilter_count_++;
      }
    }
    bf_writer.reuse();
  }
  return ret;
}

int ObMacroBlockWriter::close_index_tree()
{
  int ret = OB_SUCCESS;
  const ObStoreRow* p_row = NULL;
  if (OB_UNLIKELY(index_block_builders_.count() == 0)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "the index_block_descs_ is null", K(ret));
  } else {
    IndexMicroBlockBuilder* builder = index_block_builders_.at(0);
    for (int32_t height = 1; OB_SUCC(ret) && height < index_block_builders_.count(); height++) {
      if (builder->writer_.get_row_count() > 0 && OB_FAIL(build_index_micro_block(height, p_row))) {
        STORAGE_LOG(WARN, "Fail to build index micro block, ", K(ret));
      } else if (OB_FAIL(update_index_tree(height, p_row))) {
        STORAGE_LOG(WARN, "Fail to update index tree", K(ret));
      } else {
        builder = index_block_builders_.at(height);
      }
    }
  }
  return ret;
}

int ObMacroBlockWriter::create_index_block_builder()
{
  int ret = OB_SUCCESS;
  void* buf = NULL;
  IndexMicroBlockBuilder* builder = NULL;
  if (OB_ISNULL(buf = allocator_.alloc(sizeof(IndexMicroBlockBuilder)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "fail to alloc memory", K(ret));
  } else if (OB_ISNULL(builder = new (buf) IndexMicroBlockBuilder())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "fail to new a IndexMicroBlockBuilder", K(ret));
  } else if (OB_FAIL(builder->writer_.init(index_store_desc_->micro_block_size_limit_,
                 index_store_desc_->rowkey_column_count_,
                 index_store_desc_->row_column_count_))) {
    STORAGE_LOG(WARN, "Fail to init index micro block writer, ", K(ret));
  } else if (OB_FAIL(index_block_builders_.push_back(builder))) {
    STORAGE_LOG(WARN, "Fail to push back micro block writer to index_entry_writer_list_", K(ret));
  }
  if (OB_FAIL(ret)) {
    if (OB_NOT_NULL(buf)) {
      allocator_.free(buf);
    }
  }
  return ret;
}

int ObMacroBlockWriter::build_intermediate_row(const ObString& rowkey, const ObBlockIntermediateHeader& header,
    ObBlockIntermediateBuilder& builder, const storage::ObStoreRow*& row)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!header.is_valid() || rowkey.empty())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument.", K(ret), K(header), K(rowkey));
  } else {
    builder.reuse();
    if (OB_FAIL(builder.init(index_store_desc_->rowkey_column_count_))) {
      STORAGE_LOG(WARN, "fail to init ObBlockIntermediateBuilder", K(ret));
    } else if (OB_FAIL(builder.set_rowkey(*index_store_desc_, rowkey))) {
      STORAGE_LOG(WARN, "fail to set rowkey", K(ret));
    } else if (OB_FAIL(builder.build_intermediate_row(header, row))) {
      STORAGE_LOG(WARN, "fail to build intermediate row", K(ret));
    }
  }
  return ret;
}

int ObMacroBlockWriter::build_intermediate_row(const ObStoreRowkey& rowkey, const ObBlockIntermediateHeader& header,
    ObBlockIntermediateBuilder& builder, const storage::ObStoreRow*& row)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!header.is_valid() || !rowkey.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument.", K(ret), K(header), K(rowkey));
  } else {
    builder.reuse();
    if (OB_FAIL(builder.init(index_store_desc_->rowkey_column_count_))) {
      STORAGE_LOG(WARN, "fail to init ObBlockIntermediateBuilder", K(ret));
    } else if (OB_FAIL(builder.set_rowkey(rowkey))) {
      STORAGE_LOG(WARN, "fail to set rowkey", K(ret));
    } else if (OB_FAIL(builder.build_intermediate_row(header, row))) {
      STORAGE_LOG(WARN, "fail to build intermediate row", K(ret));
    }
  }
  return ret;
}

int ObMacroBlockWriter::update_index_tree(const int32_t height, const ObStoreRow* intermediate_row)
{
  int ret = OB_SUCCESS;
  IndexMicroBlockBuilder* builder = NULL;
  const ObStoreRow* current_level_row = NULL;
  const ObStoreRow* next_level_row = NULL;
  if (OB_UNLIKELY(OB_ISNULL(intermediate_row) || !intermediate_row->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument.", K(ret), K(*intermediate_row));
  } else {
    current_level_row = intermediate_row;
    for (int32_t ht = height; OB_SUCC(ret) && OB_NOT_NULL(current_level_row); ht++) {
      if (OB_ISNULL(builder = index_block_builders_.at(ht))) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "Fail to get index block builders, ", K(ret), K(ht));
      } else {
        if (OB_FAIL(builder->writer_.append_row(*current_level_row))) {
          if (OB_BUF_NOT_ENOUGH == ret) {
            if (0 == builder->writer_.get_row_count()) {
              ret = OB_NOT_SUPPORTED;
              STORAGE_LOG(WARN, "The single row is too large, ", K(ret), K(*current_level_row));
            } else if (OB_FAIL(build_index_micro_block(ht + 1, next_level_row))) {
              STORAGE_LOG(WARN, "Fail to build index micro block, ", K(ret));
            } else if (OB_FAIL(builder->writer_.append_row(*current_level_row))) {
              STORAGE_LOG(WARN, "Fail to append row to micro block, ", K(ret), K(*current_level_row));
            }
          } else {
            STORAGE_LOG(WARN, "Fail to append row to micro block, ", K(ret), K(*current_level_row));
          }
        } else {
          if (builder->writer_.get_block_size() >= index_store_desc_->micro_block_size_) {
            if (OB_FAIL(build_index_micro_block(ht + 1, next_level_row))) {
              STORAGE_LOG(WARN, "Fail to build micro block, ", K(ret));
            }
          }
        }
        current_level_row = next_level_row;
        next_level_row = NULL;
      }
    }
  }
  return ret;
}

int ObMacroBlockWriter::build_index_micro_block(const int32_t height, const ObStoreRow*& mid_row)
{
  int ret = OB_SUCCESS;
  char* block_buffer = NULL;
  int64_t block_size = 0;
  ObMicroBlockDesc micro_block_desc;
  IndexMicroBlockBuilder* current_level_builder = NULL;
  IndexMicroBlockBuilder* next_level_builder = NULL;
  void* buf = NULL;
  if (OB_UNLIKELY(height <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument.", K(ret), K(height));
  } else if (index_block_builders_.count() == height && OB_FAIL(create_index_block_builder())) {
    STORAGE_LOG(WARN, "Fail to push one index micro block builder.", K(ret));
  } else if (OB_ISNULL(next_level_builder = index_block_builders_.at(height))) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "Fail to get index block builders, ", K(ret), K(height));
  } else if (OB_ISNULL(current_level_builder = index_block_builders_.at(height - 1)) || OB_ISNULL(next_level_builder) ||
             current_level_builder->writer_.get_row_count() <= 0) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN,
        "invalid inner arguments",
        K(ret),
        KP(current_level_builder),
        KP(next_level_builder),
        K(current_level_builder->writer_.get_row_count()));
  } else if (OB_FAIL(current_level_builder->writer_.build_block(block_buffer, block_size))) {
    STORAGE_LOG(WARN, "Fail to build block, ", K(ret));
  } else if (OB_FAIL(
                 compressor_.compress(block_buffer, block_size, micro_block_desc.buf_, micro_block_desc.buf_size_))) {
    STORAGE_LOG(WARN, "macro block writer fail to compress.", K(ret), K(OB_P(block_buffer)), K(block_size));
  } else if (MICRO_BLOCK_MERGE_VERIFY_LEVEL::NONE !=
                 current_level_builder->writer_.get_micro_block_merge_verify_level() &&
             OB_FAIL(check_micro_block(micro_block_desc.buf_,
                 micro_block_desc.buf_size_,
                 block_buffer,
                 block_size,
                 &current_level_builder->writer_))) {
    STORAGE_LOG(WARN, "failed to check micro block", K(ret));
  } else {
    int64_t data_offset = 0;
    micro_block_desc.last_rowkey_ = current_level_builder->writer_.get_last_rowkey();
    micro_block_desc.data_size_ = current_level_builder->writer_.get_data_size();
    micro_block_desc.row_count_ = current_level_builder->writer_.get_row_count();
    micro_block_desc.column_count_ = current_level_builder->writer_.get_column_count();
    micro_block_desc.row_count_delta_ = current_level_builder->writer_.get_row_count_delta();
    micro_block_desc.can_mark_deletion_ = false;
    micro_block_desc.column_checksums_ = NULL;
    if (OB_FAIL(task_index_writer_->write_micro_block(micro_block_desc, data_offset))) {
      STORAGE_LOG(WARN, "build_micro_block failed", K(micro_block_desc), K(ret));
    } else {
      ObBlockIntermediateHeader header(index_store_desc_->data_version_,
          task_index_writer_->get_current_macro_seq(),
          data_offset,
          micro_block_desc.buf_size_,
          current_level_builder->writer_.get_row_count(),
          false);
      if (OB_FAIL(build_intermediate_row(
              current_level_builder->writer_.get_last_rowkey(), header, next_level_builder->row_builder_, mid_row))) {
        STORAGE_LOG(WARN, "fail to build intermediate row", K(ret));
      } else {
        current_level_builder->writer_.reuse();
      }
    }
  }
  return ret;
}

}  // end namespace blocksstable
}  // end namespace oceanbase
