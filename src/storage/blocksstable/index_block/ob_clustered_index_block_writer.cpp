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

#include "ob_clustered_index_block_writer.h"
#include "storage/blocksstable/index_block/ob_index_block_builder.h"

namespace oceanbase
{
namespace blocksstable
{

ObClusteredIndexBlockWriter::ObClusteredIndexBlockWriter()
    : clustered_index_store_desc_(),
      data_store_desc_(nullptr),
      leaf_block_desc_(nullptr),
      row_builder_(),
      root_ctx_(nullptr),
      macro_writer_(nullptr),
      micro_writer_(nullptr),
      task_allocator_(nullptr),
      row_allocator_("ClusteredIdxRow"),
      macro_block_io_allocator_("clusteredIdxIO"),
      decompress_allocator_("clusteredDecom"),
      last_rowkey_(),
      is_inited_(false)
{
}

ObClusteredIndexBlockWriter::~ObClusteredIndexBlockWriter()
{
  reset();
}

void ObClusteredIndexBlockWriter::reset()
{
  if (OB_NOT_NULL(micro_writer_)) {
    micro_writer_->~ObIMicroBlockWriter();
    task_allocator_->free(micro_writer_);
    micro_writer_ = nullptr;
  }
  if (OB_NOT_NULL(macro_writer_)) {
    macro_writer_->~ObMacroBlockWriter();
    task_allocator_->free(macro_writer_);
    macro_writer_ = nullptr;
  }
  leaf_block_desc_ = nullptr;
  root_ctx_ = nullptr;
  task_allocator_ = nullptr;
  row_allocator_.reset();
  macro_block_io_allocator_.reset();
  decompress_allocator_.reset();
  last_rowkey_.reset();
  is_inited_ = false;
}

int ObClusteredIndexBlockWriter::init(const ObDataStoreDesc &data_store_desc,
                                      ObDataStoreDesc &leaf_block_desc,
                                      const blocksstable::ObMacroSeqParam &macro_seq_param,
                                      const share::ObPreWarmerParam &pre_warm_param,
                                      ObIndexTreeRootCtx *root_ctx,
                                      common::ObIAllocator &task_allocator,
                                      ObIMacroBlockFlushCallback *ddl_callback)
{
  int ret = OB_SUCCESS;
  // Shallow copy desc (let micro block size to 2MB and builder pointer to null).
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("fail to init clustered index writer, init twice", K(ret), K(is_inited_));
  } else if (OB_UNLIKELY(compaction::is_mds_merge(data_store_desc.get_merge_type()))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument, mds merge should not init clustered index writer",
             K(ret), K(data_store_desc), K(data_store_desc.get_merge_type()));
  } else if (OB_FAIL(clustered_index_store_desc_.shallow_copy(leaf_block_desc))) {
    LOG_WARN("fail to set clustered index store desc", K(ret));
  } else {
    clustered_index_store_desc_.sstable_index_builder_ = nullptr;
    clustered_index_store_desc_.data_store_type_ = ObMacroBlockCommonHeader::MacroBlockType::SSTableIndex;
  }
  // Build micro writer and set others.
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ObMacroBlockWriter::build_micro_writer(
                 &clustered_index_store_desc_, task_allocator, micro_writer_))) {
    LOG_WARN("fail to build clustered index micro writer", K(ret));
  } else {
    data_store_desc_ = &data_store_desc;
    leaf_block_desc_ = &leaf_block_desc;
    root_ctx_ = root_ctx;
    task_allocator_ = &task_allocator;
  }
  // Build macro writer.
  ObSSTablePrivateObjectCleaner *object_cleaner = nullptr;
  if (OB_SUCC(ret)) {
    abort_unless(macro_writer_ == nullptr);
    if (OB_ISNULL(macro_writer_ = OB_NEWx(ObMacroBlockWriter, task_allocator_, true /* use double buffer */))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate and construct macro writer in clustered index block writer", K(ret));
    } else if (OB_FAIL(ObSSTablePrivateObjectCleaner::get_cleaner_from_data_store_desc(leaf_block_desc,
                                                                                       object_cleaner))) {
      LOG_WARN("fail to get cleaner from data store desc", K(ret), K(leaf_block_desc), KP(object_cleaner));
    } else if (OB_FAIL(macro_writer_->open(clustered_index_store_desc_,
                                           0 /* parallel_idx */,
                                           macro_seq_param,
                                           pre_warm_param,
                                           *object_cleaner,
                                           ddl_callback))) {
      LOG_WARN("fail to open macro writer in clustered index block writer",
               K(ret), K(leaf_block_desc), KPC(object_cleaner));
    }
  }
  // Build clustered row builder.
  if (OB_SUCC(ret)) {
    if (OB_FAIL(row_builder_.init(*task_allocator_, *data_store_desc_, *leaf_block_desc_))) {
      LOG_WARN("fail to init row builder for clustered writer", K(ret));
    } else {
      is_inited_ = true;
    }
  }

  if (OB_FAIL(ret)) {
    if (OB_NOT_NULL(micro_writer_)) {
      micro_writer_->~ObIMicroBlockWriter();
      task_allocator.free(micro_writer_);
      micro_writer_ = nullptr;
    }
    if (OB_NOT_NULL(macro_writer_)) {
      macro_writer_->~ObMacroBlockWriter();
      task_allocator_->free(macro_writer_);
      macro_writer_ = nullptr;
    }
  } else {
    LOG_INFO("succeed to init clustered writer", K(ret), K(is_inited_),
             K(leaf_block_desc.get_row_store_type()),
             K(data_store_desc.get_row_store_type()),
             K(clustered_index_store_desc_.get_row_store_type()));
  }
  return ret;
}

int ObClusteredIndexBlockWriter::append_row(const ObIndexBlockRowDesc &old_row_desc)
{
  int ret = OB_SUCCESS;
  const ObDatumRow *row_to_append = nullptr;
  // Shallow copy from normal row_desc and set for clustered row_desc.
  ObIndexBlockRowDesc clustered_row_desc = old_row_desc;
  clustered_row_desc.set_for_clustered_index();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("fail to append row in clustered index writer, not inited", K(ret), K(is_inited_));
  } else if (OB_FAIL(check_order(clustered_row_desc))) {
    LOG_WARN("fail to check order", K(ret), K(clustered_row_desc));
  } else if (OB_FAIL(row_builder_.build_row(clustered_row_desc, row_to_append))) {
    LOG_WARN("fail to build index row", K(ret), K(clustered_row_desc));
  } else if (OB_FAIL(micro_writer_->append_row(*row_to_append))) {
    LOG_ERROR("fail to append index row to clustered index block writer",
              K(ret), K(clustered_row_desc), KPC(row_to_append));
  } else {
    last_rowkey_.reset();
    row_allocator_.reuse();
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(clustered_row_desc.row_key_.deep_copy(last_rowkey_, row_allocator_))) {
    LOG_WARN("fail to deep copy last rowkey", K(ret), K(clustered_row_desc));
  } else {
    LOG_DEBUG("clustered writer succeed to append row", K(ret), K(clustered_row_desc), K(old_row_desc));
  }
  return ret;
}

int ObClusteredIndexBlockWriter::build_and_append_clustered_index_micro_block()
{
  int ret = OB_SUCCESS;
  ObMicroBlockDesc clustered_index_micro_block_desc;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("fail to build and append clustered index micro block, not inited", K(ret), K(is_inited_));
  } else if (OB_FAIL(build_clustered_index_micro_block(clustered_index_micro_block_desc))) {
    LOG_WARN("fail to build clustered index micro block", K(ret), K(clustered_index_micro_block_desc));
  } else if (OB_FAIL(macro_writer_->append_index_micro_block(clustered_index_micro_block_desc))) {
    LOG_WARN("fail to append clustered index micro block", K(ret));
  } else {
    // Add clustered index micro block info to root ctx.
    const MacroBlockId &macro_id = clustered_index_micro_block_desc.macro_id_;
    const int64_t block_offset = clustered_index_micro_block_desc.block_offset_;
    const int64_t block_size = clustered_index_micro_block_desc.buf_size_ +
        clustered_index_micro_block_desc.header_->header_size_;
    const ObLogicMicroBlockId &logic_micro_id = clustered_index_micro_block_desc.logic_micro_id_;
    if (OB_FAIL(root_ctx_->add_clustered_index_block_micro_infos(
            macro_id, block_offset, block_size, logic_micro_id))) {
      LOG_WARN("fail to add clustered index block micro infos", K(ret),
               K(macro_id), K(block_offset), K(block_size), K(logic_micro_id));
    } else if (clustered_index_store_desc_.is_cg()) {
      // Reset last rowkey, cause row idx in cg[0] is relatively row idx.
      last_rowkey_.reset();
    }
    // Clean micro writer status.
    micro_writer_->reuse();
  }
  return ret;
}

int ObClusteredIndexBlockWriter::build_clustered_index_micro_block(
    ObMicroBlockDesc &clustered_index_micro_block_desc)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!last_rowkey_.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("last rowkey is in_valid", K(ret), K(last_rowkey_));
  } else if (OB_UNLIKELY(micro_writer_->get_row_count() == 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected empty clustered index micro block", K(ret), K(micro_writer_->get_row_count()));
  } else if (OB_FAIL(micro_writer_->build_micro_block_desc(clustered_index_micro_block_desc))) {
    LOG_WARN("fail to build clustered index micro block", K(ret));
  } else {
    clustered_index_micro_block_desc.last_rowkey_ = last_rowkey_;
    LOG_DEBUG("succeed to build clustered index micro block", K(ret), K(clustered_index_micro_block_desc));
  }
  return ret;
}

int ObClusteredIndexBlockWriter::reuse_clustered_micro_block(
    const MacroBlockId &macro_id,
    const ObMicroBlockData &micro_block_data)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("fail to reuse clustered micro block, not inited", K(ret), K(is_inited_));
  } else if (OB_UNLIKELY(micro_writer_->get_row_count() > 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected clustered micro writer row count, should be 0", K(ret),
             K(micro_writer_->get_row_count()));
  } else if (OB_FAIL(make_clustered_index_micro_block_with_reuse(
                 micro_block_data,
                 macro_id))) {
    LOG_WARN("fail to make clustered index micro block", K(ret));
  } else {
    LOG_DEBUG("succeed to reuse clustered micro block", K(ret), K(macro_id));
  }
  return ret;
}

int ObClusteredIndexBlockWriter::rewrite_and_append_clustered_index_micro_block(
    const char *macro_buf, const int64_t macro_size, const ObDataMacroBlockMeta &macro_meta)
{
  int ret = OB_SUCCESS;
  const char * micro_buffer = macro_buf + macro_meta.get_meta_val().block_offset_;
  int64_t micro_size = macro_meta.get_meta_val().block_size_;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("fail to rewrite and append clustered index micro block, not inited", K(ret), K(is_inited_));
  } else if (OB_FAIL(decompress_and_make_clustered_index_micro_block(
                 micro_buffer, micro_size, macro_meta.get_macro_id(), macro_meta))) {
    LOG_WARN("fail to make clustered index micro block", K(ret), K(macro_meta));
  } else {
    LOG_DEBUG("succeed to make clustered index micro block in rebuilder",
              K(ret), K(macro_size), K(macro_meta));
  }
  return ret;
}

int ObClusteredIndexBlockWriter::rewrite_and_append_clustered_index_micro_block(
    const ObDataMacroBlockMeta &macro_meta)
{
  int ret = OB_SUCCESS;
  char * micro_buf = nullptr;
  ObStorageObjectReadInfo read_info;
  ObStorageObjectHandle object_handle;
  int64_t micro_size = 0;
  // Dispatch IO to load micro block.
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("fail to rewrite and append clustered index micro block", K(ret), K(is_inited_));
  } else if (OB_UNLIKELY(!macro_meta.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid macro meta", K(ret), K(macro_meta));
  } else if (FALSE_IT(micro_size = macro_meta.get_meta_val().block_size_)) {
  } else if (OB_ISNULL(micro_buf = static_cast<char *>(
                           macro_block_io_allocator_.alloc(micro_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate macro block buffer", K(ret), K(macro_meta), K(micro_size));
  } else {
    read_info.macro_block_id_ = macro_meta.get_macro_id();
    read_info.offset_ = macro_meta.get_meta_val().block_offset_;
    read_info.size_ = micro_size;
    read_info.io_desc_.set_mode(ObIOMode::READ);
    read_info.io_desc_.set_wait_event(ObWaitEventIds::DB_FILE_DATA_READ);
    read_info.io_desc_.set_sys_module_id(ObIOModule::SSTABLE_INDEX_BUILDER_IO);
    read_info.io_timeout_ms_ = GCONF._data_storage_io_timeout / 1000L;
    read_info.mtl_tenant_id_ = MTL_ID();
    read_info.buf_ = micro_buf;
    if (OB_FAIL(ObObjectManager::read_object(read_info, object_handle))) {
      LOG_WARN("fail to read index micro block", K(ret), K(read_info));
    }
  }
  // Make clustered index micro block.
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(decompress_and_make_clustered_index_micro_block(
                 micro_buf, micro_size, macro_meta.get_macro_id(), macro_meta))) {
    LOG_WARN("fail to make clustered index micro block", K(ret), KP(micro_buf),
             K(micro_size), K(macro_meta));
  } else {
    LOG_DEBUG("succeed to make clustered index micro block in rebuilder",
              K(ret), K(macro_meta));
  }
  object_handle.reset();
  // Recycle buffer.
  if (OB_NOT_NULL(micro_buf)) {
    macro_block_io_allocator_.free(micro_buf);
    micro_buf = nullptr;
    macro_block_io_allocator_.reuse();
  }
  return ret;
}

int ObClusteredIndexBlockWriter::close()
{
  int ret = OB_SUCCESS;
  int64_t index_row_count = 0;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("fail to close clustered index writer, not inited", K(ret), K(is_inited_));
  } else if (OB_UNLIKELY((index_row_count = micro_writer_->get_row_count()) > 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("clustered index block writer should flush all clustered index row",
        K(ret), K(index_row_count));
  }

  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(macro_writer_)) {
    // do nothing.
  } else {
    // Transfer macro block refcnt and close clustered index macro writer.
    ObMacroBlocksWriteCtx &write_ctx = macro_writer_->get_macro_block_write_ctx();
    if (OB_FAIL(macro_writer_->close())) {
      LOG_WARN("fail to close clustered index macro writer", K(ret));
    } else if (OB_FAIL(write_ctx.deep_copy(root_ctx_->clustered_index_write_ctx_, *(root_ctx_->allocator_)))) {
      LOG_WARN("fail to deep copy clustered index macro writer's write ctx", K(ret), K(write_ctx));
    }

    // Print clustered macro block id.
    if (OB_FAIL(ret)) {
    } else {
      common::ObIArray<blocksstable::MacroBlockId> &clustered_index_macro_ids =
          root_ctx_->clustered_index_write_ctx_->get_macro_block_list();
      for (int64_t i = 0; i < clustered_index_macro_ids.count(); ++i) {
        auto & macro_block_id = clustered_index_macro_ids.at(i);
        LOG_INFO("clustered index macro block id", K(macro_block_id));
      }
      LOG_INFO("close clustered index block writer",
               K(clustered_index_macro_ids.count()),
               KPC(root_ctx_->clustered_micro_info_array_));
    }
  }
  return ret;
}

int ObClusteredIndexBlockWriter::check_order(const ObIndexBlockRowDesc &row_desc)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!row_desc.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("fail to check order, row_desc invalid", K(ret), K(row_desc));
  } else if (!last_rowkey_.is_valid()) {
    // do nothing.
  } else if (clustered_index_store_desc_.is_cg()) {
    if (row_desc.row_key_.get_datum(0).get_int() <= last_rowkey_.get_datum(0).get_int()) {
      ret = OB_ROWKEY_ORDER_ERROR;
      LOG_ERROR("input rowkey is less then last rowkey.", K(ret), K(row_desc.row_key_), K(last_rowkey_));
    }
  } else {
    const ObDatumRowkey &cur_rowkey = row_desc.row_key_;
    int32_t compare_result = 0;
    const ObStorageDatumUtils &datum_utils = clustered_index_store_desc_.get_datum_utils();
    if (OB_FAIL(cur_rowkey.compare(last_rowkey_, datum_utils, compare_result))) {
      LOG_WARN("Failed to compare last key", K(ret), K(cur_rowkey), K(last_rowkey_));
    } else if (OB_UNLIKELY(compare_result < 0)) {
      ret = OB_ROWKEY_ORDER_ERROR;
      LOG_ERROR("input rowkey is less then last rowkey.", K(ret), K(cur_rowkey), K(last_rowkey_));
    }
  }
  return ret;
}

int ObClusteredIndexBlockWriter::decompress_and_make_clustered_index_micro_block(
    const char *micro_buffer,
    const int64_t micro_size,
    const MacroBlockId &macro_id,
    const ObDataMacroBlockMeta &macro_meta)
{
  int ret = OB_SUCCESS;
  ObMicroBlockData micro_block_data;
  micro_block_data.buf_ = micro_buffer;
  micro_block_data.size_ = micro_size;
  if (OB_FAIL(decompress_micro_block_data(macro_meta, micro_block_data))) {
    LOG_WARN("fail to decompress micro block data", K(ret), K(macro_meta));
  } else if (OB_UNLIKELY(micro_block_data.get_buf() == micro_buffer)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to decompress micro block data, unexpected pointer value",
             K(ret), KP(micro_buffer), KP(micro_block_data.get_buf()),
             K(micro_size), K(micro_block_data.get_buf_size()));
  } else if (OB_FAIL(make_clustered_index_micro_block_with_rewrite(micro_block_data, macro_id))) {
    LOG_WARN("fail to make clustered index micro block", K(ret), K(macro_id));
  } else {
    micro_block_data.buf_ = nullptr;
    micro_block_data.size_ = 0;
    decompress_allocator_.reuse();
  }
  return ret;
}

int ObClusteredIndexBlockWriter::make_clustered_index_micro_block_with_rewrite(
    const ObMicroBlockData &micro_block_data,
    const MacroBlockId &macro_id)
{
  int ret = OB_SUCCESS;
  compaction::ObLocalArena temp_allocator("MakeClusterMic");
  ObMicroBlockReaderHelper micro_reader_helper;
  ObIMicroBlockReader *micro_block_reader = nullptr;
  const ObMicroBlockHeader *micro_block_header =
      reinterpret_cast<const ObMicroBlockHeader *>(micro_block_data.get_buf());
  const int64_t rowkey_column_count = micro_block_header->rowkey_column_count_;
  if (OB_FAIL(micro_reader_helper.init(temp_allocator))) {
    LOG_WARN("fail to init micro reader helper", K(ret));
  } else if (OB_FAIL(micro_reader_helper.get_reader(
                 micro_block_data.get_store_type(), micro_block_reader))) {
    LOG_WARN("fail to get micro reader", K(ret), K(micro_block_data.get_store_type()));
  } else if (OB_FAIL(micro_block_reader->init(micro_block_data, nullptr))) {
    LOG_WARN("fail to init micro block reader", K(ret));
  }
  // Defensive check, no row should be in micro writer.
  if (OB_FAIL(ret)) {
  } else if (OB_UNLIKELY(micro_writer_->get_row_count() > 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected clustered micro writer row count, should be 0", K(ret),
             K(micro_writer_->get_row_count()));
  }
  // Iterate index row and transfer it to clustered index row.
  int64_t row_count = 0;
  ObDatumRow row;
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(micro_block_reader->get_row_count(row_count))) {
    LOG_WARN("fail to get row count from micro block reader", K(ret));
  } else if (OB_FAIL(row.init(rowkey_column_count + 1))) {
    LOG_WARN("fail to init datum row", K(ret));
  } else {
    ObIndexBlockRowParser idx_row_parser;
    for (int64_t row_idx = 0; OB_SUCC(ret) && row_idx < row_count; ++row_idx) {
      idx_row_parser.reset();
      row.reuse();
      if (OB_FAIL(micro_block_reader->get_row(row_idx, row))) {
        LOG_WARN("fail to get next row", K(ret), K(row.is_valid()));
      } else if (OB_FAIL(idx_row_parser.init(rowkey_column_count, row))) {
        LOG_WARN("fail to init idx row parser", K(ret), K(rowkey_column_count));
      } else {
        // Actually, we only need `row_store_type` and `static_desc` from data store desc.
        ObStaticDataStoreDesc rewrite_static_desc;
        ObDataStoreDesc rewrite_data_store_desc;
        if (OB_FAIL(rewrite_static_desc.assign(*(data_store_desc_->static_desc_)))) {
          LOG_WARN("fail to assign static desc for rewrite desc", K(ret), KPC(data_store_desc_->static_desc_));
        } else if (OB_FAIL(rewrite_data_store_desc.shallow_copy(*data_store_desc_))) {
          LOG_WARN("fail to shallow copy for rewrite desc", K(ret), KPC(data_store_desc_));
        } else {
          rewrite_data_store_desc.static_desc_ = &rewrite_static_desc;
        }
        ObIndexBlockRowDesc clustered_row_desc(rewrite_data_store_desc);

        // The following code needs to consider compatibility.
        int64_t agg_row_size = 0;
        const ObIndexBlockRowHeader *idx_row_header = nullptr;
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(idx_row_parser.get_header(idx_row_header))) {
          LOG_WARN("fail to get idx row header", K(ret));
        } else if (!idx_row_header->is_major_node() || !idx_row_header->is_pre_aggregated()) {
          clustered_row_desc.serialized_agg_row_buf_ = nullptr;
        } else if (OB_FAIL(idx_row_parser.get_agg_row(
                       clustered_row_desc.serialized_agg_row_buf_,
                       agg_row_size))) {
          LOG_WARN("fail to get aggregate row", K(ret));
        }
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(clustered_row_desc.row_key_.assign(row.storage_datums_, rowkey_column_count))) {
          LOG_WARN("fail to assign rowkey to row_desc", K(ret), K(row), K(rowkey_column_count));
        } else {
          // The following items need to remain consistent with meta_to_row_desc.
          rewrite_data_store_desc.row_store_type_ = idx_row_header->get_row_store_type();
          rewrite_data_store_desc.static_desc_->compressor_type_ = idx_row_header->get_compressor_type();
          rewrite_data_store_desc.static_desc_->master_key_id_ = idx_row_header->get_master_key_id();
          rewrite_data_store_desc.static_desc_->encrypt_id_ = idx_row_header->get_encrypt_id();
          MEMCPY(rewrite_data_store_desc.static_desc_->encrypt_key_,
                 idx_row_header->get_encrypt_key(),
                 sizeof(rewrite_data_store_desc.static_desc_->encrypt_key_));
          rewrite_data_store_desc.static_desc_->schema_version_ = idx_row_header->get_schema_version();
          if (idx_row_header->is_data_index() && !idx_row_header->is_major_node()) {
            // Snapshot version should use data's value.
            rewrite_data_store_desc.static_desc_->end_scn_.convert_for_tx(idx_row_parser.get_snapshot_version());
          }

          clustered_row_desc.is_serialized_agg_row_ = true;
          clustered_row_desc.row_offset_ = idx_row_parser.get_row_offset();
          clustered_row_desc.is_secondary_meta_ = false;
          clustered_row_desc.is_data_block_ = idx_row_header->is_data_block();
          clustered_row_desc.is_macro_node_ = idx_row_header->is_macro_node();
          clustered_row_desc.has_string_out_row_ = idx_row_header->has_string_out_row();
          clustered_row_desc.has_lob_out_row_ = idx_row_header->has_lob_out_row();
          clustered_row_desc.is_deleted_ = idx_row_header->is_deleted();
          clustered_row_desc.block_offset_ = idx_row_header->get_block_offset();
          clustered_row_desc.block_size_ = idx_row_header->get_block_size();
          clustered_row_desc.logic_micro_id_ = idx_row_header->get_logic_micro_id();
          clustered_row_desc.data_checksum_ = idx_row_header->get_data_checksum();
          clustered_row_desc.shared_data_macro_id_ = idx_row_header->get_shared_data_macro_id();
          clustered_row_desc.macro_block_count_ = idx_row_header->macro_block_count_;
          clustered_row_desc.micro_block_count_ = idx_row_header->micro_block_count_;
          clustered_row_desc.row_count_ = idx_row_header->get_row_count();
          clustered_row_desc.contain_uncommitted_row_ = idx_row_header->contain_uncommitted_row();
          clustered_row_desc.max_merged_trans_version_ = idx_row_parser.get_max_merged_trans_version();
          clustered_row_desc.row_count_delta_ = idx_row_parser.get_row_count_delta();

          // Rebuilder, use input macro_id.
          clustered_row_desc.macro_id_ = macro_id;
        }

        // Append clustered row to clustered writer.
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(append_row(clustered_row_desc))) {
          LOG_WARN("fail to append clustered row", K(ret), K(clustered_row_desc));
        }
      }
    }  // end of for

    // Build clustered index micro block and append.
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(build_and_append_clustered_index_micro_block())) {
      LOG_WARN("fail to build and append clustered index micro block", K(ret));
    } else {
      LOG_DEBUG("succeed to make clustered index micro block with rewrite", K(ret), K(macro_id));
    }
  }
  return ret;
}

int ObClusteredIndexBlockWriter::make_clustered_index_micro_block_with_reuse(
    const ObMicroBlockData &micro_block_data,
    const MacroBlockId &macro_id)
{
  int ret = OB_SUCCESS;
  compaction::ObLocalArena temp_allocator("MakeClusterMic");
  const ObMicroBlockHeader *micro_block_header =
      reinterpret_cast<const ObMicroBlockHeader *>(micro_block_data.get_buf());
  int64_t rowkey_column_count = micro_block_header->rowkey_column_count_;
  const ObStorageDatumUtils *datum_utils;
  ObIndexBlockRowScanner index_block_row_scanner;
  // Init and open index block row scanner for reused clustered micro block
  // (maybe transformed in micro block cache).
  common::ObQueryFlag mock_query_flag;
  mock_query_flag.multi_version_minor_merge_ = compaction::is_mini_merge(clustered_index_store_desc_.get_merge_type());
  if (clustered_index_store_desc_.is_cg()) {  // Fetch datum utils for index row scanner
    const ObITableReadInfo *index_read_info;
    if (OB_FAIL(MTL(ObTenantCGReadInfoMgr *)->get_index_read_info(index_read_info))) {
      LOG_WARN("fail to get index read info for cg sstable", K(ret), K(clustered_index_store_desc_));
    } else if (OB_UNLIKELY(!index_read_info->get_datum_utils().is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected invalid datum utails for cg sstable", K(ret), KPC(index_read_info));
    } else {
      datum_utils = &index_read_info->get_datum_utils();
    }
  } else {
    datum_utils = &(clustered_index_store_desc_.get_datum_utils());
  }

  if (FAILEDx(index_block_row_scanner.init(*datum_utils,
                                           temp_allocator,
                                           mock_query_flag,
                                           0 /* nested offset */,
                                           clustered_index_store_desc_.is_cg()))) {
    LOG_WARN("fail to init index block row scanner", K(ret),
             K(clustered_index_store_desc_.get_datum_utils()),
             K(clustered_index_store_desc_.is_cg()));
  } else if (OB_FAIL(index_block_row_scanner.open(macro_id, micro_block_data))) {
    LOG_WARN("fail to open reused micro block data", K(ret), K(macro_id));
  }
  // Defensive check, no row should be in micro writer.
  if (OB_FAIL(ret)) {
  } else if (OB_UNLIKELY(micro_writer_->get_row_count() > 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected clustered micro writer row count, should be 0", K(ret),
             K(micro_writer_->get_row_count()));
  }
  // Iterator index info and transfer it to clustered index row. We cannot reuse clustered index micro block through
  // `append_micro_block` or `append_index_micro_block`, because this clustered micro block is transformed in
  // `ObIndexBlockTreeCursor` (dispatch IO + reuse clustered micro block is worse).
  ObMicroIndexInfo index_info;
  ObArenaAllocator row_key_allocator(common::ObMemAttr(MTL_ID(), "MakeClustRK"));
  while (OB_SUCC(ret)) {
    row_key_allocator.reuse();
    // Actually, we only need `row_store_type` and `static_desc` from data store desc.
    ObStaticDataStoreDesc reuse_static_desc;
    ObDataStoreDesc reuse_data_store_desc;
    if (OB_FAIL(reuse_static_desc.assign(*(data_store_desc_->static_desc_)))) {
      LOG_WARN("fail to assign static desc for rewrite desc", K(ret), KPC(data_store_desc_->static_desc_));
    } else if (OB_FAIL(reuse_data_store_desc.shallow_copy(*data_store_desc_))) {
      LOG_WARN("fail to shallow copy for rewrite desc", K(ret), KPC(data_store_desc_));
    } else {
      reuse_data_store_desc.static_desc_ = &reuse_static_desc;
    }
    ObIndexBlockRowDesc clustered_row_desc(reuse_data_store_desc);

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(index_block_row_scanner.get_next(index_info,
                                                        false /* is_multi_check */,
                                                        false /* is_sorted_multi_get */))) {
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
        break;
      } else {
        LOG_WARN("fail to get next from index block row scanner", K(ret));
      }
    } else if (OB_FAIL(index_info.endkey_.deep_copy(clustered_row_desc.row_key_, row_key_allocator))) {
      LOG_WARN("fail to assign last rowkey", K(ret), K(index_info), K(rowkey_column_count));
    } else {
      const ObIndexBlockRowHeader * idx_row_header = index_info.row_header_;

      if (!idx_row_header->is_major_node() || !idx_row_header->is_pre_aggregated()) {
        clustered_row_desc.serialized_agg_row_buf_ = nullptr;
      } else {
        clustered_row_desc.serialized_agg_row_buf_ = index_info.agg_row_buf_;
      }

      // The following items need to remain consistent with meta_to_row_desc.
      reuse_data_store_desc.row_store_type_ = idx_row_header->get_row_store_type();
      reuse_data_store_desc.static_desc_->compressor_type_ = idx_row_header->get_compressor_type();
      reuse_data_store_desc.static_desc_->master_key_id_ = idx_row_header->get_master_key_id();
      reuse_data_store_desc.static_desc_->encrypt_id_ = idx_row_header->get_encrypt_id();
      MEMCPY(reuse_data_store_desc.static_desc_->encrypt_key_,
             idx_row_header->get_encrypt_key(),
             sizeof(reuse_data_store_desc.static_desc_->encrypt_key_));
      reuse_data_store_desc.static_desc_->schema_version_ = idx_row_header->get_schema_version();
      if (idx_row_header->is_data_index() && !idx_row_header->is_major_node()) {
        // Snapshot version should use data's value.
        reuse_data_store_desc.static_desc_->end_scn_.convert_for_tx(index_info.minor_meta_info_->snapshot_version_);
      }

      // Reuse, no need to change macro_id.
      clustered_row_desc.macro_id_ = idx_row_header->get_macro_id();

      clustered_row_desc.is_serialized_agg_row_ = true;
      clustered_row_desc.row_offset_ = index_info.cs_row_range_.end_row_id_;
      clustered_row_desc.is_secondary_meta_ = false;
      clustered_row_desc.is_data_block_ = idx_row_header->is_data_block();
      clustered_row_desc.is_macro_node_ = idx_row_header->is_macro_node();
      clustered_row_desc.has_string_out_row_ = idx_row_header->has_string_out_row();
      clustered_row_desc.has_lob_out_row_ = idx_row_header->has_lob_out_row();
      clustered_row_desc.is_deleted_ = idx_row_header->is_deleted();
      clustered_row_desc.block_offset_ = idx_row_header->get_block_offset();
      clustered_row_desc.block_size_ = idx_row_header->get_block_size();
      clustered_row_desc.logic_micro_id_ = idx_row_header->get_logic_micro_id();
      clustered_row_desc.data_checksum_ = idx_row_header->get_data_checksum();
      clustered_row_desc.shared_data_macro_id_ = idx_row_header->get_shared_data_macro_id();
      clustered_row_desc.macro_block_count_ = idx_row_header->macro_block_count_;
      clustered_row_desc.micro_block_count_ = idx_row_header->micro_block_count_;
      clustered_row_desc.row_count_ = idx_row_header->get_row_count();
      clustered_row_desc.contain_uncommitted_row_ = idx_row_header->contain_uncommitted_row();
      clustered_row_desc.max_merged_trans_version_ =
          idx_row_header->is_major_node() ? 0 : index_info.minor_meta_info_->max_merged_trans_version_;
      clustered_row_desc.row_count_delta_ =
          idx_row_header->is_major_node() ? 0 : index_info.minor_meta_info_->row_count_delta_;

      // Append clustered row to clustered writer.
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(append_row(clustered_row_desc))) {
        LOG_WARN("fail to append clustered row", K(ret), K(clustered_row_desc), K(index_info));
      }
    }
    clustered_row_desc.row_key_.reset();
  }

  // Build clustered index micro block and append.
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(build_and_append_clustered_index_micro_block())) {
    LOG_WARN("fail to build and append clustered index micro block", K(ret));
  } else {
    LOG_DEBUG("succeed to make clustered index micro block with reuse", K(ret), K(macro_id));
  }

  return ret;
}

int ObClusteredIndexBlockWriter::decompress_micro_block_data(
    const ObDataMacroBlockMeta &macro_meta,
    ObMicroBlockData &micro_block_data)
{
  int ret = OB_SUCCESS;
  bool is_compressed = false;
  ObMacroBlockReader macro_reader;
  ObMicroBlockDesMeta block_des_meta;
  ObMicroBlockHeader header;

  block_des_meta.compressor_type_ = macro_meta.get_meta_val().compressor_type_;
  block_des_meta.encrypt_id_ = macro_meta.get_meta_val().encrypt_id_;
  block_des_meta.encrypt_key_ = macro_meta.get_meta_val().encrypt_key_;
  block_des_meta.master_key_id_ = macro_meta.get_meta_val().master_key_id_;

  const char * micro_block_buf = micro_block_data.get_buf();
  const int64_t micro_block_size = micro_block_data.get_buf_size();

  if (OB_FAIL(header.deserialize_and_check_header(micro_block_buf, micro_block_size))) {
    LOG_WARN("fail to deserialize and check header", K(ret));
  } else if (OB_FAIL(macro_reader.do_decrypt_and_decompress_data(
                 header, block_des_meta, micro_block_buf, micro_block_size,
                 micro_block_data.get_buf(), micro_block_data.get_buf_size(),
                 is_compressed, true /* need deep copy */,
                 &decompress_allocator_ /* allocator */))) {
    LOG_WARN("Fail to decrypt and decompress data", K(ret));
  }

  return ret;
}

int ObClusteredIndexBlockWriter::print_macro_ids()
{
  int ret = OB_SUCCESS;
  common::ObIArray<blocksstable::MacroBlockId> *macro_block_list =
      &(macro_writer_->get_macro_block_write_ctx().macro_block_list_);
  LOG_INFO("print clustered index macro block ids", KPC(macro_block_list));
  return ret;
}

} // namespace blocksstable
} // namespace oceanbase
