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

#define USING_LOG_PREFIX STORAGE_BLKMGR

#include "storage/blocksstable/ob_shared_macro_block_manager.h"

#include "lib/oblog/ob_log_module.h"
#include "lib/utility/ob_macro_utils.h"
#include "share/ob_force_print_log.h"
#include "storage/blocksstable/ob_block_manager.h"
#include "share/ob_force_print_log.h"
#include "storage/blocksstable/ob_imicro_block_writer.h"
#include "storage/blocksstable/ob_macro_block_struct.h"
#include "storage/meta_mem/ob_tenant_meta_mem_mgr.h"
#include "storage/meta_mem/ob_tablet_handle.h"
#include "storage/blocksstable/ob_index_block_builder.h"
#include "storage/blocksstable/ob_sstable_sec_meta_iterator.h"
#include "storage/tablet/ob_tablet_create_delete_helper.h"
#include "storage/ls/ob_ls.h"
#include "share/ob_ls_id.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/slog_ckpt/ob_server_checkpoint_slog_handler.h"

namespace oceanbase
{
namespace blocksstable
{
using namespace common;
using namespace common::hash;
using namespace share;

/**
 * ---------------------------------------ObBlockInfo----------------------------------------
 */
ObBlockInfo::~ObBlockInfo()
{
  reset();
}

void ObBlockInfo::reset()
{
  nested_size_ = OB_DEFAULT_MACRO_BLOCK_SIZE;
  nested_offset_ = 0;
  macro_id_.reset();
}

bool ObBlockInfo::is_valid() const
{
  return macro_id_.is_valid()
      && nested_offset_ >= 0
      && nested_size_ >= 0;
}

bool ObBlockInfo::is_small_sstable() const
{
  return OB_DEFAULT_MACRO_BLOCK_SIZE != nested_size_;
}

/**
 * ---------------------------------------ObSharedMacroBlockMgr----------------------------------------
 */
ObSharedMacroBlockMgr::ObSharedMacroBlockMgr()
  : offset_(OB_DEFAULT_MACRO_BLOCK_SIZE),
    common_header_buf_(nullptr),
    header_size_(0),
    mutex_(),
    blocks_mutex_(),
    block_used_size_(),
    defragmentation_task_(*this),
    tg_id_(-1),
    is_inited_(false)
{
}

ObSharedMacroBlockMgr::~ObSharedMacroBlockMgr()
{
  destroy();
}

void ObSharedMacroBlockMgr::destroy()
{
  TG_DESTROY(tg_id_);
  tg_id_ = -1;
  macro_handle_.reset();
  offset_ = OB_DEFAULT_MACRO_BLOCK_SIZE; // so we can init block automatically for first write
  header_size_ = 0;
  if (nullptr != common_header_buf_) {
    ob_free(common_header_buf_);
  }
  common_header_buf_ = nullptr;
  block_used_size_.destroy();
  is_inited_ = false;
}

int ObSharedMacroBlockMgr::mtl_init(ObSharedMacroBlockMgr* &shared_block_mgr)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == shared_block_mgr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("shared_block_mgr is null", K(ret));
  } else if (OB_FAIL(shared_block_mgr->init())) {
    LOG_WARN("fail to init shared_block_mgr", K(ret));
  }
  return ret;
}

int ObSharedMacroBlockMgr::init()
{
  int ret = OB_SUCCESS;
  ObMacroBlockCommonHeader common_header;
  common_header.reset();
  header_size_ = upper_align(common_header.get_serialize_size(), DIO_READ_ALIGN_SIZE);
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("shared macro block handle has been inited", K(ret));
  } else if (FALSE_IT(common_header.set_attr(ObMacroBlockCommonHeader::MacroBlockType::SharedSSTableData))) {
  } else if (OB_ISNULL(common_header_buf_ = reinterpret_cast<char*>(ob_malloc(header_size_,
      ObMemAttr(MTL_ID(), ObModIds::OB_MACRO_FILE))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory for buffer that holds common header", K(ret), K(common_header));
  } else if (FALSE_IT(MEMSET(common_header_buf_, 9, header_size_))) {
  } else if (OB_FAIL(common_header.build_serialized_header(common_header_buf_, common_header.get_serialize_size()))) {
    LOG_WARN("fail to serialize common header", K(ret), K(common_header));
  } else if (OB_FAIL(block_used_size_.init("ShareBlksMap", MTL_ID()))) {
    LOG_WARN("fail to init block used size array", K(ret));
  } else if (OB_FAIL(TG_CREATE_TENANT(lib::TGDefIDs::SSTableDefragment, tg_id_))) {
    LOG_WARN("fail to create thread for sstable defragmentation", K(ret));
  } else {
    is_inited_ = true;
  }

  if (IS_NOT_INIT) {
    destroy();
  }
  return ret;
}

int ObSharedMacroBlockMgr::start()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSharedMacroBlockMgr hasn't been inited", K(ret));
  } else if (OB_FAIL(TG_START(tg_id_))) {
    LOG_WARN("fail to start sstable defragmentation thread", K(ret), K(tg_id_));
  } else if (OB_FAIL(TG_SCHEDULE(tg_id_, defragmentation_task_, DEFRAGMENT_DELAY_US, true/*repeat*/))) {
    LOG_WARN("fail to schedule defragmentation task", K(ret), K(tg_id_));
  }
  return ret;
}

void ObSharedMacroBlockMgr::stop()
{
  if (OB_LIKELY(is_inited_)) {
    TG_STOP(tg_id_);
  }
}

void ObSharedMacroBlockMgr::wait()
{
  if (OB_LIKELY(is_inited_)) {
    TG_WAIT(tg_id_);
  }
}

int ObSharedMacroBlockMgr::write_block(
    const char *buf,
    const int64_t size,
    ObBlockInfo &block_info,
    ObMacroBlocksWriteCtx &write_ctx)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("Shared Macro Block Handle hasn't been inited.", K(ret));
  } else if (OB_ISNULL(buf) || OB_UNLIKELY(size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(buf), K(size));
  } else if (OB_UNLIKELY(0 != size % DIO_READ_ALIGN_SIZE)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("write size is not aligned", K(ret), K(size));
  } else {
    ObMacroBlockWriteInfo write_info;
    write_info.buffer_ = buf;
    write_info.io_desc_.set_wait_event(ObWaitEventIds::DB_FILE_COMPACT_WRITE);
    write_info.size_ = size;
    lib::ObMutexGuard guard(mutex_);

    if (size >= SMALL_SSTABLE_STHRESHOLD_SIZE) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("small sstable's size shouldn't be larger than 1 MB", K(ret), K(write_info.size_));
    } else if (offset_ + size > OB_DEFAULT_MACRO_BLOCK_SIZE) {
      if (OB_FAIL(try_switch_macro_block())) {
        LOG_WARN("fail to switch macro handle", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      write_info.offset_ = offset_;
      if (OB_FAIL(do_write_block(write_info, block_info))) {
        LOG_WARN("fail to do write block", K(ret), K(write_info), K(block_info));
      } else {
        FLOG_INFO("successfully write small sstable",
          K(ret), K(block_info), K(offset_), "old_block", write_ctx.get_macro_block_list());
        write_ctx.reset();
        if (OB_FAIL(write_ctx.add_macro_block_id(macro_handle_.get_macro_id()))) {
          LOG_WARN("fail to add macro block id into write_ctx",
            K(ret), K(macro_handle_.get_macro_id()), K(write_ctx));
        }
      }
    }
  }

  return ret;
}

int ObSharedMacroBlockMgr::do_write_block(
    const ObMacroBlockWriteInfo &write_info,
    ObBlockInfo &block_info)
{
  int ret = OB_SUCCESS;
  ObMacroBlockHandle write_macro_handle;
  const int64_t io_timeout_ms = std::max(GCONF._data_storage_io_timeout / 1000, DEFAULT_IO_WAIT_TIME_MS);

  if (OB_FAIL(write_macro_handle.set_macro_block_id(macro_handle_.get_macro_id()))) {
    LOG_WARN("fail to set macro block id", K(ret), K(macro_handle_.get_macro_id()));
  } else if (OB_FAIL(write_macro_handle.async_write(write_info))) {
    LOG_WARN("fail to async write virtual macro block", K(ret), K(write_macro_handle));
  } else if (OB_FAIL(write_macro_handle.wait(io_timeout_ms))) {
    LOG_WARN("fail to wait previous io", K(ret), K(io_timeout_ms));
  } else if (!write_macro_handle.is_empty() && MICRO_BLOCK_MERGE_VERIFY_LEVEL::ENCODING_AND_COMPRESSION_AND_WRITE_COMPLETE ==
      GCONF.micro_block_merge_verify_level && 0 != offset_) {
    if (OB_FAIL(check_write_complete(write_macro_handle.get_macro_id(), write_info.size_))) {
      LOG_WARN("fail to check write completion", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    block_info.macro_id_ = write_macro_handle.get_macro_id();
    block_info.nested_size_ = write_info.size_;
    block_info.nested_offset_ = offset_;
    offset_ += write_info.size_;
  }
  return ret;
}

int ObSharedMacroBlockMgr::check_write_complete(const MacroBlockId &macro_id, const int64_t macro_size)
{
  int ret = OB_SUCCESS;
  ObMacroBlockReadInfo read_info;
  read_info.macro_block_id_ = macro_id;
  read_info.size_ = macro_size;
  read_info.offset_ = offset_;
  read_info.io_desc_.set_wait_event(ObWaitEventIds::DB_FILE_COMPACT_READ);
  const int64_t io_timeout_ms = std::max(GCONF._data_storage_io_timeout / 1000, DEFAULT_IO_WAIT_TIME_MS);
  ObMacroBlockHandle read_handle;
  ObSSTableMacroBlockChecker macro_block_checker;

  if (OB_FAIL(ObBlockManager::async_read_block(read_info, read_handle))) {
    LOG_WARN("fail to async read macro block", K(ret), K(read_info));
  } else if (OB_FAIL(read_handle.wait(io_timeout_ms))) {
    LOG_WARN("fail to wait io finish", K(ret), K(io_timeout_ms));
  } else if (OB_FAIL(macro_block_checker.check(
      read_handle.get_buffer(),
      read_handle.get_data_size(),
      CHECK_LEVEL_PHYSICAL))) {
    LOG_WARN("fail to verify macro block", K(ret), K(macro_id));
  }
  return ret;
}

int ObSharedMacroBlockMgr::try_switch_macro_block()
{
  int ret = OB_SUCCESS;
  const MacroBlockId &block_id = macro_handle_.get_macro_id();
  const int32_t used_size = offset_;
  if (block_id.is_valid() && OB_FAIL(add_block(block_id, used_size))) {
    LOG_WARN("fail to add cur block to map", K(ret), K(block_id));
  } else if (FALSE_IT(macro_handle_.reset())) {
  } else if (FALSE_IT(offset_ = OB_DEFAULT_MACRO_BLOCK_SIZE /* invalid offset */)) {
  } else if (OB_FAIL(OB_SERVER_BLOCK_MGR.alloc_block(macro_handle_))) {
    LOG_WARN("fail to alloc block for new macro block", K(ret));
  } else {
    offset_ = 0;
    ObMacroBlockWriteInfo write_info;
    ObBlockInfo block_info;
    write_info.buffer_ = common_header_buf_;
    write_info.size_ = header_size_;
    write_info.offset_ = 0;
    write_info.io_desc_.set_wait_event(ObWaitEventIds::DB_FILE_COMPACT_WRITE);
    if (OB_FAIL(do_write_block(write_info, block_info))) {
      LOG_WARN("fail to write common header to the shared macro block", K(ret), K(block_info));
    }
  }

  return ret;
}

int64_t ObSharedMacroBlockMgr::get_shared_block_cnt()
{
  int64_t count = 0;
  {
    lib::ObMutexGuard guard(blocks_mutex_);
    count = block_used_size_.count();
  }
  return count;
}

int ObSharedMacroBlockMgr::add_block(const MacroBlockId &block_id, const int64_t block_size)
{
  int ret = OB_SUCCESS;
  int32_t curr_size = 0;
  if (OB_UNLIKELY(!block_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid block id", K(ret), K(block_id), K(block_size));
  } else {
    // block_size may execeeds default 2M
    // since we need get_and_set used_size of blocks, we need mutex to protect array
    lib::ObMutexGuard guard(blocks_mutex_);
    if (OB_FAIL(block_used_size_.get(block_id, curr_size)) && OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("fail to get block id from map", K(ret), K(block_id));
    } else if ((curr_size += block_size) == 0) {
      if (OB_FAIL(block_used_size_.erase(block_id))) {
        LOG_WARN("fail to erase id from map", K(ret), K(block_id));
      }
    } else if (OB_FAIL(block_used_size_.insert_or_update(block_id, curr_size))) {
      LOG_WARN("fail to add block to map", K(ret), K(block_id), K(curr_size));
    }
  }
  return ret;
}

int ObSharedMacroBlockMgr::free_block(const MacroBlockId &block_id, const int64_t block_size)
{
  int ret = OB_SUCCESS;
  int32_t curr_size = 0;
  if (OB_UNLIKELY(!block_id.is_valid() || block_size <= 0
      || block_size >= SMALL_SSTABLE_STHRESHOLD_SIZE)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid block size or id", K(ret), K(block_id), K(block_size));
  } else {
    // since we need get_and_set used_size of blocks, we need mutex to protect array
    lib::ObMutexGuard guard(blocks_mutex_);
    if (OB_FAIL(block_used_size_.get(block_id, curr_size)) && OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("fail to get block id from map", K(ret), K(block_id));
    } else if ((curr_size -= block_size) == 0) {
      if (OB_FAIL(block_used_size_.erase(block_id))) {
        LOG_WARN("fail to erase id from map", K(ret), K(block_id));
      }
    } else if (OB_FAIL(block_used_size_.insert_or_update(block_id, curr_size))) {
      LOG_WARN("fail to set block used size", K(ret), K(block_id), K(block_size), K(curr_size));
    }
  }
  return ret;
}

int ObSharedMacroBlockMgr::get_recyclable_blocks(ObIAllocator &allocator, ObIArray<MacroBlockId> &block_ids)
{
  int ret = OB_SUCCESS;
  {
    // since we need for_loop, we need mutex to protect array
    lib::ObMutexGuard guard(blocks_mutex_);
    ObFixedArray<MacroBlockId, ObIAllocator> recycled_block_ids(allocator);
    GetSmallBlockOp getOp(block_ids, recycled_block_ids);

    if (OB_FAIL(recycled_block_ids.init(MAX_RECYCLABLE_BLOCK_CNT))) {
      LOG_WARN("fail to init recycled_block_ids", K(ret));
    } else if (OB_FAIL(block_used_size_.for_each(getOp))) {
      if (OB_ITER_END == getOp.get_execution_ret() && MAX_RECYCLABLE_BLOCK_CNT == block_ids.count()) {
        ret = OB_SUCCESS;
        FLOG_INFO("number of recyclable blocks reaches 1000", K(ret));
      } else {
        LOG_WARN("fail to get recyclable blocks", K(ret), K(block_ids), K(recycled_block_ids));
      }
    }

    if (OB_FAIL(ret)) {
      // do nothing
    } else {
      int tmp_ret = OB_SUCCESS;
      for (int64_t i = 0; i < recycled_block_ids.count(); ++i) { // ignore tmp_ret
        const MacroBlockId &block_id = recycled_block_ids.at(i);
        if (OB_TMP_FAIL(block_used_size_.erase(block_id))) {
          LOG_WARN("fail to erase id from map", K(tmp_ret), K(block_id));
        }
      }
    }
  }
  return ret;
}

int ObSharedMacroBlockMgr::defragment()
{
  int ret = OB_SUCCESS;
  ObArenaAllocator task_allocator("SSTDefragTask", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
  ObArenaAllocator iter_allocator("SSTDefragIter", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
  ObFixedArray<MacroBlockId, ObIAllocator> macro_ids(task_allocator);
  ObTenantTabletIterator tablet_iter(*(MTL(ObTenantMetaMemMgr*)), iter_allocator);
  ObSSTableIndexBuilder *sstable_index_builder = nullptr;
  ObIndexBlockRebuilder *index_block_rebuilder = nullptr;
  int64_t rewrite_cnt = 0;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSharedMacroBlockMgr hasn't been initiated", K(ret));
  } else if (OB_FAIL(macro_ids.init(MAX_RECYCLABLE_BLOCK_CNT))) {
    LOG_WARN("fail to init macro ids", K(ret));
  } else if (OB_FAIL(get_recyclable_blocks(task_allocator, macro_ids))) {
    LOG_WARN("fail to get recycle blocks", K(ret));
  } else if (macro_ids.empty()) {
    // skip following steps
  } else if (OB_FAIL(alloc_for_tools(task_allocator, sstable_index_builder, index_block_rebuilder))) {
    LOG_WARN("fail to allocate memory for index builders", K(ret));
  } else {
    ObTabletHandle tablet_handle;
    while (OB_SUCC(ret)) {
      tablet_handle.reset();
      iter_allocator.reuse();
      if (OB_FAIL(tablet_iter.get_next_tablet(tablet_handle))) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          LOG_WARN("fail to get tablet", K(ret), K(tablet_handle));
        }
      } else if (OB_UNLIKELY(!tablet_handle.is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid tablet handle", K(ret), K(tablet_handle));
      } else if (tablet_handle.get_obj()->is_ls_inner_tablet()) {
        // skip update
      } else if (OB_FAIL(update_tablet(
          tablet_handle,
          macro_ids,
          rewrite_cnt,
          *sstable_index_builder,
          *index_block_rebuilder))) {
        if (OB_UNLIKELY(OB_EAGAIN != ret)) {
          LOG_WARN("fail to update tablet", K(ret), K(tablet_handle), K(macro_ids));
        } else {
          ret = OB_SUCCESS;
        }
      }
    }
  }

  if (nullptr != sstable_index_builder) {
    sstable_index_builder->~ObSSTableIndexBuilder();
    task_allocator.free(sstable_index_builder);
    sstable_index_builder = nullptr;
  }
  if (nullptr != index_block_rebuilder) {
    index_block_rebuilder->~ObIndexBlockRebuilder();
    task_allocator.free(index_block_rebuilder);
    index_block_rebuilder = nullptr;
  }

  if (OB_ITER_END == ret || OB_SUCC(ret)) {
    ret = OB_SUCCESS;
    LOG_INFO("successfully defragment data blocks", K(rewrite_cnt));
  } else if (OB_ALLOCATE_MEMORY_FAILED != ret && OB_SERVER_OUTOF_DISK_SPACE != ret
      && REACH_COUNT_INTERVAL(FAILURE_COUNT_INTERVAL)) {
    LOG_ERROR("defragmentation can't be finished, something is wrong", K(ret), K(macro_ids));
  }

  return ret;
}

int ObSharedMacroBlockMgr::update_tablet(
    const ObTabletHandle &tablet_handle,
    const ObIArray<MacroBlockId> &macro_ids,
    int64_t &rewrite_cnt,
    ObSSTableIndexBuilder &sstable_index_builder,
    ObIndexBlockRebuilder &index_block_rebuilder)
{
  int ret = OB_SUCCESS;
  ObSArray<ObTableHandleV2> table_handles;
  ObTableHandleV2 sstable_handle;
  ObSArray<ObITable *> sstables;
  uint64_t data_version = 0;

  if (OB_FAIL(tablet_handle.get_obj()->get_all_sstables(sstables))) {
    LOG_WARN("fail to get sstables of this tablet", K(ret));
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(MTL_ID(), data_version))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_EAGAIN;
    } else {
      LOG_WARN("fail to get data version", K(ret));
    }
  }
  for (int64_t i = 0; i < sstables.count() && OB_SUCC(ret); i++) {
    const ObSSTable *sstable = static_cast<ObSSTable *>(sstables.at(i));
    if (OB_ISNULL(sstable) || !sstable->is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("the sstable is null or invalid", K(ret));
    } else if (sstable->is_small_sstable()) {
      const ObIArray<MacroBlockId> &data_block_ids = sstable->get_meta().get_macro_info().get_data_block_ids();
      if (OB_UNLIKELY(1 != data_block_ids.count())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("this sstable is not small", K(ret), K(data_block_ids.count()), K(sstable->is_small_sstable()));
      } else if (is_contain(macro_ids, data_block_ids.at(0))) {
        if (OB_FAIL(rebuild_sstable(
            *(tablet_handle.get_obj()),
            *sstable,
            data_version,
            sstable_index_builder,
            index_block_rebuilder,
            sstable_handle))) {
          LOG_WARN("fail to rebuild sstable and update tablet", K(ret));
        } else if (OB_FAIL(table_handles.push_back(sstable_handle))) {
          LOG_WARN("fail to push table handle to array", K(ret), K(sstable_handle));
        }
      }
    }
  }

  if (OB_SUCC(ret) && !table_handles.empty()) {
    const ObTabletMeta &tablet_meta = tablet_handle.get_obj()->get_tablet_meta();
    const share::ObLSID &ls_id = tablet_meta.ls_id_;
    ObLSService *ls_svr = MTL(ObLSService*);
    ObLSHandle ls_handle;

    if (OB_FAIL(ls_svr->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD))) {
      LOG_WARN("fail to get ls handle", K(ret), K(ls_id));
    } else {
      const int64_t rebuild_seq = ls_handle.get_ls()->get_rebuild_seq();
      if (OB_UNLIKELY(!ls_handle.is_valid())) {
        LOG_WARN("la handle is invalid", K(ret), K(ls_handle));
      } else if (OB_FAIL(ls_handle.get_ls()->update_tablet_table_store(
          rebuild_seq, tablet_handle, table_handles))) {
        LOG_WARN("fail to replace small sstables in the tablet", K(ret), K(rebuild_seq), K(tablet_handle), K(table_handles));
      } else {
        rewrite_cnt += table_handles.count();
      }
    }
  }

  return ret;
}

int ObSharedMacroBlockMgr::rebuild_sstable(
    const ObTablet &tablet,
    const ObSSTable &old_sstable,
    const uint64_t data_version,
    ObSSTableIndexBuilder &sstable_index_builder,
    ObIndexBlockRebuilder &index_block_rebuilder,
    ObTableHandleV2 &table_handle)
{
  int ret = OB_SUCCESS;
  ObDataStoreDesc data_desc;
  ObMergeType merge_type;
  sstable_index_builder.reset();
  index_block_rebuilder.reset();
  table_handle.reset();
  ObDataMacroBlockMeta data_macro_meta;
  ObMacroBlockHandle block_handle;
  ObBlockInfo block_info;
  ObMacroBlocksWriteCtx write_ctx;
  ObSSTableMergeRes res;
  const int64_t column_count = old_sstable.get_meta().get_basic_meta().column_cnt_;

  if (OB_FAIL(parse_merge_type(old_sstable, merge_type))) {
    LOG_WARN("fail to parse merge type from old_sstable", K(ret));
  } else if (OB_FAIL(prepare_data_desc(
      tablet,
      old_sstable.get_meta().get_basic_meta(),
      merge_type,
      tablet.get_snapshot_version(),
      data_version,
      data_desc))) {
    LOG_WARN("fail to prepare data desc", K(ret), K(merge_type), K(tablet.get_snapshot_version()));
  } else if (OB_FAIL(sstable_index_builder.init(data_desc, nullptr, ObSSTableIndexBuilder::DISABLE))) {
    LOG_WARN("fail to init sstable index builder", K(ret), K(data_desc));
  } else if (OB_FAIL(index_block_rebuilder.init(sstable_index_builder))) {
    LOG_WARN("fail to init index block rebuilder", K(ret));
  } else if (OB_FAIL(read_sstable_block(old_sstable, block_handle))) {
    LOG_WARN("fail to read old_sstable's block", K(ret), K(old_sstable));
  } else if (OB_FAIL(write_block(
      block_handle.get_buffer(), block_handle.get_data_size(), block_info, write_ctx))) {
    LOG_WARN("fail to write old_sstable's buf to new block", K(ret));
  } else if (OB_FAIL(index_block_rebuilder.append_macro_row(
      block_handle.get_buffer(), block_handle.get_data_size(), block_info.macro_id_))) {
    LOG_WARN("fail to append macro row", K(ret), K(block_info));
  } else if (OB_FAIL(index_block_rebuilder.close())) {
    LOG_WARN("fail to close index block rebuilder", K(ret));
  } else if (OB_FAIL(sstable_index_builder.close(column_count, res))) {
    LOG_WARN("fail to close sstable index builder", K(ret), K(column_count));
  } else if (OB_FAIL(create_new_sstable(res, tablet, old_sstable, block_info, table_handle))) {
    LOG_WARN("fail to create new sstable", K(ret), K(tablet.get_tablet_meta()), K(old_sstable));
  } else {
    ObSSTable *new_sstable = nullptr;
    if (OB_FAIL(table_handle.get_sstable(new_sstable))) {
      LOG_WARN("fail to get new sstable", K(table_handle));
    } else if (OB_FAIL(new_sstable->set_upper_trans_version(old_sstable.get_meta().get_basic_meta().upper_trans_version_))) {
      LOG_WARN("fail to update upper trans version", K(ret), K(old_sstable.get_meta().get_basic_meta().upper_trans_version_));
    } else if (OB_UNLIKELY(new_sstable->get_key() != old_sstable.get_key())
        || OB_FAIL(ObSSTableMetaChecker::check_sstable_meta_strict_equality(old_sstable.get_meta(), new_sstable->get_meta()))) {
      ret = OB_INVALID_DATA;
      LOG_WARN("new sstable is not equal to old sstable", K(ret), KPC(new_sstable), K(old_sstable));
    } else {
      FLOG_INFO("successfully rebuild one sstable", K(ret), K(block_info), K(new_sstable->get_key()), K(new_sstable->get_meta()));
    }
  }

  return ret;
}

int ObSharedMacroBlockMgr::create_new_sstable(
    const ObSSTableMergeRes &res,
    const ObTablet &tablet,
    const ObSSTable &old_table,
    const ObBlockInfo &block_info,
    ObTableHandleV2 &table_handle) const
{
  int ret = OB_SUCCESS;
  const ObStorageSchema &storage_schema = tablet.get_storage_schema();
  const ObSSTableBasicMeta &basic_meta = old_table.get_meta().get_basic_meta();
  table_handle.reset();
  ObTabletCreateSSTableParam param;

  param.filled_tx_scn_ = basic_meta.filled_tx_scn_;
  param.ddl_scn_ = basic_meta.ddl_scn_;
  param.table_key_ = old_table.get_key();
  param.sstable_logic_seq_ = old_table.get_sstable_seq();
  param.table_mode_ = basic_meta.table_mode_;
  param.index_type_ = static_cast<share::schema::ObIndexType>(basic_meta.index_type_);
  param.schema_version_ = basic_meta.schema_version_;
  param.create_snapshot_version_ = basic_meta.create_snapshot_version_;
  param.progressive_merge_round_ = basic_meta.progressive_merge_round_;
  param.progressive_merge_step_ = basic_meta.progressive_merge_step_;
  param.rowkey_column_cnt_ = basic_meta.rowkey_column_count_;
  param.recycle_version_ = basic_meta.recycle_version_;
  param.latest_row_store_type_ = basic_meta.latest_row_store_type_;
  param.is_ready_for_read_ = true;

  ObSSTableMergeRes::fill_addr_and_data(res.root_desc_,
      param.root_block_addr_, param.root_block_data_);
  ObSSTableMergeRes::fill_addr_and_data(res.data_root_desc_,
      param.data_block_macro_meta_addr_, param.data_block_macro_meta_);
  param.root_row_store_type_ = res.root_row_store_type_;
  param.data_index_tree_height_ = res.root_desc_.height_;
  param.index_blocks_cnt_ = res.index_blocks_cnt_;
  param.data_blocks_cnt_ = res.data_blocks_cnt_;
  param.micro_block_cnt_ = res.micro_block_cnt_;
  param.use_old_macro_block_count_ = res.use_old_macro_block_count_;
  param.row_count_ = res.row_count_;
  param.column_cnt_ = res.data_column_cnt_;
  param.data_checksum_ = res.data_checksum_;
  param.occupy_size_ = res.occupy_size_;
  param.original_size_ = res.original_size_;
  param.max_merged_trans_version_ = res.max_merged_trans_version_;
  param.contain_uncommitted_row_ = res.contain_uncommitted_row_;
  param.compressor_type_ = res.compressor_type_;
  param.encrypt_id_ = res.encrypt_id_;
  param.master_key_id_ = res.master_key_id_;
  param.data_block_ids_ = res.data_block_ids_;
  param.is_meta_root_ = res.data_root_desc_.is_meta_root_;
  param.nested_offset_ = block_info.nested_offset_;
  param.nested_size_ = block_info.nested_size_;
  param.other_block_ids_ = res.other_block_ids_;
  MEMCPY(param.encrypt_key_, res.encrypt_key_, share::OB_MAX_TABLESPACE_ENCRYPT_KEY_LENGTH);

  if (param.table_key_.is_major_sstable()) {
    if (OB_FAIL(res.fill_column_checksum(&storage_schema, param.column_checksums_))) {
      LOG_WARN("fail to fill column checksum", K(ret), K(res));
    }
  }
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_FAIL(ObTabletCreateDeleteHelper::create_sstable(param, table_handle))) {
    LOG_WARN("fail to create sstable", K(ret), K(param));
  }

  return ret;
}

int ObSharedMacroBlockMgr::prepare_data_desc(
    const ObTablet &tablet,
    const ObSSTableBasicMeta &basic_meta,
    const ObMergeType &merge_type,
    const int64_t snapshot_version,
    const int64_t cluster_version,
    ObDataStoreDesc &data_desc) const
{
  int ret = OB_SUCCESS;
  data_desc.reset();
  if (OB_FAIL(data_desc.init(
      tablet.get_storage_schema(),
      tablet.get_tablet_meta().ls_id_,
      tablet.get_tablet_meta().tablet_id_,
      merge_type,
      snapshot_version,
      cluster_version))) {
    LOG_WARN("fail to init data store desc", K(ret),
      K(tablet), K(merge_type), K(snapshot_version), K(cluster_version));
  } else {
    // overwrite the encryption related memberships, otherwise these memberships of new sstable may differ
    // from that of old sstable, since the encryption method of one tablet may change before defragmentation
    data_desc.row_store_type_ = basic_meta.root_row_store_type_;
    data_desc.compressor_type_ = basic_meta.compressor_type_;
    data_desc.master_key_id_ = basic_meta.master_key_id_;
    data_desc.encrypt_id_ = basic_meta.encrypt_id_;
    data_desc.encoder_opt_.set_store_type(basic_meta.root_row_store_type_);
    MEMCPY(data_desc.encrypt_key_, basic_meta.encrypt_key_, share::OB_MAX_TABLESPACE_ENCRYPT_KEY_LENGTH);
    data_desc.row_column_count_ = data_desc.rowkey_column_count_ + 1;
    data_desc.col_desc_array_.reset();
    data_desc.need_prebuild_bloomfilter_ = false;
    if (OB_FAIL(data_desc.col_desc_array_.init(data_desc.row_column_count_))) {
      LOG_WARN("fail to reserve column desc array", K(ret));
    } else if (OB_FAIL(tablet.get_storage_schema().get_rowkey_column_ids(data_desc.col_desc_array_))) {
      LOG_WARN("fail to get rowkey column ids", K(ret));
    } else if (OB_FAIL(storage::ObMultiVersionRowkeyHelpper::add_extra_rowkey_cols(data_desc.col_desc_array_))) {
      LOG_WARN("fail to add extra rowkey cols", K(ret));
    } else {
      ObObjMeta meta;
      meta.set_varchar();
      meta.set_collation_type(CS_TYPE_BINARY);
      share::schema::ObColDesc col;
      col.col_id_ = static_cast<uint64_t>(data_desc.row_column_count_ + OB_APP_MIN_COLUMN_ID);
      col.col_type_ = meta;
      col.col_order_ = DESC;
      if (OB_FAIL(data_desc.col_desc_array_.push_back(col))) {
        LOG_WARN("fail to push back last col for index", K(ret), K(col));
      }
    }
  }
  return ret;
}

int ObSharedMacroBlockMgr::parse_merge_type(const ObSSTable &sstable, ObMergeType &merge_type) const
{
  int ret = OB_SUCCESS;
  merge_type = ObMergeType::INVALID_MERGE_TYPE;

  if (sstable.is_major_sstable()) {
    merge_type = ObMergeType::MAJOR_MERGE;
  } else if (sstable.is_minor_sstable()) {
    merge_type = ObMergeType::MINOR_MERGE;
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sstable type is unexpected", K(ret), K(sstable));
  }
  return ret;
}

int ObSharedMacroBlockMgr::alloc_for_tools(
    ObIAllocator &allocator,
    ObSSTableIndexBuilder *&sstable_index_builder,
    ObIndexBlockRebuilder *&index_block_rebuilder)
{
  int ret = OB_SUCCESS;
  void *buf = nullptr;
  if (OB_ISNULL(buf = allocator.alloc(sizeof(ObSSTableIndexBuilder)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate memory for sstable index builder", K(ret));
  } else if (FALSE_IT(sstable_index_builder = new (buf) ObSSTableIndexBuilder)) {
  } else if (OB_ISNULL(buf = allocator.alloc(sizeof(ObIndexBlockRebuilder)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memory failed", K(ret));
  } else if (FALSE_IT(index_block_rebuilder = new (buf) ObIndexBlockRebuilder)) {
    LOG_WARN("fail to allocate memory for index rebuilder", K(ret));
  }
  return ret;
}

int ObSharedMacroBlockMgr::read_sstable_block(
    const ObSSTable &sstable,
    ObMacroBlockHandle &block_handle)
{
  int ret = OB_SUCCESS;
  ObMacroBlockReadInfo read_info;
  const ObSSTableMacroInfo &macro_info = sstable.get_meta().get_macro_info();
  read_info.macro_block_id_ = macro_info.get_data_block_ids().at(0);
  read_info.offset_ = macro_info.get_nested_offset();
  read_info.size_ = upper_align(macro_info.get_nested_size(), DIO_READ_ALIGN_SIZE);
  read_info.io_desc_.set_wait_event(ObWaitEventIds::DB_FILE_COMPACT_READ);

  if (OB_FAIL(ObBlockManager::read_block(read_info, block_handle))) {
    LOG_WARN("fail to read block", K(ret), K(read_info));
  } else if (OB_UNLIKELY(!block_handle.is_valid()
      || macro_info.get_nested_size() != block_handle.get_data_size())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("block handle is invalid", K(ret), K(block_handle));
  }
  return ret;
}

/**
 * ---------------------------------------ObBlockDefragmentationTask----------------------------------------
 */
void ObSharedMacroBlockMgr::ObBlockDefragmentationTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  if (ObServerCheckpointSlogHandler::get_instance().is_started() && OB_FAIL(shared_mgr_.defragment())) {
    LOG_WARN("fail to defragment small sstables", K(ret));
  }
}

} // namespace blocksstable
} // namespace oceanbase
