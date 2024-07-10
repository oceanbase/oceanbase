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
#include "storage/blocksstable/index_block/ob_index_block_builder.h"
#include "storage/blocksstable/index_block/ob_sstable_sec_meta_iterator.h"
#include "storage/tablet/ob_tablet_create_delete_helper.h"
#include "storage/tablet/ob_mds_schema_helper.h"
#include "storage/ls/ob_ls.h"
#include "share/ob_ls_id.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/slog_ckpt/ob_server_checkpoint_slog_handler.h"
#include "storage/blocksstable/ob_data_store_desc.h"

namespace oceanbase
{
namespace blocksstable
{
using namespace common;
using namespace common::hash;
using namespace share;
using namespace compaction;

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
    need_defragment_(false),
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
  need_defragment_ = false;
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
    write_info.io_timeout_ms_ = GCONF._data_storage_io_timeout / 1000;
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
      write_info.io_timeout_ms_ = std::max(GCONF._data_storage_io_timeout / 1000, DEFAULT_IO_WAIT_TIME_MS);
      if (OB_FAIL(do_write_block(macro_handle_.get_macro_id(), write_info, block_info))) {
        LOG_WARN("fail to do write block", K(ret), K(write_info), K(block_info));
      }

      // no matter success or failure, advance offset_
      offset_ += size;

      if (OB_SUCC(ret)) {
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

/*static*/int ObSharedMacroBlockMgr::do_write_block(
    const MacroBlockId& macro_id,
    const ObMacroBlockWriteInfo &write_info,
    ObBlockInfo &block_info)
{
  int ret = OB_SUCCESS;
  ObMacroBlockHandle write_macro_handle;
  const int64_t offset = write_info.offset_;
  const int64_t size = write_info.size_;

  if (OB_FAIL(write_macro_handle.set_macro_block_id(macro_id))) {
    LOG_WARN("fail to set macro block id", K(ret), K(macro_id));
  } else if (OB_FAIL(write_macro_handle.async_write(write_info))) {
    LOG_WARN("fail to async write virtual macro block", K(ret), K(write_macro_handle));
  } else if (OB_FAIL(write_macro_handle.wait())) {
    LOG_WARN("fail to wait previous io", K(ret), K(write_info));
  }

  if (OB_SUCC(ret)
      && !write_macro_handle.is_empty()
      && MICRO_BLOCK_MERGE_VERIFY_LEVEL::ENCODING_AND_COMPRESSION_AND_WRITE_COMPLETE == GCONF.micro_block_merge_verify_level
      && 0 != offset) {
    if (OB_FAIL(check_write_complete(write_macro_handle.get_macro_id(), offset, size))) {
      LOG_WARN("fail to check write completion", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    block_info.macro_id_ = macro_id;
    block_info.nested_size_ = size;
    block_info.nested_offset_ = offset;
  }
  return ret;
}

/*static*/int ObSharedMacroBlockMgr::check_write_complete(
  const MacroBlockId &macro_id,
  const int64_t offset,
  const int64_t size)
{
  int ret = OB_SUCCESS;
  ObMacroBlockReadInfo read_info;
  read_info.macro_block_id_ = macro_id;
  read_info.size_ = size;
  read_info.offset_ = offset;
  read_info.io_desc_.set_wait_event(ObWaitEventIds::DB_FILE_COMPACT_READ);
  read_info.io_timeout_ms_ = std::max(GCONF._data_storage_io_timeout / 1000, DEFAULT_IO_WAIT_TIME_MS);
  read_info.io_desc_.set_resource_group_id(THIS_WORKER.get_group_id());
  read_info.io_desc_.set_sys_module_id(ObIOModule::SHARED_MACRO_BLOCK_MGR_IO);
  ObMacroBlockHandle read_handle;
  ObSSTableMacroBlockChecker macro_block_checker;
  ObArenaAllocator io_allocator("SMBM_IOUB", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());

  if (OB_ISNULL(read_info.buf_ = reinterpret_cast<char*>(io_allocator.alloc(read_info.size_)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "failed to alloc macro read info buffer", K(ret), K(read_info.size_));
  } else {
    if (OB_FAIL(ObBlockManager::async_read_block(read_info, read_handle))) {
      LOG_WARN("fail to async read macro block", K(ret), K(read_info));
    } else if (OB_FAIL(read_handle.wait())) {
      LOG_WARN("fail to wait io finish", K(ret), K(read_info));
    } else if (OB_FAIL(macro_block_checker.check(
        read_info.buf_,
        read_handle.get_data_size(),
        CHECK_LEVEL_PHYSICAL))) {
      LOG_WARN("fail to verify macro block", K(ret), K(macro_id));
    }
  }
  return ret;
}

int ObSharedMacroBlockMgr::try_switch_macro_block()
{
  int ret = OB_SUCCESS;
  const MacroBlockId &block_id = macro_handle_.get_macro_id();
  const int32_t used_size = offset_;
  ObMacroBlockHandle new_macro_handle;
  // we add_block_size extraly to avoid defragmenting the previous block if some sstables haven't been inited
  if (block_id.is_valid() && OB_FAIL(add_block(block_id, used_size))) {
    LOG_WARN("fail to add cur block to map", K(ret), K(block_id));
  } else if (FALSE_IT(macro_handle_.reset())) {
  } else if (FALSE_IT(offset_ = OB_DEFAULT_MACRO_BLOCK_SIZE /* invalid offset */)) {
  } else if (OB_FAIL(OB_SERVER_BLOCK_MGR.alloc_block(new_macro_handle))) {
    LOG_WARN("fail to alloc block for new macro block", K(ret));
  } else {
    ObMacroBlockWriteInfo write_info;
    ObBlockInfo block_info;
    write_info.buffer_ = common_header_buf_;
    write_info.size_ = header_size_;
    write_info.offset_ = 0;
    write_info.io_desc_.set_wait_event(ObWaitEventIds::DB_FILE_COMPACT_WRITE);
    write_info.io_timeout_ms_ = std::max(GCONF._data_storage_io_timeout / 1000, DEFAULT_IO_WAIT_TIME_MS);
    if (OB_FAIL(do_write_block(new_macro_handle.get_macro_id(), write_info, block_info))) {
      LOG_WARN("fail to write common header to the shared macro block", K(ret), K(new_macro_handle), K(write_info), K(block_info));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(macro_handle_.set_macro_block_id(new_macro_handle.get_macro_id()))) {
      LOG_WARN("fail to set new macro id", K(ret), K(new_macro_handle));
    } else {
      offset_ = header_size_;
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

void ObSharedMacroBlockMgr::get_cur_shared_block(MacroBlockId &macro_id)
{
  lib::ObMutexGuard guard(mutex_);
  macro_id = macro_handle_.get_macro_id();
}

bool ObSharedMacroBlockMgr::is_recyclable(const MacroBlockId &macro_id, const int64_t &used_size) const
{
  // current macro block is excluded
  return macro_id != macro_handle_.get_macro_id() && used_size < RECYCLABLE_BLOCK_SIZE;
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
    } else if (FALSE_IT(curr_size += block_size)) {
    } else if (OB_FAIL(block_used_size_.insert_or_update(block_id, curr_size))) {
      LOG_WARN("fail to add block to map", K(ret), K(block_id), K(curr_size));
    } else if (is_recyclable(block_id, curr_size)) {
      ATOMIC_SET(&need_defragment_, true);
    }
  }
  return ret;
}

int ObSharedMacroBlockMgr::free_block(const MacroBlockId &block_id, const int64_t block_size)
{
  int ret = OB_SUCCESS;
  int32_t curr_size = 0;
  if (OB_UNLIKELY(!block_id.is_valid() || block_size <= 0)) {
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
    } else if (is_recyclable(block_id, curr_size)) {
      ATOMIC_SET(&need_defragment_, true);
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
    GetSmallBlockOp getOp(*this, block_ids, recycled_block_ids);

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
      ATOMIC_SET(&need_defragment_, false);
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
  ObHasNestedTableFilterOp op;
  ObTenantTabletIterator tablet_iter(*(MTL(ObTenantMetaMemMgr*)), iter_allocator, &op);
  ObSSTableIndexBuilder *sstable_index_builder = nullptr;
  ObIndexBlockRebuilder *index_block_rebuilder = nullptr;
  int64_t rewrite_cnt = 0;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSharedMacroBlockMgr hasn't been initiated", K(ret));
  } else if (!(ATOMIC_LOAD(&need_defragment_))) { // skip
    LOG_INFO("skip defragment task", K(ret), K_(need_defragment));
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
      } else if (!tablet_handle.get_obj()->is_row_store()) {
        // TODO @danling support small sstable for column store
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

  if (OB_ITER_END == ret || OB_SUCC(ret)) {
    ret = OB_SUCCESS;
    FLOG_INFO("successfully defragment data blocks", K(ret), K(rewrite_cnt), K(block_used_size_.count()));
  } else {
    ATOMIC_SET(&need_defragment_, true); // set need_defragment_ true to trigger next round
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
  } else if (OB_ALLOCATE_MEMORY_FAILED != ret
      && OB_SERVER_OUTOF_DISK_SPACE != ret
      && OB_TIMEOUT != ret
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
  common::ObArenaAllocator allocator("ShareBlkUpTab");
  ObSArray<ObITable *> new_sstables;
  ObTableStoreIterator table_store_iter;
  uint64_t data_version = 0;
  const ObTabletMeta &tablet_meta = tablet_handle.get_obj()->get_tablet_meta();
  const share::ObLSID &ls_id = tablet_meta.ls_id_;
  ObTabletHandle updated_tablet_handle;
  ObMetaDiskAddr cur_addr;
  const ObTabletMapKey key(ls_id, tablet_meta.tablet_id_);

  //ATTENTION!!! get_all_sstables should unpack cosstable, and make cosstable again finally
  if (OB_FAIL(tablet_handle.get_obj()->get_all_sstables(table_store_iter))) {
    LOG_WARN("fail to get sstables of this tablet", K(ret));
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(MTL_ID(), data_version))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_EAGAIN;
    } else {
      LOG_WARN("fail to get data version", K(ret));
    }
  }
  while (OB_SUCC(ret)) {
    ObITable *table = nullptr;
    ObSSTableMetaHandle meta_handle;
    const ObSSTable *sstable = nullptr;
    if (OB_FAIL(table_store_iter.get_next(table))) {
      if (OB_UNLIKELY(OB_ITER_END == ret)) {
        ret = OB_SUCCESS;
        break;
      } else {
        LOG_WARN("fail to get next table from iter", K(ret), K(table_store_iter));
      }
    } else if (FALSE_IT(sstable = static_cast<ObSSTable *>(table))) {
    } else if (OB_ISNULL(sstable) || !sstable->is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("the sstable is null or invalid", K(ret));
    } else if (OB_FAIL(sstable->get_meta(meta_handle))) {
      LOG_WARN("get meta handle fail", K(ret), KPC(sstable));
    } else if (sstable->is_small_sstable()) {
      const int64_t data_block_count = meta_handle.get_sstable_meta().get_data_macro_block_count();
      ObMacroIdIterator id_iterator;
      MacroBlockId macro_id;
      if (OB_UNLIKELY(1 != data_block_count)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("this sstable is not small", K(ret), K(data_block_count));
      } else if (OB_FAIL(meta_handle.get_sstable_meta().get_macro_info().get_data_block_iter(id_iterator))) {
        LOG_WARN("get id iterator fail", K(ret));
      } else if (OB_FAIL(id_iterator.get_next_macro_id(macro_id))) {
        LOG_WARN("get first id fail", K(ret));
      } else if (is_contain(macro_ids, macro_id)) {
        void *buf = nullptr;
        ObSSTable *new_sstable = nullptr;
        if (!updated_tablet_handle.is_valid() // only get tablet for the first time
            && OB_FAIL(ObTabletCreateDeleteHelper::get_tablet(key, updated_tablet_handle))) {
          if (OB_TABLET_NOT_EXIST == ret) {
            ret = OB_EAGAIN;
            // tablet has been deleted, skip the defragmentation
          } else {
            LOG_WARN("fail to get tablet", K(ret), K(key));
          }
        } else if (OB_FAIL(updated_tablet_handle.get_obj()->get_meta_disk_addr(cur_addr))) {
          LOG_WARN("fail to get cur tablet addr", K(ret));
        } else if (OB_UNLIKELY(!tablet_handle.get_obj()->get_tablet_addr().is_equal_for_persistence(cur_addr))) {
          ret = OB_EAGAIN;
          // tablet has been changed, skip the defragmentation
        } else if (OB_ISNULL(buf = allocator.alloc(sizeof(ObSSTable)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to acquire sstable object", K(ret));
        } else if (FALSE_IT(new_sstable = new (buf) ObSSTable())) {
        } else if (OB_FAIL(rebuild_sstable(
            allocator,
            *(updated_tablet_handle.get_obj()),
            *sstable,
            data_version,
            sstable_index_builder,
            index_block_rebuilder,
            *new_sstable))) {
          LOG_WARN("fail to rebuild sstable and update tablet", K(ret));
        } else if (OB_FAIL(new_sstables.push_back(new_sstable))) {
          new_sstable->~ObSSTable();
          allocator.free(new_sstable);
          LOG_WARN("fail to push table handle to array", K(ret), KPC(sstable));
        }
      }
    }
  }

  if (OB_SUCC(ret) && !new_sstables.empty()) {
    ObLSService *ls_svr = MTL(ObLSService*);
    ObLSHandle ls_handle;

    if (OB_FAIL(ls_svr->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD))) {
      LOG_WARN("fail to get ls handle", K(ret), K(ls_id), KPC(tablet_handle.get_obj()));
    } else {
      const int64_t rebuild_seq = ls_handle.get_ls()->get_rebuild_seq();
      if (OB_UNLIKELY(!ls_handle.is_valid())) {
        LOG_WARN("la handle is invalid", K(ret), K(ls_handle));
      } else if (OB_FAIL(ls_handle.get_ls()->update_tablet_table_store(
          rebuild_seq, updated_tablet_handle, new_sstables))) {
        LOG_WARN("fail to replace small sstables in the tablet",
            K(ret), K(rebuild_seq), K(updated_tablet_handle), K(new_sstables));
      } else {
        rewrite_cnt += new_sstables.count();
      }
    }
  }

  if (!new_sstables.empty()) {
    for (int64_t i = 0; i < new_sstables.count(); i++) {
      ObITable *table = new_sstables[i];
      if (OB_LIKELY(nullptr != table)) {
        table->~ObITable();
        allocator.free(table);
      }
    }
  }

  return ret;
}

int ObSharedMacroBlockMgr::rebuild_sstable(
    common::ObArenaAllocator &allocator,
    const ObTablet &tablet,
    const ObSSTable &old_sstable,
    const uint64_t data_version,
    ObSSTableIndexBuilder &sstable_index_builder,
    ObIndexBlockRebuilder &index_block_rebuilder,
    ObSSTable &new_sstable)
{
  int ret = OB_SUCCESS;
  ObWholeDataStoreDesc data_desc;
  ObMergeType merge_type;
  sstable_index_builder.reset();
  index_block_rebuilder.reset();
  ObDataMacroBlockMeta data_macro_meta;
  ObMacroBlockHandle block_handle;
  ObBlockInfo block_info;
  ObMacroBlocksWriteCtx write_ctx;
  ObSSTableMergeRes res;
  ObSSTableMetaHandle old_meta_handle;
  ObSSTableMetaHandle new_meta_handle;
  common::ObArenaAllocator read_allocator;

  if (OB_FAIL(old_sstable.get_meta(old_meta_handle))) {
    LOG_WARN("get meta handle fail", K(ret), K(old_sstable));
  } else if (OB_FAIL(parse_merge_type(old_sstable, merge_type))) {
    LOG_WARN("fail to parse merge type from old_sstable", K(ret));
  } else if (OB_FAIL(prepare_data_desc(
      tablet,
      old_meta_handle.get_sstable_meta().get_basic_meta(),
      merge_type,
      old_sstable.get_key(),
      tablet.get_snapshot_version(),
      data_version,
      old_sstable.get_end_scn(),
      data_desc))) {
    LOG_WARN("fail to prepare data desc", K(ret), "merge_type", merge_type_to_str(merge_type), K(tablet.get_snapshot_version()));
  } else if (OB_FAIL(sstable_index_builder.init(data_desc.get_desc(), nullptr, ObSSTableIndexBuilder::DISABLE))) {
    LOG_WARN("fail to init sstable index builder", K(ret), K(data_desc));
  } else if (OB_FAIL(index_block_rebuilder.init(sstable_index_builder))) {
    LOG_WARN("fail to init index block rebuilder", K(ret));
  } else if (OB_FAIL(read_sstable_block(old_sstable, block_handle, read_allocator))) {
    LOG_WARN("fail to read old_sstable's block", K(ret), K(old_sstable));
  } else if (OB_FAIL(write_block(
      block_handle.get_buffer(), block_handle.get_data_size(), block_info, write_ctx))) {
    LOG_WARN("fail to write old_sstable's buf to new block", K(ret));
  } else if (OB_FAIL(index_block_rebuilder.append_macro_row(
      block_handle.get_buffer(), block_handle.get_data_size(), block_info.macro_id_))) {
    LOG_WARN("fail to append macro row", K(ret), K(block_info));
  } else if (OB_FAIL(index_block_rebuilder.close())) {
    LOG_WARN("fail to close index block rebuilder", K(ret));
  } else if (OB_FAIL(sstable_index_builder.close(res))) {
    LOG_WARN("fail to close sstable index builder", K(ret));
  } else if (OB_FAIL(create_new_sstable(allocator, res, old_sstable, block_info, new_sstable))) {
    LOG_WARN("fail to create new sstable", K(ret), K(tablet.get_tablet_meta()), K(old_sstable));
  } else if (OB_FAIL(new_sstable.set_upper_trans_version(allocator, old_sstable.get_upper_trans_version()))) {
    LOG_WARN("fail to update upper trans version", K(ret), K(old_sstable.get_upper_trans_version()));
  } else if (OB_FAIL(new_sstable.get_meta(new_meta_handle))) {
    LOG_WARN("get meta handle fail", K(ret), K(new_sstable));
  } else if (OB_UNLIKELY(new_sstable.get_key() != old_sstable.get_key())
      || OB_FAIL(ObSSTableMetaChecker::check_sstable_meta_strict_equality(
          old_meta_handle.get_sstable_meta(), new_meta_handle.get_sstable_meta()))) {
    ret = OB_INVALID_DATA;
    LOG_WARN("new sstable is not equal to old sstable", K(ret), K(new_sstable), K(old_sstable));
  } else {
    FLOG_INFO("successfully rebuild one sstable", K(ret), K(block_info), K(new_sstable.get_key()));
  }
  return ret;
}

int ObSharedMacroBlockMgr::create_new_sstable(
    common::ObArenaAllocator &allocator,
    const ObSSTableMergeRes &res,
    const ObSSTable &old_table,
    const ObBlockInfo &block_info,
    ObSSTable &new_sstable) const
{
  int ret = OB_SUCCESS;
  ObTabletCreateSSTableParam param;
  ObSSTableMetaHandle meta_handle;
  if (OB_FAIL(old_table.get_meta(meta_handle))) {
    LOG_WARN("get meta handle fail", K(ret), K(old_table));
  } else if (OB_FAIL(param.init_for_small_sstable(res, old_table.get_key(),
                                                  meta_handle.get_sstable_meta(), block_info))) {
    LOG_WARN("fail to init create sstable param for small sst",
        K(ret), K(res), K(old_table.get_key()), K(meta_handle.get_sstable_meta()), K(block_info));
  } else if (OB_UNLIKELY(!param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(param));
  } else if (OB_FAIL(new_sstable.init(param, &allocator))) {
    LOG_WARN("failed to init sstable", K(ret), K(param));
  }

  return ret;
}

int ObSharedMacroBlockMgr::prepare_data_desc(
    const ObTablet &tablet,
    const ObSSTableBasicMeta &basic_meta,
    const ObMergeType &merge_type,
    const storage::ObITable::TableKey &table_key,
    const int64_t snapshot_version,
    const int64_t cluster_version,
    const share::SCN &end_scn,
    ObWholeDataStoreDesc &data_desc) const
{
  int ret = OB_SUCCESS;
  data_desc.reset();
  if (is_mds_merge(merge_type)) {
    const ObStorageSchema *storage_schema = ObMdsSchemaHelper::get_instance().get_storage_schema();
    if (OB_ISNULL(storage_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("storage schema is null", K(ret), KP(storage_schema));
    } else if (OB_UNLIKELY(!storage_schema->is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("mds storage schema is invalid", K(ret), KP(storage_schema), KPC(storage_schema));
    } else if (OB_FAIL(data_desc.init(
          *storage_schema,
          tablet.get_tablet_meta().ls_id_,
          tablet.get_tablet_meta().tablet_id_,
          merge_type,
          snapshot_version,
          cluster_version,
          end_scn))) {
      LOG_WARN("failed to init static desc", K(ret), KPC(storage_schema),
        K(tablet), "merge_type", merge_type_to_str(merge_type), K(snapshot_version), K(cluster_version));
    }
  } else {
    ObArenaAllocator tmp_arena("ShrBlkMgrTmp");
    const uint16_t cg_idx = table_key.get_column_group_id();
    const ObStorageColumnGroupSchema *cg_schema = nullptr;
    ObStorageSchema *storage_schema = nullptr;
    if (OB_FAIL(tablet.load_storage_schema(tmp_arena, storage_schema))) {
    LOG_WARN("fail to load storage schema", K(ret), K(tablet));
    } else {
      if (table_key.is_cg_sstable()) {
        if (OB_UNLIKELY(cg_idx < 0 || cg_idx >= storage_schema->get_column_group_count())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected cg idx", K(ret), K(cg_idx), KPC(storage_schema));
        } else {
          cg_schema = &storage_schema->get_column_groups().at(cg_idx);
        }
      }
    }

    if (FAILEDx(data_desc.init(
          *storage_schema,
          tablet.get_tablet_meta().ls_id_,
          tablet.get_tablet_meta().tablet_id_,
          merge_type,
          snapshot_version,
          cluster_version,
          end_scn,
          cg_schema,
          cg_idx))) {
      LOG_WARN("failed to init static desc", K(ret), KPC(storage_schema), KPC(cg_schema), K(cg_idx),
        K(tablet), "merge_type", merge_type_to_str(merge_type), K(snapshot_version), K(cluster_version));
    } else if (OB_FAIL(data_desc.get_desc().update_basic_info_from_macro_meta(basic_meta))) {
      // overwrite the encryption related memberships, otherwise these memberships of new sstable may differ
      // from that of old sstable, since the encryption method of one tablet may change before defragmentation
      LOG_WARN("failed to update basic info from macro_meta", KR(ret), K(basic_meta));
    } else if (OB_FAIL(data_desc.get_col_desc().mock_valid_col_default_checksum_array(basic_meta.column_cnt_))) {
      LOG_WARN("fail to mock valid col default checksum array", K(ret), K(basic_meta.column_cnt_));
    }
    ObTabletObjLoadHelper::free(tmp_arena, storage_schema);
  }
  return ret;
}

int ObSharedMacroBlockMgr::parse_merge_type(const ObSSTable &sstable, ObMergeType &merge_type) const
{
  int ret = OB_SUCCESS;
  merge_type = ObMergeType::INVALID_MERGE_TYPE;

  if (sstable.is_major_sstable()) {
    merge_type = sstable.is_meta_major_sstable()
               ? ObMergeType::META_MAJOR_MERGE
               : ObMergeType::MAJOR_MERGE;
  } else if (sstable.is_minor_sstable()) {
    merge_type = ObMergeType::MINOR_MERGE;
  } else if (sstable.is_mds_mini_sstable()) {
    merge_type = ObMergeType::MDS_MINI_MERGE;
  } else if (sstable.is_mds_minor_sstable()) {
    merge_type = ObMergeType::MDS_MINOR_MERGE;
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
    ObMacroBlockHandle &block_handle,
    common::ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  ObMacroBlockReadInfo read_info;
  ObSSTableMetaHandle meta_handle;
  ObMacroIdIterator id_iterator;
  MacroBlockId macro_id;

  if (OB_FAIL(sstable.get_meta(meta_handle))) {
    LOG_WARN("get meta handle fail", K(ret), K(sstable));
  } else if (OB_FAIL(meta_handle.get_sstable_meta().get_macro_info().get_data_block_iter(id_iterator))) {
    LOG_WARN("get id iterator fail", K(ret));
  } else if (OB_FAIL(id_iterator.get_next_macro_id(macro_id))) {
    LOG_WARN("get first id fail", K(ret));
  } else {
    read_info.macro_block_id_ = macro_id;
    read_info.offset_ = sstable.get_macro_offset();
    read_info.size_ = upper_align(sstable.get_macro_read_size(), DIO_READ_ALIGN_SIZE);
    read_info.io_desc_.set_wait_event(ObWaitEventIds::DB_FILE_COMPACT_READ);
    read_info.io_timeout_ms_ = GCONF._data_storage_io_timeout / 1000L;
    read_info.io_desc_.set_resource_group_id(THIS_WORKER.get_group_id());
    read_info.io_desc_.set_sys_module_id(ObIOModule::SHARED_MACRO_BLOCK_MGR_IO);

    if (OB_ISNULL(read_info.buf_ = reinterpret_cast<char*>(allocator.alloc(read_info.size_)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "failed to alloc macro read info buffer", K(ret), K(read_info.size_));
    } else {
      if (OB_FAIL(ObBlockManager::read_block(read_info, block_handle))) {
        LOG_WARN("fail to read block", K(ret), K(read_info));
      } else if (OB_UNLIKELY(!block_handle.is_valid()
            || sstable.get_macro_read_size() != block_handle.get_data_size())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("block handle is invalid", K(ret), K(block_handle));
      }
    }
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
