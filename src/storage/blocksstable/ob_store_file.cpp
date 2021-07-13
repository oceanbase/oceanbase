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
#include "ob_store_file.h"
#include "lib/file/file_directory_utils.h"
#include "share/config/ob_server_config.h"
#include "share/ob_force_print_log.h"
#include "storage/ob_partition_service.h"
#include "storage/ob_partition_meta_block_reader.h"
#include "storage/ob_table_mgr_meta_block_reader.h"
#include "storage/ob_tenant_config_mgr.h"
#include "storage/ob_tenant_config_meta_block_reader.h"
#include "storage/ob_server_checkpoint_log_reader.h"
#include "storage/ob_server_checkpoint_log_reader_v1.h"
#include "storage/ob_server_checkpoint_writer.h"
#include "storage/ob_data_macro_id_iterator.h"
#include "ob_macro_meta_block_reader.h"
#include "ob_storage_cache_suite.h"
#include "lib/utility/ob_tracepoint.h"
#include "storage/compaction/ob_micro_block_iterator.h"
#include "ob_raid_file_system.h"
#include "storage/ob_file_system_util.h"
#include "ob_local_file_system.h"
#include "storage/blocksstable/ob_macro_block_struct.h"

using namespace oceanbase::common;
using namespace oceanbase::blocksstable;
using namespace oceanbase::storage;

namespace oceanbase {
namespace blocksstable {
/**
 * ---------------------------------------------------------ObMacroBlockHandle-----------------------------------------------------------
 */
ObMacroBlocksHandle::ObMacroBlocksHandle() : macro_id_list_(), file_(nullptr)
{}

ObMacroBlocksHandle::~ObMacroBlocksHandle()
{
  reset();
}

int ObMacroBlocksHandle::add(const MacroBlockId& macro_id)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(macro_id_list_.push_back(macro_id))) {
    LOG_WARN("failed to add macro id", K(ret));
  } else {
    if (OB_ISNULL(file_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("fail to get file", K(ret), KP(file_));
    } else if (OB_FAIL(file_->inc_ref(macro_id))) {
      LOG_ERROR("failed to inc ref of pg_file", K(ret), K(macro_id));
    }

    if (OB_FAIL(ret)) {
      macro_id_list_.pop_back();
    }
  }

  return ret;
}

int ObMacroBlocksHandle::assign(const common::ObIArray<MacroBlockId>& list)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(file_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("fail to get file", K(ret), KP(file_));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < list.count(); ++i) {
    if (!list.at(i).is_valid()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("list.at(i) isn't valid, ", K(ret), K(i), K(list.at(i)));
    } else if (OB_FAIL(macro_id_list_.push_back(list.at(i)))) {
      LOG_WARN("failed to add macro", K(ret));
    } else {
      if (OB_FAIL(file_->inc_ref(list.at(i)))) {
        LOG_ERROR("failed to inc ref of pg_file", K(ret), "macro_id", list.at(i));
      }
      if (OB_FAIL(ret)) {
        macro_id_list_.pop_back();
      }
    }
  }
  return ret;
}

void ObMacroBlocksHandle::reset()
{
  if (macro_id_list_.count() > 0) {
    int tmp_ret = OB_SUCCESS;
    if (OB_ISNULL(file_)) {
      tmp_ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("fail to get file", K(tmp_ret), KP(file_));
    } else {
      for (int64_t i = 0; i < macro_id_list_.count(); ++i) {
        if (OB_SUCCESS != (tmp_ret = file_->dec_ref(macro_id_list_.at(i)))) {
          LOG_ERROR("failed to dec ref of pg_file", K(tmp_ret), K(*file_), "macro_id", macro_id_list_.at(i));
        }
      }
    }
  }
  macro_id_list_.reset();
}

int ObMacroBlocksHandle::reserve(const int64_t block_cnt)
{
  int ret = OB_SUCCESS;
  if (block_cnt > 0) {
    if (OB_FAIL(macro_id_list_.reserve(block_cnt))) {
      LOG_WARN("fail to reserve macro id list", K(ret));
    }
  }
  return ret;
}

ObMacroBlockHandle::~ObMacroBlockHandle()
{
  if (macro_id_.is_valid()) {
    int ret = OB_SUCCESS;
    if (OB_ISNULL(file_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("fail to get file", K(ret));
    } else if (OB_FAIL(file_->dec_ref(macro_id_))) {
      LOG_ERROR("failed to dec ref of pg_file", K(ret), K(macro_id_));
    }
  }
}

ObMacroBlockHandle::ObMacroBlockHandle(const ObMacroBlockHandle& other)
{
  *this = other;
}

ObMacroBlockHandle& ObMacroBlockHandle::operator=(const ObMacroBlockHandle& other)
{
  int ret = OB_SUCCESS;
  if (&other != this) {
    reset();
    file_ = other.file_;
    io_handle_ = other.io_handle_;
    macro_id_ = other.macro_id_;
    if (macro_id_.is_valid()) {
      if (OB_ISNULL(file_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("fail to get file", K(ret));
      } else if (OB_FAIL(file_->inc_ref(macro_id_))) {
        LOG_ERROR("failed to inc ref of pg_file", K(ret), K(macro_id_));
      }
      if (OB_FAIL(ret)) {
        macro_id_.reset();
      }
    }
  }
  return *this;
}

void ObMacroBlockHandle::reset()
{
  io_handle_.reset();
  if (macro_id_.is_valid()) {
    int ret = OB_SUCCESS;
    if (OB_ISNULL(file_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("fail to get file", K(ret));
    } else if (OB_FAIL(file_->dec_ref(macro_id_))) {
      LOG_ERROR("failed to dec ref of pg_file", K(ret), K(macro_id_));
    } else {
      file_ = NULL;
      macro_id_.reset();
    }
  }
  block_write_ctx_ = nullptr;
}

void ObMacroBlockHandle::reuse()
{
  io_handle_.reset();
  if (macro_id_.is_valid()) {
    int ret = OB_SUCCESS;
    if (OB_ISNULL(file_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("fail to get file", K(ret));
    } else if (OB_FAIL(file_->dec_ref(macro_id_))) {
      LOG_ERROR("failed to dec ref of pg_file", K(ret), K(macro_id_));
    } else {
      macro_id_.reset();
    }
  }
  block_write_ctx_ = nullptr;
}

int ObMacroBlockHandle::wait(const int64_t timeout_ms)
{
  int ret = OB_SUCCESS;
  if (io_handle_.is_empty()) {
    // do nothing
  } else if (OB_FAIL(io_handle_.wait(timeout_ms))) {
    STORAGE_LOG(WARN, "Fail to wait block io, ", K(macro_id_), K(ret), K(timeout_ms));
    int tmp_ret = OB_SUCCESS;
    int io_errno = 0;
    if (OB_SUCCESS != (tmp_ret = io_handle_.get_io_errno(io_errno))) {
      STORAGE_LOG(WARN, "Fail to get io errno, ", K(macro_id_), K(tmp_ret));
    } else if (0 != io_errno) {
      STORAGE_LOG(ERROR, "Fail to io macro block, ", K(macro_id_), K(ret), K(timeout_ms), K(io_errno));
      if (OB_ISNULL(file_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("fail to get file", K(ret));
      } else {
        char error_msg[common::OB_MAX_ERROR_MSG_LEN];
        MEMSET(error_msg, 0, sizeof(error_msg));
        if (OB_SUCCESS != (tmp_ret = databuff_printf(error_msg,
                               sizeof(error_msg),
                               "Sys IO error, ret=%d, errno=%d, errstr=%s",
                               ret,
                               io_errno,
                               strerror(io_errno)))) {
          STORAGE_LOG(WARN, "error msg is too long, ", K(macro_id_), K(tmp_ret), K(sizeof(error_msg)), K(io_errno));
        } else if (OB_SUCCESS !=
                   (tmp_ret = OB_FILE_SYSTEM.report_bad_block(macro_id_, ret, error_msg, file_->get_path()))) {
          STORAGE_LOG(WARN, "Fail to report bad block, ", K(macro_id_), K(tmp_ret), "erro_type", ret, K(error_msg));
        }
      }
    }
  }
  return ret;
}

int ObMacroBlockHandle::set_macro_block_id(const MacroBlockId& macro_block_id)
{
  int ret = common::OB_SUCCESS;

  if (macro_id_.is_valid()) {
    ret = common::OB_ERR_SYS;
    STORAGE_LOG(ERROR, "cannot set macro block id twice", K(ret), K(macro_block_id), K(*this));
  } else if (!macro_block_id.is_valid()) {
    ret = common::OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid args", K(ret), K(macro_block_id));
  } else {
    macro_id_ = macro_block_id;
    if (macro_id_.is_valid()) {
      if (OB_ISNULL(file_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("fail to get file", K(ret));
      } else if (OB_FAIL(file_->inc_ref(macro_id_))) {
        LOG_ERROR("failed to inc ref of pg_file", K(ret), K(macro_id_));
      }
      if (OB_FAIL(ret)) {
        macro_id_.reset();
      }
    }
  }
  return ret;
}

/*MacroBlockHandleV1: to be removed in next version*/

ObMacroBlockHandleV1::ObMacroBlockHandleV1() : macro_id_(), io_handle_()
{}

ObMacroBlockHandleV1::~ObMacroBlockHandleV1()
{
  if (macro_id_.is_valid()) {
    OB_STORE_FILE.dec_ref(macro_id_);
  }
}

ObMacroBlockHandleV1::ObMacroBlockHandleV1(const ObMacroBlockHandleV1& other)
{
  *this = other;
}

ObMacroBlockHandleV1& ObMacroBlockHandleV1::operator=(const ObMacroBlockHandleV1& other)
{
  if (&other != this) {
    reset();
    io_handle_ = other.io_handle_;
    macro_id_ = other.macro_id_;
    if (macro_id_.is_valid()) {
      OB_STORE_FILE.inc_ref(macro_id_);
    }
  }
  return *this;
}

void ObMacroBlockHandleV1::reset()
{
  io_handle_.reset();
  if (macro_id_.is_valid()) {
    OB_STORE_FILE.dec_ref(macro_id_);
    macro_id_.reset();
  }
}

int ObMacroBlockHandleV1::wait(const int64_t timeout_ms)
{
  int ret = OB_SUCCESS;
  if (!io_handle_.is_empty() && OB_FAIL(io_handle_.wait(timeout_ms))) {
    STORAGE_LOG(WARN, "Fail to wait block io, ", K(macro_id_), K(ret), K(timeout_ms));
    int tmp_ret = OB_SUCCESS;
    int io_errno = 0;
    if (OB_SUCCESS != (tmp_ret = io_handle_.get_io_errno(io_errno))) {
      STORAGE_LOG(WARN, "Fail to get io errno, ", K(macro_id_), K(tmp_ret));
    } else if (0 != io_errno) {
      STORAGE_LOG(ERROR, "Fail to io macro block, ", K(macro_id_), K(ret), K(timeout_ms), K(io_errno));
    }
  }
  return ret;
}

int ObMacroBlockHandleV1::set_macro_block_id(const MacroBlockId& macro_block_id)
{
  int ret = common::OB_SUCCESS;

  if (macro_id_.is_valid()) {
    ret = common::OB_ERR_SYS;
    STORAGE_LOG(ERROR, "cannot set macro block id twice", K(ret), K(macro_block_id), K(*this));
  } else if (!macro_block_id.is_valid()) {
    ret = common::OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid args", K(ret), K(macro_block_id));
  } else {
    macro_id_ = macro_block_id;
    if (macro_id_.is_valid()) {
      OB_STORE_FILE.inc_ref(macro_id_);
    }
  }
  return ret;
}

/**
 * ---------------------------------------------------------ObStoreFileGCTask-----------------------------------------------------------
 */
ObStoreFileGCTask::ObStoreFileGCTask()
{}

ObStoreFileGCTask::~ObStoreFileGCTask()
{}

// TODO Before mini merge adjustment, create PG, which will be cored here
void ObStoreFileGCTask::runTimerTask()
{
  OB_STORE_FILE.mark_and_sweep();
}

/**
 * ---------------------------------------------------------ObAllMacroIdIterator-----------------------------------------------------------
 */
ObAllMacroIdIterator::ObAllMacroIdIterator() : cur_pos_(0)
{}

ObAllMacroIdIterator::~ObAllMacroIdIterator()
{}

int ObAllMacroIdIterator::get_next_macro_id(MacroBlockId& block_id)
{
  int ret = OB_SUCCESS;
  ObMacroBlockInfo macro_info;
  if (OB_FAIL(OB_STORE_FILE.get_macro_block_info(cur_pos_, macro_info))) {
    if (OB_BEYOND_THE_RANGE == ret) {
      ret = OB_ITER_END;
    } else {
      STORAGE_LOG(WARN, "fail to get macro block info", K(ret), K(cur_pos_));
    }
  } else {
    MacroBlockId macro_id(0, 0, macro_info.write_seq_, cur_pos_);
    block_id = macro_id;
    ++cur_pos_;
  }
  return ret;
}

/**
 * ---------------------------------------------------------ObStoreFile-----------------------------------------------------------
 */
ObStoreFile::ObStoreFile()
    : is_inited_(false),
      is_opened_(false),
      allocator_(),
      block_lock_(),
      free_block_push_pos_(0),
      free_block_pop_pos_(0),
      free_block_cnt_(0),
      free_block_array_(NULL),
      macro_block_bitmap_(NULL),
      macro_block_info_(),
      ckpt_lock_(),
      meta_array_lock_(),
      cur_meta_array_pos_(0),
      gc_task_(),
      inspect_bad_block_task_(),
      print_buffer_(NULL),
      print_buffer_size_(ObLogger::MAX_LOG_SIZE - 100),
      mark_cost_time_(0),
      sweep_cost_time_(0),
      hold_macro_cnt_(0),
      bad_block_lock_(),
      store_file_system_(NULL),
      is_mark_sweep_enabled_(false),
      is_doing_mark_sweep_(false),
      cond_()
{
  MEMSET(used_macro_cnt_, 0, sizeof(used_macro_cnt_));
}

ObStoreFile::~ObStoreFile()
{
  destroy();
}

ObStoreFile& ObStoreFile::get_instance()
{
  static ObStoreFile instance_;
  return instance_;
}

int ObStoreFile::init(const ObStorageEnv& storage_env, ObStoreFileSystem* store_file_system)
{
  int ret = OB_SUCCESS;
  ObLogCursor replay_start_cursor;

  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "The ObStoreFile has been inited, ", K(ret));
  } else if (OB_UNLIKELY(!storage_env.is_valid()) || OB_ISNULL(store_file_system)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument, ", K(ret), K(storage_env), KP(store_file_system));
  } else if (OB_FAIL(databuff_printf(sstable_dir_, OB_MAX_FILE_NAME_LENGTH, "%s", storage_env.sstable_dir_))) {
    STORAGE_LOG(WARN, "The block file path is too long, ", K(ret), K(storage_env.data_dir_));
  } else if (OB_FAIL(allocator_.init(OB_MALLOC_NORMAL_BLOCK_SIZE, lib::ObLabel("StorFileMgr")))) {
    STORAGE_LOG(WARN, "fail to init allocator", K(ret));
  } else if (OB_UNLIKELY(NULL == (print_buffer_ = (char*)allocator_.alloc(print_buffer_size_)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "Fail to allocate memory, ", K(ret), K_(print_buffer_size));
  } else if (OB_FAIL(cond_.init(common::ObWaitEventIds::DEFAULT_COND_WAIT))) {
    STORAGE_LOG(WARN, "fail to init thread cond", K(ret));
  } else {
    store_file_system_ = store_file_system;

    if (OB_SUCC(ret)) {
      if (OB_FAIL(alloc_memory(store_file_system_->get_total_macro_block_count(),
              free_block_array_,
              macro_block_bitmap_,
              macro_block_info_))) {
        STORAGE_LOG(WARN, "fail to alloc memory", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      free_block_push_pos_ = 0;
      free_block_pop_pos_ = 0;
      free_block_cnt_ = 0;
      is_inited_ = true;
    }
  }

  if (!is_inited_) {
    destroy();
  } else {
    STORAGE_LOG(INFO, "The store file has been inited!");
  }
  return ret;
}

int ObStoreFile::open(const bool is_physical_flashback)
{
  int ret = OB_SUCCESS;
  bool is_replay_old = false;

  FLOG_INFO("open store file");
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "The ObStoreFile has not been inited, ", K(ret));
  } else if (OB_UNLIKELY(is_opened_)) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "The ObStoreFile has been started, ", K(ret));
  } else if (OB_FAIL(
                 ObMacroBlockMetaMgr::get_instance().init(store_file_system_->get_total_macro_block_count() + 16))) {
    STORAGE_LOG(WARN, "Fail to init ObMacroBlockMetaMgr, ", K(ret));
  } else if (OB_FAIL(read_checkpoint_and_replay_log(is_replay_old))) {
    STORAGE_LOG(WARN, "fail to read checkpoint and replay log", K(ret));
  } else {
    // add ref to reserve macro blocks
    for (uint32_t i = 0; i < ObStoreFileSystem::RESERVED_MACRO_BLOCK_INDEX; ++i) {
      MacroBlockId macro_id;
      macro_id.set_local_block_id(i);
      inc_ref(macro_id);
    }

    // add ref to meta macro blocks
    for (int64_t i = 0; OB_SUCC(ret) && i < meta_block_ids_[cur_meta_array_pos_].count(); ++i) {
      inc_ref(meta_block_ids_[cur_meta_array_pos_].at(i));
    }
    // mark and sweep, gc free macro blocks
    if (OB_SUCC(ret)) {
      enable_mark_sweep();
      mark_and_sweep();
      if (is_physical_flashback) {
        is_opened_ = true;
        STORAGE_LOG(INFO, "in physical flashback mode, no need start gc task");
      } else if (OB_FAIL(TG_START(lib::TGDefIDs::StoreFileGC))) {
        STORAGE_LOG(WARN, "The timer has not been inited, ", K(ret));
      } else if (OB_FAIL(TG_SCHEDULE(lib::TGDefIDs::StoreFileGC, gc_task_, RECYCLE_DELAY_US, true))) {
        STORAGE_LOG(WARN, "Fail to schedule gc task, ", K(ret));
      } else if (OB_FAIL(TG_SCHEDULE(lib::TGDefIDs::StoreFileGC, inspect_bad_block_task_, INSPECT_DELAY_US, true))) {
        STORAGE_LOG(WARN, "Fail to schedule bad_block_inspect task, ", K(ret));
      } else {
        is_opened_ = true;
        if (is_replay_old) {
          ObLogCursor cur_cursor;
          if (OB_FAIL(SLOGGER.get_active_cursor(cur_cursor))) {
            LOG_WARN("fail to get active cursor", K(ret));
          } else if (OB_FAIL(ObServerCheckpointWriter::get_instance().write_checkpoint(cur_cursor))) {
            LOG_WARN("fail to write checkpoint", K(ret));
          } else {
            for (int64_t i = 0; OB_SUCC(ret) && i < meta_block_ids_[cur_meta_array_pos_].count(); ++i) {
              dec_ref(meta_block_ids_[cur_meta_array_pos_].at(i));
            }
            meta_block_ids_[cur_meta_array_pos_].reset();
          }
        }
      }
    }
  }

  if (!is_opened_) {
    STORAGE_LOG(INFO, "The store file open failed", K(ret));
  } else {
    STORAGE_LOG(INFO, "The store file has been opened!");
  }
  return ret;
}

void ObStoreFile::destroy()
{
  STORAGE_LOG(INFO, "The store file will be destroyed!");
  inspect_bad_block_task_.destroy();
  TG_STOP(lib::TGDefIDs::StoreFileGC);
  TG_WAIT(lib::TGDefIDs::StoreFileGC);
  SLOGGER.destroy();
  ObMacroBlockMetaMgr::get_instance().destroy();

  lib::ObMutexGuard bad_block_guard(bad_block_lock_);
  bad_block_infos_.reset();

  meta_block_ids_[0].reset();
  meta_block_ids_[1].reset();

  cur_meta_array_pos_ = 0;
  macro_block_info_.reset();
  if (nullptr != macro_block_bitmap_) {
    allocator_.free(macro_block_bitmap_);
    macro_block_bitmap_ = nullptr;
  }
  if (nullptr != free_block_array_) {
    allocator_.free(free_block_array_);
    free_block_array_ = nullptr;
  }
  allocator_.reset();
  free_block_push_pos_ = 0;
  free_block_pop_pos_ = 0;
  free_block_cnt_ = 0;
  print_buffer_ = NULL;
  MEMSET(used_macro_cnt_, 0, sizeof(used_macro_cnt_));

  is_inited_ = false;
  is_opened_ = false;
  is_mark_sweep_enabled_ = false;
  is_doing_mark_sweep_ = false;
  cond_.destroy();
}

void ObStoreFile::stop()
{
  TG_STOP(lib::TGDefIDs::StoreFileGC);
  is_opened_ = false;
  STORAGE_LOG(INFO, "the store file gc task stopped");
}

void ObStoreFile::wait()
{
  TG_WAIT(lib::TGDefIDs::StoreFileGC);
  STORAGE_LOG(INFO, "the store file finish wait");
}

int ObStoreFile::write_block(const ObMacroBlockWriteInfo& write_info, ObMacroBlockHandle& macro_handle)
{
  int ret = OB_SUCCESS;
  const int64_t io_timeout_ms = GCONF._data_storage_io_timeout / 1000L;
  if (OB_FAIL(async_write_block(write_info, macro_handle))) {
    STORAGE_LOG(WARN, "Fail to async read block, ", K(ret), K(write_info));
  } else if (OB_FAIL(macro_handle.wait(io_timeout_ms))) {
    STORAGE_LOG(WARN, "Fail to wait io finish, ", K(ret), K(io_timeout_ms));
  }
  return ret;
}

int ObStoreFile::async_write_block(const ObMacroBlockWriteInfo& write_info, ObMacroBlockHandle& macro_handle)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_opened_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "The ObStoreFile has not been started, ", K(ret));
  } else if (OB_UNLIKELY(!write_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument, ", K(ret), K(write_info));
  } else if (!write_info.is_reuse_macro_block()) {
    if (OB_FAIL(alloc_block(macro_handle))) {
      STORAGE_LOG(WARN, "Fail to alloc data block, ", K(ret));
    }
  } else {  // reuse macro block
    macro_handle.reuse();
    if (OB_FAIL(macro_handle.set_macro_block_id(write_info.reuse_block_ctx_->get_macro_block_id()))) {
      LOG_WARN("failed to set macro block id", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    ObStoreFileWriteInfo store_write_info;
    store_write_info.block_id_ = macro_handle.get_macro_id();
    store_write_info.buf_ = write_info.buffer_;
    store_write_info.size_ = write_info.size_;
    store_write_info.io_desc_ = write_info.io_desc_;
    store_write_info.ctx_ = &write_info.block_write_ctx_->file_ctx_;
    store_write_info.reuse_block_ctx_ = write_info.reuse_block_ctx_;

    if (macro_handle.get_macro_id().block_index() < ObStoreFileSystem::RESERVED_MACRO_BLOCK_INDEX) {
      ret = OB_ERR_SYS;
      LOG_ERROR("cannot write super block",
          K(ret),
          "block_index",
          macro_handle.get_macro_id(),
          K(write_info),
          "real_block_id",
          macro_handle.get_macro_id().block_index());
    } else if (OB_FAIL(store_file_system_->async_write(store_write_info, macro_handle.get_io_handle()))) {
      STORAGE_LOG(WARN, "Fail to aio write, ", K(ret), K(write_info));
    } else {
      FLOG_INFO("Async write macro block, ", "macro_block_id", macro_handle.get_macro_id(), K_(free_block_cnt));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(write_info.block_write_ctx_->add_macro_block(macro_handle.get_macro_id(), write_info.meta_))) {
      LOG_WARN("failed to add macro block", K(ret), K(macro_handle));
    }
  }

  if (OB_FAIL(ret)) {
    macro_handle.reset();
  }
  return ret;
}

int ObStoreFile::read_block(const ObMacroBlockReadInfo& read_info, ObMacroBlockHandle& macro_handle)
{
  int ret = OB_SUCCESS;
  const int64_t io_timeout_ms = GCONF._data_storage_io_timeout / 1000L;
  if (OB_FAIL(async_read_block(read_info, macro_handle))) {
    STORAGE_LOG(WARN, "Fail to async read block, ", K(ret), K(read_info));
  } else if (OB_FAIL(macro_handle.wait(io_timeout_ms))) {
    STORAGE_LOG(WARN, "Fail to wait io finish, ", K(ret), K(io_timeout_ms));
  }
  return ret;
}

int ObStoreFile::async_read_block(const ObMacroBlockReadInfo& read_info, ObMacroBlockHandle& macro_handle)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "The ObStoreFile has not been inited, ", K(ret));
  } else if (OB_UNLIKELY(!read_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument, ", K(ret), K(read_info));
  } else {
    macro_handle.reuse();

    ObStoreFileReadInfo store_read_info;
    store_read_info.macro_block_ctx_ = read_info.macro_block_ctx_;
    store_read_info.offset_ = read_info.offset_;
    store_read_info.size_ = read_info.size_;
    store_read_info.io_desc_ = read_info.io_desc_;
    store_read_info.io_callback_ = read_info.io_callback_;
    if (OB_FAIL(store_file_system_->async_read(store_read_info, macro_handle.get_io_handle()))) {
      STORAGE_LOG(WARN, "Fail to async_read, ", K(ret));
    } else if (OB_FAIL(macro_handle.set_macro_block_id(read_info.macro_block_ctx_->get_macro_block_id()))) {
      LOG_WARN("failed to set macro block id", K(ret));
    }
  }
  return ret;
}

bool ObStoreFile::is_bad_block(const MacroBlockId& macro_block_id)
{
  bool is_exist = false;
  lib::ObMutexGuard bad_block_guard(bad_block_lock_);
  for (int64_t i = 0; i < bad_block_infos_.count(); ++i) {
    if (bad_block_infos_[i].macro_block_id_.block_index() == macro_block_id.block_index()) {
      is_exist = true;
      break;
    }
  }
  return is_exist;
}

int ObStoreFile::fsync()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "The ObStoreFile has not been inited, ", K(ret));
  } else if (OB_FAIL(store_file_system_->fsync())) {
    STORAGE_LOG(ERROR, "Fail to fsync data file, ", K(ret), KERRMSG);
  }
  return ret;
}

void ObStoreFile::inc_ref(const MacroBlockId macro_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(ERROR, "The ObStoreFile has not been inited, ", K(ret));
  } else if (OB_UNLIKELY(!is_valid(macro_id))) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(ERROR, "Invalid argument, ", K(ret), K(macro_id));
  } else {
    ATOMIC_AAF(&macro_block_info_[macro_id.block_index()].ref_cnt_, 1);
  }
}

void ObStoreFile::dec_ref(const MacroBlockId macro_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(ERROR, "The ObStoreFile has not been inited, ", K(ret));
  } else if (OB_UNLIKELY(!is_valid(macro_id))) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(ERROR, "Invalid argument, ", K(ret), K(macro_id));
  } else {
    const int64_t ref_cnt = ATOMIC_SAF(&macro_block_info_[macro_id.block_index()].ref_cnt_, 1);
    if (ref_cnt < 0) {
      ret = OB_ERR_SYS;
      STORAGE_LOG(ERROR,
          "macro block ref must not less than 0, fatal error",
          K(ret),
          "macro_block_id",
          macro_id.block_index(),
          K(ref_cnt));
      ob_abort();
    }
  }
}

void ObStoreFile::inc_ref(const ObIArray<MacroBlockId>& macro_id_list)
{
  for (int64_t i = 0; i < macro_id_list.count(); ++i) {
    inc_ref(macro_id_list.at(i));
  }
}

void ObStoreFile::dec_ref(const ObIArray<MacroBlockId>& macro_id_list)
{
  for (int64_t i = 0; i < macro_id_list.count(); ++i) {
    dec_ref(macro_id_list.at(i));
  }
}

int ObStoreFile::check_disk_full(const int64_t required_size) const
{
  int ret = OB_SUCCESS;
  const int64_t NO_LIMIT_PERCENT = 100;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
  } else if (required_size < 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid args", K(ret), K(required_size));
  } else {
    const int64_t required_count = required_size / store_file_system_->get_macro_block_size();
    const int64_t free_count = free_block_cnt_ - required_count;
    const int64_t used_percent = 100 - 100 * free_count / store_file_system_->get_total_macro_block_count();
    if (GCONF.data_disk_usage_limit_percentage != NO_LIMIT_PERCENT &&
        used_percent >= GCONF.data_disk_usage_limit_percentage) {
      ret = OB_CS_OUTOF_DISK_SPACE;
      if (REACH_TIME_INTERVAL(24 * 3600LL * 1000 * 1000 /* 24h */)) {
        STORAGE_LOG(
            ERROR, "disk is almost full", K(ret), K(required_size), K(required_count), K(free_count), K(used_percent));
      }
    }
  }
  return ret;
}

bool ObStoreFile::is_disk_full() const
{
  bool is_full = false;

  if (OB_SUCCESS != check_disk_full(0)) {
    is_full = true;
  }
  return is_full;
}

int ObStoreFile::get_store_status(ObMacroBlockMarkerStatus& status)
{
  int ret = OB_SUCCESS;
  if (!is_opened_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "The ObStoreFile has not been opened, ", K(ret));
  } else {
    status.total_block_count_ = store_file_system_->get_total_macro_block_count();
    status.reserved_block_count_ = 2;
    status.macro_meta_block_count_ = used_macro_cnt_[ObMacroBlockCommonHeader::MacroMeta];
    status.partition_meta_block_count_ = used_macro_cnt_[ObMacroBlockCommonHeader::PartitionMeta];
    status.data_block_count_ = used_macro_cnt_[ObMacroBlockCommonHeader::SSTableData];
    status.second_index_count_ = used_macro_cnt_[ObMacroBlockCommonHeader::MacroBlockSecondIndex];
    status.lob_data_block_count_ = used_macro_cnt_[ObMacroBlockCommonHeader::LobData];
    status.lob_second_index_count_ = used_macro_cnt_[ObMacroBlockCommonHeader::LobIndex];
    status.bloomfilter_count_ = used_macro_cnt_[ObMacroBlockCommonHeader::BloomFilterData];
    status.hold_count_ = hold_macro_cnt_;
    status.pending_free_count_ = 0;
    status.free_count_ = free_block_cnt_;
    status.mark_cost_time_ = mark_cost_time_;
    status.sweep_cost_time_ = sweep_cost_time_;
    status.hold_alert_time_ = 0;
  }
  return ret;
}

int ObStoreFile::report_bad_block(const MacroBlockId& macro_block_id, const int64_t error_type, const char* error_msg)
{
  int ret = OB_SUCCESS;
  const int64_t MAX_BAD_BLOCK_NUMBER = std::max(10L, store_file_system_->get_total_macro_block_count() / 100);
  if (!is_opened_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "The ObStoreFile has not been opened, ", K(ret));
  } else if (!macro_block_id.is_valid() || OB_TIMEOUT == error_type || NULL == error_msg) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument, ", K(ret), K(macro_block_id), K(error_type), KP(error_msg));
  } else if (is_bad_block(macro_block_id)) {
    ret = OB_SUCCESS;  // No need to print warn log
    STORAGE_LOG(INFO, "Already found this bad block, ", K(macro_block_id), K(error_type), K(error_msg));
  } else {
    ObBadBlockInfo bad_block_info;
    lib::ObMutexGuard bad_block_guard(bad_block_lock_);
    if (bad_block_infos_.count() >= MAX_BAD_BLOCK_NUMBER) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN,
          "Too many bad blocks! ",
          K(ret),
          K(bad_block_infos_.count()),
          K(MAX_BAD_BLOCK_NUMBER),
          K(macro_block_id),
          K(error_type),
          K(error_msg));
    } else if (OB_FAIL(
                   databuff_printf(bad_block_info.error_msg_, sizeof(bad_block_info.error_msg_), "%s", error_msg))) {
      STORAGE_LOG(WARN, "Error msg is too long, ", K(ret), K(error_msg), K(sizeof(bad_block_info.error_msg_)));
    } else {
      STRNCPY(bad_block_info.store_file_path_, get_store_file_path(), STRLEN(get_store_file_path()));
      bad_block_info.disk_id_ = macro_block_id.disk_no();
      bad_block_info.macro_block_id_ = macro_block_id;
      bad_block_info.error_type_ = error_type;
      bad_block_info.check_time_ = ObTimeUtility::current_time();
      if (OB_FAIL(bad_block_infos_.push_back(bad_block_info))) {
        STORAGE_LOG(WARN, "Fail to save bad block info, ", K(ret), K(bad_block_info), K(bad_block_infos_));
      } else {
        STORAGE_LOG(ERROR, "add bad block info", K(bad_block_info));
      }
    }
  }
  return ret;
}

int ObStoreFile::get_bad_block_infos(common::ObArray<ObBadBlockInfo>& bad_block_infos)
{
  int ret = OB_SUCCESS;
  if (!is_opened_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "The ObStoreFile has not been opened, ", K(ret));
  } else {
    lib::ObMutexGuard bad_block_guard(bad_block_lock_);
    if (OB_FAIL(bad_block_infos.assign(bad_block_infos_))) {
      STORAGE_LOG(WARN, "Fail to assign bad block infos, ", K(ret), K(bad_block_infos_));
    }
  }
  return ret;
}

int ObStoreFile::get_macro_block_info(const int64_t block_index, ObMacroBlockInfo& macro_block_info)
{
  int ret = OB_SUCCESS;
  lib::ObMutexGuard guard(block_lock_);  // guard access_time from async_write
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
  } else if (block_index < 0 || block_index >= store_file_system_->get_total_macro_block_count()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "beyond index range", K(ret), K(block_index));
  } else {
    macro_block_info = macro_block_info_[block_index];
  }
  return ret;
}

int ObStoreFile::is_free_block(const int64_t block_index, bool& is_free)
{
  int ret = OB_SUCCESS;
  is_free = false;

  lib::ObMutexGuard guard(block_lock_);
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (block_index < 0 || block_index >= store_file_system_->get_total_macro_block_count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid block index",
        K(ret),
        K(block_index),
        "total_count",
        store_file_system_->get_total_macro_block_count());
  } else {
    is_free = macro_block_info_[block_index].is_free_;
  }

  return ret;
}

int ObStoreFile::alloc_block(ObMacroBlockHandle& macro_handle)
{
  int ret = OB_SUCCESS;
  uint32_t block_idx = 0;
  macro_handle.reuse();
  const int64_t MAX_ALLOC_BLOCK_TRY_COUNT = 10;

  lib::ObMutexGuard guard(block_lock_);
  if (OB_UNLIKELY(free_block_cnt_ <= 0)) {
    ret = OB_CS_OUTOF_DISK_SPACE;
    STORAGE_LOG(ERROR,
        "Fail to alloc block, ",
        K(ret),
        K(free_block_cnt_),
        "total_count",
        store_file_system_->get_total_macro_block_count());
  } else {
    bool is_alloc_succ = false;
    for (int64_t i = 0; OB_SUCC(ret) && !is_alloc_succ && i < MAX_ALLOC_BLOCK_TRY_COUNT && free_block_cnt_ > 0; ++i) {
      block_idx = free_block_array_[free_block_pop_pos_];
      free_block_pop_pos_ = (free_block_pop_pos_ + 1) % store_file_system_->get_total_macro_block_count();
      --free_block_cnt_;
      MacroBlockId macro_id(0, 0, macro_block_info_[block_idx].write_seq_, block_idx);
      if (!is_bad_block(macro_id)) {
        if (OB_FAIL(macro_handle.set_macro_block_id(macro_id))) {
          LOG_ERROR("failed to set macro block id", K(ret), K(macro_id));
        } else {
          macro_block_info_[block_idx].is_free_ = false;
          ATOMIC_SET(&macro_block_info_[block_idx].access_time_, ObTimeUtility::current_time());
          is_alloc_succ = true;
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (is_alloc_succ) {
      } else if (OB_UNLIKELY(free_block_cnt_ <= 0)) {
        ret = OB_CS_OUTOF_DISK_SPACE;
        STORAGE_LOG(ERROR,
            "Fail to alloc block, ",
            K(ret),
            K(free_block_cnt_),
            "total_count",
            store_file_system_->get_total_macro_block_count());
      } else {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "Fail to alloc block", K(ret));
      }
    }
  }
  return ret;
}

void ObStoreFile::free_block(const uint32_t block_idx, bool& is_freed)
{
  is_freed = false;
  lib::ObMutexGuard guard(block_lock_);
  if (OB_UNLIKELY(block_idx >= store_file_system_->get_total_macro_block_count()) || OB_UNLIKELY(0 == block_idx)) {
    // BUG, should not happen
    STORAGE_LOG(ERROR,
        "The block idx is out of range, ",
        K(block_idx),
        "total_macro_block_count",
        store_file_system_->get_total_macro_block_count());
  } else if (OB_UNLIKELY(macro_block_info_[block_idx].is_free_)) {
    // BUG, should not happen
    STORAGE_LOG(ERROR, "The macro block is double freed, ", K(block_idx));
  } else if (OB_UNLIKELY(0 < ATOMIC_LOAD(&macro_block_info_[block_idx].ref_cnt_))) {
    // skip, ref cnt may larger than 0 here
  } else {
    is_freed = true;
    MacroBlockId macro_id(0, 0, macro_block_info_[block_idx].write_seq_, block_idx);
    macro_block_info_[block_idx].is_free_ = true;
    macro_block_info_[block_idx].write_seq_++;
    macro_block_info_[block_idx].access_time_ = 0;
    free_block_array_[free_block_push_pos_] = block_idx;
    free_block_push_pos_ = (free_block_push_pos_ + 1) % store_file_system_->get_total_macro_block_count();
    ++free_block_cnt_;
  }
}

int ObStoreFile::mark_macro_blocks()
{
  int ret = OB_SUCCESS;
  MacroBlockId macro_id;
  ObDataMacroIdIterator macro_id_iter;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "The ObStoreFile has not been inited, ", K(ret));
  } else if (OB_FAIL(macro_id_iter.init(store_file_system_->get_partition_service()))) {
    STORAGE_LOG(WARN, "fail to init macro id iter", K(ret));
  } else {
    // mark init
    MEMSET(macro_block_bitmap_, 0, (store_file_system_->get_total_macro_block_count() / 64 + 1) * sizeof(uint64_t));

    for (int64_t i = 0; i < ObStoreFileSystem::RESERVED_MACRO_BLOCK_INDEX && is_mark_sweep_enabled(); ++i) {
      bitmap_set(i);
    }

    // mark data block
    ObMacroBlockCommonHeader::MacroBlockType macro_type;
    used_macro_cnt_[ObMacroBlockCommonHeader::SSTableData] = 0;
    used_macro_cnt_[ObMacroBlockCommonHeader::MacroBlockSecondIndex] = 0;
    used_macro_cnt_[ObMacroBlockCommonHeader::LobData] = 0;
    used_macro_cnt_[ObMacroBlockCommonHeader::LobIndex] = 0;
    used_macro_cnt_[ObMacroBlockCommonHeader::BloomFilterData] = 0;
    while (OB_SUCC(macro_id_iter.get_next_macro_id(macro_id, macro_type)) && is_mark_sweep_enabled()) {
      if (!bitmap_test(macro_id.block_index())) {
        bitmap_set(macro_id.block_index());
        if (ObMacroBlockCommonHeader::MaxMacroType > macro_type && 0 <= macro_type) {
          used_macro_cnt_[macro_type]++;
        } else {
          STORAGE_LOG(ERROR, "Unexpected macro type iter from sstable macro iter", K(macro_type), K(macro_id));
        }
      }
    }
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    } else {
      if (is_mark_sweep_enabled()) {
        STORAGE_LOG(ERROR, "Fail to iter data macro block ids, ", K(ret));
      }
    }

    // mark meta block
    if (OB_SUCC(ret)) {
      lib::ObMutexGuard guard(meta_array_lock_);
      for (uint32_t i = 0; i < meta_block_ids_[cur_meta_array_pos_].count(); ++i) {
        bitmap_set(meta_block_ids_[cur_meta_array_pos_].at(i).block_index());
      }
    }
  }
  return ret;
}

void ObStoreFile::mark_and_sweep()
{
  int ret = OB_SUCCESS;
  MacroBlockId macro_id;
  int64_t print_pos = 0;
  int64_t hold_cnt = 0;
  int64_t free_cnt = 0;
  int64_t begin_time = 0;
  int64_t end_time = 0;
  bool is_freed = false;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "The ObStoreFile has not been inited, ", K(ret));
  } else if (!is_mark_sweep_enabled()) {
    STORAGE_LOG(INFO, "mark and sweep is disabled, do not mark and sweep this round");
  } else {
    set_mark_sweep_doing();
    // mark macro blocks
    begin_time = ObTimeUtility::current_time();
    if (OB_FAIL(mark_macro_blocks())) {
      STORAGE_LOG(WARN, "Fail to mark macro blocks, ", K(ret));
    } else {
      end_time = ObTimeUtility::current_time();
      mark_cost_time_ = end_time - begin_time;
    }

    // sweep
    begin_time = end_time;
    if (OB_SUCC(ret)) {
      for (int64_t i = 0; i < store_file_system_->get_total_macro_block_count() && is_mark_sweep_enabled(); ++i) {
        if (bitmap_test(i)) {
          // block is marked
          if (macro_block_info_[i].is_free_) {
            // BUG, should not happen
            STORAGE_LOG(ERROR, "the macro block is freed, ", K(i));
          }
        } else {
          // block is not marked
          if (0 == ATOMIC_LOAD(&(macro_block_info_[i].ref_cnt_))) {
            if (!macro_block_info_[i].is_free_) {
              free_block((uint32_t)i, is_freed);
              if (is_freed) {
                ++free_cnt;
                if (OB_SUCCESS != databuff_printf(print_buffer_, print_buffer_size_, print_pos, "%ld,", i)) {
                  print_buffer_[print_pos] = '\0';
                  STORAGE_LOG(INFO, "mark_and_sweep free blocks.", K(print_buffer_));
                  print_pos = 0;
                  databuff_printf(print_buffer_, print_buffer_size_, print_pos, ",%ld,", i);
                }
              }
            }
          } else {
            // unmark but is using
            ++hold_cnt;
          }
        }
      }
    }
    end_time = ObTimeUtility::current_time();
    sweep_cost_time_ = end_time - begin_time;

    hold_macro_cnt_ = hold_cnt;
    print_buffer_[print_pos] = '\0';
    STORAGE_LOG(INFO, "mark_and_sweep free blocks.", K(print_buffer_), K(free_cnt), K(hold_cnt));
    set_mark_sweep_done();
  }
}

ObMacroBlockWriteInfo::ObMacroBlockWriteInfo()
    : buffer_(NULL), size_(0), meta_(), io_desc_(), block_write_ctx_(NULL), reuse_block_ctx_(NULL)
{}

int ObStoreFile::add_disk(const ObString& diskgroup_name, const ObString& disk_path, const ObString& alias_name)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "The ObStoreFile has not been inited, ", K(ret));
  } else if (OB_FAIL(store_file_system_->add_disk(diskgroup_name, disk_path, alias_name))) {
    LOG_WARN("failed to add disk", K(ret), K(diskgroup_name), K(disk_path), K(alias_name));
  }
  return ret;
}

int ObStoreFile::drop_disk(const ObString& diskgroup_name, const ObString& alias_name)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "The ObStoreFile has not been inited, ", K(ret));
  } else if (OB_FAIL(store_file_system_->drop_disk(diskgroup_name, alias_name))) {
    LOG_WARN("failed to drop disk", K(ret), K(diskgroup_name), K(alias_name));
  }
  return ret;
}

int ObStoreFile::read_checkpoint_and_replay_log(bool& is_replay_old)
{
  int ret = OB_SUCCESS;
  int64_t super_block_version = -1;
  is_replay_old = false;
  if (OB_FAIL(store_file_system_->get_super_block_version(super_block_version))) {
    LOG_WARN("fail to get super block version", K(ret));
  } else {
    if (OB_SUPER_BLOCK_V2 == super_block_version) {
      ObServerCheckpointLogReaderV1 reader;
      ObSuperBlockV2 old_super_block;
      ObLogCursor cur_cursor;
      if (OB_FAIL(store_file_system_->read_old_super_block(old_super_block))) {
        LOG_WARN("fail to read old super block", K(ret));
      } else if (OB_FAIL(
                     reader.read_checkpoint_and_replay_log(old_super_block, meta_block_ids_[cur_meta_array_pos_]))) {
        STORAGE_LOG(WARN, "fail to read checkpoint and replay log", K(ret));
      } else {
        is_replay_old = true;
      }
    } else if (OB_SUPER_BLOCK_V3 == super_block_version) {
      ObServerCheckpointLogReader reader;
      if (OB_FAIL(reader.read_checkpoint_and_replay_log())) {
        STORAGE_LOG(WARN, "fail to read checkpoint and replay log", K(ret));
      }
    } else {
      ret = OB_ERR_SYS;
      LOG_ERROR("error sys, unexpected super block version", K(super_block_version));
    }
  }
  return ret;
}

// TODO(): will be removed next version
int ObStoreFile::async_read_block_v1(const ObMacroBlockReadInfo& read_info, ObMacroBlockHandleV1& macro_handle)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "The ObStoreFile has not been inited, ", K(ret));
  } else if (OB_UNLIKELY(!read_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument, ", K(ret), K(read_info));
  } else {
    macro_handle.reset();

    ObStoreFileReadInfo store_read_info;
    store_read_info.macro_block_ctx_ = read_info.macro_block_ctx_;
    store_read_info.offset_ = read_info.offset_;
    store_read_info.size_ = read_info.size_;
    store_read_info.io_desc_ = read_info.io_desc_;
    store_read_info.io_callback_ = read_info.io_callback_;
    if (OB_FAIL(store_file_system_->async_read(store_read_info, macro_handle.get_io_handle()))) {
      STORAGE_LOG(WARN, "Fail to async_read, ", K(ret));
    } else if (OB_FAIL(macro_handle.set_macro_block_id(read_info.macro_block_ctx_->get_macro_block_id()))) {
      LOG_WARN("failed to set macro block id", K(ret));
    }
  }
  return ret;
}

int ObStoreFile::wait_mark_sweep_finish()
{
  int ret = OB_SUCCESS;
  ObThreadCondGuard guard(cond_);
  while (is_doing_mark_sweep_) {
    cond_.wait_us(100);
  }
  return ret;
}

void ObStoreFile::set_mark_sweep_doing()
{
  ObThreadCondGuard guard(cond_);
  is_doing_mark_sweep_ = true;
}

void ObStoreFile::set_mark_sweep_done()
{
  ObThreadCondGuard guard(cond_);
  is_doing_mark_sweep_ = false;
  cond_.broadcast();
}

int ObStoreFile::resize_file(const int64_t new_data_file_size, const int64_t new_data_file_disk_percentage)
{
  int ret = OB_SUCCESS;
  disable_mark_sweep();
  if (OB_ISNULL(store_file_system_)) {
    // do nothing
  } else if (OB_FAIL(wait_mark_sweep_finish())) {
    LOG_WARN("fail to wait mark and sweep finish", K(ret));
  } else {
    lib::ObMutexGuard guard(block_lock_);
    if (OB_FAIL(store_file_system_->resize_file(new_data_file_size, new_data_file_disk_percentage))) {
      LOG_WARN("fail to resize file", K(ret));
    } else {
      const int64_t new_total_file_size =
          lower_align(store_file_system_->get_total_data_size(), store_file_system_->get_macro_block_size());
      const int64_t new_macro_block_cnt = new_total_file_size / store_file_system_->get_macro_block_size();
      const int64_t origin_macro_block_cnt = store_file_system_->get_total_macro_block_count();
      if (new_macro_block_cnt > origin_macro_block_cnt) {
        uint32_t* new_free_block_array = nullptr;
        uint64_t* new_macro_block_bitmap = nullptr;
        ObServerSuperBlock super_block = store_file_system_->get_server_super_block();
        super_block.content_.total_file_size_ = new_total_file_size;
        super_block.content_.total_macro_block_count_ = new_macro_block_cnt;
        super_block.content_.modify_timestamp_ = ObTimeUtility::current_time();
        ObStorageFile* file = OB_FILE_SYSTEM.get_server_root_handle().get_storage_file();
        if (OB_ISNULL(file)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("error unexpected, file must not be null", K(ret));
        } else if (OB_FAIL(alloc_memory(
                       new_macro_block_cnt, new_free_block_array, new_macro_block_bitmap, macro_block_info_))) {
          LOG_WARN("fail to alloc memory", K(ret), K(new_macro_block_cnt));
        } else if (OB_FAIL(file->write_super_block(super_block))) {
          LOG_WARN("fail to write super block", K(ret));
        } else {
          // copy free block info to new_free_block_array
          if (free_block_pop_pos_ > free_block_push_pos_) {
            MEMCPY(new_free_block_array,
                free_block_array_ + free_block_pop_pos_,
                (origin_macro_block_cnt - free_block_pop_pos_) * sizeof(uint32_t));
            MEMCPY(new_free_block_array + origin_macro_block_cnt - free_block_pop_pos_,
                free_block_array_,
                free_block_push_pos_ * sizeof(uint32_t));
          } else if (free_block_pop_pos_ < free_block_push_pos_) {
            MEMCPY(new_free_block_array,
                free_block_array_ + free_block_pop_pos_,
                (free_block_push_pos_ - free_block_pop_pos_) * sizeof(uint32_t));
          } else {
            MEMCPY(new_free_block_array, free_block_array_, free_block_cnt_ * sizeof(uint32_t));
          }
          free_block_pop_pos_ = 0;
          free_block_push_pos_ = free_block_cnt_;
          MEMCPY(new_macro_block_bitmap,
              macro_block_bitmap_,
              get_macro_bitmap_array_cnt(origin_macro_block_cnt) * sizeof(uint64_t));
          allocator_.free(free_block_array_);
          allocator_.free(macro_block_bitmap_);
          free_block_array_ = new_free_block_array;
          macro_block_bitmap_ = new_macro_block_bitmap;
          LOG_INFO("succeed to resize file", K(new_data_file_size));
        }
        if (OB_FAIL(ret)) {
          allocator_.free(new_free_block_array);
          allocator_.free(new_macro_block_bitmap);
        }
      }
    }
  }
  enable_mark_sweep();
  return ret;
}

int ObStoreFile::alloc_memory(const int64_t total_macro_block_cnt, uint32_t*& free_block_array,
    uint64_t*& macro_block_bitmap, ObSegmentArray<ObMacroBlockInfo>& macro_block_info_array)
{
  int ret = OB_SUCCESS;
  int64_t buf_len = 0;
  char* buf = nullptr;
  free_block_array = nullptr;
  macro_block_bitmap = nullptr;
  // alloc free block array
  if (OB_SUCC(ret)) {
    buf_len = sizeof(uint32_t) * total_macro_block_cnt;
    if (OB_UNLIKELY(NULL == (buf = static_cast<char*>(allocator_.alloc(buf_len))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "Fail to allocate memory, ", K(ret), K(buf_len));
    } else {
      MEMSET(buf, 0, buf_len);
      free_block_array = new (buf) uint32_t[total_macro_block_cnt];
    }
  }

  // alloc block bitmap
  if (OB_SUCC(ret)) {
    const int64_t new_bitmap_cnt = get_macro_bitmap_array_cnt(total_macro_block_cnt);
    buf_len = new_bitmap_cnt * sizeof(uint64_t);
    if (OB_UNLIKELY(NULL == (buf = static_cast<char*>(allocator_.alloc(buf_len))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "Fail to allocate memory, ", K(ret), K(buf_len));
    } else {
      MEMSET(buf, 0, buf_len);
      macro_block_bitmap = new (buf) uint64_t[new_bitmap_cnt];
    }
  }

  // alloc block info array
  if (OB_SUCC(ret)) {
    if (OB_FAIL(macro_block_info_array.reserve(total_macro_block_cnt))) {
      LOG_WARN("fail to reserve total macro block cnt", K(ret), K(total_macro_block_cnt));
    }
  }
#ifdef ERRSIM
  if (OB_SUCC(ret)) {
    ret = E(EventTable::EN_ALLOCATE_RESIZE_MEMORY_FAILED) OB_SUCCESS;
    if (OB_FAIL(ret)) {
      LOG_WARN("error sim, alloc memory failed");
    }
  }
#endif
  if (OB_FAIL(ret)) {
    if (nullptr != free_block_array) {
      allocator_.free(free_block_array);
      free_block_array = nullptr;
    }
    if (nullptr != macro_block_bitmap) {
      allocator_.free(macro_block_bitmap);
      macro_block_bitmap = nullptr;
    }
  }
  return ret;
}

}  // namespace blocksstable
}  // namespace oceanbase
