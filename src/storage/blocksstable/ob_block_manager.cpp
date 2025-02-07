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

#include "storage/blocksstable/ob_block_manager.h"
#include "common/storage/ob_io_device.h"
#include "lib/file/file_directory_utils.h"
#include "lib/utility/ob_tracepoint.h"
#include "lib/worker.h"
#include "observer/ob_server_struct.h"
#include "observer/ob_server_utils.h"
#include "observer/omt/ob_multi_tenant.h"
#include "share/config/ob_server_config.h"
#include "share/ob_force_print_log.h"
#include "share/ob_io_device_helper.h"
#include "share/ob_unit_getter.h"
#include "share/rc/ob_tenant_base.h"
#include "share/resource_manager/ob_resource_manager.h"
#include "storage/blocksstable/ob_macro_block_struct.h"
#include "storage/blocksstable/ob_object_manager.h"
#include "storage/blocksstable/ob_shared_macro_block_manager.h"
#include "storage/blocksstable/ob_sstable_meta.h"
#include "storage/tmp_file/ob_tmp_file_manager.h"
#include "storage/meta_mem/ob_tenant_meta_mem_mgr.h"
#include "storage/meta_store/ob_server_storage_meta_service.h"
#include "storage/meta_store/ob_tenant_storage_meta_service.h"
#include "storage/ob_super_block_struct.h"
#include "storage/tablet/ob_tablet_macro_info_iterator.h"
#include "lib/worker.h"
#include "storage/backup/ob_backup_device_wrapper.h"

using namespace oceanbase::common;
using namespace oceanbase::common::hash;
using namespace oceanbase::blocksstable;
using namespace oceanbase::tmp_file;
using namespace oceanbase::storage;
using namespace oceanbase::share;

namespace oceanbase {
namespace blocksstable {
/**
 * --------------------------------ObSuperBlockPreadChecker------------------------------------
 */
int ObSuperBlockPreadChecker::do_check(void *read_buf,
                                       const int64_t read_size) {
  int ret = OB_SUCCESS;
  int64_t pos = 0;

  HEAP_VAR(ObServerSuperBlock, tmp_super_block) {
    if (OB_FAIL(
            tmp_super_block.deserialize((char *)read_buf, read_size, pos))) {
      LOG_WARN("deserialize super block fail", K(ret), KP(read_buf),
               K(read_size), K(pos));
    } else if (OB_UNLIKELY(!tmp_super_block.is_valid())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("deserialize super block is invalid", K(ret), K(tmp_super_block),
               KP(read_buf), K(read_size), K(pos));
    }

    if (OB_FAIL(ret)) {
      // ignore ret, just report warning because the other super block may be
      // valid
      ret = OB_SUCCESS;
    } else {
      if (!super_block_.is_valid()) {
        super_block_ = tmp_super_block;
        LOG_WARN("get super block", K(ret), K(super_block_));
      } else {
        if (super_block_.body_.modify_timestamp_ <
            tmp_super_block.body_.modify_timestamp_) {
          super_block_ = tmp_super_block;
          LOG_WARN("get super block", K(ret), K(super_block_));
        }
      }
    }
  }

  return ret;
}

/**
 * ------------------------------------ObMacroBlockWriteInfo-------------------------------------
 */

int ObMacroBlockWriteInfo::fill_io_info_for_backup(const blocksstable::MacroBlockId &macro_id, ObIOInfo &io_info) const
{
  int ret = OB_SUCCESS;
  if (!backup::ObBackupDeviceMacroBlockId::is_backup_block_file(macro_id.first_id())) {
    // do nothing
  } else if (!has_backup_device_handle_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("device handle should not be null", K(ret));
  } else {
    backup::ObBackupWrapperIODevice *device = static_cast<backup::ObBackupWrapperIODevice *>(device_handle_);
    io_info.fd_.fd_id_ = device->simulated_fd_id();
    io_info.fd_.slot_version_ = device->simulated_slot_version();
  }
  return ret;
}

/**
 * ------------------------------------ObMacroBlockRewriteSeqGenerator-------------------------------------
 */
ObMacroBlockRewriteSeqGenerator::ObMacroBlockRewriteSeqGenerator()
    : rewrite_seq_(0), lock_(common::ObLatchIds::BLOCK_ID_GENERATOR_LOCK) {}

ObMacroBlockRewriteSeqGenerator::~ObMacroBlockRewriteSeqGenerator() {
  rewrite_seq_ = 0;
}

void ObMacroBlockRewriteSeqGenerator::reset() { rewrite_seq_ = 0; }

int ObMacroBlockRewriteSeqGenerator::generate_next_sequence(uint64_t &blk_seq) {
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(lock_);
  if (OB_UNLIKELY(MacroBlockId::MAX_WRITE_SEQ == rewrite_seq_)) {
    ret = OB_ERROR_OUT_OF_RANGE;
    LOG_ERROR("rewrite sequence number overflow!", K(ret),
              LITERAL_K(MacroBlockId::MAX_WRITE_SEQ), K(rewrite_seq_));
  } else {
    blk_seq = ++rewrite_seq_;
    if (OB_UNLIKELY(BLOCK_SEQUENCE_WARNING_LINE < blk_seq)) {
      const int64_t remaining_rewritten_block_count =
          MacroBlockId::MAX_WRITE_SEQ - blk_seq;
      LOG_ERROR("No rewritten sequence!!! This ObServer needs to migrate data "
                "and offline!!!",
                K(remaining_rewritten_block_count));
    }
  }
  return ret;
}

/**
 * -----------------------------------------ObBlockManager------------------------------------------
 */
ObBlockManager::ObBlockManager()
    : bucket_lock_(), block_map_(), super_block_fd_(), default_block_size_(0),
      marker_status_(), marker_lock_(), is_mark_sweep_enabled_(false),
      sweep_lock_(), mark_block_task_(*this), inspect_bad_block_task_(*this),
      timer_(), bad_block_lock_(), io_device_(NULL), blk_seq_generator_(),
      alloc_num_(0), group_id_(0), is_inited_(false), is_started_(false) {}

ObBlockManager::~ObBlockManager() { destroy(); }

int ObBlockManager::init(ObIODevice *io_device, const int64_t block_size) {
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("already inited", K(ret));
  } else if (OB_ISNULL(io_device) ||
             OB_UNLIKELY(block_size <
                         ObServerSuperBlockHeader::OB_MAX_SUPER_BLOCK_SIZE)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument, ", K(ret), KP(io_device), K(block_size));
  } else if (OB_FAIL(timer_.init("BlkMgr"))) {
    LOG_WARN("fail to init timer", K(ret));
  } else if (OB_FAIL(timer_.set_run_wrapper(MTL_CTX()))) {
    LOG_WARN("fail to set_run_wrapper for timer", K(ret));
  } else if (OB_FAIL(bucket_lock_.init(DEFAULT_LOCK_BUCKET_COUNT,
                                       ObLatchIds::BLOCK_MANAGER_LOCK))) {
    LOG_WARN("fail to init bucket lock", K(ret));
  } else if (OB_FAIL(block_map_.init(SET_USE_UNEXPECTED_500(
                 ObMemAttr(OB_SERVER_TENANT_ID, "BlockMap"))))) {
    LOG_WARN("fail to init block map", K(ret));
  } else {
    io_device_ = io_device;
    super_block_fd_.first_id_ = 0;  // super block default fd
    super_block_fd_.second_id_ = 0; // super block default fd
    default_block_size_ = block_size;
    ATOMIC_STORE(&alloc_num_, 0);
    is_inited_ = true;
  }

  if (IS_NOT_INIT) {
    destroy();
  }
  return ret;
}

int ObBlockManager::start(const int64_t reserved_size, bool &need_format) {
  int ret = OB_SUCCESS;
  need_format = false;
  ObIODOpts opts;
  ObIODOpt opt;
  opts.opt_cnt_ = 1;
  opts.opts_ = &(opt);
  opt.set("reserved size", reserved_size);
  LOG_DBA_INFO_V2(OB_SERVER_BLOCK_MANAGER_START_BEGIN,
                  DBA_STEP_INC_INFO(server_start),
                  "block manager start begin.");

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(io_device_->start(opts))) {
    LOG_WARN("start io device fail", K(ret));
  } else {
    if (!timer_.task_exist(inspect_bad_block_task_)) {
      if (OB_FAIL(timer_.schedule(inspect_bad_block_task_, INSPECT_DELAY_US,
                                  true))) {
        LOG_WARN("Fail to schedule inspect bad block task, ", K(ret));
      }
    }
    if (OB_SUCC(ret) && !timer_.task_exist(mark_block_task_)) {
      if (OB_FAIL(timer_.schedule(mark_block_task_, RECYCLE_DELAY_US, true))) {
        LOG_WARN("Fail to schedule GC task, ", K(ret));
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(timer_.start())) {
      LOG_WARN("Fail to start GC task timer, ", K(ret));
    }
  }
  if (OB_FAIL(ret)) {
    LOG_DBA_ERROR_V2(OB_SERVER_BLOCK_MANAGER_START_FAIL, ret,
                     DBA_STEP_INC_INFO(server_start),
                     "block manager start fail. ",
                     "you may find solutions in previous error logs or seek "
                     "help from official technicians.");
  } else {
    need_format = opt.value_.value_bool;
    is_started_ = true;
    LOG_INFO("start block manager", K(need_format));
    LOG_DBA_INFO_V2(OB_SERVER_BLOCK_MANAGER_START_SUCCESS,
                    DBA_STEP_INC_INFO(server_start),
                    "block manager start success.");
  }

  return ret;
}

void ObBlockManager::stop() { timer_.stop(); }

void ObBlockManager::wait() {
  timer_.wait();
  LOG_INFO("the block manager finish wait");
}

void ObBlockManager::destroy() {
  timer_.destroy();
  inspect_bad_block_task_.reset();
  bucket_lock_.destroy();
  block_map_.destroy();
  {
    lib::ObMutexGuard bad_block_guard(bad_block_lock_);
    bad_block_infos_.destroy();
  }
  io_device_ = NULL;
  super_block_fd_.reset();
  default_block_size_ = 0;
  is_mark_sweep_enabled_ = false;
  marker_status_.reset();
  blk_seq_generator_.reset();
  ATOMIC_STORE(&alloc_num_, 0);
  group_id_ = 0;
  is_inited_ = false;
}
int ObBlockManager::alloc_object(ObStorageObjectHandle &object_handle) {
  int ret = OB_SUCCESS;
  MacroBlockId macro_id;
  ObIOFd io_fd;
  ObIODOpts opts;
  uint64_t write_seq = 0;
  ObIODOpt opt_array[1];

  if (IS_NOT_INIT || !is_started()) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObBlockManager not init", K(ret));
  } else if (OB_FAIL(io_device_->alloc_block(&opts, io_fd))) {
    if (ret != OB_SERVER_OUTOF_DISK_SPACE) {
      LOG_WARN("Failed to alloc block from io device", K(ret));
    }
  }
  // try alloc block
  if (ret == OB_SERVER_OUTOF_DISK_SPACE) {
    if (OB_FAIL(extend_file_size_if_need())) { // block to get disk
      ret = OB_SERVER_OUTOF_DISK_SPACE;        // reuse last ret code
      LOG_ERROR("The data file disk space is exhausted. Please expand the capacity by resizing datafile!!!", K(ret));
    } else if (OB_FAIL(io_device_->alloc_block(&opts, io_fd))) {
      if (OB_SERVER_OUTOF_DISK_SPACE == ret) {
        LOG_ERROR("The data file disk space is exhausted. Please expand the capacity by resizing datafile!!!", K(ret));
      } else {
        LOG_ERROR("Failed to alloc block from io device", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(blk_seq_generator_.generate_next_sequence(write_seq))) {
      LOG_WARN("Failed to generate next block id", K(ret), K(write_seq),
               K_(blk_seq_generator));
    } else {
      macro_id.reset();
      macro_id.set_write_seq(write_seq);
      macro_id.set_block_index(io_fd.second_id_);
      if (OB_FAIL(object_handle.set_macro_block_id(macro_id))) {
        LOG_ERROR("Failed to set macro block id", K(ret), K(macro_id));
      } else {
        ATOMIC_AAF(&alloc_num_, 1);
        FLOG_INFO("successfully alloc block", K(macro_id));
      }
    }
  }
  return ret;
}

int ObBlockManager::alloc_block(ObMacroBlockHandle &macro_handle) {
  int ret = OB_SUCCESS;
  ObIOFd io_fd;
  ObIODOpts opts;
  uint64_t write_seq = 0;
  ObIODOpt opt_array[1];

  if (IS_NOT_INIT || !is_started()) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObBlockManager not init", K(ret));
  } else if (OB_FAIL(io_device_->alloc_block(&opts, io_fd))) {
    if (ret != OB_SERVER_OUTOF_DISK_SPACE) {
      LOG_WARN("Failed to alloc block from io device", K(ret));
    }
  }
  // try alloc block
  if (ret == OB_SERVER_OUTOF_DISK_SPACE) {
    if (OB_FAIL(extend_file_size_if_need())) { // block to get disk
      ret = OB_SERVER_OUTOF_DISK_SPACE;        // reuse last ret code
      LOG_ERROR("The data file disk space is exhausted. Please expand the capacity by resizing datafile!!!", K(ret));
    } else if (OB_FAIL(io_device_->alloc_block(&opts, io_fd))) {
      if (OB_SERVER_OUTOF_DISK_SPACE == ret) {
        LOG_ERROR("The data file disk space is exhausted. Please expand the capacity by resizing datafile!!!", K(ret));
      } else {
        LOG_ERROR("Failed to alloc block from io device", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(blk_seq_generator_.generate_next_sequence(write_seq))) {
      LOG_WARN("Failed to generate next block id", K(ret), K(write_seq),
               K_(blk_seq_generator));
    } else {
      MacroBlockId macro_id(write_seq, io_fd.second_id_, 0);
      if (OB_FAIL(macro_handle.set_macro_block_id(macro_id))) {
        LOG_ERROR("Failed to set macro block id", K(ret), K(macro_id));
      } else {
        ATOMIC_AAF(&alloc_num_, 1);
        FLOG_INFO("successfully alloc block", K(macro_id));
      }
    }
  }

  return ret;
}

int ObBlockManager::async_read_block(const ObMacroBlockReadInfo &read_info,
                                     ObMacroBlockHandle &macro_handle) {
  return macro_handle.async_read(read_info);
}

int ObBlockManager::async_write_block(const ObMacroBlockWriteInfo &write_info,
                                      ObMacroBlockHandle &macro_handle) {
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!write_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret), K(write_info));
  } else if (OB_FAIL(OB_SERVER_BLOCK_MGR.alloc_block(macro_handle))) {
    LOG_WARN("fail to alloc block from block manager", K(ret));
  } else if (OB_FAIL(macro_handle.async_write(write_info))) {
    LOG_WARN("Fail to async write block", K(ret), K(macro_handle));
  }
  return ret;
}

int ObBlockManager::read_block(const ObMacroBlockReadInfo &read_info,
                               ObMacroBlockHandle &macro_handle) {
  int ret = OB_SUCCESS;
  if (OB_FAIL(async_read_block(read_info, macro_handle))) {
    LOG_WARN("Fail to sync read block", K(ret), K(read_info));
  } else if (OB_FAIL(macro_handle.wait())) {
    LOG_WARN("Fail to wait io finish", K(ret), K(read_info));
  }
  return ret;
}

int ObBlockManager::write_block(const ObMacroBlockWriteInfo &write_info,
                                ObMacroBlockHandle &macro_handle) {
  int ret = OB_SUCCESS;
  if (OB_FAIL(async_write_block(write_info, macro_handle))) {
    LOG_WARN("Fail to sync write block", K(ret), K(write_info),
             K(macro_handle));
  } else if (OB_FAIL(macro_handle.wait())) {
    LOG_WARN("Fail to wait io finish", K(ret), K(write_info));
  }
  return ret;
}

int ObBlockManager::read_super_block(ObServerSuperBlock &super_block,
                                     ObSuperBlockBufferHolder &buf_holder) {
  int ret = OB_SUCCESS;
  int64_t read_size = 0;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    HEAP_VAR(ObSuperBlockPreadChecker, checker) {
      if (OB_FAIL(io_device_->pread(
              super_block_fd_, SUPER_BLOCK_OFFSET, buf_holder.get_len(),
              buf_holder.get_buffer(), read_size, &checker))) {
        LOG_WARN("fail to write super block", K(ret), K_(super_block_fd),
                 K(buf_holder), K(read_size));
      } else if (OB_UNLIKELY(buf_holder.get_len() != read_size)) {
        ret = OB_IO_ERROR;
        LOG_WARN("read size not equal super block size", K(ret), K(buf_holder),
                 K(read_size));
      } else {
        super_block = checker.get_super_block();
        if (OB_UNLIKELY(!super_block.is_valid())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected error, invalid super block", K(ret),
                   K(super_block));
        } else {
          LOG_INFO("finish read_super_block", K(ret), K(super_block_fd_),
                   K(super_block));
        }
      }
    }
  }
  return ret;
}

int ObBlockManager::write_super_block(const ObServerSuperBlock &super_block,
                                      ObSuperBlockBufferHolder &buf_holder) {
  int ret = OB_SUCCESS;
  int64_t write_size = 0;
#ifdef ERRSIM
  ErrsimModuleGuard guard(ObErrsimModuleType::ERRSIM_MODULE_NONE);
#endif

  if (!super_block.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(super_block));
  } else if (OB_FAIL(buf_holder.serialize_super_block(super_block))) {
    LOG_ERROR("failed to serialize super block", K(ret), K(buf_holder),
              K(super_block));
  } else if (OB_FAIL(io_device_->pwrite(super_block_fd_, SUPER_BLOCK_OFFSET,
                                        buf_holder.get_len(),
                                        buf_holder.get_buffer(), write_size))) {
    LOG_WARN("fail to write super block", K(ret), K_(super_block_fd),
             K(buf_holder), K(write_size));
  } else if (OB_UNLIKELY(buf_holder.get_len() != write_size)) {
    ret = OB_IO_ERROR;
    LOG_WARN("write size not equal super block size", K(ret), K(buf_holder),
             K(write_size));
  } else {
    LOG_INFO("succeed to write super block", K(ret), K(super_block));
  }
  return ret;
}

int ObBlockManager::first_mark_device() {
  int ret = OB_SUCCESS;
  BlockMapIterator iter(block_map_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObBlockManager not init", K(ret));
  } else if (OB_FAIL(io_device_->mark_blocks(iter))) {
    LOG_WARN("fail to first mark blocks before running", K(ret));
  } else {
    blk_seq_generator_.update_sequence(iter.get_max_write_sequence());
    enable_mark_sweep();
  }
  return ret;
}

int64_t ObBlockManager::get_max_macro_block_count(int64_t reserved_size) const {
  return io_device_->get_max_block_count(reserved_size);
}

int64_t ObBlockManager::get_free_macro_block_count() const {
  return io_device_->get_free_block_count();
}

int64_t ObBlockManager::get_used_macro_block_count() const {
  return block_map_.count();
}

int64_t ObBlockManager::get_total_block_size() const {
  return io_device_->get_total_block_size();
}

int ObBlockManager::get_macro_block_info(
    const MacroBlockId &macro_id, ObMacroBlockInfo &macro_block_info,
    ObMacroBlockHandle &macro_block_handle) {
  int ret = OB_SUCCESS;
  BlockInfo block_info;
  bool has_inc_ref = false;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!macro_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument, ", K(ret), K(macro_id));
  } else {
    ObBucketHashWLockGuard lock_guard(bucket_lock_, macro_id.hash());
    if (OB_FAIL(block_map_.get(macro_id, block_info)) &&
        ret != OB_HASH_NOT_EXIST) {
      // BUG, should not happen
      LOG_ERROR("fatal error, this block should be in block map", K(ret),
                K(macro_id));
    } else if (OB_UNLIKELY(OB_SUCCESS == ret && block_info.ref_cnt_ < 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("fatal error, invalid refcnt", K(ret), K(macro_id),
                K(block_info));
    } else if (OB_UNLIKELY(OB_HASH_NOT_EXIST == ret ||
                           0 == block_info.ref_cnt_)) {
      // set `is_free_` to true, skip this MacroBlock in upper layer.
      ret = OB_SUCCESS;
      macro_block_info.is_free_ = true;
    } else {
      macro_block_info.is_free_ = false;
      macro_block_info.ref_cnt_ = block_info.ref_cnt_;
      macro_block_info.access_time_ = block_info.last_write_time_;
      block_info.access_time_ = ObTimeUtility::fast_current_time();
      block_info.ref_cnt_++;
      if (OB_FAIL(block_map_.insert_or_update(macro_id, block_info))) {
        LOG_ERROR("update block info fail", K(ret), K(macro_id), K(block_info));
      } else {
        has_inc_ref = true;
        LOG_DEBUG("debug ref_cnt: inc_ref in memory", K(ret), K(macro_id),
                  K(block_info), K(lbt()));
      }
    }
  }
  if (OB_SUCC(ret) && !macro_block_info.is_free_) {
    if (OB_FAIL(macro_block_handle.set_macro_block_id(macro_id))) {
      LOG_ERROR("fatal error, fail to set macro block id", K(ret), K(macro_id),
                K(macro_block_info));
    }
  }
  if (has_inc_ref) {
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(dec_ref(macro_id))) {
      LOG_ERROR("fail to decrease reference count", K(ret), K(macro_id));
    }
  }

  return ret;
}

int ObBlockManager::get_all_macro_ids(ObArray<MacroBlockId> &ids_array) {
  int ret = OB_SUCCESS;
  ids_array.reset();
  ObBlockManager::GetAllMacroBlockIdFunctor getter(ids_array);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(ids_array.reserve(block_map_.count()))) {
    LOG_WARN("fail to reserver macro id array", K(ret), "block count",
             block_map_.count());
  } else if (OB_FAIL(block_map_.for_each(getter))) {
    LOG_WARN("fail to for each block map", K(ret));
  }
  return ret;
}

int ObBlockManager::check_macro_block_free(const MacroBlockId &macro_id,
                                           bool &is_free) const {
  int ret = OB_SUCCESS;
  is_free = false;
  BlockInfo block_info;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!macro_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument, ", K(ret), K(macro_id));
  } else if (OB_FAIL(block_map_.get(macro_id, block_info))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("fail to get macro id, ", K(ret), K(macro_id));
    } else {
      is_free = true;
      ret = OB_SUCCESS;
    }
  } else {
    is_free = 0 == block_info.ref_cnt_;
  }
  return ret;
}

int ObBlockManager::get_bad_block_infos(
    common::ObIArray<ObBadBlockInfo> &bad_block_infos) {
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("The block manager has not been opened, ", K(ret));
  } else {
    lib::ObMutexGuard bad_block_guard(bad_block_lock_);
    if (OB_FAIL(bad_block_infos.assign(bad_block_infos_))) {
      LOG_WARN("fail to assign bad block infos, ", K(ret), K(bad_block_infos_));
    }
  }
  return ret;
}

int ObBlockManager::report_bad_block(const MacroBlockId &macro_block_id,
                                     const int64_t error_type,
                                     const char *error_msg,
                                     const char *file_path) {
  int ret = OB_SUCCESS;
  const int64_t MAX_BAD_BLOCK_NUMBER =
      std::max(10L, OB_STORAGE_OBJECT_MGR.get_total_macro_block_count() / 100);
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("The block manager has not been inited", K(ret));
  } else if (OB_UNLIKELY(!macro_block_id.is_valid() ||
                         OB_TIMEOUT == error_type || NULL == error_msg)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument, ", K(ret), K(macro_block_id), K(error_type),
             KP(error_msg));
  } else if (is_bad_block(macro_block_id)) {
    ret = OB_SUCCESS; // No need to print warn log
    LOG_INFO("already found this bad block, ", K(macro_block_id), K(error_type),
             K(error_msg));
  } else {
    ObBadBlockInfo bad_block_info;
    lib::ObMutexGuard bad_block_guard(bad_block_lock_);
    if (bad_block_infos_.count() >= MAX_BAD_BLOCK_NUMBER) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("too many bad blocks! ", K(ret), "count",
               bad_block_infos_.count(), K(MAX_BAD_BLOCK_NUMBER),
               K(macro_block_id), K(error_type), K(error_msg));
    } else if (OB_FAIL(databuff_printf(bad_block_info.error_msg_,
                                       sizeof(bad_block_info.error_msg_), "%s",
                                       error_msg))) {
      LOG_WARN("Error msg is too long, ", K(ret), K(error_msg),
               K(sizeof(bad_block_info.error_msg_)));
    } else {
      STRNCPY(bad_block_info.store_file_path_, file_path,
              sizeof(bad_block_info.store_file_path_) - 1);
      bad_block_info.disk_id_ = macro_block_id.first_id();
      bad_block_info.macro_block_id_ = macro_block_id;
      bad_block_info.error_type_ = error_type;
      bad_block_info.check_time_ = ObTimeUtility::current_time();
      if (OB_FAIL(bad_block_infos_.push_back(bad_block_info))) {
        LOG_WARN("fail to save bad block info, ", K(ret), K(bad_block_info),
                 K(bad_block_infos_));
      } else {
        LOG_ERROR("add bad block info", K(bad_block_info));
      }
    }
  }
  return ret;
}

int ObBlockManager::resize_file(const int64_t new_data_file_size,
                                const int64_t new_data_file_disk_percentage,
                                const int64_t reserved_size,
                                ObServerSuperBlock &super_block) {
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(reserved_size < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(reserved_size));
  } else if (!is_mark_sweep_enabled()) {
    LOG_INFO("mark and sweep is disabled, do not resize file at present");
  } else {
    SpinWLockGuard sweep_guard(sweep_lock_);
    const int64_t old_macro_block_cnt =
        get_total_block_size() / OB_STORAGE_OBJECT_MGR.get_macro_block_size();
    ObIODOpts io_d_opts;
    ObIODOpt opts[3];
    opts[0].set("datafile_size", new_data_file_size);
    opts[1].set("datafile_disk_percentage", new_data_file_disk_percentage);
    opts[2].set("reserved_size", reserved_size);
    io_d_opts.opts_ = opts;
    io_d_opts.opt_cnt_ = 3;
    if (OB_FAIL(io_device_->reconfig(io_d_opts))) {
      LOG_WARN("fail to resize file", K(ret), K(new_data_file_size));
    } else {
      const int64_t new_actual_file_size = get_total_block_size();
      const int64_t new_macro_block_cnt =
          new_actual_file_size / OB_STORAGE_OBJECT_MGR.get_macro_block_size();

      if (old_macro_block_cnt < new_macro_block_cnt) {
        super_block.body_.total_file_size_ = new_actual_file_size;
        super_block.body_.total_macro_block_count_ = new_macro_block_cnt;
        super_block.body_.modify_timestamp_ = ObTimeUtility::current_time();
      }
      // super block may have format upgrade. Whatever body changed, reconstruct
      // header for safe
      if (FAILEDx(super_block.construct_header())) {
        LOG_WARN("fail to construct header", K(ret));
      }
    }
  }
  return ret;
}

int ObBlockManager::inc_ref(const MacroBlockId &macro_id) {
  int ret = OB_SUCCESS;
  BlockInfo block_info;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("not init", K(ret));
  } else if (OB_UNLIKELY(!macro_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("Invalid argument, ", K(ret), K(macro_id));
  } else if (macro_id.is_local_id()) {
    ObBucketHashWLockGuard lock_guard(bucket_lock_, macro_id.hash());
    if (OB_FAIL(block_map_.get(macro_id, block_info))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        block_info.reset();
        ret = OB_SUCCESS;
      } else {
        LOG_ERROR("get block_info fail", K(ret), K(macro_id));
      }
    } else if (OB_UNLIKELY(
                   block_info.ref_cnt_ < 0 ||
                   (0 == block_info.ref_cnt_ && is_mark_sweep_enabled()))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("un-expected MacroBlock refcnt", K(ret), K(macro_id),
                K(block_info));
    }

    if (OB_SUCC(ret)) {
      block_info.access_time_ = ObTimeUtility::fast_current_time();
      block_info.ref_cnt_++;
      if (OB_FAIL(block_map_.insert_or_update(macro_id, block_info))) {
        LOG_ERROR("update block info fail", K(ret), K(macro_id), K(block_info));
      } else {
        LOG_DEBUG("debug ref_cnt: inc_ref in memory", K(ret), K(macro_id),
                  K(block_info), K(lbt()));
      }
    }
  }
  return ret;
}

int ObBlockManager::dec_ref(const MacroBlockId &macro_id) {
  int ret = OB_SUCCESS;
  BlockInfo block_info;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("not init", K(ret));
  } else if (OB_UNLIKELY(!macro_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("Invalid argument, ", K(ret), K(macro_id));
  } else if (macro_id.is_local_id()) {
    ObBucketHashWLockGuard lock_guard(bucket_lock_, macro_id.hash());
    if (OB_FAIL(block_map_.get(macro_id, block_info))) {
      LOG_ERROR("get block_info fail", K(ret), K(macro_id));
    } else if (OB_UNLIKELY(0 == block_info.ref_cnt_)) {
      // BUG, should not happen
      ret = OB_ERR_SYS;
      LOG_ERROR("fatal error, ref cnt must not less than 0", K(ret),
                K(macro_id), K(block_info));
    } else {
      block_info.access_time_ = ObTimeUtility::fast_current_time();
      block_info.ref_cnt_--;
      if (OB_FAIL(block_map_.insert_or_update(macro_id, block_info))) {
        LOG_ERROR("update block info fail", K(ret), K(macro_id), K(block_info));
      } else {
        LOG_DEBUG("debug ref_cnt: dec_ref in memory", K(ret), K(macro_id),
                  K(block_info), K(lbt()));
      }
    }
  }
  return ret;
}

int ObBlockManager::update_write_time(const MacroBlockId &macro_id,
                                      const bool update_to_max_time) {
  int ret = OB_SUCCESS;
  BlockInfo block_info;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("not init", K(ret));
  } else if (OB_UNLIKELY(!macro_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", K(ret), K(macro_id));
  } else {
    ObBucketHashWLockGuard lock_guard(bucket_lock_, macro_id.hash());
    if (OB_FAIL(block_map_.get(macro_id, block_info))) {
      LOG_WARN("get block_info fail", K(ret), K(macro_id));
    } else {
      block_info.last_write_time_ =
          update_to_max_time ? INT64_MAX : ObTimeUtility::fast_current_time();
      if (OB_FAIL(block_map_.insert_or_update(macro_id, block_info))) {
        LOG_WARN("update block info fail", K(ret), K(macro_id), K(block_info));
      }
    }
  }
  return ret;
}

int ObBlockManager::get_marker_status(ObMacroBlockMarkerStatus &status) {
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObBlockManager not init", K(ret));
  } else {
    SpinRLockGuard guard(marker_lock_);
    status = marker_status_;
  }
  return ret;
}

void ObBlockManager::update_marker_status(const ObMacroBlockMarkerStatus &tmp_status)
{
  SpinWLockGuard guard(marker_lock_);
  ObMacroBlockMarkerStatus prev_result = marker_status_;
  marker_status_.reset();
  marker_status_.total_block_count_ = OB_STORAGE_OBJECT_MGR.get_total_macro_block_count();
  marker_status_.reserved_block_count_ = io_device_->get_reserved_block_count() + tmp_status.reserved_block_count_;
  marker_status_.free_count_ = get_free_macro_block_count();
  marker_status_.hold_count_ = tmp_status.hold_count_;
  marker_status_.mark_cost_time_ = tmp_status.mark_cost_time_;
  marker_status_.sweep_cost_time_ = tmp_status.sweep_cost_time_;
  marker_status_.start_time_ = tmp_status.start_time_;
  marker_status_.mark_finished_ = tmp_status.mark_finished_;
  if (tmp_status.mark_finished_) {
    // Mark succeed, update marker status with new result.
    marker_status_.last_end_time_ = tmp_status.last_end_time_;
    marker_status_.linked_block_count_ = tmp_status.linked_block_count_;
    marker_status_.index_block_count_ = tmp_status.index_block_count_;
    marker_status_.ids_block_count_ = tmp_status.ids_block_count_;
    marker_status_.tmp_file_count_ = tmp_status.tmp_file_count_;
    marker_status_.data_block_count_ = tmp_status.data_block_count_;
    marker_status_.shared_data_block_count_ = tmp_status.shared_data_block_count_;
    marker_status_.pending_free_count_ = tmp_status.pending_free_count_;
    marker_status_.shared_meta_block_count_ = tmp_status.shared_meta_block_count_;
    marker_status_.hold_info_ = tmp_status.hold_info_;
  } else {
    // Mark skipped, update marker status with previous result.
    marker_status_.last_end_time_ = prev_result.last_end_time_;
    marker_status_.linked_block_count_ = prev_result.linked_block_count_;
    marker_status_.index_block_count_ = prev_result.index_block_count_;
    marker_status_.ids_block_count_ = prev_result.ids_block_count_;
    marker_status_.tmp_file_count_ = prev_result.tmp_file_count_;
    marker_status_.data_block_count_ = prev_result.data_block_count_;
    marker_status_.shared_data_block_count_ = prev_result.shared_data_block_count_;
    marker_status_.pending_free_count_ = prev_result.pending_free_count_;
    marker_status_.shared_meta_block_count_ = prev_result.shared_meta_block_count_;
    marker_status_.hold_info_ = prev_result.hold_info_;
  }
}

bool ObBlockManager::GetOldestHoldBlockFunctor::operator()(
    const MacroBlockId &key, const BlockInfo &value) {
  int ret = OB_SUCCESS;
  if (OB_FAIL(macro_id_set_.exist_refactored(key))) {
    if (OB_HASH_EXIST == ret) {
      ret = OB_SUCCESS;
    } else if (OB_HASH_NOT_EXIST == ret) {
      // TODO yunshan.tys : add new solutions to find leaked macro blocks
      if (0 != value.ref_cnt_ // not wash tablet block
          && (!oldest_hold_block_info_.macro_id_.is_valid() ||
              value.access_time_ < oldest_hold_block_info_.last_access_time_)) {
        oldest_hold_block_info_.macro_id_ = key;
        oldest_hold_block_info_.last_access_time_ = value.access_time_;
        oldest_hold_block_info_.ref_cnt_ = value.ref_cnt_;
      }
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to check exist for macro id", K(ret), K(key));
    }
  }
  ret_code_ = ret;
  return OB_SUCCESS == ret;
}

bool ObBlockManager::GetPendingFreeBlockFunctor::operator()(
    const MacroBlockId &key, const BlockInfo &value) {
  int ret = OB_SUCCESS;
  if (value.ref_cnt_ > 0) {
    hold_count_++;
  } else if (OB_UNLIKELY(value.ref_cnt_ < 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("fatal error, macro block ref cnt less than 0", K(ret), K(key),
              K(value));
  } else if (OB_UNLIKELY(blk_map_.count() >= max_free_blk_cnt_)) {
    // skip inserting more free block
  } else if (OB_FAIL(blk_map_.insert(key, true))) {
    LOG_WARN("push back block id fail", K(ret), K(key));
  }
  ret_code_ = ret;
  return OB_SUCCESS == ret;
}

bool ObBlockManager::GetAllMacroBlockIdFunctor::operator()(
    const MacroBlockId &key, const BlockInfo &value) {
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(value.ref_cnt_ < 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("fatal error, macro block ref cnt less than 0", K(ret), K(key),
              K(value));
  } else if (OB_FAIL(block_ids_.push_back(key))) {
    LOG_WARN("fail to push back macro block id", K(ret), K(key));
  }
  ret_code_ = ret;
  return OB_SUCCESS == ret;
}

bool ObBlockManager::DoBlockSweepFunctor::operator()(
    const MacroBlockId &macro_id, const bool can_free) {
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!can_free)) {
    // ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error, this block cannot be freed", K(macro_id),
             K(can_free));
  } else if (OB_FAIL(block_manager_.sweep_one_block(macro_id))) {
    LOG_WARN("fail to sweep one block", K(ret), K(macro_id));
  }
  // record last failure ret
  ret_code_ = OB_SUCCESS == ret ? ret_code_ : ret;
  // ignore ret to sweep all blocks
  return true;
}

bool ObBlockManager::is_bad_block(const MacroBlockId &macro_block_id) {
  bool is_exist = false;
  lib::ObMutexGuard bad_block_guard(bad_block_lock_);
  for (int64_t i = 0; i < bad_block_infos_.count(); ++i) {
    if (bad_block_infos_[i].macro_block_id_ == macro_block_id) {
      is_exist = true;
      break;
    }
  }
  return is_exist;
}

int ObBlockManager::do_sweep(MacroBlkIdMap &mark_info) {
  int ret = OB_SUCCESS;
  DoBlockSweepFunctor functor(*this);
  if (0 == mark_info.count()) {
    // do nothing
  } else if (OB_FAIL(mark_info.for_each(functor))) {
    ret = functor.get_ret_code();
    LOG_WARN("fail to do block sweep", K(ret));
  }
  return ret;
}

int ObBlockManager::sweep_one_block(const MacroBlockId &macro_id) {
  int ret = OB_SUCCESS;
  ObBucketHashWLockGuard lock_guard(bucket_lock_, macro_id.hash());
  BlockInfo block_info;
  ObIOFd io_fd;
  io_fd.first_id_ = macro_id.first_id();
  io_fd.second_id_ = macro_id.second_id();
  if (OB_FAIL(block_map_.get(macro_id, block_info))) {
    LOG_WARN("fail to get block info from block map", K(ret), K(macro_id));
  } else if (OB_UNLIKELY(block_info.ref_cnt_ > 0)) {
    // skip using block.
  } else if (OB_FAIL(block_map_.erase(macro_id))) {
    LOG_WARN("fail to erase block info from block map", K(ret), K(macro_id));
  } else {
    io_device_->free_block(io_fd);
    FLOG_INFO("block manager free block", K(macro_id), K(io_fd));
  }
  return ret;
}

void ObBlockManager::mark_and_sweep()
{
  int ret = OB_SUCCESS;
  ObHashSet<MacroBlockId, NoPthreadDefendMode> macro_id_set;
  MacroBlkIdMap mark_info;
  ObMacroBlockMarkerStatus tmp_status;
  // we must assign alloc_num_ before mark_macro_blocks, because it will be set to 0 in this func
  int64_t alloc_num = 0;
  // recycle maximum 400 GB space, but no more than 8MB memory consumption for mark_info
  const int64_t MAX_FREE_BLOCK_COUNT_PER_ROUND = 200000;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("block manager not init", K(ret));
  } else if (!is_mark_sweep_enabled()) {
    LOG_INFO("mark and sweep is disabled, do not mark and sweep this round");
  } else if (!SERVER_STORAGE_META_SERVICE.is_started()) {
    if (REACH_TIME_INTERVAL(10 * 1000 * 1000 /* 10s */)) {
      LOG_WARN("slog replay hasn't finished, this task can't start", K(ret));
    }
  } else {
    if (OB_FAIL(mark_info.init(ObModIds::OB_STORAGE_FILE_BLOCK_REF, OB_SERVER_TENANT_ID))) {
      LOG_WARN("fail to init mark info, ", K(ret));
    } else if (OB_FAIL(macro_id_set.create(MAX(2, MIN(MAX_FREE_BLOCK_COUNT_PER_ROUND, block_map_.get_bkt_cnt())),
                                           "BlkIdSetBkt",
                                           "BlkIdSetNode",
                                           OB_SERVER_TENANT_ID))) {
      LOG_WARN("fail to create macro id set", K(ret));
    } else {
      GetPendingFreeBlockFunctor pending_free_functor(
          MAX_FREE_BLOCK_COUNT_PER_ROUND, mark_info, tmp_status.hold_count_);
      tmp_status.start_time_ = ObTimeUtility::fast_current_time();
      if (OB_FAIL(block_map_.for_each(pending_free_functor))) {
        ret = pending_free_functor.get_ret_code();
        LOG_WARN("fail to get pending free blocks", K(ret));
      } else if ((mark_info.count() < MAX_FREE_BLOCK_COUNT_PER_ROUND)) {
        // Only try to set alloc_num_ to 0 when macro info is complete, else do mark and sweep again.
        if (0 != (alloc_num = ATOMIC_SET(&alloc_num_, 0))) {
          // Some one alloc block after GetPendingFreeBlockFunctor concurrently. let mark and sweep do again next round
          // whatever mark_info is empty or not
          ATOMIC_SET(&alloc_num_, alloc_num);
        }
      }

      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(mark_macro_blocks(mark_info, macro_id_set, tmp_status))) {
        if (OB_ALLOCATE_MEMORY_FAILED == ret) {
          LOG_INFO("mark blocks meet memory issue, still countinue sweep to lease compaction space");
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("fail to mark macro blocks", K(ret));
        }
      }

      if (OB_FAIL(ret)) {
        ATOMIC_FAA(&alloc_num_, alloc_num); // add alloc_num back to trigger next round mark
      } else {
        tmp_status.pending_free_count_ += mark_info.count();
        tmp_status.mark_cost_time_ = ObTimeUtility::fast_current_time() - tmp_status.start_time_;
        // sweep
        SpinWLockGuard guard(sweep_lock_);
        if (OB_FAIL(do_sweep(mark_info))) {
          LOG_WARN("do sweep fail", K(ret));
        } else if (tmp_status.mark_finished_) {
          tmp_status.last_end_time_ = ObTimeUtility::fast_current_time();
          tmp_status.sweep_cost_time_ = tmp_status.last_end_time_ - tmp_status.start_time_ - tmp_status.mark_cost_time_;

          GetOldestHoldBlockFunctor hold_info_functor(macro_id_set, tmp_status.hold_info_);
          if (OB_FAIL(block_map_.for_each(hold_info_functor))) {
            ret = hold_info_functor.get_ret_code();
            LOG_WARN("fail to get oldest hold block", K(ret));
          }
        }
      }
      if (OB_SUCC(ret)) {
        update_marker_status(tmp_status);
      }
      FLOG_INFO("finish once mark and sweep", K(ret), K(alloc_num),
                K(mark_info.count()), K_(marker_status), "map_cnt",
                block_map_.count());
    }
  }
  macro_id_set.destroy();
}

int ObBlockManager::mark_macro_blocks(
    MacroBlkIdMap &mark_info,
    common::hash::ObHashSet<MacroBlockId, common::hash::NoPthreadDefendMode> &macro_id_set,
    ObMacroBlockMarkerStatus &tmp_status)
{
  int ret = OB_SUCCESS;
  omt::ObMultiTenant *omt = GCTX.omt_;
  common::ObSEArray<uint64_t, 8> mtl_tenant_ids;

  if (OB_ISNULL(omt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error, omt is nullptr", K(ret), KP(omt));
  } else if (0 == mark_info.count()) {
    tmp_status.mark_finished_ = false;
    LOG_INFO("no block alloc/free, no need to mark blocks", K(ret), K(mark_info.count()));
  } else if (OB_FAIL(mark_tmp_file_blocks(mark_info, macro_id_set, tmp_status))) {
    LOG_WARN("fail to mark tmp file blocks", K(ret));
  } else if (OB_FAIL(mark_server_meta_blocks(mark_info, macro_id_set, tmp_status))) {
    LOG_WARN("fail to mark server meta blocks", K(ret));
  } else {
    omt->get_mtl_tenant_ids(mtl_tenant_ids);
    for (int64_t i = 0; OB_SUCC(ret) && i < mtl_tenant_ids.count(); i++) {
      const uint64_t tenant_id = mtl_tenant_ids.at(i);
      MacroBlockId macro_id;
      MTL_SWITCH(tenant_id)
      {
        CONSUMER_GROUP_FUNC_GUARD(ObFunctionType::PRIO_GC_MACRO_BLOCK);
        if (OB_FAIL(mark_tenant_blocks(mark_info, macro_id_set, tmp_status))) {
          LOG_WARN("fail to mark tenant blocks", K(ret), K(tenant_id));
        } else if (OB_FALSE_IT(MTL(ObSharedMacroBlockMgr *)->get_cur_shared_block(macro_id))) {
        } else if (OB_FAIL(mark_held_shared_block(macro_id, mark_info, macro_id_set, tmp_status))) {
          LOG_WARN("fail to mark shared block held by shared_macro_block_manager", K(ret), K(macro_id));
        } else if (OB_FALSE_IT(MTL(ObTenantStorageMetaService *)
                                   ->get_shared_object_reader_writer()
                                   .get_cur_shared_block(macro_id))) {
        } else if (OB_FAIL(mark_held_shared_block(macro_id, mark_info, macro_id_set, tmp_status))) {
          LOG_WARN("fail to mark shared block held by shared_reader_writer", K(ret), K(macro_id));
        }
      }
    }
    if (OB_SUCC(ret)) {
      tmp_status.mark_finished_ = true;
    }
  }
  return ret;
}

int ObBlockManager::mark_held_shared_block(
    const MacroBlockId &macro_id, MacroBlkIdMap &mark_info,
    common::hash::ObHashSet<MacroBlockId, common::hash::NoPthreadDefendMode>
        &macro_id_set,
    ObMacroBlockMarkerStatus &tmp_status) {
  int ret = OB_SUCCESS;

  if (!macro_id.is_valid()) {
    // no small sstable, skip the mark
  } else if (OB_FAIL(update_mark_info(macro_id, mark_info))) {
    LOG_WARN("fail to update mark info", K(ret), K(macro_id));
  } else if (OB_FAIL(
                 macro_id_set.set_refactored(macro_id, 0 /*no override*/))) {
    if (OB_HASH_EXIST != ret) {
      LOG_WARN("fail to put macro id into set", K(ret), K(macro_id));
    } else {
      ret = OB_SUCCESS;
    }
  } else {
    tmp_status.hold_count_--;
    tmp_status.reserved_block_count_++;
  }
  return ret;
}

int ObBlockManager::mark_tenant_blocks(
    MacroBlkIdMap &mark_info,
    common::hash::ObHashSet<MacroBlockId, common::hash::NoPthreadDefendMode>
        &macro_id_set,
    ObMacroBlockMarkerStatus &tmp_status) {
  int ret = OB_SUCCESS;
  ObTenantStorageMetaService *meta_service = MTL(ObTenantStorageMetaService *);
  ObTenantMetaMemMgr *t3m = MTL(ObTenantMetaMemMgr *);
  if (OB_ISNULL(t3m) || OB_ISNULL(meta_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error, t3m or meta_service of mtl is nullptr", K(ret),
             KP(t3m), KP(meta_service));
  } else if (OB_FAIL(mark_tenant_ckpt_blocks(mark_info, macro_id_set,
                                             *meta_service, tmp_status))) {
    LOG_WARN("fail to mark tenant meta blocks", K(ret));
  } else {
    ObArenaAllocator iter_allocator("MarkIter", OB_MALLOC_NORMAL_BLOCK_SIZE,
                                    MTL_ID());
    ObTenantTabletIterator tablet_iter(*t3m, iter_allocator, nullptr /*no op*/);
    ObTabletHandle handle;
    while (OB_SUCC(ret)) {
      handle.reset();
      iter_allocator.reuse();
      if (OB_FAIL(tablet_iter.get_next_tablet(handle))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          break;
        } else {
          LOG_WARN("fail to get next in-memory tablet", K(ret));
        }
      } else if (handle.get_obj()->get_version() <
                 ObTabletBlockHeader::TABLET_VERSION_V3) {
        if (OB_FAIL(mark_tablet_meta_blocks(mark_info, handle, macro_id_set,
                                            tmp_status))) {
          LOG_WARN("fail to mark tablet meta blocks", K(ret));
        } else if (OB_FAIL(mark_sstable_blocks(mark_info, handle, macro_id_set,
                                               tmp_status))) {
          LOG_WARN("fail to mark tablet blocks", K(ret));
        }
      } else {
        if (OB_FAIL(mark_tablet_block(mark_info, handle, macro_id_set,
                                      tmp_status))) {
          LOG_WARN("fail to mark tablet's macro blocks", K(ret), K(tmp_status),
                   KPC(handle.get_obj()));
        }
      }
    }
  }
  return ret;
}

int ObBlockManager::mark_sstable_blocks(
    MacroBlkIdMap &mark_info, ObTabletHandle &handle,
    common::hash::ObHashSet<MacroBlockId, common::hash::NoPthreadDefendMode>
        &macro_id_set,
    ObMacroBlockMarkerStatus &tmp_status) {
  int ret = OB_SUCCESS;
  ObTableStoreIterator table_store_iter(false, false);
  ObArenaAllocator sstable_allocator("LoadSST", OB_MALLOC_NORMAL_BLOCK_SIZE,
                                     MTL_ID());
  ObSafeArenaAllocator safe_allocator(sstable_allocator);

  if (OB_UNLIKELY(!handle.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(handle));
  } else if (OB_FAIL(handle.get_obj()->get_all_sstables(
                 table_store_iter, true /* unpac cosstable */))) {
    LOG_WARN("fail to get all sstables", K(ret));
  } else {
    while (OB_SUCC(ret)) {
      safe_allocator.reuse();
      ObITable *table = nullptr;
      ObSSTableMetaHandle sstable_meta_hdl;
      ObSSTable *sstable = nullptr;
      if (OB_FAIL(table_store_iter.get_next(table))) {
        if (OB_UNLIKELY(OB_ITER_END == ret)) {
          ret = OB_SUCCESS;
          break;
        } else {
          LOG_WARN("fail to get next table from iter", K(ret),
                   K(table_store_iter));
        }
      } else if (FALSE_IT(sstable = static_cast<ObSSTable *>(table))) {
      } else if (OB_ISNULL(sstable)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected error, sstable is nullptr", K(ret), KP(sstable));
      } else if (OB_FAIL(mark_sstable_meta_block(*sstable, mark_info,
                                                 macro_id_set, tmp_status))) {
        LOG_WARN("fail to mark sstable meta block", K(ret), KPC(sstable));
      } else if (OB_FAIL(
                     sstable->get_meta(sstable_meta_hdl, &safe_allocator))) {
        LOG_WARN("fail to get sstable meta", K(ret));
      } else {
        const ObSSTableMeta &meta = sstable_meta_hdl.get_sstable_meta();
        ObMacroIdIterator iterator;
        MacroBlockId macro_id;
        if (OB_FAIL(meta.get_macro_info().get_data_block_iter(iterator))) {
          LOG_WARN("fail to get data block iterator", K(ret), K(meta));
        }
        while (OB_SUCC(ret)) {
          if (OB_FAIL(iterator.get_next_macro_id(macro_id))) {
            if (OB_ITER_END != ret) {
              LOG_WARN("fail to get next macro id", K(ret), K(iterator));
            } else {
              ret = OB_SUCCESS;
              break;
            }
          } else if (OB_FAIL(update_mark_info(macro_id, mark_info))) {
            LOG_WARN("fail to update mark info", K(ret), K(macro_id),
                     KPC(sstable));
          } else if (OB_FAIL(macro_id_set.set_refactored(macro_id,
                                                         0 /*no override*/))) {
            if (OB_HASH_EXIST != ret) {
              LOG_WARN("fail to put macro id into set", K(ret), K(macro_id));
            } else {
              ret = OB_SUCCESS;
            }
          } else {
            if (sstable->is_small_sstable()) {
              tmp_status.shared_data_block_count_++;
            } else {
              tmp_status.data_block_count_++;
            }
            tmp_status.hold_count_--;
          }
        }
        if (OB_SUCC(ret)) {
          iterator.reset();
          if (OB_FAIL(meta.get_macro_info().get_other_block_iter(iterator))) {
            LOG_WARN("fail to get other block iterator", K(ret), K(meta));
          }
        }
        while (OB_SUCC(ret)) {
          if (OB_FAIL(iterator.get_next_macro_id(macro_id))) {
            if (OB_ITER_END != ret) {
              LOG_WARN("fail to get next macro id", K(ret), K(iterator));
            } else {
              ret = OB_SUCCESS;
              break;
            }
          } else if (OB_FAIL(update_mark_info(macro_id, mark_info))) {
            LOG_ERROR("fail to update mark info", K(ret), K(macro_id),
                      KPC(sstable));
          } else if (OB_FAIL(macro_id_set.set_refactored(macro_id))) {
            LOG_WARN("fail to put macro id into set", K(ret), K(macro_id));
          } else {
            tmp_status.index_block_count_++;
            tmp_status.hold_count_--;
          }
        }
        if (OB_SUCC(ret)) {
          iterator.reset();
          if (OB_FAIL(meta.get_macro_info().get_linked_block_iter(iterator))) {
            LOG_WARN("fail to get linked block iterator", K(ret), K(meta));
          }
        }
        while (OB_SUCC(ret)) {
          if (OB_FAIL(iterator.get_next_macro_id(macro_id))) {
            if (OB_ITER_END != ret) {
              LOG_WARN("fail to get next macro id", K(ret), K(iterator));
            } else {
              ret = OB_SUCCESS;
              break;
            }
          } else if (OB_FAIL(update_mark_info(macro_id, mark_info))) {
            LOG_ERROR("fail to update mark info", K(ret), K(macro_id),
                      KPC(sstable));
          } else if (OB_FAIL(macro_id_set.set_refactored(macro_id))) {
            LOG_WARN("fail to put macro id into set", K(ret), K(macro_id));
          } else {
            tmp_status.ids_block_count_++;
            tmp_status.hold_count_--;
          }
        }
      }
    }
  }
  return ret;
}

int ObBlockManager::mark_tablet_meta_blocks(
    MacroBlkIdMap &mark_info, storage::ObTabletHandle &handle,
    common::hash::ObHashSet<MacroBlockId, common::hash::NoPthreadDefendMode>
        &macro_id_set,
    ObMacroBlockMarkerStatus &tmp_status) {
  int ret = OB_SUCCESS;
  const ObTablet *tablet = handle.get_obj();
  ObSArray<MacroBlockId> meta_ids;
  if (OB_FAIL(tablet->get_tablet_first_second_level_meta_ids(meta_ids))) {
    LOG_WARN("fail to get tablet meta block ids", K(ret), KPC(tablet));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < meta_ids.count(); i++) {
      const MacroBlockId &macro_id = meta_ids[i];
      if (OB_FAIL(update_mark_info(macro_id, mark_info))) {
        LOG_WARN("fail to update mark info", K(ret), K(macro_id));
      } else if (OB_FAIL(macro_id_set.set_refactored(macro_id,
                                                     0 /* not overwrite */))) {
        if (OB_HASH_EXIST != ret) {
          LOG_WARN("fail to put macro id into set", K(ret), K(macro_id));
        } else {
          ret = OB_SUCCESS;
        }
      } else {
        tmp_status.shared_meta_block_count_++;
        tmp_status.hold_count_--;
      }
    }
  }
  return ret;
}

int ObBlockManager::mark_sstable_meta_block(
    const blocksstable::ObSSTable &sstable, MacroBlkIdMap &mark_info,
    common::hash::ObHashSet<MacroBlockId, common::hash::NoPthreadDefendMode>
        &macro_id_set,
    ObMacroBlockMarkerStatus &tmp_status) {
  int ret = OB_SUCCESS;
  const ObMetaDiskAddr &addr = sstable.get_addr();
  MacroBlockId macro_id;
  if (addr.is_block()) {
    if (OB_UNLIKELY(!addr.is_valid())) {
      LOG_WARN("sstable addr is invalid", K(ret), K(addr));
    } else if (FALSE_IT(macro_id = addr.block_id())) {
    } else if (OB_FAIL(update_mark_info(macro_id, mark_info))) {
      LOG_WARN("fail to update mark info", K(ret), K(addr), K(macro_id));
    } else if (OB_FAIL(macro_id_set.set_refactored(macro_id,
                                                   0 /* not overwrite */))) {
      if (OB_HASH_EXIST != ret) {
        LOG_WARN("fail to put macro id into set", K(ret), K(macro_id));
      } else {
        ret = OB_SUCCESS;
      }
    } else {
      tmp_status.shared_meta_block_count_++;
      tmp_status.hold_count_--;
    }
  }
  return ret;
}

int ObBlockManager::mark_tablet_block(
    MacroBlkIdMap &mark_info, storage::ObTabletHandle &handle,
    common::hash::ObHashSet<MacroBlockId, common::hash::NoPthreadDefendMode>
        &macro_id_set,
    ObMacroBlockMarkerStatus &tmp_status) {
  int ret = OB_SUCCESS;
  const ObTablet *tablet = handle.get_obj();
  ObTabletBlockInfo block_info(tablet->get_tablet_addr().block_id(),
                               ObTabletMacroType::SHARED_META_BLOCK,
                               0 /*useless param*/);

  if (tablet->get_tablet_addr().is_block() &&
      OB_FAIL(do_mark_tablet_block(block_info, mark_info, macro_id_set,
                                   tmp_status))) {
    LOG_WARN("fail to mark tablet macro id", K(ret), K(block_info));
  } else if (!tablet
                  ->is_empty_shell()) { // empty shell may don't have macro info
    ObArenaAllocator allocator("MarkTabletBlock");
    ObTabletMacroInfo *macro_info = nullptr;
    bool in_memory = true;
    ObMacroInfoIterator macro_iter;
    if (OB_FAIL(tablet->load_macro_info(0 /* ls_epoch in shared_storage */,
                                        allocator, macro_info, in_memory))) {
      LOG_WARN("fail to load macro info", K(ret));
    } else if (OB_FAIL(macro_iter.init(ObTabletMacroType::MAX, *macro_info))) {
      LOG_WARN("fail to init macro iterator", K(ret), KPC(macro_info));
    }
    while (OB_SUCC(ret)) {
      block_info.reset();
      if (OB_FAIL(macro_iter.get_next(block_info))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("fail to get next block info", K(ret), K(block_info));
        } else {
          ret = OB_SUCCESS;
          break;
        }
      } else if (block_info.macro_id_.is_backup_id()) {
        // do nothing
      } else if (OB_FAIL(do_mark_tablet_block(block_info, mark_info, macro_id_set, tmp_status))) {
        LOG_WARN("fail to mark macro id", K(ret), K(block_info));
      }
    }
    if (OB_NOT_NULL(macro_info) && !in_memory) {
      macro_info->reset();
    }
  }
  return ret;
}

int ObBlockManager::do_mark_tablet_block(
    const ObTabletBlockInfo &block_info, MacroBlkIdMap &mark_info,
    common::hash::ObHashSet<MacroBlockId, common::hash::NoPthreadDefendMode>
        &macro_id_set,
    ObMacroBlockMarkerStatus &tmp_status) {
  int ret = OB_SUCCESS;
  const MacroBlockId &macro_id = block_info.macro_id_;
  if (OB_FAIL(update_mark_info(macro_id, mark_info))) {
    LOG_WARN("fail to update mark info", K(ret), K(macro_id));
  } else if (OB_FAIL(macro_id_set.set_refactored(macro_id,
                                                 0 /* not overwrite */))) {
    if (OB_HASH_EXIST != ret) {
      LOG_WARN("fail to put macro id into set", K(ret), K(macro_id));
    } else {
      ret = OB_SUCCESS;
    }
  } else {
    switch (block_info.block_type_) {
    case ObTabletMacroType::META_BLOCK:
    case ObTabletMacroType::LINKED_BLOCK:
      tmp_status.index_block_count_++;
      break;
    case ObTabletMacroType::DATA_BLOCK:
      tmp_status.data_block_count_++;
      break;
    case ObTabletMacroType::SHARED_META_BLOCK:
      tmp_status.shared_meta_block_count_++;
      break;
    case ObTabletMacroType::SHARED_DATA_BLOCK:
      tmp_status.shared_data_block_count_++;
      break;
    default:
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("block type is invalid", K(ret), K(block_info));
      break;
    }
    if (OB_SUCC(ret)) {
      tmp_status.hold_count_--;
    }
  }
  return ret;
}

int ObBlockManager::mark_tenant_ckpt_blocks(
    MacroBlkIdMap &mark_info,
    common::hash::ObHashSet<MacroBlockId, common::hash::NoPthreadDefendMode>
        &macro_id_set,
    ObTenantStorageMetaService &meta_service,
    ObMacroBlockMarkerStatus &tmp_status) {
  int ret = OB_SUCCESS;
  ObArray<MacroBlockId> macro_block_list;

  if (OB_FAIL(macro_block_list.reserve(DEFAULT_PENDING_FREE_COUNT))) {
    LOG_WARN("fail to reserve macro block list", K(ret));
  } else if (OB_FAIL(meta_service.get_meta_block_list(macro_block_list))) {
    LOG_WARN("fail to get tenant checkpoint meta blocks, ", K(ret));
  } else if (OB_FAIL(
                 update_mark_info(macro_block_list, macro_id_set, mark_info))) {
    LOG_WARN("fail to update mark info", K(ret), K(macro_block_list.count()));
  } else {
    tmp_status.linked_block_count_ += macro_block_list.count();
    tmp_status.hold_count_ -= macro_block_list.count();
  }
  return ret;
}

int ObBlockManager::mark_tmp_file_blocks(
    MacroBlkIdMap &mark_info,
    common::hash::ObHashSet<MacroBlockId, common::hash::NoPthreadDefendMode>
        &macro_id_set,
    ObMacroBlockMarkerStatus &tmp_status) {
  int ret = OB_SUCCESS;
  omt::ObMultiTenant *omt = GCTX.omt_;
  common::ObSEArray<uint64_t, 8> mtl_tenant_ids;

  omt->get_mtl_tenant_ids(mtl_tenant_ids);
  for (int64_t i = 0; OB_SUCC(ret) && i < mtl_tenant_ids.count(); i++) {
    const uint64_t tenant_id = mtl_tenant_ids.at(i);
    MTL_SWITCH(tenant_id) {
      ObArray<MacroBlockId> macro_block_list;
      if (OB_FAIL(MTL(ObTenantTmpFileManager*)->get_sn_file_manager().get_macro_block_list(macro_block_list))) {
        LOG_WARN("fail to get macro block list", K(ret));
      } else if (OB_FAIL(update_mark_info(macro_block_list, macro_id_set, mark_info))){
        LOG_WARN("fail to update mark info", K(ret), K(macro_block_list.count()));
      } else {
        tmp_status.tmp_file_count_ += macro_block_list.count();
        tmp_status.hold_count_ -= macro_block_list.count();
      }
    }
  }

  return ret;
}

int ObBlockManager::mark_server_meta_blocks(
    MacroBlkIdMap &mark_info,
    common::hash::ObHashSet<MacroBlockId, common::hash::NoPthreadDefendMode>
        &macro_id_set,
    ObMacroBlockMarkerStatus &tmp_status) {
  int ret = OB_SUCCESS;
  ObArray<MacroBlockId> macro_block_list;

  if (OB_FAIL(macro_block_list.reserve(DEFAULT_PENDING_FREE_COUNT))) {
    LOG_WARN("fail to reserve macro block list", K(ret));
  } else if (OB_FAIL(SERVER_STORAGE_META_SERVICE.get_meta_block_list(
                 macro_block_list))) {
    LOG_WARN("fail to get macro block list", K(ret));
  } else if (OB_FAIL(
                 update_mark_info(macro_block_list, macro_id_set, mark_info))) {
    LOG_WARN("fail to update mark info", K(ret), K(macro_block_list.count()));
  } else {
    tmp_status.linked_block_count_ += macro_block_list.count();
    tmp_status.hold_count_ -= macro_block_list.count();
  }
  return ret;
}

int ObBlockManager::update_mark_info(
    const ObIArray<MacroBlockId> &macro_block_list,
    common::hash::ObHashSet<MacroBlockId, common::hash::NoPthreadDefendMode>
        &macro_id_set,
    MacroBlkIdMap &mark_info) {
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < macro_block_list.count(); i++) {
    const MacroBlockId &macro_id = macro_block_list.at(i);
    if (OB_FAIL(update_mark_info(macro_id, mark_info))) {
      LOG_WARN("fail to update mark info", K(ret), K(macro_id));
    } else if (OB_FAIL(macro_id_set.set_refactored(macro_id))) {
      LOG_WARN("fail to put macro id into set", K(ret), K(macro_id));
    }
  }
  return ret;
}

int ObBlockManager::update_mark_info(const MacroBlockId &macro_id,
                                     MacroBlkIdMap &mark_info) {
  int ret = OB_SUCCESS;
  BlockInfo block_info;
  bool can_free = false;
  if (OB_UNLIKELY(!macro_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(macro_id));
  } else if (OB_FAIL(block_map_.get(macro_id, block_info))) { // double check.
    if (OB_ENTRY_NOT_EXIST == ret) {
      // BUG, should not happen
      LOG_ERROR("macro block is using, not exist in block map, fatal error",
                K(ret), K(macro_id), K(block_info));
    } else {
      LOG_WARN("fail to get from block map", K(ret), K(macro_id));
    }
  } else if (OB_UNLIKELY(block_info.ref_cnt_ < 0)) {
    LOG_ERROR("macro block should is using, ref cnt shouldn't be less than or "
              "equal to 0, "
              "fatal error",
              K(ret), K(macro_id), K(block_info));
  } else if (OB_FAIL(mark_info.get(macro_id, can_free))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to get from mark info", K(ret), K(macro_id),
               K(block_info));
    }
  } else if (!can_free) {
    // do nothing.
  } else {
    if (OB_UNLIKELY(0 == block_info.ref_cnt_)) {
      // BUG, should not happen
      LOG_ERROR("macro block is using, should not mark sweep, fatal error",
                K(ret), K(macro_id), K(block_info));
    } else {
      LOG_INFO("macro block is using, and ref cnt is more than 0", K(macro_id),
               K(block_info));
    }

    if (OB_FAIL(mark_info.insert_or_update(macro_id, false))) {
      LOG_WARN("fail to insert or update mark info", K(ret), K(macro_id));
    }
  }
  return ret;
}

int ObBlockManager::BlockMapIterator::get_next_block(common::ObIOFd &block_id)
{
  int ret = OB_SUCCESS;
  MacroBlockId key;
  BlockInfo blk_info;
  if (OB_FAIL(iter_.next(key, blk_info))) {
    LOG_WARN("fail to get next block", K(ret));
  } else {
    block_id.first_id_ = key.first_id();
    block_id.second_id_ = key.second_id();
    if (max_write_seq_ < key.write_seq()) {
      max_write_seq_ = key.write_seq();
    }
  }
  return ret;
}

void ObBlockManager::MarkBlockTask::runTimerTask() {
  blk_mgr_.mark_and_sweep();
  (void)blk_mgr_.extend_file_size_if_need(); // auto extend
}

// 2 days
const int64_t ObBlockManager::InspectBadBlockTask::ACCESS_TIME_INTERVAL =
    2 * 86400 * 1000000ull;
// block count inspected per round
const int64_t ObBlockManager::InspectBadBlockTask::MIN_OPEN_BLOCKS_PER_ROUND =
    1;
// max search number per round
const int64_t ObBlockManager::InspectBadBlockTask::MAX_SEARCH_COUNT_PER_ROUND =
    1000;

ObBlockManager::InspectBadBlockTask::InspectBadBlockTask(
    ObBlockManager &blk_mgr)
    : blk_mgr_(blk_mgr), last_macro_idx_(-1), last_check_time_(0) {}

ObBlockManager::InspectBadBlockTask::~InspectBadBlockTask() { reset(); }

void ObBlockManager::InspectBadBlockTask::reset() {
  last_macro_idx_ = -1;
  last_check_time_ = 0;
}

void ObBlockManager::InspectBadBlockTask::runTimerTask() {
  const int64_t next_check_time = last_check_time_ + ACCESS_TIME_INTERVAL;
  if (next_check_time <=
      ObTimeUtility::fast_current_time()) { /* exceed 2 days */
    inspect_bad_block();
    if (last_macro_idx_ == -1) { // finished
      last_check_time_ = ObTimeUtility::fast_current_time();
    }
  } else {
    LOG_INFO("skip inspect bad block", K_(last_check_time), K_(last_macro_idx));
  }
}

int ObBlockManager::InspectBadBlockTask::check_block(
    ObMacroBlockHandle &macro_block_handle) {
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!macro_block_handle.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(macro_block_handle));
  } else {
    const MacroBlockId &macro_id = macro_block_handle.get_macro_id();
    ObMacroBlockReadInfo read_info;
    common::ObArenaAllocator allocator(ObModIds::OB_SSTABLE_BLOCK_FILE);
    read_info.io_timeout_ms_ = std::max(GCONF._data_storage_io_timeout / 1000,
                                        DEFAULT_IO_WAIT_TIME_MS);
    read_info.macro_block_id_ = macro_id;
    read_info.offset_ = 0;
    read_info.size_ = OB_STORAGE_OBJECT_MGR.get_macro_block_size();
    read_info.io_desc_.set_wait_event(ObWaitEventIds::DB_FILE_COMPACT_READ);
    read_info.io_desc_.set_sys_module_id(ObIOModule::INSPECT_BAD_BLOCK_IO);

    if (OB_ISNULL(read_info.buf_ = reinterpret_cast<char *>(
                      allocator.alloc(read_info.size_)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "failed to alloc macro read info buffer", K(ret),
                  K(read_info.size_));
    } else if (OB_FAIL(ObBlockManager::async_read_block(read_info,
                                                        macro_block_handle))) {
      LOG_WARN("async read block failed", K(ret), K(macro_id), K(read_info));
    } else if (OB_FAIL(macro_block_handle.wait())) {
      LOG_WARN("io wait failed", K(ret), K(macro_id), K(read_info));
    } else if (macro_block_handle.get_data_size() != read_info.size_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("buf size is too small", K(ret), K(macro_id),
               K(macro_block_handle.get_data_size()), K(read_info.size_));
    } else if (OB_FAIL(ObSSTableMacroBlockChecker::check(
                   read_info.buf_, read_info.size_,
                   ObMacroBlockCheckLevel::CHECK_LEVEL_PHYSICAL))) {
      LOG_ERROR("fail to check sstable macro block", K(ret), K(macro_id),
                KP(read_info.buf_), K(read_info.size_));
      char error_msg[common::OB_MAX_ERROR_MSG_LEN];
      char macro_id_str[128];
      MEMSET(error_msg, 0, sizeof(error_msg));
      MEMSET(macro_id_str, 0, sizeof(macro_id_str));
      int tmp_ret = OB_SUCCESS;
      macro_id.to_string(macro_id_str, sizeof(macro_id_str));
      if (OB_SUCCESS != (tmp_ret = databuff_printf(
                             error_msg, sizeof(error_msg),
                             "Bad data block: macro id=%s", macro_id_str))) {
        LOG_WARN("error msg is too long, ", K(tmp_ret), K(sizeof(error_msg)));
      } else if (OB_SUCCESS !=
                 (tmp_ret = blk_mgr_.report_bad_block(macro_id, ret, error_msg,
                                                      GCONF.data_dir))) {
        LOG_WARN("Fail to report bad block", K(tmp_ret), K(macro_id), K(ret),
                 K(error_msg));
      } else {
        ret = OB_SUCCESS; // after report bad block, overwrite ret code and
                          // continue to check.
      }
    }
  }
  return ret;
}

int ObBlockManager::extend_file_size_if_need() {
  int ret = OB_SUCCESS;
  int64_t reserved_size =
      4 * 1024 * 1024 * 1024L; // default RESERVED_DISK_SIZE -> 4G

  if (OB_ISNULL(io_device_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("block manager hasn't inited", K(ret), KP(io_device_));
  } else if (OB_FAIL(SERVER_STORAGE_META_SERVICE.get_reserved_size(
                 reserved_size))) {
    LOG_WARN("Fail to get reserved size", K(ret));
  } else if (!check_can_be_extend(reserved_size)) {
    ret = OB_NOT_READY_TO_EXTEND_FILE;
    LOG_DEBUG("Check auto extend, no need to start ssbfile auto extend",
              K(ret));
  } else {
    const int64_t total_block_cnt =
        OB_STORAGE_OBJECT_MGR.get_total_macro_block_count();
    const int64_t free_block_cnt = get_free_macro_block_count();
    const int64_t usage_upper_bound_percentage =
        GCONF._datafile_usage_upper_bound_percentage;
    const int64_t free_block_cnt_to_extend =
        total_block_cnt - total_block_cnt * usage_upper_bound_percentage / 100;
    // here we can see auto extend disk premise:
    // 1. free_block_cnt ratio is less than one percentage (default 10%)
    // 2. free_block_cnt is less than one value (512 = 1G)
    if (free_block_cnt_to_extend < free_block_cnt &&
        (free_block_cnt > AUTO_EXTEND_LEAST_FREE_BLOCK_CNT)) {
      LOG_DEBUG("Do not extend file, not reach extend trigger.",
                K(free_block_cnt_to_extend), K(free_block_cnt),
                K(total_block_cnt));
    } else {
      LOG_INFO("Start to do auto ssblock file extend.", K(total_block_cnt),
               K(free_block_cnt), K(free_block_cnt_to_extend),
               K(usage_upper_bound_percentage));

      int64_t suggest_extend_size = 0;
      int64_t datafile_disk_percentage = 0;

      if (OB_FAIL(observer::ObServerUtils::calc_auto_extend_size(
              suggest_extend_size))) {
        LOG_DEBUG("calc auto extend size error, maybe ssblock file has reach "
                  "it's max size",
                  K(ret));
      } else if (OB_FAIL(OB_STORAGE_OBJECT_MGR.resize_local_device(
                     suggest_extend_size, datafile_disk_percentage,
                     reserved_size))) {
        LOG_WARN("Fail to resize file in auto extend", K(ret),
                 K(suggest_extend_size));
      }
    }
  }
  return ret;
}

bool ObBlockManager::check_can_be_extend(const int64_t reserved_size) {
  bool can_be_extended = false;

  const int64_t datafile_maxsize = GCONF.datafile_maxsize;
  const int64_t datafile_next = GCONF.datafile_next;
  const int64_t current_block_file_size = io_device_->get_total_block_size();

  if (OB_UNLIKELY(datafile_maxsize <= 0) || OB_UNLIKELY(datafile_next <= 0) ||
      OB_UNLIKELY(current_block_file_size <= 0)) {
    LOG_DEBUG("Do not extend file size, datafile param not set or unexpected "
              "block file size",
              K(datafile_maxsize), K(datafile_next),
              K(current_block_file_size));
  } else if (datafile_maxsize <= current_block_file_size) {
    LOG_DEBUG("Do not extend file size, maxsize is smaller than datafile size",
              K(datafile_maxsize), K(current_block_file_size));
  } else {
    const int64_t max_block_cnt = get_max_macro_block_count(reserved_size);
    const int64_t current_block_cnt =
        OB_STORAGE_OBJECT_MGR.get_total_macro_block_count();
    if (max_block_cnt <= current_block_cnt) {
      LOG_DEBUG("Do not extend file size, max block cnt is smaller than "
                "current block cnt",
                K(max_block_cnt), K(current_block_cnt));
    } else {
      can_be_extended = true;
    }
  }

  return can_be_extended;
}

static inline int64_t get_disk_allowed_iops(const int64_t macro_block_size) {
  const int64_t max_bkgd_band_width = 64 * 1024 * 1024;
  const int64_t max_check_iops = max_bkgd_band_width / macro_block_size;
  return max_check_iops;
}

void ObBlockManager::InspectBadBlockTask::inspect_bad_block() {
  int ret = OB_SUCCESS;
  const int64_t macro_block_size = OB_STORAGE_OBJECT_MGR.get_macro_block_size();
  const int64_t verify_cycle = GCONF.builtin_db_data_verify_cycle;
  const int64_t sec_per_day = 24 * 3600;
  const int64_t check_times_per_day =
      sec_per_day * 1000 * 1000 / ObBlockManager::INSPECT_DELAY_US;
  ObArray<MacroBlockId> macro_ids;
  GetAllMacroBlockIdFunctor getter(macro_ids);

  if (OB_UNLIKELY(verify_cycle <= 0)) {
    // macro block inspection is disabled, do nothing
  } else if (OB_UNLIKELY(!blk_mgr_.is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("The block manager has not been inited", K(ret));
  } else if (OB_FAIL(macro_ids.reserve(blk_mgr_.block_map_.count()))) {
    LOG_WARN("fail to reserver macro id array", K(ret), "block count",
             blk_mgr_.block_map_.count());
  } else if (OB_FAIL(blk_mgr_.block_map_.for_each(getter))) {
    LOG_WARN("fail to for each block map", K(ret));
  } else if (OB_UNLIKELY(0 == macro_ids.size())) {
    // nothing to do.
  } else {
    const int64_t total_used_macro_block_count = macro_ids.size();
    const int64_t check_blk_cnt_per_day = std::max(
        MIN_OPEN_BLOCKS_PER_ROUND, total_used_macro_block_count / verify_cycle);
    const int64_t blk_cnt_per_round =
        check_blk_cnt_per_day / check_times_per_day;
    const int64_t search_num_per_round =
        0 == blk_cnt_per_round ? MIN_OPEN_BLOCKS_PER_ROUND : blk_cnt_per_round;
    const int64_t disk_allowed_iops = get_disk_allowed_iops(macro_block_size);
    const int64_t max_check_count_per_round =
        std::min(search_num_per_round,
                 std::max(MIN_OPEN_BLOCKS_PER_ROUND, disk_allowed_iops));
    const int64_t inspect_timeout_us =
        std::max(GCONF._data_storage_io_timeout * 1,
                 max_check_count_per_round * DEFAULT_IO_WAIT_TIME_MS * 1000);
    const int64_t begin_time = ObTimeUtility::current_time();
    int64_t check_count = 0;

    for (int64_t i = 0;
         i < MAX_SEARCH_COUNT_PER_ROUND &&
         check_count < max_check_count_per_round &&
         (ObTimeUtility::current_time() - begin_time) < inspect_timeout_us;
         ++i) {
      ++last_macro_idx_;
      if (last_macro_idx_ >= total_used_macro_block_count) {
        last_macro_idx_ = -1; // finished this round
        break;
      }

      const MacroBlockId &macro_id = macro_ids.at(last_macro_idx_);
      ObMacroBlockInfo block_info;
      ObMacroBlockHandle macro_block_handle;
      if (OB_FAIL(blk_mgr_.get_macro_block_info(macro_id, block_info,
                                                macro_block_handle))) {
        LOG_WARN("fail to get macro block info", K(ret), K(macro_id),
                 K(last_macro_idx_));
      } else if (OB_UNLIKELY(block_info.is_free_)) {
        // do nothing, this MacroBlock has been released. skip this MacroBlock
        // and continue.
      } else if (!block_info.is_free_ && block_info.ref_cnt_ > 0
#ifdef ERRSIM
                 && (begin_time - block_info.access_time_) >
                        static_cast<int64_t>(10_s)) {
        LOG_INFO("errsim bad block: start check macro block", K(block_info));
#else
                 && (begin_time - block_info.access_time_) >
                        ACCESS_TIME_INTERVAL) {
#endif
        ++check_count;
        LOG_INFO("check macro block", K(block_info), "time_interval",
                 begin_time - block_info.access_time_);
        if (OB_FAIL(check_block(macro_block_handle))) {
          LOG_WARN("found a bad block", K(ret), K(macro_id));
        }
      }
    }
    if (REACH_COUNT_INTERVAL(60)) { // print log per 60 times.
      const int64_t cost_time = ObTimeUtility::current_time() - begin_time;
      LOG_INFO("inspect bad block cost time", K(cost_time), K(check_count),
               K(total_used_macro_block_count), K(max_check_count_per_round));
    }
  }
}

ObServerBlockManager &ObServerBlockManager::get_instance() {
  static ObServerBlockManager instance_;
  return instance_;
}

} // namespace blocksstable
} // namespace oceanbase
