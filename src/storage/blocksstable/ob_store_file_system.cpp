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
#include "ob_store_file_system.h"
#include "ob_store_file.h"
#include "ob_micro_block_cache.h"
#include "ob_micro_block_index_cache.h"
#include "ob_local_file_system.h"
#include "ob_raid_file_system.h"
#include "share/ob_force_print_log.h"
#include "storage/ob_partition_service.h"
#include "storage/ob_data_macro_id_iterator.h"
#include "storage/ob_tenant_file_mgr.h"
#include "storage/ob_tenant_file_struct.h"
#include "storage/ob_file_system_util.h"

namespace oceanbase {
using namespace common;
using namespace share;
using namespace storage;
using namespace rootserver;
namespace blocksstable {

// ---------- ObServerWorkingDir ---------- //
const char* ObServerWorkingDir::SVR_WORKING_DIR_PREFIX[(int32_t)DirStatus::MAX] = {
    "",            // NORMAL
    "recovering",  // RECOVERING
    "recovered",   // RECOVERED
    "tmp",         // TEMPORARY
    "deleting",    // DELETING
};

ObServerWorkingDir::ObServerWorkingDir() : svr_addr_(), start_ts_(0), status_(DirStatus::MAX)
{}

ObServerWorkingDir::ObServerWorkingDir(const ObAddr svr, DirStatus status)
    : svr_addr_(svr), start_ts_(0), status_(status)
{}

bool ObServerWorkingDir::is_valid() const
{
  return svr_addr_.is_valid() && status_ < DirStatus::MAX && start_ts_ >= 0;
}

void ObServerWorkingDir::reset()
{
  svr_addr_.reset();
  start_ts_ = 0;
  status_ = DirStatus::MAX;
}

int ObServerWorkingDir::to_path_string(char* path, const int64_t path_size) const
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  char svr_str[MAX_IP_PORT_LENGTH] = {};
  const char* prefix = "";
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(*this));
  } else if (OB_FAIL(svr_addr_.ip_port_to_string(svr_str, sizeof(svr_str)))) {
    LOG_WARN("get svr ip port string fail", K(ret), K(svr_addr_));
  } else {
    prefix = SVR_WORKING_DIR_PREFIX[(int32_t)status_];
    if (DirStatus::NORMAL == status_) {
      pos = snprintf(path, path_size, "%s_%ld", svr_str, start_ts_);
    } else {
      pos = snprintf(path, path_size, "%s_%s_%ld", prefix, svr_str, start_ts_);
    }
    if (pos < 0 || pos >= path_size) {
      ret = OB_BUF_NOT_ENOUGH;
      LOG_WARN("path buffer size not enough", K(ret), K(prefix), K(svr_str), K(start_ts_), K(path_size));
    }
  }
  return ret;
}

/**
 * ------------------------------ObFileSystemInspectBadBlockTask-----------------------------------
 */
// 2 days
const int64_t ObFileSystemInspectBadBlockTask::ACCESS_TIME_INTERVAL = 2 * 86400 * 1000000ull;
// block count inspected per round
const int64_t ObFileSystemInspectBadBlockTask::MIN_OPEN_BLOCKS_PER_ROUND = 1;
// max search number per round
const int64_t ObFileSystemInspectBadBlockTask::MAX_SEARCH_COUNT_PER_ROUND = 1000;
ObFileSystemInspectBadBlockTask::ObFileSystemInspectBadBlockTask()
    : last_partition_idx_(0), last_sstable_idx_(0), last_macro_idx_(0)
{}

ObFileSystemInspectBadBlockTask::~ObFileSystemInspectBadBlockTask()
{}

void ObFileSystemInspectBadBlockTask::destroy()
{
  last_partition_idx_ = 0;
  last_sstable_idx_ = 0;
  last_macro_idx_ = 0;
  macro_checker_.destroy();
}

void ObFileSystemInspectBadBlockTask::runTimerTask()
{
  inspect_bad_block();
}

int ObFileSystemInspectBadBlockTask::check_macro_block(
    const ObMacroBlockInfoPair& pair, const ObTenantFileKey& file_key)
{
  int ret = OB_SUCCESS;
  // get macro meta
  const ObFullMacroBlockMeta& full_meta = pair.meta_;
  const MacroBlockId& macro_id = pair.block_id_;
  if (!macro_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Wrong input param, ", K(ret), K(macro_id));
  } else if (OB_UNLIKELY(!full_meta.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "The macro block meta is NULL, ", K(ret), K(macro_id));
  } else if (full_meta.meta_->is_data_block()) {
    // check data blocks
    if (OB_FAIL(check_data_block(macro_id, full_meta, file_key))) {
      STORAGE_LOG(WARN,
          "Error occurred in sstable data block, ",
          K(ret),
          K(macro_id),
          K(full_meta.meta_->partition_id_),
          K(full_meta.meta_->table_id_));
    }
  }  // check other blocks, for future extending

  return ret;
}

int ObFileSystemInspectBadBlockTask::check_data_block(
    const MacroBlockId& macro_id, const ObFullMacroBlockMeta& full_meta, const ObTenantFileKey& file_key)
{
  int ret = OB_SUCCESS;
  if (!full_meta.is_valid() || !macro_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Wrong input param, ", K(ret), K(full_meta), K(macro_id));
  } else {
    ObMacroBlockReadInfo read_info;
    ObMacroBlockHandle macro_handle;
    common::ObArenaAllocator allocator(ObModIds::OB_SSTABLE_BLOCK_FILE);
    ObStoreFileCtx file_ctx(allocator);
    ObMacroBlockCtx macro_block_ctx;  // TODO(): fix check_data_block for ofs later

    file_ctx.file_system_type_ = STORE_FILE_SYSTEM_LOCAL;
    macro_block_ctx.file_ctx_ = &file_ctx;
    macro_block_ctx.sstable_block_id_.macro_block_id_ = macro_id;

    const int64_t io_timeout_ms = std::max(GCONF._data_storage_io_timeout / 1000, DEFAULT_IO_WAIT_TIME_MS);
    const ObMacroBlockMetaV2* macro_meta = full_meta.meta_;
    read_info.macro_block_ctx_ = &macro_block_ctx;
    read_info.offset_ = 0;
    read_info.size_ = OB_FILE_SYSTEM.get_macro_block_size();
    read_info.io_desc_.category_ = SYS_IO;
    read_info.io_desc_.wait_event_no_ = ObWaitEventIds::DB_FILE_COMPACT_READ;

    ObStorageFileHandle file_handle;
    blocksstable::ObStorageFile* file = NULL;
    if (OB_FAIL(OB_SERVER_FILE_MGR.get_tenant_file(file_key, file_handle))) {
      STORAGE_LOG(WARN, "fail to get tenant file", K(ret), K(file_key));
    } else if (OB_ISNULL(file = file_handle.get_storage_file())) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "file is null", K(ret), K(file_key));
    } else if (FALSE_IT(macro_handle.set_file(file))) {
    } else if (OB_FAIL(file->async_read_block(read_info, macro_handle))) {
      STORAGE_LOG(WARN, "async read block failed, ", K(ret), K(macro_id), K(read_info));
    } else if (OB_FAIL(macro_handle.wait(io_timeout_ms))) {
      STORAGE_LOG(WARN, "io wait failed", K(ret), K(macro_id), K(io_timeout_ms));
    } else if (NULL == macro_handle.get_buffer() || macro_handle.get_data_size() != read_info.size_) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN,
          "buf is null or buf size is too small",
          K(ret),
          K(macro_id),
          KP(macro_handle.get_buffer()),
          K(macro_handle.get_data_size()),
          K(read_info.size_));
    } else if (OB_FAIL(macro_checker_.check(macro_handle.get_buffer(), macro_handle.get_data_size(), full_meta))) {
      STORAGE_LOG(ERROR,
          "Fail to check sstable macro block",
          K(ret),
          K(macro_id),
          K(macro_meta->table_id_),
          K(macro_meta->partition_id_),
          KP(macro_handle.get_buffer()),
          K(macro_handle.get_data_size()),
          K(*macro_meta));
      char error_msg[common::OB_MAX_ERROR_MSG_LEN];
      char macro_id_str[128];
      MEMSET(error_msg, 0, sizeof(error_msg));
      MEMSET(macro_id_str, 0, sizeof(macro_id_str));
      int tmp_ret = OB_SUCCESS;
      macro_id.to_string(macro_id_str, sizeof(macro_id_str));
      if (OB_SUCCESS != (tmp_ret = databuff_printf(error_msg,
                             sizeof(error_msg),
                             "Bad data block: table_id=%lu, partition_id=%ld, macro_block_index=%s",
                             macro_meta->table_id_,
                             macro_meta->partition_id_,
                             macro_id_str))) {
        STORAGE_LOG(WARN, "error msg is too long, ", K(tmp_ret), K(sizeof(error_msg)));
      } else if (OB_SUCCESS !=
                 (tmp_ret = OB_FILE_SYSTEM.report_bad_block(macro_id, ret, error_msg, file->get_path()))) {
        STORAGE_LOG(WARN, "Fail to report bad block, ", K(tmp_ret), K(macro_id), "error_type", ret, K(error_msg));
      }
    }
  }
  return ret;
}

static inline int64_t get_disk_allowed_iops(const int64_t macro_block_size)
{
  int64_t disk_allowed_iops = 0;
  const int64_t max_bkgd_band_width = 64 * 1024 * 1024;
  const int64_t max_check_iops = max_bkgd_band_width / macro_block_size;
  double iops = 0;
  if (0 == GCONF.sys_bkgd_io_low_percentage) {
    disk_allowed_iops = static_cast<int64_t>(max_bkgd_band_width / macro_block_size);
  } else if (OB_SUCCESS == ObIOBenchmark::get_instance().get_max_iops(IO_MODE_READ, macro_block_size, iops)) {
    disk_allowed_iops = static_cast<int64_t>(static_cast<double>(GCONF.sys_bkgd_io_low_percentage) / 100 * iops);
  }
  disk_allowed_iops = std::min(max_check_iops, disk_allowed_iops);
  return disk_allowed_iops;
}

bool ObFileSystemInspectBadBlockTask::has_inited()
{
  return OB_FILE_SYSTEM.is_inited_ && OB_STORE_FILE.is_opened_;
}

void ObFileSystemInspectBadBlockTask::inspect_bad_block()
{
  int ret = OB_SUCCESS;
  const int64_t check_times_per_second = 1000 * 1000 / ObStoreFile::INSPECT_DELAY_US;
  const int64_t check_cycle_in_second = GCONF.builtin_db_data_verify_cycle * 24 * 3600;
  ObDataMacroIdIterator macro_iter;
  if (check_cycle_in_second <= 0) {
    // macro block inspection is disabled, do nothing
  } else if (OB_UNLIKELY(!has_inited())) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "The file system has not been inited or opened, ", K(ret));
  } else if (OB_FAIL(macro_iter.init(ObPartitionService::get_instance()))) {
    STORAGE_LOG(WARN, "fail to init macro iter", K(ret));
  } else if (OB_FAIL(macro_iter.locate(last_partition_idx_, last_sstable_idx_, last_macro_idx_))) {
    if (OB_ITER_END != ret) {
      STORAGE_LOG(WARN, "fail to locate to last macro block iterator", K(ret));
    } else {
      last_partition_idx_ = 0;
      last_sstable_idx_ = 0;
      last_macro_idx_ = 0;
    }
  } else {
    const int64_t macro_block_size = OB_FILE_SYSTEM.get_macro_block_size();
    const int64_t total_macro_block_count = OB_FILE_SYSTEM.get_used_macro_block_count();
    const int64_t search_num_per_round =
        std::min(total_macro_block_count / (check_cycle_in_second * check_times_per_second), total_macro_block_count);
    const int64_t disk_allowed_iops = get_disk_allowed_iops(macro_block_size);
    const int64_t max_check_count_per_round = std::max(MIN_OPEN_BLOCKS_PER_ROUND, disk_allowed_iops);
    const int64_t inspect_timeout_us =
        std::max(GCONF._data_storage_io_timeout * 1, max_check_count_per_round * DEFAULT_IO_WAIT_TIME_MS * 1000);
    const int64_t begin_time = ObTimeUtility::current_time();
#ifdef ERRSIM
    const int64_t access_time_interval = 0;
#else
    const int64_t access_time_interval = ACCESS_TIME_INTERVAL;
#endif
    int64_t check_count = 0;
    ObMacroBlockInfoPair pair;
    ObTenantFileKey file_key;
    for (int64_t i = 0; i < search_num_per_round && check_count < max_check_count_per_round &&
                        (ObTimeUtility::current_time() - begin_time) < inspect_timeout_us;
         ++i) {
      if (OB_FAIL(macro_iter.get_next_macro_info(pair, file_key))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          last_partition_idx_ = 0;
          last_sstable_idx_ = 0;
          last_macro_idx_ = 0;
          break;
        }
      } else {
        ObMacroBlockInfo macro_block_info;
        if (OB_FAIL(OB_FILE_SYSTEM.get_macro_block_info(file_key, pair.block_id_, macro_block_info))) {
          STORAGE_LOG(WARN, "Fail to get macro block info, ", K(ret), K(pair));
        } else {
          if (!macro_block_info.is_free_        // not free
              && macro_block_info.ref_cnt_ > 0  // must in use
              && (begin_time - macro_block_info.access_time_) > access_time_interval) {
            STORAGE_LOG(
                INFO, "ERRSIM bad block: start check macro block, ", K(total_macro_block_count), K(macro_block_info));
            ++check_count;
            STORAGE_LOG(INFO,
                "check macro block, ",
                K(macro_block_info),
                "time_interval",
                (begin_time - macro_block_info.access_time_));
            if (OB_FAIL(check_macro_block(pair, file_key))) {
              STORAGE_LOG(WARN, "Found a bad block, ", K(ret), K(pair.block_id_));
            }
          }
        }
      }
      if (OB_ITER_END != ret) {
        if (OB_FAIL(macro_iter.get_last_idx(last_partition_idx_, last_sstable_idx_, last_macro_idx_))) {
          STORAGE_LOG(WARN, "Fail to get last idx", K(ret));
        }
      }
    }  // end for
    const int64_t cost_time = ObTimeUtility::current_time() - begin_time;
    STORAGE_LOG(INFO, "inspect_bad_block cost time, ", K(cost_time), K(check_count), K(total_macro_block_count));
  }
}

void ObStorageFileWithRef::inc_ref()
{
  ATOMIC_INC(&ref_cnt_);
}

void ObStorageFileWithRef::dec_ref()
{
  ATOMIC_DEC(&ref_cnt_);
}

int64_t ObStorageFileWithRef::get_ref()
{
  return ATOMIC_LOAD(&ref_cnt_);
}

ObStorageFileHandle::ObStorageFileHandle() : file_with_ref_(nullptr)
{}

ObStorageFileHandle::~ObStorageFileHandle()
{
  reset();
}

void ObStorageFileHandle::reset()
{
  if (nullptr != file_with_ref_) {
    file_with_ref_->dec_ref();
    file_with_ref_ = nullptr;
  }
}

void ObStorageFileHandle::set_storage_file_with_ref(ObStorageFileWithRef& file_with_ref)
{
  reset();
  file_with_ref_ = &file_with_ref;
  file_with_ref.inc_ref();
}

int ObStorageFileHandle::assign(const ObStorageFileHandle& other)
{
  int ret = OB_SUCCESS;
  if (nullptr != file_with_ref_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("can not init twice", K(ret));
  } else if (nullptr == other.file_with_ref_) {
    // do nothing
  } else {
    file_with_ref_ = other.file_with_ref_;
    file_with_ref_->inc_ref();
  }
  return ret;
}

ObStorageFilesHandle::ObStorageFilesHandle() : file_array_()
{}

ObStorageFilesHandle::~ObStorageFilesHandle()
{
  reset();
}

void ObStorageFilesHandle::reset()
{
  for (int64_t i = 0; i < file_array_.count(); ++i) {
    ObStorageFileWithRef* storage_file = file_array_.at(i);
    if (nullptr != storage_file) {
      storage_file->dec_ref();
    }
  }
  file_array_.reset();
}

int ObStorageFilesHandle::add_storage_file(ObStorageFileWithRef& file)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(file_array_.push_back(&file))) {
    LOG_WARN("fail to push back file", K(ret));
  } else {
    file.inc_ref();
  }
  return ret;
}

int ObStorageFilesHandle::get_storage_file(const int64_t idx, ObStorageFile*& file)
{
  int ret = OB_SUCCESS;
  ObStorageFileWithRef* file_with_ref = nullptr;
  file = nullptr;
  if (OB_FAIL(file_array_.at(idx, file_with_ref))) {
    LOG_WARN("fail to get file with ref", K(ret));
  } else if (nullptr == file_with_ref) {
    ret = OB_ERR_SYS;
    LOG_WARN("error sys, file with ref must not be null", K(ret));
  } else {
    file = file_with_ref->file_;
  }
  return ret;
}

ObStorageFile::ObStorageFile()
    : tenant_id_(OB_INVALID_TENANT_ID),
      file_id_(0),
      file_type_(FileType::MAX),
      fd_(OB_INVALID_FD),
      is_inited_(false),
      macro_block_info_(),
      lock_(),
      replay_map_()
{}

int ObStorageFile::init_base(const FileType file_type, const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (OB_INVALID_TENANT_ID == tenant_id || OB_INVALID_ID == tenant_id || file_type >= FileType::MAX) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id", K(ret), K(tenant_id), K(file_type));
  } else {
    if (OB_FAIL(lock_.init(LOCK_BUCKET_CNT, ObLatchIds::FILE_REF_LOCK))) {
      LOG_WARN("fail to init lock", K(ret));
    } else if (OB_FAIL(macro_block_info_.init(ObModIds::OB_STORAGE_FILE_BLOCK_REF, tenant_id))) {
      LOG_WARN("Fail to init macro_block_infos_, ", K(ret));
    } else if (OB_FAIL(replay_map_.init())) {
      LOG_WARN("Fail to init replay map", K(ret));
    } else {
      file_type_ = file_type;
      tenant_id_ = tenant_id;
    }
  }
  if (OB_FAIL(ret)) {
    reset();
  }
  return ret;
}

void ObStorageFile::reset()
{
  tenant_id_ = OB_INVALID_TENANT_ID;
  file_id_ = OB_INVALID_DATA_FILE_ID;
  file_type_ = FileType::MAX;
  fd_ = OB_INVALID_FD;
  macro_block_info_.destroy();
  replay_map_.destroy();
  lock_.destroy();
}

void ObStorageFile::reserve_ref_cnt_map_for_merge()
{
  const double bkt_expand_rate = 2.0;
  const double load_factor_limit = 1 / bkt_expand_rate;
  if (macro_block_info_.get_load_factor() > load_factor_limit) {
    const uint64_t bkt_cnt = macro_block_info_.get_bkt_cnt();
    macro_block_info_.resize(bkt_cnt * bkt_expand_rate);
  }
}

int ObStorageFile::inc_ref(const MacroBlockId& macro_id)
{
  int ret = OB_SUCCESS;
  BlockInfo block_info;
  if (OB_UNLIKELY(!macro_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(ERROR, "Invalid argument, ", K(ret), K(macro_id), K(*this));
  } else {
    ObBucketHashWLockGuard lock_guard(lock_, macro_id.hash());
    if (OB_FAIL(macro_block_info_.get(macro_id, block_info))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        block_info.ref_cnt_ = 0;
        ret = OB_SUCCESS;
      } else {
        STORAGE_LOG(WARN, "Fail to get macro block reference count, ", K(ret), K(macro_id), K(*this), K(lbt()));
      }
    }
    if (OB_SUCC(ret)) {
      block_info.ref_cnt_++;
      if (1 == block_info.ref_cnt_) {
        block_info.access_time_ = ObTimeUtility::current_time();
      }
      if (OB_FAIL(macro_block_info_.insert_or_update(macro_id, block_info))) {
        STORAGE_LOG(WARN,
            "Fail to insert or update macro block info, ",
            K(ret),
            K(macro_id),
            K(block_info),
            K(*this),
            K(lbt()));
      } else if ((1 == block_info.ref_cnt_) && OB_FAIL(OB_FILE_SYSTEM.link_block(*this, macro_id))) {
        // TODO: [OFS.BLOCK] remove singleton, need pass block mgr for ofs mode
        int tmp_ret = OB_SUCCESS;
        if (OB_SUCCESS != (tmp_ret = macro_block_info_.erase(macro_id))) {
          STORAGE_LOG(WARN, "Fail to erase macro block info, ", K(tmp_ret), K(macro_id), K(*this));
        }
        STORAGE_LOG(WARN, "Fail to link macro block, ", K(ret), K(macro_id), K(block_info), K(*this), K(lbt()));
      } else {
        STORAGE_LOG(
            DEBUG, "debug storage file ref_cnt: inc_ref", K(ret), K(macro_id), K(block_info), K(*this), K(lbt()));
      }
    }
  }
  return ret;
}

int ObStorageFile::dec_ref(const MacroBlockId& macro_id)
{
  int ret = OB_SUCCESS;
  BlockInfo block_info;
  if (OB_UNLIKELY(!macro_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(ERROR, "Invalid argument, ", K(ret), K(macro_id), K(*this));
  } else {
    ObBucketHashWLockGuard lock_guard(lock_, macro_id.hash());
    if (OB_FAIL(macro_block_info_.get(macro_id, block_info))) {
      STORAGE_LOG(WARN, "Fail to get macro block reference count, ", K(ret), K(macro_id), K(*this), K(lbt()));
    } else if (0 == block_info.ref_cnt_) {
      // BUG, should not happen
      ret = OB_ERR_SYS;
      STORAGE_LOG(
          ERROR, "macro block ref must not less than 0, fatal error", K(ret), K(macro_id), K(block_info), K(*this));
    } else {
      block_info.ref_cnt_--;
      if (0 == block_info.ref_cnt_) {
        // TODO: [OFS.BLOCK] remove singleton, need pass block mgr for ofs mode
        block_info.access_time_ = ObTimeUtility::current_time();
        if (OB_FAIL(OB_FILE_SYSTEM.unlink_block(*this, macro_id))) {
          STORAGE_LOG(WARN, "Fail to unlink macro block in file system, ", K(ret), K(macro_id), K(*this), K(lbt()));
        } else if (OB_FAIL(macro_block_info_.erase(macro_id))) {
          STORAGE_LOG(
              ERROR, "Fail to erase macro block info, fatal error", K(ret), K(macro_id), K(block_info), K(*this));
        } else {
          ObTaskController::get().allow_next_syslog();
          STORAGE_LOG(INFO, "ref cnt is 0, and succeed to unlink block, ", K(macro_id), K(*this));
        }
      } else if (OB_FAIL(macro_block_info_.insert_or_update(macro_id, block_info))) {
        STORAGE_LOG(ERROR, "Fail to insert or update macro block info, ", K(ret), K(macro_id), K(block_info), K(*this));
      }
      STORAGE_LOG(DEBUG, "debug storage file ref_cnt: dec_ref", K(ret), K(macro_id), K(block_info), K(*this), K(lbt()));
    }
  }
  return ret;
}

void ObStorageFile::enable_mark_and_sweep()
{
  // nothing to do.
}

void ObStorageFile::disable_mark_and_sweep()
{
  // nothing to do.
}

bool ObStorageFile::get_mark_and_sweep_status()
{
  return true;  // mark_and_sweep is open by default.
}

ObStoreFileSystem::ObStoreFileSystem() : super_block_(), is_inited_(false)
{
  partition_service_ = &(storage::ObPartitionService::get_instance());
}

int ObStoreFileSystem::init(const ObStorageEnv& storage_env, storage::ObPartitionService& partition_service)
{
  UNUSEDx(storage_env, partition_service);
  return OB_NOT_SUPPORTED;
}

void ObStoreFileSystem::destroy()
{}

int ObStoreFileSystem::async_write(const ObStoreFileWriteInfo& write_info, common::ObIOHandle& io_handle)
{
  UNUSEDx(write_info, io_handle);
  return OB_NOT_SUPPORTED;
}

int ObStoreFileSystem::async_read(const ObStoreFileReadInfo& read_info, common::ObIOHandle& io_handle)
{
  UNUSEDx(read_info, io_handle);
  return OB_NOT_SUPPORTED;
}

int ObStoreFileSystem::write_server_super_block(const ObServerSuperBlock& super_block)
{
  super_block_ = super_block;
  return OB_SUCCESS;
}

int ObStoreFileSystem::read_server_super_block(ObServerSuperBlock& super_block)
{
  UNUSEDx(super_block);
  return OB_NOT_SUPPORTED;
}

int ObStoreFileSystem::fsync()
{
  return OB_NOT_SUPPORTED;
}

int64_t ObStoreFileSystem::get_total_data_size() const
{
  return -1;
}

int ObStoreFileSystem::free_file(const ObStoreFileCtx* ctx)
{
  UNUSEDx(ctx);
  return OB_NOT_SUPPORTED;
}

int ObStoreFileSystem::init_file_ctx(const ObStoreFileType& file_type, blocksstable::ObStoreFileCtx& file_ctx) const
{
  UNUSEDx(file_type, file_ctx);
  return OB_NOT_SUPPORTED;
}

int ObStoreFileSystem::add_disk(const ObString& diskgroup_name, const ObString& disk_path, const ObString& alias_name)
{
  int ret = OB_NOT_SUPPORTED;
  FLOG_ERROR("not support add disk", K(ret), K(diskgroup_name), K(disk_path), K(alias_name));
  return ret;
}

int ObStoreFileSystem::drop_disk(const ObString& diskgroup_name, const ObString& alias_name)
{
  int ret = OB_NOT_SUPPORTED;
  FLOG_ERROR("not support add disk", K(ret), K(diskgroup_name), K(alias_name));
  return ret;
}

ObDiskStat::ObDiskStat() : disk_idx_(-1), install_seq_(-1), create_ts_(-1), finish_ts_(-1), percent_(-1)
{
  alias_name_[0] = '\0';
  status_ = "";
}

ObDiskStats::ObDiskStats() : disk_stats_(), data_num_(0), parity_num_(0)
{}

void ObDiskStats::reset()
{
  disk_stats_.reset();
  data_num_ = 0;
  parity_num_ = 0;
}

int ObStoreFileSystem::get_disk_status(ObDiskStats& disk_stats)
{
  int ret = OB_SUCCESS;
  disk_stats.reset();
  return ret;
}

bool ObStoreFileSystem::is_disk_full() const
{
  return false;
}

int ObStoreFileSystem::alloc_file(ObStorageFile*& file)
{
  UNUSED(file);
  return OB_NOT_SUPPORTED;
}

int ObStoreFileSystem::free_file(ObStorageFile*& file)
{
  UNUSED(file);
  return OB_NOT_SUPPORTED;
}

int ObStoreFileSystem::unlink_block(const ObStorageFile& file, const MacroBlockId& macro_id)
{
  UNUSED(file);
  UNUSED(macro_id);
  return OB_NOT_SUPPORTED;
}

int ObStoreFileSystem::link_block(const ObStorageFile& file, const MacroBlockId& macro_id)
{
  UNUSED(file);
  UNUSED(macro_id);
  return OB_NOT_SUPPORTED;
}

int ObStoreFileSystem::link_blocks(const ObStorageFile& src_file,
    const common::ObIArray<MacroBlockId>& src_macro_block_ids, ObStorageFile& dest_file,
    ObMacroBlocksHandle& dest_macro_blocks_handle)
{
  UNUSEDx(src_file, dest_file, src_macro_block_ids, dest_macro_blocks_handle);
  return OB_NOT_SUPPORTED;
}

int ObStoreFileSystem::start()
{
  return OB_NOT_SUPPORTED;
}

void ObStoreFileSystem::stop()
{}

void ObStoreFileSystem::wait()
{}

int64_t ObStoreFileSystem::get_macro_block_size() const
{
  return super_block_.get_macro_block_size();
}

int64_t ObStoreFileSystem::get_total_macro_block_count() const
{
  return super_block_.get_total_macro_block_count();
}

int64_t ObStoreFileSystem::get_free_macro_block_count() const
{
  return -1;
}

int64_t ObStoreFileSystem::get_used_macro_block_count() const
{
  return get_total_macro_block_count() - get_free_macro_block_count();
}

int ObStoreFileSystem::get_marker_status(ObMacroBlockMarkerStatus& status)
{
  UNUSED(status);
  return OB_SUCCESS;
}

int ObStoreFileSystem::get_macro_block_info(
    const ObTenantFileKey& file_key, const MacroBlockId& macro_block_id, ObMacroBlockInfo& macro_block_info)
{
  UNUSED(file_key);
  UNUSED(macro_block_id);
  UNUSED(macro_block_info);
  return OB_SUCCESS;
}

int ObStoreFileSystem::report_bad_block(
    const MacroBlockId& macro_block_id, const int64_t error_type, const char* error_msg, const char* file_path)
{
  UNUSED(macro_block_id);
  UNUSED(error_type);
  UNUSED(error_msg);
  UNUSED(file_path);
  return OB_SUCCESS;
}

int ObStoreFileSystem::get_bad_block_infos(common::ObArray<ObBadBlockInfo>& bad_block_infos)
{
  UNUSED(bad_block_infos);
  return OB_SUCCESS;
}

int ObStoreFileSystem::resize_file(const int64_t new_data_file_size, const int64_t new_data_file_disk_percentage)
{
  int ret = OB_NOT_SUPPORTED;
  UNUSEDx(new_data_file_size, new_data_file_disk_percentage);
  LOG_WARN("resize file is not supported in current file system", K(ret));
  return ret;
}

ObStoreFileSystem* ObStoreFileSystemWrapper::fs_instance_ = NULL;

/*static*/ ObStoreFileSystem& ObStoreFileSystemWrapper::get_instance()
{
  static ObStoreFileSystem fake_fs;
  ObStoreFileSystem* fs = nullptr;
  if (OB_ISNULL(fs_instance_)) {
    LOG_ERROR("store file system not init");
    fs = &fake_fs;
  } else {
    fs = fs_instance_;
  }
  return *fs;
}

/*static*/ int ObStoreFileSystemWrapper::init(
    const ObStorageEnv& storage_env, storage::ObPartitionService& partition_service)
{
  int ret = OB_SUCCESS;
  static ObLocalFileSystem local_fs;
  static ObRaidFileSystem raid_fs;
  ObStoreFileSystem* store_file_system = nullptr;
  bool is_raid = false;

  LOG_INFO("start to create store file system", K(storage_env));
  if (OB_ISNULL(storage_env.data_dir_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(storage_env));
  } else if ('/' != storage_env.data_dir_[0] && '.' != storage_env.data_dir_[0]) {
    ret = OB_IO_ERROR;
    LOG_ERROR("unknown storage type, not support", K(ret), "data_dir", storage_env.data_dir_);
  } else if (OB_FAIL(ObRaidDiskUtil::is_raid(storage_env.sstable_dir_, is_raid))) {
    LOG_WARN("failed to check has raid disk", K(ret), "sstable_dir_", storage_env.sstable_dir_);
  } else if (!is_raid) {
    store_file_system = &local_fs;
    LOG_INFO("use local file system");
  } else {
    store_file_system = &raid_fs;
    LOG_INFO("use raid file system");
  }
  if (OB_SUCC(ret) && OB_NOT_NULL(store_file_system)) {
    if (OB_FAIL(store_file_system->init(storage_env, partition_service))) {
      LOG_WARN("fail to init store file system", K(ret), K(storage_env));
    }
  }

  if (OB_FAIL(ret) && OB_NOT_NULL(store_file_system)) {
    destroy();
  }
  LOG_INFO("finish to init store file system", K(ret), K(storage_env), KP(store_file_system));

  fs_instance_ = store_file_system;

  return ret;
}

/*static*/ void ObStoreFileSystemWrapper::destroy()
{
  if (OB_NOT_NULL(fs_instance_)) {
    fs_instance_->destroy();
    fs_instance_ = nullptr;
    LOG_INFO("destroy store file system");
  }
}
}  // namespace blocksstable
}  // namespace oceanbase
