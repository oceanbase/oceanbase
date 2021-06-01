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
#include "ob_raid_file_system.h"

#include "lib/file/file_directory_utils.h"
#include "lib/ec/ob_erasure_code_isa.h"
#include "share/ob_force_print_log.h"
#include "observer/ob_server_struct.h"
#include "ob_macro_block_meta_mgr.h"
#include "ob_store_file.h"

using namespace oceanbase;
using namespace common;
using namespace share;
using namespace storage;
using namespace blocksstable;

const int64_t ObRaidCommonHeader::VERSION_1 = 1;
const int64_t ObRaidCommonHeader::HEADER_SIZE = 4096;  // must same with DIO_READ_ALIGN_SIZE

const int64_t RAID_FORMAT_VERSION_1 = 1;
const int64_t DISK_MASTER_SUPER_BLOCK_OFFSET = 0;
const int64_t DISK_BACKUP_SUPER_BLOCK_OFFSET = 1 * OB_DEFAULT_MACRO_BLOCK_SIZE;        // 2m
const int64_t DISK_MACRO_BLOCK_START_OFFSET = 2 * OB_DEFAULT_MACRO_BLOCK_SIZE;         // 4m
const int64_t SSD_MIN_STRIP_SIZE = 64 * 1024 + ObRaidCommonHeader::HEADER_SIZE;        // 64 + 4KB
const int64_t NONE_SSD_MIN_STRIP_SIZE = 512 * 1024 + ObRaidCommonHeader::HEADER_SIZE;  // 512K + 4B

ObRaidCommonHeader::ObRaidCommonHeader()
{
  static_assert(HEADER_SIZE == sizeof(ObRaidCommonHeader), "ObRaidCommonHeader size not DIO READ ALIGN");
  MEMSET(this, 0, ObRaidCommonHeader::HEADER_SIZE);
  magic_ = RAID_STRIP_HEADER_MAGIC;
  version_ = VERSION_1;
  header_length_ = ObRaidCommonHeader::HEADER_SIZE;
}

bool ObRaidCommonHeader::is_valid() const
{
  return RAID_STRIP_HEADER_MAGIC == magic_ && VERSION_1 == version_ && HEADER_SIZE == header_length_ &&
         header_length_ == ObRaidCommonHeader::HEADER_SIZE && data_size_ > 0;
}

int64_t ObRaidCommonHeader::get_header_checksum() const
{
  int64_t checksum = 0;

  checksum = ob_crc64(checksum, &magic_, sizeof(magic_));
  checksum = ob_crc64(checksum, &version_, sizeof(version_));
  checksum = ob_crc64(checksum, &header_length_, sizeof(header_length_));
  checksum = ob_crc64(checksum, &data_size_, sizeof(data_size_));
  checksum = ob_crc64(checksum, &data_checksum_, sizeof(data_checksum_));
  checksum = ob_crc64(checksum, &data_timestamp_, sizeof(data_timestamp_));
  checksum = ob_crc64(checksum, reserved_, sizeof(reserved_));

  return checksum;
}

int ObRaidCommonHeader::set_checksum(const void* buf, const int64_t len)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(buf) || len < 0) {  // allow len == 0
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(len), KP(buf));
  } else {
    data_size_ = len;
    data_checksum_ = ob_crc64(buf, len);
    data_timestamp_ = ObTimeUtility::current_time();
    header_checksum_ = get_header_checksum();
  }
  return ret;
}

int ObRaidCommonHeader::check_checksum(const void* buf, const int64_t len) const
{
  int ret = OB_SUCCESS;
  int64_t header_checksum = get_header_checksum();

  if (OB_ISNULL(buf) || len < 0 || len != data_size_) {  // allow len == 0
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), KP(buf), K(len), K_(data_size));
  } else if (header_checksum != header_checksum_) {
    ret = OB_CHECKSUM_ERROR;
    LOG_ERROR("strip header checksum not match", K(ret), K(header_checksum), K(*this));
  } else {
    const int64_t data_checksum = ob_crc64(buf, len);
    if (data_checksum != data_checksum_) {
      ret = OB_CHECKSUM_ERROR;
      LOG_ERROR("data checksum error", K(ret), K(data_checksum), K(*this));
    }
  }
  return ret;
}

ObDiskFileInfo::ObDiskFileInfo()
    : disk_id_(),
      status_(MAX),
      create_ts_(0),
      rebuild_finish_ts_(0),
      sstable_path_(sizeof(sstable_path_buf_), 0, sstable_path_buf_),
      disk_name_(sizeof(disk_name_buf_), 0, disk_name_buf_)
{
  sstable_path_buf_[0] = '\0';
  disk_name_buf_[0] = '\0';
}

ObDiskFileInfo::ObDiskFileInfo(const ObDiskFileInfo& other)
{
  *this = other;
}

bool ObDiskFileInfo::is_valid() const
{
  return disk_id_.is_valid() && status_ >= 0 && status_ < MAX && !sstable_path_.empty() && !disk_name_.empty();
}

bool ObDiskFileInfo::operator==(const ObDiskFileInfo& other) const
{
  const bool is_same = disk_id_ == other.disk_id_ && status_ == other.status_ && create_ts_ == other.create_ts_ &&
                       rebuild_finish_ts_ == other.rebuild_finish_ts_ &&
                       0 == sstable_path_.compare(other.sstable_path_) && 0 == disk_name_.compare(other.disk_name_);
  if (!is_same) {
    LOG_INFO("not same",
        K(*this),
        K(other),
        "sstable_path_length",
        sstable_path_.length(),
        "disk_name_length",
        disk_name_.length(),
        "other_sstable_path_length",
        other.sstable_path_.length(),
        "other_disk_name_length",
        other.disk_name_.length());
  }
  return is_same;
}

ObDiskFileInfo& ObDiskFileInfo::operator=(const ObDiskFileInfo& r)
{
  disk_id_ = r.disk_id_;
  status_ = r.status_;
  create_ts_ = r.create_ts_;
  rebuild_finish_ts_ = r.rebuild_finish_ts_;
  MEMCPY(sstable_path_buf_, r.sstable_path_buf_, common::MAX_PATH_SIZE);
  sstable_path_ = ObString(common::MAX_PATH_SIZE, r.sstable_path_.length(), sstable_path_buf_);
  MEMCPY(disk_name_buf_, r.disk_name_buf_, common::MAX_PATH_SIZE);
  disk_name_ = ObString(common::MAX_PATH_SIZE, r.disk_name_.length(), disk_name_buf_);

  return *this;
}

int ObDiskFileInfo::set_disk_path(const common::ObString& sstable_path, const common::ObString& disk_name)
{
  int ret = OB_SUCCESS;

  if (sstable_path.empty() || disk_name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(sstable_path), K(disk_name));
  }

  if (OB_SUCC(ret)) {
    sstable_path_.assign_buffer(sstable_path_buf_, sizeof(sstable_path_buf_));
    int64_t write_n = sstable_path_.write(sstable_path.ptr(), static_cast<int32_t>(sstable_path.length()));
    if (write_n != sstable_path.length()) {
      ret = OB_ERR_SYS;
      LOG_WARN("failed to copy sstable path", K(ret), K(write_n), "expect_n", sstable_path.length());
    }
  }

  if (OB_SUCC(ret)) {
    disk_name_.assign_buffer(disk_name_buf_, sizeof(disk_name_buf_));
    int64_t write_n = disk_name_.write(disk_name.ptr(), static_cast<int32_t>(disk_name.length()));
    if (write_n != disk_name.length()) {
      ret = OB_ERR_SYS;
      LOG_WARN("failed to copy disk name", K(ret), K(write_n), "expect_n", disk_name.length());
    }
  }
  return ret;
}

const char* ObDiskFileInfo::get_status_str() const
{
  const char* str = "";

  switch (status_) {
    case INIT: {
      str = "INIT";
      break;
    }
    case REBUILD: {
      str = "REBUILD";
      break;
    }
    case NORMAL: {
      str = "NORMAL";
      break;
    }
    case ERROR: {
      str = "ERROR";
      break;
    }
    case DROP: {
      str = "DROP";
      break;
    }
    case MAX: {
      str = "MAX";
      break;
    }
    default: {
      str = "UNKNOWN";
    }
  }

  return str;
}

OB_SERIALIZE_MEMBER(ObDiskFileInfo, disk_id_, status_, create_ts_, rebuild_finish_ts_, sstable_path_, disk_name_);

bool ObDiskFileStatus::is_valid() const
{
  return fd_ >= 0 && info_.is_valid();
}

int ObDiskFileStatus::get_disk_fd(ObDiskFd& fd) const
{
  int ret = OB_SUCCESS;

  fd.disk_id_ = info_.disk_id_;
  fd.fd_ = fd_;
  return ret;
}

ObRaidFileInfo::ObRaidFileInfo()
{
  reset();
}

bool ObRaidFileInfo::is_valid() const
{
  return format_version_ > 0 && rebuild_seq_ > 0 && rebuild_ts_ > 0 && bootstrap_ts_ > 0 && blocksstable_size_ > 0 &&
         strip_size_ > ObRaidCommonHeader::HEADER_SIZE && src_data_num_ > 0 && parity_num_ >= 0 && disk_count_ > 0 &&
         total_macro_block_count_ > 0 && macro_block_size_ > 0;
}

void ObRaidFileInfo::reset()
{
  format_version_ = OB_RAID_FILE_FORMAT_VERSION_1;
  rebuild_seq_ = -1;
  rebuild_ts_ = -1;
  bootstrap_ts_ = -1;
  blocksstable_size_ = -1;
  strip_size_ = -1;
  src_data_num_ = -1;
  parity_num_ = -1;
  disk_count_ = -1;
  total_macro_block_count_ = -1;
  macro_block_size_ = 0;
}

bool ObRaidFileInfo::equals(const ObRaidFileInfo& other) const
{
  const bool is_same = format_version_ == other.format_version_ && rebuild_seq_ == other.rebuild_seq_ &&
                       rebuild_ts_ == other.rebuild_ts_ && bootstrap_ts_ == other.bootstrap_ts_ &&
                       blocksstable_size_ == other.blocksstable_size_ && strip_size_ == other.strip_size_ &&
                       src_data_num_ == other.src_data_num_ && parity_num_ == other.parity_num_ &&
                       disk_count_ == other.disk_count_ && total_macro_block_count_ == other.total_macro_block_count_ &&
                       macro_block_size_ == other.macro_block_size_;
  return is_same;
}

OB_SERIALIZE_MEMBER(ObRaidFileInfo, format_version_, rebuild_seq_, rebuild_ts_, bootstrap_ts_, blocksstable_size_,
    strip_size_, src_data_num_, parity_num_, disk_count_, total_macro_block_count_, macro_block_size_);

ObDiskFileSuperBlock::ObDiskFileSuperBlock() : disk_idx_(OB_INVALID_DISK_ID), info_(), disk_info_()
{}

void ObDiskFileSuperBlock::reset()
{
  disk_idx_ = OB_INVALID_DISK_ID;
  info_.reset();
  disk_info_.reset();
}

bool ObDiskFileSuperBlock::is_valid() const
{
  return disk_idx_ >= 0 && disk_idx_ < common::OB_MAX_DISK_NUMBER && disk_idx_ < info_.disk_count_ &&
         disk_info_.count() == info_.disk_count_ && disk_idx_ == disk_info_.at(disk_idx_).disk_id_.disk_idx_ &&
         info_.is_valid();
}

bool ObDiskFileSuperBlock::equals(const ObDiskFileSuperBlock& other) const
{
  return info_.equals(other.info_) && is_array_equal(disk_info_, other.disk_info_);
}

OB_SERIALIZE_MEMBER(ObDiskFileSuperBlock, disk_idx_, info_, disk_info_);

ObRaidStripLocation::ObRaidStripLocation()
    : is_inited_(false),
      block_index_(-1),
      block_offset_(-1),
      strip_num_(-1),
      strip_size_(-1),
      strip_payload_size_(-1),
      disk_count_(-1),
      blocksstable_size_(-1),
      include_header_(true),
      strip_skewing_step_(-1),
      cur_idx_(-1),
      has_next_(false),
      strip_infos_()
{}

ObRaidStripLocation::StripInfo::StripInfo() : disk_idx_(-1), offset_(-1), strip_idx_(-1)
{}

int ObRaidStripLocation::init(
    const int64_t block_index, const int64_t block_offset, const ObRaidFileInfo& info, const bool include_header)
{
  int ret = OB_SUCCESS;
  const int64_t strip_num = info.src_data_num_ + info.parity_num_;
  const int64_t strip_size = info.strip_size_;
  const int64_t disk_count = info.disk_count_;
  const int64_t blocksstable_size = info.blocksstable_size_;
  const int64_t total_strip_index = block_index * strip_num;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("not opened", K(ret));
  } else if (block_index < 0 || block_offset < 0 || block_offset > strip_size * strip_num || strip_num <= 0 ||
             strip_size <= ObRaidCommonHeader::HEADER_SIZE || disk_count <= 0 ||
             blocksstable_size < DISK_MACRO_BLOCK_START_OFFSET + strip_size * strip_num) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args",
        K(ret),
        K(block_index),
        K(block_offset),
        K(strip_num),
        K(strip_size),
        K(disk_count),
        K(blocksstable_size));
  } else if (OB_FAIL(strip_infos_.prepare_allocate(strip_num))) {
    LOG_WARN("failed to prepare_allocate strip infos", K(ret), K(strip_num));
  } else {
    block_index_ = block_index;
    block_offset_ = block_offset;
    strip_num_ = strip_num;
    strip_size_ = strip_size;
    strip_payload_size_ = strip_size_ - ObRaidCommonHeader::HEADER_SIZE;
    disk_count_ = disk_count;
    blocksstable_size_ = blocksstable_size;
    include_header_ = include_header;
    strip_skewing_step_ = block_index % strip_num;
    cur_idx_ = -1;

    const int64_t cur_strip_idx = block_offset / strip_payload_size_;
    for (int64_t i = 0; OB_SUCC(ret) && i < strip_num; ++i) {
      StripInfo& strip_info = strip_infos_.at(i);
      strip_info.disk_idx_ = (total_strip_index + i) % disk_count;
      strip_info.offset_ = DISK_MACRO_BLOCK_START_OFFSET + (total_strip_index + i) / disk_count * strip_size;
      strip_info.strip_idx_ = (i + strip_skewing_step_) % strip_num;
      if (strip_info.strip_idx_ == cur_strip_idx) {
        cur_idx_ = i;
      }
    }

    if (cur_idx_ < 0) {
      ret = OB_ERR_SYS;
      LOG_ERROR("strip not found", K(ret), K(block_index), K(block_offset), K(info), K(include_header), K(*this));
    } else {
      is_inited_ = true;
      has_next_ = true;
      LOG_DEBUG("succeed to init strip location", K(block_index), K(block_offset), K(info), K(*this));
    }
  }
  return ret;
}

int ObRaidStripLocation::next_strip(int64_t& strip_idx, int64_t& disk_id, int64_t& offset, int64_t& strip_left_size)
{
  int ret = OB_SUCCESS;
  strip_idx = -1;
  disk_id = -1;
  offset = -1;
  strip_left_size = -1;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (!has_next_) {
    ret = OB_ITER_END;
  } else {
    const StripInfo& strip_info = strip_infos_[cur_idx_];

    strip_idx = strip_info.strip_idx_;
    disk_id = strip_info.disk_idx_;

    const int64_t skip_offset = block_offset_ - strip_payload_size_ * strip_idx;
    if (skip_offset > 0) {
      offset = strip_info.offset_ + skip_offset;
      strip_left_size = strip_size_ - skip_offset;
    } else {
      offset = strip_info.offset_;
      strip_left_size = strip_size_;
    }

    if (!include_header_) {
      offset += ObRaidCommonHeader::HEADER_SIZE;
      strip_left_size -= ObRaidCommonHeader::HEADER_SIZE;
    }

    if (strip_idx == strip_num_ - 1) {
      has_next_ = false;
    }
    ++cur_idx_;
    if (cur_idx_ >= strip_num_) {
      cur_idx_ = 0;
    }

    LOG_DEBUG("get next", K(strip_idx), K(disk_id), K(offset), K(strip_left_size), K(cur_idx_), K(has_next_));
  }
  return ret;
}

ObRaidFileSystem::ObRaidFileSystem()
    : ObStoreFileSystem(),
      is_opened_(false),
      mutex_(),
      lock_(),
      storage_env_(NULL),
      raid_status_(),
      super_block_buf_holder_(),
      write_mgr_(),
      timer_(),
      rebuild_task_(*this),
      file_allocator_(),
      server_root_(nullptr)
{}

ObRaidFileSystem::~ObRaidFileSystem()
{}

int ObRaidFileSystem::init(const ObStorageEnv& storage_env, ObPartitionService& partition_service)
{
  int ret = OB_SUCCESS;
  {
    lib::ObMutexGuard mutex_guard(mutex_);
    SpinWLockGuard lock_guard(lock_);

    if (is_inited_) {
      ret = OB_INIT_TWICE;
      LOG_WARN("cannot init twice", K(ret));
    } else if (!storage_env.is_valid()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid args", K(ret), K(storage_env));
    } else if (OB_FAIL(ObECCacheManager::get_ec_cache_mgr().init())) {
      LOG_WARN("failed to init ObECCacheManager", K(ret));
    } else if (OB_FAIL(super_block_buf_holder_.init(OB_DEFAULT_MACRO_BLOCK_SIZE))) {
      LOG_WARN("failed to init super_block_buf_holder_", K(ret));
    } else if (OB_FAIL(write_mgr_.init())) {
      LOG_WARN("failed to init write mgr", K(ret));
    } else if (OB_FAIL(timer_.init("RaidFile"))) {
      LOG_WARN("failed to init timer", K(ret));
    } else {
      partition_service_ = &partition_service;
      is_inited_ = true;
      is_opened_ = false;
      storage_env_ = &storage_env;
    }
  }

  if (OB_SUCC(ret)) {
    bool is_exist = false;
    if (OB_FAIL(this->open(is_exist))) {
      STORAGE_LOG(WARN, "Fail to local block_file, ", K(ret));
    } else if (is_exist) {
      if (OB_FAIL(this->read_server_super_block(super_block_))) {
        STORAGE_LOG(WARN, "Fail to load super block, ", K(ret));
      } else {
        LOG_INFO("Success to read super block, ", K(super_block_));
      }
    } else {
      // empty file system, init it
      if (OB_FAIL(
              super_block_.format_startup_super_block(storage_env.default_block_size_, this->get_total_data_size()))) {
        STORAGE_LOG(WARN, "Fail to format super block, ", K(ret));
      } else if (OB_FAIL(write_server_super_block(super_block_))) {
        STORAGE_LOG(WARN, "Fail to write super block, ", K(ret));
      } else {
        STORAGE_LOG(INFO, "Success to format super block, ", K(super_block_));
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(OB_STORE_FILE.init(storage_env, this))) {
      LOG_WARN("init store file failed, ", K(ret));
      is_inited_ = false;
    }
  }
  return ret;
}

int ObRaidFileSystem::open(bool& is_formated)
{
  int ret = OB_SUCCESS;
  bool has_free_disk = false;
  int64_t min_total_space = 0;
  int64_t min_free_space = 0;
  common::ObSEArray<ObScanDiskInfo, OB_MAX_DISK_NUMBER> disk_status;
  lib::ObMutexGuard mutex_guard(mutex_);
  SpinWLockGuard lock_guard(lock_);

  is_formated = false;

  FLOG_INFO("start open raid system");

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (is_opened_) {
    ret = OB_OPEN_TWICE;
    LOG_WARN("cannot open twice", K(ret));
  } else if (OB_FAIL(ObRaidDiskUtil::scan_store_dir(storage_env_->sstable_dir_,
                 disk_status,
                 has_free_disk,
                 is_formated,
                 min_total_space,
                 min_free_space))) {
    LOG_WARN("failed to scan sstable dir", K(ret));
  } else if (is_formated) {
    ObTaskController::get().allow_next_syslog();
    LOG_INFO("formated raid system, load raid", "sstable_dir", storage_env_->sstable_dir_);
    if (OB_FAIL(load_raid(disk_status))) {
      LOG_WARN("failed to load raid", K(ret));
    } else if (has_free_disk) {
      // TODO(): need support add free disk
      FLOG_ERROR("cannot add free disk now, will fix it later", K(disk_status));
    }
  } else {
    ObTaskController::get().allow_next_syslog();
    LOG_INFO("first bootstrap, need init raid", "sstable_dir", storage_env_->sstable_dir_);
    if (OB_FAIL(init_raid(min_total_space, min_free_space, disk_status))) {
      LOG_WARN("failed to init raid", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (raid_status_.get_write_num() > MAX_IO_BATCH_NUM) {
      ret = OB_ERR_SYS;
      LOG_ERROR("raid write num is too large", K(ret), K_(raid_status));
    } else if (OB_FAIL(timer_.start())) {
      LOG_WARN("failed to start timer", K(ret));
    } else {
      is_opened_ = true;
      ObTaskController::get().allow_next_syslog();
      LOG_INFO("succeed to open raid file system", K_(raid_status));
      if (OB_FAIL(timer_.schedule_repeate_task_immediately(rebuild_task_, 10 * 1000 * 1000 /*10s*/))) {
        LOG_WARN("failed to schedule repeat task", K(ret));
      } else if (OB_FAIL(alloc_file(server_root_))) {
        STORAGE_LOG(WARN, "fail to alloc server root", K(ret));
      } else if (OB_ISNULL(server_root_)) {
        ret = OB_ERR_SYS;
        STORAGE_LOG(WARN, "error sys, server root must not be null", K(ret));
      } else if (OB_FAIL(server_root_->init(GCTX.self_addr_, ObStorageFile::FileType::SERVER_ROOT))) {
        STORAGE_LOG(WARN, "init server_root fail", K(ret));
      } else {
        svr_root_file_with_ref_.file_ = server_root_;
        svr_root_file_with_ref_.inc_ref();
        server_root_handle_.set_storage_file_with_ref(svr_root_file_with_ref_);
      }
    }
  }
  return ret;
}

void ObRaidFileSystem::destroy()
{
  int ret = OB_SUCCESS;
  lib::ObMutexGuard mutex_guard(mutex_);
  SpinWLockGuard lock_guard(lock_);

  ObTaskController::get().allow_next_syslog();
  LOG_INFO("start close raid file system", K_(raid_status));
  is_opened_ = false;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else {
    for (int64_t disk_idx = 0; OB_SUCC(ret) && disk_idx < raid_status_.disk_status_.count(); ++disk_idx) {
      ObDiskFileStatus& disk_status = raid_status_.disk_status_.at(disk_idx);
      if (disk_status.fd_ >= 0) {
        if (OB_FAIL(close_disk(disk_status))) {
          LOG_ERROR("failed to close disk", K(ret), K(disk_status));
        }
      }
    }
  }
  is_inited_ = false;
  OB_STORE_FILE.destroy();
  if (nullptr != server_root_) {
    free_file(server_root_);
    server_root_ = nullptr;
  }
  raid_status_.reset();
  ObECCacheManager::get_ec_cache_mgr().destroy();
  super_block_buf_holder_.reset();
  write_mgr_.destroy();
  timer_.destroy();
  is_inited_ = false;
}

ObStorageFileHandle& ObRaidFileSystem::get_server_root_handle()
{
  return server_root_handle_;
}

int ObRaidDiskUtil::is_raid(const char* sstable_dir, bool& is_raid)
{
  int ret = OB_SUCCESS;
  bool has_free_disk = false;
  int64_t min_total_space = 0;
  int64_t min_free_space = 0;
  common::ObSEArray<ObScanDiskInfo, OB_MAX_DISK_NUMBER> disk_status;
  bool is_formated = false;
  bool is_exist = false;

  is_raid = false;
  if (OB_ISNULL(sstable_dir)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), KP(sstable_dir));
  } else if (OB_FAIL(is_local_file_system_exist(sstable_dir, is_exist))) {
    LOG_WARN("failed to check is_local_file_system_exist", K(ret));
  } else if (is_exist) {
    is_raid = false;
    LOG_INFO("local file system is exist", K(sstable_dir));
  } else if (OB_FAIL(scan_store_dir(
                 sstable_dir, disk_status, has_free_disk, is_formated, min_total_space, min_free_space))) {
    LOG_WARN("failed to scan store dir", K(ret), K(*sstable_dir));
  } else if (!disk_status.empty()) {
    is_raid = true;
    LOG_INFO("is raid system", K(disk_status));
  }

  return ret;
}

int ObRaidDiskUtil::is_local_file_system_exist(const char* sstable_dir, bool& is_exist)
{
  int ret = OB_SUCCESS;
  char sstable_file_path[common::OB_MAX_FILE_NAME_LENGTH];
  is_exist = false;

  if (OB_ISNULL(sstable_dir)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), KP(sstable_dir));
  } else if (OB_FAIL(databuff_printf(
                 sstable_file_path, sizeof(sstable_file_path), "%s/%s", sstable_dir, BLOCK_SSTBALE_FILE_NAME))) {
    LOG_WARN("Failed to printf sstable_path", K(ret), K(sstable_dir));
  } else if (OB_FAIL(FileDirectoryUtils::is_exists(sstable_file_path, is_exist))) {
    LOG_WARN("failed to check file exist", K(ret), K(sstable_file_path));
  } else {
    LOG_INFO("local file system sstable file not exist", K(sstable_file_path), K(is_exist));
  }
  return ret;
}

int ObRaidDiskUtil::scan_store_dir(const char* sstable_dir, common::ObIArray<ObScanDiskInfo>& disk_status,
    bool& has_free_disk, bool& is_formated, int64_t& min_total_space, int64_t& min_free_space)
{
  int ret = OB_SUCCESS;
  int64_t disk_num = 0;
  struct ::dirent** disk_dirent = NULL;
  char sub_dir_path[common::OB_MAX_FILE_NAME_LENGTH];
  char sstable_file_path[common::OB_MAX_FILE_NAME_LENGTH];
  ObScanDiskInfo disk_info;
  bool is_dir = false;
  bool is_exist = false;
  int64_t dir_name = -1;
  int64_t total_space = 0;
  int64_t free_space = 0;
  has_free_disk = false;
  is_formated = false;
  min_total_space = INT64_MAX;
  min_free_space = INT64_MAX;

  if (OB_ISNULL(sstable_dir)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), KP(sstable_dir));
  } else {
    disk_num = ::scandir(sstable_dir, &disk_dirent, disk_name_filter, ::versionsort);
    if (disk_num < 0 || disk_num > OB_MAX_DISK_NUMBER) {
      ret = OB_STORE_DIR_ERROR;
      LOG_ERROR(
          "disk num in store dir is not valid", K(ret), K(disk_num), K(OB_MAX_DISK_NUMBER), K(sstable_dir), K(errno));
    }

    for (int64_t i = 0; i < disk_num; ++i) {
      if (OB_SUCC(ret)) {
        ObTaskController::get().allow_next_syslog();
        if (1 != sscanf(disk_dirent[i]->d_name, "%ld", &dir_name)) {
          LOG_WARN("skip not valid sub dir", K(i), "sub_dir_name", disk_dirent[i]->d_name);
        } else if (dir_name < 0 || dir_name >= OB_MAX_DISK_NUMBER) {
          ret = OB_STORE_DIR_ERROR;
          LOG_ERROR("disk name is not valid", K(ret), K(dir_name));
        } else if (OB_FAIL(build_sstable_dir(sstable_dir, dir_name, sub_dir_path, sizeof(sub_dir_path)))) {
          LOG_WARN("Failed to printf sub_dir_path", K(ret), K(sstable_dir), K(dir_name));
        } else if (OB_FAIL(
                       build_sstable_file_path(sstable_dir, dir_name, sstable_file_path, sizeof(sstable_file_path)))) {
          LOG_WARN("Failed to printf sstable path", K(ret), K(sstable_dir), K(dir_name));
        } else if (OB_FAIL(FileDirectoryUtils::is_directory(sub_dir_path, is_dir))) {
          LOG_WARN("failed to check is dir", K(ret), K(sub_dir_path));
        } else if (!is_dir) {
          LOG_WARN("skip not dir", K(i), K(sub_dir_path));
        } else if (OB_FAIL(FileDirectoryUtils::is_exists(sstable_file_path, is_exist))) {
          LOG_WARN("failed to check is sstable path exists", K(ret), K(sstable_file_path));
        } else if (OB_FAIL(FileDirectoryUtils::get_disk_space(sub_dir_path, total_space, free_space))) {
          LOG_WARN("failed to get disk free space", K(ret), K(sub_dir_path));
        } else {
          disk_info.disk_id_ = dir_name;
          disk_info.has_sstable_file_ = is_exist;
          if (!is_exist) {
            has_free_disk = true;
          } else {
            is_formated = true;
          }
          if (free_space < min_free_space) {
            min_free_space = free_space;
          }
          if (total_space < min_total_space) {
            min_total_space = total_space;
          }
          ObTaskController::get().allow_next_syslog();
          LOG_INFO("scan store dir",
              K(ret),
              K(i),
              K(is_formated),
              K(disk_info),
              K(has_free_disk),
              K(sub_dir_path),
              K(sstable_file_path),
              K(min_free_space),
              K(min_total_space),
              K(free_space),
              K(total_space));
          if (OB_FAIL(disk_status.push_back(disk_info))) {
            LOG_WARN("failed to add disk info", K(ret));
          }
        }
      }
      ::free(disk_dirent[i]);
    }
    ::free(disk_dirent);
  }
  return ret;
}

int ObRaidDiskUtil::disk_name_filter(const struct ::dirent* d)
{
  int return_code = 0;  // zero means need filter

  if (NULL != d && strlen(d->d_name) > 0) {
    return_code = 1;
    for (int64_t i = 0; i < strlen(d->d_name); ++i) {
      if (!isdigit(d->d_name[i])) {
        return_code = 0;
        break;
      }
    }
  }

  return return_code;
}

int ObRaidDiskUtil::build_sstable_dir(const char* sstable_dir, const int64_t dir_name, char* buf, int64_t buf_len)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(sstable_dir)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), KP(sstable_dir));
  } else if (OB_FAIL(databuff_printf(buf, buf_len, "%s/%ld", sstable_dir, dir_name))) {
    LOG_WARN("Failed to printf sub_dir_path", K(ret), K(sstable_dir), K(dir_name));
  }
  return ret;
}

int ObRaidDiskUtil::build_sstable_file_path(const char* sstable_dir, const int64_t dir_name, char* buf, int64_t buf_len)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(sstable_dir)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), KP(sstable_dir));
  } else if (OB_FAIL(databuff_printf(buf, buf_len, "%s/%ld/%s", sstable_dir, dir_name, BLOCK_SSTBALE_FILE_NAME))) {
    LOG_WARN("Failed to printf sstable_path", K(ret), K(sstable_dir), K(dir_name));
  }
  return ret;
}

int ObRaidFileSystem::init_raid(
    const int64_t min_total_space, const int64_t min_free_space, common::ObIArray<ObScanDiskInfo>& disk_status)
{
  int ret = OB_SUCCESS;
  ObRaidFileStatus raid_status;

  FLOG_INFO("start init raid", K(disk_status), K(min_total_space), K(min_free_space));
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (is_opened_) {
    ret = OB_OPEN_TWICE;
    LOG_WARN("cannot open twice", K(ret));
  } else if (OB_FAIL(raid_status.init_raid(*storage_env_, disk_status, min_total_space, min_free_space))) {
    LOG_WARN("failed to build raid super block",
        K(ret),
        K(disk_status),
        K(min_total_space),
        K(min_free_space),
        K(raid_status));
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < raid_status.info_.disk_count_; ++i) {
    ObDiskFileStatus& disk_status = raid_status.disk_status_.at(i);
    if (OB_FAIL(open_new_disk(raid_status.info_.blocksstable_size_, disk_status))) {
      LOG_WARN("Failed to open new disk", K(ret), K(i), K(disk_status));
    } else {
      disk_status.info_.status_ = ObDiskFileInfo::NORMAL;
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(write_raid_super_block(raid_status))) {
      LOG_WARN("failed to write raid super block", K(ret), K(raid_status));
    } else {
      raid_status_ = raid_status;
      FLOG_INFO("succeed to init raid", K_(raid_status));
    }
  }

  return ret;
}

int ObRaidFileSystem::open_new_disk(const int64_t blocksstable_size, ObDiskFileStatus& disk_status)
{
  int ret = OB_SUCCESS;
  char sstable_path[common::OB_MAX_FILE_NAME_LENGTH];
  ObDiskFd disk_fd;
  disk_fd.disk_id_ = disk_status.info_.disk_id_;
  disk_fd.disk_id_.install_seq_++;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (blocksstable_size <= 0 || disk_status.fd_ >= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid idx", K(ret), K(blocksstable_size), K(disk_status));
  } else if (OB_FAIL(databuff_printf(sstable_path,
                 sizeof(sstable_path),
                 "%.*s",
                 disk_status.info_.sstable_path_.length(),
                 disk_status.info_.sstable_path_.ptr()))) {
    LOG_WARN("failed to copy sstable path", K(ret));
  } else {
    if (ObDiskFileInfo::INIT != disk_status.info_.status_) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid disk staus", K(ret), K(disk_status));
    } else if ((disk_fd.fd_ = ::open(sstable_path,
                    O_CREAT | O_EXCL | O_DIRECT | O_RDWR | O_LARGEFILE,
                    S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH)) < 0) {
      ret = OB_IO_ERROR;
      STORAGE_LOG(ERROR, "open file error", K(ret), K(sstable_path), K(errno), KERRMSG);
    } else if (OB_FAIL(fallocate(disk_fd.fd_, 0 /*MODE*/, 0 /*offset*/, blocksstable_size))) {
      ret = OB_IO_ERROR;
      STORAGE_LOG(ERROR,
          "allocate file error",
          "sstable_path",
          disk_status.info_.sstable_path_,
          K(blocksstable_size),
          K(ret),
          K(errno),
          KERRMSG);
    } else if (OB_FAIL(ObIOManager::get_instance().add_disk(disk_fd,
                   ObDisk::DEFAULT_SYS_IO_PERCENT,
                   ObDisk::MAX_DISK_CHANNEL_CNT,
                   ObIOManager::DEFAULT_IO_QUEUE_DEPTH))) {
      LOG_WARN("failed to add disk", K(ret), K(disk_fd));
    }

    if (OB_SUCC(ret)) {
      disk_status.fd_ = disk_fd.fd_;
      disk_status.info_.disk_id_ = disk_fd.disk_id_;
      disk_status.info_.create_ts_ = ObTimeUtility::current_time();
      disk_status.info_.rebuild_finish_ts_ = 0;
      ObTaskController::get().allow_next_syslog();
      LOG_INFO("succeed to init new disk", K(blocksstable_size), K(disk_status));
    } else if (-1 != disk_fd.fd_) {
      ::close(disk_fd.fd_);
      disk_fd.fd_ = -1;
    }
  }
  return ret;
}

int ObRaidFileStatus::init_raid(const ObStorageEnv& storage_env, const common::ObIArray<ObScanDiskInfo>& disk_status,
    const int64_t min_total_space, const int64_t min_free_space)
{
  int ret = OB_SUCCESS;
  int64_t src_data_num = 0;
  int64_t parity_num = 0;
  int64_t strip_size = 0;
  const int64_t disk_count = disk_status.count();
  char sstable_path[common::MAX_PATH_SIZE];
  char disk_name[common::MAX_PATH_SIZE];

  if (info_.bootstrap_ts_ >= 0) {
    ret = OB_INIT_TWICE;
    LOG_WARN("cannot init twice", K(ret), K(*this));
  } else if (!storage_env.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(storage_env));
  } else if (storage_env.disk_avail_space_ > min_free_space || disk_status.empty()) {
    ret = OB_INVALID_ARGUMENT;
    ObTaskController::get().allow_next_syslog();
    LOG_ERROR("min_free_disk_space is not enough",
        K(ret),
        "specified disk space",
        storage_env.disk_avail_space_,
        K(disk_status),
        K(min_total_space),
        K(min_free_space));
  } else if (OB_FAIL(cal_raid_param(storage_env.redundancy_level_,
                 disk_count,
                 storage_env.disk_type_,
                 storage_env.default_block_size_,
                 src_data_num,
                 parity_num,
                 strip_size))) {
    LOG_WARN("failed to cal raid param", K(ret), K(disk_count), K(storage_env));
  } else if (OB_FAIL(disk_status_.prepare_allocate(disk_count))) {
    LOG_WARN("failed to prepare allocate disk status", K(ret), K(disk_count));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < disk_count; ++i) {
      ObDiskFileInfo& info = disk_status_.at(i).info_;
      info.disk_id_.disk_idx_ = i;
      info.disk_id_.install_seq_ = 0;
      info.status_ = ObDiskFileInfo::INIT;
      if (OB_FAIL(ObRaidDiskUtil::build_sstable_file_path(
              storage_env.sstable_dir_, i, sstable_path, sizeof(sstable_path)))) {
        LOG_WARN("Failed to printf sstable path", K(ret), "sstable_dir", storage_env.sstable_dir_, K(i));
      } else if (OB_FAIL(databuff_printf(disk_name, sizeof(disk_name), "%ld", i))) {
        LOG_WARN("failed to copy disk_name", K(ret), K(i));
      } else if (OB_FAIL(info.set_disk_path(ObString::make_string(sstable_path), ObString::make_string(disk_name)))) {
        LOG_WARN("failed to set disk path", K(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    info_.format_version_ = RAID_FORMAT_VERSION_1;
    info_.rebuild_seq_ = 1;
    info_.rebuild_ts_ = ObTimeUtility::current_time();
    info_.bootstrap_ts_ = ObTimeUtility::current_time();

    info_.blocksstable_size_ = storage_env.disk_avail_space_;
    if (0 == info_.blocksstable_size_) {
      info_.blocksstable_size_ = min_total_space * storage_env.datafile_disk_percentage_ / 100;
      ObTaskController::get().allow_next_syslog();
      LOG_INFO("data file size is not set, use percent",
          K(min_total_space),
          K(min_free_space),
          "datafile_disk_percentage",
          storage_env.datafile_disk_percentage_,
          "datafile_size",
          info_.blocksstable_size_);
    }

    info_.strip_size_ = strip_size;
    info_.src_data_num_ = src_data_num;
    info_.parity_num_ = parity_num;
    info_.disk_count_ = disk_count;
    info_.macro_block_size_ = storage_env.default_block_size_;

    const int64_t total_strip_num =
        (info_.blocksstable_size_ - DISK_MACRO_BLOCK_START_OFFSET) / strip_size * disk_count;
    info_.total_macro_block_count_ = total_strip_num / (src_data_num + parity_num);

    if (info_.blocksstable_size_ > min_free_space) {
      ret = OB_INVALID_ARGUMENT;
      ObTaskController::get().allow_next_syslog();
      LOG_ERROR("min_free_disk_space is not enough",
          K(ret),
          "blocksstable_size",
          info_.blocksstable_size_,
          K(disk_count),
          K(min_total_space),
          K(min_free_space));
    } else {
      is_inited_ = true;
      ObTaskController::get().allow_next_syslog();
      LOG_INFO("succeed to build raid status", K(*this), K(storage_env));
    }
  }

  return ret;
}

int ObRaidFileSystem::write_raid_super_block(const ObRaidFileStatus& raid_status)
{
  int ret = OB_SUCCESS;
  bool is_master_block = true;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (!raid_status.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(raid_status));
  } else {

    for (int64_t i = 0; OB_SUCC(ret) && i < raid_status.get_disk_count(); ++i) {
      if (OB_FAIL(write_disk_super_block(is_master_block, raid_status, i))) {
        LOG_WARN("Failed to write master disk super block", K(ret), K(i), K(raid_status));
      }
    }

    if (OB_SUCC(ret)) {
      is_master_block = false;
      for (int64_t i = 0; OB_SUCC(ret) && i < raid_status.get_disk_count(); ++i) {
        if (OB_FAIL(write_disk_super_block(is_master_block, raid_status, i))) {
          LOG_WARN("Failed to write slave disk super block", K(ret), K(i), K(raid_status));
        }
      }

      if (OB_FAIL(ret)) {
        FLOG_WARN("master raid super block has wrote success, skip backup super write error", K(ret), K(raid_status));
        ret = OB_SUCCESS;
      }
    }

    if (OB_SUCC(ret)) {
      FLOG_INFO("succeed to write raid super block", K(raid_status));
    }
  }

  return ret;
}

int ObRaidFileSystem::write_disk_super_block(
    const bool is_master_block, const ObRaidFileStatus& raid_status, const int64_t idx)
{
  int ret = OB_SUCCESS;
  ObDiskFd fd;
  int64_t pos = 0;
  common::ObArenaAllocator allocator;
  ObDiskFileSuperBlock disk_super_block;
  const int64_t offset = is_master_block ? DISK_MASTER_SUPER_BLOCK_OFFSET : DISK_BACKUP_SUPER_BLOCK_OFFSET;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (idx < 0 || idx >= OB_MAX_DISK_NUMBER) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(idx), K(raid_status));
  } else if (OB_FAIL(raid_status.get_fd(idx, fd))) {
    LOG_WARN("failed to get fd", K(ret), K(idx));
  } else if (OB_FAIL(raid_status.get_disk_super_block(idx, disk_super_block))) {
    LOG_WARN("failed to get disk super block", K(ret), K(idx), K(raid_status));
  } else if (ObDiskFileInfo::NORMAL != raid_status.disk_status_[idx].info_.status_ &&
             ObDiskFileInfo::REBUILD != raid_status.disk_status_[idx].info_.status_) {  // TODO(): move it
    LOG_WARN("skip write super block to not valid disk",
        K(ret),
        K(idx),
        "status",
        raid_status.disk_status_[idx].info_.status_,
        K_(raid_status));
  } else {
    const int64_t super_block_size = disk_super_block.get_serialize_size();
    const int64_t buf_size = upper_align(ObRaidCommonHeader::HEADER_SIZE + super_block_size, DIO_READ_ALIGN_SIZE);
    char* buf = NULL;
    char* super_block_buf = NULL;
    ObRaidCommonHeader* header = NULL;

    if (OB_ISNULL(buf = static_cast<char*>(allocator.alloc(buf_size + DIO_READ_ALIGN_SIZE)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc buf", K(ret), K(buf_size));
    } else {
      buf = upper_align_buf(buf, DIO_READ_ALIGN_SIZE);
      header = new (buf) ObRaidCommonHeader();
      super_block_buf = buf + ObRaidCommonHeader::HEADER_SIZE;
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(disk_super_block.serialize(super_block_buf, super_block_size, pos))) {
        LOG_WARN("failed to serialize disk super block", K(ret), K(buf_size), K(pos));
      } else if (OB_FAIL(header->set_checksum(super_block_buf, super_block_size))) {
        LOG_WARN("failed to set payload checksum", K(ret));
      } else if (buf_size != ob_pwrite(fd.fd_, buf, buf_size, offset)) {
        ret = OB_IO_ERROR;
        LOG_WARN(
            "failed to write disk super block", K(ret), K(buf_size), K(offset), K(fd), K(idx), K(disk_super_block));
      } else if (0 != ::fsync(fd.fd_)) {
        ret = OB_IO_ERROR;
        LOG_WARN("failed to fsync", K(ret), K(fd), K(idx), K(disk_super_block));
      } else {
        ObTaskController::get().allow_next_syslog();
        LOG_INFO("succeed to write disk super block", K(idx), K(is_master_block), K(disk_super_block), K(buf_size));
      }
    }
  }
  return ret;
}

int ObRaidFileSystem::load_raid(const common::ObIArray<ObScanDiskInfo>& disk_status)
{
  int ret = OB_SUCCESS;
  ObDiskFileSuperBlock disk_super_block;
  ObRaidFileStatus raid_status;

  LOG_INFO("start load raid");
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (is_opened_) {
    ret = OB_OPEN_TWICE;
    LOG_WARN("cannot open twice", K(ret));
  }

  if (OB_SUCC(ret)) {
    bool is_master_super_block = false;
    if (OB_FAIL(load_raid_super_block(disk_status, is_master_super_block, disk_super_block))) {
      LOG_WARN("failed to load master raid super block", K(ret));
      is_master_super_block = true;
      // try again
      if (OB_FAIL(load_raid_super_block(disk_status, is_master_super_block, disk_super_block))) {
        LOG_WARN("failed to load backup raid super block", K(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(build_raid_status(disk_super_block, raid_status))) {
      LOG_WARN("failed to build raid status", K(ret), K(disk_super_block));
    }
  }

  if (OB_SUCC(ret)) {
    raid_status_ = raid_status;
    FLOG_INFO("succeed to load raid", K_(raid_status));
  }
  return ret;
}

int ObRaidFileSystem::load_raid_super_block(const common::ObIArray<ObScanDiskInfo>& disk_status,
    const bool is_master_super_block, ObDiskFileSuperBlock& disk_super_block)
{
  int ret = OB_SUCCESS;
  ObDiskFileSuperBlock first_disk_super_block;
  ObDiskFileSuperBlock cur_disk_super_block;
  ObArray<int64_t> loaded_disk_array;
  disk_super_block.reset();

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < disk_status.count(); ++i) {
      const ObScanDiskInfo& info = disk_status.at(i);
      if (info.has_sstable_file_) {
        if (OB_FAIL(load_disk_super_block(info.disk_id_, is_master_super_block, cur_disk_super_block))) {
          LOG_WARN("failed to load disk super block", K(ret), K(info), K(is_master_super_block));
        } else if (OB_FAIL(loaded_disk_array.push_back(cur_disk_super_block.disk_idx_))) {
          LOG_WARN("failed to add disk id", K(ret));
        } else if (1 == loaded_disk_array.count()) {
          first_disk_super_block = cur_disk_super_block;
        } else if (!first_disk_super_block.equals(cur_disk_super_block)) {
          ret = OB_RAID_SUPER_BLOCK_NOT_MATCH;
          LOG_WARN("raid super block not match", K(ret), K(i), K(first_disk_super_block), K(cur_disk_super_block));
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    disk_super_block = first_disk_super_block;
    if (loaded_disk_array.empty() ||
        disk_super_block.info_.disk_count_ - loaded_disk_array.count() > disk_super_block.info_.parity_num_) {
      ret = OB_IO_ERROR;
      LOG_ERROR(
          "disk count not enough", K(ret), K(disk_super_block), K(loaded_disk_array), K(disk_status), K(*storage_env_));
    }
  }

  if (OB_SUCC(ret)) {
    FLOG_INFO("succeed to load_raid_super_block", K(is_master_super_block), K(disk_status), K(disk_super_block));
  }

  return ret;
}

int ObRaidFileSystem::load_disk_super_block(
    const int64_t disk_idx, const bool is_master_super_block, ObDiskFileSuperBlock& disk_super_block)
{
  int ret = OB_SUCCESS;
  char sstable_file_path[common::OB_MAX_FILE_NAME_LENGTH];
  char* buf = NULL;
  int64_t pos = 0;
  common::ObArenaAllocator allocator;
  const int64_t buf_size = OB_DEFAULT_MACRO_BLOCK_SIZE;
  const int64_t offset = is_master_super_block ? DISK_MASTER_SUPER_BLOCK_OFFSET : DISK_BACKUP_SUPER_BLOCK_OFFSET;
  int64_t read_size = 0;
  int fd = -1;
  ObRaidCommonHeader* header = NULL;
  disk_super_block.reset();

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_ISNULL(buf = static_cast<char*>(allocator.alloc(buf_size + DIO_READ_ALIGN_SIZE)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc buf", K(ret), K(buf_size));
  } else {
    buf = upper_align_buf(buf, DIO_READ_ALIGN_SIZE);
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObRaidDiskUtil::build_sstable_file_path(
            storage_env_->sstable_dir_, disk_idx, sstable_file_path, sizeof(sstable_file_path)))) {
      LOG_WARN("Failed to printf sstable path", K(ret), "sstable_dir", storage_env_->sstable_dir_, K(disk_idx));
    } else if ((fd = ::open(
                    sstable_file_path, O_DIRECT | O_RDWR | O_LARGEFILE, S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH)) < 0) {
      ret = OB_IO_ERROR;
      STORAGE_LOG(ERROR, "open file error", K(ret), K(sstable_file_path), K(errno), KERRMSG);
    } else if (OB_DEFAULT_MACRO_BLOCK_SIZE != (read_size = ob_pread(fd, buf, buf_size, offset))) {
      ret = OB_IO_ERROR;
      LOG_WARN("failed to read disk super block",
          K(ret),
          K(fd),
          K(offset),
          K(read_size),
          K(disk_idx),
          K(sstable_file_path),
          KERRMSG);
    } else if (OB_ISNULL(header = reinterpret_cast<ObRaidCommonHeader*>(buf))) {
      ret = OB_ERR_SYS;
      LOG_WARN("header must not null", K(ret));
    } else if (OB_FAIL(header->check_checksum(buf + header->header_length_, header->data_size_))) {
      LOG_ERROR("failed to check disk super block header", K(ret), K(*header));
    } else if (OB_FAIL(disk_super_block.deserialize(buf + header->header_length_, header->data_size_, pos))) {
      LOG_WARN("failed to decode disk super block", K(ret), K(buf_size), K(pos), K(disk_idx), K(sstable_file_path));
    } else if (!disk_super_block.is_valid()) {
      ret = OB_RAID_SUPER_BLOCK_NOT_MATCH;
      LOG_WARN("invalid disk super block", K(ret), K(disk_super_block));
    } else if (disk_super_block.disk_idx_ != disk_idx) {
      ret = OB_RAID_SUPER_BLOCK_NOT_MATCH;
      LOG_ERROR("disk id not match", K(ret), K(disk_idx), K(disk_super_block));
    } else {
      FLOG_INFO("succeed to load disk super block",
          K(is_master_super_block),
          K(disk_idx),
          K(sstable_file_path),
          K(disk_super_block));
    }
  }

  if (-1 != fd) {
    ::close(fd);
    fd = -1;
  }
  return ret;
}

int ObRaidFileSystem::build_raid_status(const ObDiskFileSuperBlock& disk_super_block, ObRaidFileStatus& raid_status)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  char sstable_path[common::OB_MAX_FILE_NAME_LENGTH];
  ObDiskFd disk_fd;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (!disk_super_block.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(disk_super_block));
  } else if (OB_FAIL(raid_status.load_raid(disk_super_block))) {
    LOG_WARN("failed to load raid", K(ret), K(disk_super_block));
  } else {
    for (int64_t disk_idx = 0; OB_SUCC(ret) && disk_idx < raid_status.disk_status_.count(); ++disk_idx) {
      ObDiskFileStatus& disk_status = raid_status.disk_status_.at(disk_idx);

      if (ObDiskFileInfo::DROP == disk_status.info_.status_) {
        FLOG_ERROR("skip load dropped disk", K(ret), K(disk_idx), K(disk_status));
      } else if (OB_FAIL(databuff_printf(sstable_path,
                     sizeof(sstable_path),
                     "%.*s",
                     disk_status.info_.sstable_path_.length(),
                     disk_status.info_.sstable_path_.ptr()))) {
        LOG_WARN("failed to copy sstable path", K(ret));
      } else if (0 > (disk_status.fd_ = ::open(
                          sstable_path, O_DIRECT | O_RDWR | O_LARGEFILE, S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH))) {

        ret = OB_IO_ERROR;
        STORAGE_LOG(
            ERROR, "open file error", K(ret), K(sstable_path), K(disk_status), K(errno), KERRMSG, K(disk_super_block));
      } else if (OB_FAIL(disk_status.get_disk_fd(disk_fd))) {
        LOG_WARN("failed to get disk fd", K(ret), K(disk_status));
      } else if (OB_FAIL(ObIOManager::get_instance().add_disk(disk_fd,
                     ObDisk::DEFAULT_SYS_IO_PERCENT,
                     ObDisk::MAX_DISK_CHANNEL_CNT,
                     ObIOManager::DEFAULT_IO_QUEUE_DEPTH))) {
        LOG_WARN("failed to add disk", K(ret), K(disk_status.fd_), K(disk_idx));
      } else {
        FLOG_INFO("open disk", K(disk_idx), K(sstable_path), K(disk_status));
      }
    }

    if (OB_FAIL(ret)) {
      for (int64_t disk_idx = 0; OB_SUCC(ret) && disk_idx < raid_status.disk_status_.count(); ++disk_idx) {
        ObDiskFileStatus& disk_status = raid_status.disk_status_.at(disk_idx);
        if (disk_status.fd_ >= 0) {
          if (OB_SUCCESS != (tmp_ret = close_disk(disk_status))) {
            LOG_WARN("failed to close disk", K(tmp_ret), K(disk_status));
          }
        }
      }
    }
  }

  return ret;
}

int64_t ObRaidFileSystem::get_total_data_size() const
{
  SpinRLockGuard lock_guard(lock_);
  return raid_status_.info_.total_macro_block_count_ * OB_DEFAULT_MACRO_BLOCK_SIZE;
}

int64_t ObRaidFileSystem::get_raid_src_data_num() const
{
  SpinRLockGuard lock_guard(lock_);
  return raid_status_.info_.src_data_num_;
}

int ObRaidFileSystem::fsync()
{
  int ret = OB_SUCCESS;
  SpinRLockGuard lock_guard(lock_);

  if (!is_opened_) {
    ret = OB_NOT_OPEN;
    LOG_WARN("not opened", K(ret), K(raid_status_));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < raid_status_.disk_status_.count(); ++i) {
      const int32_t fd = raid_status_.disk_status_[i].fd_;  // TODO(): check status
      if (-1 != fd) {
        if (0 != ::fsync(fd)) {
          ret = OB_IO_ERROR;
          STORAGE_LOG(ERROR, "sync file error", K(ret), K(i), K(fd), K(errno), KERRMSG, K_(raid_status));
        }
      }
    }
  }

  return ret;
}

int ObRaidFileSystem::init_file_ctx(const ObStoreFileType& file_type, blocksstable::ObStoreFileCtx& file_ctx) const
{
  int ret = OB_SUCCESS;

  if (!is_opened_) {
    ret = OB_NOT_OPEN;
    LOG_WARN("not opened", K(ret), K_(raid_status));
  } else if (file_type < 0 || file_type >= STORE_FILE_TYPE_MAX || !file_ctx.file_list_.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(file_type), K(file_ctx));
  } else {
    file_ctx.file_type_ = file_type;
    file_ctx.file_system_type_ = STORE_FILE_SYSTEM_RAID;
    file_ctx.block_count_per_file_ = INT64_MAX;
  }
  return ret;
}

bool ObRaidFileSystem::is_disk_full() const
{
  return OB_STORE_FILE.is_disk_full();
}

int ObRaidFileSystem::async_write(const ObStoreFileWriteInfo& write_info, common::ObIOHandle& io_handle)
{
  int ret = OB_SUCCESS;
  const int64_t block_index = write_info.block_id_.block_index();
  SpinRLockGuard lock_guard(lock_);

  if (!is_opened_) {
    ret = OB_NOT_OPEN;
    LOG_WARN("not opened", K(ret), K_(raid_status));
  } else if (block_index < RESERVED_MACRO_BLOCK_INDEX ||
             block_index >= raid_status_.info_.total_macro_block_count_) {  // 0,1 is super block
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid block index", K(ret), K(write_info), K_(raid_status));
  } else if (write_info.is_reuse_macro_block()) {
    STORAGE_LOG(DEBUG, "no need async write reuse block for raid file system", K(write_info));
  } else if (OB_FAIL(do_async_write(write_info, io_handle))) {
    LOG_WARN("failed to do_async_write", K(ret));
  }

  return ret;
}

int ObRaidFileSystem::do_async_write(const ObStoreFileWriteInfo& write_info, common::ObIOHandle& io_handle)
{
  int ret = OB_SUCCESS;

  const int64_t block_index = write_info.block_id_.block_index();
  const int64_t block_offset = 0;
  ObRaidStripLocation location;
  common::ObArenaAllocator allocator("RaidFileSystem");
  unsigned char* buf = NULL;
  const int64_t write_num = raid_status_.get_write_num();
  const int64_t write_size = write_num * raid_status_.info_.strip_size_;
  const int64_t strip_playload_size = raid_status_.info_.strip_size_ - ObRaidCommonHeader::HEADER_SIZE;
  const bool include_strip_header = true;

  if (!is_opened_) {
    ret = OB_NOT_OPEN;
    LOG_WARN("not opened", K(ret), K(raid_status_));
  } else if (!write_info.is_valid() || write_info.is_reuse_macro_block() ||
             write_info.size_ != raid_status_.info_.macro_block_size_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid write info", K(ret), K(write_info));
  } else if (OB_FAIL(write_mgr_.record_start(write_info.block_id_.block_index()))) {
    LOG_WARN("failed to record write block index", K(ret));
  } else if (OB_FAIL(location.init(block_index, block_offset, raid_status_.info_, include_strip_header))) {
    LOG_WARN("failed to cal strip location", K(ret), K(block_index), K(raid_status_));
  } else if (OB_ISNULL(buf = static_cast<unsigned char*>(allocator.alloc(write_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc buc", K(ret), K(write_size));
  } else {
    unsigned char* data_blocks[raid_status_.info_.src_data_num_];
    unsigned char* parity_blocks[raid_status_.info_.parity_num_];
    int64_t left_size = write_info.size_;
    const char* cur_write_ptr = write_info.buf_;
    MEMCPY(buf, write_info.buf_, write_info.size_);
    for (int64_t i = 0; OB_SUCC(ret) && i < raid_status_.info_.src_data_num_; ++i) {
      unsigned char* ptr = buf + i * raid_status_.info_.strip_size_;
      data_blocks[i] = ptr + ObRaidCommonHeader::HEADER_SIZE;
      const int64_t copy_size = std::min(left_size, strip_playload_size);
      MEMCPY(data_blocks[i], cur_write_ptr, copy_size);
      cur_write_ptr += copy_size;
      left_size -= copy_size;
    }

    if (OB_SUCC(ret) && left_size != 0) {
      ret = OB_ERR_SYS;
      LOG_ERROR("left size not zero", K(ret), K(left_size), K(write_info), K(raid_status_));
    }

    for (int64_t i = 0; OB_SUCC(ret) && i < raid_status_.info_.parity_num_; ++i) {
      unsigned char* ptr = buf + (i + raid_status_.info_.src_data_num_) * raid_status_.info_.strip_size_;
      parity_blocks[i] = ptr + ObRaidCommonHeader::HEADER_SIZE;
    }

    if (OB_SUCC(ret) && raid_status_.info_.parity_num_ > 0) {
      if (OB_FAIL(ObErasureCodeIsa::encode(raid_status_.info_.src_data_num_,
              raid_status_.info_.parity_num_,
              strip_playload_size,
              data_blocks,
              parity_blocks))) {
        LOG_WARN("Failed to do ec encode", K(ret));
      }
    }

    for (int64_t i = 0; OB_SUCC(ret) && i < write_num; ++i) {
      unsigned char* ptr = buf + i * raid_status_.info_.strip_size_;
      ObRaidCommonHeader* header = new (ptr) ObRaidCommonHeader();
      const unsigned char* data_ptr = ptr + ObRaidCommonHeader::HEADER_SIZE;
      if (OB_FAIL(header->set_checksum(data_ptr, strip_playload_size))) {
        LOG_WARN("failed to set checksum", K(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    ObIOInfo io_info;
    int64_t strip_idx = -1;
    int64_t disk_id = -1;
    int64_t offset = -1;
    int64_t strip_left_size = 0;
    io_info.io_desc_ = write_info.io_desc_;
    io_info.size_ = 0;
    io_info.batch_count_ = 0;
    io_info.finish_callback_ = &write_mgr_;
    io_info.finish_id_ = block_index;
    if (io_info.batch_count_ > MAX_IO_BATCH_NUM) {
      ret = OB_ERR_SYS;
      LOG_ERROR("batch count is too large", K(ret), K(io_info));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < raid_status_.get_write_num(); ++i) {
      ObIOPoint& io_point = io_info.io_points_[io_info.batch_count_];
      if (OB_FAIL(location.next_strip(strip_idx, disk_id, offset, strip_left_size))) {
        LOG_WARN("failed to move next location", K(ret), K(location));
      } else if (i != strip_idx || strip_left_size != raid_status_.info_.strip_size_) {
        ret = OB_ERR_SYS;
        LOG_ERROR("invalid strip info", K(ret), K(i), K(strip_idx), K(strip_left_size), K(raid_status_));
      } else if (disk_id < 0 || disk_id >= raid_status_.get_disk_count()) {
        ret = OB_ERR_SYS;
        LOG_ERROR("invalid disk_id", K(ret), K(disk_id), K(raid_status_.info_));
      } else if (raid_status_.disk_status_[disk_id].fd_ < 0) {
        ObDiskFileInfo::ObDiskStatus status = raid_status_.disk_status_[disk_id].info_.status_;
        if (ObDiskFileInfo::INIT == status || ObDiskFileInfo::NORMAL == status || ObDiskFileInfo::REBUILD == status) {
          ret = OB_ERR_SYS;
          LOG_ERROR("invalid disk fd", K(ret), K(disk_id), K(raid_status_));
        }
      } else if (OB_FAIL(raid_status_.disk_status_[disk_id].get_disk_fd(io_point.fd_))) {
        LOG_WARN("failed to get disk fd", K(ret), K(disk_id), K(raid_status_));
      } else {
        io_point.size_ = static_cast<int32_t>(raid_status_.info_.strip_size_);
        io_point.offset_ = offset;
        io_point.write_buf_ = reinterpret_cast<char*>(buf + raid_status_.info_.strip_size_ * i);

        ++io_info.batch_count_;
        io_info.size_ += io_point.size_;
      }
    }
    if (OB_SUCC(ret)) {
      io_info.io_desc_.mode_ = ObIOMode::IO_MODE_WRITE;
      LOG_DEBUG("aio write", K(block_index), K(io_info));
      if (NULL == write_info.io_callback_) {
        if (OB_FAIL(ObIOManager::get_instance().aio_write(io_info, io_handle))) {
          STORAGE_LOG(WARN, "Fail to aio_write, ", K(ret), K(write_info), K(io_info));
        }
      } else {
        if (OB_FAIL(ObIOManager::get_instance().aio_write(io_info, *write_info.io_callback_, io_handle))) {
          STORAGE_LOG(WARN, "Fail to aio_write, ", K(ret), K(write_info), K(io_info));
        }
      }
    }
  }
  return ret;
}

int ObRaidFileSystem::async_read(const ObStoreFileReadInfo& read_info, common::ObIOHandle& io_handle)
{
  int ret = OB_SUCCESS;

  int64_t block_index = 0;
  const int64_t end_offset_in_block = read_info.offset_ + read_info.size_;
  ObRaidStripLocation location;
  ObRaidIOErrorHandler io_error_handler;
  const bool include_strip_header = false;

  SpinRLockGuard lock_guard(lock_);
  if (!is_opened_) {
    ret = OB_NOT_OPEN;
    LOG_WARN("not opened", K(ret), K(raid_status_));
  } else if (!read_info.is_valid() || end_offset_in_block > raid_status_.info_.macro_block_size_ ||
             raid_status_.info_.total_macro_block_count_ <
                 (block_index = read_info.macro_block_ctx_->get_macro_block_id().block_index())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(read_info));
  } else if (OB_FAIL(io_error_handler.init(this, read_info))) {
    LOG_WARN("failed to init io_error_handler", K(ret));
  } else if (OB_FAIL(location.init(block_index, read_info.offset_, raid_status_.info_, include_strip_header))) {
    LOG_WARN("failed to cal strip location", K(ret), K(block_index), K(raid_status_));
  }

  if (OB_SUCC(ret)) {
    ObIOInfo io_info;
    int64_t disk_id = -1;
    int64_t offset = -1;  // offset in block_file
    int64_t strip_idx = -1;
    int64_t left_read_size = read_info.size_;
    int64_t strip_left_payload_size = 0;
    int64_t point_idx = 0;

    io_info.io_desc_ = read_info.io_desc_;
    io_info.offset_ = read_info.offset_;
    io_info.size_ = static_cast<int32_t>(read_info.size_);
    io_info.batch_count_ = 0;
    io_info.io_error_handler_ = &io_error_handler;
    io_info.io_desc_.mode_ = ObIOMode::IO_MODE_READ;

    for (int64_t i = 0; OB_SUCC(ret) && left_read_size > 0; ++i) {
      ObIOPoint& point = io_info.io_points_[point_idx];

      if (OB_FAIL(location.next_strip(strip_idx, disk_id, offset, strip_left_payload_size))) {
        LOG_WARN("failed to get next strip", K(ret), K(location), K(left_read_size));
      } else if (OB_FAIL(raid_status_.get_read_fd(disk_id, point.fd_))) {
        if (OB_RAID_DISK_NOT_NORMAL != ret) {
          LOG_WARN("failed to get read fd", K(ret), K(disk_id));
        } else if (OB_FAIL(io_info.add_fail_disk(disk_id))) {
          LOG_WARN("failed to add fail disk id", K(ret));
        } else {
          ret = OB_SUCCESS;  // TODO(): maybe should modify get_read_fd
          if (REACH_TIME_INTERVAL(1000 * 1000 * 60)) {
            FLOG_ERROR("raid disk not normal", K(ret), K(disk_id));
          }
        }
      } else {
        ++point_idx;
        point.offset_ = offset;

        if (strip_left_payload_size <= 0 || strip_left_payload_size > raid_status_.info_.get_strip_payload_size()) {
          ret = OB_ERR_SYS;
          FLOG_ERROR("invalid strip_left_payload_size",
              K(ret),
              K(strip_left_payload_size),
              K(strip_idx),
              K(offset),
              K(raid_status_));
        } else if (left_read_size > strip_left_payload_size) {
          point.size_ = static_cast<int32_t>(strip_left_payload_size);
          left_read_size -= strip_left_payload_size;
        } else {
          point.size_ = static_cast<int32_t>(left_read_size);
          left_read_size = 0;
        }
        ++io_info.batch_count_;
        LOG_DEBUG("build point",
            K(point),
            K(left_read_size),
            K(strip_left_payload_size),
            K(offset),
            K(strip_idx),
            K(disk_id));
      }
    }

    if (OB_SUCC(ret)) {
      LOG_DEBUG("aio_read", K(block_index), K(offset), K(end_offset_in_block), K(read_info), K(io_info));
      if (NULL == read_info.io_callback_) {
        if (OB_FAIL(ObIOManager::get_instance().aio_read(io_info, io_handle))) {
          STORAGE_LOG(WARN,
              "Fail to read",
              K(ret),
              K(block_index),
              K(offset),
              K(end_offset_in_block),
              K(read_info),
              K(io_info));
        }
      } else {
        if (OB_FAIL(ObIOManager::get_instance().aio_read(io_info, *read_info.io_callback_, io_handle))) {
          STORAGE_LOG(WARN,
              "Fail to read",
              K(ret),
              K(block_index),
              K(offset),
              K(end_offset_in_block),
              K(read_info),
              K(io_info));
        }
      }
    }
  }
  return ret;
}

int ObRaidFileSystem::write_server_super_block(const ObServerSuperBlock& super_block)
{
  int ret = OB_SUCCESS;
  SpinRLockGuard lock_guard(lock_);

  LOG_INFO("start write super block", K(super_block));
  if (!is_opened_) {
    ret = OB_NOT_OPEN;
    LOG_WARN("not opened", K(ret), K(raid_status_));
  } else if (OB_FAIL(write_super_block(false /*is_master*/, super_block))) {
    LOG_WARN("failed to write slave super block", K(ret), K(super_block));
  } else if (OB_FAIL(write_super_block(true /*is_master*/, super_block))) {
    LOG_WARN("failed to write master super block", K(ret), K(super_block));
  } else {
    super_block_ = super_block;
    LOG_INFO("succeed to write super_block", K(super_block_));
  }

  return ret;
}

int ObRaidFileSystem::write_super_block(const bool is_master, const ObServerSuperBlock& super_block)
{
  int ret = OB_SUCCESS;
  ObStoreFileWriteInfo write_info;
  common::ObArenaAllocator allocator(ObModIds::OB_SSTABLE_BLOCK_FILE);
  ObStoreFileCtx file_ctx(allocator);
  common::ObIOHandle io_handle;
  LOG_INFO("before write super block", K(super_block));

  if (!is_opened_) {
    ret = OB_NOT_OPEN;
    LOG_WARN("not opened", K(ret), K(raid_status_));
  } else if (!super_block.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(super_block));
  } else if (OB_FAIL(super_block_buf_holder_.serialize_super_block(super_block))) {
    STORAGE_LOG(ERROR, "failed to serialize super block", K(ret), K_(super_block_buf_holder), K(super_block));
  } else {
    if (is_master) {
      write_info.block_id_.set_local_block_id(0);
    } else {
      write_info.block_id_.set_local_block_id(1);
    }
    write_info.buf_ = super_block_buf_holder_.get_buffer();
    write_info.size_ = super_block_buf_holder_.get_len();
    write_info.io_desc_.category_ = SYS_IO;
    write_info.io_desc_.wait_event_no_ = ObWaitEventIds::DB_FILE_COMPACT_WRITE;
    write_info.io_desc_.req_deadline_time_ = ObTimeUtility::current_time();
    write_info.ctx_ = &file_ctx;

    if (OB_FAIL(do_async_write(write_info, io_handle))) {
      LOG_WARN("failed to do async write", K(ret), K(write_info));
    } else if (OB_FAIL(io_handle.wait(DEFAULT_IO_WAIT_TIME_MS))) {
      LOG_WARN("failed to wait write super block", K(ret), K(is_master), K(super_block));
    } else {
      LOG_INFO("succeed to write super block", K(ret), K(is_master), K(super_block));
    }
  }
  return ret;
}

int ObRaidFileSystem::read_server_super_block(ObServerSuperBlock& super_block)
{
  int ret = OB_SUCCESS;
  SpinRLockGuard lock_guard(lock_);

  LOG_INFO("read super block", K(super_block));
  if (!is_opened_) {
    ret = OB_NOT_OPEN;
    LOG_WARN("not opened", K(ret), K(raid_status_));
  } else if (OB_FAIL(read_super_block(true /*is_master*/, super_block))) {
    LOG_WARN("failed to read master super block", K(ret), K(super_block));
    if (OB_FAIL(read_super_block(false /*is_master*/, super_block))) {
      LOG_WARN("failed to read slave super block", K(ret), K(super_block));
    }
  }

  if (OB_SUCC(ret)) {
    LOG_WARN("succeed to read super_block", K(super_block));
  }

  return ret;
}

int ObRaidFileSystem::read_super_block(const bool is_master, ObServerSuperBlock& super_block)
{
  int ret = OB_SUCCESS;
  ObMacroBlockCtx macro_block_ctx;
  common::ObIOHandle io_handle;
  ObStoreFileReadInfo read_info;
  super_block.reset();

  if (!is_opened_) {
    ret = OB_NOT_OPEN;
    LOG_WARN("not opened", K(ret), K(raid_status_));
  } else {
    if (is_master) {
      macro_block_ctx.sstable_block_id_.macro_block_id_.set_local_block_id(0);
    } else {
      macro_block_ctx.sstable_block_id_.macro_block_id_.set_local_block_id(1);
    }
    read_info.macro_block_ctx_ = &macro_block_ctx;
    read_info.offset_ = 0;
    read_info.size_ = super_block_buf_holder_.get_len();
    read_info.io_desc_.category_ = SYS_IO;
    read_info.io_desc_.wait_event_no_ = ObWaitEventIds::DB_FILE_COMPACT_READ;
    read_info.io_desc_.req_deadline_time_ = ObTimeUtility::current_time();
    read_info.io_callback_ = NULL;

    if (OB_FAIL(async_read(read_info, io_handle))) {
      LOG_WARN("failed to do async read", K(ret), K(read_info));
    } else if (OB_FAIL(io_handle.wait(DEFAULT_IO_WAIT_TIME_MS))) {
      LOG_WARN("failed to wait read super block", K(ret), K(is_master), K(super_block));
    } else if (OB_FAIL(super_block_buf_holder_.assign(io_handle.get_buffer(), io_handle.get_data_size()))) {
      LOG_WARN("failed to assign super block buf holder", K(ret), K(is_master), K(io_handle));
    } else if (OB_FAIL(super_block_buf_holder_.deserialize_super_block(super_block))) {
      LOG_WARN("failed to decode super block", K(ret), K(is_master));
    } else {
      LOG_INFO("succeed to read super block", K(ret), K(is_master), K(super_block), K(read_info));
    }
  }
  return ret;
}

ObRaidFileStatus::ObRaidFileStatus() : is_inited_(false), info_(), disk_status_()
{}

// used for normal read
// during rebuild, should not this func
int ObRaidFileStatus::get_read_fd(const int64_t idx, ObDiskFd& fd) const
{
  int ret = OB_SUCCESS;
  fd.reset();

  if (idx < 0 || idx >= disk_status_.count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid idx", K(ret), K(idx), K(*this));
  } else {
    const ObDiskFileStatus& disk_status = disk_status_[idx];
    if (ObDiskFileInfo::NORMAL != disk_status.info_.status_) {
      ret = OB_RAID_DISK_NOT_NORMAL;
    } else if (OB_FAIL(disk_status.get_disk_fd(fd))) {
      LOG_WARN("Failed to get disk fd", K(ret), K(idx), K(disk_status_));
    }
  }
  return ret;
}

int ObRaidFileStatus::get_fd(const int64_t idx, ObDiskFd& fd) const
{
  int ret = OB_SUCCESS;
  fd.reset();

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K(idx));
  } else if (idx < 0 || idx >= disk_status_.count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid idx", K(ret), K(idx), K(*this));
  } else if (OB_FAIL(disk_status_[idx].get_disk_fd(fd))) {
    LOG_WARN("Failed to get disk fd", K(ret), K(idx), K(disk_status_));
  }
  return ret;
}

int ObRaidFileStatus::get_fd(const ObString& disk_name, ObDiskFd& fd) const
{
  int ret = OB_SUCCESS;
  const ObDiskFileStatus* disk_status = NULL;
  fd.reset();

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K(disk_name));
  } else if (OB_FAIL(get_disk_status(disk_name, disk_status))) {
    LOG_WARN("Failed to get disk status", K(ret), K(disk_name));
  } else if (OB_ISNULL(disk_status)) {
    ret = OB_ERR_SYS;
    LOG_ERROR("disk status must not null", K(ret), K(disk_name), K(*this));
  } else if (OB_FAIL(disk_status->get_disk_fd(fd))) {
    LOG_WARN("failed to get disk fd", K(ret));
  }
  return ret;
}

int ObRaidFileStatus::get_disk_status(const ObString& disk_name, ObDiskFileStatus*& disk_status)
{
  int ret = OB_SUCCESS;
  const ObDiskFileStatus* tmp_disk_status = NULL;
  disk_status = NULL;

  if (OB_FAIL(get_disk_status(disk_name, tmp_disk_status))) {
    LOG_WARN("failed to get disk status", K(ret), K(disk_name));
  } else {
    disk_status = const_cast<ObDiskFileStatus*>(tmp_disk_status);
  }

  return ret;
}

int ObRaidFileStatus::get_disk_status(const ObString& disk_name, const ObDiskFileStatus*& disk_status) const
{
  int ret = OB_SUCCESS;
  disk_status = NULL;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K(disk_name));
  } else {
    ret = OB_ENTRY_NOT_EXIST;
    for (int64_t i = 0; i < disk_status_.count(); ++i) {
      if (0 == disk_name.case_compare(disk_status_[i].info_.disk_name_)) {
        disk_status = &disk_status_[i];
        ret = OB_SUCCESS;
        break;
      }
    }
  }

  return ret;
}

int ObRaidFileStatus::cal_raid_param(const ObStorageEnv::REDUNDANCY_LEVEL& redundancy_level, const int64_t disk_count,
    const common::ObDiskType& disk_type, const int64_t macro_block_size, int64_t& src_data_num, int64_t& parity_num,
    int64_t& strip_size)
{
  int ret = OB_SUCCESS;
  strip_size = 0;
  parity_num = 0;

  if (disk_count <= 0 || redundancy_level < 0 || redundancy_level >= ObStorageEnv::MAX_REDUNDANCY ||
      macro_block_size <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(disk_count), K(redundancy_level), K(macro_block_size));
  } else if (ObStorageEnv::EXTERNAL_REDUNDANCY == redundancy_level) {
    parity_num = 0;
  } else if (ObStorageEnv::NORMAL_REDUNDANCY == redundancy_level) {
    parity_num = 1;
  } else if (ObStorageEnv::HIGH_REDUNDANCY == redundancy_level) {
    parity_num = 2;
  }

  if (OB_SUCC(ret)) {
    if (disk_count <= parity_num) {
      src_data_num = 1;
      parity_num = disk_count - 1;
      strip_size = upper_align(macro_block_size, DIO_READ_ALIGN_SIZE) + ObRaidCommonHeader::HEADER_SIZE;
      FLOG_ERROR("disk count not enough for specified redundancy level",
          K(redundancy_level),
          K(disk_count),
          K(src_data_num),
          K(parity_num),
          K(strip_size));
    } else {
      strip_size = macro_block_size / (disk_count - parity_num) + ObRaidCommonHeader::HEADER_SIZE;
      if (DISK_SSD == disk_type) {
        if (strip_size < SSD_MIN_STRIP_SIZE) {
          strip_size = SSD_MIN_STRIP_SIZE;
        }
      } else {
        if (0 == parity_num) {
          strip_size = macro_block_size + ObRaidCommonHeader::HEADER_SIZE;
        } else if (strip_size < NONE_SSD_MIN_STRIP_SIZE) {
          strip_size = NONE_SSD_MIN_STRIP_SIZE;
        }
      }
      strip_size = upper_align(strip_size, DIO_READ_ALIGN_SIZE);
      const int64_t payload_strip_size = strip_size - ObRaidCommonHeader::HEADER_SIZE;
      src_data_num = macro_block_size / payload_strip_size;
      if (macro_block_size % payload_strip_size != 0) {
        ++src_data_num;
      }
      FLOG_INFO("cal raid param for specified redundancy level",
          K(redundancy_level),
          K(disk_count),
          K(disk_type),
          K(src_data_num),
          K(parity_num),
          K(strip_size),
          K(payload_strip_size),
          "header_size",
          ObRaidCommonHeader::HEADER_SIZE);
    }
  }

  return ret;
}

int ObRaidFileStatus::get_disk_super_block(const int64_t idx, ObDiskFileSuperBlock& super_block) const
{
  int ret = OB_SUCCESS;
  super_block.reset();

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (idx < 0 || idx >= info_.disk_count_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid args", K(ret), K(idx), K(*this));
  } else if (idx != disk_status_.at(idx).info_.disk_id_.disk_idx_) {
    ret = OB_ERR_SYS;
    LOG_ERROR("disk idx not match",
        K(ret),
        K(idx),
        "expect_disk_idx",
        disk_status_.at(idx).info_.disk_id_.disk_idx_,
        K_(disk_status));
  } else {
    super_block.disk_idx_ = idx;
    super_block.info_ = info_;
    for (int64_t i = 0; OB_SUCC(ret) && i < info_.disk_count_; ++i) {
      if (OB_FAIL(super_block.disk_info_.push_back(disk_status_.at(i).info_))) {
        LOG_WARN("failed to push back disk info", K(ret), K(i));
      }
    }
  }
  return ret;
}

int ObRaidFileStatus::load_raid(const ObDiskFileSuperBlock& super_block)
{
  int ret = OB_SUCCESS;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("cannot init twice", K(ret), K(*this));
  } else if (!super_block.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid super block", K(ret), K(super_block));
  } else if (OB_FAIL(disk_status_.prepare_allocate(super_block.info_.disk_count_))) {
    LOG_WARN("failed to prepare_allocate disk status", K(ret), K(super_block));
  } else {
    info_ = super_block.info_;
    for (int64_t disk_idx = 0; OB_SUCC(ret) && disk_idx < super_block.info_.disk_count_; ++disk_idx) {
      ObDiskFileStatus& disk_status = disk_status_.at(disk_idx);
      disk_status.info_ = super_block.disk_info_.at(disk_idx);
    }
  }

  if (OB_SUCC(ret)) {
    is_inited_ = true;
  }
  return ret;
}

bool ObRaidFileStatus::is_valid() const
{
  return is_inited_ && info_.is_valid() && disk_status_.count() == info_.disk_count_;
}

void ObRaidFileStatus::reset()
{
  is_inited_ = false;
  info_.reset();
  disk_status_.reset();
}

ObRaidIOErrorHandler::ObRaidIOErrorHandler()
    : is_inited_(false),
      macro_block_ctx_(),
      offset_(-1),
      size_(-1),
      io_desc_(),
      out_io_buf_(nullptr),
      out_io_buf_size_(0),
      aligned_offset_(0),
      raid_file_system_(nullptr)
{}

int ObRaidIOErrorHandler::init(ObRaidFileSystem* raid_file_system, const ObStoreFileReadInfo& read_info)
{
  int ret = OB_SUCCESS;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("cannot init twice", K(ret));
  } else if (OB_ISNULL(raid_file_system) || !read_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), KP(raid_file_system), K(read_info));
  } else {
    raid_file_system_ = raid_file_system;
    macro_block_ctx_ = *read_info.macro_block_ctx_;
    offset_ = read_info.offset_;
    size_ = read_info.size_;
    io_desc_ = read_info.io_desc_;
    is_inited_ = true;
  }
  return ret;
}

int64_t ObRaidIOErrorHandler::get_deep_copy_size() const
{
  return sizeof(ObRaidIOErrorHandler);
}

int ObRaidIOErrorHandler::deep_copy(char* buf, const int64_t buf_len, ObIIOErrorHandler*& handler) const
{
  int ret = OB_SUCCESS;
  handler = nullptr;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_ISNULL(buf) || buf_len < get_deep_copy_size()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), KP(buf), K(buf_len), "copy_size", get_deep_copy_size());
  } else if (nullptr != out_io_buf_ || 0 != out_io_buf_size_ || 0 != aligned_offset_) {
    ret = OB_ERR_SYS;
    LOG_ERROR("cannot deep copy handler with buf", K(ret), KP(out_io_buf_), K(out_io_buf_size_), K(aligned_offset_));
  } else {
    ObRaidIOErrorHandler* tmp = new (buf) ObRaidIOErrorHandler();
    tmp->is_inited_ = is_inited_;
    tmp->macro_block_ctx_ = macro_block_ctx_;
    tmp->offset_ = offset_;
    tmp->size_ = size_;
    tmp->io_desc_ = io_desc_;
    tmp->raid_file_system_ = raid_file_system_;
    handler = tmp;
  }
  return ret;
}

int ObRaidIOErrorHandler::set_read_io_buf(char* io_buf, const int64_t io_buf_size, const int64_t aligned_offset)
{
  int ret = OB_SUCCESS;
  int64_t expected_size = 0;
  int64_t expected_aligned_offset = 0;
  common::align_offset_size(offset_, size_, expected_aligned_offset, expected_size);

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_ISNULL(io_buf) || io_buf_size != expected_size || aligned_offset != expected_aligned_offset) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args",
        K(ret),
        KP(io_buf),
        K(io_buf_size),
        K(aligned_offset),
        K(expected_size),
        K(expected_aligned_offset),
        K(*this));
  } else {
    out_io_buf_ = io_buf;
    out_io_buf_size_ = io_buf_size;
    aligned_offset_ = aligned_offset;
  }
  return ret;
}

int ObRaidIOErrorHandler::get_recover_request_num(int64_t& recover_request_num) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_ISNULL(raid_file_system_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("raid file system is null", K(ret), KP(raid_file_system_));
  } else {
    recover_request_num = raid_file_system_->get_raid_src_data_num();
  }
  return ret;
}

int ObRaidIOErrorHandler::init_recover_io_master(
    const ObBitSet<OB_MAX_DISK_NUMBER>& recover_disk_idx_set, ObIAllocator* allocator, ObIOMaster* io_master)
{
  int ret = OB_SUCCESS;
  ObRaidRecoverIOCallback recover_callback;
  ObIOInfo io_info;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_ISNULL(allocator)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid args", K(ret), KP(allocator));
  } else if (OB_FAIL(recover_callback.init(allocator, out_io_buf_, out_io_buf_size_, aligned_offset_))) {
    LOG_WARN("failed to set out io buf", K(ret));
  } else if (OB_FAIL(raid_file_system_->init_recover_io_master(macro_block_ctx_,
                 aligned_offset_,
                 out_io_buf_size_,
                 io_desc_,
                 recover_disk_idx_set,
                 recover_callback,
                 io_master))) {
    LOG_WARN("failed to init recover io master", K(ret));
  }

  return ret;
}

// TODO(): use param struct
int ObRaidFileSystem::init_recover_io_master(const ObMacroBlockCtx& macro_block_ctx, const int64_t aligned_offset,
    const int64_t out_io_buf_size, const ObIODesc& io_desc, const ObBitSet<OB_MAX_DISK_NUMBER>& recover_disk_idx_set,
    ObRaidRecoverIOCallback& callback, ObIOMaster* io_master)
{
  int ret = OB_SUCCESS;
  ObRaidStripLocation location;
  ObIOInfo info;
  ObRaidRecoverIOCallback::RecoverParam recover_param;

  SpinRLockGuard lock_guard(lock_);
  // aligned_offset and out_io_buf_size not used for recover whole macro block data

  const int64_t total_read_size = raid_status_.info_.strip_size_ * raid_status_.info_.src_data_num_;
  const int64_t read_offset = 0;
  const int64_t block_index = macro_block_ctx.get_macro_block_id().block_index();
  const bool include_strip_header = true;
  int64_t strip_left_size = 0;
  recover_param.data_count_ = raid_status_.info_.src_data_num_;
  recover_param.parity_count_ = raid_status_.info_.parity_num_;
  recover_param.strip_size_ = raid_status_.info_.strip_size_;
  recover_param.block_index_ = block_index;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (!macro_block_ctx.is_valid() || aligned_offset < 0 || out_io_buf_size <= 0 || !io_desc.is_valid() ||
             OB_ISNULL(io_master) || block_index > raid_status_.info_.total_macro_block_count_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN(
        "invalid args", K(ret), K(macro_block_ctx), K(aligned_offset), K(out_io_buf_size), K(io_desc), KP(io_master));
  } else if (recover_disk_idx_set.num_members() > raid_status_.info_.parity_num_) {
    ret = OB_IO_ERROR;
    LOG_ERROR("cannot recover disk more than parity num", K(ret), K(recover_disk_idx_set), K(raid_status_));
  } else if (OB_FAIL(location.init(block_index, read_offset, raid_status_.info_, include_strip_header))) {
    LOG_WARN("failed to init location", K(ret), K(block_index), K(raid_status_));
  } else {
    info.offset_ = aligned_offset;
    info.size_ = static_cast<int32_t>(total_read_size);
    info.io_desc_ = io_desc;
    info.batch_count_ = 0;
    info.io_error_handler_ = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && i < raid_status_.info_.src_data_num_; ++i) {
      ObIOPoint& point = info.io_points_[info.batch_count_];
      int64_t strip_idx = -1;
      int64_t disk_idx = -1;
      int64_t offset = -1;  // offset in block_file

      while (OB_SUCC(ret)) {
        if (OB_FAIL(location.next_strip(strip_idx, disk_idx, offset, strip_left_size))) {
          if (OB_ITER_END != ret) {
            LOG_WARN("failed to get next strip", K(ret), K(location));
          } else {
            ret = OB_SUCCESS;
            break;
          }
        } else if (raid_status_.info_.strip_size_ != strip_left_size) {
          ret = OB_ERR_SYS;
          LOG_ERROR("invalid strip left size", K(ret), K(strip_left_size), K(raid_status_));
        } else if (OB_FAIL(raid_status_.get_read_fd(disk_idx, point.fd_))) {
          if (OB_RAID_DISK_NOT_NORMAL != ret) {
            LOG_WARN("failed to get read fd", K(ret), K(disk_idx));
          } else if (OB_FAIL(recover_param.recover_index_.push_back(strip_idx))) {
            LOG_WARN("failed to add fail strip_idx", K(ret));
          } else {
            LOG_DEBUG("add recover strip idx for bad disk", K(ret), K(strip_idx), K(disk_idx));
          }
        } else if (recover_disk_idx_set.has_member(disk_idx)) {
          if (OB_FAIL(recover_param.recover_index_.push_back(strip_idx))) {
            LOG_WARN("failed to add fail strip_idx", K(ret));
          } else {
            LOG_DEBUG("add recover strip idx from recover_index_set", K(ret), K(strip_idx), K(disk_idx));
          }
        } else {
          break;
        }
      }

      if (OB_SUCC(ret)) {
        const int64_t strip_size = raid_status_.info_.strip_size_;

        if (OB_FAIL(recover_param.input_index_.push_back(strip_idx))) {
          LOG_WARN("failed to add strip_idx", K(ret));
        } else {
          point.offset_ = offset;
          point.size_ = static_cast<int32_t>(strip_size);
          ++info.batch_count_;
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    info.io_desc_.mode_ = IO_MODE_READ;
    if (OB_FAIL(callback.set_recover_param(recover_param))) {
      LOG_WARN("failed to set recover param", K(ret), K(callback));
    } else if (OB_FAIL(io_master->open(IO_MODE_READ, info, &callback))) {
      LOG_WARN("failed to init master", K(ret), K(info), K(callback));
    }
  }
  return ret;
}

int ObRaidFileSystem::add_disk(const ObString& diskgroup_name, const ObString& disk_path, const ObString& alias_name)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObDiskFileStatus* disk_status = NULL;
  int64_t disk_idx = -1;
  bool result = false;
  char link_dir_path[common::OB_MAX_FILE_NAME_LENGTH];
  char add_dir_path[common::OB_MAX_FILE_NAME_LENGTH];

  lib::ObMutexGuard mutex_guard(mutex_);

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("cannot init twice", K(ret));
  } else if (diskgroup_name.case_compare("data") != 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("only support drop data diskgroup", K(ret), K(diskgroup_name), K(alias_name));
  } else if (OB_FAIL(
                 databuff_printf(add_dir_path, sizeof(add_dir_path), "%.*s", disk_path.length(), disk_path.ptr()))) {
    LOG_WARN("Failed to print add_dir_path", K(ret));
  } else if (OB_FAIL(FileDirectoryUtils::is_empty_directory(add_dir_path, result))) {
    LOG_WARN("failed to check is empty dir", K(ret), K(disk_path));
  } else if (!result) {
    ret = OB_DIR_NOT_EMPTY;
    LOG_ERROR("cannot add not empty dir", K(ret), K(disk_path));
  } else if (!alias_name.empty() &&
             OB_ENTRY_NOT_EXIST != (ret = raid_status_.get_disk_status(alias_name, disk_status))) {
    if (OB_SUCCESS != ret) {
      LOG_WARN("failed to get disk status", K(ret), K(alias_name));
    } else {
      ret = OB_ENTRY_EXIST;
      LOG_WARN("cannot add dup disk name", K(ret), K(alias_name), K(disk_path), K(*disk_status));
    }
  } else if (OB_FAIL(find_need_disk_idx(disk_idx))) {
    LOG_WARN("Failed to find need disk idx", K(ret), K(raid_status_));
  } else if (OB_FAIL(databuff_printf(
                 link_dir_path, sizeof(link_dir_path), "%s/%ld", storage_env_->sstable_dir_, disk_idx))) {
    LOG_WARN("Failed to print link dir path", K(ret));
  } else if (OB_FAIL(FileDirectoryUtils::symlink(add_dir_path, link_dir_path))) {
    LOG_WARN("failed to symlink", K(ret), K(add_dir_path), K(link_dir_path));
  } else {
    ObRaidFileStatus new_raid_status = raid_status_;
    ObDiskFileStatus& disk_status = new_raid_status.disk_status_.at(disk_idx);
    if (disk_status.info_.disk_id_.disk_idx_ != disk_idx || disk_status.fd_ >= 0 ||
        disk_status.info_.status_ != ObDiskFileInfo::DROP) {
      ret = OB_ERR_SYS;
      LOG_ERROR("invalid disk status, cannot add disk", K(ret), K(disk_idx), K(disk_status));
    } else {
      disk_status.info_.status_ = ObDiskFileInfo::INIT;
      if (OB_FAIL(open_new_disk(new_raid_status.info_.blocksstable_size_, disk_status))) {
        LOG_ERROR("failed to open new disk", K(ret), K(disk_idx));
      } else if (OB_FAIL(write_raid_super_block(new_raid_status))) {
        LOG_WARN("Failed to write raid super block", K(ret));
        if (OB_SUCCESS != (tmp_ret = close_disk(disk_status))) {
          LOG_ERROR("failed to close disk", K(tmp_ret), K(disk_status));
        }
      } else {
        FLOG_INFO("succeed to add disk", K(diskgroup_name), K(alias_name), K(new_raid_status), K(raid_status_));
        SpinWLockGuard lock_guard(lock_);
        raid_status_ = new_raid_status;
      }
    }
  }

  return ret;
}

int ObRaidFileSystem::find_need_disk_idx(int64_t& new_disk_idx)
{
  int ret = OB_SUCCESS;
  new_disk_idx = -1;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("cannot init twice", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && new_disk_idx < 0 && i < raid_status_.info_.disk_count_; ++i) {
      const ObDiskFileStatus& disk_status = raid_status_.disk_status_[i];
      if (ObDiskFileInfo::DROP == disk_status.info_.status_) {
        if (disk_status.fd_ < 0) {
          new_disk_idx = i;
        } else {
          FLOG_ERROR("drop disk fd is not valid, cannot install new disk", K(i), K(raid_status_));
        }
      }
    }

    if (OB_SUCC(ret) && new_disk_idx < 0) {
      ret = OB_ENTRY_NOT_EXIST;
      FLOG_WARN("no disk entry found", K(ret), K(raid_status_));
    }
  }
  return ret;
}

int ObRaidFileSystem::close_disk(ObDiskFileStatus& disk_status)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObDiskFd disk_fd;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("cannot init twice", K(ret));
  } else if (OB_FAIL(disk_status.get_disk_fd(disk_fd))) {
    LOG_WARN("failed to get disk fd", K(ret), K(disk_status));
  } else if (disk_fd.fd_ < 0) {
    ret = OB_ERR_SYS;
    FLOG_ERROR("cannot close disk with invalid fd", K(ret), K(disk_status));
  } else {
    bool is_deleted = false;
    while (!is_deleted) {
      if (OB_SUCCESS != (tmp_ret = disk_status.get_disk_fd(disk_fd))) {
        LOG_ERROR("failed to get disk fd", K(tmp_ret), K(disk_status));
      } else if (OB_SUCCESS != (tmp_ret = ObIOManager::get_instance().delete_disk(disk_fd))) {
        LOG_ERROR("failed to delete disk", K(tmp_ret), K(disk_status));
      } else {
        is_deleted = true;
        FLOG_INFO("delete disk", K(disk_status));
      }
      if (!is_deleted) {
        FLOG_INFO("delete it failed, retry after 100ms", K(tmp_ret));
        usleep(100 * 1000);  // 100ms
      }
    }

    if (0 != ::fsync(disk_status.fd_)) {
      STORAGE_LOG(ERROR, "data file sync data error", "fd", disk_status.fd_, K(errno), KERRMSG);
    }

    if (0 != ::close(disk_status.fd_)) {
      STORAGE_LOG(ERROR, "data file close error", "fd", disk_status.fd_, K(errno), KERRMSG);
    }
    disk_status.fd_ = OB_INVALID_FD;
  }

  return ret;
}

int ObRaidFileSystem::drop_disk(const ObString& diskgroup_name, const ObString& alias_name)
{
  int ret = OB_SUCCESS;
  ObDiskFileStatus* disk_status = NULL;
  char sub_dir_path[common::OB_MAX_FILE_NAME_LENGTH];
  ObDiskFd old_fd;

  lib::ObMutexGuard mutex_guard(mutex_);
  ObRaidFileStatus new_raid_status = raid_status_;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("cannot init twice", K(ret));
  } else if (diskgroup_name.case_compare("data") != 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("only support drop data diskgroup", K(ret), K(diskgroup_name), K(alias_name));
  } else if (OB_FAIL(new_raid_status.get_disk_status(alias_name, disk_status))) {
    LOG_WARN("failed to get disk status", K(ret), K(alias_name));
  } else if (!disk_status->info_.is_valid()) {
    ret = OB_ERR_SYS;
    LOG_ERROR("invalid disk info", K(ret), K(*disk_status));
  } else if (OB_FAIL(disk_status->get_disk_fd(old_fd))) {
    LOG_WARN("failed to get old fd", K(ret), K(*disk_status));
  } else if (OB_FAIL(ObRaidDiskUtil::build_sstable_dir(storage_env_->sstable_dir_,
                 disk_status->info_.disk_id_.disk_idx_,
                 sub_dir_path,
                 sizeof(sub_dir_path)))) {
    LOG_WARN("Failed to printf sub_dir_path", K(ret), "sstable_dir", storage_env_->sstable_dir_);
  } else {
    disk_status->info_.status_ = ObDiskFileInfo::DROP;
    disk_status->fd_ = OB_INVALID_FD;
    if (OB_FAIL(write_raid_super_block(new_raid_status))) {
      LOG_WARN("failed to write_raid_super_block", K(ret));
    } else {
      FLOG_INFO("succeed to drop disk", K(diskgroup_name), K(alias_name), K(new_raid_status), K(raid_status_));
      SpinWLockGuard lock_guard(lock_);
      raid_status_ = new_raid_status;
    }
  }

  if (OB_SUCC(ret)) {

    if (OB_FAIL(FileDirectoryUtils::unlink_symlink(sub_dir_path))) {
      LOG_ERROR("failed to unlink_symlink disk dir", K(ret), K(sub_dir_path));
    } else {
      FLOG_INFO("succeed to delete disk dir", K(sub_dir_path));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObIOManager::get_instance().delete_disk(old_fd))) {
      LOG_ERROR("failed to delete disk, retry later", K(ret), K(disk_status));
    } else {
      if (0 != ::fsync(old_fd.fd_)) {
        STORAGE_LOG(ERROR, "dropped disk data file sync data error", K(old_fd), K(errno), KERRMSG);
      }

      if (0 != ::close(old_fd.fd_)) {
        STORAGE_LOG(ERROR, "dropped disk data file close error", K(old_fd), K(errno), KERRMSG);
      }
    }
  }
  return ret;
}

ObRaidRecoverIOCallback::RecoverParam::RecoverParam()
    : input_index_(), recover_index_(), data_count_(0), parity_count_(0), strip_size_(0), block_index_(0)
{}

int ObRaidRecoverIOCallback::RecoverParam::assign(const RecoverParam& param)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(input_index_.assign(param.input_index_))) {
    LOG_WARN("failed to copy input index", K(ret));
  } else if (OB_FAIL(recover_index_.assign((param.recover_index_)))) {
    LOG_WARN("failed to copy recover index", K(ret));
  } else {
    data_count_ = param.data_count_;
    parity_count_ = param.parity_count_;
    strip_size_ = param.strip_size_;
    block_index_ = param.block_index_;
  }

  return ret;
}

bool ObRaidRecoverIOCallback::RecoverParam::is_valid() const
{
  return input_index_.count() == data_count_ && recover_index_.count() <= parity_count_ && data_count_ > 0 &&
         parity_count_ > 0 && strip_size_ > ObRaidCommonHeader::HEADER_SIZE && block_index_ >= 0;
}

ObRaidRecoverIOCallback::ObRaidRecoverIOCallback()
    : is_inited_(false),
      allocator_(nullptr),
      buf_(nullptr),
      io_buf_(nullptr),
      io_buf_size_(0),
      recover_param_(),
      out_offset_(0),
      out_io_buf_(nullptr),
      out_io_buf_size_(0)
{
  static_assert(sizeof(*this) <= CALLBACK_BUF_SIZE, "IOCallback buf size not enough");
}

ObRaidRecoverIOCallback::~ObRaidRecoverIOCallback()
{
  if (NULL != buf_ && NULL != allocator_) {
    allocator_->free(buf_);
    buf_ = nullptr;
  }
  io_buf_ = nullptr;
}

int ObRaidRecoverIOCallback::init(
    common::ObIAllocator* allocator, char* out_buf, const int64_t out_buf_size, const int64_t offset_of_macro_block)
{
  int ret = OB_SUCCESS;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("cannot init twice", K(ret));
  } else if (OB_ISNULL(allocator) || OB_ISNULL(out_buf) || out_buf_size <= 0 || offset_of_macro_block < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), KP(allocator), KP(out_buf), K(out_buf_size), K(offset_of_macro_block));
  } else {
    allocator_ = allocator;
    out_io_buf_ = out_buf;
    out_io_buf_size_ = out_buf_size;
    out_offset_ = offset_of_macro_block;
    is_inited_ = true;
  }
  return ret;
}

int ObRaidRecoverIOCallback::set_recover_param(const RecoverParam& param)
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (!param.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(param));
  } else if (OB_FAIL(recover_param_.assign(param))) {
    LOG_WARN("failed to assign param", K(ret));
  }
  return ret;
}

int64_t ObRaidRecoverIOCallback::size() const
{
  return sizeof(ObRaidRecoverIOCallback);
}

int ObRaidRecoverIOCallback::alloc_io_buf(char*& io_buf, int64_t& io_buf_size, int64_t& aligned_offset)
{
  int ret = OB_SUCCESS;
  io_buf = NULL;
  io_buf_size = 0;
  aligned_offset = 0;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K(*this));
  } else if (nullptr != buf_) {
    ret = OB_ERR_SYS;
    LOG_ERROR("cannot alloc io buf twice", K(ret), K(*this));
  } else {
    const int64_t need_strip_buf_count = recover_param_.input_index_.count() + recover_param_.recover_index_.count();
    io_buf_size = need_strip_buf_count * recover_param_.strip_size_;
    const int64_t buf_size = io_buf_size + DIO_READ_ALIGN_SIZE;
    if (OB_ISNULL(buf_ = reinterpret_cast<char*>(allocator_->alloc(buf_size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc buf", K(ret), K(buf_size));
    } else {
      io_buf = upper_align_buf(buf_, DIO_READ_ALIGN_SIZE);
      io_buf_ = io_buf;
      io_buf_size_ = io_buf_size;
    }
  }
  return ret;
}

int ObRaidRecoverIOCallback::inner_process(const bool is_success)
{
  int ret = OB_SUCCESS;
  const int64_t strip_size = recover_param_.strip_size_;
  const int64_t strip_payload_size = strip_size - ObRaidCommonHeader::HEADER_SIZE;
  unsigned char* input_buf[OB_MAX_DISK_NUMBER];
  unsigned char* recover_buf[OB_MAX_DISK_NUMBER];
  unsigned char* data_buf[OB_MAX_DISK_NUMBER];

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K(*this));
  } else if (!is_success) {
    if (nullptr != buf_) {
      allocator_->free(buf_);
      buf_ = nullptr;
    }
  } else if (out_io_buf_size_ > recover_param_.data_count_ * strip_payload_size) {
    ret = OB_ERR_SYS;
    LOG_ERROR("out_io_buf_size cannot larger than max stripe payload", K(ret), K(*this));
  } else {

    char* cur_buf = io_buf_;
    for (int64_t i = 0; i < recover_param_.input_index_.count(); ++i) {
      input_buf[i] = reinterpret_cast<unsigned char*>(cur_buf) + ObRaidCommonHeader::HEADER_SIZE;
      cur_buf += recover_param_.strip_size_;
    }
    for (int64_t i = 0; i < recover_param_.recover_index_.count(); ++i) {
      recover_buf[i] = reinterpret_cast<unsigned char*>(cur_buf) + ObRaidCommonHeader::HEADER_SIZE;
      MEMSET(recover_buf[i], 0, ObRaidCommonHeader::HEADER_SIZE);  // header cannot recover
      cur_buf += recover_param_.strip_size_;
    }

    if (OB_SUCC(ret)) {
      const int64_t strip_playload_size = recover_param_.strip_size_ - ObRaidCommonHeader::HEADER_SIZE;
      if (OB_FAIL(ObErasureCodeIsa::decode(recover_param_.data_count_,
              recover_param_.parity_count_,
              strip_playload_size,
              recover_param_.input_index_,
              recover_param_.recover_index_,
              input_buf,
              recover_buf))) {
        LOG_WARN("failed to decode ec", K(ret), K(*this));
      }
    }

    int64_t data_buf_idx = 0;
    int64_t input_idx = 0;
    int64_t recover_idx = 0;
    while (OB_SUCC(ret) &&
           (input_idx < recover_param_.input_index_.count() || recover_idx < recover_param_.recover_index_.count())) {
      bool use_input_buf = false;
      if (input_idx >= recover_param_.input_index_.count()) {
        use_input_buf = false;
      } else if (recover_idx >= recover_param_.recover_index_.count()) {
        use_input_buf = true;
      } else if (recover_param_.input_index_.at(input_idx) == recover_param_.recover_index_.at(recover_idx)) {
        ret = OB_ERR_SYS;
        LOG_ERROR("input id must not same as recover id", K(ret), K(recover_param_));
      } else if (recover_param_.input_index_.at(input_idx) < recover_param_.recover_index_.at(recover_idx)) {
        use_input_buf = true;
      } else {
        use_input_buf = false;
      }

      if (OB_SUCC(ret)) {
        if (use_input_buf) {
          data_buf[data_buf_idx] = input_buf[input_idx];
          ++input_idx;
        } else {
          data_buf[data_buf_idx] = recover_buf[recover_idx];
          ++recover_idx;
        }
        ++data_buf_idx;
      }
    }

    int64_t strip_idx = out_offset_ / strip_payload_size;
    int64_t strip_offset = out_offset_ - strip_idx * strip_payload_size;
    int64_t left_size = out_io_buf_size_;
    char* out_ptr = out_io_buf_;
    while (OB_SUCC(ret) && left_size > 0) {
      const unsigned char* buf_copy_ptr = data_buf[strip_idx] + strip_offset;
      const int64_t buf_copy_size = std::min(left_size, strip_payload_size - strip_offset);
      MEMCPY(out_ptr, buf_copy_ptr, buf_copy_size);
      ++strip_idx;
      strip_offset = 0;
      left_size -= buf_copy_size;
      out_ptr += buf_copy_size;
    }
  }
  return ret;
}

int ObRaidRecoverIOCallback::inner_deep_copy(char* buf, const int64_t buf_len, ObIOCallback*& callback) const
{
  int ret = OB_SUCCESS;
  callback = nullptr;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_ISNULL(buf) || buf_len < size()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), KP(buf), K(buf_len), "size", size());
  } else if (nullptr != buf_) {
    ret = OB_ERR_SYS;
    LOG_ERROR("cannot deep copy callback when buf is not null", K(ret), K(*this));
  } else {
    ObRaidRecoverIOCallback* tmp = new (buf) ObRaidRecoverIOCallback();
    // won't copy buf_, input_buf_, revcover_buf_
    tmp->is_inited_ = is_inited_;
    tmp->allocator_ = allocator_;
    tmp->out_offset_ = out_offset_;
    tmp->out_io_buf_ = out_io_buf_;
    tmp->out_io_buf_size_ = out_io_buf_size_;
    if (OB_FAIL(tmp->recover_param_.assign(recover_param_))) {
      LOG_WARN("failed to copy recover param", K(ret));
      tmp->~ObRaidRecoverIOCallback();
    } else {
      callback = tmp;
    }
  }
  return ret;
}

const char* ObRaidRecoverIOCallback::get_data()
{
  return buf_;
}

int ObRaidFileSystem::do_rebuild_task()
{
  int ret = OB_SUCCESS;
  ObDiskID rebuild_disk_id;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_FAIL(get_need_build_disk(rebuild_disk_id))) {
    if (OB_NO_DISK_NEED_REBUILD != ret) {
      LOG_WARN("failed to get rebuild disk id", K(ret));
    } else {
      ret = OB_SUCCESS;
    }
  } else if (OB_FAIL(rebuild_disk(rebuild_disk_id))) {
    LOG_WARN("failed to rebuild disk", K(ret), K(rebuild_disk_id));
  }
  return ret;
}

int ObRaidFileSystem::get_need_build_disk(ObDiskID& disk_id)
{
  int ret = OB_SUCCESS;
  bool found = false;
  disk_id.reset();

  lib::ObMutexGuard mutex_guard(mutex_);
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  }
  // find rebuild disk first
  for (int64_t i = 0; OB_SUCC(ret) && i < raid_status_.disk_status_.count(); ++i) {
    const ObDiskFileStatus& disk_status = raid_status_.disk_status_[i];
    if (ObDiskFileInfo::REBUILD == disk_status.info_.status_) {
      disk_id = disk_status.info_.disk_id_;
      found = true;
      FLOG_INFO("got need rebuild disk in REBUILD status", K(i), K(disk_id));
    }
  }
  // find init disk
  if (!found) {
    ObRaidFileStatus new_raid_status = raid_status_;
    for (int64_t i = 0; OB_SUCC(ret) && i < new_raid_status.disk_status_.count(); ++i) {
      ObDiskFileStatus& disk_status = new_raid_status.disk_status_.at(i);
      if (ObDiskFileInfo::INIT == disk_status.info_.status_) {
        disk_id = disk_status.info_.disk_id_;
        disk_status.info_.status_ = ObDiskFileInfo::REBUILD;
        found = true;
        FLOG_INFO("got need rebuild disk in INIT status", K(i), K(disk_id));
        if (OB_FAIL(write_raid_super_block(new_raid_status))) {
          LOG_WARN("failed to writ raid super block", K(ret), K(new_raid_status));
        } else {
          FLOG_INFO("succeed to set disk in rebuild status", K(disk_id), K(new_raid_status), K(raid_status_));
          SpinWLockGuard lock_guard(lock_);
          raid_status_ = new_raid_status;
        }
      }
    }
  }

  if (OB_SUCC(ret) && !found) {
    ret = OB_NO_DISK_NEED_REBUILD;
  }

  return ret;
}

int ObRaidFileSystem::rebuild_disk(const ObDiskID& disk_id)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  int64_t rebuild_macro_count = 0;
  int64_t total_macro_count = 0;
  int64_t rebuild_start_ts = ObTimeUtility::current_time();

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (!disk_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(disk_id));
  } else {
    {
      SpinRLockGuard lock_guard(lock_);
      total_macro_count = raid_status_.info_.total_macro_block_count_;
    }
    while (OB_SUCC(ret) && rebuild_macro_count < total_macro_count) {
      const int64_t block_index = rebuild_macro_count;

      if (!is_opened_) {
        ret = OB_SERVER_IS_STOPPING;
        LOG_WARN("server is stopping", K(ret));
      } else if (OB_FAIL(write_mgr_.record_start(block_index))) {
        LOG_WARN("failed to record write block index", K(ret), K(block_index));
      } else {
        if (OB_FAIL(rebuild_macro_block(disk_id, block_index))) {
          LOG_WARN("failed to rebuild macro block", K(ret), K(disk_id), K(block_index));
        } else {
          LOG_INFO("rebuild macro block end", K(ret), K(disk_id), K(block_index), K(total_macro_count));
        }

        if (OB_SUCCESS != (tmp_ret = write_mgr_.notice_finish(block_index))) {
          if (OB_SUCC(ret)) {
            ret = tmp_ret;
          }
          LOG_ERROR("failed to notice finish", K(ret), K(tmp_ret), K(block_index));
        }
      }
      ++rebuild_macro_count;
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(finish_rebuild_disk(disk_id, rebuild_macro_count))) {
        LOG_WARN("failed to finish rebuild disk", K(ret), K(disk_id));
      }
    }
  }

  const int64_t cost_ts = ObTimeUtility::current_time() - rebuild_start_ts;
  FLOG_INFO("finish rebuild disk", K(disk_id), K(cost_ts));
  return ret;
}

int ObRaidFileSystem::rebuild_macro_block(const ObDiskID& disk_id, const int64_t block_index)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator("RaidFileSystem");
  bool need_rebuild = true;
  int64_t recover_offset = -1;
  ObIOInfo info;
  common::ObSEArray<int64_t, OB_MAX_DISK_NUMBER> input_index;
  common::ObSEArray<int64_t, OB_MAX_DISK_NUMBER> recover_index;
  ObMacroBlockMetaHandle meta_handle;
  bool is_free = false;

  SpinRLockGuard lock_guard(lock_);

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (!disk_id.is_valid() || block_index < 0 || block_index >= raid_status_.info_.total_macro_block_count_ ||
             block_index >= UINT32_MAX) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(disk_id), K(block_index), K(raid_status_));
  } else if (OB_FAIL(OB_STORE_FILE.is_free_block(block_index, is_free))) {
    LOG_WARN("failed to check is free block", K(ret), K(block_index));
  } else if (is_free) {
    need_rebuild = false;
    FLOG_INFO("skip rebuild not used macro block", K(ret), K(disk_id), K(block_index));
  } else if (OB_FAIL(cal_rebuild_read_io_info(
                 disk_id, block_index, need_rebuild, info, input_index, recover_index, recover_offset))) {
    LOG_WARN("failed to cal rebuild read io info", K(ret), K(disk_id));
  } else if (!need_rebuild) {
    FLOG_INFO("skip rebuild not macro block has no strip on bad disk", K(ret), K(disk_id), K(block_index));
  }

  if (OB_SUCC(ret) && need_rebuild) {
    ObIOHandle io_handle;
    const int64_t strip_size = raid_status_.info_.strip_size_;
    char* strip_buf = nullptr;
    if (info.batch_count_ < raid_status_.info_.src_data_num_ || recover_index.count() != 1) {
      ret = OB_ERR_SYS;
      LOG_ERROR("cannot find recover data", K(ret), K(info), K(raid_status_));
    } else if (OB_FAIL(ObIOManager::get_instance().aio_read(info, io_handle))) {
      STORAGE_LOG(WARN, "Fail to read", K(ret), K(disk_id), K(block_index), K(info));
    } else if (OB_FAIL(io_handle.wait(DEFAULT_IO_WAIT_TIME_MS))) {
      LOG_WARN("failed to wait rebuild io handle", K(ret));
    } else if (OB_ISNULL(strip_buf = reinterpret_cast<char*>(allocator.alloc(strip_size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc strip buf", K(ret), K(strip_size));
    } else {
      char* read_buf = const_cast<char*>(io_handle.get_buffer());
      unsigned char* input_buf[OB_MAX_DISK_NUMBER];
      unsigned char* recover_buf[OB_MAX_DISK_NUMBER];
      const int64_t strip_playload_size = strip_size - ObRaidCommonHeader::HEADER_SIZE;
      for (int64_t i = 0; i < input_index.count(); ++i) {
        input_buf[i] = reinterpret_cast<unsigned char*>(read_buf) + strip_size * i + ObRaidCommonHeader::HEADER_SIZE;
      }
      recover_buf[0] = reinterpret_cast<unsigned char*>(strip_buf) + ObRaidCommonHeader::HEADER_SIZE;

      if (OB_FAIL(ObErasureCodeIsa::decode(raid_status_.info_.src_data_num_,
              raid_status_.info_.parity_num_,
              strip_playload_size,
              input_index,
              recover_index,
              input_buf,
              recover_buf))) {
        LOG_WARN("failed to decode ec", K(ret), K(*this));
      } else {
        ObRaidCommonHeader* header = new (strip_buf) ObRaidCommonHeader();
        const char* data_ptr = strip_buf + ObRaidCommonHeader::HEADER_SIZE;
        if (OB_FAIL(header->set_checksum(data_ptr, strip_playload_size))) {
          LOG_WARN("failed to set checksum", K(ret));
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(write_strip_data(disk_id, recover_offset, strip_buf, strip_size))) {
        LOG_WARN("failed to wrtie strip data", K(ret), K(disk_id), K(block_index), K(recover_offset));
      } else {
        FLOG_INFO("succeed to rebuild macro block", K(ret), K(disk_id), K(block_index));
      }
    }
  }

  return ret;
}

int ObRaidFileSystem::cal_rebuild_read_io_info(const ObDiskID& rebuild_disk_id, const int64_t block_index,
    bool& need_rebuild, ObIOInfo& info, common::ObIArray<int64_t>& input_index,
    common::ObIArray<int64_t>& recover_index, int64_t& recover_offset)
{
  int ret = OB_SUCCESS;
  const int64_t read_offset = 0;
  const bool include_strip_header = true;
  ObRaidStripLocation location;
  const int64_t strip_size = raid_status_.info_.strip_size_;
  const int64_t total_read_size = raid_status_.info_.strip_size_ * raid_status_.info_.src_data_num_;

  need_rebuild = false;
  info.reset();
  input_index.reset();
  recover_index.reset();
  recover_offset = -1;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (total_read_size > INT32_MAX) {
    ret = OB_ERR_SYS;
    LOG_ERROR("invalid total read size", K(ret), K(total_read_size), K(raid_status_));
  } else if (OB_FAIL(location.init(block_index, read_offset, raid_status_.info_, include_strip_header))) {
    LOG_WARN("Failed to init location", K(ret), K(block_index), K(raid_status_));
  } else {
    // info.offset need fill
    info.size_ = static_cast<int32_t>(total_read_size);
    info.io_desc_.category_ = SYS_IO;
    info.io_desc_.wait_event_no_ = ObWaitEventIds::DB_FILE_COMPACT_READ;
    info.io_desc_.req_deadline_time_ = ObTimeUtility::current_time();
    info.batch_count_ = 0;
    info.io_error_handler_ = nullptr;
    info.io_desc_.mode_ = ObIOMode::IO_MODE_READ;

    for (int64_t i = 0; OB_SUCC(ret) && i < raid_status_.info_.src_data_num_; ++i) {
      ObIOPoint& point = info.io_points_[info.batch_count_];
      int64_t strip_idx = -1;
      int64_t disk_idx = -1;
      int64_t offset = -1;  // offset in block_file
      int64_t strip_left_size = 0;

      if (recover_index.count() > 0 && info.batch_count_ >= raid_status_.info_.src_data_num_) {
        break;
      }
      while (OB_SUCC(ret)) {
        if (OB_FAIL(location.next_strip(strip_idx, disk_idx, offset, strip_left_size))) {
          if (OB_ITER_END != ret) {
            LOG_WARN("failed to get next strip", K(ret));
          } else {
            ret = OB_SUCCESS;
            break;
          }
        } else if (strip_size != strip_left_size) {
          ret = OB_ERR_SYS;
          LOG_ERROR("invalid strip left size", K(ret), K(strip_left_size), K(raid_status_));
        } else if (disk_idx == rebuild_disk_id.disk_idx_) {
          need_rebuild = true;
          recover_offset = offset;
          if (OB_FAIL(recover_index.push_back(disk_idx))) {
            LOG_WARN("failed to add fail disk id", K(ret));
          }
        } else if (OB_FAIL(raid_status_.get_read_fd(disk_idx, point.fd_))) {
          if (OB_RAID_DISK_NOT_NORMAL != ret) {
            LOG_WARN("failed to get read fd", K(ret), K(disk_idx));
          } else {
            ret = OB_SUCCESS;
            LOG_DEBUG("skip raid disk not normal", K(ret), K(disk_idx));
          }
        } else {
          break;
        }
      }

      if (OB_SUCC(ret)) {
        if (OB_FAIL(input_index.push_back((disk_idx)))) {
          LOG_WARN("failed to add disk idx", K(ret));
        } else {
          point.offset_ = offset;
          point.size_ = static_cast<int32_t>(strip_size);
          ++info.batch_count_;
        }
      }
    }
  }
  return ret;
}

int ObRaidFileSystem::write_strip_data(
    const ObDiskID& disk_id, const int64_t offset, const char* strip_buf, const int64_t strip_size)
{
  int ret = OB_SUCCESS;
  common::ObIOHandle io_handle;
  ObIOInfo io_info;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (!disk_id.is_valid() || offset < DISK_MACRO_BLOCK_START_OFFSET || OB_ISNULL(strip_buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(disk_id), K(offset), KP(strip_buf));
  } else {
    ObIOPoint& io_point = io_info.io_points_[0];

    io_info.io_desc_.category_ = SYS_IO;
    io_info.io_desc_.wait_event_no_ = ObWaitEventIds::DB_FILE_COMPACT_READ;
    io_info.io_desc_.req_deadline_time_ = ObTimeUtility::current_time();
    io_info.size_ = static_cast<int32_t>(strip_size);
    io_info.batch_count_ = 1;

    io_point.size_ = static_cast<int32_t>(strip_size);
    io_point.offset_ = offset;
    io_point.write_buf_ = strip_buf;

    if (OB_FAIL(raid_status_.get_fd(disk_id.disk_idx_, io_point.fd_))) {
      LOG_WARN("failed to get fd", K(ret), K(disk_id));
    } else if (OB_FAIL(ObIOManager::get_instance().aio_write(io_info, io_handle))) {
      LOG_WARN("failed to aio_write, ", K(ret), K(io_info));
    } else if (OB_FAIL(io_handle.wait(DEFAULT_IO_WAIT_TIME_MS))) {
      LOG_WARN("failed to wait write rebuild macro block", K(ret), K(io_info));
    }
  }

  return ret;
}

int ObRaidFileSystem::finish_rebuild_disk(const ObDiskID& disk_id, const int64_t rebuild_macro_count)
{
  int ret = OB_SUCCESS;

  lib::ObMutexGuard mutex_guard(mutex_);

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("cannot init twice", K(ret));
  } else if (!disk_id.is_valid() || disk_id.disk_idx_ >= raid_status_.disk_status_.count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(disk_id), K(raid_status_));
  } else {
    ObRaidFileStatus new_raid_status = raid_status_;
    ObDiskFileStatus& disk_status = new_raid_status.disk_status_.at(disk_id.disk_idx_);
    if (disk_status.info_.disk_id_.disk_idx_ != disk_id.disk_idx_ || disk_status.fd_ < 0 ||
        disk_status.info_.status_ != ObDiskFileInfo::REBUILD ||
        rebuild_macro_count != new_raid_status.info_.total_macro_block_count_) {
      ret = OB_ERR_SYS;
      LOG_ERROR("invalid disk status, cannot finish rebuild disk", K(ret), K(disk_id), K(disk_status));
    } else {
      disk_status.info_.status_ = ObDiskFileInfo::NORMAL;
      if (OB_FAIL(write_raid_super_block(new_raid_status))) {
        LOG_ERROR("Failed to write raid super block for rebuild disk", K(ret));
      } else {
        FLOG_INFO("succeed to rebuild disk", K(disk_id), K(new_raid_status), K(raid_status_));
        SpinWLockGuard lock_guard(lock_);
        raid_status_ = new_raid_status;
      }
    }
  }

  return ret;
}

int ObRaidFileSystem::get_disk_status(ObDiskStats& disk_stats)
{
  int ret = OB_SUCCESS;
  ObDiskStat stat;
  disk_stats.reset();

  SpinRLockGuard lock_guard(lock_);
  disk_stats.data_num_ = raid_status_.info_.src_data_num_;
  disk_stats.parity_num_ = raid_status_.info_.parity_num_;
  for (int64_t i = 0; OB_SUCC(ret) && i < raid_status_.disk_status_.count(); ++i) {
    const ObDiskFileInfo& info = raid_status_.disk_status_.at(i).info_;
    stat.disk_idx_ = info.disk_id_.disk_idx_;
    stat.install_seq_ = info.disk_id_.install_seq_;
    stat.create_ts_ = info.create_ts_;
    stat.finish_ts_ = info.rebuild_finish_ts_;
    stat.percent_ = 0;
    stat.status_ = info.get_status_str();
    if (info.status_ == ObDiskFileInfo::NORMAL) {
      stat.percent_ = 100;
    }
    if (OB_FAIL(databuff_printf(
            stat.alias_name_, common::MAX_PATH_SIZE, "%.*s", info.disk_name_.length(), info.disk_name_.ptr()))) {
      LOG_WARN("failed to printf alias name", K(ret), K(info));
    } else if (OB_FAIL(disk_stats.disk_stats_.push_back(stat))) {
      LOG_WARN("failed to add disk stat", K(ret), K(stat));
    }
  }

  LOG_DEBUG("get disk status", K(disk_stats), K(raid_status_));

  return ret;
}

int ObRaidFileSystem::start()
{
  return OB_STORE_FILE.open();
}

void ObRaidFileSystem::stop()
{
  OB_STORE_FILE.stop();
  timer_.stop();
}

void ObRaidFileSystem::wait()
{
  OB_STORE_FILE.wait();
  timer_.wait();
}

int ObRaidFileSystem::get_macro_block_info(
    const ObTenantFileKey& file_key, const MacroBlockId& macro_block_id, ObMacroBlockInfo& macro_block_info)
{
  UNUSED(file_key);
  return OB_STORE_FILE.get_macro_block_info(macro_block_id.block_index(), macro_block_info);
}
int ObRaidFileSystem::report_bad_block(
    const MacroBlockId& macro_block_id, const int64_t error_type, const char* error_msg, const char* file_path)
{
  UNUSED(file_path);
  return OB_STORE_FILE.report_bad_block(macro_block_id, error_type, error_msg);
}
int ObRaidFileSystem::get_bad_block_infos(common::ObArray<ObBadBlockInfo>& bad_block_infos)
{
  return OB_STORE_FILE.get_bad_block_infos(bad_block_infos);
}

int ObRaidFileSystem::get_marker_status(ObMacroBlockMarkerStatus& status)
{
  return OB_STORE_FILE.get_store_status(status);
}

int64_t ObRaidFileSystem::get_free_macro_block_count() const
{
  return OB_STORE_FILE.get_free_macro_block_count();
}

int ObRaidFileSystem::alloc_file(ObStorageFile*& file)
{
  int ret = OB_SUCCESS;
  ObLocalStorageFile* local_file = nullptr;
  if (OB_UNLIKELY(!is_opened_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("The ObLocalFileSystem hasn't been inited", K(ret));
  } else if (OB_FAIL(file_allocator_.alloc_file(local_file))) {
    LOG_WARN("alloc ObStorageFile fail", K(ret), KP(file));
  } else {
    local_file->file_system_ = this;
    local_file->set_fd(0);
    file = static_cast<ObStorageFile*>(local_file);
  }
  return ret;
}

int ObRaidFileSystem::free_file(ObStorageFile*& file)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_opened_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("The ObLocalFileSystem hasn't been inited", K(ret));
  } else {
    ObLocalStorageFile* local_file = static_cast<ObLocalStorageFile*>(file);
    if (OB_FAIL(file_allocator_.free_file(local_file))) {
      LOG_WARN("free ObStorageFile fail", K(ret));
    } else {
      file = nullptr;
    }
  }
  return ret;
}

int ObRaidFileSystem::get_super_block_version(int64_t& version)
{
  int ret = OB_SUCCESS;
  version = OB_SUPER_BLOCK_V3;
  return ret;
}

int ObRaidFileSystem::unlink_block(const ObStorageFile& file, const MacroBlockId& macro_id)
{
  UNUSED(file);
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_opened_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("The ObRaidFileSystem hasn't been inited", K(ret));
  } else if (!macro_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(macro_id));
  } else {
    OB_STORE_FILE.dec_ref(macro_id);
  }
  return ret;
}

int ObRaidFileSystem::link_block(const ObStorageFile& file, const MacroBlockId& macro_id)
{
  UNUSED(file);
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_opened_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("The ObRaidFileSystem hasn't been inited", K(ret));
  } else if (!macro_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(macro_id));
  } else {
    OB_STORE_FILE.inc_ref(macro_id);
  }
  return ret;
}

ObRaidRebuildTask::ObRaidRebuildTask(ObRaidFileSystem& file_system) : file_system_(file_system)
{}

ObRaidRebuildTask::~ObRaidRebuildTask()
{}

void ObRaidRebuildTask::runTimerTask()
{
  int tmp_ret = OB_SUCCESS;

  if (OB_SUCCESS != (tmp_ret = file_system_.do_rebuild_task())) {
    LOG_ERROR("failed to do rebuild task", K(tmp_ret));
  }
}

ObRaidFileWriteMgr::ObRaidFileWriteMgr() : is_inited_(false), cond_()
{}

ObRaidFileWriteMgr::~ObRaidFileWriteMgr()
{}

int ObRaidFileWriteMgr::init()
{
  int ret = OB_SUCCESS;
  const int64_t BLOCK_SET_BUCKET_COUNT = 1024;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("cannot init twice", K(ret));
  } else if (OB_FAIL(cond_.init(ObWaitEventIds::RAID_WRITE_RECORD_LOCK_WAIT))) {
    COMMON_LOG(WARN, "failed to init cond ", K(ret));
  } else if (OB_FAIL(block_set_.create(BLOCK_SET_BUCKET_COUNT))) {
    LOG_WARN("failed to create block set", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

void ObRaidFileWriteMgr::destroy()
{
  is_inited_ = false;
  cond_.destroy();
  block_set_.destroy();
}

// if old request to same block not finish, will wait
int ObRaidFileWriteMgr::record_start(const int64_t block_index)
{
  int ret = OB_SUCCESS;
  int hash_ret = OB_SUCCESS;
  const int64_t write_wait_timeout_ms = 1000;
  int64_t start_ts = ObTimeUtility::current_time();
  int64_t retry_count = 0;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (block_index < 0) {
    ret = OB_ERR_SYS;
    LOG_ERROR("invalid block index", K(ret), K(block_index));
  } else {
    while (OB_SUCC(ret)) {
      ObThreadCondGuard guard(cond_);
      hash_ret = block_set_.set_refactored(block_index, 0 /*not overwrite*/);
      if (OB_SUCCESS == hash_ret) {
        break;
      } else if (OB_HASH_EXIST == hash_ret) {
        cond_.wait(write_wait_timeout_ms);
        if (REACH_TIME_INTERVAL(10 * 1000 * 1000 /*10s*/)) {
          int64_t cost_ts = ObTimeUtility::current_time() - start_ts;
          FLOG_WARN("waiting write request", K(block_index), K(cost_ts));
        }
      } else {
        ret = hash_ret;
        LOG_WARN("failed to set block index", K(ret), K(block_index));
      }
      ++retry_count;
    }
  }

  if (retry_count != 0) {
    int64_t cost_ts = ObTimeUtility::current_time() - start_ts;
    FLOG_INFO("record write request start", K(ret), K(block_index), K(cost_ts));
  }
  return ret;
}

int ObRaidFileWriteMgr::notice_finish(const int64_t block_index)
{
  int ret = OB_SUCCESS;
  int hash_ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (block_index < 0) {
    ret = OB_ERR_SYS;
    LOG_ERROR("invalid block index", K(ret), K(block_index));
  } else {
    ObThreadCondGuard guard(cond_);
    hash_ret = block_set_.erase_refactored(block_index);
    if (OB_SUCCESS != hash_ret) {
      ret = hash_ret;
      LOG_ERROR("failed to erase block index", K(ret), K(block_index));
    }
    cond_.broadcast();
  }

  return ret;
}
