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
#include <sys/statvfs.h>
#include "ob_local_file_system.h"
#include "lib/file/file_directory_utils.h"
#include "share/config/ob_server_config.h"
#include "storage/blocksstable/ob_store_file.h"
#include "observer/ob_server_struct.h"

namespace oceanbase {
using namespace common;
using namespace storage;
namespace blocksstable {

//------------------ Implementation of ObLocalStorageFile -----------------------//
ObLocalStorageFile::ObLocalStorageFile() : ObStorageFile(), file_system_(nullptr)
{}

int ObLocalStorageFile::init(const ObAddr& svr_addr, const FileType file_type)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_UNLIKELY(svr_addr.is_valid() && GCTX.self_addr_ != svr_addr) ||
             OB_UNLIKELY(FileType::SERVER_ROOT != file_type && FileType::TMP_FILE != file_type)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(svr_addr), K(file_type));
  } else if (OB_FAIL(ObStorageFile::init_base(file_type, OB_SERVER_TENANT_ID))) {
    LOG_WARN("Fail to init storage file, ", K(ret));
  } else {
    // ignore svr_addr and svr_id, local file doesn't care
    is_inited_ = true;
  }

  if (IS_NOT_INIT) {
    reset();
  }
  return ret;
}

int ObLocalStorageFile::init(
    const common::ObAddr& svr_addr, const uint64_t tenant_id, const int64_t file_id, const FileType file_type)
{
  int ret = OB_SUCCESS;
  UNUSED(svr_addr);
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id || OB_INVALID_ID == tenant_id) ||
             OB_UNLIKELY(FileType::TENANT_DATA != file_type && FileType::TENANT_DATA_INFO != file_type)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(file_id), K(file_type));
  } else if (OB_FAIL(ObStorageFile::init_base(file_type, tenant_id))) {
    LOG_WARN("Fail to init storage file, ", K(ret));
  } else {
    file_id_ = file_id < 0 ? ObTimeUtility::current_time() : file_id;
    is_inited_ = true;
  }

  if (IS_NOT_INIT) {
    reset();
  }
  return ret;
}
void ObLocalStorageFile::reset()
{
  tenant_id_ = OB_INVALID_TENANT_ID;
  file_id_ = OB_INVALID_DATA_FILE_ID;
  file_system_ = nullptr;
  ObStorageFile::reset();
  is_inited_ = false;
}

int ObLocalStorageFile::open(const int flags)
{
  UNUSEDx(flags);
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    // local file system, storage file are logic entity, do nothing
  }
  return ret;
}

int ObLocalStorageFile::close()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    // local file system, storage file are logic entity, do nothing
  }
  return ret;
}

int ObLocalStorageFile::unlink()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    // local file system, storage file are logic entity, do nothing
  }
  return ret;
}

int ObLocalStorageFile::fsync()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(file_system_->fsync())) {
    LOG_ERROR("Fail to fsync data file, ", K(ret), K(errno), KERRMSG);
  }
  return ret;
}

int ObLocalStorageFile::read_super_block(ObISuperBlock& super_block)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (FileType::TMP_FILE == file_type_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tmp file has no super block", K(ret));
  } else if (FileType::SERVER_ROOT == file_type_) {
    if (OB_FAIL(file_system_->read_server_super_block(static_cast<ObServerSuperBlock&>(super_block)))) {
      LOG_WARN("read super block from file system fail", K(ret), K(super_block));
    }
  } else if (FileType::TENANT_DATA == file_type_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("local super block is in slog", K(ret));
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid path", K(ret));
  }

  return ret;
}

int ObLocalStorageFile::write_super_block(const ObISuperBlock& super_block)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (FileType::TMP_FILE == file_type_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tmp file has no super block", K(ret));
  } else if (FileType::SERVER_ROOT == file_type_) {
    if (OB_FAIL(file_system_->write_server_super_block(static_cast<const ObServerSuperBlock&>(super_block)))) {
      LOG_WARN("write super block from file system fail", K(ret), K(super_block));
    }
  } else if (FileType::TENANT_DATA == file_type_) {
    // local pg_file super block is written in slog
    // do nothing
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid path", K(ret));
  }

  return ret;
}

int ObLocalStorageFile::async_read_block(const ObMacroBlockReadInfo& read_info, ObMacroBlockHandle& macro_handle)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(OB_STORE_FILE.async_read_block(read_info, macro_handle))) {
    LOG_WARN("async read block fail", K(ret));
  }
  return ret;
}

int ObLocalStorageFile::async_write_block(const ObMacroBlockWriteInfo& write_info, ObMacroBlockHandle& macro_handle)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(OB_STORE_FILE.async_write_block(write_info, macro_handle))) {
    LOG_WARN("async write block fail", K(ret));
  }
  return ret;
}

int ObLocalStorageFile::read_block(const ObMacroBlockReadInfo& read_info, ObMacroBlockHandle& macro_handle)
{
  int ret = OB_SUCCESS;
  const int64_t io_timeout_ms = GCONF._data_storage_io_timeout / 1000L;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(async_read_block(read_info, macro_handle))) {
    LOG_WARN("Fail to sync read block, ", K(ret), K(read_info));
  } else if (OB_FAIL(macro_handle.wait(io_timeout_ms))) {
    LOG_WARN("Fail to wait io finish, ", K(ret));
  }
  return ret;
}

int ObLocalStorageFile::write_block(const ObMacroBlockWriteInfo& write_info, ObMacroBlockHandle& macro_handle)
{
  int ret = OB_SUCCESS;
  const int64_t io_timeout_ms = GCONF._data_storage_io_timeout / 1000L;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(async_write_block(write_info, macro_handle))) {
    STORAGE_LOG(WARN, "Fail to sync write block, ", K(ret), K(write_info));
  } else if (OB_FAIL(macro_handle.wait(io_timeout_ms))) {
    STORAGE_LOG(WARN, "Fail to wait io finish, ", K(ret));
  }
  return ret;
}
//------------------ Implementation of ObLocalFileSystem -----------------------//
ObLocalFileSystem::ObLocalFileSystem()
    : ObStoreFileSystem(),
      is_opened_(false),
      fd_(),
      macro_block_size_(0),
      data_file_size_(0),
      datafile_disk_percentage_(0),
      super_block_buf_holder_(),
      file_allocator_(),
      store_file_(ObStoreFile::get_instance()),
      server_root_(nullptr),
      sstable_dir_(nullptr)
{
  store_path_[0] = '\0';
  fd_.disk_id_.disk_idx_ = 0;
  fd_.disk_id_.install_seq_ = 0;
}

ObLocalFileSystem::~ObLocalFileSystem()
{
  destroy();
}

int ObLocalFileSystem::init(const ObStorageEnv& storage_env, ObPartitionService& partition_service)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "The ObLocalFileSystem has been inited, ", K(ret));
  } else if (OB_UNLIKELY(!storage_env.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument, ", K(ret), K(storage_env));
  } else if (OB_FAIL(databuff_printf(store_path_,
                 OB_MAX_FILE_NAME_LENGTH,
                 "%s/%s/%s",
                 storage_env.data_dir_,
                 BLOCK_SSTBALE_DIR_NAME,
                 BLOCK_SSTBALE_FILE_NAME))) {
    STORAGE_LOG(WARN, "The block file path is too long, ", K(ret), K(storage_env.data_dir_));
  } else if (OB_FAIL(super_block_buf_holder_.init(ObSuperBlockHeader::OB_MAX_SUPER_BLOCK_SIZE))) {
    STORAGE_LOG(WARN, "Fail to init super block buffer holder, ", K(ret));
  } else {
    macro_block_size_ = storage_env.default_block_size_;
    data_file_size_ = storage_env.disk_avail_space_;
    datafile_disk_percentage_ = storage_env.datafile_disk_percentage_;
    partition_service_ = &partition_service;
    sstable_dir_ = storage_env.sstable_dir_;
    is_inited_ = true;
  }

  if (OB_SUCC(ret)) {
    bool is_exist = false;
    if (OB_FAIL(this->open(is_exist))) {
      STORAGE_LOG(WARN, "Fail to local block_file, ", K(ret));
    } else if (is_exist) {
      int64_t version = 0;
      if (OB_FAIL(get_super_block_version(version))) {
        LOG_WARN("fail to get super block version", K(ret));
      } else {
        if (OB_SUPER_BLOCK_V2 == version) {
          if (OB_FAIL(read_old_super_block(old_super_block_))) {
            LOG_WARN("fail to read old super block", K(ret));
          } else {
            LOG_INFO("succeed to read old super block", K(old_super_block_));
            const int64_t macro_block_size = old_super_block_.content_.macro_block_size_;
            const int64_t total_macro_block_count = old_super_block_.content_.total_macro_block_count_;
            super_block_.content_.macro_block_size_ = macro_block_size;
            super_block_.content_.total_macro_block_count_ = total_macro_block_count;
            super_block_.content_.total_file_size_ = macro_block_size * total_macro_block_count;
            super_block_.content_.create_timestamp_ = ObTimeUtility::current_time();
          }
        } else if (OB_SUPER_BLOCK_V3 == version) {
          if (OB_FAIL(read_server_super_block(super_block_))) {
            LOG_WARN("fail to read server super block", K(ret));
          } else {
            LOG_INFO("succeed to read super block", K(super_block_));
          }
        }
      }
    } else {
      // empty file system, init it
      if (OB_FAIL(super_block_.format_startup_super_block(storage_env.default_block_size_, data_file_size_))) {
        STORAGE_LOG(WARN, "Fail to format super block, ", K(ret));
      } else if (OB_FAIL(write_server_super_block(super_block_))) {
        STORAGE_LOG(WARN, "Fail to write super block, ", K(ret));
      } else {
        STORAGE_LOG(INFO, "Success to format super block, ", K(super_block_));
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(store_file_.init(storage_env, this))) {
      STORAGE_LOG(WARN, "init store file failed, ", K(ret));
      is_inited_ = false;
    }
  }

  if (IS_NOT_INIT) {
    destroy();
  }

  return ret;
}

int ObLocalFileSystem::open(bool& exist)
{
  int ret = OB_SUCCESS;
  exist = false;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "The ObLocalFileSystem hasn't been inited", K(ret));
  } else if (fd_.is_valid()) {
    ret = OB_ERR_SYS;
    LOG_ERROR("cannot open twice", K(ret), K_(fd));
  } else if (OB_FAIL(FileDirectoryUtils::is_exists(store_path_, exist))) {
    exist = false;
    STORAGE_LOG(WARN, "Fail to check if file exist, ", K(ret), K(store_path_));
  } else {
    int open_flag = exist ? O_DIRECT | O_RDWR | O_LARGEFILE : O_CREAT | O_EXCL | O_DIRECT | O_RDWR | O_LARGEFILE;
    if ((fd_.fd_ = ::open(store_path_, open_flag, S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH)) < 0) {
      ret = OB_IO_ERROR;
      STORAGE_LOG(ERROR, "open file error", K(ret), K(store_path_), K(errno), KERRMSG);
    } else if (OB_FAIL(ObIOManager::get_instance().add_disk(fd_, ObDisk::DEFAULT_SYS_IO_PERCENT))) {
      STORAGE_LOG(WARN, "add_disk failed", K(ret), K_(fd));
    } else {
      is_opened_ = true;
      if (OB_FAIL(alloc_file(server_root_))) {
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

  // init file space if not exist
  if (OB_SUCC(ret) && !exist) {
    // check space availability if file not exist
    int64_t total_space = 0;
    int64_t free_space = 0;
    if (OB_FAIL(FileDirectoryUtils::get_disk_space(sstable_dir_, total_space, free_space))) {
      STORAGE_LOG(WARN, "Failed to get disk space ", K(ret));
    } else {
      if (data_file_size_ <= 0) {
        data_file_size_ = total_space * datafile_disk_percentage_ / 100;
      }
      if (data_file_size_ > free_space) {
        ret = OB_CS_OUTOF_DISK_SPACE;
        STORAGE_LOG(ERROR, "data file size is too large, ", K(ret), K(free_space), K(data_file_size_));
      } else {
        int64_t falloc_size = lower_align(data_file_size_, macro_block_size_);
        if (OB_FAIL(fallocate(fd_.fd_, 0 /*MODE*/, 0 /*offset*/, falloc_size))) {
          ret = OB_IO_ERROR;
          STORAGE_LOG(ERROR, "allocate file error", K(store_path_), K(falloc_size), K(errno), KERRMSG, K(ret));
        }
      }
    }
  }
  LOG_INFO("finish open store file", K(ret), K(fd_), K(store_path_), K(data_file_size_));
  return ret;
}

int ObLocalFileSystem::async_write(const ObStoreFileWriteInfo& write_info, common::ObIOHandle& io_handle)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_opened_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "The ObLocalFileSystem has not been started, ", K(ret));
  } else if (OB_UNLIKELY(!write_info.is_valid() || macro_block_size_ < write_info.size_)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(ERROR, "Invalid argument, ", K(ret), K(write_info), K(macro_block_size_));
  } else if (write_info.block_id_.block_index() < RESERVED_MACRO_BLOCK_INDEX) {  // 0 and 1 is super block
    ret = OB_ERR_SYS;
    STORAGE_LOG(ERROR,
        "not allow write super block",
        K(ret),
        K(write_info),
        "block_index",
        write_info.block_id_.block_index(),
        LITERAL_K(RESERVED_MACRO_BLOCK_INDEX));
  } else if (write_info.is_reuse_macro_block()) {
    STORAGE_LOG(DEBUG, "no need async write reuse block for local file system", K(write_info));
  } else {
    ObIOInfo io_info;

    ObIOPoint& io_point = io_info.io_points_[0];
    io_point.fd_ = fd_;
    io_point.offset_ = write_info.block_id_.block_index() * macro_block_size_;
    io_point.size_ = static_cast<int32_t>(write_info.size_);
    io_point.write_buf_ = write_info.buf_;

    io_info.size_ = io_point.size_;
    io_info.io_desc_ = write_info.io_desc_;
    io_info.io_desc_.mode_ = ObIOMode::IO_MODE_WRITE;
    io_info.batch_count_ = 1;

    if (NULL == write_info.io_callback_) {
      if (OB_FAIL(ObIOManager::get_instance().aio_write(io_info, io_handle))) {
        STORAGE_LOG(WARN, "Fail to aio_write, ", K(ret), K(write_info));
      }
    } else {
      if (OB_FAIL(ObIOManager::get_instance().aio_write(io_info, *write_info.io_callback_, io_handle))) {
        STORAGE_LOG(WARN, "Fail to aio_write, ", K(ret), K(write_info));
      }
    }
  }
  return ret;
};

int ObLocalFileSystem::async_read(const ObStoreFileReadInfo& read_info, common::ObIOHandle& io_handle)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_opened_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "The ObLocalFileSystem has not been started, ", K(ret));
  } else if (OB_UNLIKELY(!read_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument, ", K(ret), K(read_info));
  } else {
    ObIOInfo io_info;
    io_info.offset_ = read_info.offset_;
    io_info.size_ = static_cast<int32_t>(read_info.size_);
    io_info.io_desc_ = read_info.io_desc_;
    io_info.batch_count_ = 1;
    io_info.io_desc_.mode_ = ObIOMode::IO_MODE_READ;

    ObIOPoint& io_point = io_info.io_points_[0];
    io_point.fd_ = fd_;
    io_point.size_ = io_info.size_;
    io_point.offset_ =
        read_info.macro_block_ctx_->get_macro_block_id().block_index() * macro_block_size_ + read_info.offset_;

    io_handle.reset();
    if (NULL == read_info.io_callback_) {
      if (OB_FAIL(ObIOManager::get_instance().aio_read(io_info, io_handle))) {
        STORAGE_LOG(WARN, "Fail to aio_read, ", K(read_info), K(ret));
      }
    } else {
      if (OB_FAIL(ObIOManager::get_instance().aio_read(io_info, *read_info.io_callback_, io_handle))) {
        STORAGE_LOG(WARN, "Fail to aio_read, ", K(read_info), K(ret));
      }
    }
  }
  return ret;
}

int ObLocalFileSystem::write_server_super_block(const ObServerSuperBlock& super_block)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_opened_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("The ObLocalFileSystem has not been started, ", K(ret));
  } else if (OB_UNLIKELY(!super_block.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument, ", K(ret), K(super_block));
  } else if (OB_FAIL(super_block_buf_holder_.serialize_super_block(super_block))) {
    LOG_ERROR("failed to serialize super block", K(ret), K_(super_block_buf_holder), K(super_block));
  } else if (OB_FAIL(inner_write_super_block())) {
    LOG_WARN("write super block from buffer hold fail", K(ret), K(super_block_buf_holder_));
  } else {
    super_block_ = super_block;
    old_super_block_.reset();
  }
  return ret;
}

int ObLocalFileSystem::fsync()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_opened_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "The ObStoreFile has not been started, ", K(ret));
  } else if (0 != ::fsync(fd_.fd_)) {
    ret = OB_IO_ERROR;
    STORAGE_LOG(ERROR, "Fail to fsync data file, ", K(ret), K(fd_), KERRMSG);
  }
  return ret;
}

int ObLocalFileSystem::init_file_ctx(const ObStoreFileType& file_type, blocksstable::ObStoreFileCtx& file_ctx) const
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(file_ctx.init(STORE_FILE_SYSTEM_LOCAL, file_type, INT64_MAX))) {
    STORAGE_LOG(WARN, "failed to init write ctx", K(ret));
  }
  return ret;
}

bool ObLocalFileSystem::is_disk_full() const
{
  return store_file_.is_disk_full();
}

void ObLocalFileSystem::destroy()
{
  int tmp_ret = OB_SUCCESS;
  STORAGE_LOG(INFO, "ObLocalFileSystem will be destroyed!");

  if (fd_.is_valid()) {
    if (OB_SUCCESS != (tmp_ret = ObIOManager::get_instance().delete_disk(fd_))) {
      LOG_WARN("failed to delete disk", K(tmp_ret), K(fd_));
    }

    if (0 != ::fsync(fd_.fd_)) {
      STORAGE_LOG(ERROR, "data file sync data error", K_(fd), K(errno), KERRMSG);
    }
    if (0 != ::close(fd_.fd_)) {
      STORAGE_LOG(ERROR, "data file close error", K_(fd), K(errno), KERRMSG);
    }
    fd_.fd_ = -1;
  }

  if (nullptr != server_root_) {
    free_file(server_root_);
    server_root_ = nullptr;
  }
  store_file_.destroy();

  super_block_buf_holder_.reset();
  macro_block_size_ = 0;
  data_file_size_ = 0;
  file_allocator_.destroy();
  is_opened_ = false;
  is_inited_ = false;
}

int ObLocalFileSystem::start()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(store_file_.open())) {
    LOG_WARN("open store file fail", K(ret));
  } else {
    // open server_root
    server_root_->set_fd(fd_.fd_);
  }
  return ret;
}

void ObLocalFileSystem::stop()
{
  store_file_.stop();
}

void ObLocalFileSystem::wait()
{
  store_file_.wait();
}

int ObLocalFileSystem::alloc_file(ObStorageFile*& file)
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
    local_file->fd_ = fd_.fd_;
    file = static_cast<ObStorageFile*>(local_file);
  }
  return ret;
}

int ObLocalFileSystem::free_file(ObStorageFile*& file)
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

ObStorageFileHandle& ObLocalFileSystem::get_server_root_handle()
{
  return server_root_handle_;
}

int ObLocalFileSystem::read_server_super_block(ObServerSuperBlock& super_block)
{
  int ret = OB_SUCCESS;
  bool is_master_correct = true;
  bool is_backup_correct = true;
  ObServerSuperBlock master_sb;
  ObServerSuperBlock backup_sb;

  if (OB_FAIL(inner_read_super_block(MASTER_SUPER_BLOCK_OFFSET, master_sb))) {
    is_master_correct = false;
    STORAGE_LOG(WARN, "load master super block error.", K(ret));
  }
  if (OB_FAIL(inner_read_super_block(BACKUP_SUPER_BLOCK_OFFSET, backup_sb))) {
    is_backup_correct = false;
    STORAGE_LOG(WARN, "load backup super block error.", K(ret));
  }

  if (is_master_correct || is_backup_correct) {
    ret = OB_SUCCESS;
    if (is_master_correct) {
      if (is_backup_correct && master_sb.content_.modify_timestamp_ < backup_sb.content_.modify_timestamp_) {
        super_block = backup_sb;
      } else {
        super_block = master_sb;
      }
    } else {
      // backup superblock is correct
      super_block = backup_sb;
    }
  }
  return ret;
}

template <typename SuperBlockClass>
int ObLocalFileSystem::inner_read_super_block(const int64_t offset, SuperBlockClass& super_block)
{
  int ret = OB_SUCCESS;
  int64_t read_size = 0;

  if (super_block_buf_holder_.get_len() !=
      (read_size =
              ob_pread(fd_.fd_, super_block_buf_holder_.get_buffer(), super_block_buf_holder_.get_len(), offset))) {
    ret = OB_IO_ERROR;
    STORAGE_LOG(WARN,
        "read superblock error.",
        K(ret),
        K(offset),
        K(read_size),
        K(errno),
        K_(super_block_buf_holder),
        K_(fd),
        KERRMSG);
  } else if (OB_FAIL(super_block_buf_holder_.deserialize_super_block(super_block))) {
    STORAGE_LOG(WARN, "deserialize superblock error.", K(ret), K_(super_block_buf_holder));
  } else if (!super_block.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "super block is invalid", K(ret), K(super_block));
  } else {
    STORAGE_LOG(INFO, "finish read_super_block", K(ret), K(offset), K(super_block));
  }
  return ret;
}

int ObLocalFileSystem::inner_write_super_block()
{
  int ret = OB_SUCCESS;
  int64_t write_num = 0;

  if (super_block_buf_holder_.get_len() != (write_num = ob_pwrite(fd_.fd_,
                                                super_block_buf_holder_.get_buffer(),
                                                super_block_buf_holder_.get_len(),
                                                BACKUP_SUPER_BLOCK_OFFSET))) {
    ret = OB_IO_ERROR;
    LOG_WARN("write backup super block meta block error.",
        K(ret),
        KERRMSG,
        K_(fd),
        K_(super_block_buf_holder),
        K(write_num));
  } else {
    if (super_block_buf_holder_.get_len() != (write_num = ob_pwrite(fd_.fd_,
                                                  super_block_buf_holder_.get_buffer(),
                                                  super_block_buf_holder_.get_len(),
                                                  MASTER_SUPER_BLOCK_OFFSET))) {
      ret = OB_IO_ERROR;
      LOG_WARN("write master super block meta block error.",
          K(ret),
          KERRMSG,
          K_(fd),
          K_(super_block_buf_holder),
          K(write_num));
    }

    // backup super block has been success, allow master super block write fail
    ret = OB_SUCCESS;
    if (0 != ::fsync(fd_.fd_)) {
      ret = OB_IO_ERROR;
      LOG_ERROR("Fail to fsync data file, ", K(ret), KERRMSG);
    }
#ifdef ERRSIM
    if (OB_SUCC(ret)) {
      ret = E(EventTable::EN_WRITE_SUPER_BLOCK_FAILED) OB_SUCCESS;
      if (OB_FAIL(ret)) {
        LOG_WARN("error sim, write super block failed");
      }
    }
#endif
  }
  return ret;
}

int ObLocalFileSystem::unlink_block(const ObStorageFile& file, const MacroBlockId& macro_id)
{
  UNUSED(file);
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_opened_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("The ObLocalFileSystem hasn't been inited", K(ret));
  } else if (!macro_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(macro_id));
  } else {
    store_file_.dec_ref(macro_id);
  }
  return ret;
}

int ObLocalFileSystem::link_block(const ObStorageFile& file, const MacroBlockId& macro_id)
{
  UNUSED(file);
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_opened_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("The ObLocalFileSystem hasn't been inited", K(ret));
  } else if (!macro_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(macro_id));
  } else {
    store_file_.inc_ref(macro_id);
  }
  return ret;
}

int ObLocalFileSystem::get_macro_block_info(
    const ObTenantFileKey& file_key, const MacroBlockId& macro_block_id, ObMacroBlockInfo& macro_block_info)
{
  UNUSED(file_key);
  return store_file_.get_macro_block_info(macro_block_id.block_index(), macro_block_info);
}

int ObLocalFileSystem::report_bad_block(
    const MacroBlockId& macro_block_id, const int64_t error_type, const char* error_msg, const char* file_path)
{
  UNUSED(file_path);
  return store_file_.report_bad_block(macro_block_id, error_type, error_msg);
}

int ObLocalFileSystem::get_bad_block_infos(common::ObArray<ObBadBlockInfo>& bad_block_infos)
{
  return store_file_.get_bad_block_infos(bad_block_infos);
}

int64_t ObLocalFileSystem::get_free_macro_block_count() const
{
  return store_file_.get_free_macro_block_count();
}

int ObLocalFileSystem::get_marker_status(ObMacroBlockMarkerStatus& status)
{
  return store_file_.get_store_status(status);
}

int ObLocalFileSystem::read_old_super_block(ObSuperBlockV2& super_block)
{
  int ret = OB_SUCCESS;
  bool is_master_correct = true;
  bool is_backup_correct = true;
  ObSuperBlockV2 master_sb;
  ObSuperBlockV2 backup_sb;

  if (OB_FAIL(inner_read_super_block(MASTER_SUPER_BLOCK_OFFSET, master_sb))) {
    is_master_correct = false;
    STORAGE_LOG(WARN, "load master super block error.", K(ret));
  }
  if (OB_FAIL(inner_read_super_block(BACKUP_SUPER_BLOCK_OFFSET, backup_sb))) {
    is_backup_correct = false;
    STORAGE_LOG(WARN, "load backup super block error.", K(ret));
  }

  if (is_master_correct || is_backup_correct) {
    ret = OB_SUCCESS;
    if (is_master_correct) {
      if (is_backup_correct && master_sb.content_.modify_timestamp_ < backup_sb.content_.modify_timestamp_) {
        super_block = backup_sb;
      } else {
        super_block = master_sb;
      }
    } else {
      // backup superblock is correct
      super_block = backup_sb;
    }
  }
  return ret;
}

int ObLocalFileSystem::get_super_block_version(int64_t& super_block_version)
{
  int ret = OB_SUCCESS;
  super_block_version = 0;
  if (OB_FAIL(inner_get_super_block_version(MASTER_SUPER_BLOCK_OFFSET, super_block_version))) {
    LOG_WARN("fail to get super block version from master", K(ret));
    if (OB_FAIL(inner_get_super_block_version(BACKUP_SUPER_BLOCK_OFFSET, super_block_version))) {
      LOG_WARN("fail to get super block version from backup", K(ret));
    }
  }
  return ret;
}

int ObLocalFileSystem::inner_get_super_block_version(const int64_t offset, int64_t& version)
{
  int ret = OB_SUCCESS;
  int64_t read_size = 0;
  version = 0;

  if (super_block_buf_holder_.get_len() !=
      (read_size =
              ob_pread(fd_.fd_, super_block_buf_holder_.get_buffer(), super_block_buf_holder_.get_len(), offset))) {
    ret = OB_IO_ERROR;
    STORAGE_LOG(WARN,
        "read superblock error.",
        K(ret),
        K(offset),
        K(read_size),
        K(errno),
        K_(super_block_buf_holder),
        K_(fd),
        KERRMSG);
  } else if (OB_FAIL(super_block_buf_holder_.deserialize_super_block_header_version(version))) {
    STORAGE_LOG(WARN, "deserialize superblock error.", K(ret), K_(super_block_buf_holder));
  }

  STORAGE_LOG(INFO, "finish read_super_block_header_version", K(ret), K(offset), K(version));
  return ret;
}

int ObLocalFileSystem::resize_file(const int64_t new_data_file_size, const int64_t new_data_file_disk_percentage)
{
  int ret = OB_SUCCESS;
  int64_t new_cal_data_file_size = new_data_file_size;
  int64_t total_space = 0;
  int64_t free_space = 0;
  if (OB_FAIL(FileDirectoryUtils::get_disk_space(sstable_dir_, total_space, free_space))) {
    STORAGE_LOG(WARN, "Failed to get disk space ", K(ret));
  } else if (new_cal_data_file_size <= 0) {
    new_cal_data_file_size = total_space * new_data_file_disk_percentage / 100;
  }
#ifdef ERRSIM
  if (OB_SUCC(ret)) {
    ret = E(EventTable::EN_RESIZE_PHYSICAL_FILE_FAILED) OB_SUCCESS;
    if (OB_FAIL(ret)) {
      LOG_WARN("error sim, resize physical file failed");
    }
  }
#endif
  if (OB_SUCC(ret)) {
    const int64_t curr_aligned_file_size = lower_align(data_file_size_, macro_block_size_);
    const int64_t new_aligned_file_size = lower_align(new_cal_data_file_size, macro_block_size_);
    if (curr_aligned_file_size > new_aligned_file_size) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("can not resize file to smaller", K(ret), K(curr_aligned_file_size), K(new_aligned_file_size));
    } else if (curr_aligned_file_size == new_aligned_file_size) {
      LOG_INFO("new file size is equal to current file size, do not need to resize file",
          K(curr_aligned_file_size),
          K(new_aligned_file_size));
    } else {
      const int64_t offset = curr_aligned_file_size;
      const int64_t delta_size = new_aligned_file_size - curr_aligned_file_size;
      if (OB_FAIL(fallocate(fd_.fd_, 0, offset, delta_size))) {
        LOG_WARN("fail to expand file size", K(ret), K(offset), K(delta_size), K(errno), KERRMSG);
      } else {
        data_file_size_ = new_cal_data_file_size;
        datafile_disk_percentage_ = new_data_file_disk_percentage;
        LOG_INFO("succeed to resize file", K(data_file_size_), K(datafile_disk_percentage_));
      }
    }
  }
  return ret;
}

}  // namespace blocksstable
}  // namespace oceanbase
