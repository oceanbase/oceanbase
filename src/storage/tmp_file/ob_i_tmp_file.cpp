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

#include "storage/tmp_file/ob_i_tmp_file.h"
#include "share/ob_errno.h"
#include "share/config/ob_server_config.h"
#include "storage/tmp_file/ob_tmp_file_io_ctx.h"

namespace oceanbase
{
namespace tmp_file
{
ObITmpFileHandle::ObITmpFileHandle(ObITmpFile *tmp_file)
  : ptr_(tmp_file)
{
  if (ptr_ != nullptr) {
    ptr_->inc_ref_cnt();
  }
}

ObITmpFileHandle::ObITmpFileHandle(const ObITmpFileHandle &handle)
  : ptr_(nullptr)
{
  operator=(handle);
}

ObITmpFileHandle & ObITmpFileHandle::operator=(const ObITmpFileHandle &other)
{
  if (other.get() != ptr_) {
    reset();
    ptr_ = other.get();
    if (ptr_ != nullptr) {
      ptr_->inc_ref_cnt();
    }
  }
  return *this;
}

void ObITmpFileHandle::reset()
{
  if (ptr_ != nullptr) {
    int64_t ref_cnt = -1;
    ptr_->dec_ref_cnt(ref_cnt);
    if (OB_UNLIKELY(ref_cnt < 0)) {
      int ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("invalid ref cnt", K(ret), KP(ptr_), K(ref_cnt));
    } else if (ref_cnt == 0) {
      ptr_->~ObITmpFile();
    }
    ptr_ = nullptr;
  }
}

int ObITmpFileHandle::init(ObITmpFile *tmp_file)
{
  int ret = OB_SUCCESS;

  if (is_inited()) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret), KP(ptr_));
  } else if (OB_ISNULL(tmp_file)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(tmp_file));
  } else {
    ptr_ = tmp_file;
    ptr_->inc_ref_cnt();
  }

  return ret;
}

ObITmpFile::ObITmpFile()
    : is_inited_(false),
      mode_(ObTmpFileMode::INVALID),
      tenant_id_(OB_INVALID_TENANT_ID),
      dir_id_(ObTmpFileGlobal::INVALID_TMP_FILE_DIR_ID),
      fd_(ObTmpFileGlobal::INVALID_TMP_FILE_FD),
      is_deleting_(false),
      is_sealed_(false),
      ref_cnt_(0),
      truncated_offset_(0),
      read_offset_(0),
      file_size_(0),
      meta_lock_(common::ObLatchIds::TMP_FILE_LOCK),
      stat_lock_(common::ObLatchIds::TMP_FILE_LOCK),
      callback_allocator_(nullptr),
      trace_id_(),
      birth_ts_(-1),
      label_(),
      file_type_(OB_TMP_FILE_TYPE::NORMAL),
      compressible_fd_(ObTmpFileGlobal::INVALID_TMP_FILE_FD),
      compressible_file_(nullptr)
{
}

ObITmpFile::~ObITmpFile()
{
}

int ObITmpFile::init(const int64_t tenant_id,
                     const int64_t dir_id,
                     const int64_t fd,
                     ObIAllocator *callback_allocator,
                     const char* label)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret), KPC(this));
  } else if (OB_UNLIKELY(ObTmpFileGlobal::INVALID_TMP_FILE_FD == fd ||
                         ObTmpFileGlobal::INVALID_TMP_FILE_DIR_ID == dir_id ||
                         !is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(fd), K(dir_id));
  } else if (OB_ISNULL(callback_allocator)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(fd), KP(callback_allocator));
  } else {
    is_inited_ = true;
    dir_id_ = dir_id;
    fd_ = fd;
    tenant_id_ = tenant_id;
    callback_allocator_ = callback_allocator;

    /******for virtual table begin******/
    ObCurTraceId::TraceId *cur_trace_id = ObCurTraceId::get_trace_id();
    if (nullptr != cur_trace_id) {
      trace_id_ = *cur_trace_id;
    } else {
      trace_id_.init(GCONF.self_addr_);
    }
    birth_ts_ = ObTimeUtility::current_time();
    if (NULL != label) {
      label_.assign_strive(label);
    }
    /******for virtual table end******/
  }
  return ret;
}

void ObITmpFile::reset()
{
  if (is_inited_) {
    is_inited_ = false;
    mode_ = ObTmpFileMode::INVALID;
    tenant_id_ = OB_INVALID_TENANT_ID;
    dir_id_ = ObTmpFileGlobal::INVALID_TMP_FILE_DIR_ID;
    fd_ = ObTmpFileGlobal::INVALID_TMP_FILE_FD;
    is_deleting_ = false;
    is_sealed_ = false;
    ref_cnt_ = 0;
    truncated_offset_ = 0;
    read_offset_ = 0;
    file_size_ = 0;
    callback_allocator_ = nullptr;
    /******for virtual table begin******/
    // common info
    trace_id_.reset();
    birth_ts_ = -1;
    label_.reset();
    file_type_ = OB_TMP_FILE_TYPE::NORMAL;
    compressible_fd_ = ObTmpFileGlobal::INVALID_TMP_FILE_FD;
    compressible_file_ = nullptr;
    /******for virtual table end******/
  }
}

int ObITmpFile::delete_file()
{
  int ret = OB_SUCCESS;
  LOG_INFO("tmp file delete start", K(fd_));
  SpinWLockGuard guard(meta_lock_);
  if (IS_INIT && !is_deleting_) {
    LOG_INFO("tmp file inner delete start", KR(ret), KPC(this));
    if (OB_FAIL(inner_delete_file_())) {
      LOG_WARN("fail to inner delete file", KR(ret), KPC(this));
    } else {
      // read, write, truncate, flush and evict function will fail when is_deleting_ == true.
      ATOMIC_SET(&is_deleting_, true);
    }
  }

  LOG_INFO("tmp file delete over", KR(ret), KPC(this));
  return ret;
}

int ObITmpFile::aio_pread(ObTmpFileIOReadCtx &io_ctx)
{
  int ret = OB_SUCCESS;
  LOG_DEBUG("aio pread start", KR(ret), K(fd_), K(io_ctx));
  SpinRLockGuard guard(meta_lock_);

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tmp file has not been inited", KR(ret), K(tenant_id_), KPC(this));
  } else if (OB_UNLIKELY(is_deleting_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("attempt to read a deleting file", KR(ret), K(fd_));
  } else {
    if (io_ctx.get_start_read_offset_in_file() < 0) {
      io_ctx.set_read_offset_in_file(read_offset_);
    }
    if (0 != io_ctx.get_start_read_offset_in_file() % ObTmpFileGlobal::PAGE_SIZE
        || 0 != io_ctx.get_todo_size() % ObTmpFileGlobal::PAGE_SIZE) {
      io_ctx.set_is_unaligned_read(true);
    }

    LOG_DEBUG("start to inner read tmp file", K(fd_), K(io_ctx), KPC(this));
    if (OB_UNLIKELY(!io_ctx.is_valid())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", KR(ret), K(fd_), K(io_ctx));
    } else if (OB_UNLIKELY(io_ctx.get_todo_size() <= 0)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", KR(ret), K(fd_), K(io_ctx));
    } else if (OB_UNLIKELY(io_ctx.get_start_read_offset_in_file() >= file_size_)) {
      ret = OB_ITER_END;
      LOG_WARN("iter end", KR(ret), K(fd_), K(file_size_), K(io_ctx));
    } else if (io_ctx.get_start_read_offset_in_file() < truncated_offset_ &&
              OB_FAIL(inner_read_truncated_part_(io_ctx))) {
      LOG_WARN("fail to read truncated part", KR(ret), K(fd_), K(io_ctx), K(truncated_offset_), KPC(this));
    } else if (OB_UNLIKELY(io_ctx.get_todo_size() == 0)) {
      // do nothing
    } else if (OB_FAIL(inner_read_valid_part_(io_ctx))) {
      LOG_WARN("fail to read valid part", KR(ret), K(fd_), K(io_ctx), KPC(this));
    }
  }
  LOG_DEBUG("aio pread over", KR(ret), K(fd_), KPC(this), K(io_ctx));
  return ret;
}

int ObITmpFile::inner_read_truncated_part_(ObTmpFileIOReadCtx &io_ctx)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(io_ctx.get_start_read_offset_in_file() >= truncated_offset_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("read offset should be less than truncated offset", KR(ret), K(fd_), K(io_ctx), K(truncated_offset_));
  } else if (OB_UNLIKELY(!io_ctx.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid io_ctx", KR(ret), K(fd_), K(io_ctx));
  } else if (OB_UNLIKELY(io_ctx.get_todo_size() == 0)) {
    // do nothing
  } else {
    int64_t total_truncated_page_read_cnt = 0;
    const int64_t start_read_offset = io_ctx.get_start_read_offset_in_file();
    int64_t read_size = MIN(truncated_offset_ - io_ctx.get_start_read_offset_in_file(),
                            io_ctx.get_todo_size());
    char *read_buf = io_ctx.get_todo_buffer();
    if (OB_UNLIKELY(!io_ctx.check_buf_range_valid(read_buf, read_size))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid buf range", KR(ret), K(fd_), K(read_buf), K(read_size), K(io_ctx));
    } else if (FALSE_IT(MEMSET(read_buf, 0, read_size))) {
    } else if (OB_FAIL(io_ctx.update_data_size(read_size))) {
      LOG_WARN("fail to update data size", KR(ret), K(fd_), K(read_size));
    } else if (FALSE_IT(total_truncated_page_read_cnt = (get_page_end_offset_(io_ctx.get_start_read_offset_in_file()) -
                                                         get_page_begin_offset_(start_read_offset)) /
                                                         ObTmpFileGlobal::PAGE_SIZE)) {
    } else if (FALSE_IT(io_ctx.update_read_truncated_stat(total_truncated_page_read_cnt))) {
    } else if (OB_UNLIKELY(io_ctx.get_todo_size() > 0 &&
                           truncated_offset_ == file_size_)) {
      ret = OB_ITER_END;
      LOG_WARN("iter end", KR(ret), K(fd_), K(file_size_), K(truncated_offset_), K(io_ctx));
    }
  }

  return ret;
}

bool ObITmpFile::can_remove()
{
  SpinRLockGuard guard(meta_lock_);
  return is_deleting_ && get_ref_cnt() == 1;
}

bool ObITmpFile::is_deleting()
{
  SpinRLockGuard guard(meta_lock_);
  return is_deleting_;
}

int64_t ObITmpFile::get_file_size()
{
  SpinRLockGuard guard(meta_lock_);
  return file_size_;
}

void ObITmpFile::update_read_offset(int64_t read_offset)
{
  SpinWLockGuard guard(meta_lock_);
  if (read_offset > read_offset_) {
    read_offset_ = read_offset;
  }
}

void ObITmpFile::set_read_stats_vars(const ObTmpFileIOReadCtx &ctx, const int64_t read_size)
{
  ObSpinLockGuard guard(stat_lock_);
  inner_set_read_stats_vars_(ctx, read_size);
}

void ObITmpFile::set_write_stats_vars(const ObTmpFileIOWriteCtx &ctx)
{
  ObSpinLockGuard guard(stat_lock_);
  inner_set_write_stats_vars_(ctx);
}

}  // end namespace tmp_file
}  // end namespace oceanbase
