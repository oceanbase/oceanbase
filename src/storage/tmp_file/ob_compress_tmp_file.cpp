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
#include "storage/tmp_file/ob_tmp_file_manager.h"
#include "storage/tmp_file/ob_compress_tmp_file.h"

namespace oceanbase
{
namespace tmp_file
{

ObCompressTmpFile::ObCompressTmpFile()
  : COMPRESS_UNIT_SIZE(0),
    ref_cnt_(0),
    fd_(ObTmpFileGlobal::INVALID_TMP_FILE_FD),
    compressed_fd_(ObTmpFileGlobal::INVALID_TMP_FILE_FD),
    end_offset_for_compress_(0),
    compressed_read_offset_(0),
    read_offset_(0),
    is_sealed_(false),
    is_deleting_(false),
    err_marked_(false),
    next_unit_header_inited_(false),
    next_unit_header_(),
    buf_allocator_(nullptr),
    compressor_(nullptr),
    file_size_(0),
    moved_data_size_(0),
    lock_(common::ObLatchIds::TMP_FILE_LOCK),
    move_data_lock_(common::ObLatchIds::TMP_FILE_LOCK),
    wait_list_(),
    decompress_buf_(nullptr),
    decompress_buf_start_offset_(-1),
    decompress_buf_valid_len_(0),
    read_buf_(nullptr),
    read_buf_start_offset_(-1),
    read_buf_valid_len_(0),
    file_bunch_(nullptr) {}

ObCompressTmpFile::~ObCompressTmpFile()
{
  reset();
}

void ObCompressTmpFile::reset()
{
  COMPRESS_UNIT_SIZE = 0;
  ref_cnt_ = 0;
  fd_ = ObTmpFileGlobal::INVALID_TMP_FILE_FD;
  compressed_fd_ = ObTmpFileGlobal::INVALID_TMP_FILE_FD;
  buffer_tmp_file_handle_.reset();
  compressed_tmp_file_handle_.reset();

  end_offset_for_compress_ = 0;
  compressed_read_offset_ = 0;
  read_offset_ = 0;
  is_sealed_ = false;
  is_deleting_ = false;
  err_marked_ = false;
  next_unit_header_inited_ = false;
  next_unit_header_.reset();
  compressor_ = nullptr;
  file_size_ = 0;
  moved_data_size_ = 0;
  wait_list_.reset();
  if (nullptr != decompress_buf_) {
    buf_allocator_->free(decompress_buf_);
    decompress_buf_ = nullptr;
  }
  decompress_buf_start_offset_ = -1;
  decompress_buf_valid_len_ = 0;
  if (nullptr != read_buf_) {
    buf_allocator_->free(read_buf_);
    read_buf_ = nullptr;
  }
  read_buf_start_offset_= -1;
  read_buf_valid_len_ = 0;
  buf_allocator_ = nullptr;
  file_bunch_ = nullptr;
}

int ObCompressTmpFile::init(
    const uint64_t tenant_id,
    int64_t &fd,
    const int64_t dir_id,
    const ObCompressorType comptype,
    const int64_t comp_unit_size,
    const char* const label,
    ObFIFOAllocator* buf_allocator,
    const void* file_bunch)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(INVALID_COMPRESSOR == comptype
                  || 0 > comp_unit_size
                  || NULL == buf_allocator)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid comptype or comp_unit_size", KR(ret), K(comptype), K(comp_unit_size), KP(buf_allocator));
  } else if (OB_FAIL(FILE_MANAGER_INSTANCE_WITH_MTL_SWITCH.open(tenant_id, fd_, dir_id, label))) {
    LOG_WARN("fail to open buffer tmp file", KR(ret), K(tenant_id), K(dir_id), KP(label), K(compressed_fd_));
  } else if (OB_FAIL(FILE_MANAGER_INSTANCE_WITH_MTL_SWITCH.get_tmp_file(tenant_id, fd_, buffer_tmp_file_handle_))) {
    LOG_WARN("fail to get buffer tmp file handle", KR(ret), K(tenant_id), K(dir_id), KP(label), K(fd_));
  } else if (OB_FAIL(FILE_MANAGER_INSTANCE_WITH_MTL_SWITCH.open(tenant_id, compressed_fd_, dir_id, label))) {
    LOG_WARN("fail to open compressed tmp file", KR(ret), K(fd_), K(tenant_id), K(dir_id), KP(label));
  } else if (OB_FAIL(FILE_MANAGER_INSTANCE_WITH_MTL_SWITCH.get_tmp_file(tenant_id, compressed_fd_, compressed_tmp_file_handle_))) {
    LOG_WARN("fail to get buffer tmp file handle", KR(ret), K(tenant_id), K(dir_id), KP(label), K(fd_), K(compressed_fd_));
  } else if (OB_FAIL(ObCompressorPool::get_instance().get_compressor(comptype, compressor_))) {
    LOG_WARN("fail to get compressor", KR(ret), K(fd_), K(compressed_fd_), K(comptype));
  } else {
    fd = fd_;
    buf_allocator_ = buf_allocator;
    if (0 < comp_unit_size) {
      COMPRESS_UNIT_SIZE = comp_unit_size;
    } else {
      COMPRESS_UNIT_SIZE = 64 * 1024; //64K
    }
    PREFETCH_SIZE = COMPRESS_UNIT_SIZE + upper_align(sizeof(CompressedUnitHeader), ObTmpFileGlobal::PAGE_SIZE);
    READ_BUF_SIZE = PREFETCH_SIZE * 2;
    FILE_MANAGER_INSTANCE_WITH_MTL_SWITCH.set_compressible_info(tenant_id, fd_, OB_TMP_FILE_TYPE::COMPRESS_BUFFER, fd_, this);
    FILE_MANAGER_INSTANCE_WITH_MTL_SWITCH.set_compressible_info(tenant_id, compressed_fd_, OB_TMP_FILE_TYPE::COMPRESS_STORE, fd_, this);
    file_bunch_ = file_bunch;
  }
  return ret;
}

int ObCompressTmpFile::read(
    const uint64_t tenant_id,
    const ObTmpFileIOInfo &io_info,
    ObCompTmpFileIOHandle &comp_io_handle)
{
  int ret = OB_SUCCESS;
  ObSpinLockGuard lock_guard(lock_);
  //read from compress tmp file
  bool is_align_read = 0 == read_offset_ % COMPRESS_UNIT_SIZE
                       && (0 == io_info.size_ % COMPRESS_UNIT_SIZE
                           || read_offset_ + io_info.size_ == file_size_);
  if (OB_UNLIKELY(!is_sealed_
                  || !wait_list_.is_empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("unexpected is_sealed_ or wait_list_", KR(ret), KPC(this));
  } else if (OB_UNLIKELY(ATOMIC_LOAD(&is_deleting_))) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("file is deleting", KR(ret), KPC(this));
  } else if (OB_UNLIKELY(ATOMIC_LOAD(&err_marked_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("file is err marked", KR(ret), K(tenant_id), KPC(this));
  } else if (OB_UNLIKELY(comp_io_handle.get_basic_io_handle().is_valid() && !comp_io_handle.get_basic_io_handle().is_finished())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tmp file io handle has remain data need to be waited", KR(ret), K(io_info), K(comp_io_handle.get_basic_io_handle()));
  } else if (OB_UNLIKELY(read_offset_ >= file_size_)) {
    ret = OB_ITER_END;
    LOG_WARN("iter end", KR(ret), K(fd_), K(compressed_fd_), K(read_offset_), K(file_size_));
  } else if (nullptr == read_buf_ && OB_ISNULL(read_buf_ = static_cast<char*>(buf_allocator_->alloc(READ_BUF_SIZE)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate read_buf_ memory failed", KR(ret), K(READ_BUF_SIZE), K(fd_), K(compressed_fd_));
  } else if (nullptr == decompress_buf_ && OB_ISNULL(decompress_buf_ = static_cast<char*>(buf_allocator_->alloc(COMPRESS_UNIT_SIZE)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate decompress_buf_ memory failed", KR(ret), K(COMPRESS_UNIT_SIZE), K(fd_), K(compressed_fd_));
  } else if (OB_FAIL(compress_read_(tenant_id, io_info, comp_io_handle.get_basic_io_handle()))) {
    LOG_WARN("fail to compress read", KR(ret), K(tenant_id), K(io_info), KP(read_buf_), KP(decompress_buf_), KPC(this));
  } else {
    comp_io_handle.set_user_buf(io_info.buf_);
    comp_io_handle.set_user_buf_size(io_info.size_);
  }
  if (OB_FAIL(ret)) {
    print_wait_list_(ret);
  }

  if (OB_SUCC(ret) && OB_UNLIKELY(read_offset_ >= file_size_)) {
    if (nullptr != decompress_buf_) {
      buf_allocator_->free(decompress_buf_);
      decompress_buf_ = nullptr;
    }
    decompress_buf_start_offset_ = -1;
    decompress_buf_valid_len_ = 0;
    if (nullptr != read_buf_) {
      buf_allocator_->free(read_buf_);
      read_buf_ = nullptr;
    }
    read_buf_start_offset_= -1;
    read_buf_valid_len_ = 0;
  }

  return ret;
}

int ObCompressTmpFile::next_comp_read_buf_(const ObTmpFileIOInfo &user_io_info,
                                           const uint64_t tenant_id,
                                           ObTmpFileIOHandle &io_handle,
                                           char*& comp_read_buf,
                                           int64_t& comp_read_size)
{
  int ret = OB_SUCCESS;

  comp_read_buf = nullptr;
  comp_read_size = 0;
  const int64_t UNIT_HEADER_SIZE = sizeof(CompressedUnitHeader);
  int64_t compressed_file_size = 0;
  if (OB_FAIL(FILE_MANAGER_INSTANCE_WITH_MTL_SWITCH.get_tmp_file_size(
          tenant_id, compressed_fd_, compressed_file_size, &compressed_tmp_file_handle_))) {
    LOG_WARN("fail to get compressed tmp file size",
        KR(ret), K(tenant_id), K(fd_), K(compressed_fd_));
  } else if (OB_UNLIKELY(compressed_read_offset_ >= compressed_file_size)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("unexpected compressed_read_offset_",
        KR(ret), K(compressed_file_size), K(compressed_read_offset_), K(fd_), K(compressed_fd_));
  } else if (!next_unit_header_.is_valid(COMPRESS_UNIT_SIZE)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("unexpected next_unit_header_",
        KR(ret), K(next_unit_header_), K(COMPRESS_UNIT_SIZE), K(compressed_file_size),
        K(fd_), K(compressed_fd_));
  } else {
    comp_read_size = MIN(next_unit_header_.data_len_ + UNIT_HEADER_SIZE,
                         compressed_file_size - compressed_read_offset_);
    if (OB_UNLIKELY(0 >= comp_read_size)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("unexpected comp_read_size",
          KR(ret), K(comp_read_size), K(next_unit_header_),
          K(COMPRESS_UNIT_SIZE), K(compressed_file_size), K(compressed_read_offset_),
          K(fd_), K(compressed_fd_));
    } else {
      int64_t io_buf_pos = 0;
      int64_t remain_size = comp_read_size;

      ObTmpFileIOInfo comp_read_io_info = user_io_info;
      comp_read_io_info.fd_ = compressed_fd_;

      if (read_buf_start_offset_ != -1 && read_buf_ != nullptr &&
          compressed_read_offset_ >= read_buf_start_offset_ &&
          compressed_read_offset_ < read_buf_start_offset_ + read_buf_valid_len_) {
        int64_t read_buf_start_pos = compressed_read_offset_ - read_buf_start_offset_;
        int64_t tail_buf_len = read_buf_valid_len_ - read_buf_start_pos;
        if (tail_buf_len >= comp_read_size) {
          comp_read_buf = read_buf_ + read_buf_start_pos;
          remain_size = 0;
        } else {
          MEMMOVE(read_buf_, read_buf_ + read_buf_start_pos, tail_buf_len);
          read_buf_start_offset_ += read_buf_start_pos;
          read_buf_valid_len_ = tail_buf_len;
          remain_size -= tail_buf_len;
          comp_read_buf = read_buf_;
          io_buf_pos = tail_buf_len;
        }
      } else {
        comp_read_buf = read_buf_;
        read_buf_start_offset_ = -1;
        read_buf_valid_len_ = 0;
      }

      if (remain_size > 0) {
        io_handle.reset();

        int64_t file_read_offset = 0;
        comp_read_io_info.buf_ = read_buf_ + io_buf_pos;
        if (read_buf_start_offset_ != -1) {
          comp_read_io_info.size_ = MIN(PREFETCH_SIZE,
                                        (compressed_file_size - (read_buf_start_offset_ + read_buf_valid_len_)));
          file_read_offset = read_buf_start_offset_ + read_buf_valid_len_;
        } else {
          comp_read_io_info.size_ = MIN(PREFETCH_SIZE, compressed_file_size - compressed_read_offset_);
          file_read_offset = compressed_read_offset_;
        }

        if (io_buf_pos + comp_read_io_info.size_ > READ_BUF_SIZE) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("comp read buf is out of range",
              KR(ret), K(io_buf_pos), K(comp_read_io_info),
              K(read_buf_start_offset_), K(read_buf_valid_len_), K(READ_BUF_SIZE));
        } else if (OB_FAIL(FILE_MANAGER_INSTANCE_WITH_MTL_SWITCH.pread(
                tenant_id, comp_read_io_info, file_read_offset, io_handle, &compressed_tmp_file_handle_))) {
          LOG_WARN("fail to read",
              KR(ret), K(tenant_id), K(comp_read_io_info),
              K(read_buf_start_offset_), K(read_buf_valid_len_), K(fd_), K(compressed_fd_));
        } else {
          if (read_buf_start_offset_ == -1) {
            read_buf_start_offset_ = file_read_offset;
          }
          read_buf_valid_len_ += comp_read_io_info.size_;
          if (read_buf_valid_len_ > READ_BUF_SIZE) {
            ret = OB_ERR_UNEXPECTED;
            LOG_ERROR("read_buf_valid_len_ is out of range",
                KR(ret), K(io_buf_pos), K(comp_read_io_info),
                K(read_buf_start_offset_), K(read_buf_valid_len_), K(READ_BUF_SIZE));
          }
        }
      }
    }
  }
  return ret;
}

int ObCompressTmpFile::compress_read_(
    const uint64_t tenant_id,
    const ObTmpFileIOInfo &user_io_info,
    ObTmpFileIOHandle &io_handle)
{
  int ret = OB_SUCCESS;
  int64_t remain_size = user_io_info.size_;
  int64_t io_buf_pos = 0;
  int64_t copy_size = 0;

  if (decompress_buf_start_offset_ != -1 && decompress_buf_ != nullptr &&
     read_offset_ >= decompress_buf_start_offset_ &&
     read_offset_ < decompress_buf_start_offset_ + decompress_buf_valid_len_) {
    copy_size = MIN((decompress_buf_valid_len_ - (read_offset_ - decompress_buf_start_offset_)),
                    remain_size);
    MEMCPY(user_io_info.buf_, decompress_buf_ + (read_offset_ - decompress_buf_start_offset_), copy_size);
    read_offset_ += copy_size;
    io_buf_pos += copy_size;
    remain_size -= copy_size;
    if (read_offset_ >= decompress_buf_start_offset_ + decompress_buf_valid_len_) {
      decompress_buf_start_offset_ = -1;
      decompress_buf_valid_len_ = 0;
    }
  }

  while (OB_SUCC(ret) && 0 < remain_size) {
    char *comp_read_buf = nullptr;
    int64_t comp_read_size = 0;
    if (OB_FAIL(next_comp_read_buf_(user_io_info, tenant_id, io_handle, comp_read_buf, comp_read_size))) {
      LOG_WARN("fail to next_comp_read_buf_",
          KR(ret), K(user_io_info), K(tenant_id), KP(comp_read_buf), K(comp_read_size));
    } else if (OB_FAIL(decompress_data_and_update_var_(remain_size, comp_read_buf, comp_read_size,
                user_io_info.buf_ + io_buf_pos, copy_size))) {
      LOG_WARN("fail to decompress data and update var", KR(ret), K(remain_size),
                                      KP(comp_read_buf), K(comp_read_size),
                                      K(user_io_info), K(io_buf_pos), K(fd_), K(compressed_fd_));
    } else {
      io_buf_pos += copy_size;
      remain_size -= copy_size;
    }
  }

  return ret;
}

int ObCompressTmpFile::decompress_data_and_update_var_(
    const int64_t remain_size,
    char *comp_read_buf,
    const int64_t comp_read_size,
    char *user_buf,
    int64_t &copy_size)
{
  int ret = OB_SUCCESS;
  // LOG_WARN("feifei-read", KR(ret), K(remain_size), K(comp_read_size), K(next_unit_header_));
  const int64_t UNIT_HEADER_SIZE = sizeof(CompressedUnitHeader);
  int64_t unit_read_offset = read_offset_ % COMPRESS_UNIT_SIZE;
  if (OB_UNLIKELY(0 >= remain_size
                  || NULL == comp_read_buf
                  || 0 >= comp_read_size
                  || NULL == user_buf
                  || NULL == decompress_buf_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument",
        KR(ret), K(remain_size), KP(comp_read_buf), K(comp_read_size), KP(user_buf), KP(decompress_buf_),
        K(fd_), K(compressed_fd_));
  } else {
    int64_t decomp_size = 0;
    if (next_unit_header_.is_compressed_) {
      if (OB_UNLIKELY(!next_unit_header_.is_valid(COMPRESS_UNIT_SIZE))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("unexpected next_unit_header_",
            KR(ret), K(remain_size), K(next_unit_header_), K(COMPRESS_UNIT_SIZE),
            K(fd_), K(compressed_fd_));
      } else if (OB_FAIL(compressor_->decompress(comp_read_buf,
                                                next_unit_header_.data_len_,
                                                decompress_buf_,
                                                next_unit_header_.original_data_len_,
                                                decomp_size))) {
        LOG_WARN("fail to decompress", KR(ret), KP(comp_read_buf), K(next_unit_header_),
            KP(decompress_buf_), K(fd_), K(compressed_fd_));
      } else if (OB_UNLIKELY(decomp_size != next_unit_header_.original_data_len_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("unexpected decomp_size", KR(ret), K(decomp_size), K(next_unit_header_),
            K(fd_), K(compressed_fd_));
      } else if (next_unit_header_.original_data_offset_ != lower_align(read_offset_, COMPRESS_UNIT_SIZE)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("unexpected next_unit_header_", KR(ret), K(next_unit_header_), K(read_offset_),
            K(fd_), K(compressed_fd_));
      } else {
        decompress_buf_start_offset_ = next_unit_header_.original_data_offset_;
        decompress_buf_valid_len_ = decomp_size;
      }
    } else {
      if (OB_UNLIKELY(next_unit_header_.data_len_ != next_unit_header_.original_data_len_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("unexpected data_len_", KR(ret), K(next_unit_header_), K(fd_), K(compressed_fd_));
      } else if (next_unit_header_.original_data_offset_ != lower_align(read_offset_, COMPRESS_UNIT_SIZE)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("unexpected next_unit_header_", KR(ret), K(next_unit_header_), K(read_offset_),
            K(fd_), K(compressed_fd_));
      } else {
        decomp_size = next_unit_header_.original_data_len_;
        MEMCPY(decompress_buf_, comp_read_buf, decomp_size);
        decompress_buf_start_offset_ = next_unit_header_.original_data_offset_;
        decompress_buf_valid_len_ = decomp_size;
      }
    }
    if (OB_SUCC(ret)) {
      copy_size = MIN(remain_size, decomp_size - unit_read_offset);
      if (OB_UNLIKELY(NULL == decompress_buf_ || 0 >= copy_size)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("unexpected decompress_buf_ or copy_size", KR(ret), KP(decompress_buf_),
                          K(remain_size), K(decomp_size), K(unit_read_offset), K(fd_), K(compressed_fd_));
      } else {
        MEMCPY(user_buf, decompress_buf_ + unit_read_offset, copy_size);
      }
      if (OB_SUCC(ret)) {
        read_offset_ += copy_size;
      }
    }
    if (OB_SUCC(ret)) {
      int64_t compressed_data_len = next_unit_header_.data_len_;
      if (comp_read_size > compressed_data_len) {
        if (OB_UNLIKELY(UNIT_HEADER_SIZE != comp_read_size - compressed_data_len)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("unexpected in_buf_size or next_unit_header_", KR(ret), K(next_unit_header_), K(comp_read_size),
                                                                                K(fd_), K(compressed_fd_));
        } else {
          next_unit_header_ = *((CompressedUnitHeader *)(comp_read_buf + compressed_data_len));
          compressed_read_offset_ += compressed_data_len + UNIT_HEADER_SIZE;
          if (!next_unit_header_.is_valid(COMPRESS_UNIT_SIZE)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_ERROR("unexpected next_unit_header_",
                KR(ret), K(next_unit_header_), K(fd_), K(compressed_fd_));
          }
        }
      } else if (OB_UNLIKELY(comp_read_size < compressed_data_len
                             || remain_size > next_unit_header_.original_data_len_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("unexpected in_buf_size", KR(ret), K(comp_read_size), K(next_unit_header_), K(remain_size),
                                                              K(fd_), K(compressed_fd_));
      } else if (remain_size == next_unit_header_.original_data_len_) {
        //read to end
        next_unit_header_.reset();
        next_unit_header_inited_ = false;
        compressed_read_offset_ += compressed_data_len;
      }
    }
  }
  return ret;
}

int ObCompressTmpFile::write(
    const uint64_t tenant_id,
    const ObTmpFileIOInfo &io_info,
    ObCompTmpFileIOHandle &comp_io_handle)
{
  int ret = OB_SUCCESS;
  int64_t cur_file_size = 0;
  int64_t end_offset = 0;
  if (OB_UNLIKELY(ATOMIC_LOAD(&is_deleting_))) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("file is deleting", KR(ret), KPC(this));
  } else if (OB_UNLIKELY(ATOMIC_LOAD(&err_marked_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("file is err marked", KR(ret), K(tenant_id), KPC(this));
  } else {
    ObSpinLockGuard lock_guard(lock_);
    const int64_t prev_file_size = file_size_;
    if (is_sealed_) {
      ret = OB_ERR_TMP_FILE_ALREADY_SEALED;
      LOG_WARN("tmp file already sealed", KR(ret), K(io_info), K(is_sealed_));
    } else if (OB_UNLIKELY(fd_ != io_info.fd_)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_ERROR("invalid argument", KR(ret), K(io_info), KPC(this));
    //first write to buffer tmp file
    } else if (OB_FAIL(FILE_MANAGER_INSTANCE_WITH_MTL_SWITCH.write(
            tenant_id, io_info, &buffer_tmp_file_handle_))) {
      LOG_WARN("fail to aio write", KR(ret), K(tenant_id), K(io_info), KPC(this));
    //second write to compressed tmp file
    } else if (OB_FAIL(FILE_MANAGER_INSTANCE_WITH_MTL_SWITCH.get_tmp_file_size(
            tenant_id, fd_, cur_file_size, &buffer_tmp_file_handle_))) {
      LOG_WARN("fail to get buffer tmp file size", KR(ret), K(tenant_id), KPC(this));
    } else if (OB_UNLIKELY(prev_file_size + io_info.size_ != cur_file_size)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("unexpected file size", KR(ret), K(prev_file_size), K(io_info), K(cur_file_size), KPC(this));
    } else {
      file_size_ = cur_file_size;
      end_offset = lower_align(file_size_, COMPRESS_UNIT_SIZE);
    }
  }
  if (OB_SUCC(ret)) {
    const int16_t MAX_RETRY_TIME = 3;
    int16_t retry_time = 0;
    CompressTask compress_task;
    compress_task.task_step_ = FIRST_STEP;
    while (OB_SUCC(ret)) {
      if (OB_FAIL(move_data_to_compressed_file_(tenant_id, io_info, end_offset, compress_task, comp_io_handle))) {
        LOG_WARN("fail to move data to compressed file", KR(ret), K(tenant_id), K(io_info), K(end_offset), K(compress_task), KPC(this));
        if (FIRST_STEP == compress_task.task_step_ && OB_ALLOCATE_MEMORY_FAILED == ret) {
          //do nothing
          ret = OB_SUCCESS;
          break;
        } else if (THIRD_STEP == compress_task.task_step_
                   && (OB_ALLOCATE_MEMORY_FAILED == ret || OB_TIMEOUT == ret)) {
          if (retry_time < MAX_RETRY_TIME) {
            ret = OB_SUCCESS;
            if (0 == retry_time) {
              ob_usleep(10 * 1000); // 10ms
            } else if (0 < retry_time) {
              ob_usleep(100 * 1000); // 100ms
            }
            retry_time++;
          }
        }
      } else {
        if (OB_UNLIKELY(FINISHED != compress_task.task_step_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("unexpected compress_task", KR(ret), KPC(this));
        }
        break;
      }
    }
    if (NULL != compress_task.compress_buf_) {
      buf_allocator_->free(compress_task.compress_buf_);
    }
    if (NULL != compress_task.read_buf_) {
      buf_allocator_->free(compress_task.read_buf_);
    }
    if (OB_FAIL(ret)) {
      mark_err_();
    }
  }
  if (OB_FAIL(ret)) {
    print_wait_list_(ret);
  }
  return ret;
}

//after concurrent write, this function is a single-threaded operation.
int ObCompressTmpFile::seal(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  int64_t cur_file_size = 0;
  ObCompTmpFileIOHandle comp_io_handle;
  //write to compress tmp file
  ObSpinLockGuard lock_guard(lock_);
  //because locked and bunchfile is sealed,
  //  we can be sure that the file size will not be changed.
  if (is_sealed_) {
    ret = OB_ERR_TMP_FILE_ALREADY_SEALED;
    LOG_INFO("compressible tmp file already sealed", KR(ret), K(is_sealed_));
  } else if (OB_UNLIKELY(ATOMIC_LOAD(&is_deleting_))) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("file is deleting", KR(ret), KPC(this));
  } else if (OB_UNLIKELY(ATOMIC_LOAD(&err_marked_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("file is err marked", KR(ret), K(tenant_id), KPC(this));
  } else if (OB_FAIL(FILE_MANAGER_INSTANCE_WITH_MTL_SWITCH.get_tmp_file_size(
          tenant_id, fd_, cur_file_size, &buffer_tmp_file_handle_))) {
    LOG_WARN("fail to get buffer tmp file size", KR(ret), K(tenant_id), KPC(this));
  } else if (OB_UNLIKELY(file_size_ != cur_file_size)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("unexpected file size", KR(ret), K(cur_file_size), KPC(this));
  } else {
    const int16_t MAX_RETRY_TIME = 3;
    int16_t retry_time = 0;
    CompressTask compress_task;
    compress_task.task_step_ = FIRST_STEP;
    while (OB_SUCC(ret)) {
      if (OB_FAIL(move_data_to_compressed_file_(tenant_id, compress_task, comp_io_handle))) {
        LOG_WARN("fail to move data to compressed file", KR(ret), K(tenant_id), K(compress_task), KPC(this));
        if (OB_ALLOCATE_MEMORY_FAILED == ret || OB_TIMEOUT == ret) {
          if (retry_time < MAX_RETRY_TIME) {
            ret = OB_SUCCESS;
            if (0 == retry_time) {
              ob_usleep(10 * 1000); // 10ms
            } else if (0 < retry_time) {
              ob_usleep(100 * 1000); // 10ms
            }
            retry_time++;
          }
        }
      } else {
        if (OB_UNLIKELY(FINISHED != compress_task.task_step_
                        || cur_file_size != moved_data_size_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("unexpected compress_task or cur_file_size", KR(ret), K(cur_file_size), KPC(this));
        } else if (OB_FAIL(FILE_MANAGER_INSTANCE_WITH_MTL_SWITCH.seal(tenant_id, fd_, &buffer_tmp_file_handle_))) {
          LOG_WARN("fail to seal", KR(ret), K(tenant_id), K(fd_), KPC(this));
        } else if (OB_FAIL(FILE_MANAGER_INSTANCE_WITH_MTL_SWITCH.seal(tenant_id, compressed_fd_, &compressed_tmp_file_handle_))) {
          LOG_WARN("fail to seal", KR(ret), K(tenant_id), K(compressed_fd_), KPC(this));
        } else {
          is_sealed_ = true;
        }
        break;
      }
    }
    if (NULL != compress_task.compress_buf_) {
      buf_allocator_->free(compress_task.compress_buf_);
    }
    if (NULL != compress_task.read_buf_) {
      buf_allocator_->free(compress_task.read_buf_);
    }
    if (OB_FAIL(ret)) {
      mark_err_();
    }
  }
  if (OB_FAIL(ret)) {
    print_wait_list_(ret);
  }
  return ret;
}

int ObCompressTmpFile::remove_files(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  {
    ObSpinLockGuard lock_guard(lock_);
    buffer_tmp_file_handle_.reset();
    compressed_tmp_file_handle_.reset();
    if (OB_FAIL(FILE_MANAGER_INSTANCE_WITH_MTL_SWITCH.remove(tenant_id, fd_))) {
      LOG_WARN("fail to remove buffer tmp file", KR(ret), K(fd_), KPC(this));
    } else if (OB_FAIL(FILE_MANAGER_INSTANCE_WITH_MTL_SWITCH.remove(tenant_id, compressed_fd_))) {
      LOG_WARN("fail to remove compressed tmp file", KR(ret), K(compressed_fd_), KPC(this));
    }
  }
  return ret;
}

int ObCompressTmpFile::set_deleting()
{
  int ret = OB_SUCCESS;
  ATOMIC_STORE(&is_deleting_, true);
  if (OB_FAIL(signal_wait_tasks_())) {
    LOG_ERROR("fail to signal wait tasks", KR(ret), KPC(this));
  }
  return ret;
}

int ObCompressTmpFile::mark_err_()
{
  int ret = OB_SUCCESS;
  ATOMIC_STORE(&err_marked_, true);
  if (OB_FAIL(signal_wait_tasks_())) {
    LOG_ERROR("fail to signal wait tasks", KR(ret), KPC(this));
  }
  return ret;
}

int ObCompressTmpFile::remove_files_if_needed(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  buffer_tmp_file_handle_.reset();
  compressed_tmp_file_handle_.reset();
  if (ObTmpFileGlobal::INVALID_TMP_FILE_FD != fd_ &&
      OB_FAIL(FILE_MANAGER_INSTANCE_WITH_MTL_SWITCH.remove(tenant_id, fd_))) {
    LOG_WARN("fail to remove buffer tmp file", KR(ret), K(fd_), KPC(this));
  } else if (ObTmpFileGlobal::INVALID_TMP_FILE_FD != compressed_fd_ &&
             OB_FAIL(FILE_MANAGER_INSTANCE_WITH_MTL_SWITCH.remove(tenant_id, compressed_fd_))) {
    LOG_WARN("fail to remove compressed tmp file", KR(ret), K(compressed_fd_), KPC(this));
  }
  return ret;
}

int ObCompressTmpFile::signal_wait_tasks_()
{
  int ret = OB_SUCCESS;
  ObSpinLockGuard lock_guard(move_data_lock_);
  WaitTask *it = wait_list_.get_first();
  while (OB_SUCC(ret) && it != wait_list_.get_header()) {
    WaitTask *next = it->get_next();
    if (OB_ISNULL(wait_list_.remove(it))) {
      //should not happen
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("Fail to remove it from wait list", KR(ret), KP(it), K(wait_list_));
    } else {
      it->write_cond_.signal();
      it = next;
    }
  }
  return ret;
}

void ObCompressTmpFile::print_wait_list_(int return_ret)
{
  int ret = OB_SUCCESS;
  ObSpinLockGuard lock_guard(move_data_lock_);
  if (OB_SUCCESS == return_ret) {
    LOG_INFO("wait list of compress tmp file", KR(return_ret), K(wait_list_));
  } else {
    LOG_WARN("wait list of compress tmp file", KR(return_ret), K(wait_list_));
  }
}

int ObCompressTmpFile::move_data_to_compressed_file_(
    const uint64_t tenant_id,
    const ObTmpFileIOInfo &io_info,
    const int64_t end_offset,
    CompressTask &compress_task,
    ObCompTmpFileIOHandle &io_handle)
{
  int ret = OB_SUCCESS;
  io_handle.reset();
  if (OB_UNLIKELY(INVALID_STEP == compress_task.task_step_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("unexpected task step", KR(ret), K(compress_task));
  } else if (FIRST_STEP == compress_task.task_step_) {
    bool need_move_data = false;
    //just a simple test to see if the condition is met
    {
      ObSpinLockGuard lock_guard(move_data_lock_);
      if (end_offset_for_compress_ + COMPRESS_UNIT_SIZE <= end_offset) {
        need_move_data = true;
      } else {
        compress_task.task_step_ = FINISHED;
      }
    }
    if (need_move_data) {
      int64_t max_overflow_size = 0;
      int64_t comp_buf_size = 0;
      if (OB_UNLIKELY(nullptr == io_info.buf_ || 0 >= io_info.size_)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_ERROR("invalid argument", KR(ret), K(io_info));
      } else if (OB_FAIL(compressor_->get_max_overflow_size(COMPRESS_UNIT_SIZE, max_overflow_size))) {
        LOG_WARN("fail to get max_overflow_size", KR(ret), K(COMPRESS_UNIT_SIZE), K(fd_), K(compressed_fd_));
      } else if (FALSE_IT(comp_buf_size = sizeof(CompressedUnitHeader) + COMPRESS_UNIT_SIZE + max_overflow_size)) {
      } else if (OB_ISNULL(compress_task.compress_buf_ = static_cast<char*>(buf_allocator_->alloc(comp_buf_size)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate compress_buf memory failed", KR(ret), K(comp_buf_size), K(fd_), K(compressed_fd_));
      } else if (OB_ISNULL(compress_task.read_buf_ = static_cast<char*>(buf_allocator_->alloc(COMPRESS_UNIT_SIZE)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate orignal_data_buf memory failed", KR(ret), K(COMPRESS_UNIT_SIZE), K(fd_), K(compressed_fd_));
      } else {
        compress_task.compress_buf_size_ = comp_buf_size;
        compress_task.read_size_ = COMPRESS_UNIT_SIZE;
        compress_task.task_step_ = SECOND_STEP;
      }
    }
  }
  if (OB_SUCC(ret) && FINISHED != compress_task.task_step_) {
    while (OB_SUCC(ret) && FINISHED != compress_task.task_step_) {
      if (SECOND_STEP == compress_task.task_step_) {
        compress_task.wait_task_.start_offset_ = -1;
        ObSpinLockGuard lock_guard(move_data_lock_);
        if (end_offset_for_compress_ + COMPRESS_UNIT_SIZE <= end_offset) {
          compress_task.wait_task_.start_offset_ = end_offset_for_compress_;
          end_offset_for_compress_ += COMPRESS_UNIT_SIZE;
          compress_task.task_step_ = THIRD_STEP;
        } else {
          compress_task.task_step_ = FINISHED;
        }
      }
      if (THIRD_STEP == compress_task.task_step_) {
        if (OB_FAIL(read_and_then_write_(tenant_id, io_info, compress_task, io_handle))) {
          LOG_WARN("fail to read and then write", KR(ret), K(tenant_id), K(io_info), K(compress_task),
                                                                K(fd_), K(compressed_fd_));
        } else {
          compress_task.task_step_ = SECOND_STEP;
        }
      }
    }
  }
  return ret;
}

int ObCompressTmpFile::move_data_to_compressed_file_(
    const uint64_t tenant_id,
    CompressTask &compress_task,
    ObCompTmpFileIOHandle &io_handle)
{
  int ret = OB_SUCCESS;
  io_handle.reset();
  int64_t file_read_size = 0;
  if (OB_UNLIKELY(INVALID_STEP == compress_task.task_step_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("unexpected task step", KR(ret), K(compress_task));
  } else if (FIRST_STEP == compress_task.task_step_) {
    bool need_move_data = false;
    //just a simple test to see if the condition is met
    {
      ObSpinLockGuard lock_guard(move_data_lock_);
      if (end_offset_for_compress_ < file_size_) {
        file_read_size = MIN(file_size_ - end_offset_for_compress_, COMPRESS_UNIT_SIZE);
        need_move_data = true;
      } else {
        compress_task.task_step_ = FINISHED;
      }
    }
    if (need_move_data) {
      int64_t max_overflow_size = 0;
      int64_t comp_buf_size = 0;
      if (OB_UNLIKELY(0 >= file_read_size)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("unexpected read_file_size", KR(ret), K(file_read_size), K(fd_), K(compressed_fd_));
      } else if (OB_FAIL(compressor_->get_max_overflow_size(file_read_size, max_overflow_size))) {
        LOG_WARN("fail to get max_overflow_size", KR(ret), K(file_read_size), K(fd_), K(compressed_fd_));
      } else if (FALSE_IT(comp_buf_size = sizeof(CompressedUnitHeader) + file_read_size + max_overflow_size)) {
      } else if (OB_ISNULL(compress_task.compress_buf_ = static_cast<char*>(buf_allocator_->alloc(comp_buf_size)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate compress_buf memory failed", KR(ret), K(comp_buf_size), K(fd_), K(compressed_fd_));
      } else if (OB_ISNULL(compress_task.read_buf_ = static_cast<char*>(buf_allocator_->alloc(file_read_size)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate orignal_data_buf memory failed", KR(ret), K(file_read_size), K(fd_), K(compressed_fd_));
      } else {
        compress_task.compress_buf_size_ = comp_buf_size;
        compress_task.read_size_ = file_read_size;
        compress_task.task_step_ = SECOND_STEP;
      }
    }
  }
  if (OB_SUCC(ret) && FINISHED != compress_task.task_step_) {
    ObTmpFileIOInfo invalid_user_io_info;
    while (OB_SUCC(ret) && FINISHED != compress_task.task_step_) {
      invalid_user_io_info.reset();
      if (SECOND_STEP == compress_task.task_step_) {
        compress_task.wait_task_.start_offset_ = -1;
        ObSpinLockGuard lock_guard(move_data_lock_);
        if (end_offset_for_compress_ < file_size_) {
          compress_task.wait_task_.start_offset_ = end_offset_for_compress_;
          compress_task.read_size_ = MIN(file_size_ - end_offset_for_compress_, COMPRESS_UNIT_SIZE);
          end_offset_for_compress_ += file_read_size;
          compress_task.task_step_ = THIRD_STEP;
        } else {
          compress_task.task_step_ = FINISHED;
        }
      }
      if (THIRD_STEP == compress_task.task_step_) {
        if (OB_FAIL(read_and_then_write_(tenant_id, invalid_user_io_info, compress_task, io_handle))) {
          LOG_WARN("fail to read and then write", KR(ret), K(tenant_id), K(compress_task), K(fd_), K(compressed_fd_));
        } else {
          compress_task.task_step_ = SECOND_STEP;
        }
      }
    }
  }
  return ret;
}

int ObCompressTmpFile::read_and_then_write_(
    const uint64_t tenant_id,
    const ObTmpFileIOInfo &user_io_info,
    CompressTask &compress_task,
    ObCompTmpFileIOHandle &comp_io_handle)
{
  int ret = OB_SUCCESS;
  comp_io_handle.reset();
  if (OB_UNLIKELY(!compress_task.is_valid_to_compress(COMPRESS_UNIT_SIZE))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", KR(ret), K(compress_task), K(file_size_), K(fd_), K(compressed_fd_));
  } else {
    const int64_t UNIT_HEADER_SIZE = sizeof(CompressedUnitHeader);
    const int64_t buffer_file_read_offset = compress_task.wait_task_.start_offset_;
    ObTmpFileIOInfo read_io_info;
    ObTmpFileIOInfo write_io_info;
    CompressedUnitHeader unit_header;
    int64_t compressed_data_size = 0;
    bool need_wait = false;
    char *compress_buf = compress_task.compress_buf_;
    int64_t compress_buf_size = compress_task.compress_buf_size_;
    generate_read_io_info_(compress_task.read_buf_, compress_task.read_size_, user_io_info, read_io_info);
    if (OB_FAIL(FILE_MANAGER_INSTANCE_WITH_MTL_SWITCH.pread(tenant_id, read_io_info, buffer_file_read_offset, comp_io_handle.get_basic_io_handle()))) {
      LOG_WARN("fail to pread", KR(ret), K(tenant_id), K(read_io_info), K(buffer_file_read_offset), K(fd_), K(compressed_fd_));
    } else if (OB_FAIL(compressor_->compress(read_io_info.buf_, read_io_info.size_,
                                             compress_buf + UNIT_HEADER_SIZE, compress_buf_size - UNIT_HEADER_SIZE,
                                             compressed_data_size))) {
      LOG_WARN("compressor fail to compress", KR(ret), K(read_io_info), KP(compress_buf), K(compress_buf_size),
                                                    K(UNIT_HEADER_SIZE), K(fd_), K(compressed_fd_));
    } else if (compressed_data_size >= read_io_info.size_) {
      unit_header.is_compressed_ = false;
      unit_header.data_len_ = read_io_info.size_;
      unit_header.original_data_offset_ = buffer_file_read_offset;
      unit_header.original_data_len_ = read_io_info.size_;
      MEMCPY(compress_buf + UNIT_HEADER_SIZE, read_io_info.buf_, read_io_info.size_);
    } else {
      unit_header.is_compressed_ = true;
      unit_header.data_len_ = compressed_data_size;
      unit_header.original_data_offset_ = buffer_file_read_offset;
      unit_header.original_data_len_ = read_io_info.size_;
    }
    if (OB_SUCC(ret)) {
      ObSpinLockGuard lock_guard(move_data_lock_);
      if (buffer_file_read_offset > moved_data_size_) {
        if (!wait_list_.add_last(&compress_task.wait_task_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("fail to push back", KR(ret), K(compress_task), K(fd_), K(compressed_fd_));
        } else {
          need_wait = true;
        }
      } else if (OB_UNLIKELY(buffer_file_read_offset < moved_data_size_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected buffer_file_read_offset", KR(ret), K(buffer_file_read_offset), K(moved_data_size_),
                                                              K(fd_), K(compressed_fd_));
      }
    }
    if (OB_SUCC(ret) && need_wait) {
      if (OB_UNLIKELY(ATOMIC_LOAD(&is_deleting_))) {
        ret = OB_STATE_NOT_MATCH;
        LOG_WARN("compress tmp file is deleting", KR(ret), K(fd_), K(compressed_fd_));
      } else if (OB_UNLIKELY(ATOMIC_LOAD(&err_marked_))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("file is err marked", KR(ret), K(tenant_id), KPC(this));
      } else {
        int tmp_ret = OB_TIMEOUT;
        int64_t start_wait_ts = ObTimeUtility::current_time();
        while (OB_TIMEOUT == tmp_ret) {
          if (OB_TMP_FAIL(compress_task.wait_task_.write_cond_.wait())) {
            int64_t current_time = ObTimeUtility::current_time();
            LOG_WARN("fail to cond wait", KR(tmp_ret), K(start_wait_ts), K(current_time), K(fd_), K(compressed_fd_));
          } else {
            if (ATOMIC_LOAD(&is_deleting_)) {
              ret = OB_STATE_NOT_MATCH;
              LOG_WARN("compress tmp file is deleting", KR(ret), K(fd_), K(compressed_fd_), KPC(this));
            } else if (OB_UNLIKELY(ATOMIC_LOAD(&err_marked_))) {
              ret = OB_ERR_UNEXPECTED;
              LOG_ERROR("file is err marked", KR(ret), K(tenant_id), KPC(this));
            }
          }
        }
        ret = OB_SUCCESS == ret ? tmp_ret : ret;
      }
    }
    if (OB_SUCC(ret)) {
      MEMCPY(compress_buf, &unit_header, UNIT_HEADER_SIZE);
      generate_write_io_info_(compress_buf, UNIT_HEADER_SIZE + unit_header.data_len_, user_io_info, write_io_info);
      if (OB_FAIL(FILE_MANAGER_INSTANCE_WITH_MTL_SWITCH.write(tenant_id, write_io_info))) {
        LOG_WARN("fail to aio write", KR(ret), K(tenant_id), K(write_io_info), K(fd_), K(compressed_fd_));
      } else if (OB_FAIL(FILE_MANAGER_INSTANCE_WITH_MTL_SWITCH.truncate(
              tenant_id, fd_, buffer_file_read_offset + read_io_info.size_, &buffer_tmp_file_handle_))) {
        LOG_WARN("fail to truncate", KR(ret), K(tenant_id), K(buffer_file_read_offset), K(read_io_info), K(fd_), K(compressed_fd_));
      } else {
        ObSpinLockGuard lock_guard(move_data_lock_);
        moved_data_size_ += unit_header.original_data_len_;
        if (!next_unit_header_inited_) {
          next_unit_header_ = unit_header;
          compressed_read_offset_ += UNIT_HEADER_SIZE;
          next_unit_header_inited_ = true;
        }
        WaitTask *it = wait_list_.get_first();
        while (OB_SUCC(ret) && it != wait_list_.get_header()) {
          if (it->start_offset_ == moved_data_size_) {
            if (OB_ISNULL(wait_list_.remove(it))) {
              //should not happen
              ret = OB_ERR_UNEXPECTED;
              LOG_ERROR("Fail to remove it from wait list, ", KR(ret), KP(it), K(wait_list_));
            } else {
              it->write_cond_.signal();
            }
            break;
          } else {
            it = it->get_next();
          }
        }
      }
    }
  }
  return ret;
}

void ObCompressTmpFile::generate_read_io_info_(
  char *read_buf,
  const int64_t read_size,
  const ObTmpFileIOInfo &user_io_info,
  ObTmpFileIOInfo &read_io_info)
{
  read_io_info.fd_ = fd_;
  read_io_info.disable_page_cache_ = true;
  read_io_info.buf_ = read_buf;
  read_io_info.size_ = read_size;
  if (!user_io_info.is_valid()) {
    read_io_info.io_desc_.set_wait_event(ObWaitEventIds::DB_FILE_DATA_READ);
    read_io_info.io_timeout_ms_ = GCONF._data_storage_io_timeout / 1000L;
  } else {
    read_io_info.io_desc_ = user_io_info.io_desc_;
    read_io_info.io_timeout_ms_ = user_io_info.io_timeout_ms_;
  }
}

void ObCompressTmpFile::generate_write_io_info_(
  char *buf,
  const int64_t write_size,
  const ObTmpFileIOInfo &user_io_info,
  ObTmpFileIOInfo &write_io_info)
{
  write_io_info = user_io_info;
  write_io_info.fd_ = compressed_fd_;
  write_io_info.buf_ = buf;
  write_io_info.size_ = write_size;
  if (!user_io_info.is_valid()) {
    write_io_info.io_desc_.set_wait_event(ObWaitEventIds::DB_FILE_COMPACT_WRITE);
    write_io_info.io_timeout_ms_ = GCONF._data_storage_io_timeout / 1000L;
  }
}

bool ObCompressTmpFile::can_remove()
{
  return 1 == get_ref_cnt();
}

void ObCompressTmpFile::get_file_size(int64_t &file_size)
{
  ObSpinLockGuard lock_guard(lock_);
  file_size = file_size_;
}

ObCompressTmpFileHandle::ObCompressTmpFileHandle(ObCompressTmpFile *comp_tmp_file)
  : ptr_(comp_tmp_file)
{
  if (ptr_ != nullptr) {
    ptr_->inc_ref_cnt();
  }
}

ObCompressTmpFileHandle::ObCompressTmpFileHandle(const ObCompressTmpFileHandle &handle)
  : ptr_(nullptr)
{
  operator=(handle);
}

ObCompressTmpFileHandle & ObCompressTmpFileHandle::operator=(const ObCompressTmpFileHandle &other)
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

void ObCompressTmpFileHandle::reset()
{
  if (ptr_ != nullptr) {
    ptr_->dec_ref_cnt();
    if (0 == ptr_->get_ref_cnt()) {
      ptr_->~ObCompressTmpFile();
    }
    ptr_ = nullptr;
  }
}

ObCompressTmpFileBunch::ObCompressTmpFileBunch()
  : is_inited_(false),
    err_marked_(false),
    is_sealed_(false),
    is_deleting_(false),
    fd_(ObTmpFileGlobal::INVALID_TMP_FILE_FD),
    ref_cnt_(0),
    compressible_tmp_file_cnt_(0),
    write_req_seq_(0),
    next_read_fd_index_(0),
    next_read_file_offset_(0),
    sealed_tmp_file_cnt_(0),
    compressible_files_(nullptr),
    fds_(),
    lock_(common::ObLatchIds::TMP_FILE_LOCK) {}

ObCompressTmpFileBunch::~ObCompressTmpFileBunch()
{
  reset();
}

void ObCompressTmpFileBunch::reset()
{
  is_inited_ = false;
  err_marked_ = false;
  is_sealed_ = false;
  is_deleting_ = false;
  fd_ = ObTmpFileGlobal::INVALID_TMP_FILE_FD;
  ref_cnt_ = 0;
  compressible_tmp_file_cnt_ = 0;
  write_req_seq_ = 0;
  next_read_fd_index_ = 0;
  next_read_file_offset_ = 0;
  sealed_tmp_file_cnt_ = 0;
  compressible_files_ = nullptr;
  fds_.reset();
}

int ObCompressTmpFileBunch::init(
    const uint64_t tenant_id,
    int64_t &target_fd,
    const int64_t dir_id,
    const char* const label,
    const ObCompressorType comptype,
    const int64_t comp_unit_size,
    const int64_t compressible_tmp_file_cnt,
    ObConcurrentFIFOAllocator *comp_tmp_file_allocator,
    ObFIFOAllocator *compress_buf_allocator,
    CompTmpFileMap *compressible_files)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(ObTmpFileGlobal::INVALID_TMP_FILE_DIR_ID == dir_id
                  || 0 >= compressible_tmp_file_cnt
                  || NULL == comp_tmp_file_allocator
                  || NULL == compress_buf_allocator
                  || NULL == compressible_files)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", KR(ret), K(dir_id), K(compressible_tmp_file_cnt),
            KP(comp_tmp_file_allocator), KP(compress_buf_allocator), KP(compressible_files));
  } else {
    compressible_tmp_file_cnt_ = compressible_tmp_file_cnt;
    compressible_files_ = compressible_files;
    for (int64_t i = 0; i < compressible_tmp_file_cnt_ && OB_SUCC(ret); i++) {
      void *buf = nullptr;
      ObCompressTmpFile *comp_tmp_file = nullptr;
      int64_t fd = ObTmpFileGlobal::INVALID_TMP_FILE_FD;
      if (OB_ISNULL(buf = comp_tmp_file_allocator->alloc(sizeof(ObCompressTmpFile),
                                                          lib::ObMemAttr(tenant_id, "CompTmpFile")))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to allocate memory for compressible tmp file",
                KR(ret), K(tenant_id), K(sizeof(ObCompressTmpFile)));
      } else if (FALSE_IT(comp_tmp_file = new (buf) ObCompressTmpFile())) {
      } else if (OB_FAIL(comp_tmp_file->init(tenant_id, fd, dir_id, comptype, comp_unit_size, label, compress_buf_allocator, this))) {
        LOG_WARN("fail to init compress tmp file", KR(ret), K(tenant_id), K(dir_id), K(comptype), K(comp_unit_size), KP(label), KP(compress_buf_allocator));
      } else if (OB_FAIL(compressible_files_->insert(ObTmpFileKey(fd), comp_tmp_file))) {
        LOG_WARN("fail to set refactored to compress tmp file map", KR(ret), K(fd), KP(comp_tmp_file));
      } else if (OB_FAIL(fds_.push_back(fd))) {
        LOG_WARN("fail to push back to fds", KR(ret), K(fd));
      } else if (OB_FAIL(comp_file_handles_.push_back(comp_tmp_file))) {
        LOG_WARN("fail to push back to comp file handles", KR(ret), K(fd));
      } else if (0 == i) {
        target_fd = fd;
        fd_ = target_fd;
      }

      //rollback
      if (OB_FAIL(ret)) {
        int tmp_ret = OB_SUCCESS;
        for (int64_t j = 0; j <= i; j++) {
          ObCompressTmpFile *alloced_file = nullptr;
          if (j < i) {
            int64_t remove_fd = ObTmpFileGlobal::INVALID_TMP_FILE_FD;
            ObCompressTmpFileHandle comp_tmp_file_handle;
            if (OB_UNLIKELY(j >= fds_.count())) {
              tmp_ret = OB_ERR_UNEXPECTED;
              LOG_WARN("unexpected fds_", KR(ret), KR(tmp_ret), K(target_fd), K(j), K(fds_));
            } else if (FALSE_IT(remove_fd = fds_.at(j))) {
            } else if (OB_TMP_FAIL(compressible_files_->get(ObTmpFileKey(remove_fd), comp_tmp_file_handle))) {
              LOG_WARN("fail to get compressible tmp file", KR(ret), KR(tmp_ret), K(remove_fd));
            } else if (OB_ISNULL(comp_tmp_file_handle.get())) {
              tmp_ret = OB_ERR_UNEXPECTED;
              LOG_WARN("get invalid compressible tmp file pointer", KR(ret), KR(tmp_ret), K(remove_fd), KP(comp_tmp_file_handle.get()));
            } else {
              alloced_file = comp_tmp_file_handle.get();
            }
          } else { //j == i
            alloced_file = comp_tmp_file;
          }
          if (NULL != alloced_file) {
            if (OB_TMP_FAIL(alloced_file->remove_files_if_needed(tenant_id))) {
              LOG_WARN("fail to remove files if needed", KR(ret), KR(tmp_ret), K(fd), K(tenant_id), KPC(comp_tmp_file));
            } else {
              alloced_file->~ObCompressTmpFile();
              comp_tmp_file_allocator->free(alloced_file);
              alloced_file = nullptr;
            }
          }
        }
        if (OB_SUCCESS == tmp_ret) {
          fds_.reset();
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    ATOMIC_STORE(&is_inited_, true);
  }
  return ret;
}

int ObCompressTmpFileBunch::write(
    const uint64_t tenant_id,
    const ObTmpFileIOInfo &io_info)
{
  int ret = OB_SUCCESS;
  int64_t write_req = -1;
  int64_t unsealed_file_cnt = 0;
  if (true == ATOMIC_LOAD(&is_sealed_)) {
    ret = OB_ERR_TMP_FILE_ALREADY_SEALED;
    LOG_WARN("tmp file bunch already sealed", KR(ret), K(io_info), K(is_sealed_));
  } else if (OB_UNLIKELY(ATOMIC_LOAD(&is_deleting_))) {
    ret = OB_STATE_NOT_MATCH;
    LOG_ERROR("bunch is deleting", KR(ret), K(tenant_id), KPC(this));
  } else if (OB_UNLIKELY(ATOMIC_LOAD(&err_marked_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("bunch is err marked", KR(ret), K(tenant_id), KPC(this));
  } else {
    write_req = ATOMIC_FAA(&write_req_seq_, 1);
  }
  if (OB_SUCC(ret)) {
    ObCompTmpFileIOHandle io_handle;
    ObCompressTmpFileHandle* comp_tmp_file_handle = nullptr;
    int64_t sealed_tmp_file_cnt = ATOMIC_LOAD(&sealed_tmp_file_cnt_);
    int64_t new_sealed_tmp_file_cnt = 0;
    int64_t file_index = -1;
    while (OB_SUCC(ret)) {
      io_handle.reset();
      comp_tmp_file_handle = nullptr;
      int64_t fd = ObTmpFileGlobal::INVALID_TMP_FILE_FD;
      ObTmpFileIOInfo new_io_info = io_info;
      if (OB_UNLIKELY(compressible_tmp_file_cnt_ <= sealed_tmp_file_cnt
                      || fds_.count() != compressible_tmp_file_cnt_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("unexpected file_index or sealed_tmp_file_cnt", KR(ret), KPC(this));
      } else {
        file_index = sealed_tmp_file_cnt + write_req % (compressible_tmp_file_cnt_ - sealed_tmp_file_cnt);
        fd = fds_.at(file_index);
        comp_tmp_file_handle = &comp_file_handles_.at(file_index);
        new_io_info.fd_ = fd;
      }

      if (OB_FAIL(ret)) {
      } else if (OB_ISNULL(comp_tmp_file_handle->get())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("get invalid compressible tmp file pointer", KR(ret), K(fd), KP(comp_tmp_file_handle->get()), KPC(this));
      } else if (OB_FAIL(comp_tmp_file_handle->get()->write(tenant_id, new_io_info, io_handle))) {
        LOG_WARN("fail to write", KR(ret), K(tenant_id), K(new_io_info), K(fd), KPC(this));
        if (OB_ERR_TMP_FILE_ALREADY_SEALED == ret) {
          if (!ATOMIC_LOAD(&is_sealed_)) {
            ret = OB_SUCCESS;
            new_sealed_tmp_file_cnt = ATOMIC_LOAD(&sealed_tmp_file_cnt_);
            if (new_sealed_tmp_file_cnt > sealed_tmp_file_cnt) {
              sealed_tmp_file_cnt = new_sealed_tmp_file_cnt;
            } else {
              sealed_tmp_file_cnt++;
            }
          }
        }
      } else {
        break;
      }
    }
  }
  return ret;
}

int ObCompressTmpFileBunch::read(
    const uint64_t tenant_id,
    const ObTmpFileIOInfo &io_info,
    ObCompTmpFileIOHandle &io_handle)
{
  int ret = OB_SUCCESS;
  ObCompressTmpFileHandle* comp_tmp_file_handle = nullptr;
  int64_t fd = ObTmpFileGlobal::INVALID_TMP_FILE_FD;
  int64_t file_size = 0;
  ObTmpFileIOInfo new_io_info = io_info;
  int64_t remain_size = io_info.size_;
  if (OB_UNLIKELY(!ATOMIC_LOAD(&is_sealed_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("bunch is not sealed", KR(ret), K(tenant_id), K(io_info), KPC(this));
  } else if (OB_UNLIKELY(ATOMIC_LOAD(&is_deleting_))) {
    ret = OB_STATE_NOT_MATCH;
    LOG_ERROR("bunch is deleting", KR(ret), K(tenant_id), K(io_info), KPC(this));
  } else if (OB_UNLIKELY(ATOMIC_LOAD(&err_marked_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("bunch is err marked", KR(ret), K(tenant_id), K(io_info), KPC(this));
  } else {
    ObSpinLockGuard lock_guard(lock_);
    if (OB_UNLIKELY(next_read_fd_index_ < 0
                    || next_read_file_offset_ < 0
                    || fds_.empty())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected next_read_fd_index_ or next_read_file_offset_ or fds_ ", KR(ret), KPC(this));
    } else {
      while (OB_SUCC(ret)) {
        if (OB_UNLIKELY(next_read_fd_index_ >= fds_.count())) {
          ret = OB_ITER_END;
          LOG_WARN("tmp file bunch iter end", KR(ret), KPC(this));
        } else {
          comp_tmp_file_handle = &comp_file_handles_.at(next_read_fd_index_);
          new_io_info.fd_ = fd;
          file_size = 0;
          if (OB_ISNULL(comp_tmp_file_handle->get())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("get invalid compressible tmp file pointer", KR(ret), K(fd), KP(comp_tmp_file_handle->get()), KPC(this));
          } else if (FALSE_IT(comp_tmp_file_handle->get()->get_file_size(file_size))) {
          } else if (0 == file_size) {
            next_read_fd_index_++;
          } else {
            if (next_read_file_offset_ + remain_size >= file_size) {
              new_io_info.size_ = file_size - next_read_file_offset_;
            } else {
              new_io_info.size_ = remain_size;
            }
            if (OB_FAIL(comp_tmp_file_handle->get()->read(tenant_id, new_io_info, io_handle))) {
              LOG_WARN("fail to read", KR(ret), K(tenant_id), K(new_io_info), K(fd), KPC(this));
            } else {
              const int64_t last_read_size = new_io_info.size_;
              remain_size -= last_read_size;
              new_io_info.buf_ += last_read_size;
              if (next_read_file_offset_ + last_read_size >= file_size) {
                next_read_fd_index_++;
                next_read_file_offset_ = 0;
              } else {
                next_read_file_offset_ += last_read_size;
              }
              if (0 >= remain_size) {
                break;
              }
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObCompressTmpFileBunch::seal(
    const uint64_t tenant_id,
    int64_t &sealed_comp_file_num)
{
  int ret = OB_SUCCESS;
  sealed_comp_file_num = 0;
  if (false == ATOMIC_VCAS(&is_sealed_, false, true)) {
  } else {
    ret = OB_ERR_TMP_FILE_ALREADY_SEALED;
    LOG_WARN("tmp file bunch already sealed", KR(ret), K(is_sealed_));
  }
  if (OB_SUCC(ret)) {
    if (OB_UNLIKELY(ATOMIC_LOAD(&is_deleting_))) {
      ret = OB_STATE_NOT_MATCH;
      LOG_ERROR("bunch is deleting", KR(ret), K(tenant_id), KPC(this));
    } else if (OB_UNLIKELY(ATOMIC_LOAD(&err_marked_))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("bunch is err marked", KR(ret), K(tenant_id), KPC(this));
    } else if (OB_UNLIKELY(fds_.empty()
                           || fds_.count() != compressible_tmp_file_cnt_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fds is not expected", KR(ret), K(tenant_id), KPC(this));
    }
    if (OB_SUCC(ret)) {
      ObCompressTmpFileHandle comp_tmp_file_handle;
      int64_t fd = ObTmpFileGlobal::INVALID_TMP_FILE_FD;
      int64_t index = ATOMIC_LOAD(&sealed_tmp_file_cnt_);
      int64_t tmp_sealed_comp_file_num = 0;
      while (OB_SUCC(ret) && index < compressible_tmp_file_cnt_) {
        fd = fds_.at(index);
        comp_tmp_file_handle.reset();
        if (OB_FAIL(compressible_files_->get(ObTmpFileKey(fd), comp_tmp_file_handle))) {
          if (OB_ENTRY_NOT_EXIST == ret) {
            LOG_WARN("compressible tmp file does not exist", KR(ret), K(fd), KPC(this));
          } else {
            LOG_WARN("fail to get compressible tmp file", KR(ret), K(fd), KPC(this));
          }
        } else if (OB_ISNULL(comp_tmp_file_handle.get())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get invalid compressible tmp file pointer", KR(ret), K(fd), KP(comp_tmp_file_handle.get()), KPC(this));
        } else if (OB_FAIL(comp_tmp_file_handle.get()->seal(tenant_id))) {
          LOG_WARN("fail to seal", KR(ret), K(tenant_id), K(fd), KPC(this));
          if (OB_ERR_TMP_FILE_ALREADY_SEALED == ret) {
            index++;
            ret = OB_SUCCESS;
          }
        } else {
          index++;
          tmp_sealed_comp_file_num++;
        }
      }
      if (OB_SUCC(ret) && 0 < tmp_sealed_comp_file_num) {
        ObSpinLockGuard lock_guard(lock_);
        if (OB_UNLIKELY(ATOMIC_LOAD(&is_deleting_))) {
          ret = OB_STATE_NOT_MATCH;
          LOG_ERROR("bunch is deleting", KR(ret), K(tenant_id), KPC(this));
        } else {
          int64_t new_cnt = ATOMIC_AAF(&sealed_tmp_file_cnt_, tmp_sealed_comp_file_num);
          sealed_comp_file_num = tmp_sealed_comp_file_num;
          if (new_cnt > compressible_tmp_file_cnt_) {
            ret = OB_ERR_UNEXPECTED;
            LOG_ERROR("unexpected new_cnt", KR(ret), K(new_cnt), K(sealed_comp_file_num), KPC(this));
          }
        }
      }
      if (OB_SUCC(ret)) {
        const int64_t total_cnt = compressible_tmp_file_cnt_;
        int64_t start_time = ObTimeUtility::current_time();
        int64_t cur_time = -1;
        while (OB_SUCC(ret)) {
          cur_time = ObTimeUtility::current_time();
          if (total_cnt == ATOMIC_LOAD(&sealed_tmp_file_cnt_)) {
            break;
          } else if (cur_time - start_time > 1 * 1000 * 1000) {
            LOG_WARN("wait sealed_tmp_file_cnt_ equal to total_cnt cost too much time", KR(ret), K(start_time), K(cur_time), KPC(this));
            start_time = ObTimeUtility::current_time();
            sleep(1); // 1s
          } else {
            if (OB_UNLIKELY(ATOMIC_LOAD(&is_deleting_))) {
              ret = OB_STATE_NOT_MATCH;
              LOG_ERROR("bunch is deleting", KR(ret), K(tenant_id), KPC(this));
            } else if (OB_UNLIKELY(ATOMIC_LOAD(&err_marked_))) {
              ret = OB_ERR_UNEXPECTED;
              LOG_ERROR("bunch is err marked", KR(ret), K(tenant_id), KPC(this));
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObCompressTmpFileBunch::seal_partial_comp_tmp_files(
    const uint64_t tenant_id,
    const int64_t unsealed_file_num_each_bunch,
    int64_t &seal_file_num)
{
  int ret = OB_SUCCESS;
  seal_file_num = 0;
  if (false == ATOMIC_LOAD(&is_sealed_)) {
    if (OB_UNLIKELY(0 >= unsealed_file_num_each_bunch)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_ERROR("invalid argument", KR(ret), K(unsealed_file_num_each_bunch));
    } else if (!ATOMIC_LOAD(&is_inited_)) {
      //do nothing
      LOG_INFO("file bunch is not inited", K(tenant_id), K(unsealed_file_num_each_bunch));
    } else if (OB_UNLIKELY(ATOMIC_LOAD(&is_deleting_))) {
      // do nothing
      LOG_INFO("file bunch is deleting", K(tenant_id), K(unsealed_file_num_each_bunch));
    } else if (OB_UNLIKELY(ATOMIC_LOAD(&err_marked_))) {
      // do nothing
      LOG_WARN("file bunch is err marked", K(tenant_id), K(unsealed_file_num_each_bunch), KPC(this));
    } else {
      ObCompressTmpFileHandle comp_tmp_file_handle;
      int64_t fd = ObTmpFileGlobal::INVALID_TMP_FILE_FD;
      //NOTE: we only increase sealed_tmp_file_cnt_ after seal is successful to avoid errors in statistics.
      int64_t index = ATOMIC_LOAD(&sealed_tmp_file_cnt_);
      const int64_t need_seal_file_cnt = compressible_tmp_file_cnt_ - unsealed_file_num_each_bunch;
      int64_t tmp_seal_file_num = 0;
      while (OB_SUCC(ret)
            && index < need_seal_file_cnt) {
        fd = fds_.at(index);
        comp_tmp_file_handle.reset();
        if (OB_FAIL(compressible_files_->get(ObTmpFileKey(fd), comp_tmp_file_handle))) {
          if (OB_ENTRY_NOT_EXIST == ret) {
            LOG_WARN("compressible tmp file does not exist", KR(ret), K(fd), KPC(this));
          } else {
            LOG_WARN("fail to get compressible tmp file", KR(ret), K(fd), KPC(this));
          }
        } else if (OB_ISNULL(comp_tmp_file_handle.get())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get invalid compressible tmp file pointer", KR(ret), K(fd), KP(comp_tmp_file_handle.get()), KPC(this));
        } else if (OB_FAIL(comp_tmp_file_handle.get()->seal(tenant_id))) {
          LOG_WARN("fail to seal", KR(ret), K(tenant_id), KPC(this));
          if (OB_ERR_TMP_FILE_ALREADY_SEALED == ret) {
            if (ATOMIC_LOAD(&is_sealed_)) {
              ret = OB_SUCCESS;
            }
            break;
          }
        } else {
          tmp_seal_file_num++;
          index++;
        }
      }
      if (OB_SUCC(ret) && 0 < tmp_seal_file_num) {
        ObSpinLockGuard lock_guard(lock_);
        if (OB_UNLIKELY(ATOMIC_LOAD(&is_deleting_))) {
          ret = OB_STATE_NOT_MATCH;
          LOG_ERROR("bunch is deleting", KR(ret), K(tenant_id), KPC(this));
        } else {
          //it's ok if thread_B sets sealed_tmp_file_cnt_ first
          //    (threadA started the seal operation before threadB)
          //    because threadA must finish seal operations without setting sealed_tmp_file_cnt_
          //That is to say, the files before sealed_tmp_file_cnt_ are all sealed.
          int64_t new_cnt = ATOMIC_AAF(&sealed_tmp_file_cnt_, tmp_seal_file_num);
          seal_file_num = tmp_seal_file_num;
          if (new_cnt > compressible_tmp_file_cnt_) {
            ret = OB_ERR_UNEXPECTED;
            LOG_ERROR("unexpected new_cnt", KR(ret), K(new_cnt), KPC(this));
          }
        }
      }
    }
    if (OB_FAIL(ret)) {
      ATOMIC_STORE(&err_marked_, true);
    }
  } else {
    //skip this bunch
  }
  return ret;
}

int ObCompressTmpFileBunch::get_total_file_size(int64_t &file_size)
{
  int ret = OB_SUCCESS;
  file_size = 0;
  if (OB_UNLIKELY(fds_.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected fds", KR(ret), KPC(this));
  } else {
    ObCompressTmpFileHandle* comp_tmp_file_handle = nullptr;
    int64_t fd = ObTmpFileGlobal::INVALID_TMP_FILE_FD;
    int64_t comp_file_size = 0;
    ARRAY_FOREACH_N(fds_, i, cnt) {
      fd = fds_.at(i);
      comp_tmp_file_handle = &comp_file_handles_.at(i);
      if (OB_ISNULL(comp_tmp_file_handle->get())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get invalid compressible tmp file pointer",
            KR(ret), K(fd), KP(comp_tmp_file_handle->get()), KPC(this));
      } else {
        comp_file_size = 0;
        comp_tmp_file_handle->get()->get_file_size(comp_file_size);
        file_size += comp_file_size;
      }
    }
  }
  return ret;
}

bool ObCompressTmpFileBunch::can_remove()
{
  return 1 == get_ref_cnt();
}

void ObCompressTmpFileBunch::set_deleting(int64_t &unsealed_file_cnt)
{
  ObSpinLockGuard lock_guard(lock_);
  ATOMIC_STORE(&is_deleting_, true);
  unsealed_file_cnt = compressible_tmp_file_cnt_ - ATOMIC_LOAD(&sealed_tmp_file_cnt_);
  comp_file_handles_.reset();
}

void ObCompressTmpFileBunch::get_unsealed_file_cnt(
     bool &is_normal_bunch,
     int64_t &unsealed_file_cnt)
{
  is_normal_bunch = false;
  unsealed_file_cnt = 0;
  if (!ATOMIC_LOAD(&is_sealed_) && !ATOMIC_LOAD(&is_deleting_) && !ATOMIC_LOAD(&err_marked_)) {
    unsealed_file_cnt = compressible_tmp_file_cnt_ - ATOMIC_LOAD(&sealed_tmp_file_cnt_);
  }
  if (!ATOMIC_LOAD(&is_sealed_) && !ATOMIC_LOAD(&is_deleting_) && !ATOMIC_LOAD(&err_marked_)) {
    is_normal_bunch = true;
  }
}

ObCompTmpFileBunchHandle::ObCompTmpFileBunchHandle(ObCompressTmpFileBunch *tmp_file_bunch)
  : ptr_(tmp_file_bunch)
{
  if (ptr_ != nullptr) {
    ptr_->inc_ref_cnt();
  }
}

ObCompTmpFileBunchHandle::ObCompTmpFileBunchHandle(const ObCompTmpFileBunchHandle &handle)
  : ptr_(nullptr)
{
  operator=(handle);
}

ObCompTmpFileBunchHandle & ObCompTmpFileBunchHandle::operator=(const ObCompTmpFileBunchHandle &other)
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

void ObCompTmpFileBunchHandle::reset()
{
  if (ptr_ != nullptr) {
    ptr_->dec_ref_cnt();
    if (0 == ptr_->get_ref_cnt()) {
      ptr_->~ObCompressTmpFileBunch();
    }
    ptr_ = nullptr;
  }
}

}
}