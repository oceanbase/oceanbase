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

#include "storage/tmp_file/ob_tmp_file_io_info.h"
#include "storage/tmp_file/ob_sn_tmp_file_io_handle.h"
#include "storage/tmp_file/ob_tmp_file_manager.h"

namespace oceanbase
{
using namespace storage;
using namespace share;

namespace tmp_file
{
ObSNTmpFileIOHandle::ObSNTmpFileIOHandle()
  : is_inited_(false),
    fd_(ObTmpFileGlobal::INVALID_TMP_FILE_FD),
    ctx_(),
    buf_(nullptr),
    update_offset_in_file_(false),
    buf_size_(-1),
    done_size_(-1),
    read_offset_in_file_(-1)
{
}

ObSNTmpFileIOHandle::~ObSNTmpFileIOHandle()
{
  reset();
}

void ObSNTmpFileIOHandle::reset()
{
  is_inited_ = false;
  ctx_.reset();
  fd_ = ObTmpFileGlobal::INVALID_TMP_FILE_FD;
  buf_ = nullptr;
  update_offset_in_file_ = false;
  buf_size_ = -1;
  done_size_ = -1;
  read_offset_in_file_ = -1;
}

int ObSNTmpFileIOHandle::init_write(const ObTmpFileIOInfo &io_info)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObSNTmpFileIOHandle has been inited twice", KR(ret), KPC(this));
  } else if (OB_UNLIKELY(!io_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(io_info), KPC(this));
  } else if (OB_FAIL(ctx_.init(io_info.fd_, io_info.dir_id_, false /*is_read*/, io_info.io_desc_,
                               io_info.io_timeout_ms_, io_info.disable_page_cache_, io_info.disable_block_cache_))) {
    LOG_WARN("failed to init io handle context", KR(ret), K(io_info));
  } else if (OB_FAIL(ctx_.prepare_write(io_info.buf_, io_info.size_))) {
    LOG_WARN("fail to prepare write context", KR(ret), KPC(this));
  } else {
    is_inited_ = true;
    fd_ = io_info.fd_;
    buf_ = io_info.buf_;
    buf_size_ = io_info.size_;
    done_size_ = 0;
  }

  return ret;
}

int ObSNTmpFileIOHandle::init_pread(const ObTmpFileIOInfo &io_info, const int64_t read_offset)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObSNTmpFileIOHandle has been inited twice", KR(ret), KPC(this));
  } else if (OB_UNLIKELY(!io_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(io_info));
  } else if (OB_UNLIKELY(read_offset < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(read_offset));
  } else if (OB_FAIL(ctx_.init(io_info.fd_, io_info.dir_id_, true /*is_read*/, io_info.io_desc_,
                               io_info.io_timeout_ms_, io_info.disable_page_cache_, io_info.disable_block_cache_))) {
    LOG_WARN("failed to init io handle context", KR(ret), K(io_info));
  } else if (OB_FAIL(ctx_.prepare_read(io_info.buf_, MIN(io_info.size_, ObTmpFileGlobal::TMP_FILE_READ_BATCH_SIZE), read_offset))) {
    LOG_WARN("fail to prepare read context", KR(ret), KPC(this), K(read_offset));
  } else {
    is_inited_ = true;
    fd_ = io_info.fd_;
    buf_ = io_info.buf_;
    buf_size_ = io_info.size_;
    done_size_ = 0;
    read_offset_in_file_ = read_offset;
  }

  return ret;
}

int ObSNTmpFileIOHandle::init_read(const ObTmpFileIOInfo &io_info)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObSNTmpFileIOHandle has been inited twice", KR(ret), KPC(this));
  } else if (OB_UNLIKELY(!io_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(io_info));
  } else if (OB_FAIL(ctx_.init(io_info.fd_, io_info.dir_id_, true /*is_read*/, io_info.io_desc_,
                               io_info.io_timeout_ms_, io_info.disable_page_cache_, io_info.disable_block_cache_))) {
    LOG_WARN("failed to init io handle context", KR(ret), K(io_info));
  } else if (OB_FAIL(ctx_.prepare_read(io_info.buf_, MIN(io_info.size_, ObTmpFileGlobal::TMP_FILE_READ_BATCH_SIZE)))) {
    LOG_WARN("fail to prepare read context", KR(ret), KPC(this));
  } else {
    is_inited_ = true;
    fd_ = io_info.fd_;
    buf_ = io_info.buf_;
    buf_size_ = io_info.size_;
    done_size_ = 0;
    read_offset_in_file_ = -1;
    update_offset_in_file_ = true;
  }

  return ret;
}

bool ObSNTmpFileIOHandle::is_valid() const
{
  return is_inited_ &&
         nullptr != buf_ &&
         done_size_ >= 0 && buf_size_ > 0 &&
         buf_size_ >= done_size_;
}

int ObSNTmpFileIOHandle::wait()
{
  int ret = OB_SUCCESS;
  ObTmpFileHandle file_handle;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret));
  } else if (OB_UNLIKELY(!is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid handle", KR(ret), KPC(this));
  } else if (is_finished() || !ctx_.is_read()) {
    // do nothing
  } else if (OB_FAIL(ctx_.wait())) {
    LOG_WARN("fail to wait tmp file io", KR(ret), K(ctx_), KPC(this));
  } else if (OB_FAIL(handle_finished_ctx_(ctx_))) {
    LOG_WARN("fail to handle finished ctx", KR(ret), KPC(this));
  } else if (OB_UNLIKELY(done_size_ > buf_size_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("done size is larger than total todo size", KR(ret), KPC(this));
  } else if (OB_FAIL(MTL(ObTenantTmpFileManager*)->get_sn_file_manager().get_tmp_file(fd_, file_handle))) {
    LOG_WARN("fail to get tmp file handle", KR(ret), K(fd_));
  } else {
    while (OB_SUCC(ret) && !is_finished()) {
      if (OB_FAIL(ctx_.prepare_read(buf_ + done_size_,
                                    MIN(buf_size_ - done_size_,
                                        ObTmpFileGlobal::TMP_FILE_READ_BATCH_SIZE),
                                    read_offset_in_file_))) {
        LOG_WARN("fail to generate read ctx", KR(ret), KPC(this));
      } else if (OB_FAIL(file_handle.get()->aio_pread(ctx_))) {
        LOG_WARN("fail to continue read once batch", KR(ret), K(ctx_));
      } else if (OB_FAIL(ctx_.wait())) {
        LOG_WARN("fail to wait tmp file io", KR(ret), K(ctx_));
      } else if (OB_FAIL(handle_finished_ctx_(ctx_))) {
        LOG_WARN("fail to handle finished ctx", KR(ret), KPC(this));
      }
    } // end while

    if (update_offset_in_file_ && (OB_SUCC(ret) || OB_ITER_END == ret)) {
      file_handle.get()->update_read_offset(read_offset_in_file_);
    }
  }

  return ret;
}

int ObSNTmpFileIOHandle::handle_finished_ctx_(ObTmpFileIOCtx &ctx)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!ctx.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(ctx));
  } else {
    if (ctx_.is_read()) {
      read_offset_in_file_ = ctx.get_read_offset_in_file();
    }
    done_size_ += ctx.get_done_size();
    ctx.reuse();
  }

  return ret;
}

} // end namespace tmp_file
} // end namespace oceanbase
