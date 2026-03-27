/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE

#include "storage/tmp_file/ob_compress_tmp_file_io_handle.h"
#include "storage/tmp_file/ob_compress_tmp_file_manager.h"

namespace oceanbase
{
using namespace storage;
using namespace share;

namespace tmp_file
{

ObCompTmpFileIOHandle::ObCompTmpFileIOHandle()
  : io_handle_(),
    tenant_id_(OB_INVALID_TENANT_ID),
    fd_(ObTmpFileGlobal::INVALID_TMP_FILE_FD),
    is_aio_read_(false),
    decompress_buf_(nullptr),
    user_buf_(nullptr),
    user_buf_size_(0)
{
}

ObCompTmpFileIOHandle::~ObCompTmpFileIOHandle()
{
  io_handle_.reset();
  tenant_id_ = OB_INVALID_TENANT_ID;
  fd_ = ObTmpFileGlobal::INVALID_TMP_FILE_FD;
  is_aio_read_ = false;
  decompress_buf_ = nullptr;
  user_buf_ = nullptr;
  user_buf_size_ = 0;
}

void ObCompTmpFileIOHandle::reset()
{
  io_handle_.reset();
  tenant_id_ = OB_INVALID_TENANT_ID;
  fd_ = ObTmpFileGlobal::INVALID_TMP_FILE_FD;
  is_aio_read_ = false;
  decompress_buf_ = nullptr;
  user_buf_ = nullptr;
  user_buf_size_ = 0;
}

bool ObCompTmpFileIOHandle::is_valid()
{
  return io_handle_.is_valid()
          && ObTmpFileGlobal::INVALID_TMP_FILE_FD != fd_
          && (!is_aio_read_ ||
              (is_aio_read_ && nullptr != user_buf_ && 0 < user_buf_size_));
}

char* ObCompTmpFileIOHandle::get_buffer()
{
  return user_buf_;
}

} // end namespace tmp_file
} // end namespace oceanbase