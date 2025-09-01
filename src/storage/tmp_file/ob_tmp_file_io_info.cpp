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
#include "share/config/ob_server_config.h"

namespace oceanbase
{
using namespace share;

namespace tmp_file
{

ObTmpFileIOInfo::ObTmpFileIOInfo()
    : fd_(ObTmpFileGlobal::INVALID_TMP_FILE_FD), buf_(nullptr), size_(0),
      disable_page_cache_(false), prefetch_(false),
      io_desc_(), io_timeout_ms_(GCONF._data_storage_io_timeout / 1000L)
{}

ObTmpFileIOInfo::~ObTmpFileIOInfo()
{
  reset();
}

void ObTmpFileIOInfo::reset()
{
  fd_ = ObTmpFileGlobal::INVALID_TMP_FILE_FD;
  size_ = 0;
  io_timeout_ms_ = GCONF._data_storage_io_timeout / 1000L;
  buf_ = nullptr;
  io_desc_.reset();
  disable_page_cache_ = false;
}

bool ObTmpFileIOInfo::is_valid() const
{
  return fd_ != ObTmpFileGlobal::INVALID_TMP_FILE_FD &&
         size_ > 0 &&
         nullptr != buf_ && io_desc_.is_valid() && io_timeout_ms_ >= 0;
}

} // end namespace tmp_file
} // end namespace oceanbase
