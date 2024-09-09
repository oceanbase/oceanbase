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

#include "storage/tmp_file/ob_tmp_file_io_handle.h"
#include "observer/ob_server_struct.h"

namespace oceanbase
{
using namespace storage;
using namespace share;

namespace tmp_file
{

ObTmpFileIOHandle::ObTmpFileIOHandle()
{
  is_shared_storage_mode_ = false;
  type_decided_ = false;
  sn_handle_ = nullptr;
  ss_handle_ = nullptr;
}

ObTmpFileIOHandle::~ObTmpFileIOHandle()
{
  if (type_decided_) {
    if (!is_shared_storage_mode_) {
      sn_handle_->~ObSNTmpFileIOHandle();
    } else {
      ss_handle_->~ObSSTmpFileIOHandle();
    }
  }
  is_shared_storage_mode_ = false;
  type_decided_ = false;
  sn_handle_ = nullptr;
  ss_handle_ = nullptr;
}

int ObTmpFileIOHandle::wait()
{
  int ret = OB_SUCCESS;
  check_or_set_handle_type_();
  if (!GCTX.is_shared_storage_mode()) {
    if (OB_FAIL(sn_handle_->wait())) {
      LOG_WARN("fail to wait", KR(ret), K(sn_handle_));
    }
#ifdef OB_BUILD_SHARED_STORAGE
  } else  {
    if (OB_FAIL(ss_handle_->wait())) {
      LOG_WARN("fail to wait", KR(ret), K(ss_handle_));
    }
#endif
  }
  return ret;
}

void ObTmpFileIOHandle::reset()
{
  check_or_set_handle_type_();
  if (!GCTX.is_shared_storage_mode()) {
    sn_handle_->reset();
#ifdef OB_BUILD_SHARED_STORAGE
  } else  {
    ss_handle_->reset();
#endif
  }
}

bool ObTmpFileIOHandle::is_valid()
{
  bool b_ret = false;
  check_or_set_handle_type_();
  if (!GCTX.is_shared_storage_mode()) {
    b_ret = sn_handle_->is_valid();
#ifdef OB_BUILD_SHARED_STORAGE
  } else  {
    b_ret = ss_handle_->is_valid();
#endif
  }

  return b_ret;
}

int64_t ObTmpFileIOHandle::get_done_size()
{
  int64_t done_size = -1;
  check_or_set_handle_type_();
  if (!GCTX.is_shared_storage_mode()) {
    done_size = sn_handle_->get_done_size();
#ifdef OB_BUILD_SHARED_STORAGE
  } else  {
    if (OB_UNLIKELY(ss_handle_->is_read())) {
      done_size = ss_handle_->get_data_size();
    } else {
      done_size = ss_handle_->get_expect_write_size() - ss_handle_->get_data_size();
    }
#endif
  }
  return done_size;
}

char* ObTmpFileIOHandle::get_buffer()
{
  char* buf = nullptr;
  check_or_set_handle_type_();
  if (!GCTX.is_shared_storage_mode()) {
    buf = sn_handle_->get_buffer();
#ifdef OB_BUILD_SHARED_STORAGE
  } else  {
    buf = ss_handle_->get_buffer();
#endif
  }

  return buf;
}

ObSNTmpFileIOHandle &ObTmpFileIOHandle::get_sn_handle()
{
  int ret = OB_SUCCESS;
  check_or_set_handle_type_();
  if (GCTX.is_shared_storage_mode()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("is_shared_storage_mode in GCTX is unexpected",
        KR(ret), K(GCTX.is_shared_storage_mode()), KPC(this));
    ob_abort();
  }
  return *sn_handle_;
}

blocksstable::ObSSTmpFileIOHandle &ObTmpFileIOHandle::get_ss_handle()
{
  int ret = OB_SUCCESS;
  check_or_set_handle_type_();
  if (!GCTX.is_shared_storage_mode()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("is_shared_storage_mode in GCTX is unexpected",
        KR(ret), K(GCTX.is_shared_storage_mode()), KPC(this));
    ob_abort();
  }
  return *ss_handle_;
}

void ObTmpFileIOHandle::check_or_set_handle_type_()
{
  int ret = OB_SUCCESS;

  if (!type_decided_) {
    if (!GCTX.is_shared_storage_mode()) {
      is_shared_storage_mode_ = false;
      sn_handle_ = new(buf_) ObSNTmpFileIOHandle();
    } else {
      is_shared_storage_mode_ = true;
      ss_handle_ = new(buf_) blocksstable::ObSSTmpFileIOHandle();
    }
    type_decided_ = true;
  } else {
    if (GCTX.is_shared_storage_mode() != is_shared_storage_mode_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("is_shared_storage_mode_ of ObTmpFileIOHandle not equal to GCTX",
          KR(ret), K(GCTX.is_shared_storage_mode()), K(is_shared_storage_mode_), KPC(this));
      ob_abort();
    }
    if (!is_shared_storage_mode_) {
      if (OB_ISNULL(sn_handle_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("sn_handle_ is unexpected nullptr", KR(ret), KPC(this));
        ob_abort();
      }
    } else {
      if (OB_ISNULL(ss_handle_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("ss_handle_ is unexpected nullptr", KR(ret), KPC(this));
        ob_abort();
      }
    }
  }
}

} // end namespace tmp_file
} // end namespace oceanbase
