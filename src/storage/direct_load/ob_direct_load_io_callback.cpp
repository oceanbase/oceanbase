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

#include "storage/direct_load/ob_direct_load_io_callback.h"
#include "lib/allocator/ob_malloc.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/utility/ob_utility.h"
#include "share/ob_errno.h"

namespace oceanbase
{
namespace storage
{
using namespace common;

ObDirectLoadIOCallback::ObDirectLoadIOCallback(uint64_t tenant_id)
  : tenant_id_(tenant_id), data_buf_(nullptr)
{
}

ObDirectLoadIOCallback::~ObDirectLoadIOCallback()
{
  if (nullptr != data_buf_) {
    ob_free(data_buf_);
    data_buf_ = nullptr;
  }
}

int ObDirectLoadIOCallback::inner_process(const char *data_buffer, const int64_t size)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(size <= 0 || data_buffer == nullptr)) {
    ret = OB_INVALID_DATA;
    LOG_WARN("invalid data buffer size", K(ret), K(size), KP(data_buffer));
  } else if (OB_ISNULL(data_buf_ = static_cast<char *>(ob_malloc(size, "LoadIO")))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("Fail to allocate memory, ", K(ret));
  } else {
    MEMCPY(data_buf_, data_buffer, size);
  }
  if (OB_FAIL(ret) && nullptr != data_buf_) {
    ob_free(data_buf_);
    data_buf_ = nullptr;
  }
  return ret;
}

int ObDirectLoadIOCallback::inner_deep_copy(char *buf, const int64_t buf_len,
                                            ObIOCallback *&copied_callback) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf) || OB_UNLIKELY(buf_len < size())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(buf), K(buf_len));
  } else {
    copied_callback = new (buf) ObDirectLoadIOCallback(tenant_id_);
  }
  return ret;
}

}  // namespace storage
}  // namespace oceanbase
