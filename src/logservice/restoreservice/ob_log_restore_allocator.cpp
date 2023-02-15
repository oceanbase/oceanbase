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

#include "ob_log_restore_allocator.h"
#include "lib/ob_define.h"
#include "lib/ob_errno.h"
#include <cstdint>

namespace oceanbase
{
namespace logservice
{
ObLogRestoreAllocator::ObLogRestoreAllocator() :
  inited_(false),
  tenant_id_(OB_INVALID_TENANT_ID),
  iterator_buf_allocator_()
{}

ObLogRestoreAllocator::~ObLogRestoreAllocator()
{
  destroy();
}

int ObLogRestoreAllocator::init(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  const int64_t max_restore_data_size = 1024 * 1024 * 1024L;
  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
    CLOG_LOG(WARN, "ObLogRestoreAllocator has been inited", K(ret), K(tenant_id));
  } else if (OB_FAIL(iterator_buf_allocator_.init("IterBuf", max_restore_data_size))) {
    CLOG_LOG(WARN, "iterator_buf_allocator_ init failed", K(ret));
  } else {
    tenant_id_ = tenant_id;
    inited_ = true;
  }
  return ret;
}

void ObLogRestoreAllocator::destroy()
{
  if (inited_) {
    tenant_id_ = OB_INVALID_TENANT_ID;
    (void)iterator_buf_allocator_.destroy();
    inited_ = false;
  }
}

char *ObLogRestoreAllocator::alloc_iterator_buffer(const int64_t size)
{
  char *data = NULL;

  if (OB_UNLIKELY(! inited_)) {
    ARCHIVE_LOG_RET(WARN, OB_NOT_INIT, "ObArchiveAllocator not init");
  } else if (OB_ISNULL(data = static_cast<char *>(iterator_buf_allocator_.acquire(size)))) {
    // alloc fail
  } else {
  }
  return data;
}

void ObLogRestoreAllocator::free_iterator_buffer(void *buf)
{
  if (NULL != buf) {
    iterator_buf_allocator_.reclaim(buf);
  }
}

void ObLogRestoreAllocator::weed_out_iterator_buffer()
{
  iterator_buf_allocator_.weed_out();
}

} // namespace logservice
} // namespace oceanbase
