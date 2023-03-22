// Copyright (c) 2022-present Oceanbase Inc. All Rights Reserved.
// Author:
//   suzhi.yt <>

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
  : tenant_id_(tenant_id), io_buf_(nullptr), data_buf_(nullptr)
{
}

ObDirectLoadIOCallback::~ObDirectLoadIOCallback()
{
  if (nullptr != io_buf_) {
    ob_free_align(io_buf_);
    io_buf_ = nullptr;
  }
  data_buf_ = nullptr;
}

int ObDirectLoadIOCallback::alloc_io_buf(char *&io_buf, int64_t &size, int64_t &offset)
{
  int ret = OB_SUCCESS;
  const int64_t aligned_offset = lower_align(offset, DIO_READ_ALIGN_SIZE);
  const int64_t aligned_size = upper_align(size + offset - aligned_offset, DIO_READ_ALIGN_SIZE);
  ObMemAttr attr;
  attr.tenant_id_ = tenant_id_;
  attr.label_ = ObModIds::OB_SQL_LOAD_DATA;
  if (OB_ISNULL(io_buf_ =
                  static_cast<char *>(ob_malloc_align(DIO_READ_ALIGN_SIZE, aligned_size, attr)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate memory", KR(ret), K(aligned_size));
  } else {
    data_buf_ = io_buf_ + (offset - aligned_offset);
    io_buf = io_buf_;
    size = aligned_size;
    offset = aligned_offset;
  }
  return ret;
}

int ObDirectLoadIOCallback::inner_process(const bool is_success)
{
  return OB_SUCCESS;
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
