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
#include "ob_super_block_buffer_holder.h"

namespace oceanbase
{
using namespace common;
namespace blocksstable
{
ObSuperBlockBufferHolder::ObSuperBlockBufferHolder()
  : is_inited_(false),
    buf_(NULL),
    len_(0),
    allocator_(ObModIds::OB_SUPER_BLOCK_BUFFER)
{
}

ObSuperBlockBufferHolder::~ObSuperBlockBufferHolder()
{
}

int ObSuperBlockBufferHolder::init(const int64_t buf_size)
{
  int ret = OB_SUCCESS;
  char *tmp_buf = NULL;

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "inited twice", K(ret));
  } else if (buf_size <= 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(buf_size));
  } else if (NULL == (tmp_buf = (char*) allocator_.alloc(buf_size + DIO_READ_ALIGN_SIZE))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(ERROR, "allocate buffer for super block fail", K(ret), K(buf_size), KP(tmp_buf));
  } else {
    // ObArenaAllocator::alloc_aligned has bug, manually align address
    buf_ = (char *) upper_align((int64_t) tmp_buf, DIO_READ_ALIGN_SIZE);
    len_ = buf_size;
    is_inited_ = true;
  }

  if (IS_NOT_INIT) {
    reset();
  }
  return ret;
}

void ObSuperBlockBufferHolder::reset()
{
  buf_ = NULL;
  len_ = 0;
  allocator_.reset();
  is_inited_ = false;
}

int ObSuperBlockBufferHolder::serialize_super_block(const storage::ObServerSuperBlock &super_block)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
  } else if (OB_FAIL(super_block.serialize(buf_, len_, pos))) {
    STORAGE_LOG(ERROR, "fail to write super block buf", K(ret), KP_(buf), K_(len),
        K(pos), K(super_block));
  }
  return ret;
}

int ObSuperBlockBufferHolder::assign(const char *buf, const int64_t buf_len)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
  } else if (OB_ISNULL(buf) || buf_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), KP(buf), K(buf_len));
  } else if (buf_len > len_) {
    ret = OB_BUF_NOT_ENOUGH;
    LOG_WARN("buf not enough", K(ret), K(buf_len), K(len_));
  } else {
    MEMCPY(buf_, buf, buf_len);
  }
  return ret;
}

int ObSuperBlockBufferHolder::deserialize_super_block_header_version(int64_t &version)
{
  int ret = OB_SUCCESS;
  storage::ObServerSuperBlockHeader new_header;
  int64_t pos = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
  } else if (OB_FAIL(new_header.deserialize(buf_, len_, pos))) {
    LOG_WARN("fail to deserialize super block header", K(ret));
  } else {
    version = new_header.version_;
    STORAGE_LOG(INFO, "load superblock header version ok.", K(version));
  }
  return ret;
}

} // namespace blocksstable
} // namespace oceanbase
