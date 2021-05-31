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

#define USING_LOG_PREFIX SQL_EXE

#include "ob_interm_result_item.h"
#include "share/ob_scanner.h"
#include "storage/blocksstable/ob_tmp_file.h"
#include "share/ob_worker.h"

namespace oceanbase {
using namespace common;
namespace sql {

void ObIIntermResultItem::reset()
{
  row_count_ = 0;
  data_len_ = 0;
}

ObIntermResultItem::ObIntermResultItem(const char* label, uint64_t tenant_id)
    : ObIIntermResultItem(),
      allocator_(label, common::OB_MALLOC_NORMAL_BLOCK_SIZE, tenant_id, common::ObCtxIds::WORK_AREA),
      data_buf_(NULL)
{}

ObIntermResultItem::~ObIntermResultItem()
{
  reset();
}

void ObIntermResultItem::reset()
{
  ObIIntermResultItem::reset();
  data_buf_ = NULL;
  allocator_.reset();
}

int ObIntermResultItem::assign(const ObIntermResultItem& other)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!other.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("other interm result interm is invalid", K(ret), K(other));
  } else {
    reset();
    row_count_ = other.row_count_;
    data_len_ = other.data_len_;
    if (other.data_len_ > 0) {
      if (OB_ISNULL(data_buf_ = static_cast<char*>(allocator_.alloc(other.data_len_)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc memory", K(ret), K(other.data_len_));
      } else {
        MEMCPY(data_buf_, other.data_buf_, other.data_len_);
      }
    }
  }
  return ret;
}

int ObIntermResultItem::from_scanner(const ObScanner& scanner)
{
  int ret = OB_SUCCESS;
  reset();
  row_count_ = scanner.get_row_count();
  data_len_ = scanner.get_serialize_size();
  if (data_len_ > 0) {
    int64_t pos = 0;
    if (OB_ISNULL(data_buf_ = static_cast<char*>(allocator_.alloc(data_len_)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc memory", K(ret), K(data_len_));
    } else if (OB_FAIL(scanner.serialize(data_buf_, data_len_, pos))) {
      LOG_WARN("fail to serialize scanner", K(ret), K(data_len_), K(pos));
    }
  }
  return ret;
}

int ObIntermResultItem::from_disk_ir_item(ObDiskIntermResultItem& disk_item)
{
  int ret = OB_SUCCESS;
  if (!disk_item.is_inited()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid disk interm result", K(ret), K(disk_item));
  } else {
    reset();
    row_count_ = disk_item.get_row_count();
    data_len_ = disk_item.get_data_len();
    if (data_len_ > 0) {
      if (OB_ISNULL(data_buf_ = static_cast<char*>(allocator_.alloc(data_len_)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("alloc memory failed", K(ret), K_(data_len));
      } else if (OB_FAIL(disk_item.copy_data(data_buf_, data_len_))) {
        LOG_WARN("copy disk interm result item data to memory failed", K(ret));
      }
    }
  }
  return ret;
}

int ObIntermResultItem::to_scanner(ObScanner& scanner)
{
  int ret = OB_SUCCESS;
  scanner.reset();
  if (data_len_ > 0) {
    int64_t pos = 0;
    if (OB_FAIL(scanner.deserialize(data_buf_, data_len_, pos))) {
      LOG_WARN("fail to deserialize scanner", K(ret), K(pos), K(data_len_));
    }
  }
  return ret;
}

int ObIntermResultItem::copy_data(char* buf, const int64_t size) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf) || size < data_len_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalud argument", KP(buf), K(size), K_(data_len));
  } else {
    if (data_len_ > 0) {
      MEMCPY(buf, data_buf_, data_len_);
    }
  }
  return ret;
}

ObDiskIntermResultItem::ObDiskIntermResultItem()
    : ObIIntermResultItem(), tenant_id_(0), fd_(-1), dir_id_(-1), offset_(0)
{}

ObDiskIntermResultItem::~ObDiskIntermResultItem()
{
  reset();
}

int ObDiskIntermResultItem::init(uint64_t tenant_id, const int64_t fd, const int64_t dir_id, const int64_t offset)
{
  int ret = OB_SUCCESS;
  if (is_inited()) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (fd < 0 || offset < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(fd), K(offset));
  } else {
    tenant_id_ = tenant_id;
    fd_ = fd;
    dir_id_ = dir_id;
    offset_ = offset;
  }
  return ret;
}

void ObDiskIntermResultItem::reset()
{
  ObIIntermResultItem::reset();
  tenant_id_ = 0;
  fd_ = -1;
  dir_id_ = -1;
  offset_ = 0;
}

int ObDiskIntermResultItem::from_scanner(const ObScanner& scanner)
{
  int ret = OB_SUCCESS;
  if (!is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    row_count_ = scanner.get_row_count();
    data_len_ = scanner.get_serialize_size();

    blocksstable::ObTmpFileIOInfo io;
    io.fd_ = fd_;
    io.dir_id_ = dir_id_;
    io.size_ = data_len_;
    io.tenant_id_ = tenant_id_;
    io.io_desc_.category_ = GCONF._large_query_io_percentage.get_value() > 0 ? common::LARGE_QUERY_IO : common::USER_IO;
    io.io_desc_.wait_event_no_ = ObWaitEventIds::INTERM_RESULT_DISK_WRITE;
    int64_t timeout_ms = 0;
    if (data_len_ > 0) {
      ObArenaAllocator allocator(
          ObModIds::OB_SQL_EXECUTOR_INTERM_RESULT_ITEM, OB_MALLOC_NORMAL_BLOCK_SIZE, tenant_id_, ObCtxIds::WORK_AREA);
      int64_t size = scanner.get_serialize_size();
      char* buf = static_cast<char*>(allocator.alloc(size));
      io.buf_ = buf;
      int64_t pos = 0;
      if (NULL == buf) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("alloc memory failed", K(ret), K(size));
      } else if (OB_FAIL(get_timeout(timeout_ms))) {
        LOG_WARN("get timeout failed", K(ret));
      } else if (OB_FAIL(scanner.serialize(buf, size, pos))) {
        LOG_WARN("serialize scanner failed", K(ret), KP(buf), K(size));
      } else if (OB_FAIL(FILE_MANAGER_INSTANCE_V2.write(io, timeout_ms))) {
        LOG_WARN("write to disk failed", K(ret), K(io), K(timeout_ms));
      }
    }
  }
  return ret;
}

int ObDiskIntermResultItem::copy_data(char* buf, const int64_t size) const
{
  int ret = OB_SUCCESS;
  if (!is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init");
  } else if (data_len_ > 0) {
    if (NULL == buf || size < data_len_) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(ret), KP(buf), K(size), K_(data_len));
    } else {
      int64_t timeout_ms = 0;
      blocksstable::ObTmpFileIOInfo io;
      io.fd_ = fd_;
      io.dir_id_ = dir_id_;
      io.buf_ = buf;
      io.size_ = data_len_;
      io.tenant_id_ = tenant_id_;
      io.io_desc_.category_ =
          GCONF._large_query_io_percentage.get_value() > 0 ? common::LARGE_QUERY_IO : common::USER_IO;
      io.io_desc_.wait_event_no_ = ObWaitEventIds::INTERM_RESULT_DISK_READ;
      blocksstable::ObTmpFileIOHandle handle;
      if (OB_FAIL(get_timeout(timeout_ms))) {
        LOG_WARN("get timeout failed", K(ret));
      } else if (OB_FAIL(FILE_MANAGER_INSTANCE_V2.pread(io, offset_, timeout_ms, handle))) {
        LOG_WARN("read from disk failed", K(ret), K(io), K(timeout_ms));
      } else if (handle.get_data_size() != data_len_) {
        ret = OB_INNER_STAT_ERROR;
        LOG_WARN("read data less than expected", K(ret), K(io), "read_size", handle.get_data_size());
      }
    }
  }
  return ret;
}

int ObDiskIntermResultItem::get_timeout(int64_t& timeout_ms)
{
  int ret = OB_SUCCESS;
  const int64_t timeout_us = THIS_WORKER.get_timeout_remain();
  if (timeout_us / 1000 <= 0) {
    ret = OB_TIMEOUT;
    LOG_WARN("query is timeout", K(ret), K(timeout_us));
  } else {
    timeout_ms = timeout_us / 1000;
  }
  return ret;
}

}  // end namespace sql
}  // end namespace oceanbase
